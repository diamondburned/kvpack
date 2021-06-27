package kvpack

import (
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"unsafe"

	"github.com/diamondburned/kvpack/defract"
	"github.com/pkg/errors"
)

// Put puts the given value into the database ID'd by the given key. If v's type
// is a value or a pointer to a byte slice or a string, then a fast path is
// used, and the values are put into the database as-is.
func (tx *Transaction) Put(k []byte, v interface{}) error {
	if tx.ro {
		return ErrReadOnly
	}

	key := tx.kb.Append(tx.namespace(), k)
	if err := tx.Tx.DeletePrefix(key); err != nil {
		return errors.Wrap(err, "failed to override key")
	}

	switch v := v.(type) {
	case []byte:
		return tx.Tx.Put(key, v)
	case *[]byte:
		return tx.Tx.Put(key, *v)
	case string:
		ptr := &(*(*[]byte)(unsafe.Pointer(&v)))[0]
		return tx.Tx.Put(key, unsafe.Slice(ptr, len(v)))
	case *string:
		ptr := &(*(*[]byte)(unsafe.Pointer(v)))[0]
		return tx.Tx.Put(key, unsafe.Slice(ptr, len(*v)))
	}

	typ, ptr := defract.UnderlyingPtr(v)
	if ptr == nil {
		// Skip the nil pointer entirely. This isn't useless, since we've
		// already wiped the key above.
		return nil
	}

	// Not setting a zero-value field is likely faster overall, since the
	// database is likely slower than our code.
	if defract.IsZero(ptr, typ.Size()) {
		return nil
	}

	return tx.putValue(key, typ, typ.Kind(), ptr, 0)
}

func (tx *Transaction) putValue(
	k []byte, typ reflect.Type, kind reflect.Kind, ptr unsafe.Pointer, rec int) error {

	if rec > recursionLimit {
		return ErrTooRecursed
	}

	if kind == reflect.Ptr {
		typ, ptr = defract.Indirect(typ, ptr)
		if ptr == nil {
			// Do nothing with a nil pointer.
			return nil
		}
	}

	// Comparing Kind is a lot faster.
	switch kind := typ.Kind(); kind {
	// Handle uint and int like variable-length integers. These helper functions
	// pool the backing array, so this should be decently fast.
	case reflect.Uint:
		return tx.Tx.Put(k, defract.UintLE((*uint)(ptr)))
	case reflect.Int:
		return tx.Tx.Put(k, defract.IntLE((*int)(ptr)))

	case reflect.Bool:
		// A bool can probably be treated as 1 byte, so we can cast it to that
		// and convert it to a pointer.
		return tx.Tx.Put(k, (*[1]byte)(ptr)[:])

	case
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128,
		reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:

		return tx.Tx.Put(k, defract.NumberLE(kind, ptr))

	case reflect.String:
		return tx.putBytes(k, ptr)

	case reflect.Slice:
		if typ != defract.ByteSlice {
			return tx.putSlice(k, typ, ptr, rec+1)
		}
		return tx.putBytes(k, ptr)

	case reflect.Array:
		panic("TODO: array")

	case reflect.Struct:
		return tx.putStruct(k, defract.GetStructInfo(typ), ptr, rec+1)

	case reflect.Map:
		return tx.putMap(k, typ, ptr, rec+1)
	}

	return fmt.Errorf("unknown type %s", typ)
}

func (tx *Transaction) putBytes(k []byte, ptr unsafe.Pointer) error {
	// Pull only the dat and length out.
	backing, len := defract.StringInfo(ptr)
	if len == 0 {
		// Empty string, so put nothing. Accessing [0] will cause out of
		// bounds.
		return tx.Tx.Put(k, nil)
	}

	return tx.Tx.Put(k, unsafe.Slice((*byte)(backing), len))
}

func (tx *Transaction) putSlice(k []byte, typ reflect.Type, ptr unsafe.Pointer, rec int) error {
	if rec > recursionLimit {
		return ErrTooRecursed
	}

	// slice of <this> type
	underlying := typ.Elem()
	valueKind := underlying.Kind()
	valueSize := underlying.Size()

	// Keeping this as int64 is possibly slower on 32-bit architecture machines,
	// but most machines should be 64-bit nowadays.
	dataPtr, length, _ := defract.SliceInfo(ptr)
	length64 := int64(length)

	// Write the slice length conveniently into the same buffer as the key.
	mapKey, lengthValue := tx.kb.AppendExtra(k, nil, 8)
	mapKey = mapKey[:len(mapKey)-1]
	defract.WriteInt64LE(lengthValue, length64)

	if err := tx.Tx.Put(mapKey, lengthValue); err != nil {
		return errors.Wrap(err, "failed to write slice len")
	}

	rec++

	for i := int64(0); i < length64; i++ {
		elemPtr := unsafe.Add(dataPtr, int64(valueSize)*i)
		// Skip zero-values.
		if defract.IsZero(elemPtr, valueSize) {
			continue
		}

		key, extra := tx.kb.AppendExtra(k, nil, 30)
		key = tx.kb.AvoidOverflow(strconv.AppendInt(key, i, 10), len(key)+len(extra))

		if err := tx.putValue(key, underlying, valueKind, elemPtr, rec); err != nil {
			return errors.Wrapf(err, "index %d", i)
		}
	}

	return nil
}

func (tx *Transaction) putStruct(
	k []byte, info *defract.StructInfo, ptr unsafe.Pointer, rec int) error {

	if rec > recursionLimit {
		return ErrTooRecursed
	}

	// Indicate that the struct does, in fact, exist.
	if err := tx.Tx.Put(k, nil); err != nil {
		return errors.Wrap(err, "failed to write struct presence")
	}

	var err error

	for _, field := range info.Fields {
		ptr := unsafe.Add(ptr, field.Offset)
		// Skip zero-values.
		if defract.IsZero(ptr, field.Size) {
			continue
		}

		key := tx.kb.Append(k, field.Name)

		if field.ChildStruct != nil {
			if field.Indirect {
				if ptr = defract.IndirectOnce(ptr); ptr == nil {
					return nil
				}
			}
			// Field is a struct, use the fast path and skip the map lookup.
			err = tx.putStruct(key, field.ChildStruct, ptr, rec+1)
		} else {
			err = tx.putValue(key, field.Type, field.Kind, ptr, rec+1)
		}

		if err != nil {
			return errors.Wrapf(err, "struct %s field %s", info.Type, field.Name)
		}
	}

	return nil
}

// putMap is slow and allocates. The zero-allocation guarantee does not apply
// for it, because it is too bothersome to be dealt with. Just don't use maps.
func (tx *Transaction) putMap(
	k []byte, typ reflect.Type, ptr unsafe.Pointer, rec int) error {

	if rec > recursionLimit {
		return ErrTooRecursed
	}

	keyType := typ.Key()
	valueType := typ.Elem()
	valueKind := valueType.Kind()

	// keyer gets the reflect.Value's underlying pointer and returns the key.
	var keyer func(reflect.Value) (key []byte)

	switch kind := keyType.Kind(); kind {
	case reflect.Float32, reflect.Float64:
		keyer = func(v reflect.Value) []byte {
			key, extra := tx.kb.AppendExtra(k, nil, 300)

			return tx.kb.AvoidOverflow(
				strconv.AppendFloat(key, v.Float(), 'f', -1, 64),
				len(key)+len(extra),
			)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		keyer = func(v reflect.Value) []byte {
			key, extra := tx.kb.AppendExtra(k, nil, 20)

			return tx.kb.AvoidOverflow(
				strconv.AppendInt(key, v.Int(), 10),
				len(key)+len(extra),
			)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		keyer = func(v reflect.Value) []byte {
			key, extra := tx.kb.AppendExtra(k, nil, 20)

			return tx.kb.AvoidOverflow(
				strconv.AppendUint(key, v.Uint(), 10),
				len(key)+len(extra),
			)
		}
	case reflect.String:
		keyer = func(v reflect.Value) []byte {
			data, len := defract.StringInfo(defract.InterfacePtr(v.Interface()))
			return tx.kb.Append(k, unsafe.Slice((*byte)(data), len))
		}
	default:
		return fmt.Errorf("unsupported key type %s", keyType)
	}

	mapValue := reflect.NewAt(typ, ptr).Elem()
	mapLen := mapValue.Len()
	defer runtime.KeepAlive(&mapLen)

	// Write the length.
	if err := tx.Tx.Put(k, defract.IntLE(&mapLen)); err != nil {
		return errors.Wrap(err, "failed to write map len")
	}

	// There's really no choice but to handle maps the slow way.
	for mapIter := mapValue.MapRange(); mapIter.Next(); {
		mapKey := mapIter.Key()
		mapValue := mapIter.Value()

		key := keyer(mapKey)
		// Do a small hack to get the pointer to the map value without using
		// UnsafeAddr, as that would panic.
		ptr := defract.InterfacePtr(mapValue.Interface())

		if err := tx.putValue(key, valueType, valueKind, ptr, rec+1); err != nil {
			return errors.Wrapf(err, "map %s key %q", typ, mapValue)
		}
	}

	return nil
}
