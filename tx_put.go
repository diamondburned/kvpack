package kvpack

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	"github.com/diamondburned/kvpack/defract"
	"github.com/pkg/errors"
)

// PutFields puts the given value into the databse ID'd by the given keys
// (plural). This function is similar to GetFields, except it's Put.
func (tx *Transaction) PutFields(fields string, v interface{}) error {
	if tx.ro {
		return ErrReadOnly
	}

	// Ensure that key does not contain the separator.
	if strings.Contains(fields, Separator) {
		return errors.New("key contains illegal null character")
	}

	key := tx.makeFieldsKey(fields)
	return tx.put(key, v)
}

// Put puts the given value into the database ID'd by the given key. If v's type
// is a value or a pointer to a byte slice or a string, then a fast path is
// used, and the values are put into the database as-is.
func (tx *Transaction) Put(k []byte, v interface{}) error {
	if tx.ro {
		return ErrReadOnly
	}

	// Ensure that key does not contain the separator.
	if bytes.Contains(k, []byte(Separator)) {
		return errors.New("key contains illegal null character")
	}

	key := tx.kb.Append(tx.namespace(), []byte(k))
	return tx.put(key, v)
}

func (tx *Transaction) put(k []byte, v interface{}) error {
	switch v := v.(type) {
	case []byte:
		return tx.Tx.Put(k, v)
	case *[]byte:
		return tx.Tx.Put(k, *v)
	case string:
		return tx.Tx.Put(k, []byte(v)) // cannot reference
	case *string:
		return tx.Tx.Put(k, defract.StrToBytes(v))
	}

	typ, ptr := defract.UnderlyingPtr(v)
	if ptr == nil {
		return ErrValueNeedsPtr
	}

	return tx.putValue(k, typ, typ.Kind(), ptr, 0)
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

	v, err := tx.Tx.Get(k)
	if err == nil {
		oldLen, ok := defract.ReadInt64LE(v)
		if ok && oldLen <= length64 {
			goto overridden
		}
	}

	// Override the previous slice entirely.
	if err := tx.Tx.DeletePrefix(k); err != nil {
		return errors.Wrap(err, "failed to clean up old slice")
	}

overridden:
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

	// Verify that the struct schema is the same. If not, wipe everything and
	// rewrite. If it is, then we don't need to scan the database.
	if v, err := tx.Tx.Get(k); err == nil && bytes.Equal(info.RawSchema, v) {
		goto afterWipe
	}

	// Wipe the entire old map.
	if err := tx.Tx.DeletePrefix(k); err != nil {
		return errors.Wrap(err, "failed to override struct marking key")
	}

afterWipe:
	// Indicate that the struct does, in fact, exist, by writing down the known
	// struct schema.
	if err := tx.Tx.Put(k, info.RawSchema); err != nil {
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
