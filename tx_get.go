package kvpack

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"unsafe"

	"github.com/diamondburned/kvpack/defract"
	"github.com/pkg/errors"
)

func (tx *Transaction) Get(k []byte, v interface{}) error {
	return tx.get(tx.kb.Append(tx.namespace(), k), v)
}

// Access is a convenient function around Get that accesses struct or struct
// fields using the period syntax. Each field inside the given fields string is
// delimited by a period, for example, "raining.Cats.Dogs", where "raining" is
// the key.
func (tx *Transaction) Access(fields string, v interface{}) error {
	key := tx.kb.Append(tx.namespace(), []byte(fields))
	// Replace all periods with the right separator.
	for i := len(tx.namespace()); i < len(key); i++ {
		if key[i] == '.' {
			// We know that separator is a single character, which makes this a
			// lot easier. Had it been more than one, this wouldn't work.
			key[i] = Separator[0]
		}
	}

	return tx.get(key, v)
}

func (tx *Transaction) get(k []byte, v interface{}) error {
	switch v := v.(type) {
	case *[]byte:
		return tx.Tx.Get(k, func(theirs []byte) error {
			dst := *v

			if cap(dst) >= len(theirs) {
				*v = append(dst[:0], theirs...)
				return nil
			}

			dst = make([]byte, len(theirs))
			copy(dst, theirs)
			*v = dst
			return nil
		})
	case *string:
		return tx.Tx.Get(k, func(theirs []byte) error {
			*v = string(theirs)
			return nil
		})
	}

	typ, ptr := defract.UnderlyingPtr(v)
	kind := typ.Kind()

	return tx.getValue(k, typ, kind, ptr, 0)
}

func (tx *Transaction) getValue(
	k []byte, typ reflect.Type, kind reflect.Kind, ptr unsafe.Pointer, rec int) error {

	if rec > recursionLimit {
		return ErrTooRecursed
	}

	return tx.Tx.Get(k, func(b []byte) error {
		return tx.getValueBytes(k, b, typ, kind, ptr, rec)
	})
}

func (tx *Transaction) getValueBytes(
	k, b []byte, typ reflect.Type, kind reflect.Kind, ptr unsafe.Pointer, rec int) error {

	if kind == reflect.Ptr {
		typ, ptr = defract.AllocIndirect(typ, ptr)
	}

	// Comparing Kind is a lot faster.
	switch kind := typ.Kind(); kind {
	case reflect.Bool:
		if len(b) == 0 {
			return io.ErrUnexpectedEOF
		}

		// A bool can probably be treated as 1 byte, so we can check the first
		// byte if it's 0 or 1.
		*(*bool)(ptr) = b[0] != 0
		return nil

	case
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:

		if !defract.ReadNumberLE(b, kind, ptr) {
			return io.ErrUnexpectedEOF
		}
		return nil

	case reflect.String:
		defract.CopyString(ptr, b)
		return nil

	case reflect.Slice:
		if typ == defract.ByteSlice {
			dst := (*[]byte)(ptr)
			// If the existing slice has enough capacity, then we can
			// directly copy over.
			if cap(*dst) >= len(b) {
				*dst = (*dst)[:len(b)]
			} else {
				// Else, allocate a new one.
				*dst = make([]byte, len(b))
			}
			copy(*dst, b)
			return nil
		}

		return tx.getSlice(k, b, typ, ptr, rec+1)

	case reflect.Array:
		panic("TODO: array")

	case reflect.Struct:
		return tx.getStruct(k, defract.GetStructInfo(typ), ptr, rec+1)

	case reflect.Map:
		return tx.getMap(k, b, typ, ptr, rec+1)
	}

	return fmt.Errorf("unknown type %s", typ)
}

func (tx *Transaction) getSlice(
	k, lenBytes []byte, typ reflect.Type, ptr unsafe.Pointer, rec int) error {

	if rec > recursionLimit {
		return ErrTooRecursed
	}

	length64, ok := defract.ReadInt64LE(lenBytes)
	if !ok {
		return io.ErrUnexpectedEOF
	}
	if length64 < 0 {
		return fmt.Errorf("length %d (%q) is negative", length64, lenBytes)
	}
	if length64 == 0 {
		return nil
	}

	underlying := typ.Elem()
	valueKind := underlying.Kind()
	valueSize := underlying.Size()

	// Ensure the slice has enough capacity.
	if _, _, cap := defract.SliceInfo(ptr); int64(cap) < length64 {
		// Allocate a new slice with the known size.
		defract.AllocSlice(ptr, int64(typ.Size()), length64)
	}

	dataPtr := defract.SliceSetLen(ptr, length64)

	// Make a new key with the trailing separator so the length doesn't get
	// included.
	prefix := tx.kb.Append(k, nil)

	return tx.Tx.Iterate(prefix, func(k, v []byte) error {
		// Trim the key, split the delimiters and parse the index.
		keyTail := bytes.TrimPrefix(k, prefix)
		ixBytes := bytes.SplitN(keyTail, []byte(Separator), 2)[0]

		i, err := strconv.ParseInt(defract.BytesToStr(ixBytes), 10, 64)
		if err != nil {
			return errors.Wrap(err, "ix failed")
		}
		if i < 0 || i >= length64 {
			return errors.New("ix overflow")
		}

		elemPtr := unsafe.Add(dataPtr, int64(valueSize)*i)
		return tx.getValueBytes(k, v, underlying, valueKind, elemPtr, rec+1)
	})
}

func (tx *Transaction) getStruct(
	k []byte, info *defract.StructInfo, ptr unsafe.Pointer, rec int) error {

	if rec > recursionLimit {
		return ErrTooRecursed
	}

	for _, field := range info.Fields {
		ptr := unsafe.Add(ptr, field.Offset)
		key := tx.kb.Append(k, field.Name)

		if err := tx.getValue(key, field.Type, field.Kind, ptr, rec+1); err != nil {
			return errors.Wrapf(err, "struct %s field %s", info.Type, field.Name)
		}
	}

	return nil
}

func (tx *Transaction) getMap(
	k, lenBytes []byte, typ reflect.Type, ptr unsafe.Pointer, rec int) error {

	length64, ok := defract.ReadInt64LE(lenBytes)
	if !ok {
		return io.ErrUnexpectedEOF
	}

	keyType := typ.Key()
	valueType := typ.Elem()
	valueKind := valueType.Kind()

	// Allocate a new temporary value to be written into and copied from.
	tmpKey := reflect.New(keyType)
	tmpKeyPtr := unsafe.Pointer(tmpKey.Pointer())

	// Dereference the value for reading.
	tmpKey = tmpKey.Elem()

	// keyer gets the reflect.Value's underlying pointer and returns the key.
	// The callback must set the value into tmpValue or tmpPtr.
	var keyer func([]byte) error

	switch kind := keyType.Kind(); kind {
	case reflect.Float32, reflect.Float64:
		keyer = func(b []byte) error {
			f, err := strconv.ParseFloat(defract.BytesToStr(b), 64)
			if err != nil {
				return err
			}
			tmpKey.SetFloat(f)
			return nil
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var bitSize int
		switch kind {
		case reflect.Int, reflect.Int64:
			bitSize = 64
		case reflect.Int32:
			bitSize = 32
		case reflect.Int16:
			bitSize = 16
		case reflect.Int8:
			bitSize = 8
		}
		keyer = func(b []byte) error {
			i, err := strconv.ParseInt(defract.BytesToStr(b), 10, bitSize)
			if err != nil {
				return err
			}
			tmpKey.SetInt(i)
			return nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		var bitSize int
		switch kind {
		case reflect.Uint, reflect.Uint64:
			bitSize = 64
		case reflect.Uint32:
			bitSize = 32
		case reflect.Uint16:
			bitSize = 16
		case reflect.Uint8:
			bitSize = 8
		}
		keyer = func(b []byte) error {
			u, err := strconv.ParseUint(defract.BytesToStr(b), 10, bitSize)
			if err != nil {
				return err
			}
			tmpKey.SetUint(u)
			return nil
		}
	case reflect.String:
		keyer = func(b []byte) error {
			// Always allocate a new string here, since strings use a reference
			// backing array too.
			*(*string)(tmpKeyPtr) = *(*string)(unsafe.Pointer(&b))
			return nil
		}
	default:
		return fmt.Errorf("unsupported key type %s", keyType)
	}

	var mapValue reflect.Value

	if mapPtr := (*unsafe.Pointer)(ptr); *mapPtr == nil {
		// Current map is nil. Allocate a new map with the exact length. I
		// probably won't even bother to reuse the old map.
		mapValue = reflect.MakeMapWithSize(typ, int(length64))
		// Set this new map in.
		*mapPtr = unsafe.Pointer(mapValue.Pointer())
	} else {
		// Else, reuse the existing map.
		mapValue = reflect.NewAt(typ, *mapPtr).Elem()
	}

	dbPrefix := tx.kb.Append(k, nil)

	return tx.Tx.Iterate(dbPrefix, func(k, v []byte) error {
		mapKey := k[len(dbPrefix):]
		if err := keyer(mapKey); err != nil {
			return errors.Wrapf(err, "key error at key %q", mapKey)
		}

		// Values may be pointers (e.g. slices), so allocate a new value for
		// each.
		// Allocate a temporary value for the map value as well.
		tmpValue := reflect.New(valueType)
		tmpValuePtr := unsafe.Pointer(tmpValue.Pointer())

		if err := tx.getValueBytes(k, v, valueType, valueKind, tmpValuePtr, rec+1); err != nil {
			return errors.Wrapf(err, "value error at key %q", mapKey)
		}

		mapValue.SetMapIndex(tmpKey, tmpValue.Elem())
		return nil
	})
}
