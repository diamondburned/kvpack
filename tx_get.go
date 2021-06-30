package kvpack

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"unsafe"

	"github.com/diamondburned/kvpack/defract"
	"github.com/diamondburned/kvpack/driver"
	"github.com/pkg/errors"
)

// ErrKeyNotFound is returned if the given key is not found. If the key is
// found, but a child key is not found, then this error is not returned.
var ErrKeyNotFound = errors.New("key not found")

func (tx *Transaction) Get(k []byte, v interface{}) error {
	return tx.get(tx.kb.Append(tx.namespace(), k), v)
}

// GetFields is a convenient function around Get that accesses struct or struct
// fields using the period syntax. Each field inside the given fields string is
// delimited by a period, for example, "raining.Cats.Dogs", where "raining" is
// the key.
func (tx *Transaction) GetFields(fields string, v interface{}) error {
	return tx.get(tx.makeFieldsKey(fields), v)
}

func (tx *Transaction) preloader() driver.Preloader {
	if !tx.lazy.preloaderOK {
		tx.lazy.preloader, _ = tx.Tx.(driver.Preloader)
		tx.lazy.preloaderOK = true
	}

	return tx.lazy.preloader
}

func (tx *Transaction) get(k []byte, v interface{}) error {
	switch v := v.(type) {
	case *[]byte:
		b, err := tx.Tx.Get(k)
		if err != nil {
			if errors.Is(err, driver.ErrKeyNotFound) {
				return ErrKeyNotFound
			}
			return err
		}

		if cap(*v) >= len(b) {
			*v = append((*v)[:0], b...)
			return nil
		}

		*v = make([]byte, len(b))
		copy(*v, b)
		return nil

	case *string:
		b, err := tx.Tx.Get(k)
		if err != nil {
			if errors.Is(err, driver.ErrKeyNotFound) {
				return ErrKeyNotFound
			}
			return err
		}

		*v = string(b)
		return nil
	}

	typ, ptr := defract.UnderlyingPtr(v)
	if typ == nil {
		return ErrValueNeedsPtr
	}

	// Optionally preload the whole prefix if possible.
	if preloader := tx.preloader(); preloader != nil {
		preloader.Preload(k)
		defer preloader.Unload(k)
	}

	return tx.getValue(k, nil, typ, typ.Kind(), ptr, 0)
}

func (tx *Transaction) keyIterator() driver.KeyIterator {
	if !tx.lazy.keyIteratorOK {
		tx.lazy.keyIterator, _ = tx.Tx.(driver.KeyIterator)
		tx.lazy.keyIteratorOK = true
	}

	return tx.lazy.keyIterator
}

var internEachBreak = errors.New("break each")

// Each iterates over each instance of the given dot-syntax fields key and calls
// the eachFn callback on each iteration. If fields is empty, then the current
// namespace is iterated over.
//
// The callback must capture the pointer passed in, and it must not move or take
// any of the fields inside the given value until Each exits. The callback must
// also not take the given key away; it has to copy it into a new slice.
//
// The order of iteration is undefined and unguaranteed by kvpack, however, that
// is entirely up to the driver and its order of iteration. Refer to the
// driver's documentation if possible.
//
// Below is an example with error checking omitted for brevity:
//
//    tx.PutFields("app.users.1", User{Name: "don't need this user"})
//    tx.PutFields("app.users.2", User{Name: "don't need this user either"})
//    tx.PutFields("app.users.3", User{Name: "need this user"})
//    tx.PutFields("app.users.4", User{Name: "but not this user"})
//
//    var user User
//    return &user, tx.Each("app.users", &user, func(k []byte) bool {
//        log.Println("found user with ID", string(k))
//        return user.Name == "need this user"
//    })
//
func (tx *Transaction) Each(fields string, v interface{}, eachFn func(k []byte) (done bool)) error {
	var key []byte
	key = tx.makeFieldsKey(fields)
	key = tx.kb.Append(key, nil) // ensure trailing separator

	typ, ptr := defract.UnderlyingPtr(v)
	if typ == nil {
		return ErrValueNeedsPtr
	}

	onEach := func(k, v []byte) error {
		fieldKey := bytes.TrimPrefix(k, key)
		// Ensure that this is the key we expect by verifying that it only has
		// one part.
		if bytes.Contains(fieldKey, []byte(Separator)) {
			return nil
		}

		// Optionally preload the whole prefix if possible.
		if preloader := tx.preloader(); preloader != nil {
			preloader.Preload(k)
			defer preloader.Unload(k)
		}

		// Wipe the underlying value before we write to it.
		defract.ZeroOut(ptr, typ.Size())

		if err := tx.getValue(k, v, typ, typ.Kind(), ptr, 1); err != nil {
			return err
		}

		if eachFn(fieldKey) {
			return internEachBreak
		}

		return nil
	}

	var err error
	if it := tx.keyIterator(); it != nil {
		err = it.IterateKey(key, func(k []byte) error { return onEach(k, nil) })
	} else {
		err = tx.Tx.Iterate(key, onEach)
	}

	if err != nil && errors.Is(err, internEachBreak) {
		return nil
	}

	return err
}

func (tx *Transaction) getValue(
	k, b []byte, typ reflect.Type, kind reflect.Kind, ptr unsafe.Pointer, rec int) error {

	if rec > recursionLimit {
		return ErrTooRecursed
	}

	if b == nil {
		var err error

		b, err = tx.Tx.Get(k)
		if err != nil {
			if errors.Is(err, driver.ErrKeyNotFound) {
				return ErrKeyNotFound
			}
			return err
		}
	}

	if kind == reflect.Ptr {
		typ, ptr = defract.AllocIndirect(typ, ptr)
	}

	// Comparing Kind is a lot faster.
	switch kind := typ.Kind(); kind {
	case reflect.Bool:
		if len(b) == 0 {
			return errors.Wrap(io.ErrUnexpectedEOF, "error parsing bool")
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
			return errors.Wrap(io.ErrUnexpectedEOF, "error parsing "+kind.String())
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
		return errors.Wrapf(io.ErrUnexpectedEOF, "error reading slice length at key %q", k)
	}
	if length64 == 0 {
		return nil
	}
	if length64 < 0 {
		return fmt.Errorf("length %d (%q) is invalid", length64, lenBytes)
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
		ixBytes := bytes.TrimPrefix(k, prefix)
		// Confirm that the key only contains the needed tail. If it's any of
		// the children keys, then we skip it, because we're doing more work
		// than needed.
		if bytes.Contains(ixBytes, []byte(Separator)) {
			return nil
		}

		i, err := strconv.ParseInt(defract.BytesToStr(ixBytes), 10, 64)
		if err != nil {
			return errors.Wrap(err, "ix failed")
		}
		if i < 0 || i >= length64 {
			return errors.New("ix overflow")
		}

		elemPtr := unsafe.Add(dataPtr, int64(valueSize)*i)
		return tx.getValue(k, v, underlying, valueKind, elemPtr, rec+1)
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

		b, err := tx.Tx.Get(key)
		if err != nil {
			if errors.Is(err, driver.ErrKeyNotFound) {
				continue
			}
			return errors.Wrapf(err, "failed to get value for struct field key %q", key)
		}

		if err := tx.getValue(key, b, field.Type, field.Kind, ptr, rec+1); err != nil {
			return errors.Wrapf(err, "struct %s field %s", info.Type, field.Name)
		}
	}

	return nil
}
