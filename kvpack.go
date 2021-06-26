package kvpack

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"strconv"
	"unsafe"

	"github.com/diamondburned/kvpack/defract"
	"github.com/diamondburned/kvpack/driver"
	"github.com/diamondburned/kvpack/internal/key"
	"github.com/pkg/errors"
)

const (
	// Namespace is the prefix of all keys managed by kvpack.
	Namespace = "__kvpack"
	// Separator is the delimiter of all key fields that are inserted by kvpack.
	Separator = "\x00"
)

// recursionLimit defines the maximum recursive limit for unmarshaling.
const recursionLimit = 1024

// ErrTooRecursed is returned when kvpack functions are being recursed too
// deeply. When this happens, an error occurs to prevent running out of memory.
var ErrTooRecursed = errors.New("kvpack recursed too deep (> 1024)")

// ErrReadOnly is returned if the transaction is read-only but a write action is
// being performed.
var ErrReadOnly = errors.New("transaction is read-only")

// Transaction describes a transaction of a database managed by kvpack.
type Transaction struct {
	tx driver.Transaction
	ns int
	kb key.Arena
	ro bool
}

// NewTransaction creates a new transaction from an existing one. This is useful
// for working around Database's limited APIs.
func NewTransaction(tx driver.Transaction, namespace string, ro bool) *Transaction {
	kb := key.TakeArena(Separator)
	kb.Buffer = append(kb.Buffer, namespace...)

	return &Transaction{
		tx: tx,
		ns: len(namespace),
		kb: kb,
		ro: ro,
	}
}

// Commit commits the transaction.
func (tx *Transaction) Commit() error {
	if tx.ro {
		tx.Rollback()
		return ErrReadOnly
	}

	if err := tx.tx.Commit(); err != nil {
		tx.Rollback()
		return errors.Wrap(err, "failed to rollback")
	}

	// Always rollback to ensure we properly repool resources as well as
	// cleaning up the resources.
	return tx.Rollback()
}

// Rollback rolls back the transaction. Use of a transaction after rolling back
// will cause a panic.
func (tx *Transaction) Rollback() error {
	err := tx.tx.Rollback()
	tx.kb.Put()
	return err
}

// Delete deletes the value with the given key.
func (tx *Transaction) Delete(k []byte) error {
	if tx.ro {
		return ErrReadOnly
	}

	return tx.Put(k, nil)
}

// namespace returns the namespace from the shared buffer.
func (tx *Transaction) namespace() []byte {
	return tx.kb.Buffer[:tx.ns]
}

// Put puts the given value into the database ID'd by the given key. If v's type
// is a value or a pointer to a byte slice or a string, then a fast path is
// used, and the values are put into the database as-is.
func (tx *Transaction) Put(k []byte, v interface{}) error {
	if tx.ro {
		return ErrReadOnly
	}

	key := tx.kb.Append(tx.namespace(), k)
	if err := tx.tx.DeletePrefix(key); err != nil {
		return errors.Wrap(err, "failed to override key")
	}

	switch v := v.(type) {
	case []byte:
		return tx.tx.Put(key, v)
	case *[]byte:
		return tx.tx.Put(key, *v)
	case string:
		ptr := &(*(*[]byte)(unsafe.Pointer(&v)))[0]
		return tx.tx.Put(key, unsafe.Slice(ptr, len(v)))
	case *string:
		ptr := &(*(*[]byte)(unsafe.Pointer(v)))[0]
		return tx.tx.Put(key, unsafe.Slice(ptr, len(*v)))
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
		return tx.tx.Put(k, defract.UintLE((*uint)(ptr)))
	case reflect.Int:
		return tx.tx.Put(k, defract.IntLE((*int)(ptr)))

	case reflect.Bool:
		// A bool can probably be treated as 1 byte, so we can cast it to that
		// and convert it to a pointer.
		return tx.tx.Put(k, (*[1]byte)(ptr)[:])

	case
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128,
		reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:

		return tx.tx.Put(k, defract.NumberLE(kind, ptr))

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
		return tx.tx.Put(k, nil)
	}

	return tx.tx.Put(k, unsafe.Slice((*byte)(backing), len))
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

	if err := tx.tx.Put(mapKey, lengthValue); err != nil {
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
	if err := tx.tx.Put(k, nil); err != nil {
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
	if err := tx.tx.Put(k, defract.IntLE(&mapLen)); err != nil {
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
		return tx.tx.Get(k, func(theirs []byte) error {
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
		return tx.tx.Get(k, func(theirs []byte) error {
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

	return tx.tx.Get(k, func(b []byte) error {
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

	return tx.tx.Iterate(prefix, func(k, v []byte) error {
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

	return tx.tx.Iterate(dbPrefix, func(k, v []byte) error {
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

// Database describes a database that's managed by kvpack. A database is safe to
// use concurrently.
type Database struct {
	driver.Database
	namespace string
}

// NewDatabase creates a new database from an existing database instance. The
// given namespace will be prepended into the keys of all transactions. This is
// useful for separating database instances.
func NewDatabase(db driver.Database, namespace string) *Database {
	return &Database{
		Database:  db,
		namespace: Namespace + Separator + namespace,
	}
}

// Begin starts a transaction.
func (db *Database) Begin(readOnly bool) (*Transaction, error) {
	tx, err := db.Database.Begin(readOnly)
	if err != nil {
		return nil, err
	}

	return NewTransaction(tx, db.namespace, readOnly), nil
}

// View opens a read-only transaction and runs the given function with that
// opened transaction, then cleans it up.
func (db *Database) View(f func(*Transaction) error) error {
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}

	if err := f(tx); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Rollback()
}

// Update opens a read-write transaction and runs the given function with that
// opened transaction, then commits the transaction.
func (db *Database) Update(f func(*Transaction) error) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}

	if err := f(tx); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// Access gets the given dot-syntax key and unmarshals its value into the given
// pointer in a single read-only transaction. For more information, see
// Transaction's Access.
func (db *Database) Access(k string, v interface{}) error {
	tx, err := db.Begin(true)
	if err != nil {
		return errors.Wrap(err, "failed to start transaction")
	}
	defer tx.Rollback()

	if err := tx.Access(k, v); err != nil {
		return errors.Wrap(err, "failed to get")
	}

	return nil
}

// Get gets the given key and unmarshals its value into the given pointer in a
// single read-only transaction.
func (db *Database) Get(k []byte, v interface{}) error {
	tx, err := db.Begin(true)
	if err != nil {
		return errors.Wrap(err, "failed to start transaction")
	}
	defer tx.Rollback()

	if err := tx.Get(k, v); err != nil {
		return errors.Wrap(err, "failed to get")
	}

	return nil
}

// Put puts the given value into the database with the key in a single
// transaction.
func (db *Database) Put(k []byte, v interface{}) error {
	tx, err := db.Begin(false)
	if err != nil {
		return errors.Wrap(err, "failed to start transaction")
	}
	defer tx.Rollback()

	if err := tx.Put(k, v); err != nil {
		return errors.Wrap(err, "failed to put")
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit")
	}

	return nil
}
