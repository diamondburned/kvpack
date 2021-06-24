package kvpack

import (
	"fmt"
	"io"
	"math"
	"reflect"
	"sync"
	"unsafe"

	"github.com/diamondburned/kvpack/defract"
	"github.com/diamondburned/kvpack/driver"
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

// keyPool is a pool of 1024-or-larger capacity byte slices.
var keyPool = sync.Pool{
	New: func() interface{} { return make([]byte, 0, 1024) },
}

// Transaction describes a transaction of a database managed by kvpack.
type Transaction struct {
	tx driver.Transaction
	ns []byte
	kb keybuf

	// lazy contains fields lazily filled.
	lazy struct {
		orderedIter   driver.OrderedIterator
		orderedIterOk bool
		manualIter    driver.UnorderedIterator
		manualIterOk  bool
	}
}

// NewTransaction creates a new transaction from an existing one. This is useful
// for working around Database's limited APIs.
func NewTransaction(tx driver.Transaction, namespace string) *Transaction {
	// ns := keyPool.Get().([]byte)
	ns := make([]byte, 0, 4096)
	ns = append(ns, namespace...)

	return &Transaction{
		tx: tx,
		ns: ns,
		kb: keybuf{buffer: ns},
	}
}

// Commit commits the transaction.
func (tx *Transaction) Commit() error {
	return tx.tx.Commit()
}

// Rollback rolls back the transaction.
func (tx *Transaction) Rollback() error {
	err := tx.tx.Rollback()

	// Put the current byte slice back to the pool and invalidate them in the
	// transaction. The byte slices inside the pool must have a length of 0.
	keyPool.Put(tx.ns[:0])
	tx.ns = nil

	return err
}

// Delete deletes the value with the given key.
func (tx *Transaction) Delete(k []byte) error {
	return tx.Put(k, nil)
}

// Put puts the given value into the database ID'd by the given key. If v's type
// is a value or a pointer to a byte slice or a string, then a fast path is
// used, and the values are put into the database as-is.
func (tx *Transaction) Put(k []byte, v interface{}) error {
	key := tx.kb.append(tx.ns, k)
	if err := tx.tx.DeletePrefix(k); err != nil {
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
		b := defract.Uvarint(uint64(*(*uint)(ptr)))
		err := tx.tx.Put(k, b.Bytes)
		b.Return()
		return err
	case reflect.Int:
		b := defract.Varint(int64(*(*int)(ptr)))
		err := tx.tx.Put(k, b.Bytes)
		b.Return()
		return err

	case reflect.Bool:
		// A bool can probably be treated as 1 byte, so we can cast it to that
		// and convert it to a pointer.
		return tx.tx.Put(k, (*[1]byte)(ptr)[:])

	case
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128,
		reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:

		b := defract.NumberLE(kind, ptr)
		defer b.Return()

		return tx.tx.Put(k, b.Bytes)

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
	mapKey, lengthValue := tx.kb.appendPadded(k, 8)
	defract.WriteInt64LE(lengthValue, length64)

	if err := tx.tx.Put(mapKey, lengthValue); err != nil {
		return errors.Wrap(err, "failed to write slice len")
	}

	rec++

	for i := int64(0); i < length64; i++ {
		key, extra := tx.kb.appendExtra(k, nil, 8)
		defract.WriteInt64LE(extra, i)

		if err := tx.putValue(key, underlying, valueKind, dataPtr, rec); err != nil {
			return errors.Wrapf(err, "index %d", i)
		}

		// Increment the data pointer for the next loop.
		dataPtr = unsafe.Add(dataPtr, valueSize)
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

		key := tx.kb.append(k, field.Name)

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
	case reflect.Float32, reflect.Float64,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:

		key := func(borrowed defract.BorrowedBytes) []byte {
			k := tx.kb.append(k, borrowed.Bytes)
			borrowed.Return()
			return k
		}

		switch kind {
		case reflect.Float32, reflect.Float64:
			keyer = func(v reflect.Value) []byte {
				return key(defract.Uint64LE(math.Float64bits(v.Float())))
			}
		case reflect.Int:
			keyer = func(v reflect.Value) []byte {
				return key(defract.Varint(v.Int()))
			}
		case reflect.Uint:
			keyer = func(v reflect.Value) []byte {
				return key(defract.Uvarint(v.Uint()))
			}
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			keyer = func(v reflect.Value) []byte {
				return key(defract.Int64LE(v.Int()))
			}
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			keyer = func(v reflect.Value) []byte {
				return key(defract.Uint64LE(v.Uint()))
			}
		}
	case reflect.String:
		keyer = func(v reflect.Value) []byte {
			// Like putBytes, this is safe as long as we don't access the
			// capacity. Not to mention, mapIter.Value() does a copy.
			str := v.Interface().(string)
			return tx.kb.append(k, *(*[]byte)(unsafe.Pointer(&str)))
		}
	default:
		return fmt.Errorf("unsupported key type %s", keyType)
	}

	mapValue := reflect.NewAt(typ, ptr).Elem()

	// Write the length.
	lengthBytes := defract.Int64LE(int64(mapValue.Len()))
	err := tx.tx.Put(k, lengthBytes.Bytes)
	lengthBytes.Return()

	if err != nil {
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
	return tx.get(tx.kb.append(tx.ns, k), v)
}

// Access is a convenient function around Get that accesses struct or struct
// fields using the period syntax. Each field inside the given fields string is
// delimited by a period, for example, "raining.Cats.Dogs", where "raining" is
// the key.
func (tx *Transaction) Access(fields string, v interface{}) error {
	key := tx.kb.append(tx.ns, []byte(fields))
	// Replace all periods with the right separator.
	for i := len(tx.ns); i < len(key); i++ {
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
	// Handle uint and int like variable-length integers. These helper functions
	// pool the backing array, so this should be decently fast.
	case reflect.Uint, reflect.Int:
		if !defract.ReadVarInt(b, kind, ptr) {
			return errors.New("(u)varint overflow")
		}
		return nil

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
		reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:

		if !defract.ReadNumberLE(b, kind, ptr) {
			return io.ErrUnexpectedEOF
		}
		return nil

	case reflect.String:
		*(*string)(ptr) = string(b) // copies
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

// orderedIterator returns a non-nil OrderedIterator if the transaction supports
// it.
func (tx *Transaction) orderedIterator() driver.OrderedIterator {
	if !tx.lazy.orderedIterOk {
		tx.lazy.orderedIter, _ = tx.tx.(driver.OrderedIterator)
		tx.lazy.orderedIterOk = true
	}

	return tx.lazy.orderedIter
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
	if length64 == 0 {
		return nil
	}

	underlying := typ.Elem()
	valueKind := underlying.Kind()
	valueSize := underlying.Size()

	var dataPtr unsafe.Pointer

	// Ensure the slice has enough capacity.
	if _, _, cap := defract.SliceInfo(ptr); int64(cap) < length64 {
		// Allocate a new slice with the known size.
		dataPtr = defract.AllocSlice(ptr, typ, length64)
	} else {
		// Otherwise, reuse the backing array.
		dataPtr = defract.SliceSetLen(ptr, length64)
	}

	rec++

	if iter := tx.orderedIterator(); iter != nil {
		// Make a new key with the trailing separator so the length doesn't get
		// included.
		key := tx.kb.append(k, nil)

		return iter.OrderedIterate(key, func(k, v []byte) error {
			// Skip excess data to avoid going out of bounds.
			if length64 <= 0 {
				return nil
			}

			if err := tx.getValueBytes(k, v, underlying, valueKind, dataPtr, rec); err != nil {
				return err
			}

			dataPtr = unsafe.Add(dataPtr, valueSize)
			length64--
			return nil
		})
	}

	for i := int64(0); i < length64; i++ {
		key, extra := tx.kb.appendExtra(k, nil, 8)
		defract.WriteInt64LE(extra, i)

		if err := tx.tx.Get(key, func(b []byte) error {
			return tx.getValue(key, underlying, valueKind, dataPtr, rec)
		}); err != nil {
			return errors.Wrapf(err, "index %d", i)
		}

		dataPtr = unsafe.Add(dataPtr, valueSize)
	}

	return nil
}

func (tx *Transaction) getStruct(
	k []byte, info *defract.StructInfo, ptr unsafe.Pointer, rec int) error {

	if rec > recursionLimit {
		return ErrTooRecursed
	}

	for _, field := range info.Fields {
		ptr := unsafe.Add(ptr, field.Offset)
		key := tx.kb.append(k, field.Name)

		if err := tx.getValue(key, field.Type, field.Kind, ptr, rec+1); err != nil {
			return errors.Wrapf(err, "struct %s field %s", info.Type, field.Name)
		}
	}

	return nil
}

// manualIterator returns a non-nil UnorderedIterator if the transaction supports
// it.
func (tx *Transaction) manualIterator() driver.UnorderedIterator {
	if !tx.lazy.manualIterOk {
		tx.lazy.manualIter, _ = tx.tx.(driver.UnorderedIterator)
		tx.lazy.manualIterOk = true
	}

	return tx.lazy.manualIter
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
	case reflect.Int, reflect.Uint:
		keyer = func(b []byte) error {
			// Treat sized integers as uint64 always.
			if !defract.ReadVarInt(b, kind, tmpKeyPtr) {
				return io.ErrUnexpectedEOF
			}
			return nil
		}
	case reflect.Float32, reflect.Float64,
		reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:

		keyer = func(b []byte) error {
			// Treat sized integers as uint64 always.
			if !defract.ReadNumberLE(b, reflect.Uint64, tmpKeyPtr) {
				return io.ErrUnexpectedEOF
			}
			return nil
		}
	case reflect.String:
		// TODO: fast, zero-alloc path.
		keyer = func(b []byte) error {
			*(*string)(tmpKeyPtr) = string(b)
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

	dbPrefix := tx.kb.append(k, nil)

	onValue := func(k, v []byte) error {
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
	}

	if iterator := tx.manualIterator(); iterator != nil {
		return iterator.IterateUnordered(dbPrefix, onValue)
	}

	if iterator := tx.orderedIterator(); iterator != nil {
		return iterator.OrderedIterate(dbPrefix, onValue)
	}

	return errors.New("tx does not implement ManualIterator or OrderedIterator")
}

// reusableKey describes a buffer where keys are appended into a single,
// reusable buffer.
type keybuf struct {
	buffer []byte
}

// append appends data into the given dst byte slice. The dst byte slice's
// backing must share with buffer, and the data up to buffer's length must not
// be changed.
func (kb *keybuf) append(dst, data []byte) []byte {
	newBuf, _ := kb.appendExtra(dst, data, 0)
	return newBuf
}

// appendExtra appends data into dst while reserving the extra few bytes at the
// end right after data without any separators. If data is nil, then the
// delimiter is right on the left of the extra slice.
func (kb *keybuf) appendExtra(dst, data []byte, extra int) (newBuf, extraBuf []byte) {
	newBuf, extraBuf = appendKey(&dst, data, extra, 0)

	if &kb.buffer[0] != &newBuf[0] {
		// Slice is grown; update the backing array.
		kb.buffer = newBuf[:len(kb.buffer)]
	}

	return newBuf, extraBuf
}

// appendPadded is similar to appendExtra, except data is never appended, and
// newBuf does not contain the padded bytes..
func (kb *keybuf) appendPadded(dst []byte, padded int) (newBuf, paddedBuf []byte) {
	newBuf, paddedBuf = appendKey(&dst, nil, 0, padded)

	if &kb.buffer[0] != &newBuf[0] {
		// Slice is grown; update the backing array.
		kb.buffer = newBuf[:len(kb.buffer)]
	}

	return newBuf, paddedBuf
}

// appendKey appends into the given buffer the key that's separated by the
// separator. If extra is not 0, then an additional region at the end of the
// buffer is reserved into the extra slice.
func appendKey(buf *[]byte, k []byte, extra, padded int) (newBuf, otherBuf []byte) {
	bufHeader := *buf

	// Don't write k if it's nil and padded > 0.
	hasK := k != nil || padded == 0

	if min := len(bufHeader) + len(k) + len(Separator) + extra + padded; cap(bufHeader) < min {
		// Existing key slice is not enough, so allocate a new one.
		bufHeader = make([]byte, min)

		i := 0
		i += copy(bufHeader[i:], *buf)
		if hasK {
			i += copy(bufHeader[i:], Separator)
			i += copy(bufHeader[i:], k)
		}

		bufHeader = bufHeader[:i]

		// Set the backing array.
		*buf = bufHeader[:len(*buf)]
	} else if hasK {
		bufHeader = append(bufHeader, Separator...)
		bufHeader = append(bufHeader, k...)
	}

	if padded > 0 {
		return bufHeader, bufHeader[len(bufHeader) : len(bufHeader)+padded]
	}

	if extra > 0 {
		end := len(bufHeader) + extra
		return bufHeader[:end], bufHeader[len(bufHeader):end]
	}

	return bufHeader, nil
}

// Database describes a database that's managed by kvpack. A database is safe to
// use concurrently.
type Database struct {
	db driver.Database
	ns string
}

// NewDatabase creates a new database from an existing database instance. The
// given namespace will be prepended into the keys of all transactions. This is
// useful for separating database instances.
func NewDatabase(db driver.Database, namespace string) *Database {
	return &Database{
		db: db,
		ns: Namespace + Separator + namespace,
	}
}

// Begin starts a transaction.
func (db *Database) Begin() (*Transaction, error) {
	tx, err := db.db.Begin()
	if err != nil {
		return nil, err
	}

	return NewTransaction(tx, db.ns), nil
}

// Put puts the given value into the database with the key in a single
// transaction.
func (db *Database) Put(k []byte, v interface{}) error {
	tx, err := db.Begin()
	if err != nil {
		return errors.Wrap(err, "failed to start transaction")
	}
	defer tx.Rollback()

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit")
	}
	return nil
}
