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
		manualIter   driver.ManualIterator
		manualIterOk bool
	}
}

// NewTransaction creates a new transaction from an existing one. This is useful
// for working around Database's limited APIs.
func NewTransaction(tx driver.Transaction, namespace string) *Transaction {
	ns := keyPool.Get().([]byte)
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
		// We can treat a string like a []byte as long as we don't touch the
		// capacity. It is likely a better idea to use reflect.StringHeader,
		// though.
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
	bytes := *(*[]byte)(ptr)
	if len(bytes) == 0 {
		// Empty string, so put nothing. Accessing [0] will cause out of
		// bounds.
		return tx.tx.Put(k, nil)
	}

	return tx.tx.Put(k, unsafe.Slice(&bytes[0], len(bytes)))
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
	dataPtr, length := defract.SliceInfo(ptr)
	length64 := int64(length)

	// Write the slice length into a separate key so we can preallocate the
	// slice when we obtain it.
	lengthKey, lengthValue := tx.kb.appendExtra(k, []byte("l"), 8)
	defract.WriteInt64LE(lengthValue, length64)

	if err := tx.tx.Put(lengthKey, lengthValue); err != nil {
		return errors.Wrap(err, "failed to write slice len")
	}

	rec++

	for i := int64(0); i < length64; i++ {
		// We can do this without thinking if this will collide with "l" or not,
		// because this is always 8 bytes, even with 0.
		key, extra := tx.kb.appendExtra(k, nil, 8)
		defract.WriteInt64LE(extra, i)
		// Slice key to include the extra slice.
		key = key[:len(key)+8]

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
				ptr = defract.IndirectOnce(ptr)
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

	// keyer gets the reflect.Value's underlying pointer and returns the key.
	var keyer func(reflect.Value) (key []byte)

	keyType := typ.Key()
	valueType := typ.Elem()
	valueKind := valueType.Kind()

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

	// There's really no choice but to handle maps the slow way.
	mapIter := reflect.NewAt(typ, ptr).Elem().MapRange()
	for mapIter.Next() {
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

// manualIterator returns a non-nil ManualIterator if the transaction supports
// it.
func (tx *Transaction) manualIterator() driver.ManualIterator {
	if !tx.lazy.manualIterOk {
		tx.lazy.manualIter, _ = tx.tx.(driver.ManualIterator)
		tx.lazy.manualIterOk = true
	}

	return tx.lazy.manualIter
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
	for i := len(tx.ns) - 1; i < len(key); i++ {
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

	typ := reflect.TypeOf(v)
	kind := typ.Kind()

	if kind != reflect.Ptr {
		return errors.New("given v is not pointer")
	}

	ptr := defract.InterfacePtr(v)
	return tx.getValue(k, typ, kind, ptr, 0)
}

func (tx *Transaction) getValue(
	k []byte, typ reflect.Type, kind reflect.Kind, ptr unsafe.Pointer, rec int) error {

	if rec > recursionLimit {
		return ErrTooRecursed
	}

	if kind == reflect.Ptr {
		typ, ptr = defract.AllocIndirect(typ, ptr)
		if ptr == nil {
			// Do nothing with a nil pointer.
			return nil
		}
	}

	// Comparing Kind is a lot faster.
	switch kind := typ.Kind(); kind {
	// Handle uint and int like variable-length integers. These helper functions
	// pool the backing array, so this should be decently fast.
	case reflect.Uint, reflect.Int:
		return tx.tx.Get(k, func(b []byte) error {
			if !defract.ReadVarInt(b, kind, ptr) {
				return errors.New("(u)varint overflow")
			}
			return nil
		})

	case reflect.Bool:
		// A bool can probably be treated as 1 byte, so we can check the first
		// byte if it's 0 or 1.
		return tx.tx.Get(k, func(b []byte) error {
			*(*bool)(ptr) = b[0] != 0
			return nil
		})

	case
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128,
		reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:

		return tx.tx.Get(k, func(b []byte) error {
			if !defract.ReadNumberLE(b, kind, ptr) {
				return io.ErrUnexpectedEOF
			}
			return nil
		})

	case reflect.String:
		return tx.tx.Get(k, func(b []byte) error {
			*(*string)(ptr) = string(b) // copies
			return nil
		})

	case reflect.Slice:
		if typ != defract.ByteSlice {
			return tx.getSlice(k, typ, ptr, rec+1)
		}

		return tx.tx.Get(k, func(b []byte) error {
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
		})

	case reflect.Array:
		panic("TODO: array")

	case reflect.Struct:
		return tx.putStruct(k, defract.GetStructInfo(typ), ptr, rec+1)

	case reflect.Map:
		return tx.putMap(k, typ, ptr, rec+1)
	}

	return fmt.Errorf("unknown type %s", typ)
}

func (tx *Transaction) getSlice(k []byte, typ reflect.Type, ptr unsafe.Pointer, rec int) error {
	panic("TODO")
}

func (tx *Transaction) getStruct(
	k []byte, info *defract.StructInfo, ptr unsafe.Pointer, rec int) error {

	var length int64
	if err := tx.tx.Get(tx.kb.append(k, []byte("l")), func(b []byte) error {
		var ok bool
		length, ok = defract.ReadInt64LE(b)
		if !ok {
			return io.ErrUnexpectedEOF
		}
		return nil
	}); err != nil {
		return errors.Wrap(err, "failed to read slice len")
	}

	panic("TODO")
}

func (tx *Transaction) getMap(
	k []byte, typ reflect.Type, ptr unsafe.Pointer, rec int) error {

	panic("TODO")
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
// end.
func (kb *keybuf) appendExtra(dst, data []byte, extra int) (newBuf, extraBuf []byte) {
	newBuf, extraBuf = appendKey(&dst, data, extra)

	if &kb.buffer[0] != &newBuf[0] {
		// Slice is grown; update the backing array.
		kb.buffer = newBuf[:len(kb.buffer)]
	}

	return newBuf, extraBuf
}

// appendKey appends into the given buffer the key that's separated by the
// separator. If extra is not 0, then an additional region at the end of the
// buffer is reserved into the extra slice.
func appendKey(buf *[]byte, k []byte, extra int) (newBuf, extraBuf []byte) {
	bufHeader := *buf

	if cap(bufHeader) < len(bufHeader)+len(k)+len(Separator)+extra {
		// Existing key slice is not enough, so allocate a new one.
		key := make([]byte, len(bufHeader)+len(Separator)+len(k)+extra)

		var i int
		i += copy(key[i:], bufHeader)
		i += copy(key[i:], Separator)
		i += copy(key[i:], k)

		*buf = key[:len(bufHeader)]
		return key[:i], key[i:]
	}

	bufHeader = append(bufHeader, Separator...)
	bufHeader = append(bufHeader, k...)
	return bufHeader, bufHeader[len(bufHeader) : len(bufHeader)+extra]
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
