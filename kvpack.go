package kvpack

import (
	"fmt"
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

	return tx.putValue(key, typ, ptr, 0)
}

func (tx *Transaction) putValue(k []byte, typ reflect.Type, ptr unsafe.Pointer, rec int) error {
	if rec > recursionLimit {
		return ErrTooRecursed
	}

	typ, ptr = defract.Indirect(typ, ptr)
	if ptr == nil {
		// Do nothing with a nil pointer.
		return nil
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
		if typ == defract.ByteSlice {
			return tx.putBytes(k, ptr)
		}
		return tx.putSlice(k, typ, ptr, rec+1)

	case reflect.Array:
		panic("TODO: array")

	case reflect.Struct:
		return tx.putStruct(k, typ, ptr, rec+1)

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

		if err := tx.putValue(key, underlying, dataPtr, rec); err != nil {
			return errors.Wrapf(err, "index %d", i)
		}

		// Increment the data pointer for the next loop.
		dataPtr = unsafe.Add(dataPtr, valueSize)
	}

	return nil
}

func (tx *Transaction) putStruct(k []byte, typ reflect.Type, ptr unsafe.Pointer, rec int) error {
	if rec > recursionLimit {
		return ErrTooRecursed
	}

	info := defract.GetStructInfo(typ)

	for _, field := range info.Fields {
		ptr := unsafe.Add(ptr, field.Offset)
		// Skip zero-values.
		if defract.IsZero(ptr, field.Size) {
			continue
		}

		key := tx.kb.append(k, field.Name)

		if err := tx.putValue(key, field.Type, ptr, rec+1); err != nil {
			return errors.Wrapf(err, "struct %s field %s", typ, field.Name)
		}
	}

	return nil
}

func (tx *Transaction) putMap(k []byte, typ reflect.Type, ptr unsafe.Pointer, rec int) error {
	if rec > recursionLimit {
		return ErrTooRecursed
	}

	// keyer gets the reflect.Value's underlying pointer and returns the key.
	var keyer func(reflect.Value) (key []byte)

	keyType := typ.Key()
	valueType := typ.Elem()

	switch kind := keyType.Kind(); kind {
	case reflect.Float32, reflect.Float64:
		keyer = func(v reflect.Value) []byte {
			b := defract.Uint64LE(math.Float64bits(v.Float()))
			k := tx.kb.append(k, b.Bytes)
			b.Return()

			return k
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		keyer = func(v reflect.Value) []byte {
			b := defract.Int64LE(v.Int())
			k := tx.kb.append(k, b.Bytes)
			b.Return()

			return k
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		keyer = func(v reflect.Value) []byte {
			b := defract.Uint64LE(v.Uint())
			k := tx.kb.append(k, b.Bytes)
			b.Return()

			return k
		}
	case reflect.String:
		keyer = func(v reflect.Value) []byte {
			// Like putBytes, this is safe as long as we don't access the
			// capacity.
			strPtr := *(*[]byte)(unsafe.Pointer(v.UnsafeAddr()))
			return tx.kb.append(k, strPtr)
		}
	default:
		return fmt.Errorf("unknown type %s", keyType)
	}

	// There's really no choice but to handle maps the slow way.
	mapIter := reflect.NewAt(typ, ptr).Elem().MapRange()
	for mapIter.Next() {
		key := keyer(mapIter.Key())

		if err := tx.putMapValue(key, valueType, mapIter.Value(), rec+1); err != nil {
			return errors.Wrapf(err, "map %s key %q", typ, mapIter.Key())
		}
	}

	return nil
}

func (tx *Transaction) putMapValue(k []byte, typ reflect.Type, val reflect.Value, rec int) error {
	if rec > recursionLimit {
		return ErrTooRecursed
	}
	panic("TODO")
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

	panic("unimplemented")
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
