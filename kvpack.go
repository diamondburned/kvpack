package kvpack

import (
	"encoding/binary"
	"fmt"
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

var varintPool = sync.Pool{
	New: func() interface{} { return make([]byte, binary.MaxVarintLen64) },
}

// Varint is a helper function that converts an integer into a byte slice to be
// used as a database key. The given callback must not move the slice outside
// the closure.
func Varint(i int64, f func([]byte) error) error {
	b := varintPool.Get().([]byte)
	defer varintPool.Put(b)

	return f(b[:binary.PutVarint(b, i)])
}

// Uvarint is a helper function that converts an unsigned integer into a byte
// slice to be used as a database key. The given callback must not move the
// slice outside the closure.
func Uvarint(u uint64, f func([]byte) error) error {
	b := varintPool.Get().([]byte)
	defer varintPool.Put(b)

	return f(b[:binary.PutUvarint(b, u)])
}

// keyPool is a pool of 1024-or-larger capacity byte slices.
var keyPool = sync.Pool{
	New: func() interface{} { return make([]byte, 0, 1024) },
}

// Transaction describes a transaction of a database managed by kvpack.
type Transaction struct {
	tx driver.Transaction
	ns []byte
	kb keybuf
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

	return tx.putValue(key, typ, ptr, 0)
}

func (tx *Transaction) putStruct(k []byte, typ reflect.Type, ptr unsafe.Pointer, rec int) error {
	if rec > recursionLimit {
		return ErrTooRecursed
	}

	info := defract.GetStructInfo(typ)

	for _, field := range info.Fields {
		key := tx.kb.append(k, field.Name)
		ptr := unsafe.Add(ptr, field.Offset)

		if err := tx.putValue(key, field.Type, ptr, rec+1); err != nil {
			return errors.Wrapf(err, "struct %s field %s", typ, field.Name)
		}
	}

	return nil
}

func (tx *Transaction) putValue(k []byte, typ reflect.Type, ptr unsafe.Pointer, rec int) error {
	if rec > recursionLimit {
		return ErrTooRecursed
	}

	// Not setting a zero-value field is likely faster overall, since the
	// database is likely slower than our code.
	if defract.IsZero(typ, ptr) {
		return nil
	}

	typ, ptr = defract.Indirect(typ, ptr)
	if ptr == nil {
		// Do nothing with a nil pointer.
		return nil
	}

	switch typ {
	// Handle uint and int like variable-length integers. These helper functions
	// pool the backing array, so this should be decently fast.
	case defract.Uint:
		return Uvarint(uint64(*(*uint)(ptr)), func(b []byte) error {
			return tx.tx.Put(k, b)
		})
	case defract.Int:
		return Varint(int64(*(*int)(ptr)), func(b []byte) error {
			return tx.tx.Put(k, b)
		})

	case defract.Bool:
		// A bool can probably be treated as 1 byte, so we can cast it to that
		// and convert it to a pointer.
		return tx.tx.Put(k, (*[1]byte)(ptr)[:])

	case defract.Byte,
		defract.Float32, defract.Float64,
		defract.Complex64, defract.Complex128,
		defract.Int8, defract.Int16, defract.Int32, defract.Int64,
		defract.Uint8, defract.Uint16, defract.Uint32, defract.Uint64:

		return defract.NumberLE(typ, ptr, func(b []byte) error {
			return tx.tx.Put(k, b)
		})

	case defract.String:
		// We can treat a string like a []byte as long as we don't touch the
		// capacity. It is likely a better idea to use reflect.StringHeader,
		// though.
		fallthrough
	case defract.ByteSlice:
		ptr := *(*[]byte)(ptr)
		if len(ptr) == 0 {
			// Empty string, so put nothing. Accessing [0] will cause out of
			// bounds.
			return tx.tx.Put(k, nil)
		}
		return tx.tx.Put(k, unsafe.Slice(&ptr[0], len(ptr)))
	}

	switch typ.Kind() {
	case reflect.Struct:
		return tx.putStruct(k, typ, ptr, rec+1)
	case reflect.Slice:
		panic("TODO slice")
	case reflect.Map:
		panic("TODO map")
	}

	return fmt.Errorf("unknown type %s", typ)
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
	new := appendKey(&dst, data)

	if &kb.buffer[0] != &new[0] {
		// Slice is grown; update the backing array.
		kb.buffer = new[:len(kb.buffer)]
	}

	return new
}

// appendKey appends into the given buffer the key that's separated by the
// separator.
func appendKey(buf *[]byte, k []byte) []byte {
	bufHeader := *buf

	if cap(bufHeader) < len(bufHeader)+len(k)+len(Separator) {
		// Existing key slice is not enough, so allocate a new one.
		key := make([]byte, len(bufHeader)+len(Separator)+len(k))

		var i int
		i += copy(key[i:], bufHeader)
		i += copy(key[i:], Separator)
		i += copy(key[i:], k)

		*buf = key[:len(bufHeader)]
		return key
	}

	bufHeader = append(bufHeader, Separator...)
	bufHeader = append(bufHeader, k...)
	return bufHeader
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
