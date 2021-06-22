package kvpack

import (
	"encoding/binary"
	"reflect"
	"sync"
	"unsafe"

	"github.com/diamondburned/kvpack/driver"
	"github.com/pkg/errors"
	"golang.org/x/sync/singleflight"
)

const (
	// Namespace is the prefix of all keys managed by kvpack.
	Namespace = "__kvpack"
	// Separator is the delimiter of all key fields that are inserted by kvpack.
	Separator = "\x00"
)

// Varint is a helper function that converts an integer into a byte slice to be
// used as a database key.
func Varint(i int64) []byte {
	b := make([]byte, binary.MaxVarintLen64)
	return b[:binary.PutVarint(b, i)]
}

// Uvarint is a helper function that converts an unsigned integer into a byte
// slice to be used as a database key.
func Uvarint(u uint64) []byte {
	b := make([]byte, binary.MaxVarintLen64)
	return b[:binary.PutUvarint(b, u)]
}

var (
	structCache  sync.Map // unsafe.Pointer -> *structInfo
	structFlight singleflight.Group
)

// Primitive types.
type structInfo struct {
	Fields []structField
}

type structField struct {
	Name   []byte
	Type   reflect.Type
	Offset uintptr
}

// getStructInfo returns the struct type information for the given struct value.
// It assumes that typ is a type of a struct and does not do checks.
func getStructInfo(typ reflect.Type) *structInfo {
	// A reflect.Type is basically an interface containing the type pointer and
	// the value pointer. The type pointer is most likely *rtype, but we don't
	// care about that. Instead, we care about the pointer value of that type,
	// which is the value pointer. This allows us to access the map faster.

	v, ok := structCache.Load(typ)
	if ok {
		return v.(*structInfo)
	}

	var typeName string

	// cpu: Intel(R) Core(TM) i5-8250U CPU @ 1.60GHz
	// BenchmarkReflectType-8   	 9097218	       123.1 ns/op

	pkgPath := typ.PkgPath()
	if pkgPath == "" {
		typeName = typ.Name()
	} else {
		typeName = pkgPath + "." + typ.Name()
	}

	v, _, _ = structFlight.Do(typeName, func() (interface{}, error) {
		var info structInfo
		info.get(typ)

		structCache.Store(typ, &info)
		return &info, nil
	})

	return v.(*structInfo)
}

func (info *structInfo) get(typ reflect.Type) {
	numField := typ.NumField()
	info.Fields = make([]structField, 0, numField)

	for i := 0; i < numField; i++ {
		fieldType := typ.Field(i)
		if !fieldType.IsExported() {
			// We cannot read unexported fields. Skip.
			continue
		}

		info.Fields = append(info.Fields, structField{
			Name:   []byte(fieldType.Name),
			Type:   fieldType.Type,
			Offset: fieldType.Offset,
		})
	}
}

// keyPool is a pool of 1024-or-larger capacity byte slices.
var keyPool = sync.Pool{
	New: func() interface{} { return make([]byte, 0, 1024) },
}

// Transaction describes a transaction of a database managed by kvpack.
type Transaction struct {
	tx driver.Transaction
	ns []byte
	pf []byte // ns + key
}

// NewTransaction creates a new transaction from an existing one. This is useful
// for working around Database's limited APIs.
func NewTransaction(tx driver.Transaction, namespace string) *Transaction {
	ns := keyPool.Get().([]byte)
	ns = append(ns, namespace...)

	return &Transaction{
		tx: tx,
		ns: ns,
		pf: nil,
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
	tx.pf = nil

	return err
}

// Put puts the given value into the database ID'd by the given key.
func (tx *Transaction) Put(k []byte, v interface{}) error {
	tx.pf = appendKey(tx.ns, k)
	// Reuse the backing array for the prefix. This helps the next time
	// appendKey() is called, since capacity is preserved.
	tx.ns = tx.pf[:len(tx.ns)]

	switch v := v.(type) {
	case []byte:
		return tx.tx.Put(tx.pf, v)

	case *[]byte:
		return tx.tx.Put(tx.pf, *v)

	case string:
		ptr := &(*(*[]byte)(unsafe.Pointer(&v)))[0]
		return tx.tx.Put(tx.pf, unsafe.Slice(ptr, len(v)))

	case *string:
		ptr := &(*(*[]byte)(unsafe.Pointer(v)))[0]
		return tx.tx.Put(tx.pf, unsafe.Slice(ptr, len(*v)))
	}

	switch typ, ptr := underlyingPtr(v); typ.Kind() {
	case reflect.Struct:
		info := getStructInfo(typ)

		for _, field := range info.Fields {
			key := appendKey(tx.pf, field.Name)
			// Reuse backing array for next appendKey call.
			tx.pf = key[:len(tx.pf)]

			switch field.Type.Kind() {
			case reflect.String:
				// We can treat a string like a []byte as long as we don't touch
				// the capacity. It is likely a better idea to use
				// reflect.StringHeader, though.
				ptr := *(*[]byte)(unsafe.Add(ptr, field.Offset))
				tx.tx.Put(key, unsafe.Slice(&ptr[0], len(ptr)))
			default:
				panic("unimplemented")
			}
		}
	default:
		panic("unimplemented")
	}

	return nil
}

func (tx *Transaction) Get(k []byte, v interface{}) error {
	panic("TODO")
}

func underlyingPtr(v interface{}) (reflect.Type, unsafe.Pointer) {
	type iface struct {
		_ uintptr
		p unsafe.Pointer
	}

	// Traverse the pointer until it no longer is.
	typ := reflect.TypeOf(v)
	ptr := (*iface)(unsafe.Pointer(&v)).p

	var dereferenced bool
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()

		if dereferenced {
			// Defer the dereference once, because Go will try to use the
			// given pointer value directly if it's already one.
			ptr = *(*unsafe.Pointer)(ptr)
		}
		dereferenced = true
	}

	return typ, ptr
}

// appendKey appends into the given buffer the key that's separated by the
// separator.
func appendKey(buf []byte, k []byte) []byte {
	if cap(buf) < len(buf)+len(k)+len(Separator) {
		// Existing key slice is not enough, so allocate a new one.
		key := make([]byte, len(buf)+len(Separator)+len(k))

		var i int
		i += copy(key[i:], buf)
		i += copy(key[i:], Separator)
		i += copy(key[i:], k)

		return key
	}

	buf = append(buf, Separator...)
	buf = append(buf, k...)
	return buf
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
