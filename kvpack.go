package kvpack

import (
	"io"
	"strings"

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

// ErrValueNeedsPtr is returned if the given value is not a pointer. This is
// required to handle pointers around in a sane way internally, so it is
// required of both Get and Put.
var ErrValueNeedsPtr = errors.New("given value must be a non-nil pointer")

// Transaction describes a transaction of a database managed by kvpack. A
// transaction must not be shared across goroutines, as it is not concurrently
// safe. To work around this, create multiple transactions.
type Transaction struct {
	Tx driver.Transaction

	lazy struct {
		preloader   driver.Preloader
		preloaderOK bool

		keyIterator   driver.KeyIterator
		keyIteratorOK bool
	}

	kb key.Arena
	ns int
	ro bool
}

// NewTransaction creates a new transaction from an existing one. This is useful
// for working around Database's limited APIs. Users shouldn't call this
// directly, as this function is primarily used for drivers.
func NewTransaction(tx driver.Transaction, fullNamespace []byte, ro bool) *Transaction {
	kb := key.TakeArena(Separator)
	kb.Buffer = append(kb.Buffer, fullNamespace...)

	return &Transaction{
		Tx: tx,
		ns: len(fullNamespace),
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

	if err := tx.Tx.Commit(); err != nil {
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
	err := tx.Tx.Rollback()
	tx.kb.Put()
	return err
}

// Delete deletes the value with the given key.
func (tx *Transaction) Delete(k []byte) error {
	if tx.ro {
		return ErrReadOnly
	}

	key := tx.kb.Append(tx.namespace(), k)
	return tx.Tx.DeletePrefix(key)
}

// Delete deletes the value with the given dot-syntax key.
func (tx *Transaction) DeleteFields(fields string) error {
	if tx.ro {
		return ErrReadOnly
	}

	key := tx.makeFieldsKey(fields)
	return tx.Tx.DeletePrefix(key)
}

func (tx *Transaction) makeFieldsKey(fields string) []byte {
	// Special case: if fields is empty, then use the current root namespace.
	if fields == "" {
		return tx.namespace()
	}

	key := tx.kb.Append(tx.namespace(), []byte(fields))
	// Replace all periods with the right separator.
	for i := len(tx.namespace()); i < len(key); i++ {
		if key[i] == '.' {
			// We know that separator is a single character, which makes this a
			// lot easier. Had it been more than one, this wouldn't work.
			key[i] = Separator[0]
		}
	}

	return key
}

// namespace returns the namespace from the shared buffer.
func (tx *Transaction) namespace() []byte {
	return tx.kb.Buffer[:tx.ns]
}

// Database describes a database that's managed by kvpack. A database is safe to
// use concurrently.
type Database struct {
	driver.Database
	namespace []byte
}

// NewDatabase creates a new database from an existing database instance. The
// default namespace is the root namespace; most users should follow this call
// with .WithNamespace().
func NewDatabase(db driver.Database) *Database {
	return &Database{
		Database:  db,
		namespace: []byte(Namespace), // root namespace
	}
}

// Close closes the database if it implements io.Closer.
func (db *Database) Close() error {
	closer, ok := db.Database.(io.Closer)
	if ok {
		return closer.Close()
	}
	return nil
}

// Descend returns a new database that has been moved into the children
// namespaces of the previous namespace. For example, if "app-name" is the
// string passed into WithNamespace, then calling Descend("users") will give
// "app-name.users" in dot-syntax.
func (db *Database) Descend(namespaces ...string) *Database {
	cpy := *db
	cpy.namespace = append(cpy.namespace, Separator...)
	cpy.namespace = append(cpy.namespace, strings.Join(namespaces, Separator)...)
	return &cpy
}

// WithNamespace creates a new database instance from the existing one with a
// different namespace. If multiple namespaces are given, then it is treated as
// nested field keys. Because of this, the meaning of how nested the fields are
// is entirely up to the user.
func (db *Database) WithNamespace(namespaces ...string) *Database {
	cpy := *db

	if len(namespaces) > 0 {
		cpy.namespace = []byte(Namespace + Separator + strings.Join(namespaces, Separator))
	} else {
		cpy.namespace = []byte(Namespace)
	}

	return &cpy
}

// Namespace returns the database's namespace, which is the prefix that is
// always prepended into keys. The returned namespace string is raw, meaning it
// is not dot-syntax.
func (db *Database) Namespace() string {
	return string(db.namespace)
}

// Begin starts a transaction.
func (db *Database) Begin(readOnly bool) (*Transaction, error) {
	tx, err := db.Database.Begin(db.namespace, readOnly)
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
		return errors.Wrap(err, "failed to start transaction")
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
		return errors.Wrap(err, "failed to start transaction")
	}

	if err := f(tx); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// GetFields gets the given dot-syntax key and unmarshals its value into the
// given pointer in a single read-only transaction. For more information, see
// Transaction's GetFields.
func (db *Database) GetFields(fields string, v interface{}) error {
	tx, err := db.Begin(true)
	if err != nil {
		return errors.Wrap(err, "failed to start transaction")
	}
	defer tx.Rollback()

	if err := tx.GetFields(fields, v); err != nil {
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

// Each iterates over the dot-syntax fields key from the database in a single
// transaction. Refer to Transaction's Each for more documentation.
func (db *Database) Each(fields string, v interface{}, eachFn func(k []byte) (done bool)) error {
	tx, err := db.Begin(true)
	if err != nil {
		return errors.Wrap(err, "failed to start transaction")
	}
	defer tx.Rollback()

	if err := tx.Each(fields, v, eachFn); err != nil {
		return errors.Wrap(err, "failed to get")
	}

	return nil
}

// PutFields puts the given value into the database with the given dot-syntax
// key in a single transaction.
func (db *Database) PutFields(fields string, v interface{}) error {
	tx, err := db.Begin(false)
	if err != nil {
		return errors.Wrap(err, "failed to start transaction")
	}
	defer tx.Rollback()

	if err := tx.PutFields(fields, v); err != nil {
		return errors.Wrap(err, "failed to put")
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit")
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

// Delete deletes the given prefix from the database in a single transaction.
func (db *Database) Delete(prefix []byte) error {
	tx, err := db.Begin(false)
	if err != nil {
		return errors.Wrap(err, "failed to start transaction")
	}
	defer tx.Rollback()

	if err := tx.Delete(prefix); err != nil {
		return errors.Wrap(err, "failed to put")
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit")
	}

	return nil
}

// Delete deletes the given dot-syntax prefix from the database in a single
// transaction.
func (db *Database) DeleteFields(fields string) error {
	tx, err := db.Begin(false)
	if err != nil {
		return errors.Wrap(err, "failed to start transaction")
	}
	defer tx.Rollback()

	if err := tx.DeleteFields(fields); err != nil {
		return errors.Wrap(err, "failed to put")
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit")
	}

	return nil
}
