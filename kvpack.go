package kvpack

import (
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
	Tx driver.Transaction

	kb key.Arena
	ns int
	ro bool
}

// NewTransaction creates a new transaction from an existing one. This is useful
// for working around Database's limited APIs.
func NewTransaction(tx driver.Transaction, namespace string, ro bool) *Transaction {
	kb := key.TakeArena(Separator)
	kb.Buffer = append(kb.Buffer, namespace...)

	return &Transaction{
		Tx: tx,
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

// namespace returns the namespace from the shared buffer.
func (tx *Transaction) namespace() []byte {
	return tx.kb.Buffer[:tx.ns]
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

// Namespace returns the database's namespace, which is the prefix that is
// always prepended into keys.
func (db *Database) Namespace() string {
	return db.namespace
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
