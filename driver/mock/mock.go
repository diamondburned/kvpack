package mock

import (
	"bytes"
	"errors"
	"strings"
	"sync"

	"github.com/diamondburned/kvpack"
	"github.com/diamondburned/kvpack/driver"
)

// Database is thread-safe.
type Database struct {
	m sync.RWMutex
	v map[string]string
}

// NewDatabase makes a new empty database. If a namespace is provided, then
// Expect will use that namespace.
func NewDatabase(expectNamespace string) *kvpack.Database {
	return kvpack.NewDatabase(newDatabase(), expectNamespace)
}

func newDatabase() *Database {
	return &Database{
		v: make(map[string]string),
	}
}

// Begin starts a transaction.
func (db *Database) Begin(ro bool) (driver.Transaction, error) {
	if ro {
		return &Transaction{db: db}, nil
	}

	return &Transaction{
		tmp: make([]kvPair, 0, 128),
		db:  db,
	}, nil
}

// Transaction is not thread-safe.
type Transaction struct {
	tmp []kvPair
	del []kvPair // prefixes
	db  *Database
}

type kvPair [2][]byte

var _ driver.Transaction = (*Transaction)(nil)

func (tx *Transaction) isRO() bool { return tx.tmp == nil }

// Commit saves changes in the transaction to the database. If the transaction
// is read-only, then Commit does nothing.
func (tx *Transaction) Commit() error {
	if tx.isRO() {
		// Already committed.
		return nil
	}

	tx.db.m.Lock()
	defer tx.db.m.Unlock()

	for _, prefix := range tx.del {
		prefixString := string(prefix[0])

		for k := range tx.db.v {
			if strings.HasPrefix(k, prefixString) {
				delete(tx.db.v, k)
			}
		}
	}

	for _, pair := range tx.tmp {
		tx.db.v[string(pair[0])] = string(pair[1])
	}

	// Invalidate.
	tx.tmp = nil
	tx.del = nil

	return nil
}

// Rollback invalidates the transaction and does not commit data to the
// database.
func (tx *Transaction) Rollback() error {
	tx.tmp = nil
	tx.del = nil
	return nil
}

// Get checks both the changes made during the transaction and before it, and
// calls fn() on the gotten value, if any.
func (tx *Transaction) Get(k []byte, fn func([]byte) error) error {
	for _, pair := range tx.tmp {
		if string(pair[0]) == string(k) {
			return fn(pair[1])
		}
	}

	tx.db.m.RLock()
	defer tx.db.m.RUnlock()

	s, ok := tx.db.v[string(k)]
	if ok {
		return fn([]byte(s))
	}

	return nil
}

// Put adds the given key and value pair into the uncommitted map.
func (tx *Transaction) Put(k, v []byte) error {
	if tx.isRO() {
		return errors.New("mock: cannot put in a read-only transaction")
	}
	if tx.tmp == nil {
		return errors.New("transaction closed")
	}

	for i, pair := range tx.tmp {
		if string(pair[0]) == string(k) {
			tx.tmp[i] = kvPair{k, v}
			return nil
		}
	}

	tx.tmp = append(tx.tmp, kvPair{k, v})
	return nil
}

// Iterate iterates over both the uncommitted transaction store and the
// committed database region.
func (tx *Transaction) Iterate(prefix []byte, fn func(k, v []byte) error) error {
	for _, pair := range tx.tmp {
		if bytes.HasPrefix(pair[0], prefix) {
			if err := fn(pair[0], pair[1]); err != nil {
				return err
			}
		}
	}

	prefixString := string(prefix)

	tx.db.m.RLock()
	defer tx.db.m.RUnlock()

	for k := range tx.db.v {
		if strings.HasPrefix(k, prefixString) {
			if err := fn([]byte(k), []byte(tx.db.v[k])); err != nil {
				return err
			}
		}
	}

	return nil
}

// IterateKey iterates over all keys with the given prefix in lexicographic
// order.
func (tx *Transaction) IterateKey(prefix []byte, fn func(k []byte) error) error {
	for _, pair := range tx.tmp {
		if bytes.HasPrefix(pair[0], prefix) {
			if err := fn(pair[0]); err != nil {
				return err
			}
		}
	}

	prefixString := string(prefix)

	tx.db.m.RLock()
	defer tx.db.m.RUnlock()

	for k := range tx.db.v {
		if strings.HasPrefix(k, prefixString) {
			if err := fn([]byte(k)); err != nil {
				return err
			}
		}
	}

	return nil
}

// DeletePrefix registers the given prefix to be deleted once committed.
func (tx *Transaction) DeletePrefix(prefix []byte) error {
	if tx.isRO() {
		return errors.New("mock: cannot delete prefix in a read-only transaction")
	}

	tx.del = append(tx.del, kvPair{prefix, nil})
	return nil
}
