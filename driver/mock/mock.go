package mock

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/diamondburned/kvpack"
	"github.com/diamondburned/kvpack/driver"
)


// Database is thread-safe.
type Database struct {
	m sync.RWMutex
	n string
	v map[string]string
}

// NewDatabase makes a new empty database. If a namespace is provided, then
// Expect will use that namespace.
func NewDatabase(expectNamespace string) *kvpack.Database {
	return kvpack.NewDatabase(&Database{
		n: expectNamespace,
		v: make(map[string]string),
	}, expectNamespace)
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

// Expect calls the Expect method of the mock database. If the kvpack database
// doesn't contain the mock database, then it panics.
func Expect(db *kvpack.Database, t *testing.T, key string, o map[string]string) {
	mockDB := db.Database.(*Database)
	mockDB.Expect(t, key, o)
}

// Expect verifies that the database contains the given data in the o map.
func (db *Database) Expect(t *testing.T, key string, o map[string]string) {
	if db.n == "" {
		t.Error("expectNamespace not given")
		return
	}

	makeFullKey := func(k string) string {
		if k != "" {
			k = kvpack.Separator + strings.ReplaceAll(k, ".", kvpack.Separator)
		}

		return string(kvpack.Namespace + kvpack.Separator + db.n + kvpack.Separator + key + k)
	}

	// Copy all the keys so we can keep track of which one is found.
	keys := make(map[string]struct{}, len(db.v))
	for k := range db.v {
		keys[k] = struct{}{}
	}

	for k, v := range o {
		fullKey := makeFullKey(k)

		got, ok := db.v[fullKey]
		if !ok {
			t.Errorf("missing key %q value %q", fullKey, v)
			continue
		}

		if string(got) != v {
			t.Errorf("key %q value expected %q, got %q", fullKey, v, got)
			continue
		}

		delete(keys, fullKey)
		delete(o, k)
	}

	for k := range keys {
		t.Errorf("excess key %q value %q", k, db.v[k])
	}
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
	prefixString := string(prefix)

	for _, pair := range tx.tmp {
		if bytes.HasPrefix(pair[0], prefix) {
			if err := fn(pair[0], pair[1]); err != nil {
				return err
			}
		}
	}

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

// DeletePrefix registers the given prefix to be deleted once committed.
func (tx *Transaction) DeletePrefix(prefix []byte) error {
	if tx.isRO() {
		return errors.New("mock: cannot delete prefix in a read-only transaction")
	}

	tx.del = append(tx.del, kvPair{prefix, nil})
	return nil
}
