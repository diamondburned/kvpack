// Package badgerpack implements the kvpack drivers using BadgerDB.
package badgerpack

import (
	"bytes"

	"github.com/dgraph-io/badger/v3"
	"github.com/diamondburned/kvpack"
	"github.com/diamondburned/kvpack/defract"
	"github.com/diamondburned/kvpack/driver"
	"github.com/pkg/errors"
)

// DB implements driver.Database.
type DB struct {
	badger.DB
}

// Open opens a new Badger database wrapped inside a driver.Database-compatible
// implementation.
func Open(namespace string, opts badger.Options) (*kvpack.Database, error) {
	d, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return kvpack.NewDatabase(&DB{*d}, namespace), nil
}

// Begin starts a transaction.
func (db *DB) Begin(ro bool) (driver.Transaction, error) {
	txn := db.NewTransaction(!ro)
	return &Txn{
		Txn:       txn,
		preloaded: nil,
	}, nil
}

// Txn implements driver.Transaction.
type Txn struct {
	*badger.Txn
	preloaded map[string][]byte
}

var (
	_ driver.Transaction = (*Txn)(nil)
	_ driver.Preloader   = (*Txn)(nil)
)

// Commit commits the current transaction.
func (txn *Txn) Commit() error {
	return txn.Txn.Commit()
}

// Rollback discards the current transaction.
func (txn *Txn) Rollback() error {
	txn.Txn.Discard()
	txn.preloaded = nil
	return nil
}

// Preload preloads the given prefix.
func (txn *Txn) Preload(prefix []byte) {
	if txn.preloaded == nil {
		txn.preloaded = make(map[string][]byte, 1)
	}

	iter := txn.Txn.NewIterator(badger.IteratorOptions{
		Prefix:         prefix,
		PrefetchSize:   10,
		PrefetchValues: true,
	})
	defer iter.Close()

	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()

		v, err := item.ValueCopy(nil)
		if err == nil {
			txn.preloaded[string(item.Key())] = v
		}
	}
}

// Unload unloads the given prefix.
func (txn *Txn) Unload(prefix []byte) {
	for k := range txn.preloaded {
		if bytes.HasPrefix(defract.StrToBytes(&k), prefix) {
			delete(txn.preloaded, string(prefix))
		}
	}
}

// Get gets the value with the given key.
func (txn *Txn) Get(k []byte, fn func([]byte) error) error {
	if v, ok := txn.preloaded[defract.BytesToStr(k)]; ok {
		return fn(v)
	}

	v, err := txn.Txn.Get(k)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}

		return err
	}

	return v.Value(fn)
}

// Put puts the given value into the given key.
func (txn *Txn) Put(k, v []byte) error {
	return txn.Txn.Set(k, v)
}

// DeletePrefix deletes all keys with the given prefix.
func (txn *Txn) DeletePrefix(prefix []byte) error {
	// Nested structures always have the marker put down, so we can preemptively
	// check that.
	if _, err := txn.Txn.Get(prefix); errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	}

	iter := txn.Txn.NewIterator(badger.IteratorOptions{
		Prefix:         prefix,
		PrefetchValues: false,
	})
	defer iter.Close()

	for iter.Rewind(); iter.Valid(); iter.Next() {
		// We have to copy the key here, because Delete will retain the key
		// buffer, while the iterator will change the key buffer.
		key := iter.Item().KeyCopy(nil)

		if err := txn.Txn.Delete(key); err != nil {
			return errors.Wrapf(err, "failed to delete key %q", key)
		}
	}

	return nil
}

// Iterate iterates over all keys with the given prefix in lexicographic order.
func (txn *Txn) Iterate(prefix []byte, fn func(k, v []byte) error) error {
	iter := txn.Txn.NewIterator(badger.IteratorOptions{
		Prefix:         prefix,
		PrefetchSize:   10,
		PrefetchValues: true,
	})
	defer iter.Close()

	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()

		if err := item.Value(func(v []byte) error {
			return fn(item.Key(), v)
		}); err != nil {
			return err
		}
	}

	return nil
}

// IterateKey iterates over all keys with the given prefix in lexicographic
// order.
func (txn *Txn) IterateKey(prefix []byte, fn func(k []byte) error) error {
	iter := txn.Txn.NewIterator(badger.IteratorOptions{Prefix: prefix})
	defer iter.Close()

	for iter.Rewind(); iter.Valid(); iter.Next() {
		if err := fn(iter.Item().Key()); err != nil {
			return err
		}
	}

	return nil
}
