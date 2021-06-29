// Package bboltpack implements the kvpack drivers using Bolt.
package bboltpack

import (
	"bytes"
	"log"
	"os"

	"github.com/diamondburned/kvpack"
	"github.com/diamondburned/kvpack/driver"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
)

// DB implements driver.Database.
type DB struct {
	*bbolt.DB
	namespace []byte
}

// Open opens a new Badger database wrapped inside a driver.Database-compatible
// implementation.
func Open(namespace, path string, mode os.FileMode, opts *bbolt.Options) (*kvpack.Database, error) {
	d, err := bbolt.Open(path, mode, opts)
	if err != nil {
		return nil, err
	}

	db := DB{d, nil}
	kvdb := kvpack.NewDatabase(&db, namespace)
	db.namespace = []byte(kvdb.Namespace())

	if err := d.Update(func(tx *bbolt.Tx) error {
		// Create our own bucket.
		_, err = tx.CreateBucketIfNotExists(db.namespace)
		if err != nil {
			return errors.Wrap(err, "failed to create namespace bucket")
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return kvdb, nil
}

// Close closes the database.
func (db *DB) Close() error {
	return db.DB.Close()
}

// Begin starts a transaction.
func (db *DB) Begin(ro bool) (driver.Transaction, error) {
	tx, err := db.DB.Begin(!ro)
	if err != nil {
		return nil, err
	}

	bucket := tx.Bucket(db.namespace)
	if bucket == nil {
		return nil, errors.New("namespace bucket not found")
	}

	return &Tx{
		Bucket: bucket,
		done:   false,
	}, nil
}

// Tx implements driver.Transaction.
type Tx struct {
	*bbolt.Bucket
	done bool
}

var (
	_ driver.Transaction = (*Tx)(nil)
	// _ driver.Preloader   = (*Tx)(nil)
)

// Commit commits the current transaction. Calling Commit multiple times does
// nothing and will return nil.
func (tx *Tx) Commit() error {
	if tx.done {
		return nil
	}

	tx.done = true
	return tx.Bucket.Tx().Commit()
}

// Rollback discards the current transaction.
func (tx *Tx) Rollback() error {
	if tx.done {
		return nil
	}

	tx.done = true
	return tx.Bucket.Tx().Rollback()
}

// Get gets the value with the given key.
func (tx *Tx) Get(k []byte) ([]byte, error) {
	defer func() {
		if v := recover(); v != nil {
			log.Printf("repanicking... root %v, tx id %v", tx.Bucket.Root(), tx.Bucket.Tx().ID())
			panic(v)
		}
	}()
	v := tx.Bucket.Get(k)
	if v != nil {
		return v, nil
	}
	return nil, driver.ErrKeyNotFound
}

// Put puts the given value into the given key.
func (tx *Tx) Put(k, v []byte) error {
	return tx.Bucket.Put(k, v)
}

// DeletePrefix deletes all keys with the given prefix.
func (tx *Tx) DeletePrefix(prefix []byte) error {
	cursor := tx.Bucket.Cursor()

	for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
		if bytes.HasPrefix(k, prefix) {
			if err := cursor.Delete(); err != nil {
				return errors.Wrapf(err, "failed to delete key %q", k)
			}
		}
	}

	return nil
}

// Iterate iterates over all keys with the given prefix in lexicographic order.
func (tx *Tx) Iterate(prefix []byte, fn func(k, v []byte) error) error {
	cursor := tx.Bucket.Cursor()

	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		if bytes.HasPrefix(k, prefix) {
			if err := fn(k, v); err != nil {
				return err
			}
		}
	}

	return nil
}
