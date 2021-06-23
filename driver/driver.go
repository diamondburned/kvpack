// Package driver contains interfaces that describes a generic transactional
// key-value database.
package driver

import "errors"

// Database describes a generic transactional key-value database.
type Database interface {
	Begin() (Transaction, error)
}

// ErrUnsupportedIterator should be returned by transactions from Iterate if the
// database doesn't support iterating using the Iterate API. In that case,
// kvpack will try using the ManualIterator API instead.
var ErrUnsupportedIterator = errors.New("unsupported iterator")

// Transaction describes a regular transaction with a simple read and write API.
type Transaction interface {
	Commit() error
	Rollback() error

	// Get gets the value with the given key and passes the value into the given
	// callback. The value should not be copied if it doesn't have to be; the
	// callback will never store the byte slice outside. The error returned from
	// the callback should be passed through.
	Get(k []byte, fn func([]byte) error) error
	// Put puts the given value into the given key. It must not keep any of the
	// given byte slices after the call.
	Put(k, v []byte) error
	// Iterate iterates over all keys with the given prefix, in which the actual
	// key and value will be put into the callback. Databases that don't
	// implement this interface will not support arrays or objects.
	Iterate(prefix []byte, fn func(k, v []byte) error) error
	// DeletePrefix wipes a key with the given prefix.
	DeletePrefix(prefix []byte) error
}

// ManualIterator is an interator API that transactions can implement to have an
// alternative method of iterating over arrays. It is specifically made for
// databases that cannot iterate incrementally (but still can iterate), and
// should not be implemented by databases that can iterate incrementally, since
// this interface is usually slower.
type ManualIterator interface {
	// IterateManually iterates over the keys that the next function returns
	// until it returns nil. It is used for iterating over slices. The database
	// must not keep the byte slice returned by next.
	IterateManually(next func() []byte, fn func(v []byte) error) error
	// IteratePrefix iterates over all keys with the given prefix in undefined
	// order. It is used for iterating over maps.
	IteratePrefix(prefix []byte, fn func(k, v []byte) error) error
}
