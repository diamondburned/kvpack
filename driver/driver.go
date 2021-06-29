// Package driver contains interfaces that describes a generic transactional
// key-value database.
package driver

import "errors"

// Database describes a generic transactional key-value database.
type Database interface {
	Begin(readOnly bool) (Transaction, error)
}

// ErrUnsupportedIterator should be returned by transactions from Iterate if the
// database doesn't support iterating using the Iterate API. In that case,
// kvpack will try using the ManualIterator API instead.
var ErrUnsupportedIterator = errors.New("unsupported iterator")

// Transaction describes a regular transaction with a simple read and write API.
// Transactions must implement UnorderedIterator to read maps.
type Transaction interface {
	Commit() error
	Rollback() error

	// Get gets the value with the given key and passes the value into the given
	// callback. The value should not be copied if it doesn't have to be; the
	// callback will never store the byte slice outside. The error returned from
	// the callback should be passed through. If the key is not found, then Get
	// should return a nil error and not call fn.
	Get(k []byte, fn func([]byte) error) error
	// Put puts the given value into the given key. It must not keep any of the
	// given byte slices after the call.
	Put(k, v []byte) error
	// DeletePrefix wipes a key with the given prefix.
	DeletePrefix(prefix []byte) error
	// Iterate iterates over all keys with the given prefix in undefined order.
	// It is used for iterating over maps and optionally arrays.
	Iterate(prefix []byte, fn func(k, v []byte) error) error
}

// KeyIterator is similar to Transaction's Iterate method, except only the key
// is iterated over.
type KeyIterator interface {
	IterateKey(prefix []byte, fn func(k []byte) error) error
}

// Preloader is an optional interface that a transaction can implement that
// preloads all keys and values with the given prefix.
type Preloader interface {
	// Preload preloads all keys and values with the given prefix. Preload
	// errors should be silently discarded. Implementations don't need to update
	// the cache on Put; users expecting such a behavior is not good.
	Preload(prefix []byte)
	// Unload wipes the preloaded cache of all keys with the given prefix. This
	// is usually called by the handler to ensure that cache invalidation can be
	// done trivially. Like Preload, errors should be silently ignored.
	Unload(prefix []byte)
}
