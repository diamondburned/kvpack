// Package driver contains interfaces that describes a generic transactional
// key-value database.
package driver

// Database describes a generic transactional key-value database.
type Database interface {
	Begin() (Transaction, error)
}

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
}
