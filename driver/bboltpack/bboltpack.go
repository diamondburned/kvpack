// Package bboltpack implements the kvpack drivers using Bolt.
package bboltpack

import (
	"bytes"
	"os"

	"github.com/diamondburned/kvpack"
	"github.com/diamondburned/kvpack/driver"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
)

// DB implements driver.Database.
type DB bbolt.DB

// Open opens a new Badger database wrapped inside a driver.Database-compatible
// implementation.
func Open(path string, mode os.FileMode, opts *bbolt.Options) (*kvpack.Database, error) {
	d, err := bbolt.Open(path, mode, opts)
	if err != nil {
		return nil, err
	}

	if err := d.Update(func(tx *bbolt.Tx) error {

		return nil
	}); err != nil {
		return nil, err
	}

	return kvpack.NewDatabase((*DB)(d)), nil
}

// Close closes the database.
func (db *DB) Close() error {
	return (*bbolt.DB)(db).Close()
}

// Begin starts a transaction.
func (db *DB) Begin(namespace []byte, ro bool) (driver.Transaction, error) {
	tx, err := (*bbolt.DB)(db).Begin(!ro)
	if err != nil {
		return nil, err
	}

	var b *bbolt.Bucket
	if !ro {
		b, err = tx.CreateBucketIfNotExists(namespace)
		if err != nil {
			tx.Rollback()
			return nil, errors.Wrap(err, "failed to create namespace bucket")
		}
	} else {
		b = tx.Bucket(namespace)
	}

	return &Tx{
		bucket: b,
		tx:     tx,

		namespace: len(namespace) + 1,
		done:      false,
	}, nil
}

// Tx implements driver.Transaction.
type Tx struct {
	bucket *bbolt.Bucket // can be nil if RO tx
	tx     *bbolt.Tx

	namespace int
	done      bool
}

var _ driver.Transaction = (*Tx)(nil)

// NamespaceBucket returns the current namespace's bucket inside the
// transaction. If the transaction is read-only and no keys have been put
// before, then nil is returned.
func (tx *Tx) NamespaceBucket() *bbolt.Bucket {
	return tx.bucket
}

// Using recursive buckets might worsen performance by quite a margin, but this
// has only been tested with small structs, so the performance difference may
// not be too pronounced.

// Commit commits the current transaction. Calling Commit multiple times does
// nothing and will return nil.
func (tx *Tx) Commit() error {
	if tx.done {
		return nil
	}

	tx.done = true
	return tx.tx.Commit()
}

// Rollback discards the current transaction.
func (tx *Tx) Rollback() error {
	if tx.done {
		return nil
	}

	tx.done = true
	return tx.tx.Rollback()
}

// Get gets the value with the given key.
func (tx *Tx) Get(k []byte) ([]byte, error) {
	if tx.bucket == nil {
		return nil, driver.ErrKeyNotFound
	}

	v := tx.bucket.Get(k)
	if v == nil {
		return nil, driver.ErrKeyNotFound
	}

	return v, nil
}

// Put puts the given value into the given key.
func (tx *Tx) Put(k, v []byte) error {
	if tx.bucket == nil {
		return errors.New("Put called on RO transaction")
	}

	return tx.bucket.Put(k, v)
}

// DeletePrefix deletes all keys with the given prefix.
func (tx *Tx) DeletePrefix(prefix []byte) error {
	if tx.bucket == nil {
		return errors.New("DeletePrefix called on RO transaction")
	}

	cursor := tx.bucket.Cursor()

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
	if tx.bucket == nil {
		// Not having the prefix is not an error, but we don't call fn.
		return nil
	}

	cursor := tx.bucket.Cursor()

	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		if bytes.HasPrefix(k, prefix) {
			if err := fn(k, v); err != nil {
				return err
			}
		}
	}

	return nil
}

// BUG(diamond): Iterate seems to have a bug if a new transaction is created
// every GetKVPack benchmark iteration; the workaround was to change it so that
// all iterations are done inside a transaction, but this doesn't fix the root
// issue.

/*
2021/06/29 16:08:33 panicking at key "__kvpack\x00get_kvpack\x00benchmark_get_kvpack\x00OtherCharacters\x001\x00CharacterScore"
2021/06/29 16:08:33 panicking at key "__kvpack\x00get_kvpack\x00benchmark_get_kvpack\x00OtherCharacters"
panic: runtime error: index out of range [2599146904983207936] with length 281474976710655 [recovered]
	panic: runtime error: index out of range [2599146904983207936] with length 281474976710655 [recovered]
	panic: runtime error: index out of range [2599146904983207936] with length 281474976710655

goroutine 35 [running]:
github.com/diamondburned/kvpack.(*Transaction).getStruct.func1()
	/home/diamond/Scripts/kvpack/tx_get.go:322 +0x9d
panic({0x5974c0, 0xc000020738})
	/nix/store/l1nrqby8hqd9jks99f5ipvskgdrvbwcp-go2-unstable-2021-06-25/share/go/src/runtime/panic.go:1038 +0x215
github.com/diamondburned/kvpack.(*Transaction).getStruct.func1()
	/home/diamond/Scripts/kvpack/tx_get.go:322 +0x9d
panic({0x5974c0, 0xc000020738})
	/nix/store/l1nrqby8hqd9jks99f5ipvskgdrvbwcp-go2-unstable-2021-06-25/share/go/src/runtime/panic.go:1038 +0x215
go.etcd.io/bbolt.(*DB).page(...)
	/home/diamond/.go/pkg/mod/go.etcd.io/bbolt@v1.3.6/db.go:933
go.etcd.io/bbolt.(*Tx).page(...)
	/home/diamond/.go/pkg/mod/go.etcd.io/bbolt@v1.3.6/tx.go:617
go.etcd.io/bbolt.(*Bucket).pageNode(0xc000100800, 0x69724120656d6948)
	/home/diamond/.go/pkg/mod/go.etcd.io/bbolt@v1.3.6/bucket.go:726 +0x109
go.etcd.io/bbolt.(*Cursor).search(0xc000467208, {0xc000680250, 0x14, 0x35}, 0xc0000751a0)
	/home/diamond/.go/pkg/mod/go.etcd.io/bbolt@v1.3.6/cursor.go:248 +0x54
go.etcd.io/bbolt.(*Cursor).seek(0xc000467208, {0xc000680250, 0xc000680250, 0x35})
	/home/diamond/.go/pkg/mod/go.etcd.io/bbolt@v1.3.6/cursor.go:159 +0x48
go.etcd.io/bbolt.(*Bucket).Bucket(0xc00045a880, {0xc000680250, 0xc0000752e0, 0x35})
	/home/diamond/.go/pkg/mod/go.etcd.io/bbolt@v1.3.6/bucket.go:105 +0xc8
github.com/diamondburned/kvpack/driver/bboltpack.(*Tx).childBucket.func2({0xc000680250, 0xc000075338, 0x5295a9})
	/home/diamond/Scripts/kvpack/driver/bboltpack/bboltpack.go:131 +0x31
github.com/diamondburned/kvpack/driver/bboltpack.(*Tx).childBucket(0xc00002c260, {0xc00068023c, 0xc000414f50, 0xe}, 0x0, 0x0)
	/home/diamond/Scripts/kvpack/driver/bboltpack/bboltpack.go:145 +0x1a6
github.com/diamondburned/kvpack/driver/bboltpack.(*Tx).ChildKey(...)
	/home/diamond/Scripts/kvpack/driver/bboltpack/bboltpack.go:91
github.com/diamondburned/kvpack/driver/bboltpack.(*Tx).Get(0xc000414f90, {0xc00068023c, 0x3a, 0xc00007a3c0})
	/home/diamond/Scripts/kvpack/driver/bboltpack/bboltpack.go:195 +0x25
github.com/diamondburned/kvpack.(*Transaction).getStruct(0xc000414f50, {0xc000453680, 0x3a, 0x239}, 0xc00004a780, 0xc00002c260, 0x5)
	/home/diamond/Scripts/kvpack/tx_get.go:331 +0x25e
github.com/diamondburned/kvpack.(*Transaction).getValueBytes(0x40, {0xc000453680, 0x3a, 0x239}, {0x7f65bc7e215c, 0xc00045c3f0, 0x2c}, {0x5e62c0, 0x58f0e0}, 0x19, ...)
	/home/diamond/Scripts/kvpack/tx_get.go:248 +0x4eb
github.com/diamondburned/kvpack.(*Transaction).getSlice.func1({0xc000453680, 0x3a, 0x239}, {0x7f65bc7e215c, 0x2c, 0x2c})
	/home/diamond/Scripts/kvpack/tx_get.go:307 +0x2cf
github.com/diamondburned/kvpack/driver/bboltpack.(*Tx).Iterate(0xc000414f90, {0xc0006800e0, 0x39, 0xf}, 0xc0003f2d20)
	/home/diamond/Scripts/kvpack/driver/bboltpack/bboltpack.go:264 +0x2ca
github.com/diamondburned/kvpack.(*Transaction).getSlice(0xc000414f50, {0xc0006800a8, 0x38, 0x38}, {0x7f65bc7e30a7, 0x8, 0x8}, {0x5e62c0, 0x574700}, 0xc00011a708)
	/home/diamond/Scripts/kvpack/tx_get.go:288 +0x351
github.com/diamondburned/kvpack.(*Transaction).getValueBytes(0x18, {0xc0006800a8, 0x38, 0x38}, {0x7f65bc7e30a7, 0x2, 0x8}, {0x5e62c0, 0x574700}, 0x17, ...)
	/home/diamond/Scripts/kvpack/tx_get.go:242 +0x37c
github.com/diamondburned/kvpack.(*Transaction).getStruct(0xc000414f50, {0xc000680013, 0x28, 0x28}, 0xc00004a780, 0xc00011a6f0, 0x1)
	/home/diamond/Scripts/kvpack/tx_get.go:339 +0x305
github.com/diamondburned/kvpack.(*Transaction).getValueBytes(0xc000414f50, {0xc000680013, 0x28, 0x28}, {0x7f65bc7e6069, 0x5e62c0, 0x2c}, {0x5e62c0, 0x58f0e0}, 0x19, ...)
	/home/diamond/Scripts/kvpack/tx_get.go:248 +0x4eb
github.com/diamondburned/kvpack.(*Transaction).getValue(0xc000414f50, {0xc000680013, 0x28, 0x28}, {0x5e62c0, 0x58f0e0}, 0x7f65bc7af6a8, 0x50, 0x0)
	/home/diamond/Scripts/kvpack/tx_get.go:190 +0x165
github.com/diamondburned/kvpack.(*Transaction).get(0xc000414f50, {0xc000680013, 0x28, 0x28}, {0x570fc0, 0xc00011a6f0})
	/home/diamond/Scripts/kvpack/tx_get.go:86 +0x525
github.com/diamondburned/kvpack.(*Transaction).Get(0xc000414f50, {0xc000075dec, 0x60dba7f1, 0x11af76c3}, {0x570fc0, 0xc00011a6f0})
	/home/diamond/Scripts/kvpack/tx_get.go:22 +0x89
github.com/diamondburned/kvpack.(*Database).Get(0xc00007c020, {0xc000075dec, 0x14, 0x14}, {0x570fc0, 0xc00011a6f0})
	/home/diamond/Scripts/kvpack/kvpack.go:231 +0xb9
github.com/diamondburned/kvpack/driver/tests.Benchmarker.BenchmarkGetKVPack({{0x5e5d98, 0xc0000c0480}, 0xc00007c020}, 0xc0000c0480)
	/home/diamond/Scripts/kvpack/driver/tests/benchmark.go:97 +0x29c
github.com/diamondburned/kvpack/driver/bboltpack.BenchmarkGetKVPack(0xc0000c0480)
	/home/diamond/Scripts/kvpack/driver/bboltpack/bboltpack_test.go:100 +0x4b
testing.(*B).runN(0xc0000c0480, 0x2710)
	/nix/store/l1nrqby8hqd9jks99f5ipvskgdrvbwcp-go2-unstable-2021-06-25/share/go/src/testing/benchmark.go:192 +0x126
testing.(*B).launch(0xc0000c0480)
	/nix/store/l1nrqby8hqd9jks99f5ipvskgdrvbwcp-go2-unstable-2021-06-25/share/go/src/testing/benchmark.go:328 +0x1c5
created by testing.(*B).doBench
	/nix/store/l1nrqby8hqd9jks99f5ipvskgdrvbwcp-go2-unstable-2021-06-25/share/go/src/testing/benchmark.go:283 +0x7b
*/
