package badgerpack

import (
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/diamondburned/kvpack"
	"github.com/diamondburned/kvpack/driver/tests"
)

func mustOpenInMemory(tb testing.TB, namespace string) *kvpack.Database {
	opts := badger.DefaultOptions("")
	opts.Logger = nil
	opts.InMemory = true

	d, err := Open(namespace, opts)
	if err != nil {
		tb.Fatal("failed to open in-memory badgerDB:", err)
	}

	return d
}

func TestSuite(t *testing.T) {
	tests.DoTests(t, mustOpenInMemory(t, "tests"))
}

func BenchmarkSuite(b *testing.B) {
	tests.DoBenchmark(b, mustOpenInMemory(b, "benchmarks"))
}