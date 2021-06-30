package bboltpack

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/diamondburned/kvpack"
	"github.com/diamondburned/kvpack/driver/tests"
	"go.etcd.io/bbolt"
)

func mustOpen(tb testing.TB, namespace string) *kvpack.Database {
	temp := tb.TempDir()

	opts := &bbolt.Options{
		Timeout:      0,
		NoSync:       true,
		NoGrowSync:   true,
		FreelistType: bbolt.FreelistArrayType,
	}

	d, err := Open(filepath.Join(temp, namespace), os.ModePerm, opts)
	if err != nil {
		tb.Fatal("failed to open in-memory badgerDB:", err)
	}
	tb.Cleanup(func() { d.Close() })

	return d.WithNamespace(namespace)
}

func TestSuite(t *testing.T) {
	tests.DoTests(t, mustOpen(t, "tests"))
}

func BenchmarkSuite(b *testing.B) {
	tests.DoBenchmark(b, mustOpen(b, "benchmarks"))
}
