package bboltpack

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/diamondburned/kvpack"
	"github.com/diamondburned/kvpack/driver/tests"
	"go.etcd.io/bbolt"
)

func mustOpenInMemory(tb testing.TB, namespace string) *kvpack.Database {
	temp := tb.TempDir()

	opts := &bbolt.Options{
		Timeout:      0,
		NoGrowSync:   false,
		FreelistType: bbolt.FreelistArrayType,
	}

	d, err := Open(namespace, filepath.Join(temp, "db"), os.ModePerm, opts)
	if err != nil {
		tb.Fatal("failed to open in-memory badgerDB:", err)
	}
	tb.Cleanup(func() { d.Close() })

	return d
}

func TestSuite(t *testing.T) {
	tests.DoTests(t, mustOpenInMemory(t, "tests"))
}

func BenchmarkGetKVPack(b *testing.B) {
	db := mustOpenInMemory(b, "get_kvpack")
	tests.NewBenchmarker(b, db).BenchmarkGetKVPack(b)
}

func BenchmarkGetJSON(b *testing.B) {
	db := mustOpenInMemory(b, "get_json")
	tests.NewBenchmarker(b, db).BenchmarkGetJSON(b)
}

func BenchmarkPutKVPack(b *testing.B) {
	db := mustOpenInMemory(b, "put_kvpack")
	tests.NewBenchmarker(b, db).BenchmarkPutKVPack(b)
}

func BenchmarkPutJSON(b *testing.B) {
	db := mustOpenInMemory(b, "put_json")
	tests.NewBenchmarker(b, db).BenchmarkPutJSON(b)
}