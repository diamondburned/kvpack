package mock

import (
	"testing"

	"github.com/diamondburned/kvpack/driver/tests"
)

func TestSuite(t *testing.T) {
	tests.DoTests(t, NewDatabase("tests"))
}

func BenchmarkSuite(b *testing.B) {
	tests.DoBenchmark(b, NewDatabase("benchmarks"))
}
