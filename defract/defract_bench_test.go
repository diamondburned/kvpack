package defract

import (
	"bytes"
	"testing"
)

// Benchmark results for the below:
//
//    cpu: Intel(R) Core(TM) i5-8250U CPU @ 1.60GHz
//
//    BenchmarkIsZeroStreq/8-8             252594735    4.426 ns/op
//    BenchmarkIsZeroStreq/16-8            218680728    5.658 ns/op
//    BenchmarkIsZeroStreq/2048-8          19221823     52.74 ns/op
//    BenchmarkIsZeroStreq/2048_parallel-8 56079915     18.30 ns/op
//    BenchmarkIsZeroLoop/8-8              221373572    5.384 ns/op
//    BenchmarkIsZeroLoop/16-8             130468988    10.18 ns/op
//    BenchmarkIsZeroLoop/2048-8           1463259      786.4 ns/op
//    BenchmarkIsZeroLoop/2048_parallel-8  5172028      226.9 ns/op

func isZeroSamples() map[string][]byte {
	var word [8]byte
	word[7] = '1'

	var dword [16]byte
	dword[15] = '1'

	var big [2048]byte
	big[2047] = '1'

	return map[string][]byte{
		"8":    word[:],
		"16":   dword[:],
		"2048": big[:],
	}
}

func isZeroBenchmark(b *testing.B, fn func(*testing.B, []byte)) {
	for name, value := range isZeroSamples() {
		b.Run(name, func(b *testing.B) { fn(b, value) })
	}
}

func BenchmarkIsZeroStreq(b *testing.B) {
	isZeroBenchmark(b, benchmarkIsZeroStreq)

	b.Run("2048_parallel", func(b *testing.B) {
		value := make([]byte, 2048)
		value[2047] = '1'

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if bytes.Equal(zeroes[:len(value)], value[:]) {
					b.Fatal("unexpected byte equal")
				}
			}
		})
	})
}

func benchmarkIsZeroStreq(b *testing.B, v []byte) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if bytes.Equal(zeroes[:], v) {
			b.Fatal("unexpected byte equal")
		}
	}
}

func BenchmarkIsZeroLoop(b *testing.B) {
	isZeroBenchmark(b, benchmarkIsZeroAny)

	b.Run("2048_parallel", func(b *testing.B) {
		value := make([]byte, 2048)
		value[2047] = '1'

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if isZeroAny(value) {
					b.Fatal("unexpected byte equal")
				}
			}
		})
	})
}

func benchmarkIsZeroAny(b *testing.B, v []byte) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if isZeroAny(v) {
			b.Fatal("unexpected byte equal")
		}
	}
}
