package defract

import (
	"testing"
	"unsafe"
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

type isZeroBenchData struct {
	name string
	data []byte
}

func isZeroSamples() []isZeroBenchData {
	var word [8]byte
	word[7] = '1'

	var dword [16]byte
	dword[15] = '1'

	var big [2048]byte
	big[2047] = '1'

	var massive [512 * 1024]byte
	massive[512*1024-1] = '1'

	var gigantic [zeroesLen * 2]byte
	gigantic[zeroesLen*2-1] = '1'

	return []isZeroBenchData{
		{"byte", word[:]},
		{"dbyte", dword[:]},
		{"big", big[:]},
		{"massive", massive[:]},
		{"gigantic", gigantic[:]},
	}
}

func isZeroBenchmark(b *testing.B, fn func([]byte) bool) {
	for _, bench := range isZeroSamples() {
		b.Run(bench.name, func(b *testing.B) {
			b.SetBytes(int64(len(bench.data)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if fn(bench.data) {
					b.Fatal("unexpected byte equal")
				}
			}
		})
	}
}

func BenchmarkIsZeroStreq(b *testing.B) {
	isZeroBenchmark(b, IsZeroBytes)
}

func BenchmarkIsZero8Bytes(b *testing.B) {
	isZeroBenchmark(b, isZero8Bytes)
}

func BenchmarkIsZero16Bytes(b *testing.B) {
	isZeroBenchmark(b, isZero16Bytes)
}

func isZero8Bytes(bytes []byte) bool {
	// vec8End defines the boundary in which the increment-by-8 loop cannot go
	// further.
	vec8End := len(bytes) - (len(bytes) % 8)
	current := 0

	// Compare (hopefully) most of the buffer 8 bytes at a time.
	for current < vec8End {
		if *(*uint64)(unsafe.Pointer(&bytes[current])) != 0 {
			return false
		}
		current += 8
	}

	// Compare the rest using a regular loop.
	for current < len(bytes) {
		if bytes[current] != 0 {
			return false
		}
		current++
	}

	return true
}

func isZero16Bytes(bytes []byte) bool {
	// vec8End defines the boundary in which the increment-by-8 loop cannot go
	// further.
	vec16End := len(bytes) - (len(bytes) % 16)
	current := 0

	// Compare (hopefully) most of the buffer 8 bytes at a time.
	for current < vec16End {
		if *(*complex128)(unsafe.Pointer(&bytes[current])) != 0 {
			return false
		}
		current += 16
	}

	// Compare the rest using a regular loop.
	for current < len(bytes) {
		if bytes[current] != 0 {
			return false
		}
		current++
	}

	return true
}

func BenchmarkZeroOutFast(b *testing.B) {
	random := make([]byte, zeroesLen*4)

	b.SetBytes(zeroesLen * 4)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ZeroOutBytes(random)
	}
}

func BenchmarkZeroOut16Bytes(b *testing.B) {
	random := make([]byte, zeroesLen*4)

	b.SetBytes(zeroesLen * 4)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		zeroOutBytes16(random)
	}
}

// zeroOutBytes16 is a decent implementation of SIMD zeroing. It doesn't require
// allocating a big array, but is slower than copy(). The implementation will
// try to set 16 bytes at a time.
func zeroOutBytes16(bytes []byte) {
	current := 0

	// Use the fast path only if we still have more than 16 bytes remaining.
	if len(bytes) > 16 {
		// Fill out the rest if copy returns exactly the length of zeroes. We
		// can do this 16 bytes at a time by using complex128.
		vec16End := len(bytes) - (len(bytes) % 16)

		for current < vec16End {
			*(*complex128)(unsafe.Pointer(&bytes[current])) = 0
			current += 16
		}
	}

	for current < len(bytes) {
		bytes[current] = 0
		current++
	}
}
