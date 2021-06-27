package defract

import (
	"bytes"
	"math"
	"os/exec"
	"reflect"
	"testing"
	"unsafe"
)

type mockType string

func (v mockType) String() string { return string(v) }
func (v mockType) FortyTwo() int  { return 42 }

func TestUnderlyingPtr(t *testing.T) {
	const sample = "Hello, 世界"

	eqType := func(t *testing.T, typ, expect reflect.Type) {
		if typ != expect {
			t.Error("expected type string, got", typ)
		}
	}

	// eq verifies the backing array.
	eq := func(t *testing.T, ptr unsafe.Pointer) {
		var (
			length  = (*reflect.StringHeader)(ptr).Len
			pointer = unsafe.Pointer((*reflect.StringHeader)(ptr).Data)
		)

		if length != len(sample) {
			t.Fatalf("expected length %d, got %d", len(sample), length)
		}

		array := unsafe.Slice((*byte)(pointer), len(sample))
		if string(array) != sample {
			t.Fatalf("expected %q, got %q", sample, string(array))
		}
	}

	t.Run("nil", func(t *testing.T) {
		typ, got := UnderlyingPtr(nil)
		if got != nil {
			t.Fatal("unexpected non-nil ptr returned from nil")
		}
		if typ != nil {
			t.Fatal("unexpected type non-nil")
		}
	})

	check := func(t *testing.T, v, expectTyp interface{}) {
		typ, got := UnderlyingPtr(v)
		eqType(t, typ, reflect.TypeOf(expectTyp))
		eq(t, got)
	}

	t.Run("0-level", func(t *testing.T) {
		str := sample
		check(t, str, sample)
	})
	t.Run("1-level", func(t *testing.T) {
		str := sample
		check(t, &str, sample)
	})
	t.Run("2-level", func(t *testing.T) {
		str := sample
		ptr := &str
		check(t, &ptr, sample)
	})

	t.Run("method-0-level", func(t *testing.T) {
		str := mockType(sample)
		check(t, str, mockType(""))
	})
	t.Run("method-1-level", func(t *testing.T) {
		str := mockType(sample)
		check(t, &str, mockType(""))
	})
	t.Run("method-2-level", func(t *testing.T) {
		str := mockType(sample)
		ptr := &str
		check(t, &ptr, mockType(""))
	})
}

func TestIsZero(t *testing.T) {
	testWithSize := func(t *testing.T, size, offset int) {
		value := make([]byte, size)
		if !IsZero(unsafe.Pointer(&value[0]), uintptr(len(value))) {
			t.Error("value is not eq when it should be")
		}

		value[size-offset] = '\x01'
		if IsZero(unsafe.Pointer(&value[0]), uintptr(len(value))) {
			t.Error("value is eq when it should not be")
		}
	}

	t.Run("1024", func(t *testing.T) { testWithSize(t, 1024, 1) })
	t.Run("4096", func(t *testing.T) { testWithSize(t, 4096, 1) })
	t.Run("1024000_end", func(t *testing.T) { testWithSize(t, 1024000, 1) })
	t.Run("1024000_start", func(t *testing.T) { testWithSize(t, 1024000, 1024000/4) })
}

func TestIsLittleEndian(t *testing.T) {
	lscpu := exec.Command("lscpu")

	o, err := lscpu.Output()
	if err != nil {
		t.Skip("no lscpu:", err)
	}

	for _, line := range bytes.Split(o, []byte("\n")) {
		if !bytes.Contains(line, []byte("Byte Order:")) {
			continue
		}

		words := bytes.Fields(line)
		lastTwo := bytes.Join(words[len(words)-2:], []byte(" "))

		switch string(lastTwo) {
		case "Little Endian":
			if !IsLittleEndian {
				t.Fatal("not little endian")
			}
			return
		case "Big Endian":
			if IsLittleEndian {
				t.Fatal("not big endian")
			}
			return
		default:
			t.Skipf("unknown Byte Order value %q", words)
		}
	}

	t.Skip("unrecognized lscpu output")
}

func TestWithinSlice(t *testing.T) {
	outer := make([]byte, 0, 50)
	inner := outer[2:34]

	if !WithinBytes(outer, inner) {
		t.Fatal("unexpected outer/inner result")
	}

	if WithinBytes(outer, make([]byte, 0, 10)) {
		t.Fatal("new slice is incorrectly within outer")
	}
}

func TestNumberLE(t *testing.T) {
	if !IsLittleEndian {
		t.Skip("skipping NumberLE test, since not LE machine")
	}

	// Restore LE after done.
	t.Cleanup(func() { IsLittleEndian = true })

	var tests = []interface{}{
		uint8('c'),
		uint16(math.MaxUint16),
		uint32(math.MaxUint32),
		uint64(math.MaxUint64),
		int8('c'),
		int16(math.MaxInt16),
		int32(math.MaxInt32),
		int64(math.MaxInt64),
		float32(math.Inf(-1)),
		float32(math.Inf(+1)),
		float64(math.Inf(-1)),
		float64(math.Inf(+1)),
		complex64(5 + 10i),
		complex128(5 + 10i),
	}

	for _, test := range tests {
		typ, ptr := UnderlyingPtr(test)

		IsLittleEndian = true
		le := NumberLE(typ.Kind(), ptr)

		IsLittleEndian = false
		be := NumberLE(typ.Kind(), ptr)

		if !bytes.Equal(le, be) {
			t.Fatalf("big endian != little endian output\nLE: %v\nBE: %v", le, be)
		}

		for name, input := range map[string][]byte{"LE": le, "BE": be} {
			valLE := reflect.New(typ)

			IsLittleEndian = true
			if !ReadNumberLE(input, typ.Kind(), unsafe.Pointer(valLE.Pointer())) {
				t.Fatalf("ReadNumberLE fail on Little Endian with %s input", name)
			}

			if v := valLE.Elem().Interface(); v != test {
				t.Fatalf("ReadNumberLE Little Endian output differs: %v != %v", v, test)
			}

			valBE := reflect.New(typ)

			IsLittleEndian = false
			if !ReadNumberLE(input, typ.Kind(), unsafe.Pointer(valBE.Pointer())) {
				t.Fatalf("ReadNumberLE fail on Big Endian with %s input", name)
			}

			if v := valBE.Elem().Interface(); v != test {
				t.Fatalf("ReadNumberLE Big Endian output differs: %v != %v", v, test)
			}
		}
	}
}
