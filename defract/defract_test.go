package defract

import (
	"bytes"
	"math"
	"math/rand"
	"os/exec"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/go-test/deep"
)

type mockType string

func (v mockType) String() string { return string(v) }
func (v mockType) FortyTwo() int  { return 42 }

func TestUnderlyingPtr(t *testing.T) {
	const sample = "Hello, 世界"

	eqType := func(t *testing.T, typ, expect reflect.Type) {
		t.Helper()

		if typ != expect {
			t.Error("expected type string, got", typ)
		}
	}

	// eq verifies the backing array.
	eq := func(t *testing.T, ptr unsafe.Pointer) {
		t.Helper()

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

	t.Run("nested-nil", func(t *testing.T) {
		typ, got := UnderlyingPtr((**string)(nil))
		if got != nil {
			t.Fatal("unexpected non-nil ptr returned from nil")
		}
		if typ != nil {
			t.Fatal("unexpected type non-nil")
		}
	})

	t.Run("0-level", func(t *testing.T) {
		typ, got := UnderlyingPtr(sample)
		if got != nil {
			t.Fatal("unexpected non-nil ptr returned from nil")
		}
		if typ != nil {
			t.Fatal("unexpected type non-nil")
		}
	})

	t.Run("method-0-level", func(t *testing.T) {
		typ, got := UnderlyingPtr(mockType(sample))
		if got != nil {
			t.Fatal("unexpected non-nil ptr returned from nil")
		}
		if typ != nil {
			t.Fatal("unexpected type non-nil")
		}
	})

	check := func(t *testing.T, v, expectTyp interface{}) {
		t.Helper()

		typ, got := UnderlyingPtr(v)
		eqType(t, typ, reflect.TypeOf(expectTyp))
		eq(t, got)
	}

	t.Run("1-level", func(t *testing.T) {
		str := sample
		check(t, &str, sample)
	})
	t.Run("2-level", func(t *testing.T) {
		str := sample
		ptr := &str
		check(t, &ptr, sample)
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

func TestAllocIndirect(t *testing.T) {
	// skip alloc test, since that's tested in driver/tests/.

	t.Run("noalloc", func(t *testing.T) {
		str := "hello, world"
		ptr1 := &str
		ptr2 := &ptr1

		typ, ptr := AllocIndirect(reflect.TypeOf(ptr2), unsafe.Pointer(&ptr2))
		if typ != reflect.TypeOf("") {
			t.Fatalf("unexpected (not string) type: %v", typ)
		}
		if ptr != unsafe.Pointer(&str) {
			t.Fatalf("unexpected ptr returned: expected %p got %p", unsafe.Pointer(&str), ptr)
		}
	})
}

func TestIsZero(t *testing.T) {
	testWithSize := func(t *testing.T, size, offset int) {
		t.Helper()

		value := make([]byte, size)
		if !IsZero(unsafe.Pointer(&value[0]), uintptr(len(value))) {
			t.Error("value is not eq when it should be")
		}

		value[size-offset] = '\x01'
		if IsZero(unsafe.Pointer(&value[0]), uintptr(len(value))) {
			t.Error("value is eq when it should not be")
		}
	}

	t.Run("1024_end", func(t *testing.T) { testWithSize(t, 1024, 1) })
	t.Run("1024_start", func(t *testing.T) { testWithSize(t, 1024, 1024/4) })
	t.Run("4096_end", func(t *testing.T) { testWithSize(t, 4096, 1) })
	t.Run("4096_start", func(t *testing.T) { testWithSize(t, 4096, 4096/4) })
	t.Run("1024000_end", func(t *testing.T) { testWithSize(t, 1024000, 1) })
	t.Run("1024000_start", func(t *testing.T) { testWithSize(t, 1024000, 1024000/4) })
	t.Run("zlen_end", func(t *testing.T) { testWithSize(t, zeroesLen, 1) })
	t.Run("zlen_start", func(t *testing.T) { testWithSize(t, zeroesLen, zeroesLen/4) })
	t.Run("4096000_end", func(t *testing.T) { testWithSize(t, 4096000, 1) })
	t.Run("4096000_start", func(t *testing.T) { testWithSize(t, 4096000, 4096000/4) })
}

func TestZeroOut(t *testing.T) {
	rander := rand.New(rand.NewSource(time.Now().UnixNano()))

	testWithSize := func(t *testing.T, size int) {
		value := make([]byte, size)

		// Read until the values are not completely zero. We can use IsZeroBytes
		// because we've already tested it above.
		for IsZeroBytes(value) {
			_, err := rander.Read(value)
			if err != nil {
				t.Error("failed to math.rand Read:", err)
				return
			}
		}

		ZeroOutBytes(value)

		if !IsZeroBytes(value) {
			t.Log("ZeroOutBytes fail, last 0 at", bytes.LastIndexByte(value, '0'))
			t.Error("ZeroOutBytes did not zero out completely")
		}
	}

	t.Run("1024", func(t *testing.T) { testWithSize(t, 1024) })
	t.Run("4096", func(t *testing.T) { testWithSize(t, 4096) })
	t.Run("1024000", func(t *testing.T) { testWithSize(t, 1024000) })
	t.Run("zeroesLen", func(t *testing.T) { testWithSize(t, zeroesLen) })
	t.Run("4096000", func(t *testing.T) { testWithSize(t, 4096000) })
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
		ptr := InterfacePtr(test)
		typ := reflect.TypeOf(test)

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

type testStruct struct {
	Field1  string
	Field2  string
	Foo     int
	Bar     int
	Astolfo anotherStruct
}

type anotherStruct struct {
	Astolfo string
}

func TestStructInfo(t *testing.T) {
	expect := StructInfo{
		Type:      reflect.TypeOf(testStruct{}),
		RawSchema: []byte("Field1\x00Field2\x00Foo\x00Bar\x00Astolfo"),
		Fields: []StructField{
			{
				Type:   reflect.TypeOf(""),
				Kind:   reflect.String,
				Name:   []byte("Field1"),
				Size:   2 * unsafe.Sizeof(0),
				Offset: 0,
			},
			{
				Type:   reflect.TypeOf(""),
				Kind:   reflect.String,
				Name:   []byte("Field2"),
				Size:   2 * unsafe.Sizeof(0),
				Offset: 2 * unsafe.Sizeof(0),
			},
			{
				Type:   reflect.TypeOf(int(0)),
				Kind:   reflect.Int,
				Name:   []byte("Foo"),
				Size:   unsafe.Sizeof(0),
				Offset: 4 * unsafe.Sizeof(0),
			},
			{
				Type:   reflect.TypeOf(int(0)),
				Kind:   reflect.Int,
				Name:   []byte("Bar"),
				Size:   unsafe.Sizeof(0),
				Offset: 5 * unsafe.Sizeof(0),
			},
			{
				Type:   reflect.TypeOf(anotherStruct{}),
				Kind:   reflect.Struct,
				Name:   []byte("Astolfo"),
				Size:   2 * unsafe.Sizeof(0),
				Offset: 6 * unsafe.Sizeof(0),
			},
		},
	}

	got := GetStructInfo(reflect.TypeOf(testStruct{}))

	for _, ineq := range deep.Equal(&expect, got) {
		t.Errorf("expect/got: %q", ineq)
	}
}
