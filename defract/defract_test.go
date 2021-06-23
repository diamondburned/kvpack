package defract

import (
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
	t.Run("1024000_1", func(t *testing.T) { testWithSize(t, 1024000, 1) })
	t.Run("1024000_2", func(t *testing.T) { testWithSize(t, 1024000, 1024000/4) })
}
