package kvpack

import (
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/diamondburned/kvpack/defract"
	"github.com/diamondburned/kvpack/driver"
)

type mockTx struct {
	// use strings, which is slower but easier to test
	v map[string]string
}

func newMockTx(cap int) *mockTx {
	return &mockTx{make(map[string]string, cap)}
}

func (tx *mockTx) Commit() error   { return nil }
func (tx *mockTx) Rollback() error { return nil }

var errNotFoundTest = errors.New("not found")

func (tx *mockTx) Get(k []byte, fn func([]byte) error) error {
	b, ok := tx.v[string(k)]
	if ok {
		return fn([]byte(b))
	}
	return errNotFoundTest
}

func (tx *mockTx) Put(k, v []byte) error {
	tx.v[string(k)] = string(v)
	return nil
}

func (tx *mockTx) DeletePrefix(prefix []byte) error {
	prefixString := string(prefix)
	for k := range tx.v {
		if strings.HasPrefix(k, prefixString) {
			delete(tx.v, k)
		}
	}
	return nil
}

func (tx *mockTx) expect(t *testing.T, ns, key string, o map[string]string) {
	makeFullKey := func(k string) string {
		return string(Namespace + Separator + ns + Separator + key + Separator + k)
	}

	for k, v := range o {
		fullKey := makeFullKey(k)

		got, ok := tx.v[fullKey]
		if !ok {
			t.Errorf("missing key %q value %q", fullKey, v)
			continue
		}

		if got != v {
			t.Errorf("key %q value expected %q, got %q", fullKey, v, got)
			continue
		}

		delete(tx.v, fullKey)
		delete(o, k)
	}

	for k, v := range tx.v {
		t.Errorf("excess key %q value %q", k, v)
	}
}

func newTestTx(tx driver.Transaction, ns string) *Transaction {
	return NewTransaction(tx, Namespace+Separator+ns)
}

func TestTransactionStruct(t *testing.T) {
	type extinct struct {
		Dinosaurs string // raw bytes
	}

	type numbers struct {
		Byte byte       // raw byte
		Int  int        // varint
		Uint uint       // varuint
		I8   int8       // LE
		I16  int16      // LE
		I32  int32      // LE
		I64  int64      // LE
		U8   uint8      // raw byte
		U16  uint16     // LE
		U32  uint32     // LE
		U64  uint64     // LE
		F32  float32    // LE
		F64  float64    // LE
		C64  complex64  // LE
		C128 complex128 // LE
	}

	type quirks struct {
		BoolNil   *bool     // \x00, \x01, omitted if nil
		BoolPtr   *bool     // ^^^^^^^^^^^^^^^^^^^^^^^^^^
		StructNil *struct{} // always omitted
		StructPtr *struct{} // always omitted
		StringNil *string
		StringPtr *string
	}

	type animals struct {
		Extinct  extinct
		Cats     string // raw bytes
		Dogs     string // raw bytes
		SoTrue   bool   // \x01, omitted if false
		SoUntrue bool   // ^^^^^^^^^^^^^^^^^^^^^^
		Numbers  numbers
		More     *animals
		NoMore   *animals // nil
		Quirks   *quirks
		junk     string
	}

	testValue := animals{
		Extinct: extinct{
			Dinosaurs: "???",
		},
		Cats:     "meow",
		Dogs:     "woof",
		SoTrue:   true,
		SoUntrue: false,
		Numbers: numbers{
			Byte: 1,
			Int:  math.MaxInt,
			Uint: math.MaxUint,
			I8:   math.MaxInt8,
			I16:  math.MaxInt16,
			I32:  math.MaxInt32,
			I64:  math.MaxInt64,
			U8:   math.MaxUint8,
			U16:  math.MaxUint16,
			U32:  math.MaxUint32,
			U64:  math.MaxUint64,
			F32:  math.MaxFloat32,
			F64:  math.MaxFloat64,
			C64:  5 + 6i,
			C128: 10 + 12i,
		},
		More: &animals{
			Cats: "nya nya",
			Dogs: "wan wan",
		},
		NoMore: nil,
		Quirks: &quirks{
			BoolPtr:   new(bool),
			StructPtr: new(struct{}),
			StringPtr: new(string),
		},
		junk: "ignore me",
	}

	kv := newMockTx(1)
	tx := newTestTx(kv, "kvpack_test")

	if err := tx.Put([]byte("animals"), &testValue); err != nil {
		t.Fatal("failed to put:", err)
	}

	f := func(fields ...string) string {
		return strings.Join(fields, Separator)
	}

	kv.expect(t, "kvpack_test", "animals", map[string]string{
		f("Cats"):                 "meow",
		f("Dogs"):                 "woof",
		f("SoTrue"):               "\x01",
		f("Extinct", "Dinosaurs"): "???",
		f("Numbers", "Byte"):      "\x01",
		f("Numbers", "Int"):       copyNumBytes(int(math.MaxInt)),
		f("Numbers", "Uint"):      copyNumBytes(uint(math.MaxUint)),
		f("Numbers", "I8"):        copyNumBytes(int8(math.MaxInt8)),
		f("Numbers", "I16"):       copyNumBytes(int16(math.MaxInt16)),
		f("Numbers", "I32"):       copyNumBytes(int32(math.MaxInt32)),
		f("Numbers", "I64"):       copyNumBytes(int64(math.MaxInt64)),
		f("Numbers", "U8"):        copyNumBytes(uint8(math.MaxUint8)),
		f("Numbers", "U16"):       copyNumBytes(uint16(math.MaxUint16)),
		f("Numbers", "U32"):       copyNumBytes(uint32(math.MaxUint32)),
		f("Numbers", "U64"):       copyNumBytes(uint64(math.MaxUint64)),
		f("Numbers", "F32"):       copyNumBytes(float32(math.MaxFloat32)),
		f("Numbers", "F64"):       copyNumBytes(float64(math.MaxFloat64)),
		f("Numbers", "C64"):       copyNumBytes(complex64(5 + 6i)),
		f("Numbers", "C128"):      copyNumBytes(complex128(10 + 12i)),
		f("More", "Cats"):         "nya nya",
		f("More", "Dogs"):         "wan wan",
		f("Quirks", "BoolPtr"):    "\x00",
		f("Quirks", "StringPtr"):  "",
	})
}

func copyNumBytes(v interface{}) string {
	typ, ptr := defract.UnderlyingPtr(v)

	if typ == defract.Int || typ == defract.Uint {
		b := make([]byte, 10)
		n := 0
		switch typ {
		case defract.Int:
			n = binary.PutVarint(b, int64(*(*int)(ptr)))
		case defract.Uint:
			n = binary.PutUvarint(b, uint64(*(*uint)(ptr)))
		}
		return string(b[:n])
	}

	var out string
	defract.NumberLE(typ, ptr, func(b []byte) error {
		out = string(b)
		return nil
	})

	return out
}
