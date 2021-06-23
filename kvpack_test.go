package kvpack

import (
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/diamondburned/kvpack/defract"
	"github.com/diamondburned/kvpack/driver"
)

type mockTx struct {
	// use strings, which is slower but easier to test
	v map[string]string
}

var (
	_ driver.Transaction    = (*mockTx)(nil)
	_ driver.ManualIterator = (*mockTx)(nil)
)

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

func (tx *mockTx) Iterate([]byte, func(k, v []byte) error) error {
	return driver.ErrUnsupportedIterator
}

func (tx *mockTx) IterateManually(next func() []byte, fn func(v []byte) error) error {
	for key := next(); key != nil; key = next() {
		v, ok := tx.v[string(key)]
		if ok {
			if err := fn([]byte(v)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (tx *mockTx) IteratePrefix(prefix []byte, fn func(k, v []byte) error) error {
	prefixString := string(prefix)

	for k, v := range tx.v {
		if strings.HasPrefix(k, prefixString) {
			if err := fn([]byte(k), []byte(v)); err != nil {
				return err
			}
		}
	}

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

	type maps struct {
		Uint map[uint]int
		Int  map[int]uint
		F64  map[float64][]byte
		Strs map[string]string
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
		Strings  []string
		MoreNums []int
		Maps     maps
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
		Strings:  []string{"Astolfo", "Felix", "idk lol"},
		MoreNums: []int{1, 2, 3, 4, 2, 0, 6, 9, 10},
		Maps: maps{
			Uint: map[uint]int{
				0:      -1,
				100000: -100,
			},
			Int: map[int]uint{
				-1:   0,
				-100: 100000,
			},
			F64: map[float64][]byte{
				math.NaN():   []byte("NaN lol"),
				math.Inf(+1): []byte("infty"),
				math.Inf(-1): []byte("neg infty"),
				-0:           []byte("negative zero?!"),
			},
			Strs: map[string]string{
				"hello": "world",
				"felix": "argyle",
			},
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

	// make this function shorter for tests
	n := copyNumBytes

	kv.expect(t, "kvpack_test", "animals", map[string]string{
		f("Cats"):   "meow",
		f("Dogs"):   "woof",
		f("SoTrue"): "\x01",

		f("Extinct", "Dinosaurs"): "???",

		f("Numbers", "Byte"): "\x01",
		f("Numbers", "Int"):  n(int(math.MaxInt)),
		f("Numbers", "Uint"): n(uint(math.MaxUint)),
		f("Numbers", "I8"):   n(int8(math.MaxInt8)),
		f("Numbers", "I16"):  n(int16(math.MaxInt16)),
		f("Numbers", "I32"):  n(int32(math.MaxInt32)),
		f("Numbers", "I64"):  n(int64(math.MaxInt64)),
		f("Numbers", "U8"):   n(uint8(math.MaxUint8)),
		f("Numbers", "U16"):  n(uint16(math.MaxUint16)),
		f("Numbers", "U32"):  n(uint32(math.MaxUint32)),
		f("Numbers", "U64"):  n(uint64(math.MaxUint64)),
		f("Numbers", "F32"):  n(float32(math.MaxFloat32)),
		f("Numbers", "F64"):  n(float64(math.MaxFloat64)),
		f("Numbers", "C64"):  n(complex64(5 + 6i)),
		f("Numbers", "C128"): n(complex128(10 + 12i)),

		f("More", "Cats"): "nya nya",
		f("More", "Dogs"): "wan wan",

		f("Quirks", "BoolPtr"):   "\x00",
		f("Quirks", "StringPtr"): "",

		f("Strings", "l"):         n(int64(3)),
		f("Strings", n(int64(0))): "Astolfo",
		f("Strings", n(int64(1))): "Felix",
		f("Strings", n(int64(2))): "idk lol",

		f("MoreNums", "l"):         n(int64(9)),
		f("MoreNums", n(int64(0))): n(int(1)),
		f("MoreNums", n(int64(1))): n(int(2)),
		f("MoreNums", n(int64(2))): n(int(3)),
		f("MoreNums", n(int64(3))): n(int(4)),
		f("MoreNums", n(int64(4))): n(int(2)),
		f("MoreNums", n(int64(5))): n(int(0)),
		f("MoreNums", n(int64(6))): n(int(6)),
		f("MoreNums", n(int64(7))): n(int(9)),
		f("MoreNums", n(int64(8))): n(int(10)),

		f("Maps", "Uint", n(uint(0))):      n(int(-1)),
		f("Maps", "Uint", n(uint(100000))): n(int(-100)),

		f("Maps", "Int", n(int(-1))):   n(uint(0)),
		f("Maps", "Int", n(int(-100))): n(uint(100000)),

		f("Maps", "F64", n(float64(math.NaN()))):   "NaN lol",
		f("Maps", "F64", n(float64(math.Inf(+1)))): "infty",
		f("Maps", "F64", n(float64(math.Inf(-1)))): "neg infty",
		f("Maps", "F64", n(float64(-0))):           "negative zero?!",

		f("Maps", "Strs", "hello"): "world",
		f("Maps", "Strs", "felix"): "argyle",
	})
}

func copyNumBytes(v interface{}) string {
	typ, ptr := defract.UnderlyingPtr(v)
	kind := typ.Kind()

	switch kind {
	case reflect.Int, reflect.Uint:
		b := make([]byte, 10)
		n := 0
		switch kind {
		case reflect.Int:
			n = binary.PutVarint(b, int64(*(*int)(ptr)))
		case reflect.Uint:
			n = binary.PutUvarint(b, uint64(*(*uint)(ptr)))
		}
		return string(b[:n])
	}

	b := defract.NumberLE(kind, ptr)
	out := string(b.Bytes)
	b.Return()

	return out
}

func TestAppendKey(t *testing.T) {
	type expect struct {
		key   string
		extra string
	}
	type test struct {
		name   string
		buf    []byte
		key    []byte
		extra  int
		expect expect
	}

	var tests = []test{
		{
			name:   "no extra",
			buf:    []byte("hi"),
			key:    []byte("key"),
			extra:  0,
			expect: expect{"hi\x00key", ""},
		},
		{
			name:   "3 extra",
			buf:    []byte("hi"),
			key:    []byte("key"),
			extra:  3,
			expect: expect{"hi\x00key", "\x00\x00\x00"},
		},
		{
			name: "3 extra reuse",
			buf: func() []byte {
				buf := make([]byte, 128)
				copy(buf, "hi")
				// ignore 2
				// ignore 1 + 3
				copy(buf[6:], "lol")
				return buf[:2]
			}(),
			key:    []byte("key"),
			extra:  3,
			expect: expect{"hi\x00key", "lol"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			new, extra := appendKey(&test.buf, test.key, test.extra)
			if string(test.expect.key) != string(new) {
				t.Fatalf("key expected %q, got %q", test.expect.key, new)
			}
			if string(test.expect.extra) != string(extra) {
				t.Fatalf("extra expected %q, got %q", test.expect.extra, extra)
			}
		})
	}
}
