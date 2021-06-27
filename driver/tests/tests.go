// Package tests provides a test suite and a benchmark suite for any driver.
package tests

import (
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/diamondburned/kvpack"
	"github.com/diamondburned/kvpack/defract"
	"github.com/go-test/deep"
)

// suite is the test suite.
type suite struct {
	db *kvpack.Database
}

// DoTests runs all tests in the suite.
func DoTests(t *testing.T, db *kvpack.Database) {
	s := suite{
		db: db,
	}

	// Be sure to restore the endianness once we're done.
	wasLittleEndian := defract.IsLittleEndian
	t.Cleanup(func() { defract.IsLittleEndian = wasLittleEndian })

runTest:
	t.Run("Put", func(t *testing.T) {
		t.Run("ptr", s.testPutPtr)
		t.Run("value", s.testPutValue)
	})
	t.Run("Get", s.testGet)

	// Run the test twice if we're on a Little-Endian machine.
	if defract.IsLittleEndian {
		defract.IsLittleEndian = false
		goto runTest
	}
}

type extinct struct {
	Dinosaurs string // raw bytes
	Dodo      string // raw bytes
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
	Extincts []extinct
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
	junk     string `deep:"-"` // unexpected so ignore
}

func newTestValue() animals {
	return animals{
		Extincts: []extinct{
			{Dinosaurs: "???"},
			{Dodo: "???"},
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
				// Don't test NaN; it's always unequal, so it's the most useless
				// map key.
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
}

func (s suite) testPutPtr(t *testing.T) {
	testValue := newTestValue()

	if err := s.db.Put([]byte("put_ptr"), &testValue); err != nil {
		t.Error("failed to put &testValue:", err)
	}
	s.testExpect(t, "put_ptr")
}

func (s suite) testPutValue(t *testing.T) {
	testValue := newTestValue()

	if err := s.db.Put([]byte("put_value"), testValue); err != nil {
		t.Error("failed to put testValue:", err)
	}
	s.testExpect(t, "put_value")
}

func (s suite) testExpect(t *testing.T, key string) {
	t.Helper()

	// n is a helper function for transforming numbers.
	n := func(v interface{}) string {
		typ, ptr := defract.UnderlyingPtr(v)
		kind := typ.Kind()

		switch kind {
		case reflect.Int:
			return string(defract.IntLE((*int)(ptr)))
		case reflect.Uint:
			return string(defract.UintLE((*uint)(ptr)))
		}

		return string(defract.NumberLE(kind, ptr))
	}

	s.Expect(t, key, map[string]string{
		"Cats":   "meow",
		"Dogs":   "woof",
		"SoTrue": "\x01",

		"Extincts":             n(int(2)),
		"Extincts.0":           "",
		"Extincts.0.Dinosaurs": "???",
		"Extincts.1":           "",
		"Extincts.1.Dodo":      "???",

		"Numbers":      "",
		"Numbers.Byte": "\x01",
		"Numbers.Int":  n(int(math.MaxInt)),
		"Numbers.Uint": n(uint(math.MaxUint)),
		"Numbers.I8":   n(int8(math.MaxInt8)),
		"Numbers.I16":  n(int16(math.MaxInt16)),
		"Numbers.I32":  n(int32(math.MaxInt32)),
		"Numbers.I64":  n(int64(math.MaxInt64)),
		"Numbers.U8":   n(uint8(math.MaxUint8)),
		"Numbers.U16":  n(uint16(math.MaxUint16)),
		"Numbers.U32":  n(uint32(math.MaxUint32)),
		"Numbers.U64":  n(uint64(math.MaxUint64)),
		"Numbers.F32":  n(float32(math.MaxFloat32)),
		"Numbers.F64":  n(float64(math.MaxFloat64)),
		"Numbers.C64":  n(complex64(5 + 6i)),
		"Numbers.C128": n(complex128(10 + 12i)),

		"More":      "",
		"More.Cats": "nya nya",
		"More.Dogs": "wan wan",

		"Quirks":           "",
		"Quirks.BoolPtr":   "\x00",
		"Quirks.StructPtr": "",
		"Quirks.StringPtr": "",

		"Strings":   n(int64(3)),
		"Strings.0": "Astolfo",
		"Strings.1": "Felix",
		"Strings.2": "idk lol",

		"MoreNums":   n(int64(9)),
		"MoreNums.0": n(int(1)),
		"MoreNums.1": n(int(2)),
		"MoreNums.2": n(int(3)),
		"MoreNums.3": n(int(4)),
		"MoreNums.4": n(int(2)),
		"MoreNums.6": n(int(6)),
		"MoreNums.7": n(int(9)),
		"MoreNums.8": n(int(10)),

		"Maps":             "",
		"Maps.Uint":        n(int64(2)),
		"Maps.Uint.0":      n(int(-1)),
		"Maps.Uint.100000": n(int(-100)),
		"Maps.Int":         n(int64(2)),
		"Maps.Int.-1":      n(uint(0)),
		"Maps.Int.-100":    n(uint(100000)),
		"Maps.F64":         n(uint64(3)),
		"Maps.F64.+Inf":    "infty",
		"Maps.F64.-Inf":    "neg infty",
		"Maps.F64.0":       "negative zero?!",
		"Maps.Strs":        n(int64(2)),
		"Maps.Strs.hello":  "world",
		"Maps.Strs.felix":  "argyle",
	})
}

func (s suite) testGet(t *testing.T) {
	accessAssert := func(key string, unmarshal, expect interface{}) {
		t.Helper()

		if err := s.db.Access(key, unmarshal); err != nil {
			t.Fatalf("failed to access %q: %v", key, err)
		}

		if ineqs := deep.Equal(expect, unmarshal); ineqs != nil {
			for _, ineq := range ineqs {
				t.Errorf("expect != got: %q", ineq)
			}
		}
	}

	expect := newTestValue()

	var gotValue1 animals
	accessAssert("put_ptr", &gotValue1, &expect)
	var gotValue2 animals
	accessAssert("put_value", &gotValue2, &expect)

	var gotMoreNums1 numbers
	accessAssert("put_ptr.Numbers", &gotMoreNums1, &expect.Numbers)
	var gotMoreNums2 numbers
	accessAssert("put_value.Numbers", &gotMoreNums2, &expect.Numbers)

	expectNthNum := 3
	var nthNum1 int
	accessAssert("put_ptr.MoreNums.2", &nthNum1, &expectNthNum)
	var nthNum2 int
	accessAssert("put_value.MoreNums.2", &nthNum2, &expectNthNum)
}

// makeKey makes a full key from the given two parts.
func (s suite) makeKey(midKey, tailKey string) string {
	parts := []string{
		s.db.Namespace(),
		midKey,
		strings.ReplaceAll(tailKey, ".", kvpack.Separator),
	}

	return strings.Join(parts, kvpack.Separator)
}

// Expect verifies that the test suite's database contains the given data in the
// o map of the key.
func (s suite) Expect(t *testing.T, key string, o map[string]string) {
	t.Helper()

	rootKey := s.db.Namespace() + kvpack.Separator + key
	dump := make(map[string]string)

	// Dump the whole database out.
	if err := s.db.View(func(tx *kvpack.Transaction) error {
		return tx.Tx.Iterate([]byte(rootKey), func(k, v []byte) error {
			dump[string(k)] = string(v)
			return nil
		})
	}); err != nil {
		t.Error("failed to dump db:", err)
		return
	}

	// Expect the root key.
	if _, ok := dump[rootKey]; !ok {
		t.Errorf("missing root key %q", rootKey)
		return
	}
	delete(dump, rootKey)

	for k, v := range o {
		fullKey := s.makeKey(key, k)

		got, ok := dump[fullKey]
		if !ok {
			t.Errorf("missing key %q value %q", fullKey, v)
			continue
		}

		if v != got {
			t.Errorf("key %q value expected %q, got %q", fullKey, v, got)
			continue
		}

		delete(dump, fullKey)
		delete(o, k)
	}

	for k, v := range dump {
		t.Errorf("excess key %q value %q", k, v)
	}
}
