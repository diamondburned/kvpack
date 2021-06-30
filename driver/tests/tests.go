// Package tests provides a test suite and a benchmark suite for any driver.
package tests

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/diamondburned/kvpack"
	"github.com/diamondburned/kvpack/defract"
	"github.com/diamondburned/kvpack/driver"
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
	t.Run("Override", s.testOverride)
	t.Run("Delete", s.testDelete)
	t.Run("DeleteFields", s.testDeleteFields)
	t.Run("Each", s.testEach)

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
	Bytes    []byte
	MoreNums []int
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
		Bytes:    []byte("<3Astolfo"),
		MoreNums: []int{1, 2, 3, 4, 2, 0, 6, 9, 10},
		junk:     "ignore me",
	}
}

func (s suite) testPutPtr(t *testing.T) {
	testValue := newTestValue()

	// Test db.Put.
	if err := s.db.Put([]byte("put_ptr_1"), &testValue); err != nil {
		t.Error("failed to put &testValue using db.Put:", err)
	}
	s.testExpect(t, "put_ptr_1")

	// Test db.Update.
	if err := s.db.Update(func(tx *kvpack.Transaction) error {
		return tx.Put([]byte("put_ptr_2"), &testValue)
	}); err != nil {
		t.Error("failed to put &testValue using db.Update:", err)
	}
	s.testExpect(t, "put_ptr_2")

	testStr := "Hello, world"
	if err := s.db.Put([]byte("put_str"), &testStr); err != nil {
		t.Error("failed to put str ptr:", err)
	}
}

func (s suite) testPutValue(t *testing.T) {
	testValue := newTestValue()

	if err := s.db.Put([]byte("put_value"), testValue); !errors.Is(err, kvpack.ErrValueNeedsPtr) {
		t.Fatal("unexpected error putting value:", err)
	}
}

func (s suite) testExpect(t *testing.T, key string) {
	t.Helper()

	// n is a helper function for transforming numbers.
	n := func(v interface{}) string {
		ptr := defract.InterfacePtr(v)
		typ := reflect.TypeOf(v)
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
		"Extincts.0":           defract.GetStructSchema(extinct{}),
		"Extincts.0.Dinosaurs": "???",
		"Extincts.1":           defract.GetStructSchema(extinct{}),
		"Extincts.1.Dodo":      "???",

		"Numbers":      defract.GetStructSchema(numbers{}),
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

		"More":      defract.GetStructSchema(animals{}),
		"More.Cats": "nya nya",
		"More.Dogs": "wan wan",

		"Quirks":           defract.GetStructSchema(quirks{}),
		"Quirks.BoolPtr":   "\x00",
		"Quirks.StructPtr": "",
		"Quirks.StringPtr": "",

		"Strings":   n(int64(3)),
		"Strings.0": "Astolfo",
		"Strings.1": "Felix",
		"Strings.2": "idk lol",

		"Bytes": "<3Astolfo",

		"MoreNums":   n(int64(9)),
		"MoreNums.0": n(int(1)),
		"MoreNums.1": n(int(2)),
		"MoreNums.2": n(int(3)),
		"MoreNums.3": n(int(4)),
		"MoreNums.4": n(int(2)),
		"MoreNums.6": n(int(6)),
		"MoreNums.7": n(int(9)),
		"MoreNums.8": n(int(10)),
	})
}

func (s suite) testGet(t *testing.T) {
	accessAssert := func(key string, unmarshal, expect interface{}) {
		t.Helper()

		if err := s.db.GetFields(key, unmarshal); err != nil {
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
	accessAssert("put_ptr_1", &gotValue1, &expect)
	var gotValue2 animals
	accessAssert("put_ptr_2", &gotValue2, &expect)

	var gotMoreNums1 numbers
	accessAssert("put_ptr_1.Numbers", &gotMoreNums1, &expect.Numbers)
	var gotMoreNums2 numbers
	accessAssert("put_ptr_2.Numbers", &gotMoreNums2, &expect.Numbers)

	expectNthNum := 3
	var nthNum1 int
	accessAssert("put_ptr_1.MoreNums.2", &nthNum1, &expectNthNum)
	var nthNum2 int
	accessAssert("put_ptr_2.MoreNums.2", &nthNum2, &expectNthNum)

	var catOutput1 string
	accessAssert("put_ptr_1.Cats", &catOutput1, &expect.Cats)
	var catOutput2 string
	accessAssert("put_ptr_2.Cats", &catOutput2, &expect.Cats)

	stringsOut1 := make([]string, 3)
	accessAssert("put_ptr_1.Strings", &stringsOut1, &expect.Strings)
	stringsOut2 := make([]string, 3)
	accessAssert("put_ptr_2.Strings", &stringsOut2, &expect.Strings)

	toBytesPtr := func(s string) *[]byte {
		v := []byte(s)
		return &v
	}
	var catOutput3 []byte
	accessAssert("put_ptr_1.Cats", &catOutput3, toBytesPtr(expect.Cats))
	var catOutput4 []byte
	accessAssert("put_ptr_2.Cats", &catOutput4, toBytesPtr(expect.Cats))

	var gotStr string
	expectStr := "Hello, world"
	accessAssert("put_str", &gotStr, &expectStr)
}

// dummyTestType is a made-up new type to ensure that things are cleaned up
// properly.
type dummyTestType struct {
	Nothing string
}

func (s suite) testOverride(t *testing.T) {
	dummyValue := dummyTestType{"absolutely nothing"}
	type input struct {
		key string
		val interface{}
	}

	inputs := []input{{"put_ptr_1", &dummyValue}, {"put_value_2", &dummyValue}}

	for _, input := range inputs {
		if err := s.db.Put([]byte(input.key), input.val); err != nil {
			t.Error("failed to override put_ptr:", err)
		}

		s.Expect(t, input.key, map[string]string{
			"Nothing": "absolutely nothing",
		})
	}
}

func (s suite) testDelete(t *testing.T) {
	if err := s.db.Put([]byte("delete_testkey"), []byte("a")); err != nil {
		t.Fatal("failed to put:", err)
	}

	var out string
	if err := s.db.Get([]byte("delete_testkey"), &out); err != nil {
		t.Fatal("failed to get what's put:", err)
	}

	if out != "a" {
		t.Fatalf("unexpected output: %q", out)
	}

	if err := s.db.Delete([]byte("delete_testkey")); err != nil {
		t.Fatal("failed to delete:", err)
	}

	values := []interface{}{
		// Special cases.
		new(string),
		new([]byte),
		// Non-special cases.
		new(int),
	}

	for _, value := range values {
		// Check the special-case path.
		err := s.db.Get([]byte("delete_testkey"), value)
		if !errors.Is(err, kvpack.ErrKeyNotFound) {
			t.Fatal("unexpected error getting deleted key:", err)
		}

		if elem := reflect.ValueOf(value).Elem(); !elem.IsZero() {
			t.Fatalf("unexpected value after deletion: %#v", elem.Interface())
		}
	}

	// Check the non-special-case path.
}

type superNestedStruct struct {
	Wow struct {
		Much struct {
			Nested struct {
				Such struct {
					Wow string
				}
			}
		}
	}
}

func (s suite) testDeleteFields(t *testing.T) {
	var nested superNestedStruct
	nested.Wow.Much.Nested.Such.Wow = "doge meme"

	if err := s.db.Put([]byte("deletefields_testkey"), &nested); err != nil {
		t.Fatal("failed to put:", err)
	}

	var out superNestedStruct
	if err := s.db.Get([]byte("deletefields_testkey"), &out); err != nil {
		t.Fatal("failed to get what's put:", err)
	}

	if nested != out {
		t.Fatalf("unexpected output: %#v", out)
	}

	// Delete the deeply nested field. Since this was the only field that didn't
	// have a non-zero-value, deleting this field will effectively clean up the
	// whole struct in the database. This is assuming the database is
	// implemented as a flat key-value store, however. If it's not, then Get
	// will return a nil error without filling up the value. We have to account
	// for both cases.
	if err := s.db.DeleteFields("deletefields_testkey.Wow.Much.Nested.Such"); err != nil {
		t.Fatal("failed to delete:", err)
	}

	out = superNestedStruct{} // reset
	if err := s.db.Get([]byte("deletefields_testkey"), &out); err == nil {
		// Case 1: if the database is not implemented as a flat key-value store.
		if out.Wow.Much.Nested.Such.Wow != "" {
			t.Fatalf("value remains in output: %#v", out)
		}
	} else {
		// Case 2: if the database is implemented as a flat key-value store.
		if !errors.Is(err, kvpack.ErrKeyNotFound) {
			t.Fatal("unexpected error getting deleted key:", err)
		}
	}
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

// TODO Dumper interface

// // Dumper is an interface that databases can satisfy to dump the database.
// type Dumper interface {
// 	Dump(prefix []byte, v map[string]string) error
// }

// Expect verifies that the test suite's database contains the given data in the
// o map of the key.
func (s suite) Expect(t *testing.T, key string, o map[string]string) {
	t.Helper()

	key = strings.ReplaceAll(key, ".", kvpack.Separator)

	rootKey := s.db.Namespace() + kvpack.Separator + key

	tx, err := s.db.Begin(true)
	if err != nil {
		t.Error("failed to begin RO tx in Expect:", err)
	}
	defer tx.Rollback()

	// Expect the root key.
	if _, err := tx.Tx.Get([]byte(rootKey)); err != nil {
		if errors.Is(err, driver.ErrKeyNotFound) {
			t.Errorf("missing root key %q", rootKey)
			return
		}

		t.Fatal("unexpected error getting root key:", err)
	}

	for k, v := range o {
		fullKey := s.makeKey(key, k)

		b, err := tx.Tx.Get([]byte(fullKey))
		if err != nil {
			if errors.Is(err, driver.ErrKeyNotFound) {
				t.Errorf("missing key %q", fullKey)
				continue
			}

			t.Fatalf("unexpected error getting key %q: %v", b, err)
		}

		if v != string(b) {
			t.Errorf("key %q value expected %q, got %q", fullKey, v, b)
			continue
		}

		// delete(o, k)
	}
}

func (s suite) testEach(t *testing.T) {
	mustPut := func(k string, v interface{}) {
		if err := s.db.PutFields(k, v); err != nil {
			t.Fatalf("failed to put key %q: %v", k, err)
		}
	}

	mustPut("each.extincts", "") // satisfy s.Expect
	mustPut("each.extincts.dinosaurs", &extinct{Dinosaurs: "???"})
	mustPut("each.extincts.dodo", &extinct{Dodo: "???"})

	s.Expect(t, "each.extincts", map[string]string{
		"dinosaurs":           defract.GetStructSchema(extinct{}),
		"dinosaurs.Dinosaurs": "???",

		"dodo":      defract.GetStructSchema(extinct{}),
		"dodo.Dodo": "???",
	})

	expects := map[string]extinct{
		"dinosaurs": extinct{Dinosaurs: "???"},
		"dodo":      extinct{Dodo: "???"},
	}

	var dst extinct
	err := s.db.Each("each.extincts", &dst, func(k []byte) (br bool) {
		expect, ok := expects[string(k)]
		if !ok {
			t.Fatalf("unexpected Each key %q", k)
			return true
		}

		if expect != dst {
			t.Errorf("unexpected dst\n"+
				"expected %#v\n"+
				"got      %#v", expect, dst)
		}

		return false
	})

	if err != nil {
		t.Fatal("failed to Each:", err)
	}
}
