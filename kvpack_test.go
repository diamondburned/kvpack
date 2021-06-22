package kvpack

import (
	"errors"
	"reflect"
	"testing"
	"unsafe"

	"github.com/diamondburned/kvpack/driver"
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

	check := func(t *testing.T, v, expectTyp interface{}) {
		typ, got := underlyingPtr(v)
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

func (tx *mockTx) expect(t *testing.T, ns, key string, o map[string]string) {
	for k, v := range o {
		fullKey := string(Namespace + Separator + ns + Separator + key + Separator + k)

		got, ok := tx.v[fullKey]
		if !ok {
			t.Errorf("missing key %q", fullKey)
			continue
		}

		if got != v {
			t.Errorf("key %q value expected %q, got %q", fullKey, v, got)
			continue
		}

		delete(tx.v, fullKey)
	}

	for k := range tx.v {
		t.Errorf("excess key %q", k)
	}
}

func newTestTx(tx driver.Transaction, ns string) *Transaction {
	return NewTransaction(tx, Namespace+Separator+ns)
}

func TestTransactionStruct(t *testing.T) {
	type testStruct struct {
		Cats string
		Dogs string
		junk string
	}

	testValue := testStruct{
		Cats: "meow",
		Dogs: "woof",
		junk: "ignore me",
	}

	kv := newMockTx(1)
	tx := newTestTx(kv, "kvpack_test")

	if err := tx.Put([]byte("animals"), &testValue); err != nil {
		t.Fatal("failed to put:", err)
	}

	kv.expect(t, "kvpack_test", "animals", map[string]string{
		"Cats": "meow",
		"Dogs": "woof",
	})
}
