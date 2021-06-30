package kvpack

import (
	"errors"
	"strings"
	"testing"

	"github.com/diamondburned/kvpack/driver"
)

type miniTx struct {
	v map[string]string
}

func (tx miniTx) Commit() error   { return nil }
func (tx miniTx) Rollback() error { return nil }

func (tx miniTx) Put(k, v []byte) error {
	tx.v[string(k)] = string(v)
	return nil
}

func (tx miniTx) Get(k []byte) ([]byte, error) {
	v, ok := tx.v[string(k)]
	if ok {
		return []byte(v), nil
	}
	return nil, driver.ErrKeyNotFound
}

func (tx miniTx) Iterate(prefix []byte, fn func(k, v []byte) error) error {
	return nil
}

func (tx miniTx) DeletePrefix(prefix []byte) error {
	for k := range tx.v {
		if strings.HasPrefix(k, string(prefix)) {
			delete(tx.v, k)
		}
	}
	return nil
}

func TestNilParams(t *testing.T) {
	mn := miniTx{v: map[string]string{
		"a\x00key":        "hello",
		"a\x00key\x002":   "world",
		"a\x00other":      "a",
		"a\x00other\x002": "b",
	}}
	tx := NewTransaction(mn, []byte("a"), false)

	if err := tx.Put([]byte("key"), nil); !errors.Is(err, ErrValueNeedsPtr) {
		t.Fatal("unexpected error putting nil value:", err)
	}

	if _, ok := mn.v["key"]; ok {
		t.Fatal("key still exists after Put nil")
	}

	// Put any typed nil value.
	if err := tx.Put([]byte("other"), (*miniTx)(nil)); !errors.Is(err, ErrValueNeedsPtr) {
		t.Fatal("unexpected error putting typed nil value:", err)
	}

	if _, ok := mn.v["other"]; ok {
		t.Fatal("key still exists after Put typed nil")
	}
}

func TestRO(t *testing.T) {
	db := miniTx{make(map[string]string)}
	tx := NewTransaction(db, nil, true)

	mustErrIs(t, tx.Put([]byte("a"), []byte("b")), ErrReadOnly.Error())
	if len(db.v) > 0 {
		t.Fatal("tx modified during put while read-only")
	}

	db.v["a"] = "b"
	mustErrIs(t, tx.Delete([]byte("a")), ErrReadOnly.Error())
	if len(db.v) == 0 {
		t.Fatal("tx modified during delete while read-only")
	}

	mustErrIs(t, tx.Commit(), ErrReadOnly.Error())
}

func mustErrIs(t *testing.T, got error, expect string) {
	t.Helper()

	if got == nil {
		t.Fatal("got unexpected nil error")
	}
	if !strings.Contains(got.Error(), expect) {
		t.Fatalf("unknown error received: %q", got)
	}
}
