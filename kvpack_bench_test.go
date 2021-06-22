package kvpack

import (
	"bytes"
	"encoding/json"
	"testing"
)

type noopTx struct{}

func (tx noopTx) Commit() error   { return nil }
func (tx noopTx) Rollback() error { return nil }

func (tx noopTx) Put(k, v []byte) error {
	return nil
}

func (tx noopTx) Get(k []byte, fn func([]byte) error) error {
	return fn(nil)
}

type benchmarkStruct struct {
	BestCharacter  string
	CharacterScore string
}

var benchmarkValue = benchmarkStruct{
	BestCharacter:  "Astolfo",
	CharacterScore: "100",
}

func BenchmarkTransactionPutStruct(b *testing.B) {
	k := []byte("best_character")
	v := benchmarkValue

	tx := newTestTx(noopTx{}, "kvpack_test")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := tx.Put(k, &v); err != nil {
			b.Fatal("failed to put:", err)
		}
	}
}

func BenchmarkTransactionPutJSON(b *testing.B) {
	k := []byte("best_character")
	v := benchmarkValue

	tx := newTestTx(noopTx{}, "kvpack_test")

	var buf bytes.Buffer

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Reset()

		if err := json.NewEncoder(&buf).Encode(v); err != nil {
			b.Fatal("failed to encode:", err)
		}

		if err := tx.Put(k, &v); err != nil {
			b.Fatal("failed to put:", err)
		}
	}
}
