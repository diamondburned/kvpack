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

func (tx noopTx) Iterate(prefix []byte, fn func(k, v []byte) error) error {
	return nil
}

func (tx noopTx) DeletePrefix(prefix []byte) error {
	return nil
}

type benchmarkStruct struct {
	BestCharacter   string
	CharacterScore  int32
	OtherCharacters []benchmarkStruct
}

var benchmarkValue = benchmarkStruct{
	BestCharacter:  "Astolfo",
	CharacterScore: 100,
	OtherCharacters: []benchmarkStruct{
		{"Felix Argyle", 100, nil},
		{"Hime Arikawa", 100, nil},
	},
}

func BenchmarkPutKVPack(b *testing.B) {
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

func BenchmarkPutJSON(b *testing.B) {
	k := []byte("best_character")
	v := benchmarkValue

	db := noopTx{}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Reset()

		if err := enc.Encode(v); err != nil {
			b.Fatal("failed to encode:", err)
		}

		if err := db.Put(k, buf.Bytes()); err != nil {
			b.Fatal("failed to put:", err)
		}
	}
}

type benchmarkMapType struct {
	Name string
	Data map[string]string
}

func newBenchmarkMapSample() *benchmarkMapType {
	return &benchmarkMapType{
		Name: "Astolfo",
		Data: map[string]string{
			"gender": "???",
			"height": "164cm",
			"weight": "56kg",
		},
	}
}

func BenchmarkPutKVPackMap(b *testing.B) {
	k := []byte("best_character")
	v := newBenchmarkMapSample()

	tx := newTestTx(noopTx{}, "kvpack_test")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := tx.Put(k, &v); err != nil {
			b.Fatal("failed to put:", err)
		}
	}
}

func BenchmarkPutJSONMap(b *testing.B) {
	k := []byte("best_character")
	v := newBenchmarkMapSample()

	db := noopTx{}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Reset()

		if err := enc.Encode(v); err != nil {
			b.Fatal("failed to encode:", err)
		}

		if err := db.Put(k, buf.Bytes()); err != nil {
			b.Fatal("failed to put:", err)
		}
	}
}
