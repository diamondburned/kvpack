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
	defer tx.Rollback()

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

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Realistically, this would be done in a transaction, and the
		// transaction will own the byte slices, so we cannot reuse the same
		// buffer for each value.
		j, err := json.Marshal(v)
		if err != nil {
			b.Fatal("failed to encode:", err)
		}

		if err := db.Put(k, j); err != nil {
			b.Fatal("failed to put:", err)
		}
	}
}

func BenchmarkGetKVPack(b *testing.B) {
	k := []byte("best_character")
	v := benchmarkStruct{}

	db := newMockTx(nil, 1)
	tx := newTestTx(db, "kvpack_test")
	defer tx.Rollback()

	if err := tx.Put(k, benchmarkValue); err != nil {
		b.Fatal("failed to prep: put:", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := tx.Get(k, &v); err != nil {
			b.Fatal("failed to get:", err)
		}
	}
}

func BenchmarkGetJSON(b *testing.B) {
	k := []byte("best_character")
	v := benchmarkStruct{}
	db := newMockTx(nil, 1)

	jsonData, _ := json.Marshal(benchmarkValue)
	db.Put(k, jsonData)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := db.Get(k, func(b []byte) error {
			return json.Unmarshal(b, &v)
		}); err != nil {
			b.Fatal("failed to decode:", err)
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
