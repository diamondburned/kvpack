package tests

import (
	"encoding/json"
	"testing"

	"github.com/diamondburned/kvpack"
)

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

type benchmarker struct {
	b  *testing.B
	db *kvpack.Database
}

// DoBenchmark runs all benchmarks in the suite.
func DoBenchmark(b *testing.B, db *kvpack.Database) {
	ber := benchmarker{
		b:  b,
		db: db,
	}

	b.Run("PutKVPack", ber.benchmarkPutKVPack)
	b.Run("PutJSON", ber.benchmarkPutJSON)

	b.Run("GetKVPack", ber.benchmarkGetKVPack)
	b.Run("GetJSON", ber.benchmarkGetJSON)
}

func (ber *benchmarker) benchmarkPutKVPack(b *testing.B) {
	k := []byte("benchmark_put_kvpack")
	v := benchmarkValue

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := ber.db.Put(k, &v); err != nil {
			b.Fatal("failed to put:", err)
		}
	}
}

func (ber *benchmarker) benchmarkPutJSON(b *testing.B) {
	k := []byte("benchmark_put_json")
	v := benchmarkValue

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		j, err := json.Marshal(v)
		if err != nil {
			b.Fatal("failed to encode:", err)
		}

		if err := ber.db.Put(k, &j); err != nil {
			b.Fatal("failed to put:", err)
		}
	}
}

func (ber *benchmarker) benchmarkGetKVPack(b *testing.B) {
	k := []byte("benchmark_get_kvpack")
	v := benchmarkStruct{}

	if err := ber.db.Put(k, &benchmarkValue); err != nil {
		b.Fatal("failed to put:", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := ber.db.Get(k, &v); err != nil {
			b.Fatal("failed to get:", err)
		}
	}
}

func (ber *benchmarker) benchmarkGetJSON(b *testing.B) {
	k := []byte("benchmark_get_json")
	v := benchmarkStruct{}

	jsonData, err := json.Marshal(&benchmarkValue)
	if err != nil {
		b.Fatal("failed to encode:", err)
	}

	if err := ber.db.Put(k, &jsonData); err != nil {
		b.Fatal("failed to put:", err)
	}

	var output []byte

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := ber.db.Get(k, &output); err != nil {
			b.Fatal("failed to get:", err)
		}

		if err := json.Unmarshal(output, &v); err != nil {
			b.Fatal("failed to decode:", err)
		}
	}
}
