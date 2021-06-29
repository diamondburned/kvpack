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

type Benchmarker struct {
	tb testing.TB
	db *kvpack.Database
}

func NewBenchmarker(tb testing.TB, db *kvpack.Database) Benchmarker {
	return Benchmarker{tb, db}
}

// DoBenchmark runs all benchmarks in the suite.
func DoBenchmark(b *testing.B, db *kvpack.Database) {
	ber := NewBenchmarker(b, db)

	b.Run("PutKVPack", ber.BenchmarkPutKVPack)
	b.Run("PutJSON", ber.BenchmarkPutJSON)

	b.Run("GetKVPack", ber.BenchmarkGetKVPack)
	b.Run("GetJSON", ber.BenchmarkGetJSON)
}

func (ber Benchmarker) BenchmarkPutKVPack(b *testing.B) {
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

func (ber Benchmarker) BenchmarkPutJSON(b *testing.B) {
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

func (ber Benchmarker) BenchmarkGetKVPack(b *testing.B) {
	k := []byte("benchmark_get_kvpack")

	if err := ber.db.Put(k, &benchmarkValue); err != nil {
		b.Fatal("failed to put:", err)
	}

	v := benchmarkStruct{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := ber.db.Get(k, &v); err != nil {
			b.Fatal("failed to get:", err)
		}
	}
}

func (ber Benchmarker) BenchmarkGetJSON(b *testing.B) {
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
