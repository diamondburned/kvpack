package tests

import (
	"encoding/json"
	"testing"

	"github.com/diamondburned/kvpack"
	"github.com/pkg/errors"
)

// CharacterData is a struct used for benchmarking.
type CharacterData struct {
	BestCharacter   string
	CharacterScore  int32
	OtherCharacters []CharacterData
}

// NewCharacterData creates a character data value with dummy values.
func NewCharacterData() CharacterData {
	return CharacterData{
		BestCharacter:  "Astolfo",
		CharacterScore: 100,
		OtherCharacters: []CharacterData{
			{"Felix Argyle", 100, nil},
			{"Hime Arikawa", 100, nil},
		},
	}
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

func (ber Benchmarker) mustTx(b *testing.B, ro bool, f func(tx *kvpack.Transaction) error) {
	var err error
	if ro {
		err = ber.db.View(f)
	} else {
		err = ber.db.Update(f)
	}
	if err != nil {
		b.Fatal("tx fail:", err)
	}
}

func (ber Benchmarker) BenchmarkPutKVPack(b *testing.B) {
	k := []byte("benchmark_put_kvpack")
	v := NewCharacterData()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		ber.mustTx(b, false, func(tx *kvpack.Transaction) error {
			for pb.Next() {
				if err := tx.Put(k, &v); err != nil {
					return errors.Wrap(err, "failed to put")
				}
			}
			return nil
		})
	})
}

func (ber Benchmarker) BenchmarkPutJSON(b *testing.B) {
	k := []byte("benchmark_put_json")
	v := NewCharacterData()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		ber.mustTx(b, false, func(tx *kvpack.Transaction) error {
			for pb.Next() {
				j, err := json.Marshal(v)
				if err != nil {
					return errors.Wrap(err, "failed to encode")
				}
				if err := tx.Put(k, &j); err != nil {
					return errors.Wrap(err, "failed to put")
				}
			}
			return nil
		})
	})
}

func (ber Benchmarker) BenchmarkGetKVPack(b *testing.B) {
	k := []byte("benchmark_get_kvpack")
	v := NewCharacterData()

	if err := ber.db.Put(k, &v); err != nil {
		b.Fatal("failed to put:", err)
	}

	into := CharacterData{}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		ber.mustTx(b, true, func(tx *kvpack.Transaction) error {
			for pb.Next() {
				if err := tx.Get(k, &into); err != nil {
					return errors.Wrap(err, "failed to get")
				}
			}
			return nil
		})
	})
}

func (ber Benchmarker) BenchmarkGetJSON(b *testing.B) {
	k := []byte("benchmark_get_json")
	v := NewCharacterData()

	jsonData, err := json.Marshal(&v)
	if err != nil {
		b.Fatal("failed to encode:", err)
	}

	if err := ber.db.Put(k, &jsonData); err != nil {
		b.Fatal("failed to put:", err)
	}

	var output []byte

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		ber.mustTx(b, true, func(tx *kvpack.Transaction) error {
			for pb.Next() {
				if err := tx.Get(k, &output); err != nil {
					return errors.Wrap(err, "failed to get")
				}
				if err := json.Unmarshal(output, &v); err != nil {
					return errors.Wrap(err, "failed to decode")
				}
			}
			return nil
		})
	})
}
