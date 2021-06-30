# kvpack

[![Go Reference](https://pkg.go.dev/badge/github.com/diamondburned/kvpack#Database.Each.svg)](https://pkg.go.dev/github.com/diamondburned/kvpack#Database.Each)
[![pipeline status](https://gitlab.com/diamondburned/kvpack/badges/unsafe-footgun/pipeline.svg)](https://gitlab.com/diamondburned/kvpack/-/commits/unsafe-footgun)
[![coverage report](https://gitlab.com/diamondburned/kvpack/badges/unsafe-footgun/coverage.svg)](https://gitlab.com/diamondburned/kvpack/-/commits/unsafe-footgun)

Performant marshaler and unmarshaler library for transactional key-value
databases.

## Design

kvpack is meant to be simple in design, both for the end-user and the library
developer. Even though lots of care will be taken to optimize for performance
internally, the API surface should always be kept simple.

Because of this, kvpack will not read any struct tags. The API surface will
always be kept to a bare minimum.

## Examples

See the [reference documentation, section Examples](https://pkg.go.dev/github.com/diamondburned/kvpack@v0.0.0-20210630041153-7e1be4d9e3ac#pkg-examples).

## Supported Types

The following kinds of types are supported:

- Slices
- Strings
- Booleans
- All numbers (uint?, int?, float?, complex?, etc.)
- Structs

The following kinds of types are **not** supported:

- Maps, because they're too complicated
- Arrays (TODO)

## Performance

Note that these benchmarks should only be used to relatively compare how much
performance is compromised by having the ability to arbitrarily get and set keys
inside a struct. If your application does not benefit from having this ability,
then it most likely doesn't need `kvpack`.

Performance of these benchmarks heavily depend on the underlying database
driver, the complexity of the data structure and the size of the value of that
structure. The benchmarks below use a fairly simple struct to compare between
`kvpack` and `encoding/json`. The benchmarks are taken from the GitLab CI
results.

```
pkg: github.com/diamondburned/kvpack/driver/bboltpack
cpu: Intel(R) Xeon(R) CPU @ 2.30GHz
BenchmarkSuite/PutKVPack         	   55287	     20924 ns/op	    9753 B/op	      62 allocs/op
BenchmarkSuite/PutJSON           	   54154	     21949 ns/op	    7513 B/op	      53 allocs/op
BenchmarkSuite/GetKVPack         	  312518	      3870 ns/op	    1039 B/op	      12 allocs/op
BenchmarkSuite/GetJSON           	  271411	      4639 ns/op	     404 B/op	      14 allocs/op
```

<details>
<summary>The data structure and value of the benchmark data</summary>

```go
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
```
</details>
