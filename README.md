# kvpack

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
