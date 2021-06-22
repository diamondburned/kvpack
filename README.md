# kvpack

Performant marshaler and unmarshaler library for transactional key-value
databases.

## Design

kvpack is meant to be simple in design, both for the end-user and the library
developer. Even though lots of care will be taken to optimize for performance
internally, the API surface should always be kept simple.

Because of this, kvpack will not read any struct tags. The API surface will
always be kept to a bare minimum.
