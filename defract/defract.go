// Package defract is a reflect-like package that utilizes heavy caching with
// unsafe to improve its performance.
package defract

import (
	"encoding/binary"
	"reflect"
	"sync"
	"unsafe"

	"golang.org/x/sync/singleflight"
)

// IsLittleEndian is true if the current machine is a Little-Endian machine.
var IsLittleEndian bool

var _ = initByteOrder()

// https://groups.google.com/g/golang-nuts/c/CTZ1I7BWiF8
//
// Code apparently taken from /x/net/ipv4/helper.go.
func initByteOrder() struct{} {
	i := uint32(1)
	b := (*[4]byte)(unsafe.Pointer(&i))
	IsLittleEndian = (b[0] == 1)
	return struct{}{}
}

// ByteSlice is the reflect.Type value for a byte slice.
var ByteSlice = reflect.TypeOf([]byte(nil))

var intBufferPool = sync.Pool{
	New: func() interface{} { return make([]byte, 10) },
}

// BorrowedBytes is a type that wraps around a byte slice to allow the caller to
// borrow it. The caller MUST return those borrowed values using the Return
// method.
type BorrowedBytes struct {
	Bytes []byte
	taken bool // true if pooled
}

// Return returns the borrowed bytes back to the internal pool.
func (b *BorrowedBytes) Return() {
	if b.taken {
		// Reset the length of the buffer and put it back.
		buf := b.Bytes[:10]
		intBufferPool.Put(buf)
		// Take the returned buffer away from the caller.
		b.Bytes = nil
	}
}

// Varint is a helper function that converts an integer into a byte slice to be
// used as a database key.
func Varint(i int64) BorrowedBytes {
	b := intBufferPool.Get().([]byte)

	uvarint := b[:binary.PutVarint(b, i)]
	return BorrowedBytes{uvarint, true}
}

// Uvarint is a helper function that converts an unsigned integer into a byte
// slice to be used as a database key.
func Uvarint(u uint64) BorrowedBytes {
	b := intBufferPool.Get().([]byte)

	varint := b[:binary.PutUvarint(b, u)]
	return BorrowedBytes{varint, true}
}

// Int64LE is a helper function that converts the given int64 value into bytes,
// ideally without copying on a little-endian machine. The bytes are always in
// little-endian.
func Int64LE(i int64) BorrowedBytes {
	if IsLittleEndian {
		return BorrowedBytes{(*[8]byte)(unsafe.Pointer(&i))[:], false}
	}

	b := intBufferPool.Get().([]byte)
	binary.LittleEndian.PutUint64(b, uint64(i))
	return BorrowedBytes{b[:8], true}
}

// Uint64LE is a helper function that converts the given uint64 value into
// bytes, ideally without copying on a little-endian machine. The bytes are
// always in little-endian.
func Uint64LE(i uint64) BorrowedBytes {
	if IsLittleEndian {
		return BorrowedBytes{(*[8]byte)(unsafe.Pointer(&i))[:], false}
	}

	b := intBufferPool.Get().([]byte)
	binary.LittleEndian.PutUint64(b, uint64(i))
	return BorrowedBytes{b[:8], true}
}

// WriteInt64LE writes the given int64 into the given byte slice.
func WriteInt64LE(dst []byte, i int64) {
	if IsLittleEndian {
		// Budget SIMD lol.
		*(*int64)(unsafe.Pointer(&dst[0])) = i
	} else {
		binary.LittleEndian.PutUint64(dst, uint64(i))
	}
}

// NumberLE returns the little-endian variant of the given number at the
// pointer. It does not handle variable-length integers, and is meant only for
// other statically sized integer or floating-point types.
//
// If this method is called on a Big Endian macnine, then a new byte buffer will
// be allocated from the pool.
func NumberLE(kind reflect.Kind, ptr unsafe.Pointer) BorrowedBytes {
	if !IsLittleEndian {
		return numberAsLE(kind, ptr)
	}

	switch kind {
	case reflect.Uint8, reflect.Int8:
		return BorrowedBytes{(*[1]byte)(ptr)[:], false}
	case reflect.Uint16, reflect.Int16:
		return BorrowedBytes{(*[2]byte)(ptr)[:], false}
	case reflect.Uint32, reflect.Int32, reflect.Float32:
		return BorrowedBytes{(*[4]byte)(ptr)[:], false}
	case reflect.Uint64, reflect.Int64, reflect.Float64, reflect.Complex64:
		return BorrowedBytes{(*[8]byte)(ptr)[:], false}
	case reflect.Complex128:
		return BorrowedBytes{(*[16]byte)(ptr)[:], false}
	default:
		panic("NumberLE got unsupported kind " + kind.String())
	}
}

// numberAsLE is the slow path. It tries not to allocate by having an internal
// byte buffer pool.
func numberAsLE(kind reflect.Kind, ptr unsafe.Pointer) BorrowedBytes {
	switch kind {
	case reflect.Uint8, reflect.Int8:
		// A single byte is architecture-independent.
		return BorrowedBytes{(*[1]byte)(ptr)[:], false}
	}

	b := intBufferPool.Get().([]byte)
	defer intBufferPool.Put(b)

	switch kind {
	case reflect.Uint16, reflect.Int16:
		binary.LittleEndian.PutUint16(b, *(*uint16)(ptr))
		return BorrowedBytes{b[:2], true}
	case reflect.Uint32, reflect.Int32, reflect.Float32:
		binary.LittleEndian.PutUint32(b, *(*uint32)(ptr))
		return BorrowedBytes{b[:4], true}
	case reflect.Uint64, reflect.Int64, reflect.Float64, reflect.Complex64:
		binary.LittleEndian.PutUint64(b, *(*uint64)(ptr))
		return BorrowedBytes{b[:8], true}
	case reflect.Complex128:
		panic("Complex128 unsupported on Big Endian (TODO)")
	default:
		panic("numberAsLE got unsupported kind " + kind.String())
	}
}

// zeroes might just be one of my worst hacks to date.
var zeroes [102400]byte // 10KB

// IsZero returns true if the value that the pointer points to is nil. For
// benchmarks, see defract_bench_test.go
func IsZero(ptr unsafe.Pointer, size uintptr) bool {
	rawValue := unsafe.Slice((*byte)(ptr), size)

	if size < 102400 {
		// Fast path that utilizes Go's intrinsics for comparison.
		return string(zeroes[:size]) == string(rawValue)
	}

	return isZeroAny(rawValue)
}

func isZeroAny(bytes []byte) bool {
	for _, b := range bytes {
		if b != 0 {
			return false
		}
	}
	return true
}

var (
	structCache  = map[unsafe.Pointer]*StructInfo{} // unsafe.Pointer -> *structInfo
	structMutex  sync.RWMutex
	structFlight singleflight.Group
)

type StructInfo struct {
	Type   reflect.Type
	Fields []StructField
}

type StructField struct {
	Name   []byte
	Type   reflect.Type
	Kind   reflect.Kind
	Size   uintptr
	Offset uintptr

	// ChildStruct is provided if this field is of type struct. If the type
	// matches exactly the parent, then the same pointer is set.
	ChildStruct *StructInfo
	// Indirect is true if the type is pointer.
	Indirect bool
}

// GetStructInfo returns the struct type information for the given struct value.
// It assumes that typ is a type of a struct and does not do checks.
func GetStructInfo(typ reflect.Type) *StructInfo {
	// A reflect.Type is basically an interface containing the type pointer and
	// the value pointer. The type pointer is most likely *rtype, but we don't
	// care about that. Instead, we care about the pointer value of that type,
	// which is the value pointer. This allows us to access the map faster.
	ptr := InterfacePtr(typ)

	structMutex.RLock()
	v, ok := structCache[ptr]
	structMutex.RUnlock()
	if ok {
		return v
	}

	var typeName string

	// cpu: Intel(R) Core(TM) i5-8250U CPU @ 1.60GHz
	// BenchmarkReflectType-8   	 9097218	       123.1 ns/op

	pkgPath := typ.PkgPath()
	if pkgPath == "" {
		typeName = typ.Name()
	} else {
		typeName = pkgPath + "." + typ.Name()
	}

	ret, _, _ := structFlight.Do(typeName, func() (interface{}, error) {
		var info StructInfo
		info.Type = typ
		info.get(typ)

		structMutex.Lock()
		structCache[ptr] = &info
		structMutex.Unlock()
		return &info, nil
	})

	return ret.(*StructInfo)
}

func (info *StructInfo) get(typ reflect.Type) {
	numField := typ.NumField()
	info.Fields = make([]StructField, 0, numField)

	for i := 0; i < numField; i++ {
		fieldType := typ.Field(i)
		if !fieldType.IsExported() {
			// We cannot read unexported fields. Skip.
			continue
		}

		info.Fields = append(info.Fields, StructField{
			Name:     []byte(fieldType.Name),
			Type:     fieldType.Type,
			Kind:     fieldType.Type.Kind(),
			Size:     fieldType.Type.Size(),
			Offset:   fieldType.Offset,
			Indirect: fieldType.Type.Kind() == reflect.Ptr,
		})

		// Access the struct field that we just put in.
		structField := &info.Fields[len(info.Fields)-1]

		underlyingType := fieldType.Type
		if structField.Indirect {
			underlyingType = underlyingType.Elem()
		}

		if underlyingType.Kind() == reflect.Struct {
			if underlyingType == typ {
				// Struct field's type is the same as the one we're
				// initializing, so use that same pointer.
				structField.ChildStruct = info
			} else {
				// Prefetch the struct information if this one embeds it.
				structField.ChildStruct = GetStructInfo(underlyingType)
			}
		}
	}
}

type _iface struct {
	_ uintptr
	p unsafe.Pointer
}

// InterfacePtr returns the pointer to the internal value of the given
// interface.
func InterfacePtr(v interface{}) unsafe.Pointer {
	return (*_iface)(unsafe.Pointer(&v)).p
}

// UnderlyingPtr returns the type of and the pointer to the value of the
// interface by dereferencing it until the actual value is reached.
func UnderlyingPtr(v interface{}) (reflect.Type, unsafe.Pointer) {
	ptr := InterfacePtr(v)
	if ptr == nil {
		return nil, nil
	}

	typ := reflect.TypeOf(v)

	var dereferenced bool

	// Traverse the pointer until it no longer is.
	for typ.Kind() == reflect.Ptr {
		if dereferenced {
			// Defer the dereference once, because Go will try to use the
			// given pointer value directly if it's already one.
			ptr = *(*unsafe.Pointer)(ptr)
			if ptr == nil {
				// Early bail.
				return nil, nil
			}
		}

		dereferenced = true
		typ = typ.Elem()
	}

	return typ, ptr
}

// Indirect dereferences the pointer until the type is no longer one. If any of
// the pointers are nil, then zero-values are returned.
func Indirect(typ reflect.Type, ptr unsafe.Pointer) (reflect.Type, unsafe.Pointer) {
	for typ.Kind() == reflect.Ptr {
		if ptr == nil {
			return nil, nil
		}

		ptr = *(*unsafe.Pointer)(ptr)
		typ = typ.Elem()
	}
	return typ, ptr
}

// IndirectOnce dereferences ptr once. It returns nil if ptr is nil.
func IndirectOnce(ptr unsafe.Pointer) unsafe.Pointer {
	if ptr == nil {
		return nil
	}
	return *(*unsafe.Pointer)(ptr)
}

// SliceInfo returns the backing array pointer and length of the slice at the
// given pointer.
func SliceInfo(ptr unsafe.Pointer) (unsafe.Pointer, int) {
	return unsafe.Pointer((*reflect.SliceHeader)(ptr).Data),
		(*reflect.SliceHeader)(ptr).Len
}
