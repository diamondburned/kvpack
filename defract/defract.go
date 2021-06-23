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

// Reflected built-in types.
var (
	Bool       = reflect.TypeOf(false)
	Byte       = reflect.TypeOf(byte(0))
	Uint       = reflect.TypeOf(uint(0))
	Uint8      = reflect.TypeOf(uint8(0))
	Uint16     = reflect.TypeOf(uint16(0))
	Uint32     = reflect.TypeOf(uint32(0))
	Uint64     = reflect.TypeOf(uint64(0))
	Int        = reflect.TypeOf(int(0))
	Int8       = reflect.TypeOf(int8(0))
	Int16      = reflect.TypeOf(int16(0))
	Int32      = reflect.TypeOf(int32(0))
	Int64      = reflect.TypeOf(int64(0))
	Float32    = reflect.TypeOf(float32(0))
	Float64    = reflect.TypeOf(float64(0))
	Complex64  = reflect.TypeOf(complex64(0))
	Complex128 = reflect.TypeOf(complex128(0))
	String     = reflect.TypeOf("")
	ByteSlice  = reflect.TypeOf([]byte(nil))
)

// NumberLE returns the little-endian variant of the given number at the
// pointer. It does not handle variable-length integers, and is meant only for
// other statically sized integer or floating-point types.
//
// If this method is called on a Big Endian macnine, then a new byte buffer will
// be allocated from the pool.
func NumberLE(typ reflect.Type, ptr unsafe.Pointer, f func([]byte) error) error {
	if !IsLittleEndian {
		return numberAsLE(typ, ptr, f)
	}

	switch typ {
	case Uint8, Int8, Byte:
		return f((*[1]byte)(ptr)[:])
	case Uint16, Int16:
		return f((*[2]byte)(ptr)[:])
	case Uint32, Int32, Float32:
		return f((*[4]byte)(ptr)[:])
	case Uint64, Int64, Float64, Complex64:
		return f((*[8]byte)(ptr)[:])
	case Complex128:
		return f((*[16]byte)(ptr)[:])
	default:
		panic("NumberLE got unsupported type " + typ.String())
	}
}

var bigEndianPool = sync.Pool{
	New: func() interface{} { return make([]byte, 8) },
}

// numberAsLE is the slow path. It tries not to allocate by having an internal
// byte buffer pool.
func numberAsLE(typ reflect.Type, ptr unsafe.Pointer, f func([]byte) error) error {
	switch typ {
	case Uint8, Int8, Byte:
		// A single byte is architecture-independent.
		return f((*[1]byte)(ptr)[:])
	}

	b := bigEndianPool.Get().([]byte)
	defer bigEndianPool.Put(b)

	switch typ {
	case Uint16, Int16:
		binary.LittleEndian.PutUint16(b, *(*uint16)(ptr))
		return f(b[:2])
	case Uint32, Int32, Float32:
		binary.LittleEndian.PutUint32(b, *(*uint32)(ptr))
		return f(b[:4])
	case Uint64, Int64, Float64, Complex64:
		binary.LittleEndian.PutUint64(b, *(*uint64)(ptr))
		return f(b[:8])
	case Complex128:
		panic("Complex128 unsupported on Big Endian (TODO)")
	default:
		panic("numberAsLE got unsupported type " + typ.String())
	}
}

// IsZero returns true if the value that the pointer points to is nil.
func IsZero(typ reflect.Type, ptr unsafe.Pointer) bool {
	return reflect.NewAt(typ, ptr).Elem().IsZero()
}

var (
	structCache  sync.Map // unsafe.Pointer -> *structInfo
	structFlight singleflight.Group
)

type StructInfo struct {
	Fields []StructField
}

type StructField struct {
	Name   []byte
	Type   reflect.Type
	Offset uintptr
}

// GetStructInfo returns the struct type information for the given struct value.
// It assumes that typ is a type of a struct and does not do checks.
func GetStructInfo(typ reflect.Type) *StructInfo {
	// A reflect.Type is basically an interface containing the type pointer and
	// the value pointer. The type pointer is most likely *rtype, but we don't
	// care about that. Instead, we care about the pointer value of that type,
	// which is the value pointer. This allows us to access the map faster.

	v, ok := structCache.Load(typ)
	if ok {
		return v.(*StructInfo)
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

	v, _, _ = structFlight.Do(typeName, func() (interface{}, error) {
		var info StructInfo
		info.get(typ)

		structCache.Store(typ, &info)
		return &info, nil
	})

	return v.(*StructInfo)
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
			Name:   []byte(fieldType.Name),
			Type:   fieldType.Type,
			Offset: fieldType.Offset,
		})
	}
}

// UnderlyingPtr returns the type of and the pointer to the value of the
// interface by dereferencing it until the actual value is reached.
func UnderlyingPtr(v interface{}) (reflect.Type, unsafe.Pointer) {
	type iface struct {
		_ uintptr
		p unsafe.Pointer
	}

	ptr := (*iface)(unsafe.Pointer(&v)).p
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
