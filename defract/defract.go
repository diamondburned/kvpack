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

// SliceHeader is the header structure that contains an unsafe.Pointer data
// field instead of uintptr.
type SliceHeader struct {
	Data unsafe.Pointer
	Len  int
	Cap  int
}

// StringHeader is a safer reflect.StringHeader.
type StringHeader struct {
	Data unsafe.Pointer
	Len  int
}

const correctHeader = true &&
	unsafe.Sizeof(SliceHeader{}) == unsafe.Sizeof(reflect.SliceHeader{}) &&
	unsafe.Sizeof(StringHeader{}) == unsafe.Sizeof(reflect.StringHeader{})

func init() {
	// This will probably be optimized out.
	if !correctHeader {
		panic("SliceHeader size mismatch")
	}
}

// ByteSlice is the reflect.Type value for a byte slice.
var ByteSlice = reflect.TypeOf([]byte(nil))

// IntLE is a helper function that converts the given int value into bytes,
// ideally without copying on a 64-bit little-endian machine. The bytes are
// always in little-endian.
func IntLE(i *int) []byte {
	if unsafe.Sizeof(i) == 8 && IsLittleEndian {
		return (*[8]byte)(unsafe.Pointer(i))[:]
	}

	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(*i))
	return b[:]
}

// UintLE is a helper function that converts the given uint64 value into bytes,
// ideally without copying on a 64-bit little-endian machine. The bytes are
// always in little-endian.
func UintLE(u *uint) []byte {
	if unsafe.Sizeof(u) == 8 && IsLittleEndian {
		return (*[8]byte)(unsafe.Pointer(u))[:]
	}

	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(*u))
	return b[:]
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

// ReadInt64LE reads dst and returns an int64 if dst has enough data. Otherwise,
// false is returned.
func ReadInt64LE(dst []byte) (int64, bool) {
	if len(dst) < 8 {
		return 0, false
	}

	if IsLittleEndian {
		return *(*int64)(unsafe.Pointer(&dst[0])), true
	} else {
		return int64(binary.LittleEndian.Uint64(dst)), true
	}
}

// ReadNumberLE reads the little-endian number of the given byte slice into the
// pointer. If bound checking fails, false is returned.
func ReadNumberLE(b []byte, kind reflect.Kind, ptr unsafe.Pointer) bool {
	if !IsLittleEndian {
		panic("TODO ReadNumberLE Big Endian")
	}

	switch kind {
	case reflect.Int:
		// This is optimized away, since Sizeof is a constant.
		switch unsafe.Sizeof(int(0)) {
		case 4:
			kind = reflect.Int32
		case 8:
			kind = reflect.Int64
		default:
			panic("unknown architecture, weird int size")
		}
	case reflect.Uint:
		switch unsafe.Sizeof(uint(0)) {
		case 4:
			kind = reflect.Uint32
		case 8:
			kind = reflect.Uint64
		default:
			panic("unknown architecture, weird uint size")
		}
	}

	switch kind {
	case reflect.Uint8, reflect.Int8:
		if len(b) < 1 {
			return false
		}

		*(*uint8)(ptr) = b[0]
		return true

	case reflect.Uint16, reflect.Int16:
		if len(b) < 2 {
			return false
		}

		// Fast but shitty SIMD copying.
		*(*uint16)(ptr) = *(*uint16)(unsafe.Pointer(&b[0]))
		return true

	case reflect.Uint32, reflect.Int32, reflect.Float32:
		if len(b) < 4 {
			return false
		}

		*(*uint32)(ptr) = *(*uint32)(unsafe.Pointer(&b[0]))
		return true

	case reflect.Uint64, reflect.Int64, reflect.Float64, reflect.Complex64:
		if len(b) < 8 {
			return false
		}

		*(*uint64)(ptr) = *(*uint64)(unsafe.Pointer(&b[0]))
		return true

	case reflect.Complex128:
		// Copy returns min(b_len, value_len), so we can do this shorthand.
		return copy(unsafe.Slice((*byte)(ptr), 16), b) == 16

	default:
		panic("NumberLE got unsupported kind " + kind.String())
	}
}

func readNumberAsLE(b []byte, kind reflect.Kind, ptr unsafe.Pointer) bool {
	switch kind {
	case reflect.Uint8, reflect.Int8:
		if len(b) < 1 {
			return false
		}

		// Architecture-independent.
		*(*uint8)(ptr) = b[0]
		return true
	}

	switch kind {
	case reflect.Uint16, reflect.Int16:
		if len(b) < 2 {
			return false
		}

		*(*uint16)(ptr) = binary.LittleEndian.Uint16(b)
		return true

	case reflect.Uint32, reflect.Int32, reflect.Float32:
		if len(b) < 4 {
			return false
		}

		*(*uint32)(ptr) = binary.LittleEndian.Uint32(b)
		return true

	case reflect.Uint64, reflect.Int64, reflect.Float64, reflect.Complex64:
		if len(b) < 8 {
			return false
		}

		*(*uint64)(ptr) = binary.LittleEndian.Uint64(b)
		return true

	case reflect.Complex128:
		panic("Complex128 unsupported on Big Endian (TODO)")
	default:
		panic("numberAsLE got unsupported kind " + kind.String())
	}
}

// NumberLE returns the little-endian variant of the given number at the
// pointer. It does not handle variable-length integers, and is meant only for
// other statically sized integer or floating-point types.
//
// If this method is called on a Big Endian macnine, then a new byte buffer will
// be allocated from the pool.
func NumberLE(kind reflect.Kind, ptr unsafe.Pointer) []byte {
	if !IsLittleEndian {
		return numberAsLE(kind, ptr)
	}

	switch kind {
	case reflect.Uint8, reflect.Int8:
		return (*[1]byte)(ptr)[:]
	case reflect.Uint16, reflect.Int16:
		return (*[2]byte)(ptr)[:]
	case reflect.Uint32, reflect.Int32, reflect.Float32:
		return (*[4]byte)(ptr)[:]
	case reflect.Uint64, reflect.Int64, reflect.Float64, reflect.Complex64:
		return (*[8]byte)(ptr)[:]
	case reflect.Complex128:
		return (*[16]byte)(ptr)[:]
	default:
		panic("NumberLE got unsupported kind " + kind.String())
	}
}

// numberAsLE is the slow path. It tries not to allocate by having an internal
// byte buffer pool.
func numberAsLE(kind reflect.Kind, ptr unsafe.Pointer) []byte {
	switch kind {
	case reflect.Uint8, reflect.Int8:
		// A single byte is architecture-independent.
		return (*[1]byte)(ptr)[:]
	}

	var b [8]byte

	switch kind {
	case reflect.Uint16, reflect.Int16:
		binary.LittleEndian.PutUint16(b[:2], *(*uint16)(ptr))
		return b[:2]
	case reflect.Uint32, reflect.Int32, reflect.Float32:
		binary.LittleEndian.PutUint32(b[:4], *(*uint32)(ptr))
		return b[:4]
	case reflect.Uint64, reflect.Int64, reflect.Float64, reflect.Complex64:
		binary.LittleEndian.PutUint64(b[:8], *(*uint64)(ptr))
		return b[:8]
	case reflect.Complex128:
		panic("Complex128 unsupported on Big Endian (TODO)")
	default:
		panic("numberAsLE got unsupported kind " + kind.String())
	}
}

const zeroesLen = 102400 // 10KB

// zeroes might just be one of my worst hacks to date.
var zeroes [zeroesLen]byte

// ZeroOut fills the given buffer with zeroes.
func ZeroOut(ptr unsafe.Pointer, size uintptr) {
	if ptr == nil {
		return
	}

	if copy(unsafe.Slice((*byte)(ptr), size), zeroes[:]) < zeroesLen {
		return
	}

	// Fill out the rest if copy returns exactly the length of zeroes. We can do
	// this 8 bytes at a time by using uint64.
	vec8End := size - (size % 8)
	current := uintptr(zeroesLen)

	for current < vec8End {
		*(*uint64)(unsafe.Add(ptr, current)) = 0
		current += 8
	}

	for current < size {
		*(*byte)(unsafe.Add(ptr, current)) = 0
		current++
	}
}

// IsZero returns true if the data at the given pointer is all zero. The
// function scans the data up to the given length.
func IsZero(ptr unsafe.Pointer, size uintptr) bool {
	rawValue := unsafe.Slice((*byte)(ptr), size)

	if size < zeroesLen {
		// Fast path that utilizes Go's intrinsics for comparison.
		return string(zeroes[:size]) == string(rawValue)
	}

	// Compare using the fast path the first zeroesLen bytes anyway.
	if string(zeroes[:]) != string(rawValue[:zeroesLen]) {
		return false
	}

	return isZeroAny(rawValue[zeroesLen:])
}

func isZeroAny(bytes []byte) bool {
	// vec8End defines the boundary in which the increment-by-8 loop cannot go
	// further.
	vec8End := len(bytes) - (len(bytes) % 8)
	current := 0

	// Compare (hopefully) most of the buffer 8 bytes at a time.
	for current < vec8End {
		if *(*uint64)(unsafe.Pointer(&bytes[current])) != 0 {
			return false
		}
		current += 8
	}

	// Compare the rest using a regular loop.
	for current < len(bytes) {
		if bytes[current] != 0 {
			return false
		}
		current++
	}

	return true
}

var (
	structFlight singleflight.Group
	structMutex  sync.RWMutex
	structCache  = map[unsafe.Pointer]*StructInfo{} // unsafe.Pointer -> *structInfo
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
		info := StructInfo{
			Type: typ,
		}

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

// AllocIndirect allocates a pointer type until the value is reached, and the
// pointer to that newly-allocated value is returned, along with the underlying
// type. If the given ptr is not nil, then the memory will be reused.
func AllocIndirect(typ reflect.Type, ptr unsafe.Pointer) (reflect.Type, unsafe.Pointer) {
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()

		// Ensure that the value at the pointer is not nil if it's a pointer; if
		// it is, allocate a new one on the heap.
		if *(*unsafe.Pointer)(ptr) == nil {
			newPtr := unsafe.Pointer(reflect.New(typ).Pointer())
			*(*unsafe.Pointer)(ptr) = newPtr
			ptr = newPtr
		} else {
			ptr = *(*unsafe.Pointer)(ptr)
		}
	}

	return typ, ptr
}

// SliceInfo returns the backing array pointer and length of the slice at the
// given pointer.
func SliceInfo(ptr unsafe.Pointer) (unsafe.Pointer, int, int) {
	if *(*unsafe.Pointer)(ptr) == nil {
		return nil, 0, 0
	}

	return (*SliceHeader)(ptr).Data, (*SliceHeader)(ptr).Len, (*SliceHeader)(ptr).Cap
}

// StringInfo returns the backing array pointer and length of the string at the
// given pointer. It also works with byte slices.
func StringInfo(ptr unsafe.Pointer) (unsafe.Pointer, int) {
	if *(*unsafe.Pointer)(ptr) == nil {
		return nil, 0
	}

	return (*StringHeader)(ptr).Data, (*StringHeader)(ptr).Len
}

// AllocSlice allocates a slice that is len*typsize bytes large. The returned
// pointer is the data pointer.
func AllocSlice(ptr unsafe.Pointer, size, len int64) {
	// Increment length once to account for the last element in the list.
	len++

	*(*[]byte)(ptr) = make([]byte, int(len*size))
	h := (*SliceHeader)(ptr)
	h.Len = int(len)
	h.Cap = int(len)
}

// SliceSetLen sets the length of the slice at the given pointer and returns the
// pointer to the backing array.
func SliceSetLen(ptr unsafe.Pointer, len int64) unsafe.Pointer {
	(*SliceHeader)(ptr).Len = int(len)
	return (*SliceHeader)(ptr).Data
}

// BytesToStr converts the given bytes to string without copying.
func BytesToStr(bytes []byte) string {
	// []byte is a larger structure than string, so this is fine.
	return *(*string)(unsafe.Pointer(&bytes))
}

// CopyString tries to reduce allocations by reusing the backing array if
// there's enough length.
func CopyString(dst unsafe.Pointer, src []byte) {
	h := (*StringHeader)(dst)

	if h.Data != nil && h.Len >= len(src) {
		h.Len = len(src)
		copy(unsafe.Slice((*byte)(h.Data), len(src)), src)
		return
	}

	// Reallocate the string the normal way.
	*(*string)(dst) = string(src)
}
