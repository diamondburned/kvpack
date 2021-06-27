package key

import (
	"sync"

	"github.com/diamondburned/kvpack/defract"
)

var (
	// keyPool is a pool of 512KB-or-larger byte slices.
	keyPool sync.Pool
)

// stockKeyPoolCap sets the capacity for each new key buffer. This sets the
// threshold for when appendExtra should just allocate a new slice instead of
// taking one from the pool.
const stockKeyPoolCap = 512 * 1024 // 512KB

// Arena describes a buffer where keys are appended into a single, reusable
// buffer.
type Arena struct {
	*keyBuffer
	separator string
}

// TakeArena takes a new key arena from the pool.
func TakeArena(separator string) Arena {
	kb, ok := keyPool.Get().([]byte)
	if !ok {
		kb = make([]byte, 0, stockKeyPoolCap)
	}

	return Arena{
		keyBuffer: &keyBuffer{Buffer: kb},
		separator: separator,
	}
}

// Put puts back the internal buffers and invalidates the arena.
func (a *Arena) Put() {
	// Put the current byte slice back to the pool. The byte slices inside the
	// pool must have a length of 0.
	for ptr := a.keyBuffer; ptr != nil; ptr = ptr.prev {
		keyPool.Put(ptr.Buffer[:0])
	}
	// Free the whole linked list mess.
	a.keyBuffer = nil
}

type keyBuffer struct {
	prev   *keyBuffer
	Buffer []byte
}

// AvoidOverflow reallocates the whole key slice if it detects that the new
// size is over the promised size.
func (kb *Arena) AvoidOverflow(key []byte, promised int) []byte {
	// Exit if the given backing array is foreign.
	if !defract.WithinBytes(kb.Buffer, key) {
		return key
	}

	if len(key) > promised {
		kb.Buffer = kb.Buffer[:len(kb.Buffer)-promised]
		return append([]byte(nil), key...)
	}

	// See how many bytes we can save by rewinding the backing array.
	excess := promised - len(key)
	kb.Buffer = kb.Buffer[:len(kb.Buffer)-excess]
	return key
}

// Append appends data into the given dst byte slice. The dst byte slice's
// backing must share with buffer, and the data up to buffer's length must not
// be changed.
func (kb *Arena) Append(dst, data []byte) []byte {
	newBuf, _ := kb.AppendExtra(dst, data, 0)
	return newBuf
}

// AppendExtra appends data into dst while reserving the extra few bytes at the
// end right after data without any separators. If data is nil, then the
// delimiter is right on the left of the extra slice.
func (kb *Arena) AppendExtra(head, tail []byte, extra int) (newBuf, extraBuf []byte) {
	if kb.keyBuffer == nil {
		panic("use of invalid keyArena (use after Rollback?)")
	}

	newAppendEnd := len(head) + len(kb.separator) + len(tail) + extra
	newBufferEnd := len(kb.Buffer) + newAppendEnd

	if cap(kb.Buffer) < newBufferEnd {
		// Existing key slice is not enough, so allocate a new one.
		var new []byte
		if newAppendEnd < stockKeyPoolCap {
			b, ok := keyPool.Get().([]byte)
			if ok {
				new = b[:newAppendEnd]
			} else {
				// Allocate another slice with the same length, because the pool
				// just happens to be empty.
				new = make([]byte, newAppendEnd, stockKeyPoolCap)
			}
		} else {
			// Double the size, because the pool size isn't large enough.
			new = make([]byte, newAppendEnd, newBufferEnd*2)
		}

		end := 0
		end += copy(new[end:], head)
		end += copy(new[end:], kb.separator)
		end += copy(new[end:], tail)

		// The new slice has the exact length needed minus the extra.
		newBuf = new[:end]

		// Kill the GC. Create a new keyBuffer entry with the "previous" field
		// pointing to the current one, then set the current one to the new one.
		old := kb.keyBuffer
		kb.keyBuffer = &keyBuffer{Buffer: new, prev: old}

	} else {
		// Set newBuf to the tail of the backing array.
		start := len(kb.Buffer)

		kb.Buffer = append(kb.Buffer, head...)
		kb.Buffer = append(kb.Buffer, kb.separator...)
		kb.Buffer = append(kb.Buffer, tail...)

		// Slice the original slice to include the new section without the extra
		// part.
		newBuf = kb.Buffer[start:]

		if extra > 0 {
			// Include the extra allocated parts into the main buffer so the
			// next call doesn't override it.
			kb.Buffer = kb.Buffer[:len(kb.Buffer)+extra]
		}
	}

	if extra == 0 {
		return newBuf, nil
	}

	extraBuf = newBuf[len(newBuf) : len(newBuf)+extra]
	// Ensure the extra buffer is always zeroed out.
	defract.ZeroOutBytes(extraBuf)

	return newBuf, extraBuf
}
