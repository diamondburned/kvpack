package key

import (
	"bytes"
	"testing"

	"github.com/diamondburned/kvpack/defract"
)

func TestAppendKey(t *testing.T) {
	type expect struct {
		key   string
		extra string
	}

	type test struct {
		name   string
		buf    []byte
		key    []byte
		extra  int
		expect expect
	}

	var tests = []test{
		{
			name:   "no extra",
			buf:    []byte("hi"),
			key:    []byte("key"),
			extra:  0,
			expect: expect{"hi\x00key", ""},
		},
		{
			name:   "3 extra",
			buf:    []byte("hi"),
			key:    []byte("key"),
			extra:  3,
			expect: expect{"hi\x00key", "\x00\x00\x00"},
		},
		{
			name: "3 extra reuse",
			buf: func() []byte {
				buf := make([]byte, 128)
				copy(buf, "hi")
				return buf[:2]
			}(),
			key:    []byte("key"),
			extra:  3,
			expect: expect{"hi\x00key", "\x00\x00\x00"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			arena := Arena{
				keyBuffer: &keyBuffer{Buffer: test.buf},
				separator: "\x00",
			}

			new, extra := arena.AppendExtra(test.buf, test.key, test.extra)
			if string(test.expect.key) != string(new) {
				t.Fatalf("key expected %q, got %q", test.expect.key, new)
			}

			if string(test.expect.extra) != string(extra) {
				t.Fatalf("extra expected %q, got %q", test.expect.extra, extra)
			}
		})
	}
}

func TestAppendKeyNoAlloc(t *testing.T) {
	// no alloc test
	arena := Arena{
		keyBuffer: &keyBuffer{Buffer: make([]byte, 0, 10240)},
		separator: "\x00",
	}
	arena.Buffer = append(arena.Buffer, "prefix"...)

	for i := 0; i < 2; i++ {
		key, extra := arena.AppendExtra(arena.Buffer[:6], []byte("tail"), 10)
		key = arena.AvoidOverflow(append(key, "testtest"...), len(key)+len(extra))

		if string(key) != "prefix\x00tailtesttest" {
			t.Fatalf("buffer conflict, got %q", key)
		}
	}
}

func TestAppendKeyFixedSize(t *testing.T) {
	// no alloc test
	arena := Arena{
		keyBuffer: &keyBuffer{Buffer: make([]byte, 0, 10240)},
		separator: "\x00",
	}
	arena.Buffer = append(arena.Buffer, "prefix"...)

	for i := 0; i < 2; i++ {
		key, extra := arena.AppendExtra(arena.Buffer[:6], nil, 10)
		copy(extra, "testtest")

		if string(key[:len(key)+len(extra)]) != "prefix\x00testtest\x00\x00" {
			t.Fatalf("buffer conflict, got %q", key)
		}
	}
}

func TestAppendKeyRegrow(t *testing.T) {
	arena := Arena{
		keyBuffer: &keyBuffer{Buffer: make([]byte, 0, 12)},
		separator: "\x00",
	}
	arena.Buffer = append(arena.Buffer, "prefix"...)

	for i := 0; i < 2; i++ {
		key, extra := arena.AppendExtra(arena.Buffer[:6], nil, 10)
		copy(extra, "testtest")

		if string(key[:len(key)+len(extra)]) != "prefix\x00testtest\x00\x00" {
			t.Fatalf("buffer conflict, got %q", key)
		}
	}
}

func TestAppendKeyAvoidOverflow(t *testing.T) {
	t.Run("enough", func(t *testing.T) {
		// re-alloc test
		arena := Arena{
			keyBuffer: &keyBuffer{Buffer: make([]byte, 0, 128)},
			separator: "\x00",
		}
		arena.Buffer = append(arena.Buffer, "prefix"...)
		bufferLen := len(arena.Buffer)

		key, extra := arena.AppendExtra(arena.Buffer[:6], nil, 10)
		if len(key) != 7 {
			t.Fatalf("unexpected key len %d: %q", len(key), key)
		}
		if len(extra) != 10 {
			t.Fatalf("unexpected extra len %d: %q", len(extra), extra)
		}

		// len(key)+len(extra) here is the old slice. len(extra) is unreliable
		// after the appending.
		key = arena.AvoidOverflow(append(key, "tailtail"...), len(key)+len(extra))

		expectBuffer := []byte("prefixprefix\x00tailtail")
		if !bytes.HasPrefix(arena.Buffer, expectBuffer) {
			t.Fatalf(
				"invalid buffer data: \n%q...\n%q",
				arena.Buffer, expectBuffer,
			)
		}

		if string(key) != "prefix\x00tailtail" {
			t.Fatalf("buffer conflict, got %q", key)
		}

		// String fits into the allotted region, so length is updated. The 10+7
		// comes from the length of the key and extra slices, while the 2 is the
		// unused section's length.
		if len(arena.Buffer) != bufferLen+(10+7-2) {
			t.Fatalf(
				"unexpected buffer len changed from %d to %d",
				bufferLen, len(arena.Buffer),
			)
		}
	})

	t.Run("overflow", func(t *testing.T) {
		// re-alloc test
		arena := Arena{
			keyBuffer: &keyBuffer{Buffer: make([]byte, 0, 128)},
			separator: "\x00",
		}
		arena.Buffer = append(arena.Buffer, "prefix"...)
		bufferLen := len(arena.Buffer)

		key, extra := arena.AppendExtra(arena.Buffer[:6], nil, 5)
		key = arena.AvoidOverflow(append(key, "toolonggggg"...), len(key)+len(extra))

		if string(key) != "prefix\x00toolonggggg" {
			t.Fatalf("buffer conflict, got %q", key)
		}

		if defract.WithinBytes(arena.Buffer, key) {
			t.Fatalf("key is still within arena")
		}

		// String is too long, so the buffer length should be rolled back.
		if len(arena.Buffer) != bufferLen {
			t.Fatalf(
				"unexpected buffer len changed from %d to %d",
				bufferLen, len(arena.Buffer),
			)
		}
	})
}
