package greddis

// Functions needed for reading:
// ReadByte() byte, error
// ReadUntil(delim []byte) []byte, error
// Grow(n int) error
// Peek(n int) []byte, error
// String() string
// Bytes() []byte
// WriteTo(io.Writer) error

type buf struct {
	buf []byte
}
