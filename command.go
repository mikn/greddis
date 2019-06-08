package greddis

import (
	"io"
)

type command struct {
	array *respArray
}

func (c *command) addItem(item []byte) {
	c.array.addBulkString(item)
}

// writeTo takes a io.Writer to write to and returns the buffer that was passed in
func (c *command) writeTo(w io.Writer) []byte {
	w.Write(c.array.encode())
	c.array.reset(c.array.origBuf)
	return c.array.origBuf
}
