package greddis

import (
	"io"
	"strconv"
)

type command struct {
	array *respArray
}

func (c *command) add(item interface{}) *command {
	switch v := item.(type) {
	case string:
		c.array.addBulkString([]byte(v))
	case int:
		var i = strconv.Itoa(v)
		c.array.addBulkString([]byte(i))
	case []byte:
		c.array.addBulkString(v)
	}
	return c
}

// writeTo takes a io.Writer to write to and returns the buffer that was passed in
func (c *command) writeTo(w io.Writer) []byte {
	w.Write(c.array.encode())
	c.array.reset(c.array.origBuf)
	return c.array.origBuf
}
