package greddis

import (
	"bufio"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strconv"
	"unsafe"
)

type command struct {
	length int
	added  int
	buf    []byte
	bufw   *bufio.Writer
}

func (c *command) start(buf []byte, length int) *command {
	c.length = length
	c.buf = buf[:0]
	c.buf = append(c.buf, '*')
	c.buf = strconv.AppendInt(c.buf, int64(c.length), 10)
	c.buf = append(c.buf, sep...)
	c.bufw.Write(c.buf)
	c.buf = c.buf[:0]
	return c
}

func (c *command) writeInt(item int) {
	c.buf = strconv.AppendInt(c.buf, int64(item), 10)
	length := len(c.buf)
	c.buf = c.buf[:0]
	c.buf = append(c.buf, '$')
	c.buf = strconv.AppendInt(c.buf, int64(length), 10)
	c.buf = append(c.buf, sep...)
	c.buf = strconv.AppendInt(c.buf, int64(item), 10)
	c.buf = append(c.buf, sep...)
	c.bufw.Write(c.buf)
	c.buf = c.buf[:0]
	c.added++
}

func (c *command) writeBytes(item []byte) {
	c.buf = append(c.buf, '$')
	c.buf = strconv.AppendInt(c.buf, int64(len(item)), 10)
	c.buf = append(c.buf, sep...)
	c.bufw.Write(c.buf)
	c.buf = c.buf[:0]
	c.bufw.Write(item)
	c.bufw.Write(sep)
	c.added++
}

func (c *command) add(item interface{}) *command {
	switch d := item.(type) {
	case string:
		c.writeBytes(*(*[]byte)(unsafe.Pointer(&d)))
	case []byte:
		c.writeBytes(d)
	case int:
		c.writeInt(d)
	}
	return c
}

func (c *command) addUnsafe(item interface{}) error {
	switch d := item.(type) {
	case driver.Valuer:
		val, err := d.Value()
		if err != nil {
			return err
		}
		switch v := val.(type) {
		case []byte:
			c.writeBytes(v)
			return nil
		}
	case *string:
		// this case avoids weird race conditions per https://github.com/mikn/greddis/issues/9
		c.add(*d)
		return nil
	case string: // sigh, need this for fallthrough
		c.add(d)
		return nil
	case []byte:
		c.add(d)
		return nil
	case *[]byte:
		c.add(*d)
		return nil
	case int:
		c.add(d)
		return nil
	case *int:
		c.add(*d)
		return nil
	}
	return errors.New("Unparseable result")
}

func (c *command) reset(conn io.Writer) {
	c.buf = c.buf[:0]
	c.bufw.Reset(conn)
}

func (c *command) flush() ([]byte, error) {
	c.buf = c.buf[:0]
	if c.length != c.added {
		return c.buf, errors.New(fmt.Sprintf("Expected %d items, but %d items were added", c.length, c.added))
	}
	c.length = 0
	c.added = 0
	return c.buf, c.bufw.Flush()
}
