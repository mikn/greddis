package greddis

import (
	"bufio"
	"bytes"
	"database/sql/driver"
	"fmt"
	"io"
	"strconv"
	"unsafe"
)

type StrInt int

type arrayWriter interface {
	Add(item ...interface{}) (err error)
	Flush() (err error)
	Init(length int) (newAW ArrayWriter)
	Len() (length int)
	Reset(w io.Writer)
}

func NewArrayWriter(bufw *bufio.Writer) *ArrayWriter {
	return &ArrayWriter{
		bufw:    bufw,
		convBuf: make([]byte, 0, 20), // max length of an signed int64 is 20
	}
}

type ArrayWriter struct {
	length  int
	added   int
	convBuf []byte
	bufw    *bufio.Writer
}

func (w *ArrayWriter) Init(length int) *ArrayWriter {
	w.length = length
	w.bufw.WriteRune('*')
	w.bufw.Write(w.convInt(w.length))
	w.bufw.Write(sep)
	return w
}

func (w *ArrayWriter) Len() int {
	return w.length
}

func (w *ArrayWriter) Add(items ...interface{}) error {
	var err error
	for _, item := range items {
		err = w.addItem(item)
		if err != nil {
			return err
		}
		w.added++
	}
	return err
}

func (w *ArrayWriter) AddSingle(item interface{}) *ArrayWriter {
	w.addItem(item)
	w.added++
	return w
}

func (w *ArrayWriter) Flush() error {
	if w.length != w.added {
		return fmt.Errorf("Expected %d items, but %d items were added", w.length, w.added)
	}
	//w.convBuf = w.convBuf[:0]
	w.length = 0
	w.added = 0
	return w.bufw.Flush()
}

func (w *ArrayWriter) Reset(wr io.Writer) {
	w.bufw.Reset(wr)
	w.convBuf = w.convBuf[:0]
	w.length = 0
	w.added = 0
}

func (w *ArrayWriter) convInt(item int) []byte {
	w.convBuf = w.convBuf[:0]
	w.convBuf = strconv.AppendInt(w.convBuf, int64(item), 10)
	return w.convBuf
}

func (w *ArrayWriter) writeInt(item int) {
	w.bufw.WriteRune(':')
	w.bufw.Write(w.convInt(item))
	w.bufw.Write(sep)
}

func (w *ArrayWriter) writeIntStr(item int) {
	// get string length of int first
	length := len(w.convInt(item))
	w.bufw.WriteRune('$')
	w.bufw.Write(w.convInt(length))
	w.bufw.Write(sep)
	w.bufw.Write(w.convInt(item))
	w.bufw.Write(sep)
}

func (w *ArrayWriter) writeBytes(item []byte) {
	w.bufw.WriteRune('$')
	w.bufw.Write(w.convInt(len(item)))
	w.bufw.Write(sep)
	w.bufw.Write(item)
	w.bufw.Write(sep)
}

func (c *ArrayWriter) writeString(item string) {
	c.writeBytes(*(*[]byte)(unsafe.Pointer(&item)))
}

func (c *ArrayWriter) concreteTypeWrites(item interface{}) {
	// this case avoids weird race conditions per https://github.com/mikn/greddis/issues/9
	switch d := item.(type) {
	case string:
		c.writeString(d)
	}
}

func (w *ArrayWriter) addItem(item interface{}) error {
	switch d := item.(type) {
	case string: // sigh, need this for fallthrough
		w.concreteTypeWrites(d)
	case []byte:
		w.writeBytes(d)
	case StrInt:
		w.writeIntStr(int(d))
	case int:
		w.writeIntStr(d)
	case *string:
		w.concreteTypeWrites(*d)
	case *[]byte:
		w.writeBytes(*d)
	case *int:
		w.writeIntStr(*d)
	case *StrInt:
		w.writeIntStr(int(*d))
	case driver.Valuer:
		val, err := d.Value()
		if err != nil {
			return err
		}
		switch v := val.(type) {
		case []byte:
			w.writeBytes(v)
			return nil
		default:
			return ErrWrongType(v, "a driver.Valuer that supports []byte")
		}
	default:
		return ErrWrongType(d, "driver.Valuer, *string, string, *[]byte, []byte, *int or int")
	}
	return nil
}

type arrayReader interface {
	Expect(values ...string) (res bool)
	Init(buf []byte, size int) (self *ArrayReader)
	Len() (length int)
	Scan(dst ...interface{}) (err error)
	SwitchOnItem() (value string)
}

func NewArrayReader(buf []byte, r io.Reader, res *Result) *ArrayReader {
	return &ArrayReader{
		buf: buf,
		r:   r,
		res: res,
	}
}

type ArrayReader struct {
	buf    []byte
	r      io.Reader
	length int
	pos    int
	res    *Result
}

// Len returns the length of the ArrayReader
func (a *ArrayReader) Len() int {
	return a.length
}

func (a *ArrayReader) Init(buf []byte, size int) *ArrayReader {
	a.length = size
	a.pos = 0
	a.buf = buf
	a.res.value = nil
	return a
}

func (a *ArrayReader) reset() {
	a.length = 0
	a.pos = 0
	a.buf = a.buf[:0]
	a.res.value = nil
}

// Next prepares the next row to be used by `Scan()`, it returns either a "no more rows" error or
// a connection/read error will be wrapped.
func (a *ArrayReader) next() error {
	// TODO Next should maybe not actually read the value into the result, but rather just prepare the buffers
	if a.length > a.pos {
		var err error
		char := a.buf[0]
		if char == '$' {
			copy(a.buf, a.buf[1:])
			a.res.value, err = unmarshalBulkString(a.r, a.buf)
		} else if char == ':' || char == '-' {
			copy(a.buf, a.buf[1:])
			a.res.value, err = unmarshalSimpleString(a.r, a.buf)
		} else {
			err = fmt.Errorf("Expected prefix '$', ':' or '-', but received '%s'", string(a.buf[0]))
		}
		//a.buf = a.res.value[:cap(a.res.value)]
		a.pos++
		return err
	}
	return ErrNoMoreRows
}

// Scan operates the same as `Scan` on a single result, other than that it can take multiple dst variables
func (r *ArrayReader) Scan(dst ...interface{}) error {
	for _, d := range dst {
		if err := r.next(); err != nil {
			return err
		}
		if err := r.res.scan(d); err != nil {
			return err
		}
		if len(r.buf) >= len(r.res.value)+len(sep) {
			// TODO We should probably not do a copy here, we should be able to "slide along" the buffer instead
			copy(r.buf, r.buf[len(r.res.value)+len(sep):cap(r.buf)])
		}
		r.res.value = nil
	}
	return nil
}

func (r *ArrayReader) SwitchOnNext() string {
	if err := r.next(); err != nil {
		return ""
	}
	return *(*string)(unsafe.Pointer(&r.res.value))
}

func (r *ArrayReader) Expect(vars ...string) error {
	for _, v := range vars {
		// For ease of use, Expect takes string as input - this is a zero-alloc cast to []byte for comparison
		// this is safe because strings are immutable
		comp := *(*[]byte)(unsafe.Pointer(&v))
		if bytes.Equal(comp, r.res.value) {
			r.next()
			return nil
		}
	}
	r.next()
	return fmt.Errorf("%s was not equal to any of %s", r.res.value, vars)
}
