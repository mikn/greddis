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
	AddString(item ...string) arrayWriter
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

func (w *ArrayWriter) AddString(items ...string) *ArrayWriter {
	for _, item := range items {
		w.writeString(item)
		w.added++
	}
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

func (c *ArrayWriter) addString(item interface{}) {
	// this case avoids weird race conditions per https://github.com/mikn/greddis/issues/9
	switch d := item.(type) {
	case string:
		c.writeString(d)
	case *string:
		c.writeString(*d)
	}
}

func (w *ArrayWriter) addItem(item interface{}) error {
	switch d := item.(type) {
	case string: // sigh, need this for fallthrough
		w.addString(d)
	case []byte:
		w.writeBytes(d)
	case StrInt:
		w.writeIntStr(int(d))
	case int:
		w.writeInt(d)
	case *string:
		w.addString(d)
	case *[]byte:
		w.writeBytes(*d)
	case *int:
		w.writeInt(*d)
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
		return ErrWrongType(d, "driver.Valuer, *string, string, *[]byte, []byte, *int or int, *StrInt, StrInt")
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
	buf     []byte
	r       io.Reader
	length  int
	pos     int
	res     *Result
	read    bool
	prevLen int
}

// Len returns the length of the ArrayReader
func (a *ArrayReader) Len() int {
	return a.length
}

func (a *ArrayReader) Init(buf []byte, size int) *ArrayReader {
	a.reset()
	a.length = size
	a.buf = buf
	a.read = false
	a.prevLen = 0
	return a
}

func (a *ArrayReader) ResetReader(r io.Reader) {
	a.r = r
}

func (a *ArrayReader) reset() {
	a.buf = a.buf[:0]
	a.length = 0
	a.pos = 0
	a.prevLen = 0
	a.res.value = nil
	a.read = false
}

// Next prepares the next row to be used by `Scan()`, it returns either a "no more rows" error or
// a connection/read error will be wrapped.
func (a *ArrayReader) prepare() (int, error) {
	if !a.read {
		a.next(a.buf, a.prevLen)
	}
	// TODO Next should maybe not actually read the value into the result, but rather just prepare the buffers
	if a.length > a.pos {
		var i int
		var err error
		var readBytes int
		i = len(a.buf)
		for i == 0 {
			a.buf = a.buf[:cap(a.buf)]
			i, err = a.r.Read(a.buf)
			if err != nil {
				return i, err
			}
		}
		char := a.buf[0]
		if char == '$' {
			readBytes, a.res.value, err = unmarshalBulkString(a.r, a.buf[1:i])
			readBytes += 1
		} else if char == ':' || char == '-' {
			readBytes, a.res.value, err = unmarshalSimpleString(a.r, a.buf[1:i])
			truncate := readBytes - len(sep)
			if truncate < 0 {
				truncate = 0
			}
			a.res.value = a.res.value[:truncate]
			readBytes += 1
		} else {
			err = fmt.Errorf("Expected prefix '$', ':' or '-', but received '%s' full buffer: %s", string(a.buf[0]), a.buf)
		}
		a.pos++
		// if we read more bytes than what was in a.buf, we triggered another read and a.buf is now stale
		if readBytes > i-1 {
			a.buf = a.res.value[:cap(a.res.value)]
			readBytes -= 1 // we need to remove one since the a.res.value is 1 shorter than a.buf
		}

		a.prevLen = readBytes
		a.read = false
		return readBytes, err
	}
	return 0, ErrNoMoreRows
}

func (r *ArrayReader) next(buf []byte, i int) []byte {
	r.read = true
	if len(buf) >= i {
		// TODO We should probably not do a copy here, we should be able to "slide along" the buffer instead
		copy(buf, buf[i:])
		buf = buf[:len(buf)-i]
	}
	return buf
}

// Scan operates the same as `Scan` on a single result, other than that it can take multiple dst variables
func (r *ArrayReader) Scan(dst ...interface{}) error {
	for _, d := range dst {
		prevReadLen, err := r.prepare()
		if err != nil {
			return err
		}
		if err := r.res.scan(d); err != nil {
			return err
		}
		r.buf = r.next(r.buf, prevReadLen)
		r.res.value = nil
	}
	return nil
}

// SwitchOnNext returns a string value of the next value in the ArrayReader which is a pointer to the underlying
// byte slice - as the name implies, it is mostly implemented for switch cases where there's a guarantee
// that the next Scan/SwitchOnNext call will happen after the last use of this value. If you want to not
// only switch on the value or do a one-off comparison, please use Scan() instead.
func (r *ArrayReader) SwitchOnNext() string {
	_, err := r.prepare()
	if err != nil {
		return ""
	}
	return *(*string)(unsafe.Pointer(&r.res.value))
}

// Expect does an Any byte comparison with the values passed in against the next value in the array
func (r *ArrayReader) Expect(vars ...string) error {
	r.prepare()
	for _, v := range vars {
		// For ease of use, Expect takes string as input - this is a zero-alloc cast to []byte for comparison
		// this is safe because strings are immutable
		comp := *(*[]byte)(unsafe.Pointer(&v))
		if bytes.Equal(comp, r.res.value) {
			return nil
		}
	}
	return fmt.Errorf("%s was not equal to any of %s", r.res.value, vars)
}
