package greddis

import (
	"bufio"
	"container/list"
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
	case *string:
		w.addString(d)
	case []byte:
		w.writeBytes(d)
	case StrInt:
		w.writeIntStr(int(d))
	case int:
		w.writeInt(d)
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

// For ArrayReader we should consider using struct tags to scan all fields in one go
// The only use for Redis Arrays currently is to scan full objects (excluding the value field)
// and expect different kinds of values. Whilst currently I read straight from a channel the
// channel name I expect, we could...?
// ArrayReader.Init(reader) -> calls reader.ScanArray()

type arrayReader interface {
	Init(r *Reader) (self *ArrayReader)
	Len() (length int)
	Scan(value ...interface{})
}

func NewArrayReader(r *Reader) *ArrayReader {
	return &ArrayReader{
		r:         r,
		scanFuncs: list.New(),
	}
}

type ArrayReader struct {
	r         *Reader
	length    int
	pos       int
	scanFuncs *list.List
}

// Len returns the length of the ArrayReader
func (a *ArrayReader) Len() int {
	return a.length
}

func (a *ArrayReader) Init(scanFuncs ...ScanFunc) error {
	err := a.r.Next(ScanArray)
	a.length = a.r.Len()
	a.r.tokenLen = 0
	a.scanFuncs.Init()
	for _, scanFunc := range scanFuncs {
		a.scanFuncs.PushBack(scanFunc)
	}
	return err
}

func (r *ArrayReader) Next(scanFuncs ...ScanFunc) *ArrayReader {
	for i := len(scanFuncs) - 1; i >= 0; i-- {
		r.scanFuncs.PushFront(scanFuncs[i])
	}
	return r
}

// Next prepares the next row to be used by `Scan()`, it returns either a "no more rows" error or
// a connection/read error will be wrapped.
func (r *ArrayReader) next() error {
	if r.pos >= r.length {
		return ErrNoMoreRows
	}
	scanFunc := r.scanFuncs.Front()
	if r.scanFuncs.Len() > 1 {
		r.scanFuncs.Remove(scanFunc)
	}
	r.pos++
	return r.r.Next(scanFunc.Value.(ScanFunc))
}

// Scan operates the same as `Scan` on a single result, other than that it can take multiple dst variables
func (r *ArrayReader) Scan(dst ...interface{}) error {
	for _, d := range dst {
		err := r.next()
		if err != nil {
			return err
		}
		if err := scan(r.r, d); err != nil {
			return err
		}
	}
	return nil
}

// SwitchOnNext returns a string value of the next value in the ArrayReader which is a pointer to the underlying
// byte slice - as the name implies, it is mostly implemented for switch cases where there's a guarantee
// that the next Scan/SwitchOnNext call will happen after the last use of this value. If you want to not
// only switch on the value or do a one-off comparison, please use Scan() instead.
func (r *ArrayReader) SwitchOnNext() string {
	err := r.next()
	if err != nil {
		return ""
	}
	return r.r.String()
}

// Expect does an Any byte comparison with the values passed in against the next value in the array
func (r *ArrayReader) Expect(vars ...string) error {
	r.next()
	for _, v := range vars {
		if r.r.String() == v {
			return nil
		}
	}
	return fmt.Errorf("%s was not equal to any of %s", r.r.String(), vars)
}
