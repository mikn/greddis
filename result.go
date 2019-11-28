//go:generate mockgen -source result.go -destination ./mocks/mock_greddis/mock_result.go
package greddis

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"unsafe"
)

// Scanner is an interface that allows you to implement custom logic with direct control over
// the byte buffer being used by the connection. Do not modify it and do not return it.
// As soon as your Scan function exits, the connection will start using the buffer again.
type Scanner interface {
	Scan(dst interface{}) error
}

// Result is what is returned from the Redis client if a single response is expected
type Result struct {
	value  []byte
	finish func()
}

func NewResult(buf []byte) *Result {
	return &Result{
		value:  buf,
		finish: func() {},
	}
}

// Scan on result allows us to read with zero-copy into Scanner and io.Writer
// implementations as well as into string, int, other []byte slices (but with copy).
// However since Redis always returns a []byte slice, if you were to implement a scanner
// you would need to have a value switch that takes []byte slice and does something
// productive with it. This implementation can be simpler and forego the source
// switch since it is custom made for Redis.
func (r *Result) Scan(dst interface{}) (err error) {
	err = r.scan(dst)
	r.finish()
	return err
}

func (r *Result) scan(dst interface{}) (err error) {
	switch d := dst.(type) {
	case *string:
		*d = string(r.value)
	case *int:
		var val int
		val, err = strconv.Atoi(string(r.value))
		if err == nil {
			*d = int(val)
		}
	case *[]byte:
		t := make([]byte, len(r.value))
		copy(t, r.value)
		*d = t
	case io.Writer:
		_, err = d.Write(r.value)
	case Scanner:
		err = d.Scan(r.value)
	default:
		err = fmt.Errorf("dst is not of any supported type. Is of type %s", d)
	}
	return err
}

// ArrayResult is what is returned when multiple results are expected
type ArrayResult struct {
	buf    []byte
	r      io.Reader
	length int
	pos    int
	res    *Result
}

// Len returns the length of the ArrayResult
func (a *ArrayResult) Len() int {
	return a.length
}

func (a *ArrayResult) reset() {
	a.length = 0
	a.pos = 0
	a.buf = a.buf[:0]
	a.res.value = nil
}

// Next prepares the next row to be used by `Scan()`, it returns either a "no more rows" error or
// a connection/read error will be wrapped.
func (a *ArrayResult) Next() error {
	// TODO Next should maybe not actually read the value into the result, but rather just prepare the buffers
	if a.length > a.pos {
		var err error
		switch a.buf[0] {
		case '$':
			// TODO should not need to copy here
			copy(a.buf, a.buf[1:])
			a.res.value, err = unmarshalBulkString(a.r, a.buf)
		case ':':
			copy(a.buf, a.buf[1:])
			a.res.value, err = unmarshalSimpleString(a.r, a.buf)
		default:
			err = fmt.Errorf("Expected prefix '$', ':' or '-', but received '%s'", string(a.buf[0]))
		}
		a.buf = a.res.value[:cap(a.res.value)]
		a.pos++
		return err
	}
	return ErrNoMoreRows
}

// Scan operates the same as `Scan` on a single result, other than that it needs to be preceded by `Next()`
func (a *ArrayResult) Scan(dst interface{}) error {
	if a.res.value == nil {
		return errors.New("Need to call Next() on the ArrayResult before you can call scan")
	}
	err := a.res.scan(dst)
	if len(a.buf) >= len(a.res.value)+2 {
		// TODO We shouldn't do a copy here, we should be able to "slide along" the buffer instead
		copy(a.buf, a.buf[len(a.res.value)+2:cap(a.buf)])
	}
	a.res.value = nil
	return err
}

type internalArray struct {
	arr *ArrayResult
	err *errProxy
}

func (i *internalArray) SwitchOnNext() string {
	i.err = i.storeError(i.arr.Next())
	if i.err == nil {
		return *(*string)(unsafe.Pointer(&i.arr.res.value))
	}
	return ""
}

func (i *internalArray) storeError(err error) *errProxy {
	if err != nil {
		if i.err != nil {
			return &errProxy{i.err, err}
		} else {
			return &errProxy{Err: err}
		}
	}
	return i.err
}

func (i *internalArray) Skip() *internalArray {
	i.err = i.storeError(i.arr.Next())
	i.Next()
	return i
}

func (i *internalArray) Next() *internalArray {
	if len(i.arr.buf) >= len(i.arr.res.value)+2 {
		// TODO We shouldn't do a copy here, we should be able to "slide along" the buffer instead
		copy(i.arr.buf, i.arr.buf[len(i.arr.res.value)+2:cap(i.arr.buf)])
	}
	i.arr.res.value = nil
	return i
}

func (i *internalArray) Expect(vars ...string) *internalArray {
	i.err = i.storeError(i.arr.Next())
	if i.err != nil {
		return i
	}
	for _, v := range vars {
		comp := *(*[]byte)(unsafe.Pointer(&v))
		if bytes.Equal(comp, i.arr.res.value) {
			i.Next()
			return i
		}
	}
	i.err = &errProxy{proxied: fmt.Errorf("%s was not equal to any of %s", i.arr.res.value, vars)}
	i.Next()
	return i
}

func (i *internalArray) GetErrors() error {
	if i.err != nil {
		return i.err
	}
	return nil
}

func (i *internalArray) Scan(dst ...interface{}) *internalArray {
	for _, d := range dst {
		i.err = i.storeError(i.arr.Next())
		if i.err == nil { // and no previous problems with scan/next
			i.err = i.storeError(i.arr.Scan(d))
		}
	}
	return i
}
