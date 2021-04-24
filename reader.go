package greddis

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"unsafe"
)

type ScanFunc func(*bufio.Reader) (int, error)

type reader interface {
	Next(ScanFunc) error
	Len() int
	WriteTo(io.Writer) (int64, error) // copy on write
	String() string                   // peek
	Bytes() []byte                    // peek
	Int() int
}

type Reader struct {
	r        *bufio.Reader
	limitR   *io.LimitedReader
	err      error
	tokenLen int
	intToken int
}

// TODO Do not hide the potential error from Peek, make sure it is properly propagated
// TODO propagate error from Atoi() in Int() call
func NewReader(r *bufio.Reader) *Reader {
	return &Reader{
		r:      r,
		limitR: &io.LimitedReader{R: r},
	}
}

func (r *Reader) Len() int {
	return r.tokenLen
}

func (r *Reader) Next(scan ScanFunc) (err error) {
	if r.tokenLen > 0 {
		r.r.Discard(r.tokenLen + len(sep))
	}
	r.tokenLen, err = scan(r.r)
	return err
}

func (r *Reader) WriteTo(w io.Writer) (int64, error) {
	// CopyN internally uses a LimitedReader, but creates a new one every time, this avoids an alloc
	r.limitR.N = int64(r.tokenLen)
	i, err := io.Copy(w, r.limitR)
	r.r.Discard(r.tokenLen - int(i) + len(sep))
	r.tokenLen = 0
	return i, err
}

func (r *Reader) String() string {
	str, _ := r.r.Peek(r.tokenLen)
	return *(*string)(unsafe.Pointer(&str))
}

func (r *Reader) Bytes() []byte {
	b, _ := r.r.Peek(r.tokenLen)
	return b
}

func (r *Reader) Int() int {
	r.intToken, _ = strconv.Atoi(r.String())
	return r.intToken
}

func ScanSimpleString(r *bufio.Reader) (int, error) {
	return scanSimpleString(r, '+')
}

func ScanInteger(r *bufio.Reader) (int, error) {
	return scanSimpleString(r, ':')
}

func ScanBulkString(r *bufio.Reader) (int, error) {
	return scanBulkString(r, '$')
}

func ScanArray(r *bufio.Reader) (int, error) {
	return scanBulkString(r, '*')
}

func scanBulkString(r *bufio.Reader, token byte) (int, error) {
	countLen, err := scanSimpleString(r, token)
	if err != nil {
		return 0, err
	}
	b, _ := r.Peek(countLen)
	arrayLen, err := strconv.Atoi(*(*string)(unsafe.Pointer(&b)))
	r.Discard(countLen + len(sep))
	if err != nil {
		return arrayLen, err
	}
	return arrayLen, nil
}

func expectType(r *bufio.Reader, checkType byte) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}
	if b == checkType {
		return nil
	}
	if b == '-' {
		tokenLen, _ := scanToDelim(r, sep)
		e, _ := r.Peek(tokenLen)
		defer r.Discard(tokenLen + len(sep))
		return fmt.Errorf("Redis error: %s", e)
	}
	return ErrWrongToken(checkType, b)
}

func scanToDelim(r *bufio.Reader, delim []byte) (int, error) {
	for i := 0; ; i++ {
		peek, err := r.Peek(i + len(sep))
		if err != nil {
			return i, err
		}
		if bytes.Equal(peek[i:], sep) {
			return i, nil
		}
	}
}

func scanSimpleString(r *bufio.Reader, token byte) (int, error) {
	if err := expectType(r, token); err != nil {
		return 0, err
	}
	tokenLen, err := scanToDelim(r, sep)
	if err != nil {
		return 0, err
	}
	return tokenLen, nil
}
