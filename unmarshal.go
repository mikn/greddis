package greddis

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"unsafe"
)

func unmarshalCount(r io.Reader, buf []byte) (int, int, error) {
	intBuf, err := unmarshalSimpleString(r, buf)
	if err != nil {
		return 0, 0, err
	}
	// zero-alloc conversion, ref: https://golang.org/src/strings/builder.go#L45
	size, err := strconv.Atoi(*(*string)(unsafe.Pointer(&intBuf)))
	if size < 0 || err != nil {
		return 0, -1, err
	}
	return len(intBuf) + len(sep), size, nil
}

func unmarshalBulkString(r io.Reader, buf []byte) ([]byte, error) {
	sizeLen, size, err := unmarshalCount(r, buf)
	if err != nil || size == -1 {
		return nil, err
	}
	size = size + len(sep)
	oldBuf := buf
	// if there's not enough space, create a new []byte buffer
	if size-cap(buf) > 0 {
		buf = make([]byte, size)
	}
	// "remove" size prefix from []byte buffer
	copy(buf, oldBuf[sizeLen:])
	buf = buf[:len(oldBuf)-sizeLen]
	if len(buf) < size {
		// only pass in the bytes we want read to (and are missing)
		readBuf := buf[len(buf):size]
		_, err = io.ReadFull(r, readBuf)
		if err != nil {
			return nil, err
		}
		buf = buf[:size]
	}
	if !bytes.Equal(buf[size-len(sep):size], sep) {
		return buf, ErrMalformedString
	}
	return buf[:size-len(sep)], nil
}

func unmarshalSimpleString(r io.Reader, buf []byte) ([]byte, error) {
	readBytes := 0
	for {
		for i := readBytes; i < len(buf)-1; i++ {
			if buf[i] == '\r' && buf[i+1] == '\n' {
				return buf[:i], nil
			}
		}
		origLen := len(buf)
		if origLen >= cap(buf) {
			// TODO this is a naive way if expanding the buffer
			buf = append(buf, make([]byte, cap(buf))...)
		}
		readBuf := buf[origLen:cap(buf)]
		readLen, err := r.Read(readBuf)
		readBytes += readLen
		if err != nil {
			return nil, err
		}
		buf = append(buf, readBuf...)
	}
}

// readInteger not sure it is the right choice here to actually return an int rather
// than follow BulkString and SimpleString conventions
func readInteger(r io.Reader, buf []byte) (i int, err error) {
	buf, err = readSwitch(':', unmarshalSimpleString, r, buf)
	if err != nil {
		return
	}
	i, err = strconv.Atoi(*(*string)(unsafe.Pointer(&buf)))
	if err != nil {
		return
	}
	return
}

func readBulkString(r io.Reader, buf []byte) ([]byte, error) {
	return readSwitch('$', unmarshalBulkString, r, buf)
}

func readSimpleString(r io.Reader, buf []byte) ([]byte, error) {
	return readSwitch('+', unmarshalSimpleString, r, buf)
}

type readFunc func(io.Reader, []byte) ([]byte, error)

func readSwitch(prefix byte, callback readFunc, r io.Reader, buf []byte) ([]byte, error) {
	var i int
	var err error
	i = len(buf)
	for i == 0 {

		buf = buf[:cap(buf)]
		i, err = r.Read(buf)
		if err != nil {
			return nil, err
		}
	}
	switch buf[0] {
	case prefix:
		// TODO this may have performance impact on larger data sizes, as it is a memcopy
		// But we want to make sure that the buf returned at the end is the same, so we need to
		copy(buf, buf[1:i]) // remove prefix
		return callback(r, buf[:i-1])
	case '-':
		str, err := unmarshalSimpleString(r, buf[1:i])
		if err != nil {
			return nil, err
		}
		return nil, errors.New(string(str))
	default:
		return nil, fmt.Errorf("Expected prefix '%s' or '-', but received '%s'", string(prefix), string(buf[0]))
	}
}

func readArray(r io.Reader, arrResult *ArrayReader) (*ArrayReader, error) {
	_, err := readSwitch('*', func(r io.Reader, b []byte) ([]byte, error) {
		sizeLen, size, err := unmarshalCount(r, b)
		if err != nil {
			return nil, err
		}
		copy(b, b[sizeLen:])
		arrResult.Init(b, size)
		return nil, nil

	}, r, arrResult.buf)
	if err != nil {
		return nil, err
	}
	return arrResult, err
}
