package greddis

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"unsafe"
)

func unmarshalBulkString(r io.Reader, buf []byte) ([]byte, error) {
	var err error
	intBuf, err := unmarshalSimpleString(r, buf)
	if err != nil {
		return nil, err
	}
	sizeLen := len(intBuf) + len(sep)
	// zero-alloc conversion, ref: https://golang.org/src/strings/builder.go#L45
	size, err := strconv.Atoi(*(*string)(unsafe.Pointer(&intBuf)))
	if size < 0 || err != nil {
		return nil, err
	}
	size = size + 2
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
	if !bytes.Equal(buf[size-2:size], sep) {
		return buf, ErrMalformedString
	}
	return buf[:size-2], nil
}

func unmarshalSimpleString(r io.Reader, buf []byte) ([]byte, error) {
	for {
		for i := 0; i < len(buf)-1; i++ {
			if buf[i] == '\r' && buf[i+1] == '\n' {
				return buf[:i], nil
			}
		}
		origLen := len(buf)
		if origLen >= cap(buf) {
			buf = append(buf, make([]byte, cap(buf))...)
		}
		readBuf := buf[origLen:cap(buf)]
		_, err := r.Read(readBuf)
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
		return 0, err
	}
	i, err = strconv.Atoi(*(*string)(unsafe.Pointer(&buf)))
	if err != nil {
		return 0, err
	}
	return i, nil
}

func readBulkString(r io.Reader, buf []byte) ([]byte, error) {
	return readSwitch('$', unmarshalBulkString, r, buf)
}

func readSimpleString(r io.Reader, buf []byte) ([]byte, error) {
	return readSwitch('+', unmarshalSimpleString, r, buf)
}

type readFunc func(io.Reader, []byte) ([]byte, error)

func readSwitch(prefix byte, callback readFunc, r io.Reader, buf []byte) ([]byte, error) {
	buf = buf[:cap(buf)]
	i, err := r.Read(buf)
	if err != nil {
		return nil, err
	}
	switch buf[0] {
	case prefix:
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
