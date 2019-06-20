package greddis

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"unsafe"
)

func parseBulkString(r io.Reader, buf []byte) ([]byte, error) {
	var intBuf, err = parseSimpleString(r, buf)
	if err != nil {
		return nil, err
	}
	var size int
	// zero-alloc conversion, ref: https://golang.org/src/strings/builder.go#L45
	size, err = strconv.Atoi(*(*string)(unsafe.Pointer(&intBuf)))
	if size < 0 || err != nil {
		return nil, err
	}
	size = size + 2
	var lenSize = len(intBuf) + 2 // + 2 for sep
	if size-cap(buf) > 0 {
		var newBuf = make([]byte, 0, size)
		copy(newBuf, buf[lenSize:])
		buf = newBuf[:len(buf)-lenSize]
	} else {
		copy(buf, buf[lenSize:])
		buf = buf[:len(buf)-lenSize] // let's remove the size prefix before comparing
	}
	if len(buf) < size {
		var readBuf = buf[len(buf):size] // out of range
		_, err = io.ReadFull(r, readBuf)
		if err != nil {
			return nil, err
		}
		buf = buf[:size]
	}
	if !bytes.Equal(buf[size-2:size], sep) {
		return buf, ErrMalformedString
	}
	return buf[:size-2], err
}

func parseSimpleString(r io.Reader, buf []byte) ([]byte, error) {
	var readBuf []byte
	for {
		for i := 0; i < len(buf)-1; i++ {
			if buf[i] == '\r' && buf[i+1] == '\n' {
				return buf[:i], nil
			}
		}
		var origLen = len(buf)
		if origLen < cap(buf) {
			readBuf = buf[origLen:cap(buf)]
		} else {
			buf = append(buf, make([]byte, cap(buf))...)
			readBuf = buf[origLen:cap(buf)]
		}
		var _, err = r.Read(readBuf)
		if err != nil {
			return nil, err
		}
		buf = append(buf, readBuf...)
	}
}

// readInteger not sure it is the right choice here to actually return an int rather
// than follow BulkString and SimpleString conventions
func readInteger(r io.Reader, buf []byte) (int, error) {
	var err error
	buf, err = readSwitch(':', parseSimpleString, r, buf)
	if err != nil {
		return 0, err
	}
	var i int
	i, err = strconv.Atoi(*(*string)(unsafe.Pointer(&buf)))
	if err != nil {
		return 0, err
	}
	return i, nil
}

func readBulkString(r io.Reader, buf []byte) ([]byte, error) {
	return readSwitch('$', parseBulkString, r, buf)
}

func readSimpleString(r io.Reader, buf []byte) ([]byte, error) {
	return readSwitch('+', parseSimpleString, r, buf)
}

type readFunc func(io.Reader, []byte) ([]byte, error)

func readSwitch(prefix byte, callback readFunc, r io.Reader, buf []byte) ([]byte, error) {
	buf = buf[:cap(buf)]
	var i, err = r.Read(buf)
	if err != nil {
		return nil, err
	}
	switch buf[0] {
	case prefix:
		copy(buf, buf[1:i])
		return callback(r, buf[:i-1])
	case '-':
		var str, err = parseSimpleString(r, buf[1:i])
		if err != nil {
			return nil, err
		}
		return nil, errors.New(string(str))
	default:
		return nil, fmt.Errorf("Expected prefix '%s' or '-', but received '%s'", string(prefix), string(buf[0]))
	}
}
