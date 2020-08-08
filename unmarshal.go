package greddis

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"unsafe"
)

func unmarshalCount(r io.Reader, buf []byte) (int, int, []byte, error) {
	n, buf, err := unmarshalSimpleString(r, buf)
	if err != nil {
		return 0, 0, buf, err
	}
	intBuf := buf[:n-len(sep)]
	// zero-alloc conversion, ref: https://golang.org/src/strings/builder.go#L45
	size, err := strconv.Atoi(*(*string)(unsafe.Pointer(&intBuf)))
	if err != nil {
		return 0, 0, buf, err
	}
	return n, size, buf, nil
}

func unmarshalBulkString(r io.Reader, buf []byte) (int, []byte, error) {
	readBytes, size, buf, err := unmarshalCount(r, buf)
	if err != nil || size == -1 {
		return readBytes, nil, err
	}
	size = size + len(sep)
	truncate := readBytes + size
	oldBuf := buf
	// if there's not enough space, create a new []byte buffer
	if truncate-cap(buf) > 0 {
		truncate = cap(oldBuf)
	}
	if size-cap(buf) > 0 {
		buf = make([]byte, size)
	}
	// "remove" size prefix from []byte buffer
	if len(oldBuf) > 0 {
		// TODO this is a waste of time
		copy(buf, oldBuf[readBytes:truncate])
		buf = buf[:len(oldBuf)-readBytes]
	}
	if len(buf) < size {
		// only pass in the bytes we want read to (and are missing)
		readBuf := buf[len(buf):size]
		_, err := io.ReadFull(r, readBuf)
		if err != nil {
			return readBytes, nil, err
		}
		buf = buf[:size]
	}
	if !bytes.Equal(buf[size-len(sep):size], sep) {
		return readBytes, buf, ErrMalformedString
	}
	return readBytes + size, buf[:size-len(sep)], nil
}

func unmarshalSimpleString(r io.Reader, buf []byte) (int, []byte, error) {
	readBytes := 0
	for {
		for readBytes < len(buf)-1 {
			if buf[readBytes] == '\r' && buf[readBytes+1] == '\n' {
				return readBytes + 2, buf, nil
			}
			readBytes++
		}
		origLen := len(buf)
		if origLen >= cap(buf) {
			// TODO this is a naive way of expanding the buffer
			buf = append(buf, make([]byte, cap(buf))...)
		}
		readBuf := buf[origLen:cap(buf)]
		i, err := r.Read(readBuf)
		if err != nil {
			return 0, nil, err
		}
		buf = buf[:origLen+i]
	}
}

// readInteger not sure it is the right choice here to actually return an int rather
// than follow BulkString and SimpleString conventions
func readInteger(r io.Reader, buf []byte) (n int, i int, err error) {
	n, buf, err = readSwitch(':', unmarshalSimpleString, r, buf)
	if err != nil {
		return
	}
	intBuf := buf[:n-len(sep)-1]
	i, err = strconv.Atoi(*(*string)(unsafe.Pointer(&intBuf)))
	if err != nil {
		return
	}
	return
}

func readBulkString(r io.Reader, buf []byte) (int, []byte, error) {
	return readSwitch('$', unmarshalBulkString, r, buf)
}

func readSimpleString(r io.Reader, buf []byte) (int, []byte, error) {
	n, buf, err := readSwitch('+', unmarshalSimpleString, r, buf)
	truncate := n - len(sep) - 1
	if truncate < 0 {
		truncate = 0
	}
	return n, buf[:truncate], err
}

type readFunc func(io.Reader, []byte) (int, []byte, error)

func readSwitch(prefix byte, callback readFunc, r io.Reader, buf []byte) (int, []byte, error) {
	var i int
	var err error
	i = len(buf)
	for i == 0 {
		buf = buf[:cap(buf)]
		i, err = r.Read(buf)
		if err != nil {
			return i, nil, err
		}
	}
	switch buf[0] {
	case prefix:
		// TODO this may have performance impact on larger data sizes, as it is a memcopy
		// But we want to make sure that the buf returned at the end is the same, so we need to
		copy(buf, buf[1:i]) // remove prefix
		n, buf, err := callback(r, buf[:i-1])
		return n + 1, buf, err // need to account for the prefix length as well
	case '-':
		n, str, err := unmarshalSimpleString(r, buf[1:i])
		if err != nil {
			return n + 1, nil, err
		}
		return 1 + n, nil, errors.New(string(str))
	default:
		return 0, nil, fmt.Errorf("Expected prefix '%s' or '-', but received '%s'", string(prefix), string(buf[0]))
	}
}

func readArray(r io.Reader, arrResult *ArrayReader) (*ArrayReader, error) {
	_, _, err := readSwitch('*', func(r io.Reader, b []byte) (int, []byte, error) {
		readBytes, size, b, err := unmarshalCount(r, b)
		if err != nil {
			return readBytes, nil, err
		}
		copy(b, b[readBytes:])
		arrResult.Init(b, size)
		return readBytes, nil, nil

	}, r, arrResult.buf)
	if err != nil {
		return nil, err
	}
	return arrResult, err
}
