package redis

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"
)

func parseBulkString(r io.Reader, buf []byte, timeout time.Duration) ([]byte, error) {
	var intBuf, err = parseSimpleString(r, buf, timeout)
	if err != nil {
		return nil, err
	}
	var size int
	size, err = strconv.Atoi(string(intBuf))
	if size < 0 {
		return nil, nil
	}
	var lenSize = len(intBuf) + 2 // + 2 for sep
	var origCap = cap(buf)
	var sizeDiff = size + 2 - origCap
	if sizeDiff > 0 {
		var origLen = len(buf)
		buf = buf[:origCap]
		buf = append(buf, make([]byte, sizeDiff)...)
		buf = buf[:origLen]
	} else {
		sizeDiff = 0
	}
	// Cannot slice []bytes from the start if you plan to keep the underlying array, use copy() to shift instead
	copy(buf, buf[lenSize:cap(buf)]) // we're also copying until cap to zero out whatever was on the buf before
	buf = buf[:len(buf)-lenSize]
	var t = time.Now()
	for len(buf) < size+2 {
		var prevLen = len(buf)
		var readBuf = buf[prevLen : size+2]
		var i, err = r.Read(readBuf)
		buf = buf[:prevLen+i]
		if err != nil {
			return nil, fmt.Errorf("Error reading from connection: '%s'", err)
		}
		if time.Now().Sub(t) > timeout {
			return nil, ErrNoData
		}
	}
	if !bytes.Equal(buf[size:size+2], sep) {
		return buf, ErrMalformedString
	}
	return buf[:size], err
}

func parseSimpleString(r io.Reader, buf []byte, timeout time.Duration) ([]byte, error) {
	var t = time.Now()
	var readBuf []byte
	for {
		for i := 0; i < len(buf)-1; i++ {
			if buf[i] == '\r' {
				if buf[i+1] == '\n' {
					return buf[:i], nil
				}
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
		if time.Now().Sub(t) > timeout {
			return nil, ErrNoData
		}
	}
}

// readInteger not sure it is the right choice here to actually return an int rather
// than follow BulkString and SimpleString conventions
func readInteger(r io.Reader, buf []byte, timeout time.Duration) (int, error) {
	var err error
	buf, err = readSwitch(':', parseSimpleString, r, buf, timeout)
	if err != nil {
		return 0, err
	}
	var i int
	i, err = strconv.Atoi(string(buf))
	if err != nil {
		return 0, err
	}
	return i, nil
}

func readBulkString(r io.Reader, buf []byte, timeout time.Duration) ([]byte, error) {
	return readSwitch('$', parseBulkString, r, buf, timeout)
}

func readSimpleString(r io.Reader, buf []byte, timeout time.Duration) ([]byte, error) {
	return readSwitch('+', parseSimpleString, r, buf, timeout)
}

type readFunc func(io.Reader, []byte, time.Duration) ([]byte, error)

func readSwitch(prefix byte, callback readFunc, r io.Reader, buf []byte, timeout time.Duration) ([]byte, error) {
	buf = buf[:cap(buf)]
	var i, err = r.Read(buf)
	if err != nil {
		return nil, err
	}
	switch buf[0] {
	case prefix:
		copy(buf, buf[1:i])
		return callback(r, buf[:i-1], timeout)
	case '-':
		var str, err = parseSimpleString(r, buf[1:i], timeout)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(string(str))
	default:
		return nil, fmt.Errorf("Expected prefix '%s' or '-', but received '%s'", string(prefix), string(buf[0]))
	}
}
