package greddis

import (
	"strconv"
)

func encodeBulkString(in []byte, buf []byte) []byte {
	if in == nil {
		buf = append(buf, []byte("$-1")...)
		buf = append(buf, sep...)
		return buf
	}
	buf = append(buf, '$')
	buf = strconv.AppendInt(buf, int64(len(in)), 10)
	buf = append(buf, sep...)
	buf = append(buf, in...)
	buf = append(buf, sep...)
	return buf
}

type respArray struct {
	origBuf []byte
	buf     []byte
	length  int64
}

var maxBulkSize = 512 * 1024 * 1024
var maxBulkLen = 9 + 3

// reset (ab)uses the fact that slices are pointers to arrays and contains cap + len
// By slicing to [:0] we set len to 0, but retain cap. We keep 12 bytes at the start of the
// buffer to write the array length to (in accordance with the format). Worst case, the
// buf that gets passed in doesn't fit all the items we want to send, then buf will
// point to another underlying array that is bigger, thus this will result in an allocation.
// We however correct this by using append() on origBuf and then return origBuf so the connection
// will have this new and larger buffer and will hopefully cause less allocations over time.
func (a *respArray) reset(buf []byte) {
	a.origBuf = buf[:0]
	if len(buf) < maxBulkLen { // Let's just pad so we don't panic
		buf = append(buf, make([]byte, maxBulkLen-len(buf))...)
	}
	a.buf = buf[maxBulkLen:]
	a.buf = a.buf[:0]
	a.length = 0
}

func (a *respArray) setToNil(buf []byte) {
	a.reset(buf)
	a.length = -1
}

func (a *respArray) addBulkString(item []byte) {
	a.buf = encodeBulkString(item, a.buf)
	a.length++
}

func (a *respArray) encode() []byte {
	a.origBuf = append(a.origBuf, '*')
	a.origBuf = strconv.AppendInt(a.origBuf, a.length, 10)
	a.origBuf = append(a.origBuf, sep...)
	a.origBuf = append(a.origBuf, a.buf...)
	return a.origBuf
}
