package greddis

import (
	"bufio"
	"net"
	"sync/atomic"
	"time"
)

type conn struct {
	conn       net.Conn
	arrw       *ArrayWriter
	res        *Result
	r          *Reader
	created    time.Time
	toBeClosed int64 // only for use with atomics
	inUse      int64 // only for use with atomics
}

func newConn(c net.Conn, initBufSize int) *conn {
	r := NewReader(bufio.NewReaderSize(c, initBufSize))
	return &conn{
		conn:       c,
		r:          r,
		arrw:       NewArrayWriter((bufio.NewWriter(c))),
		res:        NewResult(r),
		created:    time.Now(),
		toBeClosed: 0,
		inUse:      0,
	}
}

func (c *conn) getToBeClosed() bool {
	return (atomic.LoadInt64(&c.toBeClosed) == 1)
}

func (c *conn) setToBeClosed() {
	atomic.StoreInt64(&c.toBeClosed, 1)
}

func (c *conn) getInUse() bool {
	return (atomic.LoadInt64(&c.inUse) == 1)
}

func (c *conn) setInUse(yes bool) {
	var truth int64
	if yes {
		truth = 1
	}
	atomic.StoreInt64(&c.inUse, truth)
}
