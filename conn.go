package greddis

import (
	"net"
	"sync/atomic"
	"time"
)

type conn struct {
	conn        net.Conn
	cmd         *command
	buf         []byte
	initBufSize int
	created     time.Time
	toBeClosed  int64 // only for use with atomics
	inUse       int64 // only for use with atomics
}

func newConn(c net.Conn, bufSize int) *conn {
	var buf = make([]byte, bufSize)
	var cmd = &command{}
	cmd.array = &respArray{}
	cmd.array.reset(buf)
	return &conn{
		conn:        c,
		buf:         buf,
		cmd:         cmd,
		initBufSize: bufSize,
		created:     time.Now(),
		toBeClosed:  0,
		inUse:       0,
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
	} else {
		truth = 0
	}
	atomic.StoreInt64(&c.inUse, truth)
}
