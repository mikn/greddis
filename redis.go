package redis

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strconv"
)

// TODO Ensure we return a connection error instead of panic with nil ref when there's no connection
// TODO Parse a DSN instead of asking people to provide a Dial function with the address in it
// TODO stats
// TODO tracing
// TODO max lifetime

var (
	sep = []byte("\r\n")
)

// Client is the interface to interact with Redis . It uses connections
// with a single buffer attached, much like the MySQL driver implementation,
// which allows it to reduce stack allocations. It uses the same []byte buffer
// to interact with Redis to save memory.
type Client interface {
	// Get executes a get command on a redis server and returns a Result type, which you can use Scan
	// on to get the result put into a variable
	Get(key string) (Result, error)
	// Set sets a Value in redis, it accepts a TTL which can be put to 0 to disable TTL
	Set(key string, value driver.Value, ttl int) error
	// Del removes a key from the redis server
	Del(key string) error
}

// NewClient returns a client with the options specified
func NewClient(ctx context.Context, opts *PoolOptions) Client {
	return &client{
		pool:     newPool(ctx, opts),
		resBuf:   &result{},
		poolOpts: opts,
	}
}

type client struct {
	pool     internalPool
	resBuf   *result
	poolOpts *PoolOptions
}

func (c *client) Get(key string) (Result, error) {
	var conn, err = c.pool.Get()
	if err != nil {
		return nil, err
	}
	var cmd = conn.cmd
	cmd.addItem([]byte("GET"))
	cmd.addItem([]byte(key))
	conn.buf = cmd.writeTo(conn.conn)
	var buf = conn.buf[:0]
	buf, err = readBulkString(conn.conn, buf, c.poolOpts.ReadTimeout)
	conn.buf = buf
	if err != nil {
		c.pool.Put(conn)
		return nil, err
	}
	return c.getResult(conn, buf), err
}

func (c *client) Set(key string, value driver.Value, ttl int) error {
	var conn, err = c.pool.Get()
	if err != nil {
		return err
	}
	var val []byte
	val, err = c.getBytesValue(value, conn.buf[:0])
	if err != nil {
		return err
	}
	conn.cmd.addItem([]byte("SET"))
	conn.cmd.addItem([]byte(key))
	conn.cmd.addItem(val)
	if ttl > 0 {
		conn.cmd.addItem([]byte("EX"))
		conn.cmd.addItem([]byte(strconv.Itoa(ttl)))
	}
	conn.buf = conn.cmd.writeTo(conn.conn)
	_, err = readSimpleString(conn.conn, conn.buf, c.poolOpts.ReadTimeout)
	c.pool.Put(conn)
	return err
}

func (c *client) Del(key string) error {
	var conn, err = c.pool.Get()
	if err != nil {
		return err
	}
	var cmd = conn.cmd
	cmd.addItem([]byte("DEL"))
	cmd.addItem([]byte(key))
	conn.buf = cmd.writeTo(conn.conn)
	_, err = readSimpleString(conn.conn, conn.buf, c.poolOpts.ReadTimeout)
	c.pool.Put(conn)
	return err
}

func (c *client) getResult(conn *conn, buf []byte) *result {
	c.resBuf.conn = conn
	c.resBuf.pool = c.pool
	c.resBuf.value = buf
	return c.resBuf
}

func (c *client) getBytesValue(value driver.Value, buf []byte) ([]byte, error) {
	switch d := value.(type) {
	case string:
		buf = append(buf, d...)
		return buf, nil
	case []byte:
		buf = append(buf, d...)
		return buf, nil
	case int:
		buf = strconv.AppendInt(buf, int64(d), 10)
		return buf, nil
	case *string:
		buf = append(buf, *d...)
		return buf, nil
	case *[]byte:
		buf = append(buf, *d...)
		return buf, nil
	case *int:
		buf = strconv.AppendInt(buf, int64(*d), 10)
		return buf, nil
	case driver.Valuer:
		var val, err = d.Value()
		if err != nil {
			return nil, err
		}
		return val.([]byte), err
	}
	return nil, fmt.Errorf("Got non-parseable value")
}
