//go:generate mockgen -source client.go -destination ./mocks/mock_greddis/mock_client.go
package greddis

import (
	"context"
	"database/sql/driver"
)

// TODO max lifetime

var (
	sep = []byte("\r\n")
)

// Client is the interface to interact with Redis. It uses connections
// with a single buffer attached, much like the MySQL driver implementation.
// This allows it to reduce stack allocations. It uses the same []byte buffer
// to send commands to Redis to save memory.
type Client interface {
	// Get executes a get command on a redis server and returns a Result type, which you can use Scan
	// on to get the result put into a variable
	Get(ctx context.Context, key string) (*Result, error)
	// Set sets a Value in redis, it accepts a TTL which can be put to 0 to disable TTL
	Set(ctx context.Context, key string, value driver.Value, ttl int) error
	// Del removes a key from the redis server
	Del(ctx context.Context, key string) error
}

// NewClient returns a client with the options specified
func NewClient(ctx context.Context, opts *PoolOptions) (Client, error) {
	pool, err := newPool(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &client{
		pool:     pool,
		poolOpts: opts,
	}, nil
}

type putConn struct {
	pool internalPool
	conn *conn
}

type client struct {
	pool     internalPool
	resBuf   *Result
	poolOpts *PoolOptions
	putConn  *putConn
}

func (c *client) Get(ctx context.Context, key string) (*Result, error) {
	conn, err := c.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	conn.cmd.add("GET").add(key)
	conn.buf = conn.cmd.writeTo(conn.conn)
	conn.buf, err = readBulkString(conn.conn, conn.buf[:0])
	if err != nil {
		c.pool.Put(ctx, conn)
		return nil, err
	}
	conn.res.value = conn.buf
	// not putting connection back here as it is put back on Result.Scan
	return conn.res, err
}

func (c *client) Set(ctx context.Context, key string, value driver.Value, ttl int) error {
	conn, err := c.pool.Get(ctx)
	if err != nil {
		return err
	}
	val, err := toBytesValue(value, conn.buf[:0])
	if err != nil {
		c.pool.Put(ctx, conn)
		return err
	}
	conn.cmd.add("SET").add(key).add(val)
	if ttl > 0 {
		conn.cmd.add("EX").add(ttl)
	}
	conn.buf = conn.cmd.writeTo(conn.conn)
	_, err = readSimpleString(conn.conn, conn.buf)
	c.pool.Put(ctx, conn)
	return err
}

func (c *client) Del(ctx context.Context, key string) error {
	conn, err := c.pool.Get(ctx)
	if err != nil {
		return err
	}
	conn.cmd.add("DEL").add(key)
	conn.buf = conn.cmd.writeTo(conn.conn)
	_, err = readSimpleString(conn.conn, conn.buf)
	c.pool.Put(ctx, conn)
	return err
}
