//go:generate mockgen -source client.go -destination ./mocks/mock_greddis/mock_client.go
package greddis

import (
	"context"
	"database/sql/driver"
	"time"
)

// TODO max lifetime
// TODO Do not read all of the contents from the connection into the buffer, just do the first read and stream
// the rest onto the variable sent in to Result.Scan..?

var (
	sep = []byte("\r\n")
)

// RedisPattern is a string that contains what is considered a pattern according to the spec here: https://redis.io/commands/KEYS
type RedisPattern string

// SubscribeHandler always receives the topic it is subscribed to together with the message
type SubscribeHandler func(ctx context.Context, topic string, message Result) error

// Allow client to be instantiated in 3 modes
// 1. with subscriber started before Subscribe call
// 2. lazy loading subscriber on first Subscribe
// 3. disallow calling Subscribe
type PubSubOpts struct {
	PingInterval time.Duration
	ReadTimeout  time.Duration
	InitBufSize  int
}

// TODO ClientOpts to encapsulate PubSubOpts and PoolOpts

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
	// Ping pings the server, mostly an internal command to ensure the subscription connection is still working
	Ping(ctx context.Context) error
	// Publish publishes a message to the selected topic, it returns an int of the number of clients
	// that received the message
	Publish(ctx context.Context, topic string, message driver.Value) (int, error)
	// Subscribe waits for the next message to arrive and returns only when a message is received
	Subscribe(ctx context.Context, topics ...interface{}) (msgChanMap *MessageChanMap, err error)
	// Unsubscribe closes the subscriptions on the channels given
	Unsubscribe(ctx context.Context, topics ...interface{}) (err error)
}

// NewClient returns a client with the options specified
func NewClient(ctx context.Context, opts *PoolOptions) (Client, error) {
	pool, err := newPool(ctx, opts)
	if err != nil {
		return nil, err
	}
	// TODO instantiate subManager
	return &client{
		pool:     pool,
		poolOpts: opts,
	}, nil
}

type client struct {
	pool     internalPool
	resBuf   *Result
	poolOpts *PoolOptions
	subMngr  *subManager
}

func (c *client) Get(ctx context.Context, key string) (*Result, error) {
	conn, err := c.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	conn.cmd.start(conn.buf, 2).add("GET").add(key)
	conn.buf, err = conn.cmd.flush()
	if err != nil {
		c.pool.Put(ctx, conn)
		conn.cmd.reset(conn.conn)
		return nil, err
	}
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
	if ttl > 0 {
		err = conn.cmd.start(conn.buf, 5).add("SET").add(key).addUnsafe(value)
		conn.cmd.add("EX").add(ttl)
	} else {
		err = conn.cmd.start(conn.buf, 3).add("SET").add(key).addUnsafe(value)
	}
	if err != nil {
		c.pool.Put(ctx, conn)
		conn.cmd.reset(conn.conn)
		return err
	}
	conn.buf, err = conn.cmd.flush()
	if err != nil {
		c.pool.Put(ctx, conn)
		conn.cmd.reset(conn.conn)
		return err
	}
	_, err = readSimpleString(conn.conn, conn.buf)
	c.pool.Put(ctx, conn)
	return err
}

func (c *client) Del(ctx context.Context, key string) error {
	conn, err := c.pool.Get(ctx)
	if err != nil {
		return err
	}
	conn.cmd.start(conn.buf, 2).add("DEL").add(key)
	conn.buf, err = conn.cmd.flush()
	if err != nil {
		conn.cmd.reset(conn.conn)
		c.pool.Put(ctx, conn)
		return err
	}
	_, err = readSimpleString(conn.conn, conn.buf)
	c.pool.Put(ctx, conn)
	return err
}

func (c *client) Ping(ctx context.Context) error {
	conn, err := c.pool.Get(ctx)
	if err != nil {
		return err
	}
	err = ping(ctx, conn)
	c.pool.Put(ctx, conn)
	return err
}

func (c *client) Publish(ctx context.Context, topic string, value driver.Value) (int, error) {
	conn, err := c.pool.Get(ctx)
	if err != nil {
		return 0, err
	}
	i, err := publish(ctx, conn, topic, value)
	c.pool.Put(ctx, conn)
	return i, err
}

func (c *client) Subscribe(ctx context.Context, topics ...interface{}) (*MessageChanMap, error) {
	conn := c.subMngr.getConn()
	err := conn.cmd.subscribe(ctx, topics)
	if err != nil {
		conn.cmd.reset(conn.conn)
		return nil, err
	}
	c.subMngr.putConn(conn)
	return nil, nil
}

func (c *client) Unsubscribe(ctx context.Context, topics ...interface{}) error {
	conn := c.subMngr.getConn()
	err := unsubscribe(ctx, conn, topics)
	if err != nil {
		conn.cmd.reset(conn.conn)
		return err
	}
	c.subMngr.putConn(conn)
	return nil
}
