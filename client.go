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

type PubSubOpts struct {
	PingInterval time.Duration
	ReadTimeout  time.Duration
	InitBufSize  int
}

// TODO ClientOpts to encapsulate PubSubOpts and PoolOpts

type Subscriber interface {
	// Subscribe returns a map of channels corresponding to the string value of the topics being subscribed to
	Subscribe(ctx context.Context, topics ...interface{}) (msgChanMap MessageChanMap, err error)
	// Unsubscribe closes the subscriptions on the channels given
	Unsubscribe(ctx context.Context, topics ...interface{}) (err error)
}

// Client is the interface to interact with Redis. It uses connections
// with a single buffer attached, much like the MySQL driver implementation.
// This allows it to reduce stack allocations.
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
	Publish(ctx context.Context, topic string, message driver.Value) (recvCount int, err error)
}

type SubClient interface {
	Subscriber
	Client
}

// NewClient returns a client with the options specified
func NewClient(ctx context.Context, opts *PoolOptions) (SubClient, error) {
	pool, err := newPool(ctx, opts)
	if err != nil {
		return nil, err
	}
	subMngr := newSubscriptionManager(pool, &PubSubOpts{
		PingInterval: 5 * time.Second,
		ReadTimeout:  opts.ReadTimeout,
		InitBufSize:  opts.InitialBufSize,
	})
	return &client{
		pool:     pool,
		poolOpts: opts,
		subMngr:  subMngr,
	}, nil
}

type client struct {
	pool     internalPool
	resBuf   *Result
	poolOpts *PoolOptions
	subMngr  *subscriptionManager
}

func (c *client) Get(ctx context.Context, key string) (*Result, error) {
	conn, err := c.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	conn.arrw.Init(2).AddString("GET", key)
	err = conn.arrw.Flush()
	if err != nil {
		c.pool.Put(ctx, conn)
		conn.arrw.Reset(conn.conn)
		return nil, err
	}
	err = conn.r.Next(ScanBulkString)
	if err != nil {
		c.pool.Put(ctx, conn)
		return nil, err
	}
	// not putting connection back here as it is put back on Result.Scan
	return conn.res, err
}

func (c *client) Set(ctx context.Context, key string, value driver.Value, ttl int) error {
	conn, err := c.pool.Get(ctx)
	if err != nil {
		return err
	}
	if ttl > 0 {
		err = conn.arrw.Init(5).AddString("SET", key).Add(value, "EX", StrInt(ttl))
	} else {
		err = conn.arrw.Init(3).AddString("SET", key).Add(value)
	}
	if err != nil {
		c.pool.Put(ctx, conn)
		conn.arrw.Reset(conn.conn)
		return err
	}
	err = conn.arrw.Flush()
	if err != nil {
		c.pool.Put(ctx, conn)
		conn.arrw.Reset(conn.conn)
		return err
	}
	err = conn.r.Next(ScanSimpleString)
	c.pool.Put(ctx, conn)
	return err
}

func (c *client) Del(ctx context.Context, key string) error {
	conn, err := c.pool.Get(ctx)
	if err != nil {
		return err
	}
	conn.arrw.Init(2).AddString("DEL", key)
	err = conn.arrw.Flush()
	if err != nil {
		conn.arrw.Reset(conn.conn)
		c.pool.Put(ctx, conn)
		return err
	}
	err = conn.r.Next(ScanSimpleString)
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
	if err := conn.arrw.Init(3).AddString("PUBLISH", topic).Add(value); err != nil {
		conn.arrw.Reset(conn.conn)
		c.pool.Put(ctx, conn)
		return 0, err
	}
	if err := conn.arrw.Flush(); err != nil {
		c.pool.Put(ctx, conn)
		return 0, err
	}
	err = conn.r.Next(ScanInteger)
	c.pool.Put(ctx, conn)
	return conn.r.Int(), err
}

func (c *client) Subscribe(ctx context.Context, topics ...interface{}) (MessageChanMap, error) {
	chanMap, err := c.subMngr.Subscribe(ctx, topics...)
	if err != nil {
		return nil, err
	}
	return chanMap, nil
}

func (c *client) Unsubscribe(ctx context.Context, topics ...interface{}) error {
	err := c.subMngr.Unsubscribe(ctx, topics...)
	if err != nil {
		return err
	}
	return err
}
