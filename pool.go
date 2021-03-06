package greddis

import (
	"bufio"
	"context"
	"math"
	"net"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/beorn7/perks/quantile"
)

type bufQuantile interface {
	Observe(float64)
	Results() map[float64]float64
	Count() int
}

type internalPool interface {
	Get(context.Context) (*conn, error)
	Put(context.Context, *conn)
}

type pool struct {
	conns           chan *conn
	connsRef        []*conn
	connsMut        sync.Mutex
	maxSize         int
	dial            func(context.Context) (net.Conn, error)
	opts            *PoolOptions
	bufSizeQuantile bufQuantile
	targetBufSize   int
}

// TrimOptions represents advanced options to fine-tune the trimming background thread for connections
type TrimOptions struct {
	Interval           time.Duration // default is 500 ms
	BufQuantileTargets []float64     // the targets to track in 0-1 percentiles, if none given, the default is []float{0.8, 1}
	BufQuantileTarget  float64       // The specific target for buf size. If invalid or omitted, it will pick the first (a[0]) percentile from BufQuantileTargets
	AllowedMargin      float64       // this is 0-1 representing a fraction of how far from the lowest percentile a higher one can be so that it shifts to using a higher percentile. To disable it, set it below 0. It can also go higher than 1, but we recommend targeting a higher percentile instead. The default is 0.1
}

func initTrimOptions(opts *TrimOptions) *TrimOptions {
	if opts == nil {
		opts = &TrimOptions{}
	}
	if opts.Interval == 0 {
		opts.Interval = 500 * time.Millisecond
	}
	if opts.BufQuantileTargets == nil {
		opts.BufQuantileTargets = []float64{0.8, 1}
	}
	targetExists := false
	for _, v := range opts.BufQuantileTargets {
		if v == opts.BufQuantileTarget {
			targetExists = true
			break
		}
	}
	if !targetExists {
		opts.BufQuantileTarget = opts.BufQuantileTargets[0]
	}
	if opts.AllowedMargin == 0 {
		opts.AllowedMargin = 0.1
	}
	if opts.AllowedMargin < 0 {
		opts.AllowedMargin = 0
	}
	return opts
}

func connTrimming(ctx context.Context, tick <-chan time.Time, pool *pool) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			pool.connsMut.Lock()
			// check whether we have too many idle connections
			for i := len(pool.conns); i > pool.opts.MaxIdle; i-- {
				c := <-pool.conns
				c.setToBeClosed()
			}
			// close connections scheduled for closing
			filtered := pool.connsRef[:0] // (ab)use the relationship of slice/array
			for _, conn := range pool.connsRef {
				if conn.getToBeClosed() && !conn.getInUse() {
					conn.conn.Close()
				} else {
					filtered = append(filtered, conn)
				}
			}
			// ensure the closed connections that didn't get overwritten also gets fed to the GC
			pool.connsRef = filtered

			// work out the new target buf size
			pool.targetBufSize = pool.calcTargetBufSize(pool.bufSizeQuantile, pool.opts.TrimOptions.BufQuantileTarget)
			// Resize buffers according to targetBufSize
			for _, conn := range pool.connsRef {
				if conn.r.r.Size() > pool.targetBufSize && !conn.getInUse() {
					conn.r.r = bufio.NewReaderSize(conn.conn, pool.targetBufSize)
				}
			}
			pool.connsMut.Unlock()
		}
	}
}

type dialFunc func(context.Context) (net.Conn, error)

// PoolOptions is specified to tune the connection pool for the client
type PoolOptions struct {
	// MaxSize is the maximum size of the connection pool. If reached, it will
	// block the next client request until a connection is free
	MaxSize int
	// MaxIdle How many connections that can remain idle in the pool, will
	// otherwise be reaped by the trimming thread
	// Default: 5
	MaxIdle int
	// How long before reads time out
	// Default: 500 ms
	ReadTimeout time.Duration
	// URL is the protocol, address and database selection concatenated into
	// a URL format. Inspired by DSN.
	URL string
	// Dial is a function that returns an established net.Conn
	Dial dialFunc
	// InitialBufSize is the initial buffer size to associate with the
	// connection, this is also the minimum buffer size allowed when creating
	// new connections, but if the trimming thread is enabled and the
	// percentile target returns a higher value, this will be used for
	// any subsequent connections
	// Default: 4096 bytes
	InitialBufSize int
	// TrimOptions are fine-tuning options for the trimming go routine, this
	// should usually not be needed
	TrimOptions     *TrimOptions
	bufSizeQuantile bufQuantile
}

func createDial(rawURL string) (dialFunc, error) {
	url, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) (net.Conn, error) {
		return net.Dial(url.Scheme, url.Host)
	}, nil
}

func newPool(ctx context.Context, opts *PoolOptions) (internalPool, error) {
	opts.TrimOptions = initTrimOptions(opts.TrimOptions)
	if opts.InitialBufSize < 1 {
		opts.InitialBufSize = 4096
	}
	if opts.ReadTimeout == 0 {
		opts.ReadTimeout = 500 * time.Millisecond
	}
	if opts.Dial != nil && opts.URL != "" {
		return nil, ErrOptsDialAndURL
	}
	if opts.URL != "" {
		dial, err := createDial(opts.URL)
		if err != nil {
			return nil, err
		}
		opts.Dial = dial
	}
	if opts.MaxSize < 1 {
		opts.MaxSize = 5
	}
	p := &pool{
		conns:           make(chan *conn, opts.MaxSize),
		maxSize:         opts.MaxSize,
		dial:            opts.Dial,
		connsMut:        sync.Mutex{},
		bufSizeQuantile: newQuantileStream(opts.TrimOptions.BufQuantileTargets),
		targetBufSize:   opts.InitialBufSize,
		opts:            opts,
	}
	if opts.TrimOptions.Interval > 0 {
		go connTrimming(ctx, time.Tick(opts.TrimOptions.Interval), p)

	}
	return p, nil
}

// calcTargetBufSize takes a quantile and selects a sane new target []byte buffer size for connections in the pool
// it takes several measures to ensure that it doesn't thrash too much on the targetBufSize
func (p *pool) calcTargetBufSize(q bufQuantile, target float64) int {
	// only spend time if we have enough samples
	if q.Count() >= 100 {
		results := q.Results()
		targetResult := results[target]
		if targetResult < float64(p.opts.InitialBufSize) {
			// make sure we don't reduce buf size below initial size
			return p.opts.InitialBufSize
		}
		if math.Abs(targetResult-float64(p.targetBufSize)) < targetResult*0.1 {
			// avoid too much bouncing of targetBufSize
			return p.targetBufSize
		}
		orderedResults := make([]float64, 0, len(results))
		for q := range results {
			orderedResults = append(orderedResults, q)
		}
		sort.Sort(sort.Reverse(sort.Float64Slice(orderedResults)))
		for _, percentile := range orderedResults {
			lastVal := results[percentile]
			if targetResult+targetResult*p.opts.TrimOptions.AllowedMargin > lastVal {
				return int(lastVal)
			}
		}
	}
	return p.opts.InitialBufSize
}

func (p *pool) Get(ctx context.Context) (*conn, error) {
	var c *conn // don't move to exit vars - results in alloc for some reason
	var err error
	select {
	case c = <-p.conns:
		if c.getToBeClosed() {
			c, err = p.Get(ctx)
		}
	default:
		conn, err := p.dial(ctx)
		if err != nil {
			return nil, err
		}
		c = newConn(conn, p.targetBufSize)
		// when result is Scanned, put conn back to pool
		c.res.finish = func() {
			p.Put(ctx, c)
		}
		p.connsMut.Lock()
		p.connsRef = append(p.connsRef, c)
		p.connsMut.Unlock()
	}
	c.setInUse(true)
	c.conn.SetReadDeadline(time.Now().Add(p.opts.ReadTimeout))
	return c, err
}

func (p *pool) Put(ctx context.Context, c *conn) {
	c.setInUse(false)
	if c.getToBeClosed() {
		return
	}
	c.conn.SetReadDeadline(time.Time{})
	p.bufSizeQuantile.Observe(float64(c.r.r.Size()))
	select {
	case p.conns <- c:
	default:
	}
}

type quantileStream struct {
	targets []float64
	q       *quantile.Stream
	mut     sync.RWMutex
}

func sliceToTargets(slice []float64) map[float64]float64 {
	formattedTarget := make(map[float64]float64, len(slice))
	for _, target := range slice {
		formattedTarget[target] = 1 - target
		if target != 1 {
			formattedTarget[target] = formattedTarget[target] / 10
		}
	}
	return formattedTarget
}

func newQuantileStream(targets []float64) bufQuantile {
	q := &quantileStream{
		targets: targets,
		q:       quantile.NewTargeted(sliceToTargets(targets)),
		mut:     sync.RWMutex{},
	}
	return q
}

func (q *quantileStream) Observe(v float64) {
	q.mut.Lock()
	q.q.Insert(v)
	q.mut.Unlock()
}

func (q *quantileStream) Results() map[float64]float64 {
	result := make(map[float64]float64, len(q.targets))
	q.mut.RLock()
	for _, target := range q.targets {
		result[target] = q.q.Query(target)
	}
	q.mut.RUnlock()
	return result
}

func (q *quantileStream) Count() int {
	q.mut.RLock()
	count := q.q.Count()
	q.mut.RUnlock()
	return count
}
