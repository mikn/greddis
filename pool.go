package redis

import (
	"context"
	"math"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beorn7/perks/quantile"
)

type bufQuantile interface {
	observe(float64)
	results() map[float64]float64
	count() int
}

type internalPool interface {
	Get() (*conn, error)
	Put(*conn)
}

type pool struct {
	conns           chan *conn
	connsRef        []*conn
	connsMut        sync.Mutex
	maxSize         int
	dial            func() (net.Conn, error)
	opts            *PoolOptions
	bufSizeQuantile bufQuantile
	quantileMut     sync.Mutex
	quantileValue   atomic.Value
	targetBufSize   int
}

type TrimOptions struct {
	Interval           time.Duration // default is 500 ms
	BufQuantileTargets []float64     // the targets to track
	BufQuantileTarget  float64       // The specific target for buf size
	AllowedMargin      float64       // this is 0-1 representing a percentage of how far from the lowest percentile a higher one can be so that it shifts to using a higher percentile. To disable it, set it below 0
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
	var targetExists = false
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
				var c = <-pool.conns
				c.setToBeClosed()
			}
			// close connections scheduled for closing
			var filtered = pool.connsRef[:0] // (ab)use the relationship of slice/array
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
				if len(conn.buf) != pool.targetBufSize && !conn.getInUse() {
					conn.buf = make([]byte, pool.targetBufSize)
				}
			}
			pool.connsMut.Unlock()
		}
	}
}

type PoolOptions struct {
	MaxSize         int
	MaxIdle         int
	ReadTimeout     time.Duration // Default is 500ms
	Dial            func() (net.Conn, error)
	InitialBufSize  int
	TrimOptions     *TrimOptions
	bufSizeQuantile bufQuantile
}

func newPool(ctx context.Context, opts *PoolOptions) internalPool {
	opts.TrimOptions = initTrimOptions(opts.TrimOptions)
	if opts.InitialBufSize < 1 {
		opts.InitialBufSize = 4096
	}
	if opts.ReadTimeout == 0 {
		opts.ReadTimeout = 500 * time.Millisecond
	}
	var p = &pool{
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
	return p
}

// calcTargetBufSize takes a quantile and selects a sane new target []byte buffer size for connections in the pool
// it takes several measures to ensure that it doesn't thrash too much on the targetBufSize
func (p *pool) calcTargetBufSize(q bufQuantile, target float64) int {
	// only spend time if we have enough samples
	if q.count() < 100 {
		return p.opts.InitialBufSize
	}
	var results = q.results()
	var targetResult = results[target]
	if targetResult < float64(p.opts.InitialBufSize) {
		// make sure we don't reduce buf size below initial size
		return p.opts.InitialBufSize
	}
	if math.Abs(targetResult-float64(p.targetBufSize)) < targetResult*0.1 {
		// avoid too much bouncing of targetBufSize
		return p.targetBufSize
	}
	var orderedResults = make([]float64, 0, len(results))
	for q := range results {
		orderedResults = append(orderedResults, q)
	}
	sort.Sort(sort.Reverse(sort.Float64Slice(orderedResults)))
	var lastVal float64
	for _, percentile := range orderedResults {
		lastVal = results[percentile]
		if targetResult+targetResult*p.opts.TrimOptions.AllowedMargin > lastVal {
			return int(lastVal)
		}
	}
	return int(lastVal) // technically unreachable :(
}

func (p *pool) Get() (c *conn, err error) {
	err = nil
	select {
	case c = <-p.conns:
		if c.getToBeClosed() {
			c, err = p.Get()
		}
		c.setInUse(true)
	default:
		var conn net.Conn
		conn, err = p.dial()
		if err != nil {
			return nil, err
		}
		c = newConn(conn, p.targetBufSize)
		p.connsMut.Lock()
		p.connsRef = append(p.connsRef, c)
		p.connsMut.Unlock()
	}
	return c, err
}

func (p *pool) Put(c *conn) {
	c.setInUse(false)
	if c.getToBeClosed() {
		return
	}
	// this causes 1B to be written every time you put the connection back into the pool, but no allocs on the stack
	p.bufSizeQuantile.observe(float64(cap(c.buf)))
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
	var formattedTarget = make(map[float64]float64, len(slice))
	for _, target := range slice {
		formattedTarget[target] = 1 - target
		if target != 1 {
			formattedTarget[target] = formattedTarget[target] / 10
		}
	}
	return formattedTarget
}

func newQuantileStream(targets []float64) bufQuantile {
	var q = &quantileStream{
		targets: targets,
		q:       quantile.NewTargeted(sliceToTargets(targets)),
		mut:     sync.RWMutex{},
	}
	return q
}

func (q *quantileStream) observe(v float64) {
	q.mut.Lock()
	q.q.Insert(v)
	q.mut.Unlock()
}

func (q *quantileStream) results() map[float64]float64 {
	var result = make(map[float64]float64, len(q.targets))
	q.mut.RLock()
	for _, target := range q.targets {
		result[target] = q.q.Query(target)
	}
	q.mut.RUnlock()
	return result
}

func (q *quantileStream) count() int {
	q.mut.RLock()
	var count = q.q.Count()
	q.mut.RUnlock()
	return count
}
