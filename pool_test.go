package redis

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

type retType struct {
	conn net.Conn
	err  error
}

func testingPool(t *testing.T) (*pool, *MockConn) {
	var ctx = context.Background()
	var ctrl = gomock.NewController(t)
	var connMock = NewMockConn(ctrl)
	return newPool(ctx, &PoolOptions{
		TrimOptions: &TrimOptions{Interval: -1, BufQuantileTargets: []float64{0.8, 0.9, 0.99, 1}},
		Dial: func() (net.Conn, error) {
			return connMock, nil
		},
		MaxIdle: 1,
		MaxSize: 2,
	}).(*pool), connMock
}

func TestPool(t *testing.T) {
	t.Run("test set target to existing quantile", func(t *testing.T) {
		var ctx = context.Background()
		var pool = newPool(ctx, &PoolOptions{
			TrimOptions: &TrimOptions{BufQuantileTargets: []float64{0.8}, BufQuantileTarget: 0.8},
		}).(*pool)
		require.Equal(t, 0, len(pool.conns))
		require.Equal(t, 0.8, pool.opts.TrimOptions.BufQuantileTarget)
	})
	t.Run("test default TrimmingAllowedMargin", func(t *testing.T) {
		var ctx = context.Background()
		var pool = newPool(ctx, &PoolOptions{
			TrimOptions: &TrimOptions{AllowedMargin: -1},
		}).(*pool)
		require.Equal(t, 0, len(pool.conns))
		require.Zero(t, pool.opts.TrimOptions.AllowedMargin)
	})
	t.Run("test default TrimmingInterval", func(t *testing.T) {
		var ctx = context.Background()
		var pool = newPool(ctx, &PoolOptions{
			TrimOptions: &TrimOptions{Interval: 0},
		}).(*pool)
		require.Equal(t, 0, len(pool.conns))
		require.Equal(t, 500*time.Millisecond, pool.opts.TrimOptions.Interval)
	})
	t.Run("get without conns in pool", func(t *testing.T) {
		var pool, connMock = testingPool(t)
		require.Equal(t, 0, len(pool.conns))
		var conn, err = pool.Get()
		require.NoError(t, err)
		require.Equal(t, connMock, conn.conn)
	})
	t.Run("get without conns in pool with fail in dial", func(t *testing.T) {
		var ctx = context.Background()
		var ctrl = gomock.NewController(t)
		var failConn = NewMockConn(ctrl)
		var successConn = NewMockConn(ctrl)
		var returns []retType
		returns = append(returns, retType{failConn, errors.New("omg failed to connect")})
		returns = append(returns, retType{successConn, nil})
		var pool = newPool(ctx, &PoolOptions{
			TrimOptions: &TrimOptions{Interval: -1},
			Dial: func() (net.Conn, error) {
				var ret retType
				ret, returns = returns[0], returns[1:]
				return ret.conn, ret.err
			},
		}).(*pool)
		require.Equal(t, 0, len(pool.conns))
		var _, err = pool.Get()
		require.Error(t, err)
		var _, err2 = pool.Get()
		require.NoError(t, err2)
	})
	t.Run("put without conns in pool", func(t *testing.T) {
		var pool, _ = testingPool(t)
		var ctrl = gomock.NewController(t)
		var mockConn = NewMockConn(ctrl)
		var c = newConn(mockConn, pool.opts.InitialBufSize)
		pool.Put(c)
		require.Equal(t, 1, len(pool.conns))
	})
	t.Run("put to be GCd connection back in pool", func(t *testing.T) {
		var pool, _ = testingPool(t)
		var ctrl = gomock.NewController(t)
		var mockConn = NewMockConn(ctrl)
		var c = newConn(mockConn, pool.opts.InitialBufSize)
		c.setToBeClosed()
		pool.Put(c)
		require.Equal(t, 0, len(pool.conns))
	})
	t.Run("put exceeding max size conns in pool", func(t *testing.T) {
		var pool, _ = testingPool(t)
		var ctrl = gomock.NewController(t)
		var mockConn = NewMockConn(ctrl)
		var c = newConn(mockConn, pool.opts.InitialBufSize)
		pool.Put(c)
		pool.Put(c)
		pool.Put(c)
		require.Equal(t, 2, len(pool.conns))
	})
	t.Run("get with conns in pool", func(t *testing.T) {
		var pool, _ = testingPool(t)
		var ctrl = gomock.NewController(t)
		var mockConn = NewMockConn(ctrl)
		var c = newConn(mockConn, pool.opts.InitialBufSize)
		pool.Put(c)
		require.Equal(t, 1, len(pool.conns))
		var conn, err = pool.Get()
		require.NoError(t, err)
		require.Equal(t, 0, len(pool.conns))
		require.Equal(t, mockConn, conn.conn)
	})
	t.Run("get conn from pool with one to be GCd", func(t *testing.T) {
		var pool, mockConn1 = testingPool(t)
		var ctrl = gomock.NewController(t)
		var mockConn2 = NewMockConn(ctrl)
		var c = newConn(mockConn2, pool.opts.InitialBufSize)
		pool.Put(c)
		require.Equal(t, 1, len(pool.conns))
		c.setToBeClosed()
		var conn, err = pool.Get()
		require.NoError(t, err)
		require.Equal(t, 0, len(pool.conns))
		require.Equal(t, mockConn1, conn.conn)
	})
}

func TestTargetBufCalc(t *testing.T) {
	t.Run("return initial buf size if not enough samples", func(t *testing.T) {
		var pool, _ = testingPool(t)
		var res = pool.calcTargetBufSize(pool.bufSizeQuantile, pool.opts.TrimOptions.BufQuantileTarget)
		require.Equal(t, pool.opts.InitialBufSize, res)
	})
	t.Run("Do not go below initBufSize", func(t *testing.T) {
		var pool, _ = testingPool(t)
		var q = pool.bufSizeQuantile
		for i := 0; i < 10; i++ {
			q.observe(512)
		}
		for i := 0; i < 90; i++ {
			q.observe(415)
		}
		var res = pool.calcTargetBufSize(q, pool.opts.TrimOptions.BufQuantileTarget)
		require.Equal(t, 4096, res)
	})
	t.Run("return targetBufSize if difference between new and old target is less than 10%", func(t *testing.T) {
		var pool, _ = testingPool(t)
		var q = pool.bufSizeQuantile
		for i := 0; i < 10; i++ {
			q.observe(5120)
		}
		for i := 0; i < 90; i++ {
			q.observe(4150)
		}
		var res = pool.calcTargetBufSize(q, pool.opts.TrimOptions.BufQuantileTarget)
		require.Equal(t, 4096, res)
	})
	t.Run("return max if within 10% of 80th", func(t *testing.T) {
		var pool, _ = testingPool(t)
		var q = pool.bufSizeQuantile
		q.observe(5120)
		for i := 0; i < 100; i++ {
			q.observe(4750)
		}
		var res = pool.calcTargetBufSize(q, pool.opts.TrimOptions.BufQuantileTarget)
		require.Equal(t, 5120, res)
	})
	t.Run("return 80th percentile", func(t *testing.T) {
		var pool, _ = testingPool(t)
		var q = pool.bufSizeQuantile
		q.observe(6192)
		for i := 0; i < 10; i++ {
			q.observe(5120)
		}
		for i := 0; i < 90; i++ {
			q.observe(4650)
		}
		var res = pool.calcTargetBufSize(q, pool.opts.TrimOptions.BufQuantileTarget)
		require.Equal(t, 4650, res)
	})
	//	t.Run("return init buf size if quantile is empty", func(t *testing.T) {
	//		var pool, _ = testingPool(t)
	//		var ctrl = gomock.NewController(t)
	//		var mockQ = NewMockbufQuantile(ctrl)
	//		pool.bufSizeQuantile = mockQ
	//		mockQ.EXPECT().count().Return(100)
	//		mockQ.EXPECT().results().Return(map[float64]float64{0.8: float64(pool.opts.InitialBufSize)})
	//		pool.opts.TrimOptions.AllowedMargin = -1
	//		var res = pool.calcTargetBufSize(mockQ, 0.8)
	//		require.Equal(t, 4096, res)
	//	})
}

func TestGC(t *testing.T) {
	t.Run("failed dial don't break things", func(t *testing.T) {
		var ctx, cancel = context.WithCancel(context.Background())
		var pool, _ = testingPool(t)
		var tick = make(chan time.Time)
		go connTrimming(ctx, tick, pool)
		cancel()
	})
	t.Run("check max idle works", func(t *testing.T) {
		var ctx = context.Background()
		var pool, connMock = testingPool(t)
		connMock.EXPECT().Close()
		var p1, _ = pool.Get()
		var p2, _ = pool.Get()
		pool.Put(p1)
		pool.Put(p2)
		var tick = make(chan time.Time)
		require.Equal(t, 2, len(pool.conns))
		go connTrimming(ctx, tick, pool)
		tick <- time.Now()
		require.Equal(t, pool.opts.MaxIdle, len(pool.conns))
		require.Equal(t, pool.opts.MaxIdle, len(pool.connsRef))
	})
	t.Run("failed dial don't break things", func(t *testing.T) {
		var ctx = context.Background()
		var pool, _ = testingPool(t)
		pool.dial = func() (net.Conn, error) { return nil, errors.New("blah") }
		var tick = make(chan time.Time)
		go connTrimming(ctx, tick, pool)
		tick <- time.Now()
		require.Equal(t, 0, len(pool.conns))
		require.Equal(t, 0, len(pool.connsRef))
	})
	t.Run("check target buf size correction", func(t *testing.T) {
		var ctx = context.Background()
		var pool, _ = testingPool(t)
		var p1, _ = pool.Get()
		var newBuf = make([]byte, 5376)
		p1.buf = p1.buf[:0]
		for _, b := range newBuf {
			p1.buf = append(p1.buf, b)
		}
		pool.Put(p1)
		require.Equal(t, 5376, cap(p1.buf))
		var tick = make(chan time.Time)
		go connTrimming(ctx, tick, pool)
		tick <- time.Now()
		require.Equal(t, pool.targetBufSize, cap(p1.buf))
	})
}
