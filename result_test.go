package redis

import (
	"bytes"
	"errors"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestScan(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		var pool, _ = testingPool(t)
		var conn, _ = pool.Get()
		var res = &result{
			value: []byte("test"),
			pool:  pool,
			conn:  conn,
		}
		var str string
		var target = &str
		res.Scan(target)
		require.Equal(t, "test", str)
	})
	t.Run("int", func(t *testing.T) {
		var pool, _ = testingPool(t)
		var conn, _ = pool.Get()
		var res = &result{
			value: []byte("15"),
			pool:  pool,
			conn:  conn,
		}
		var i int
		var target = &i
		res.Scan(target)
		require.Equal(t, 15, i)
	})
	t.Run("invalid int", func(t *testing.T) {
		var pool, _ = testingPool(t)
		var conn, _ = pool.Get()
		var res = &result{
			value: []byte("test"),
			pool:  pool,
			conn:  conn,
		}
		var i int
		var target = &i
		var err = res.Scan(target)
		require.Error(t, err)
	})
	t.Run("[]byte", func(t *testing.T) {
		var pool, _ = testingPool(t)
		var conn, _ = pool.Get()
		var res = &result{
			value: []byte("test"),
			pool:  pool,
			conn:  conn,
		}
		var b = make([]byte, 4)
		var target = &b
		res.Scan(target)
		require.Equal(t, []byte("test"), b)
	})
	t.Run("io.Writer", func(t *testing.T) {
		var pool, _ = testingPool(t)
		var conn, _ = pool.Get()
		var res = &result{
			value: []byte("test"),
			pool:  pool,
			conn:  conn,
		}
		var b = &bytes.Buffer{}
		res.Scan(b)
		require.Equal(t, "test", b.String())
	})
	t.Run("io.Writer error", func(t *testing.T) {
		var ctrl = gomock.NewController(t)
		var mockWriter = NewMockWriter(ctrl)
		var pool, _ = testingPool(t)
		var conn, _ = pool.Get()
		var res = &result{
			pool: pool,
			conn: conn,
		}
		var b = mockWriter
		mockWriter.EXPECT().Write(nil).Return(0, errors.New("test"))
		var err = res.Scan(b)
		require.Error(t, err)
	})
	t.Run("redis.Scanner", func(t *testing.T) {
		var ctrl = gomock.NewController(t)
		var mockScanner = NewMockScanner(ctrl)
		var pool, _ = testingPool(t)
		var conn, _ = pool.Get()
		var res = &result{
			value: []byte("test"),
			pool:  pool,
			conn:  conn,
		}
		mockScanner.EXPECT().Scan([]byte("test")).Return(nil)
		res.Scan(mockScanner)
	})
	t.Run("invalid type", func(t *testing.T) {
		var pool, _ = testingPool(t)
		var conn, _ = pool.Get()
		var res = &result{
			value: []byte("test"),
			pool:  pool,
			conn:  conn,
		}
		type test struct{}
		var target = &test{}
		var err = res.Scan(target)
		require.Error(t, err)
	})
}
