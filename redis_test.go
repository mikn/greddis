package redis

import (
	"context"
	"errors"
	net "net"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	var ctx = context.Background()
	var opts = &PoolOptions{}
	var client = NewClient(ctx, opts)
	require.NotNil(t, client)
}

func TestClientGet(t *testing.T) {
	t.Run("Fail get pool connection", func(t *testing.T) {
		var ctx = context.Background()
		var opts = &PoolOptions{
			Dial: func() (net.Conn, error) {
				return nil, errors.New("EOF")
			},
		}
		var client = NewClient(ctx, opts)
		var res, err = client.Get("testkey")
		require.Error(t, err)
		require.Nil(t, res)
	})
	t.Run("fail on bulk string read", func(t *testing.T) {
		var ctx = context.Background()
		var ctrl = gomock.NewController(t)
		var mockConn = NewMockConn(ctrl)
		var opts = &PoolOptions{
			Dial: func() (net.Conn, error) {
				return mockConn, nil
			},
		}
		var buf = make([]byte, 0, 4096)
		buf = append(buf, []byte("*2\r\n$3\r\nGET\r\n$7\r\ntestkey\r\n")...)
		mockConn.EXPECT().Write(buf)
		buf = buf[:cap(buf)]
		mockConn.EXPECT().Read(gomock.Any()).Return(0, errors.New("EOF"))
		var client = NewClient(ctx, opts)
		var res, err = client.Get("testkey")
		require.Error(t, err)
		require.Nil(t, res)
	})
	t.Run("Get string (successfully)", func(t *testing.T) {
		var ctx = context.Background()
		var ctrl = gomock.NewController(t)
		var mockConn = NewMockConn(ctrl)
		var opts = &PoolOptions{
			Dial: func() (net.Conn, error) {
				return mockConn, nil
			},
		}
		var buf = make([]byte, 0, 4096)
		buf = append(buf, []byte("*2\r\n$3\r\nGET\r\n$7\r\ntestkey\r\n")...)
		mockConn.EXPECT().Write(buf)
		buf = buf[:cap(buf)]
		mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			copy(b, []byte("$11\r\ntest string\r\n"))
			return 18, nil
		})
		var client = NewClient(ctx, opts)
		var res, err = client.Get("testkey")
		var str string
		res.Scan(&str)
		require.NoError(t, err)
		require.Equal(t, "test string", str)
	})
}

func TestClientSet(t *testing.T) {
	t.Run("fail on get pool connection", func(t *testing.T) {
		var ctx = context.Background()
		var opts = &PoolOptions{
			Dial: func() (net.Conn, error) {
				return nil, errors.New("EOF")
			},
		}
		var client = NewClient(ctx, opts)
		var err = client.Set("testkey", "test string", 0)
		require.Error(t, err)
	})
	t.Run("fail on get value", func(t *testing.T) {
		var ctx = context.Background()
		var opts = &PoolOptions{
			Dial: func() (net.Conn, error) {
				return nil, nil
			},
		}
		var client = NewClient(ctx, opts)
		var invalid struct{}
		var err = client.Set("testkey", invalid, 0)
		require.Error(t, err)
	})
	t.Run("set string", func(t *testing.T) {
		var ctx = context.Background()
		var ctrl = gomock.NewController(t)
		var mockConn = NewMockConn(ctrl)
		var opts = &PoolOptions{
			Dial: func() (net.Conn, error) {
				return mockConn, nil
			},
		}
		var buf = make([]byte, 0, 4096)
		buf = append(buf, []byte("*3\r\n$3\r\nSET\r\n$7\r\ntestkey\r\n$11\r\ntest string\r\n")...)
		mockConn.EXPECT().Write(buf)
		buf = buf[:cap(buf)]
		mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			copy(b, []byte("+OK\r\n"))
			return 5, nil
		})
		var client = NewClient(ctx, opts)
		var err = client.Set("testkey", "test string", 0)
		require.NoError(t, err)
	})
	t.Run("set string with TTL", func(t *testing.T) {
		var ctx = context.Background()
		var ctrl = gomock.NewController(t)
		var mockConn = NewMockConn(ctrl)
		var opts = &PoolOptions{
			Dial: func() (net.Conn, error) {
				return mockConn, nil
			},
		}
		var buf = make([]byte, 0, 4096)
		buf = append(buf, []byte("*5\r\n$3\r\nSET\r\n$7\r\ntestkey\r\n$11\r\ntest string\r\n$2\r\nEX\r\n$1\r\n1\r\n")...)
		mockConn.EXPECT().Write(buf)
		buf = buf[:cap(buf)]
		mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			copy(b, []byte("+OK\r\n"))
			return 5, nil
		})
		var client = NewClient(ctx, opts)
		var err = client.Set("testkey", "test string", 1)
		require.NoError(t, err)
	})
}

func TestClientDel(t *testing.T) {
	t.Run("fail on get pool connection", func(t *testing.T) {
		var ctx = context.Background()
		var opts = &PoolOptions{
			Dial: func() (net.Conn, error) {
				return nil, errors.New("EOF")
			},
		}
		var client = NewClient(ctx, opts)
		var err = client.Del("testkey")
		require.Error(t, err)
	})
	t.Run("delete key", func(t *testing.T) {
		var ctx = context.Background()
		var ctrl = gomock.NewController(t)
		var mockConn = NewMockConn(ctrl)
		var opts = &PoolOptions{
			Dial: func() (net.Conn, error) {
				return mockConn, nil
			},
		}
		var buf = make([]byte, 0, 4096)
		buf = append(buf, []byte("*2\r\n$3\r\nDEL\r\n$7\r\ntestkey\r\n")...)
		mockConn.EXPECT().Write(buf)
		buf = buf[:cap(buf)]
		mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			copy(b, []byte("+OK\r\n"))
			return 5, nil
		})
		var client = NewClient(ctx, opts)
		var err = client.Del("testkey")
		require.NoError(t, err)
	})
}

func TestGetBytesValue(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		var client = client{}
		var val = "hello"
		var buf = make([]byte, 0, 100)
		var res, err = client.getBytesValue(val, buf)
		require.NoError(t, err)
		require.Equal(t, []byte("hello"), res)
	})
	t.Run("[]byte", func(t *testing.T) {
		var client = client{}
		var val = []byte("hello")
		var buf = make([]byte, 0, 100)
		var res, err = client.getBytesValue(val, buf)
		require.NoError(t, err)
		require.Equal(t, []byte("hello"), res)
	})
	t.Run("int", func(t *testing.T) {
		var client = client{}
		var val = 55
		var buf = make([]byte, 0, 100)
		var res, err = client.getBytesValue(val, buf)
		require.NoError(t, err)
		require.Equal(t, []byte("55"), res)
	})
	t.Run("*string", func(t *testing.T) {
		var client = client{}
		var val = "hello"
		var buf = make([]byte, 0, 100)
		var res, err = client.getBytesValue(&val, buf)
		require.NoError(t, err)
		require.Equal(t, []byte("hello"), res)
	})
	t.Run("*[]byte", func(t *testing.T) {
		var client = client{}
		var val = []byte("hello")
		var buf = make([]byte, 0, 100)
		var res, err = client.getBytesValue(&val, buf)
		require.NoError(t, err)
		require.Equal(t, []byte("hello"), res)
	})
	t.Run("*int", func(t *testing.T) {
		var client = client{}
		var val = 55
		var buf = make([]byte, 0, 100)
		var res, err = client.getBytesValue(&val, buf)
		require.NoError(t, err)
		require.Equal(t, []byte("55"), res)
	})
	t.Run("driver.Valuer success", func(t *testing.T) {
		var ctrl = gomock.NewController(t)
		var mockValuer = NewMockValuer(ctrl)
		mockValuer.EXPECT().Value().Return([]byte("55"), nil)
		var client = client{}
		var buf = make([]byte, 0, 100)
		var res, err = client.getBytesValue(mockValuer, buf)
		require.NoError(t, err)
		require.Equal(t, []byte("55"), res)
	})
	t.Run("driver.Valuer fail", func(t *testing.T) {
		var ctrl = gomock.NewController(t)
		var mockValuer = NewMockValuer(ctrl)
		mockValuer.EXPECT().Value().Return(nil, errors.New("EOF"))
		var client = client{}
		var buf = make([]byte, 0, 100)
		var res, err = client.getBytesValue(mockValuer, buf)
		require.Error(t, err)
		require.Nil(t, res)
	})
}
