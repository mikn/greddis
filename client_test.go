package greddis_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	"github.com/mikn/greddis"
	"github.com/mikn/greddis/mocks/mock_net"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	var ctx = context.Background()
	var opts = &greddis.PoolOptions{}
	var client = greddis.NewClient(ctx, opts)
	require.NotNil(t, client)
}

func TestClientGet(t *testing.T) {
	t.Run("Fail get pool connection", func(t *testing.T) {
		var ctx = context.Background()
		var opts = &greddis.PoolOptions{
			Dial: func() (net.Conn, error) {
				return nil, errors.New("EOF")
			},
		}
		var client = greddis.NewClient(ctx, opts)
		var res, err = client.Get("testkey")
		require.Error(t, err)
		require.Nil(t, res)
	})
	t.Run("fail on bulk string read", func(t *testing.T) {
		var ctx = context.Background()
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockConn = mock_net.NewMockConn(ctrl)
		var opts = &greddis.PoolOptions{
			Dial: func() (net.Conn, error) {
				return mockConn, nil
			},
		}
		var buf = make([]byte, 0, 4096)
		buf = append(buf, []byte("*2\r\n$3\r\nGET\r\n$7\r\ntestkey\r\n")...)
		mockConn.EXPECT().Write(buf)
		buf = buf[:cap(buf)]
		mockConn.EXPECT().Read(gomock.Any()).Return(0, errors.New("EOF"))
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		mockConn.EXPECT().SetReadDeadline(time.Time{})
		var client = greddis.NewClient(ctx, opts)
		var res, err = client.Get("testkey")
		require.Error(t, err)
		require.Nil(t, res)
	})
	t.Run("Get string (successfully)", func(t *testing.T) {
		var ctx = context.Background()
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockConn = mock_net.NewMockConn(ctrl)
		var opts = &greddis.PoolOptions{
			Dial: func() (net.Conn, error) {
				return mockConn, nil
			},
		}
		var buf = make([]byte, 0, 4096)
		buf = append(buf, []byte("*2\r\n$3\r\nGET\r\n$7\r\ntestkey\r\n")...)
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		mockConn.EXPECT().SetReadDeadline(time.Time{})
		mockConn.EXPECT().Write(buf)
		buf = buf[:cap(buf)]
		mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			copy(b, []byte("$11\r\ntest string\r\n"))
			return 18, nil
		})
		var client = greddis.NewClient(ctx, opts)
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
		var opts = &greddis.PoolOptions{
			Dial: func() (net.Conn, error) {
				return nil, errors.New("EOF")
			},
		}
		var client = greddis.NewClient(ctx, opts)
		var err = client.Set("testkey", "test string", 0)
		require.Error(t, err)
	})
	t.Run("fail on get value", func(t *testing.T) {
		var ctx = context.Background()
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockConn = mock_net.NewMockConn(ctrl)
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		mockConn.EXPECT().SetReadDeadline(time.Time{})
		var opts = &greddis.PoolOptions{
			Dial: func() (net.Conn, error) {
				return mockConn, nil
			},
		}
		var client = greddis.NewClient(ctx, opts)
		var invalid struct{}
		var err = client.Set("testkey", invalid, 0)
		require.Error(t, err)
	})
	t.Run("set string", func(t *testing.T) {
		var ctx = context.Background()
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockConn = mock_net.NewMockConn(ctrl)
		var opts = &greddis.PoolOptions{
			Dial: func() (net.Conn, error) {
				return mockConn, nil
			},
		}
		var buf = make([]byte, 0, 4096)
		buf = append(buf, []byte("*3\r\n$3\r\nSET\r\n$7\r\ntestkey\r\n$11\r\ntest string\r\n")...)
		mockConn.EXPECT().Write(buf)
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		buf = buf[:cap(buf)]
		mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			copy(b, []byte("+OK\r\n"))
			return 5, nil
		})
		mockConn.EXPECT().SetReadDeadline(time.Time{})
		var client = greddis.NewClient(ctx, opts)
		var err = client.Set("testkey", "test string", 0)
		require.NoError(t, err)
	})
	t.Run("set string with TTL", func(t *testing.T) {
		var ctx = context.Background()
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockConn = mock_net.NewMockConn(ctrl)
		var opts = &greddis.PoolOptions{
			Dial: func() (net.Conn, error) {
				return mockConn, nil
			},
		}
		var buf = make([]byte, 0, 4096)
		buf = append(buf, []byte("*5\r\n$3\r\nSET\r\n$7\r\ntestkey\r\n$11\r\ntest string\r\n$2\r\nEX\r\n$1\r\n1\r\n")...)
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		mockConn.EXPECT().Write(buf)
		buf = buf[:cap(buf)]
		mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			copy(b, []byte("+OK\r\n"))
			return 5, nil
		})
		mockConn.EXPECT().SetReadDeadline(time.Time{})
		var client = greddis.NewClient(ctx, opts)
		var err = client.Set("testkey", "test string", 1)
		require.NoError(t, err)
	})
}

func TestClientDel(t *testing.T) {
	t.Run("fail on get pool connection", func(t *testing.T) {
		var ctx = context.Background()
		var opts = &greddis.PoolOptions{
			Dial: func() (net.Conn, error) {
				return nil, errors.New("EOF")
			},
		}
		var client = greddis.NewClient(ctx, opts)
		var err = client.Del("testkey")
		require.Error(t, err)
	})
	t.Run("delete key", func(t *testing.T) {
		var ctx = context.Background()
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockConn = mock_net.NewMockConn(ctrl)
		var opts = &greddis.PoolOptions{
			Dial: func() (net.Conn, error) {
				return mockConn, nil
			},
		}
		var buf = make([]byte, 0, 4096)
		buf = append(buf, []byte("*2\r\n$3\r\nDEL\r\n$7\r\ntestkey\r\n")...)
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		mockConn.EXPECT().Write(buf)
		buf = buf[:cap(buf)]
		mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			copy(b, []byte("+OK\r\n"))
			return 5, nil
		})
		mockConn.EXPECT().SetReadDeadline(time.Time{})
		var client = greddis.NewClient(ctx, opts)
		var err = client.Del("testkey")
		require.NoError(t, err)
	})
}
