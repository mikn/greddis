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
	t.Run("No error", func(t *testing.T) {
		ctx := context.Background()
		opts := &greddis.PoolOptions{}
		client, err := greddis.NewClient(ctx, opts)
		require.Nil(t, err)
		require.NotNil(t, client)
	})
	t.Run("both dial and url defined", func(t *testing.T) {
		ctx := context.Background()
		opts := &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) { return nil, nil },
			URL:  "tcp://hey",
		}
		client, err := greddis.NewClient(ctx, opts)
		require.Error(t, err, greddis.ErrOptsDialAndURL)
		require.Nil(t, client)
	})
	t.Run("Correct URL", func(t *testing.T) {
		ctx := context.Background()
		opts := &greddis.PoolOptions{
			URL: "tcp://hello",
		}
		client, err := greddis.NewClient(ctx, opts)
		require.NoError(t, err)
		require.NotNil(t, client)
	})
	t.Run("malformed URL", func(t *testing.T) {
		ctx := context.Background()
		opts := &greddis.PoolOptions{
			URL: "blah@@$%/////",
		}
		client, err := greddis.NewClient(ctx, opts)
		require.Error(t, err)
		require.Nil(t, client)
	})
}

func TestClientGet(t *testing.T) {
	t.Run("Fail get pool connection", func(t *testing.T) {
		var ctx = context.Background()
		var opts = &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) {
				return nil, errors.New("EOF")
			},
		}
		client, err := greddis.NewClient(ctx, opts)
		res, err := client.Get(ctx, "testkey")
		require.Error(t, err)
		require.Nil(t, res)
	})
	t.Run("Fail get on flush", func(t *testing.T) {
		var ctx, cancel = context.WithCancel(context.Background())
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockConn = mock_net.NewMockConn(ctrl)
		var opts = &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) {
				return mockConn, nil
			},
		}
		client, err := greddis.NewClient(ctx, opts)
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		mockConn.EXPECT().SetReadDeadline(time.Time{})
		mockConn.EXPECT().Write(gomock.Any()).Return(0, greddis.ErrConnWrite)
		res, err := client.Get(ctx, "testkey")
		require.Error(t, err)
		require.Nil(t, res)
		cancel()
	})
	t.Run("fail on bulk string read", func(t *testing.T) {
		var ctx, cancel = context.WithCancel(context.Background())
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockConn = mock_net.NewMockConn(ctrl)
		var opts = &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) {
				return mockConn, nil
			},
		}
		var buf = make([]byte, 0, 4096)
		buf = append(buf, []byte("*2\r\n$3\r\nGET\r\n$7\r\ntestkey\r\n")...)
		mockConn.EXPECT().Write(buf).Return(len(buf), nil)
		buf = buf[:cap(buf)]
		mockConn.EXPECT().Read(gomock.Any()).Return(0, errors.New("EOF"))
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		mockConn.EXPECT().SetReadDeadline(time.Time{})
		client, err := greddis.NewClient(ctx, opts)
		res, err := client.Get(ctx, "testkey")
		require.Error(t, err)
		require.Nil(t, res)
		cancel()
	})
	t.Run("Get string (successfully)", func(t *testing.T) {
		var ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockConn = mock_net.NewMockConn(ctrl)
		var opts = &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) {
				return mockConn, nil
			},
		}
		var buf = make([]byte, 0, 4096)
		buf = append(buf, []byte("*2\r\n$3\r\nGET\r\n$7\r\ntestkey\r\n")...)
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		mockConn.EXPECT().SetReadDeadline(time.Time{})
		mockConn.EXPECT().Write(buf).Return(len(buf), nil)
		buf = buf[:cap(buf)]
		mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			copy(b, []byte("$11\r\ntest string\r\n"))
			return 18, nil
		})
		client, err := greddis.NewClient(ctx, opts)
		res, err := client.Get(ctx, "testkey")
		require.NoError(t, err)
		var str string
		res.Scan(&str)
		require.Equal(t, "test string", str)
	})
}

func TestClientSet(t *testing.T) {
	t.Run("fail on get pool connection", func(t *testing.T) {
		var ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		var opts = &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) {
				return nil, errors.New("EOF")
			},
		}
		client, err := greddis.NewClient(ctx, opts)
		err = client.Set(ctx, "testkey", "test string", 0)
		require.Error(t, err)
	})
	t.Run("fail on get value", func(t *testing.T) {
		var ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockConn = mock_net.NewMockConn(ctrl)
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		mockConn.EXPECT().SetReadDeadline(time.Time{})
		var opts = &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) {
				return mockConn, nil
			},
		}
		client, err := greddis.NewClient(ctx, opts)
		var invalid struct{}
		err = client.Set(ctx, "testkey", invalid, 0)
		require.Error(t, err)
	})
	t.Run("Fail set on flush", func(t *testing.T) {
		var ctx, cancel = context.WithCancel(context.Background())
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockConn = mock_net.NewMockConn(ctrl)
		var opts = &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) {
				return mockConn, nil
			},
		}
		client, err := greddis.NewClient(ctx, opts)
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		mockConn.EXPECT().SetReadDeadline(time.Time{})
		mockConn.EXPECT().Write(gomock.Any()).Return(0, greddis.ErrConnWrite)
		err = client.Set(ctx, "testkey", "test string", 0)
		require.Error(t, err)
		cancel()
	})
	t.Run("set string", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockConn := mock_net.NewMockConn(ctrl)
		opts := &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) {
				return mockConn, nil
			},
		}
		buf := make([]byte, 0, 4096)
		buf = append(buf, []byte("*3\r\n$3\r\nSET\r\n$7\r\ntestkey\r\n$11\r\ntest string\r\n")...)
		mockConn.EXPECT().Write(buf).Return(len(buf), nil)
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		buf = buf[:cap(buf)]
		mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			copy(b, []byte("+OK\r\n"))
			return 5, nil
		})
		mockConn.EXPECT().SetReadDeadline(time.Time{})
		client, err := greddis.NewClient(ctx, opts)
		err = client.Set(ctx, "testkey", "test string", 0)
		require.NoError(t, err)
	})
	t.Run("set string with TTL", func(t *testing.T) {
		var ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockConn = mock_net.NewMockConn(ctrl)
		var opts = &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) {
				return mockConn, nil
			},
		}
		var buf = make([]byte, 0, 4096)
		buf = append(buf, []byte("*5\r\n$3\r\nSET\r\n$7\r\ntestkey\r\n$11\r\ntest string\r\n$2\r\nEX\r\n$1\r\n1\r\n")...)
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		mockConn.EXPECT().Write(buf).Return(len(buf), nil)
		buf = buf[:cap(buf)]
		mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			copy(b, []byte("+OK\r\n"))
			return 5, nil
		})
		mockConn.EXPECT().SetReadDeadline(time.Time{})
		client, err := greddis.NewClient(ctx, opts)
		err = client.Set(ctx, "testkey", "test string", 1)
		require.NoError(t, err)
	})
}

func TestClientDel(t *testing.T) {
	t.Run("fail del on get pool connection", func(t *testing.T) {
		var ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		var opts = &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) {
				return nil, errors.New("EOF")
			},
		}
		client, err := greddis.NewClient(ctx, opts)
		err = client.Del(ctx, "testkey")
		require.Error(t, err)
	})
	t.Run("Fail del on flush", func(t *testing.T) {
		var ctx, cancel = context.WithCancel(context.Background())
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockConn = mock_net.NewMockConn(ctrl)
		var opts = &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) {
				return mockConn, nil
			},
		}
		client, err := greddis.NewClient(ctx, opts)
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		mockConn.EXPECT().SetReadDeadline(time.Time{})
		mockConn.EXPECT().Write(gomock.Any()).Return(0, greddis.ErrConnWrite)
		err = client.Del(ctx, "testkey")
		require.Error(t, err)
		cancel()
	})
	t.Run("delete key", func(t *testing.T) {
		var ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockConn = mock_net.NewMockConn(ctrl)
		var opts = &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) {
				return mockConn, nil
			},
		}
		var buf = make([]byte, 0, 4096)
		buf = append(buf, []byte("*2\r\n$3\r\nDEL\r\n$7\r\ntestkey\r\n")...)
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		mockConn.EXPECT().Write(buf).Return(len(buf), nil)
		buf = buf[:cap(buf)]
		mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			copy(b, []byte("+OK\r\n"))
			return 5, nil
		})
		mockConn.EXPECT().SetReadDeadline(time.Time{})
		client, err := greddis.NewClient(ctx, opts)
		err = client.Del(ctx, "testkey")
		require.NoError(t, err)
	})
}
