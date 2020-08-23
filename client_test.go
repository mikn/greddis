package greddis_test

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net"
	"os"
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

func TestClientPing(t *testing.T) {
	t.Run("fail on pool get", func(t *testing.T) {
		var ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		var opts = &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) {
				return nil, errors.New("EOF")
			},
		}
		client, err := greddis.NewClient(ctx, opts)
		err = client.Ping(ctx)
		require.Error(t, err)
	})
	t.Run("success", func(t *testing.T) {
		var ctx, cancel = context.WithCancel(context.Background())
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockConn = mock_net.NewMockConn(ctrl)
		var opts = &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) {
				return mockConn, nil
			},
		}
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		mockConn.EXPECT().Write(gomock.Any()).Return(14, nil)
		mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			copy(b, []byte("+PONG\r\n"))
			return 7, nil
		})
		mockConn.EXPECT().SetReadDeadline(time.Time{})
		client, err := greddis.NewClient(ctx, opts)
		err = client.Ping(ctx)
		require.NoError(t, err)
		cancel()
	})
}

func TestClientPublish(t *testing.T) {
	t.Run("fail get pool connection", func(t *testing.T) {
		var ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		var opts = &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) {
				return nil, errors.New("EOF")
			},
		}
		client, err := greddis.NewClient(ctx, opts)
		_, err = client.Publish(ctx, "testkey", "")
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
		_, err = client.Publish(ctx, "testkey", invalid)
		require.Error(t, err)
	})
	t.Run("fail on flush", func(t *testing.T) {
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
		_, err = client.Publish(ctx, "testkey", "")
		require.Error(t, err)
		cancel()
	})
	t.Run("success", func(t *testing.T) {
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
		buf = append(buf, []byte("*3\r\n$7\r\nPUBLISH\r\n$7\r\ntestkey\r\n$9\r\ntestvalue\r\n")...)
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		mockConn.EXPECT().Write(buf).Return(len(buf), nil)
		buf = buf[:cap(buf)]
		mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			copy(b, []byte(":1\r\n"))
			return 5, nil
		})
		mockConn.EXPECT().SetReadDeadline(time.Time{})
		client, err := greddis.NewClient(ctx, opts)
		i, err := client.Publish(ctx, "testkey", "testvalue")
		require.NoError(t, err)
		require.Equal(t, 1, i)
	})
}

func TestClientSubscribe(t *testing.T) {
	t.Run("fail", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		testError := errors.New("TESTERROR")
		opts := &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) {
				return nil, testError
			},
		}
		c, _ := greddis.NewClient(ctx, opts)

		chanMap, err := c.Subscribe(ctx, "testtopic")

		require.Empty(t, chanMap)
		require.Error(t, err)
		require.Equal(t, testError, err)
	})
	t.Run("success", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		ctrl, ctx := gomock.WithContext(ctx, t)
		defer ctrl.Finish()
		mockConn := mock_net.NewMockConn(ctrl)
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			write := []byte("*3\r\n$9\r\nsubscribe\r\n$4\r\ntest\r\n:1\r\n")
			copy(b, write)
			return len(write), nil
		})
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		opts := &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) {
				return mockConn, nil
			},
		}
		c, _ := greddis.NewClient(ctx, opts)
		write := []byte("*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\ntest\r\n")
		mockConn.EXPECT().Write(write).Return(len(write), nil)

		defer log.SetOutput(os.Stderr)
		defer log.SetFlags(log.Flags())
		var buf bytes.Buffer
		log.SetOutput(&buf)
		log.SetFlags(0)
		chanMap, err := c.Subscribe(ctx, "test")
		require.NotEmpty(t, chanMap)
		require.NoError(t, err)
		cancel()
		time.Sleep(5 * time.Millisecond)
	})
}

func TestClientUnsubscribe(t *testing.T) {
	t.Run("fail", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		testError := errors.New("TESTERROR")
		opts := &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) {
				return nil, testError
			},
		}
		c, _ := greddis.NewClient(ctx, opts)

		err := c.Unsubscribe(ctx, "testtopic")

		require.Error(t, err)
		require.Equal(t, testError, err)
	})
	t.Run("success", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockConn := mock_net.NewMockConn(ctrl)
		mockConn.EXPECT().SetReadDeadline(gomock.Any())
		opts := &greddis.PoolOptions{
			Dial: func(ctx context.Context) (net.Conn, error) {
				return mockConn, nil
			},
		}
		c, _ := greddis.NewClient(ctx, opts)
		write := []byte("*2\r\n$11\r\nUNSUBSCRIBE\r\n$4\r\ntest\r\n")
		mockConn.EXPECT().Write(write).Return(len(write), nil)

		err := c.Unsubscribe(ctx, "test")
		require.NoError(t, err)
	})
}
