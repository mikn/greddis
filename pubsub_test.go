//go:generate mockgen -destination ./mocks/mock_net/mock_error.go net Error
package greddis

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	"github.com/mikn/greddis/mocks/mock_net"
	"github.com/stretchr/testify/require"
)

var (
	TEST_MESSAGE_RAW   = []byte("*3\r\n$7\r\nmessage\r\n$9\r\ntesttopic\r\n$7\r\ntesting\r\n")
	TEST_MESSAGE_TOPIC = "testtopic"
	TEST_MESSAGE       = "testing"

	TEST_PMESSAGE_RAW     = []byte("*4\r\n$8\r\npmessage\r\n$5\r\ntest*\r\n$6\r\ntopicp\r\n$5\r\ntest2\r\n")
	TEST_PMESSAGE_PATTERN = "test*"
	TEST_PMESSAGE_TOPIC   = "topicp"
	TEST_PMESSAGE         = "test2"

	TEST_ERROR_NO_ARRAY_RAW = []byte("-this is an error message instead of an array\r\n")
	TEST_ERROR_NO_ARRAY     = "this is an error message instead of an array"

	TEST_PSUBSCRIBE_BROKEN_EXPECT  = "test3*"
	TEST_PSUBSCRIBE_BROKEN_COUNT   = []byte("*3\r\n$10\r\npsubscribe\r\n$6\r\ntest2*\r\n:blah\r\n")
	TEST_PSUBSCRIBE_SINGLE_RAW     = []byte("*3\r\n$10\r\npsubscribe\r\n$6\r\ntest2*\r\n:1\r\n")
	TEST_PSUBSCRIBE_SINGLE_PATTERN = "test2*"

	TEST_PSUBSCRIBE_MULTIPLE_RAW      = []byte("*7\r\n$10\r\npsubscribe\r\n$6\r\ntest3*\r\n:2\r\n$6\r\ntest4*\r\n:3\r\n$6\r\ntest5*\r\n:4\r\n")
	TEST_PSUBSCRIBE_MULTIPLE_PATTERN1 = "test3*"
	TEST_PSUBSCRIBE_MULTIPLE_PATTERN2 = "test4*"
	TEST_PSUBSCRIBE_MULTIPLE_PATTERN3 = "test5*"

	TEST_SUBSCRIBE_BROKEN_EXPECT = "test3"
	TEST_SUBSCRIBE_BROKEN_COUNT  = []byte("*3\r\n$9\r\nsubscribe\r\n$5\r\ntest2\r\n:blah\r\n")
	TEST_SUBSCRIBE_SINGLE_RAW    = []byte("*3\r\n$9\r\nsubscribe\r\n$5\r\ntest2\r\n:1\r\n")
	TEST_SUBSCRIBE_SINGLE_TOPIC  = "test2"

	TEST_SUBSCRIBE_MULTIPLE_RAW    = []byte("*7\r\n$9\r\nsubscribe\r\n$5\r\ntest7\r\n:2\r\n$5\r\ntest8\r\n:3\r\n$5\r\ntest9\r\n:4\r\n")
	TEST_SUBSCRIBE_MULTIPLE_TOPIC1 = "test7"
	TEST_SUBSCRIBE_MULTIPLE_TOPIC2 = "test8"
	TEST_SUBSCRIBE_MULTIPLE_TOPIC3 = "test9"

	TEST_LISTEN_INVALID_VALUE = []byte("*3\r\n$9\r\ninvalid_value\r\n$5\r\ntest2\r\n:1\r\n")
)

func captureLog(c chan string, callback func()) {
	defer log.SetOutput(os.Stderr)
	defer log.SetFlags(log.Flags())
	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)
	callback()
	c <- strings.TrimSpace(buf.String())
}

func TestNewSubscription(t *testing.T) {
	testTopic := "testtopic"
	t.Run("unbuffered channel", func(t *testing.T) {
		expected := 0
		sub := newSubscription(testTopic, expected)
		require.Equal(t, cap(sub.msgChan), expected)
	})
	t.Run("buffered channel", func(t *testing.T) {
		bufLen := 2
		sub := newSubscription(testTopic, bufLen)
		require.Equal(t, cap(sub.msgChan), bufLen)
	})
}

func getSubMngr(ctx context.Context, ctrl *gomock.Controller) *subscriptionManager {
	mockConn := mock_net.NewMockConn(ctrl)
	mockConn.EXPECT().SetReadDeadline(gomock.Any())
	opts := &PoolOptions{
		Dial: func(ctx context.Context) (net.Conn, error) {
			return mockConn, nil
		},
	}
	c, _ := NewClient(ctx, opts)
	subMngr := c.(*client).subMngr
	subMngr.getConn(ctx)
	return subMngr
}

func TestSubscriptionManager(t *testing.T) {
	t.Run("tryRead", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		subMngr := getSubMngr(ctx, ctrl)
		mockConn := subMngr.conn.conn.(*mock_net.MockConn)
		mockErr := mock_net.NewMockError(ctrl)
		buf := make([]byte, 0, 100)

		t.Run("no data", func(t *testing.T) {
			mockConn.EXPECT().SetReadDeadline(gomock.Any())
			mockConn.EXPECT().Read(gomock.Any()).Return(0, nil)

			newBuf, err := subMngr.tryRead(ctx, buf, subMngr.conn)

			require.NotNil(t, newBuf)
			require.Equal(t, buf, newBuf)
			require.True(t, errors.Is(err, ErrRetryable))
		})

		t.Run("timeout", func(t *testing.T) {
			mockErr.EXPECT().Timeout().Return(true)
			mockConn.EXPECT().SetReadDeadline(gomock.Any())
			mockConn.EXPECT().Read(gomock.Any()).Return(0, mockErr)
			mockConn.EXPECT().SetReadDeadline(gomock.Any())
			mockConn.EXPECT().Write(gomock.Any()).Return(14, nil)
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, []byte("+PONG\r\n"))
				return 7, nil
			})

			newBuf, err := subMngr.tryRead(ctx, buf, subMngr.conn)

			require.Equal(t, buf, newBuf)
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrRetryable))
		})

		t.Run("ping error", func(t *testing.T) {
			testErr := errors.New("TESTERROR")
			mockErr.EXPECT().Timeout().Return(true)
			mockConn.EXPECT().SetReadDeadline(gomock.Any())
			mockConn.EXPECT().Read(gomock.Any()).Return(0, mockErr)
			mockConn.EXPECT().SetReadDeadline(gomock.Any())
			mockConn.EXPECT().Write(gomock.Any()).Return(0, testErr)

			newBuf, err := subMngr.tryRead(ctx, buf, subMngr.conn)

			require.Equal(t, buf, newBuf)
			require.EqualError(t, err, testErr.Error())
		})

		t.Run("successful read", func(t *testing.T) {
			mockErr.EXPECT().Timeout().Return(true)
			mockConn.EXPECT().SetReadDeadline(gomock.Any())
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_MESSAGE_RAW)
				return len(TEST_MESSAGE_RAW), nil
			})

			newBuf, err := subMngr.tryRead(ctx, buf, subMngr.conn)

			require.NoError(t, err)
			require.Equal(t, TEST_MESSAGE_RAW, newBuf)
		})
	})
	t.Run("Listen", func(t *testing.T) {
		testErr := errors.New("TESTERROR")
		t.Run("error on tryRead", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			mockErr := mock_net.NewMockError(ctrl)
			defer ctrl.Finish()
			mockErr.EXPECT().Timeout().Return(true)
			mockConn.EXPECT().SetReadDeadline(gomock.Any())
			mockConn.EXPECT().Read(gomock.Any()).Return(0, mockErr)
			mockConn.EXPECT().SetReadDeadline(gomock.Any())
			mockConn.EXPECT().Write(gomock.Any()).Return(0, testErr)

			subMngr.Listen(ctx, subMngr.conn)
		})
		t.Run("retry error on tryRead", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			mockErr := mock_net.NewMockError(ctrl)
			defer ctrl.Finish()
			mockErr.EXPECT().Timeout().Return(true)
			mockConn.EXPECT().SetReadDeadline(gomock.Any())
			mockConn.EXPECT().Read(gomock.Any()).Return(0, mockErr)
			mockConn.EXPECT().SetReadDeadline(gomock.Any())
			mockConn.EXPECT().Write(gomock.Any()).Return(14, nil)
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, []byte("+PONG\r\n"))
				return 7, nil
			})
			mockConn.EXPECT().SetReadDeadline(gomock.Any())
			// we're returning an error here just to not proceed in the function
			mockConn.EXPECT().Read(gomock.Any()).Return(0, testErr)

			logs := make(chan string, 1)
			captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
				cancel()
			})
			require.Empty(t, <-logs)
		})
		t.Run("error on readArray", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any())
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_ERROR_NO_ARRAY_RAW)
				return len(TEST_ERROR_NO_ARRAY_RAW), nil
			})

			err := make(chan string)
			go captureLog(err, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			require.Equal(t, TEST_ERROR_NO_ARRAY, <-err)
		})
		t.Run("receive pmessage", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_PMESSAGE_RAW)
				return len(TEST_PMESSAGE_RAW), nil
			}).AnyTimes()
			sub := newSubscription(TEST_PMESSAGE_PATTERN, 0)
			subMngr.chans.Store(TEST_PMESSAGE_PATTERN, sub)

			logs := make(chan string, 1)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			msg := <-sub.msgChan
			cancel()
			var res string
			msg.Result.Scan(&res)
			require.Equal(t, TEST_PMESSAGE, res)
			require.Equal(t, "context canceled", <-logs)
		})
		t.Run("receive message", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_MESSAGE_RAW)
				return len(TEST_MESSAGE_RAW), nil
			}).AnyTimes()
			sub := newSubscription(TEST_MESSAGE_TOPIC, 0)
			subMngr.chans.Store(TEST_MESSAGE_TOPIC, sub)

			logs := make(chan string, 1)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			msg := <-sub.msgChan
			cancel()
			var res string
			msg.Result.Scan(&res)
			require.Equal(t, TEST_MESSAGE, res)
			require.Equal(t, "context canceled", <-logs)
		})
		t.Run("receive psubscribe success single", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_PSUBSCRIBE_SINGLE_RAW)
				return len(TEST_PSUBSCRIBE_SINGLE_RAW), nil
			}).AnyTimes()

			logs := make(chan string, 1)
			go captureLog(logs, func() { subMngr.Listen(ctx, subMngr.conn) })
			subMngr.readChan <- TEST_PSUBSCRIBE_SINGLE_PATTERN
			cancel()
			require.Empty(t, subMngr.readChan)
			require.Zero(t, len(logs))
		})
		t.Run("receive psubscribe success multiple", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_PSUBSCRIBE_MULTIPLE_RAW)
				return len(TEST_PSUBSCRIBE_MULTIPLE_RAW), nil
			}).AnyTimes()

			logs := make(chan string, 1)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			subMngr.readChan <- TEST_PSUBSCRIBE_MULTIPLE_PATTERN1
			subMngr.readChan <- TEST_PSUBSCRIBE_MULTIPLE_PATTERN2
			subMngr.readChan <- TEST_PSUBSCRIBE_MULTIPLE_PATTERN3
			cancel()
			require.Empty(t, subMngr.readChan)
			require.Empty(t, logs)
		})
		t.Run("receive psubscribe error on expect", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_PSUBSCRIBE_SINGLE_RAW)
				return len(TEST_PSUBSCRIBE_SINGLE_RAW), nil
			}).AnyTimes()

			logs := make(chan string, 1)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			subMngr.readChan <- TEST_PSUBSCRIBE_BROKEN_EXPECT
			cancel()
			require.Empty(t, subMngr.readChan)
			require.Empty(t, logs)
		})
		t.Run("receive psubscribe error on scan", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_PSUBSCRIBE_BROKEN_COUNT)
				return len(TEST_PSUBSCRIBE_BROKEN_COUNT), nil
			}).AnyTimes()

			logs := make(chan string, 1)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			subMngr.readChan <- TEST_PSUBSCRIBE_SINGLE_PATTERN
			cancel()
			require.Empty(t, subMngr.readChan)
			require.Empty(t, logs)
		})
		t.Run("receive subscribe success single", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_SUBSCRIBE_SINGLE_RAW)
				return len(TEST_SUBSCRIBE_SINGLE_RAW), nil
			}).AnyTimes()

			logs := make(chan string, 10)
			go captureLog(logs, func() { subMngr.Listen(ctx, subMngr.conn) })
			select {
			case subMngr.readChan <- TEST_SUBSCRIBE_SINGLE_TOPIC:
			case <-time.After(10 * time.Millisecond):
			}
			cancel()
			require.Empty(t, subMngr.readChan)
			require.Zero(t, len(logs))
		})
		t.Run("receive subscribe success multiple", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_SUBSCRIBE_MULTIPLE_RAW)
				return len(TEST_SUBSCRIBE_MULTIPLE_RAW), nil
			}).AnyTimes()

			logs := make(chan string, 1)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			subMngr.readChan <- TEST_SUBSCRIBE_MULTIPLE_TOPIC1
			subMngr.readChan <- TEST_SUBSCRIBE_MULTIPLE_TOPIC2
			subMngr.readChan <- TEST_SUBSCRIBE_MULTIPLE_TOPIC3
			cancel()
			require.Empty(t, subMngr.readChan)
			require.Empty(t, logs)
		})
		t.Run("receive subscribe error on expect", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_SUBSCRIBE_SINGLE_RAW)
				return len(TEST_SUBSCRIBE_SINGLE_RAW), nil
			}).AnyTimes()

			logs := make(chan string, 1)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			subMngr.readChan <- TEST_SUBSCRIBE_BROKEN_EXPECT
			cancel()
			require.Empty(t, subMngr.readChan)
			require.Empty(t, logs)
		})
		t.Run("receive subscribe error on scan", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_SUBSCRIBE_BROKEN_COUNT)
				return len(TEST_SUBSCRIBE_BROKEN_COUNT), nil
			}).AnyTimes()

			logs := make(chan string, 1)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			subMngr.readChan <- TEST_SUBSCRIBE_SINGLE_TOPIC
			cancel()
			require.Empty(t, subMngr.readChan)
			require.Empty(t, logs)
		})
		t.Run("receive invalid value", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_LISTEN_INVALID_VALUE)
				return len(TEST_LISTEN_INVALID_VALUE), nil
			}).AnyTimes()

			logs := make(chan string, 1)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			time.Sleep(time.Millisecond)
			cancel()
			require.Empty(t, subMngr.readChan)
			require.NotEmpty(t, <-logs)
		})
	})
	t.Run("Subscribe", func(t *testing.T) {
		t.Run("error on getConn", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			testError := errors.New("TESTERROR")
			opts := &PoolOptions{
				Dial: func(ctx context.Context) (net.Conn, error) {
					return nil, testError
				},
			}
			c, _ := NewClient(ctx, opts)
			subMngr := c.(*client).subMngr

			ret, err := subMngr.Subscribe(ctx, "testtopic")

			require.Empty(t, ret)
			require.Error(t, err)
			require.Equal(t, testError, err)
		})
		t.Run("getConn success", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().Write(gomock.Any())
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_SUBSCRIBE_SINGLE_RAW)
				return len(TEST_SUBSCRIBE_SINGLE_RAW), nil
			}).AnyTimes()

			subMngr.Subscribe(ctx, "testtopic")
			cancel()

			require.True(t, subMngr.listening)
		})
		t.Run("error mixed type RedisPattern", func(t *testing.T) {
		})
		t.Run("error mixed type string", func(t *testing.T) {
		})
		t.Run("error wrong type", func(t *testing.T) {
		})
		t.Run("error on command array flush", func(t *testing.T) {
		})
		t.Run("RedisPattern success one value", func(t *testing.T) {
		})
		t.Run("RedisPattern success multiple values", func(t *testing.T) {
		})
		t.Run("string success one value", func(t *testing.T) {
		})
		t.Run("string success multiple values", func(t *testing.T) {
		})
	})
}
