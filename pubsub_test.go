package greddis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	TEST_MESSAGE_RAW_BROKEN_TOPIC = []byte("*3\r\n$7\r\nmessage\r\n:9\r\ntesttopic\r\n$7\r\ntesting\r\n")
	TEST_MESSAGE_RAW              = []byte("*3\r\n$7\r\nmessage\r\n$9\r\ntesttopic\r\n$7\r\ntesting\r\n")
	TEST_MESSAGE_TOPIC            = "testtopic"
	TEST_MESSAGE                  = "testing"

	TEST_PMESSAGE_RAW_BROKEN_PATTERN = []byte("*4\r\n$8\r\npmessage\r\n:5\r\ntest*\r\n$6\r\ntopicp\r\n$5\r\ntest2\r\n")
	TEST_PMESSAGE_RAW                = []byte("*4\r\n$8\r\npmessage\r\n$5\r\ntest*\r\n$6\r\ntopicp\r\n$5\r\ntest2\r\n")
	TEST_PMESSAGE_PATTERN            = "test*"
	TEST_PMESSAGE_TOPIC              = "topicp"
	TEST_PMESSAGE                    = "test2"

	TEST_ERROR_NO_ARRAY_RAW = []byte("-this is an error message instead of an array\r\n")
	TEST_ERROR_NO_ARRAY     = "this is an error message instead of an array"

	TEST_PSUBSCRIBE_SINGLE_RAW     = []byte("*3\r\n$10\r\npsubscribe\r\n$6\r\ntest2*\r\n:1\r\n")
	TEST_PSUBSCRIBE_SINGLE_PATTERN = "test2*"

	TEST_PUNSUBSCRIBE_SINGLE_RAW     = []byte("*3\r\n$12\r\npunsubscribe\r\n$6\r\ntest2*\r\n:1\r\n")
	TEST_PUNSUBSCRIBE_SINGLE_PATTERN = "test2*"

	TEST_SUBSCRIBE_BROKEN_EXPECT = "test3"
	TEST_SUBSCRIBE_BROKEN_COUNT  = []byte("*3\r\n$9\r\nsubscribe\r\n$5\r\ntest2\r\n:blah\r\n")
	TEST_SUBSCRIBE_SINGLE_RAW    = []byte("*3\r\n$9\r\nsubscribe\r\n$5\r\ntest2\r\n:1\r\n")
	TEST_SUBSCRIBE_SINGLE_TOPIC  = "test2"

	TEST_SUBSCRIBE_MULTIPLE_RAW    = []byte("*7\r\n$9\r\nsubscribe\r\n$5\r\ntest7\r\n:2\r\n$5\r\ntest8\r\n:3\r\n$5\r\ntest9\r\n:4\r\n")
	TEST_SUBSCRIBE_MULTIPLE_TOPICS = []string{"test7", "test8", "test9"}

	TEST_LISTEN_INVALID_VALUE = []byte("*3\r\n$9\r\ninvalid_value\r\n$5\r\ntest2\r\n:1\r\n")

	TEST_UNSUBSCRIBE_SINGLE_RAW   = []byte("*3\r\n$11\r\nunsubscribe\r\n$5\r\ntest2\r\n:0\r\n")
	TEST_UNSUBSCRIBE_SINGLE_TOPIC = "test2"
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

func timeout(t *testing.T, f func()) {
	c := make(chan bool)
	go func() {
		f()
		c <- true
	}()
	select {
	case <-c:
	case <-time.After(10 * time.Millisecond):
		t.FailNow()
	}
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
	subMngr.msgChan = make(chan *Message, 2)
	msg := newMessage(NewResult(subMngr.conn.r), subMngr.msgChan)
	subMngr.msgChan <- msg
	subMngr.msgChan <- nil
	return subMngr
}

func TestSubscriptionManager(t *testing.T) {
	t.Run("Listen", func(t *testing.T) {
		testErr := errors.New("TESTERROR")
		t.Run("ping error", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			mockErr := fmt.Errorf("MOCKERROR: %w", os.ErrDeadlineExceeded)
			testErr := errors.New("TESTERROR")
			mockConn.EXPECT().SetReadDeadline(gomock.Any())
			mockConn.EXPECT().Read(gomock.Any()).Return(0, mockErr)
			mockConn.EXPECT().SetReadDeadline(gomock.Any())
			mockConn.EXPECT().Write(gomock.Any()).Return(0, testErr)

			logs := make(chan string, 1)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})

			require.Equal(t, "Ping error: TESTERROR", <-logs)
		})
		t.Run("cancel context", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			subMngr := getSubMngr(ctx, ctrl)
			subMngr.msgChan = make(chan *Message)

			logs := make(chan string, 1)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			cancel()

			require.Equal(t, "context canceled", <-logs)
		})
		t.Run("error on read array.Init", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			mockConn.EXPECT().SetReadDeadline(gomock.Any())
			mockConn.EXPECT().Read(gomock.Any()).Return(0, testErr)

			logs := make(chan string, 1)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})

			require.Equal(t, "Array Init error: TESTERROR", <-logs)
		})
		t.Run("retry error on array.Init", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			mockErr := fmt.Errorf("MOCKERROR: %w", os.ErrDeadlineExceeded)
			mockConn.EXPECT().SetReadDeadline(gomock.Any())
			mockConn.EXPECT().Read(gomock.Any()).Return(0, mockErr)
			mockConn.EXPECT().SetReadDeadline(gomock.Any())
			mockConn.EXPECT().Write(gomock.Any()).Return(14, nil)
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, []byte("+PONG\r\n"))
				return 7, nil
			})

			logs := make(chan string)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			require.Empty(t, <-logs)
		})
		t.Run("read error on array.Init", func(t *testing.T) {
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
			require.Equal(t, fmt.Sprintf("Array Init error: Redis error: %s", TEST_ERROR_NO_ARRAY), <-err)
		})
		t.Run("receive pmessage broken pattern", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_PMESSAGE_RAW_BROKEN_PATTERN)
				return len(TEST_PMESSAGE_RAW_BROKEN_PATTERN), nil
			}).AnyTimes()

			logs := make(chan string)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			require.Equal(t, "Error: Expected token: $ but received :\nError: Expected token: $ but received 5", <-logs)
		})
		t.Run("receive pmessage", func(t *testing.T) {
			ctx := context.Background()
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

			logs := make(chan string)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			var msgRecv *Message
			select {
			case msgRecv = <-sub.msgChan:
			case <-time.After(10 * time.Millisecond):
				t.FailNow()
			}
			var res string
			msgRecv.Result.Scan(&res)
			require.Equal(t, TEST_PMESSAGE, res)
			require.Empty(t, <-logs)
		})
		t.Run("receive message broken topic", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_MESSAGE_RAW_BROKEN_TOPIC)
				return len(TEST_MESSAGE_RAW_BROKEN_TOPIC), nil
			}).AnyTimes()

			logs := make(chan string)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			require.Equal(t, "Error: Expected token: $ but received :", <-logs)
		})
		t.Run("receive message", func(t *testing.T) {
			ctx := context.Background()
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

			logs := make(chan string)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			var msgRecv *Message
			select {
			case msgRecv = <-sub.msgChan:
			case <-time.After(10 * time.Millisecond):
				t.FailNow()
			}
			var res string
			msgRecv.Result.Scan(&res)
			require.Equal(t, TEST_MESSAGE, res)
			require.Empty(t, <-logs)
		})
		t.Run("receive punsubscribe success single", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_PUNSUBSCRIBE_SINGLE_RAW)
				return len(TEST_PUNSUBSCRIBE_SINGLE_RAW), nil
			}).AnyTimes()

			logs := make(chan string)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			timeout(t, func() { subMngr.readChan <- TEST_PUNSUBSCRIBE_SINGLE_PATTERN })
			require.Empty(t, subMngr.readChan)
			require.Empty(t, <-logs)
		})
		t.Run("receive unsubscribe success single", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_UNSUBSCRIBE_SINGLE_RAW)
				return len(TEST_UNSUBSCRIBE_SINGLE_RAW), nil
			}).AnyTimes()

			logs := make(chan string)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			timeout(t, func() { subMngr.readChan <- TEST_UNSUBSCRIBE_SINGLE_TOPIC })
			require.Empty(t, subMngr.readChan)
			require.Empty(t, <-logs)
		})
		t.Run("receive psubscribe success single", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_PSUBSCRIBE_SINGLE_RAW)
				return len(TEST_PSUBSCRIBE_SINGLE_RAW), nil
			}).AnyTimes()

			logs := make(chan string)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			timeout(t, func() { subMngr.readChan <- TEST_PSUBSCRIBE_SINGLE_PATTERN })
			require.Empty(t, subMngr.readChan)
			require.Empty(t, <-logs)
		})
		t.Run("receive subscribe success single", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_SUBSCRIBE_SINGLE_RAW)
				return len(TEST_SUBSCRIBE_SINGLE_RAW), nil
			}).AnyTimes()

			logs := make(chan string)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			select {
			case subMngr.readChan <- TEST_SUBSCRIBE_SINGLE_TOPIC:
			case <-time.After(10 * time.Millisecond):
			}
			require.Empty(t, subMngr.readChan)
			require.Empty(t, <-logs)
		})
		t.Run("receive subscribe success multiple", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_SUBSCRIBE_MULTIPLE_RAW)
				return len(TEST_SUBSCRIBE_MULTIPLE_RAW), nil
			}).AnyTimes()

			logs := make(chan string)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			for _, topic := range TEST_SUBSCRIBE_MULTIPLE_TOPICS {
				timeout(t, func() { subMngr.readChan <- topic })
			}
			require.Empty(t, subMngr.readChan)
			require.Empty(t, <-logs)
		})
		t.Run("receive subscribe error on expect", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_SUBSCRIBE_SINGLE_RAW)
				return len(TEST_SUBSCRIBE_SINGLE_RAW), nil
			}).AnyTimes()

			logs := make(chan string)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			timeout(t, func() { subMngr.readChan <- TEST_SUBSCRIBE_BROKEN_EXPECT })
			require.Equal(t, "Error: test2 was not equal to any of [test3]", <-logs)
			require.Empty(t, subMngr.readChan)
		})
		t.Run("receive subscribe error on scan", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			defer ctrl.Finish()
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_SUBSCRIBE_BROKEN_COUNT)
				return len(TEST_SUBSCRIBE_BROKEN_COUNT), nil
			}).AnyTimes()

			logs := make(chan string)
			go captureLog(logs, func() {
				subMngr.Listen(ctx, subMngr.conn)
			})
			timeout(t, func() { subMngr.readChan <- TEST_SUBSCRIBE_SINGLE_TOPIC })
			require.Empty(t, subMngr.readChan)
			require.Empty(t, logs)
		})
		t.Run("receive invalid value", func(t *testing.T) {
			ctx := context.Background()
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
			require.Empty(t, subMngr.readChan)
			require.NotEmpty(t, <-logs)
		})
	})
	t.Run("Subscribe", func(t *testing.T) {
		t.Run("error len zero", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			subMngr := getSubMngr(ctx, ctrl)

			ret, err := subMngr.Subscribe(ctx)
			require.Empty(t, ret)
			require.Error(t, err)
		})
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
			subMngr.msgChan = make(chan *Message, 1)

			ret, err := subMngr.Subscribe(ctx, "testtopic")

			require.Empty(t, ret)
			require.Error(t, err)
			require.Equal(t, testError, err)
		})
		type errorFunc func(context.Context, *subscriptionManager) (MessageChanMap, error)
		errFuncs := make(map[string]errorFunc)
		errFuncs["string"] = func(ctx context.Context, subMngr *subscriptionManager) (MessageChanMap, error) {
			return subMngr.Subscribe(ctx, RedisPattern("test*"), "testtopic")
		}
		errFuncs["RedisPattern"] = func(ctx context.Context, subMngr *subscriptionManager) (MessageChanMap, error) {
			return subMngr.Subscribe(ctx, "testtopic", RedisPattern("test*"))
		}
		for name, errFunc := range errFuncs {
			t.Run(fmt.Sprintf("error mixed type %s", name), func(t *testing.T) {
				ctx := context.Background()
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				subMngr := getSubMngr(ctx, ctrl)
				mockConn := subMngr.conn.conn.(*mock_net.MockConn)
				mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
				mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
					copy(b, TEST_SUBSCRIBE_SINGLE_RAW)
					return len(TEST_SUBSCRIBE_SINGLE_RAW), nil
				}).AnyTimes()

				subMngr.msgChan = make(chan *Message, 1)
				chanMap, err := errFunc(ctx, subMngr)

				require.Empty(t, chanMap)
				require.Error(t, err)
				require.Equal(t, err, ErrMixedTopicTypes)
			})
		}
		t.Run("error wrong type", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_SUBSCRIBE_SINGLE_RAW)
				return len(TEST_SUBSCRIBE_SINGLE_RAW), nil
			}).AnyTimes()

			subMngr.msgChan = make(chan *Message, 1)
			chanMap, err := subMngr.Subscribe(ctx, []byte("hello"))

			require.Empty(t, chanMap)
			require.Error(t, err)
			require.EqualError(t, err, "Wrong type! Expected RedisPattern or string, but received: []uint8")
		})
		t.Run("error on command array flush", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			testError := errors.New("TESTERROR")
			defer ctrl.Finish()
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_SUBSCRIBE_SINGLE_RAW)
				return len(TEST_SUBSCRIBE_SINGLE_RAW), nil
			}).AnyTimes()
			mockConn.EXPECT().Write([]byte("*2\r\n$9\r\nSUBSCRIBE\r\n$9\r\ntesttopic\r\n")).Return(0, testError)

			subMngr.msgChan = make(chan *Message, 1)
			chanMap, err := subMngr.Subscribe(ctx, "testtopic")

			require.Empty(t, chanMap)
			require.Error(t, err)
			require.Equal(t, err, testError)
		})
		t.Run("RedisPattern success one value", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			subMngr := getSubMngr(ctx, ctrl)
			subMngr.listening = true
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			write := []byte("*2\r\n$10\r\nPSUBSCRIBE\r\n$6\r\ntest2*\r\n")
			mockConn.EXPECT().Write(write).Return(len(write), nil)

			subMngr.readChan = make(chan string, 1)
			chanMap, err := subMngr.Subscribe(ctx, RedisPattern("test2*"))

			require.Equal(t, "test2*", <-subMngr.readChan)
			require.NotEmpty(t, chanMap)
			require.NoError(t, err)
			require.IsType(t, make(MessageChan), chanMap["test2*"])
		})
		t.Run("string success one value", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			subMngr := getSubMngr(ctx, ctrl)
			subMngr.listening = true
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			write := []byte("*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\ntest\r\n")
			mockConn.EXPECT().Write(write).Return(len(write), nil)

			subMngr.readChan = make(chan string, 1)
			chanMap, err := subMngr.Subscribe(ctx, "test")

			require.Equal(t, "test", <-subMngr.readChan)
			require.NotEmpty(t, chanMap)
			require.NoError(t, err)
			require.IsType(t, make(MessageChan), chanMap["test2*"])
		})
		t.Run("string success multiple values", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			subMngr := getSubMngr(ctx, ctrl)
			subMngr.listening = true
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			write := []byte("*3\r\n$9\r\nSUBSCRIBE\r\n$4\r\ntest\r\n$5\r\ntest2\r\n")
			mockConn.EXPECT().Write(write).Return(len(write), nil)

			subMngr.readChan = make(chan string, 2)
			chanMap, err := subMngr.Subscribe(ctx, "test", "test2")

			require.Equal(t, "test", <-subMngr.readChan)
			require.Equal(t, "test2", <-subMngr.readChan)
			require.NotEmpty(t, chanMap)
			require.NoError(t, err)
			require.IsType(t, make(MessageChan), chanMap["test2*"])
		})
	})
	t.Run("dispatch", func(t *testing.T) {
		t.Run("context canceled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			subMngr := getSubMngr(ctx, ctrl)
			res := NewResult(subMngr.conn.r)
			sub := newSubscription("test", 0)
			msg := newMessage(res, sub.msgChan)
			msg.topic = []byte("test")
			subMngr.chans.Store("test", sub)
			go subMngr.dispatch(ctx, msg)
			cancel()
		})
	})
	t.Run("Unsubscribe", func(t *testing.T) {
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
			subMngr.msgChan = make(chan *Message, 1)

			err := subMngr.Unsubscribe(ctx, "testtopic")

			require.Error(t, err)
			require.Equal(t, testError, err)
		})
		type errorFunc func(context.Context, *subscriptionManager) error
		errFuncs := make(map[string]errorFunc)
		errFuncs["string"] = func(ctx context.Context, subMngr *subscriptionManager) error {
			return subMngr.Unsubscribe(ctx, RedisPattern("test*"), "testtopic")
		}
		errFuncs["RedisPattern"] = func(ctx context.Context, subMngr *subscriptionManager) error {
			return subMngr.Unsubscribe(ctx, "testtopic", RedisPattern("test*"))
		}
		for name, errFunc := range errFuncs {
			t.Run(fmt.Sprintf("error mixed type %s", name), func(t *testing.T) {
				ctx := context.Background()
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				subMngr := getSubMngr(ctx, ctrl)
				mockConn := subMngr.conn.conn.(*mock_net.MockConn)
				mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
				mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
					copy(b, TEST_UNSUBSCRIBE_SINGLE_RAW)
					return len(TEST_UNSUBSCRIBE_SINGLE_RAW), nil
				}).AnyTimes()

				subMngr.msgChan = make(chan *Message, 1)
				err := errFunc(ctx, subMngr)

				require.Error(t, err)
				require.Equal(t, err, ErrMixedTopicTypes)
			})
		}
		t.Run("error wrong type", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_SUBSCRIBE_SINGLE_RAW)
				return len(TEST_SUBSCRIBE_SINGLE_RAW), nil
			}).AnyTimes()

			subMngr.msgChan = make(chan *Message, 1)
			err := subMngr.Unsubscribe(ctx, []byte("hello"))

			require.Error(t, err)
			require.EqualError(t, err, "Wrong type! Expected RedisPattern or string, but received: []uint8")
		})
		t.Run("error on command array flush", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			testError := errors.New("TESTERROR")
			defer ctrl.Finish()
			subMngr := getSubMngr(ctx, ctrl)
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
			mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				copy(b, TEST_SUBSCRIBE_SINGLE_RAW)
				return len(TEST_SUBSCRIBE_SINGLE_RAW), nil
			}).AnyTimes()
			mockConn.EXPECT().Write([]byte("*2\r\n$11\r\nUNSUBSCRIBE\r\n$9\r\ntesttopic\r\n")).Return(0, testError)

			subMngr.msgChan = make(chan *Message, 1)
			err := subMngr.Unsubscribe(ctx, "testtopic")

			require.Error(t, err)
			require.Equal(t, testError, err)
		})
		t.Run("RedisPattern success one value", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			subMngr := getSubMngr(ctx, ctrl)
			subMngr.listening = true
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			write := []byte("*2\r\n$12\r\nPUNSUBSCRIBE\r\n$6\r\ntest2*\r\n")
			mockConn.EXPECT().Write(write).Return(len(write), nil)

			subMngr.readChan = make(chan string, 1)
			res := NewResult(subMngr.conn.r)
			sub := newSubscription("test2*", 0)
			msg := newMessage(res, sub.msgChan)
			msg.topic = []byte("test22")
			msg.pattern = []byte("test2*")
			subMngr.chans.Store("test2*", sub)
			err := subMngr.Unsubscribe(ctx, RedisPattern("test2*"))

			require.Equal(t, "test2*", <-subMngr.readChan)
			require.NoError(t, err)
		})
		t.Run("string success one value", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			subMngr := getSubMngr(ctx, ctrl)
			subMngr.listening = true
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			write := []byte("*2\r\n$11\r\nUNSUBSCRIBE\r\n$4\r\ntest\r\n")
			mockConn.EXPECT().Write(write).Return(len(write), nil)

			subMngr.readChan = make(chan string, 1)
			res := NewResult(subMngr.conn.r)
			sub := newSubscription("test", 0)
			msg := newMessage(res, sub.msgChan)
			msg.topic = []byte("test")
			subMngr.chans.Store("test", sub)
			err := subMngr.Unsubscribe(ctx, "test")
			require.NoError(t, err)
			val, _ := subMngr.chans.Load("test")
			require.Nil(t, val)
		})
		t.Run("string success multiple values", func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			subMngr := getSubMngr(ctx, ctrl)
			subMngr.listening = true
			mockConn := subMngr.conn.conn.(*mock_net.MockConn)
			write := []byte("*3\r\n$11\r\nUNSUBSCRIBE\r\n$4\r\ntest\r\n$5\r\ntest2\r\n")
			mockConn.EXPECT().Write(write).Return(len(write), nil)

			subMngr.readChan = make(chan string, 2)
			err := subMngr.Unsubscribe(ctx, "test", "test2")
			require.NoError(t, err)
		})
	})
}
