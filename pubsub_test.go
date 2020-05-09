//go:generate mockgen -destination ./mocks/mock_net/mock_error.go net Error
package greddis

import (
	"context"
	"errors"
	"net"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/mikn/greddis/mocks/mock_net"
	"github.com/stretchr/testify/require"
)

var (
	TEST_MESSAGE          = "testing"
	TEST_MESSAGE_TOPIC    = "testtopic"
	TEST_MESSAGE_RAW      = []byte("*3\r\n$7\r\nmessage\r\n$9\r\ntesttopic\r\n$7\r\ntesting\r\n")
	TEST_PMESSAGE_PATTERN = "test*"
	TEST_PMESSAGE_RAW     = []byte("*4\r\n$8\r\npmessage\r\n$5\r\ntest*\r\n$9\r\ntesttopic\r\n$7\r\ntesting\r\n")
)

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
	var mockConn = mock_net.NewMockConn(ctrl)
	mockConn.EXPECT().SetReadDeadline(gomock.Any())
	var opts = &PoolOptions{
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
			mockErr.EXPECT().Timeout().Return(false)
			mockConn.EXPECT().SetReadDeadline(gomock.Any())
			mockConn.EXPECT().Read(gomock.Any()).Return(0, mockErr)

			newBuf, err := subMngr.tryRead(ctx, buf, subMngr.conn)

			require.NotNil(t, newBuf)
			require.Equal(t, buf, newBuf)
			require.IsType(t, ErrRetry{}, err)
			require.EqualError(t, errors.Unwrap(err), ErrNoData.Error())
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
			require.IsType(t, ErrRetry{}, err)
			require.EqualError(t, errors.Unwrap(err), ErrTimeout.Error())
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
	})
}
