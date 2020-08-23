package greddis

import (
	"context"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/mikn/greddis/mocks/mock_net"
	"github.com/stretchr/testify/require"
)

func TestPing(t *testing.T) {
	t.Run("fail on Flush", func(t *testing.T) {
		var ctx, cancel = context.WithCancel(context.Background())
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockConn = mock_net.NewMockConn(ctrl)
		conn := newConn(mockConn, 4096)
		mockConn.EXPECT().Write(gomock.Any()).Return(0, ErrConnWrite)
		err := ping(ctx, conn)
		require.Error(t, err)
		cancel()
	})
	t.Run("fail on Scan", func(t *testing.T) {
		var ctx, cancel = context.WithCancel(context.Background())
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockConn = mock_net.NewMockConn(ctrl)
		conn := newConn(mockConn, 4096)
		mockConn.EXPECT().Write(gomock.Any()).Return(14, nil)
		mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			copy(b, []byte("*1\r\n$4\r\nPONG\r\n"))
			return 14, nil
		})
		err := ping(ctx, conn)
		require.Error(t, err)
		cancel()
	})
	t.Run("fail on reply", func(t *testing.T) {
		var ctx, cancel = context.WithCancel(context.Background())
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockConn = mock_net.NewMockConn(ctrl)
		conn := newConn(mockConn, 4096)
		mockConn.EXPECT().Write(gomock.Any()).Return(14, nil)
		mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			copy(b, []byte("+PON\r\n"))
			return 10, nil
		})
		err := ping(ctx, conn)
		require.Error(t, err)
		cancel()
	})
	t.Run("success", func(t *testing.T) {
		var ctx, cancel = context.WithCancel(context.Background())
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockConn = mock_net.NewMockConn(ctrl)
		conn := newConn(mockConn, 4096)
		mockConn.EXPECT().Write(gomock.Any()).Return(14, nil)
		mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			copy(b, []byte("+PONG\r\n"))
			return 7, nil
		})
		err := ping(ctx, conn)
		require.NoError(t, err)
		cancel()
	})
}
