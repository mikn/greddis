package greddis

import (
	"errors"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/mikn/greddis/mocks/mock_driver"
	"github.com/stretchr/testify/require"
)

//go:generate mockgen -destination ./mocks/mock_driver/mock_valuer.go database/sql/driver Valuer
func TestToBytesValue(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		var val = "hello"
		var buf = make([]byte, 0, 100)
		var res, err = toBytesValue(val, buf)
		require.NoError(t, err)
		require.Equal(t, []byte("hello"), res)
	})
	t.Run("[]byte", func(t *testing.T) {
		var val = []byte("hello")
		var buf = make([]byte, 0, 100)
		var res, err = toBytesValue(val, buf)
		require.NoError(t, err)
		require.Equal(t, []byte("hello"), res)
	})
	t.Run("int", func(t *testing.T) {
		var val = 55
		var buf = make([]byte, 0, 100)
		var res, err = toBytesValue(val, buf)
		require.NoError(t, err)
		require.Equal(t, []byte("55"), res)
	})
	t.Run("*string", func(t *testing.T) {
		var val = "hello"
		var buf = make([]byte, 0, 100)
		var res, err = toBytesValue(&val, buf)
		require.NoError(t, err)
		require.Equal(t, []byte("hello"), res)
	})
	t.Run("*[]byte", func(t *testing.T) {
		var val = []byte("hello")
		var buf = make([]byte, 0, 100)
		var res, err = toBytesValue(&val, buf)
		require.NoError(t, err)
		require.Equal(t, []byte("hello"), res)
	})
	t.Run("*int", func(t *testing.T) {
		var val = 55
		var buf = make([]byte, 0, 100)
		var res, err = toBytesValue(&val, buf)
		require.NoError(t, err)
		require.Equal(t, []byte("55"), res)
	})
	t.Run("driver.Valuer success", func(t *testing.T) {
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockValuer = mock_driver.NewMockValuer(ctrl)
		mockValuer.EXPECT().Value().Return([]byte("55"), nil)
		var buf = make([]byte, 0, 100)
		var res, err = toBytesValue(mockValuer, buf)
		require.NoError(t, err)
		require.Equal(t, []byte("55"), res)
	})
	t.Run("driver.Valuer fail", func(t *testing.T) {
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockValuer = mock_driver.NewMockValuer(ctrl)
		mockValuer.EXPECT().Value().Return(nil, errors.New("EOF"))
		var buf = make([]byte, 0, 100)
		var res, err = toBytesValue(mockValuer, buf)
		require.Error(t, err)
		require.Nil(t, res)
	})
}
