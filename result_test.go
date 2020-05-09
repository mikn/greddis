package greddis_test

import (
	"bytes"
	"errors"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/mikn/greddis"
	"github.com/mikn/greddis/mocks/mock_greddis"
	"github.com/mikn/greddis/mocks/mock_io"
	//"github.com/mikn/greddis/mocks/mock_net"
	"github.com/stretchr/testify/require"
)

var (
	TEST_BULK_STRING_CORRECT = []byte("$11\r\ntest string\r\n")
)

//go:generate mockgen -destination ./mocks/mock_io/mock_writer.go io Writer
func TestScan(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		var res = greddis.NewResult([]byte("test"))
		var str string
		var target = &str
		res.Scan(target)
		require.Equal(t, "test", str)
	})
	t.Run("int", func(t *testing.T) {
		var res = greddis.NewResult([]byte("15"))
		var i int
		var target = &i
		res.Scan(target)
		require.Equal(t, 15, i)
	})
	t.Run("invalid int", func(t *testing.T) {
		var res = greddis.NewResult([]byte("test"))
		var i int
		var target = &i
		var err = res.Scan(target)
		require.Error(t, err)
	})
	t.Run("[]byte", func(t *testing.T) {
		var res = greddis.NewResult([]byte("test"))
		var b = make([]byte, 4)
		var target = &b
		res.Scan(target)
		require.Equal(t, []byte("test"), b)
	})
	t.Run("io.Writer", func(t *testing.T) {
		var res = greddis.NewResult([]byte("test"))
		var b = &bytes.Buffer{}
		res.Scan(b)
		require.Equal(t, "test", b.String())
	})
	t.Run("io.Writer error", func(t *testing.T) {
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockWriter = mock_io.NewMockWriter(ctrl)
		var res = greddis.NewResult(nil)
		var b = mockWriter
		mockWriter.EXPECT().Write(nil).Return(0, errors.New("test"))
		var err = res.Scan(b)
		require.Error(t, err)
	})
	t.Run("greddis.Scanner", func(t *testing.T) {
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockScanner = mock_greddis.NewMockScanner(ctrl)
		var res = greddis.NewResult([]byte("test"))
		mockScanner.EXPECT().Scan([]byte("test")).Return(nil)
		res.Scan(mockScanner)
	})
	t.Run("invalid type", func(t *testing.T) {
		var res = greddis.NewResult([]byte("test"))
		type test struct{}
		var target = &test{}
		var err = res.Scan(target)
		require.Error(t, err)
	})
}

func TestArrayResult(t *testing.T) {
	//ctrl := gomock.NewController(t)
	//mockConn := mock_net.NewMockConn(ctrl)
	buf := make([]byte, 0, 4096)
	buf = append(buf, TEST_BULK_STRING_CORRECT...)
	//arrResult := greddis.NewArrayResult(buf, mockConn, greddis.NewResult(buf))

	t.Run("Next", func(t *testing.T) {
		t.Run("bulk string", func(t *testing.T) {
		})
	})
}
