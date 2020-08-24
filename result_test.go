package greddis_test

import (
	"bufio"
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
	TEST_SIMPLE_STRING       = "+test\r\n"
	TEST_BULK_STRING_CORRECT = []byte("$11\r\ntest string\r\n")
	TEST_INTEGER             = ":15\r\n"
)

//go:generate mockgen -destination ./mocks/mock_io/mock_writer.go io Writer
func TestScan(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_SIMPLE_STRING)))
		r.Next(greddis.ScanSimpleString)
		res := greddis.NewResult(r)
		var str string
		target := &str
		res.Scan(target)
		require.Equal(t, "test", str)
	})
	t.Run("int", func(t *testing.T) {
		r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_INTEGER)))
		r.Next(greddis.ScanInteger)
		res := greddis.NewResult(r)
		var i int
		target := &i
		res.Scan(target)
		require.Equal(t, 15, i)
	})
	t.Run("invalid int", func(t *testing.T) {
		r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_SIMPLE_STRING)))
		r.Next(greddis.ScanInteger)
		res := greddis.NewResult(r)
		var i int
		target := &i
		err := res.Scan(target)
		require.Error(t, err)
	})
	t.Run("[]byte", func(t *testing.T) {
		r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_SIMPLE_STRING)))
		r.Next(greddis.ScanSimpleString)
		res := greddis.NewResult(r)
		b := make([]byte, 4)
		target := &b
		res.Scan(target)
		require.Equal(t, []byte("test"), b)
	})
	t.Run("io.Writer", func(t *testing.T) {
		r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_SIMPLE_STRING)))
		r.Next(greddis.ScanSimpleString)
		res := greddis.NewResult(r)
		b := &bytes.Buffer{}
		res.Scan(b)
		require.Equal(t, "test", b.String())
	})
	t.Run("io.Writer error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockWriter := mock_io.NewMockWriter(ctrl)
		r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_SIMPLE_STRING)))
		r.Next(greddis.ScanSimpleString)
		res := greddis.NewResult(r)
		b := mockWriter
		mockWriter.EXPECT().Write(gomock.Any()).Return(0, errors.New("test"))
		err := res.Scan(b)
		require.Error(t, err)
	})
	t.Run("greddis.Scanner", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockScanner := mock_greddis.NewMockScanner(ctrl)
		r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_SIMPLE_STRING)))
		r.Next(greddis.ScanSimpleString)
		res := greddis.NewResult(r)
		mockScanner.EXPECT().Scan([]byte("test")).Return(nil)
		res.Scan(mockScanner)
	})
	t.Run("invalid type", func(t *testing.T) {
		r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_SIMPLE_STRING)))
		r.Next(greddis.ScanSimpleString)
		res := greddis.NewResult(r)
		type test struct{}
		target := &test{}
		err := res.Scan(target)
		require.Error(t, err)
	})
}
