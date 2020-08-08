package greddis

import (
	"bytes"
	"errors"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/mikn/greddis/mocks/mock_io"
	"github.com/stretchr/testify/require"
)

func TestUnmarshalCount(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		buf := []byte("42\r\n")
		b := &bytes.Buffer{}
		n, count, ret, err := unmarshalCount(b, buf)
		require.NoError(t, err)
		require.Equal(t, len(buf), n)
		require.Equal(t, buf, ret)
		require.Equal(t, 42, count)
	})
	t.Run("conversion failure", func(t *testing.T) {
		buf := []byte("This is a test\r\n")
		b := &bytes.Buffer{}
		n, count, ret, err := unmarshalCount(b, buf)
		require.Error(t, err)
		require.Zero(t, n)
		require.Equal(t, buf, ret)
		require.Zero(t, count)
	})
}

//go:generate mockgen -destination ./mocks/mock_io/mock_reader.go io Reader
func TestUnmarshalBulkString(t *testing.T) {
	t.Run("normal string that fits", func(t *testing.T) {
		buf := []byte("11\r\ntest string\r\n")
		b := &bytes.Buffer{}
		n, out, err := unmarshalBulkString(b, buf)
		require.NoError(t, err)
		require.Equal(t, len(buf), n)
		require.Equal(t, []byte("test string"), out)
	})
	t.Run("malformed string", func(t *testing.T) {
		buf := []byte("11\r\ntest stringss")
		b := &bytes.Buffer{}
		n, out, err := unmarshalBulkString(b, buf)
		require.Error(t, err, ErrMalformedString)
		require.Equal(t, 4, n)
		require.Equal(t, []byte("test stringss"), out)
	})
	t.Run("string with empty short buf", func(t *testing.T) {
		buf := make([]byte, 0, 10)
		b := bytes.NewBuffer([]byte("11\r\ntest string\r\n"))
		n, out, err := unmarshalBulkString(b, buf)
		require.NoError(t, err)
		require.Equal(t, 17, n)
		require.Equal(t, []byte("test string"), out)
	})
	t.Run("string that is longer than buf", func(t *testing.T) {
		buf := []byte("11\r\ntest stri")
		b := bytes.NewBuffer([]byte("ng\r\n"))
		n, out, err := unmarshalBulkString(b, buf)
		require.NoError(t, err)
		require.Equal(t, len(buf)+4, n)
		require.Equal(t, []byte("test string"), out)
	})
	t.Run("Error on intRead", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockReader := mock_io.NewMockReader(ctrl)
		buf := []byte("This is a")
		mockReader.EXPECT().Read(gomock.Any()).Return(0, errors.New("EOF"))
		n, msg, err := unmarshalBulkString(mockReader, buf)
		require.Error(t, err)
		require.Equal(t, 0, n)
		require.Nil(t, msg)
	})
	t.Run("Error on read after int", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockReader := mock_io.NewMockReader(ctrl)
		buf := []byte("11\r\ntest ")
		mockReader.EXPECT().Read(gomock.Any()).Return(0, errors.New("EOF"))
		n, msg, err := unmarshalBulkString(mockReader, buf)
		require.Error(t, err)
		require.Equal(t, 4, n)
		require.Nil(t, msg)
	})
}

func TestUnmarshalSimpleString(t *testing.T) {
	t.Run("Reader with fewer bytes than buf size", func(t *testing.T) {
		buf := []byte("This is a test\r\n")
		b := &bytes.Buffer{}
		n, msg, err := unmarshalSimpleString(b, buf)
		msg = msg[:n-len(sep)]
		require.NoError(t, err)
		require.Equal(t, len(buf), n)
		require.Equal(t, []byte("This is a test"), msg)
	})
	t.Run("Reader with more bytes than buf size", func(t *testing.T) {
		buf := []byte("This is a")
		b := bytes.NewBuffer([]byte(" test\r\n"))
		n, msg, err := unmarshalSimpleString(b, buf)
		msg = msg[:n-len(sep)]
		require.NoError(t, err)
		require.Equal(t, len(buf)+7, n)
		require.Equal(t, []byte("This is a test"), msg)
	})
	t.Run("Empty and too small buffer", func(t *testing.T) {
		b := bytes.NewBuffer([]byte("This is a test\r\n"))
		buf := make([]byte, 0, 10)
		n, msg, err := unmarshalSimpleString(b, buf)
		msg = msg[:n-len(sep)]
		require.NoError(t, err)
		require.Equal(t, 16, n)
		require.Equal(t, []byte("This is a test"), msg)
	})
	t.Run("Expand buffer", func(t *testing.T) {
		b := bytes.NewBuffer([]byte(" test\r\n"))
		buf := []byte("This is a")
		n, msg, err := unmarshalSimpleString(b, buf)
		msg = msg[:n-len(sep)]
		require.NoError(t, err)
		require.Equal(t, len(buf)+7, n)
		require.Equal(t, []byte("This is a test"), msg)
	})
	t.Run("Error on read", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockReader := mock_io.NewMockReader(ctrl)
		buf := []byte("This is a")
		mockReader.EXPECT().Read(gomock.Any()).Return(0, errors.New("EOF"))
		n, msg, err := unmarshalSimpleString(mockReader, buf)
		require.Error(t, err)
		require.Equal(t, 0, n)
		require.Nil(t, msg)
	})
}

func TestReadInteger(t *testing.T) {
	t.Run("read int", func(t *testing.T) {
		b := bytes.NewBuffer([]byte(":256622\r\n"))
		buf := make([]byte, 0, 16)
		n, i, err := readInteger(b, buf)
		require.NoError(t, err)
		require.Equal(t, 9, n)
		require.Equal(t, 256622, i)
	})
	t.Run("Error EOF", func(t *testing.T) {
		b := bytes.NewBuffer([]byte(":256622"))
		buf := make([]byte, 0, 16)
		n, i, err := readInteger(b, buf)
		require.Error(t, err)
		require.Equal(t, 1, n)
		require.Zero(t, i)
	})
	t.Run("Error not an int", func(t *testing.T) {
		b := bytes.NewBuffer([]byte(":25bb25\r\n"))
		buf := make([]byte, 0, 16)
		n, i, err := readInteger(b, buf)
		require.Error(t, err)
		require.Equal(t, 9, n)
		require.Zero(t, i)
	})
}

func TestReadBulkString(t *testing.T) {
	t.Run("normal string", func(t *testing.T) {
		buf := make([]byte, 0, 100)
		b := bytes.NewBuffer([]byte("$11\r\ntest string\r\n"))
		n, out, err := readBulkString(b, buf)
		require.NoError(t, err)
		require.Equal(t, 18, n)
		require.Equal(t, []byte("test string"), out)
	})
	t.Run("nil string", func(t *testing.T) {
		buf := make([]byte, 0, 100)
		b := bytes.NewBuffer([]byte("$-1\r\n"))
		n, out, err := readBulkString(b, buf)
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Nil(t, out)
	})
}

func TestReadSimpleString(t *testing.T) {
	t.Run("normal string", func(t *testing.T) {
		buf := make([]byte, 0, 100)
		b := bytes.NewBuffer([]byte("+test string\r\n"))
		n, out, err := readSimpleString(b, buf)
		require.NoError(t, err)
		require.Equal(t, 14, n)
		require.Equal(t, []byte("test string"), out)
	})
	t.Run("string with too short buffer", func(t *testing.T) {
		buf := make([]byte, 0, 10)
		b := bytes.NewBuffer([]byte("+test string\r\n"))
		n, out, err := readSimpleString(b, buf)
		require.NoError(t, err)
		require.Equal(t, 14, n)
		require.Equal(t, []byte("test string"), out)
	})
	t.Run("trigger truncate", func(t *testing.T) {
		buf := make([]byte, 0, 10)
		b := bytes.NewBuffer([]byte("+\n"))
		n, out, err := readSimpleString(b, buf)
		require.Error(t, err)
		require.Equal(t, 1, n)
		require.Nil(t, out)
	})
}

func TestReadSwitch(t *testing.T) {
	t.Run("error string", func(t *testing.T) {
		buf := make([]byte, 0, 100)
		b := bytes.NewBuffer([]byte("-test string\r\n"))
		n, out, err := readSwitch('+', unmarshalSimpleString, b, buf)
		require.Nil(t, out)
		require.Equal(t, 14, n)
		require.Error(t, err, errors.New("test string"))
	})
	t.Run("error string", func(t *testing.T) {
		buf := make([]byte, 100)
		b := bytes.NewBuffer([]byte("#test string\r\n"))
		n, out, err := readSwitch('+', unmarshalSimpleString, b, buf)
		require.Nil(t, out)
		require.Equal(t, 0, n)
		require.Error(t, err)
	})
	t.Run("error on read", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockReader := mock_io.NewMockReader(ctrl)
		buf := make([]byte, 0, 100)
		mockReader.EXPECT().Read(gomock.Any()).Return(0, errors.New("EOF"))
		n, msg, err := readSwitch('+', unmarshalSimpleString, mockReader, buf)
		require.Nil(t, msg)
		require.Equal(t, 0, n)
		require.Error(t, err)
	})
	t.Run("error on error string unmarshal", func(t *testing.T) {
		buf := make([]byte, 0, 100)
		b := bytes.NewBuffer([]byte("-test string"))
		n, out, err := readSwitch('+', unmarshalSimpleString, b, buf)
		require.Nil(t, out)
		require.Equal(t, 1, n)
		require.Error(t, err)
	})
}

func TestReadArray(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		buf := []byte("*1\r\n11\r\ntest string\r\n")
		b := &bytes.Buffer{}
		res := NewResult(buf)
		arrResult := NewArrayReader(buf, b, res)
		arrResult, err := readArray(b, arrResult)
		require.NoError(t, err)
	})
	t.Run("fail on unmarshalCount", func(t *testing.T) {
		buf := []byte("*b\r\n11\r\ntest string\r\n")
		b := &bytes.Buffer{}
		res := NewResult(buf)
		arrResult := NewArrayReader(buf, b, res)
		arrResult, err := readArray(b, arrResult)
		require.Error(t, err)
	})
}
