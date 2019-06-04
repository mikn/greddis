package redis

import (
	"bytes"
	"errors"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestParseBulkString(t *testing.T) {
	t.Run("normal string that fits", func(t *testing.T) {
		var buf = []byte("11\r\ntest string\r\n")
		var b = &bytes.Buffer{}
		var out, err = parseBulkString(b, buf, 500*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, []byte("test string"), out)
	})
	t.Run("malformed string", func(t *testing.T) {
		var buf = []byte("11\r\ntest stringss")
		var b = &bytes.Buffer{}
		var out, err = parseBulkString(b, buf, 500*time.Millisecond)
		require.Error(t, err, ErrMalformedString)
		require.Equal(t, []byte("test stringss"), out)
	})
	t.Run("string that is longer than buf", func(t *testing.T) {
		var buf = make([]byte, 0, 13)
		buf = append(buf, "11\r\ntest stri"...)
		var b = bytes.NewBuffer([]byte("ng\r\n"))
		var out, err = parseBulkString(b, buf, 500*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, []byte("test string"), out)
	})
	t.Run("multiple reads required", func(t *testing.T) {
		// OMG this was a PITA to figure out
		var ctrl = gomock.NewController(t)
		var mockReader = NewMockReader(ctrl)
		var buf = make([]byte, 0, 9)
		buf = append(buf, "11\r\ntest "...)
		mockReader.EXPECT().Read(make([]byte, 8)).DoAndReturn(func(b []byte) (int, error) {
			copy(b, []byte("string"))
			return 6, nil
		})
		mockReader.EXPECT().Read(make([]byte, 2)).DoAndReturn(func(b []byte) (int, error) {
			copy(b, []byte("\r\n"))
			return 2, nil
		})
		var msg, err = parseBulkString(mockReader, buf, 500*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, []byte("test string"), msg)
	})
	t.Run("Error on intRead", func(t *testing.T) {
		var ctrl = gomock.NewController(t)
		var mockReader = NewMockReader(ctrl)
		var buf = []byte("This is a")
		mockReader.EXPECT().Read(gomock.Any()).Return(0, errors.New("EOF"))
		var msg, err = parseBulkString(mockReader, buf, 500*time.Millisecond)
		require.Error(t, err)
		require.Nil(t, msg)
	})
	t.Run("Error on read after int", func(t *testing.T) {
		var ctrl = gomock.NewController(t)
		var mockReader = NewMockReader(ctrl)
		var buf = []byte("11\r\ntest ")
		mockReader.EXPECT().Read(gomock.Any()).Return(0, errors.New("EOF"))
		var msg, err = parseBulkString(mockReader, buf, 500*time.Millisecond)
		require.Error(t, err)
		require.Nil(t, msg)
	})
	t.Run("reach timeout", func(t *testing.T) {
		var ctrl = gomock.NewController(t)
		var mockReader = NewMockReader(ctrl)
		var buf = []byte("11\r\ntest ")
		mockReader.EXPECT().Read(gomock.Any()).Return(0, nil)
		var msg, err = parseBulkString(mockReader, buf, 0)
		require.Error(t, err, ErrNoData)
		require.Nil(t, msg)
	})
}

func TestParseSimpleString(t *testing.T) {
	t.Run("Reader with fewer bytes than buf size", func(t *testing.T) {
		var buf = []byte("This is a test\r\n")
		var b = &bytes.Buffer{}
		var msg, err = parseSimpleString(b, buf, 500*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, []byte("This is a test"), msg)
	})
	t.Run("Reader with more bytes than buf size", func(t *testing.T) {
		var b = bytes.NewBuffer([]byte(" test\r\n"))
		var buf = []byte("This is a")
		var msg, err = parseSimpleString(b, buf, 500*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, []byte("This is a test"), msg)
	})
	t.Run("Expand buffer", func(t *testing.T) {
		var b = bytes.NewBuffer([]byte(" test\r\n"))
		var buf = make([]byte, 0, 9)
		buf = append(buf, []byte("This is a")...)
		var msg, err = parseSimpleString(b, buf, 500*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, []byte("This is a test"), msg)
	})
	t.Run("Error on read", func(t *testing.T) {
		var ctrl = gomock.NewController(t)
		var mockReader = NewMockReader(ctrl)
		var buf = []byte("This is a")
		mockReader.EXPECT().Read(gomock.Any()).Return(0, errors.New("EOF"))
		var msg, err = parseSimpleString(mockReader, buf, 500*time.Millisecond)
		require.Error(t, err)
		require.Nil(t, msg)
	})
	t.Run("Expect timeout", func(t *testing.T) {
		var ctrl = gomock.NewController(t)
		var mockReader = NewMockReader(ctrl)
		var buf = []byte("This is a")
		mockReader.EXPECT().Read(gomock.Any()).Return(0, nil)
		var msg, err = parseSimpleString(mockReader, buf, 0)
		require.Error(t, err, ErrNoData)
		require.Nil(t, msg)
	})
}

func TestReadInteger(t *testing.T) {
	t.Run("read int", func(t *testing.T) {
		var b = bytes.NewBuffer([]byte(":256622\r\n"))
		var by = make([]byte, 0, 16)
		var i, err = readInteger(b, by, 500*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, 256622, i)
	})
	t.Run("Error EOF", func(t *testing.T) {
		var b = bytes.NewBuffer([]byte(":256622"))
		var by = make([]byte, 0, 16)
		var i, err = readInteger(b, by, 500*time.Millisecond)
		require.Error(t, err)
		require.Zero(t, i)
	})
	t.Run("Error not an int", func(t *testing.T) {
		var b = bytes.NewBuffer([]byte(":25bb25\r\n"))
		var by = make([]byte, 0, 16)
		var i, err = readInteger(b, by, 500*time.Millisecond)
		require.Error(t, err)
		require.Zero(t, i)
	})
}

func TestReadBulkString(t *testing.T) {
	t.Run("normal string", func(t *testing.T) {
		var buf = make([]byte, 100)
		var b = bytes.NewBuffer([]byte("$11\r\ntest string\r\n"))
		var out, err = readBulkString(b, buf, 500*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, []byte("test string"), out)
	})
	t.Run("nil string", func(t *testing.T) {
		var buf = make([]byte, 100)
		var b = bytes.NewBuffer([]byte("$-1\r\n"))
		var out, err = readBulkString(b, buf, 500*time.Millisecond)
		require.NoError(t, err)
		require.Nil(t, out)
	})
}

func TestReadSimpleString(t *testing.T) {
	t.Run("normal string", func(t *testing.T) {
		var buf = make([]byte, 100)
		var b = bytes.NewBuffer([]byte("+test string\r\n"))
		var out, err = readSimpleString(b, buf, 500*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, []byte("test string"), out)
	})
}

func TestReadSwitch(t *testing.T) {
	t.Run("error string", func(t *testing.T) {
		var buf = make([]byte, 100)
		var r = bytes.NewBuffer([]byte("-test string\r\n"))
		var out, err = readSwitch('+', parseSimpleString, r, buf, 500*time.Millisecond)
		require.Nil(t, out)
		require.Error(t, err, errors.New("test string"))
	})
	t.Run("error string", func(t *testing.T) {
		var buf = make([]byte, 100)
		var r = bytes.NewBuffer([]byte("#test string\r\n"))
		var out, err = readSwitch('+', parseSimpleString, r, buf, 500*time.Millisecond)
		require.Nil(t, out)
		require.Error(t, err)
	})
	t.Run("error on read", func(t *testing.T) {
		var ctrl = gomock.NewController(t)
		var mockReader = NewMockReader(ctrl)
		var buf = make([]byte, 100)
		mockReader.EXPECT().Read(gomock.Any()).Return(0, errors.New("EOF"))
		var msg, err = readSwitch('+', parseSimpleString, mockReader, buf, 500*time.Millisecond)
		require.Nil(t, msg)
		require.Error(t, err)
	})
	t.Run("error on error string parse", func(t *testing.T) {
		var buf = make([]byte, 100)
		var r = bytes.NewBuffer([]byte("-test string"))
		var out, err = readSwitch('+', parseSimpleString, r, buf, 500*time.Millisecond)
		require.Nil(t, out)
		require.Error(t, err)
	})
}
