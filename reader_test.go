package greddis

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	TEST_STRING                = "+hello\r\n"
	TEST_STRING_DISCARD        = "+hello\r\n+hello2\r\n"
	TEST_ERROR                 = "-error\r\n"
	TEST_BULK_STRING           = "$5\r\nhello\r\n"
	TEST_BULK_STRING_DISCARD   = "$5\r\nhello\r\n$6\r\nhello2\r\n"
	TEST_BULK_STRING_ATOI_FAIL = "$hello\r\n"
	TEST_INTEGER               = ":42\r\n"
	TEST_INTEGER_DISCARD       = ":42\r\n:43\r\n"
	TEST_ARRAY_ATOI_FAIL       = "*hello\r\n"
	TEST_ARRAY                 = "*1\r\n$5\r\nhello\r\n"
)

// TODO Test Bytes()
// TODO Test WriteTo()

func TestExpectType(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		r := bufio.NewReader(bytes.NewBufferString(TEST_STRING))
		err := expectType(r, '+')
		require.NoError(t, err)
	})
	t.Run("redis error", func(t *testing.T) {
		r := bufio.NewReader(bytes.NewBufferString(TEST_ERROR))
		err := expectType(r, '+')
		require.Error(t, err)
		require.EqualError(t, err, "Redis error: error")
	})
	t.Run("one value failure", func(t *testing.T) {
		r := bufio.NewReader(bytes.NewBufferString(TEST_STRING))
		err := expectType(r, ':')
		require.Error(t, err)
		require.Equal(t, ErrWrongToken(':', '+'), err)
	})
	t.Run("read failure", func(t *testing.T) {
		r := bufio.NewReader(bytes.NewBufferString(""))
		err := expectType(r, ':')
		require.Error(t, err)
		require.Equal(t, io.EOF, err)
	})
}

func TestScanToDelim(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		r := bufio.NewReader(bytes.NewBufferString(TEST_STRING))
		i, err := scanToDelim(r, sep)
		require.NoError(t, err)
		require.Equal(t, 6, i)

	})
	t.Run("error", func(t *testing.T) {
		r := bufio.NewReader(bytes.NewBufferString(""))
		i, err := scanToDelim(r, sep)
		require.Error(t, err)
		require.Equal(t, 0, i)
	})
}

func TestScanSimpleStringInternal(t *testing.T) {
	t.Run("fail on type", func(t *testing.T) {
		r := bufio.NewReader(bytes.NewBufferString(TEST_ERROR))
		i, err := scanSimpleString(r, ':')
		require.Error(t, err)
		require.EqualError(t, err, "Redis error: error")
		require.Equal(t, 0, i)
	})
	t.Run("fail on scan", func(t *testing.T) {
		r := bufio.NewReader(bytes.NewBufferString(":"))
		i, err := scanSimpleString(r, ':')
		require.Error(t, err)
		require.Equal(t, io.EOF, err)
		require.Equal(t, 0, i)
	})
	t.Run("success", func(t *testing.T) {
		r := bufio.NewReader(bytes.NewBufferString(TEST_STRING))
		i, err := scanSimpleString(r, '+')
		require.NoError(t, err)
		require.Equal(t, 5, i)
	})
}

func TestScanArray(t *testing.T) {
	t.Run("simpleStringScan fail", func(t *testing.T) {
		r := NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ERROR)))
		err := r.Next(ScanArray)
		require.Error(t, err)
		require.EqualError(t, err, "Redis error: error")
		require.Zero(t, r.Int())
	})
	t.Run("Atoi fail", func(t *testing.T) {
		r := NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY_ATOI_FAIL)))
		err := r.Next(ScanArray)
		require.Error(t, err)
		require.EqualError(t, err, "strconv.Atoi: parsing \"hello\": invalid syntax")
		require.Zero(t, r.Int())
	})
	t.Run("success", func(t *testing.T) {
		r := NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
		err := r.Next(ScanArray)
		require.NoError(t, err)
		require.Equal(t, 1, r.Len())
		r.tokenLen = 0
		err = r.Next(ScanBulkString)
		require.NoError(t, err)
		require.Equal(t, "hello", r.String())
	})
}

func TestScanBulkString(t *testing.T) {
	t.Run("simpleStringScan fail", func(t *testing.T) {
		r := NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ERROR)))
		err := r.Next(ScanBulkString)
		require.Error(t, err)
		require.EqualError(t, err, "Redis error: error")
	})
	t.Run("Atoi fail", func(t *testing.T) {
		r := NewReader(bufio.NewReader(bytes.NewBufferString(TEST_BULK_STRING_ATOI_FAIL)))
		err := r.Next(ScanBulkString)
		require.Error(t, err)
		require.EqualError(t, err, "strconv.Atoi: parsing \"hello\": invalid syntax")
	})
	t.Run("success", func(t *testing.T) {
		r := NewReader(bufio.NewReader(bytes.NewBufferString(TEST_BULK_STRING)))
		err := r.Next(ScanBulkString)
		require.NoError(t, err)
		require.Equal(t, "hello", r.String())
	})
	t.Run("discard success", func(t *testing.T) {
		r := NewReader(bufio.NewReader(bytes.NewBufferString(TEST_BULK_STRING_DISCARD)))
		err := r.Next(ScanBulkString)
		require.NoError(t, err)
		require.Equal(t, "hello", r.String())
		err = r.Next(ScanBulkString)
		require.NoError(t, err)
		require.Equal(t, "hello2", r.String())
	})
}

func TestScanSimpleString(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		r := NewReader(bufio.NewReader(bytes.NewBufferString(TEST_STRING)))
		err := r.Next(ScanSimpleString)
		require.NoError(t, err)
		require.Equal(t, "hello", r.String())
	})
	t.Run("success multiple returns", func(t *testing.T) {
		r := NewReader(bufio.NewReader(bytes.NewBufferString(TEST_STRING)))
		err := r.Next(ScanSimpleString)
		require.NoError(t, err)
		require.Equal(t, "hello", r.String())
		require.Equal(t, []byte("hello"), r.Bytes())
	})
	t.Run("discard success", func(t *testing.T) {
		r := NewReader(bufio.NewReader(bytes.NewBufferString(TEST_STRING_DISCARD)))
		err := r.Next(ScanSimpleString)
		require.NoError(t, err)
		require.Equal(t, "hello", r.String())
		err = r.Next(ScanSimpleString)
		require.NoError(t, err)
		require.Equal(t, "hello2", r.String())
	})
}
func TestScanInteger(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		r := NewReader(bufio.NewReader(bytes.NewBufferString(TEST_INTEGER)))
		err := r.Next(ScanInteger)
		require.NoError(t, err)
		require.Equal(t, "42", r.String())
	})
	t.Run("discard success", func(t *testing.T) {
		r := NewReader(bufio.NewReader(bytes.NewBufferString(TEST_INTEGER_DISCARD)))
		err := r.Next(ScanInteger)
		require.NoError(t, err)
		require.Equal(t, "42", r.String())
		err = r.Next(ScanInteger)
		require.NoError(t, err)
		require.Equal(t, "43", r.String())
	})
}

func TestWriteTo(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		r := NewReader(bufio.NewReader(bytes.NewBufferString(TEST_STRING)))
		s := &strings.Builder{}
		err := r.Next(ScanSimpleString)
		require.NoError(t, err)
		i, err := r.WriteTo(s)
		require.NoError(t, err)
		require.Equal(t, int64(5), i)
		require.Equal(t, "hello", s.String())
	})
	t.Run("discard success", func(t *testing.T) {
		r := NewReader(bufio.NewReader(bytes.NewBufferString(TEST_STRING_DISCARD)))
		s := &strings.Builder{}
		s2 := &strings.Builder{}
		err := r.Next(ScanSimpleString)
		require.NoError(t, err)
		i, err := r.WriteTo(s)
		require.NoError(t, err)
		require.Equal(t, int64(5), i)
		require.Equal(t, "hello", s.String())
		require.Empty(t, r.String())
		err = r.Next(ScanSimpleString)
		require.NoError(t, err)
		i, err = r.WriteTo(s2)
		require.NoError(t, err)
		require.Equal(t, int64(6), i)
		require.Equal(t, "hello2", s2.String())
		require.Empty(t, r.String())
	})
}
