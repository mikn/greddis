package greddis

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeBulkString(t *testing.T) {
	t.Run("regular string", func(t *testing.T) {
		var buf []byte
		buf = marshalBulkString([]byte("test string"), buf)
		require.NotNil(t, buf)
		require.Equal(t, []byte("$11\r\ntest string\r\n"), buf)
	})
	t.Run("nil string", func(t *testing.T) {
		var buf []byte
		buf = marshalBulkString(nil, buf)
		require.NotNil(t, buf)
		require.Equal(t, []byte("$-1\r\n"), buf)
	})
	t.Run("empty string", func(t *testing.T) {
		var buf []byte
		buf = marshalBulkString([]byte(""), buf)
		require.NotNil(t, buf)
		require.Equal(t, []byte("$0\r\n\r\n"), buf)
	})
}

func TestCommandReset(t *testing.T) {
}

func TestArray(t *testing.T) {
	t.Run("test reset", func(t *testing.T) {
		var origBuf = make([]byte, 100)
		var arr = &respArray{}
		arr.reset(origBuf)
		require.Equal(t, 0, len(arr.origBuf))
		require.Equal(t, 0, len(arr.buf))
		require.Equal(t, 100, cap(arr.origBuf))
		require.Equal(t, cap(arr.origBuf)-maxBulkLen, cap(arr.buf))
	})
	t.Run("test reset with too small buffer", func(t *testing.T) {
		var origBuf = make([]byte, 10)
		var arr = &respArray{}
		arr.reset(origBuf)
		require.Equal(t, 0, len(arr.origBuf))
		require.Equal(t, 0, len(arr.buf))
		require.Equal(t, 10, cap(arr.origBuf))
		require.Equal(t, 32-maxBulkLen, cap(arr.buf))
	})
	t.Run("nil array", func(t *testing.T) {
		var origBuf = make([]byte, 100)
		var arr = &respArray{}
		arr.setToNil(origBuf)
		origBuf = arr.marshal()
		require.Equal(t, []byte("*-1\r\n"), origBuf)
	})
	t.Run("empty array", func(t *testing.T) {
		var origBuf = make([]byte, 100)
		var arr = &respArray{}
		arr.reset(origBuf)
		origBuf = arr.marshal()
		require.Equal(t, []byte("*0\r\n"), origBuf)
	})
	t.Run("one item array", func(t *testing.T) {
		var origBuf = make([]byte, 100)
		var arr = &respArray{}
		arr.reset(origBuf)
		arr.addBulkString([]byte("test string"))
		origBuf = arr.marshal()
		require.Equal(t, []byte("*1\r\n$11\r\ntest string\r\n"), origBuf)
	})
	t.Run("three item array", func(t *testing.T) {
		var origBuf = make([]byte, 100)
		var arr = &respArray{}
		arr.reset(origBuf)
		arr.addBulkString([]byte("test string"))
		arr.addBulkString([]byte("test string"))
		arr.addBulkString([]byte("test string"))
		origBuf = arr.marshal()
		require.Equal(t, []byte("*3\r\n$11\r\ntest string\r\n$11\r\ntest string\r\n$11\r\ntest string\r\n"), origBuf)
	})
}
