package greddis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/mikn/greddis/mocks/mock_driver"
	"github.com/stretchr/testify/require"
)

//go:generate mockgen -destination ./mocks/mock_driver/mock_valuer.go database/sql/driver Valuer
func TestWriteCommand(t *testing.T) {
	t.Run("setup", func(t *testing.T) {
		var out = &bytes.Buffer{}
		var origBuf = make([]byte, 1000)
		var cmd = &command{bufw: bufio.NewWriter(out)}
		cmd.start(origBuf, 2)
		cmd.add("GET")
		cmd.add("testkey")
		cmd.flush()
		require.Equal(t, "*2\r\n$3\r\nGET\r\n$7\r\ntestkey\r\n", out.String())
	})
	t.Run("length mismatch", func(t *testing.T) {
		var out = &bytes.Buffer{}
		var origBuf = make([]byte, 1000)
		var cmd = &command{bufw: bufio.NewWriter(out)}
		cmd.start(origBuf, 3)
		cmd.add("GET")
		cmd.add("testkey")
		_, err := cmd.flush()
		require.Error(t, err)
	})
	type testUnsafe struct {
		name   string
		value  interface{}
		result []byte
	}
	strVal := "hello"
	byteVal := []byte("hello")
	intVal := 55
	tests := []testUnsafe{
		{"string", "hello", []byte("*1\r\n$5\r\nhello\r\n")},
		{"[]byte", []byte("hello"), []byte("*1\r\n$5\r\nhello\r\n")},
		{"int", 55, []byte("*1\r\n$2\r\n55\r\n")},
		{"*string", &strVal, []byte("*1\r\n$5\r\nhello\r\n")},
		{"*[]byte", &byteVal, []byte("*1\r\n$5\r\nhello\r\n")},
		{"*int", &intVal, []byte("*1\r\n$2\r\n55\r\n")},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("addUnsafe_%s", test.name), func(t *testing.T) {
			var out = &bytes.Buffer{}
			var origBuf = make([]byte, 1000)
			var cmd = &command{bufw: bufio.NewWriter(out)}
			cmd.start(origBuf, 1)
			err := cmd.addUnsafe(test.value)
			require.NoError(t, err)
			_, err = cmd.flush()
			require.NoError(t, err)
			require.Equal(t, test.result, out.Bytes())
		})
	}
	t.Run("driver.Valuer success", func(t *testing.T) {
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockValuer = mock_driver.NewMockValuer(ctrl)
		mockValuer.EXPECT().Value().Return([]byte("55"), nil)
		var out = &bytes.Buffer{}
		var origBuf = make([]byte, 1000)
		var cmd = &command{bufw: bufio.NewWriter(out)}
		cmd.start(origBuf, 1)
		err := cmd.addUnsafe(mockValuer)
		require.NoError(t, err)
		_, err = cmd.flush()
		require.NoError(t, err)
		require.Equal(t, []byte("*1\r\n$2\r\n55\r\n"), out.Bytes())
	})
	t.Run("driver.Valuer fail", func(t *testing.T) {
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockValuer = mock_driver.NewMockValuer(ctrl)
		mockValuer.EXPECT().Value().Return(nil, errors.New("EOF"))
		var out = &bytes.Buffer{}
		var origBuf = make([]byte, 1000)
		var cmd = &command{bufw: bufio.NewWriter(out)}
		cmd.start(origBuf, 1)
		err := cmd.addUnsafe(mockValuer)
		require.Error(t, err)
	})
}
