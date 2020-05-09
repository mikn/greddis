package greddis_test

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/mikn/greddis"
	"github.com/mikn/greddis/mocks/mock_driver"
	"github.com/stretchr/testify/require"
)

//go:generate mockgen -destination ./mocks/mock_driver/mock_valuer.go database/sql/driver Valuer
func TestArrayWriter(t *testing.T) {
	t.Run("setup", func(t *testing.T) {
		out := &bytes.Buffer{}
		arrw := greddis.NewArrayWriter(bufio.NewWriter(out))
		arrw.Init(2).Add("GET", "testkey")
		arrw.Flush()
		require.Equal(t, "*2\r\n$3\r\nGET\r\n$7\r\ntestkey\r\n", out.String())
	})
	t.Run("length mismatch", func(t *testing.T) {
		out := &bytes.Buffer{}
		arrw := greddis.NewArrayWriter(bufio.NewWriter(out))
		arrw.Init(3).Add("GET", "testkey")
		err := arrw.Flush()
		require.Error(t, err)
	})
	type testAdd struct {
		name   string
		value  interface{}
		result []byte
	}
	strVal := "hello"
	byteVal := []byte("hello")
	intVal := 55
	tests := []testAdd{
		{"*string", &strVal, []byte("*1\r\n$5\r\nhello\r\n")},
		{"*[]byte", &byteVal, []byte("*1\r\n$5\r\nhello\r\n")},
		{"*int", &intVal, []byte("*1\r\n$2\r\n55\r\n")},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("Add_%s", test.name), func(t *testing.T) {
			out := &bytes.Buffer{}
			arrw := greddis.NewArrayWriter(bufio.NewWriter(out))
			err := arrw.Init(1).Add(test.value)
			require.NoError(t, err)
			err = arrw.Flush()
			require.NoError(t, err)
			require.Equal(t, test.result, out.Bytes())
		})
	}
	t.Run("driver.Valuer success", func(t *testing.T) {
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockValuer = mock_driver.NewMockValuer(ctrl)
		mockValuer.EXPECT().Value().Return([]byte("55"), nil)
		out := &bytes.Buffer{}
		arrw := greddis.NewArrayWriter(bufio.NewWriter(out))
		err := arrw.Init(1).Add(mockValuer)
		require.NoError(t, err)
		err = arrw.Flush()
		require.NoError(t, err)
		require.Equal(t, []byte("*1\r\n$2\r\n55\r\n"), out.Bytes())
	})
	t.Run("driver.Valuer fail", func(t *testing.T) {
		var ctrl = gomock.NewController(t)
		defer ctrl.Finish()
		var mockValuer = mock_driver.NewMockValuer(ctrl)
		mockValuer.EXPECT().Value().Return(nil, errors.New("EOF"))
		out := &bytes.Buffer{}
		arrw := greddis.NewArrayWriter(bufio.NewWriter(out))
		err := arrw.Init(1).Add(mockValuer)
		require.Error(t, err)
	})
}
