package greddis_test

import (
	"bufio"
	"bytes"
	"errors"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/mikn/greddis"
	"github.com/mikn/greddis/mocks/mock_driver"
	"github.com/stretchr/testify/require"
)

//go:generate mockgen -destination ./mocks/mock_driver/mock_valuer.go database/sql/driver Valuer
func TestArrayWriter(t *testing.T) {
	t.Run("NewArrayWriter", func(t *testing.T) {
		out := &bytes.Buffer{}
		arrw := greddis.NewArrayWriter(bufio.NewWriter(out))
		arrw.Init(2).Add("GET", "testkey")
		arrw.Flush()
		require.Equal(t, "*2\r\n$3\r\nGET\r\n$7\r\ntestkey\r\n", out.String())
	})
	t.Run("Flush", func(t *testing.T) {
		t.Run("length mismatch", func(t *testing.T) {
			out := &bytes.Buffer{}
			arrw := greddis.NewArrayWriter(bufio.NewWriter(out))
			arrw.Init(3).Add("GET", "testkey")
			err := arrw.Flush()
			require.Error(t, err)
		})
	})
	t.Run("Len", func(t *testing.T) {
		out := &bytes.Buffer{}
		arrw := greddis.NewArrayWriter(bufio.NewWriter(out))
		arrw.Init(2)
		l := arrw.Len()
		require.Equal(t, 2, l)
	})
	t.Run("Add", func(t *testing.T) {
		type testAdd struct {
			name   string
			value  interface{}
			result []byte
		}
		strVal := "hello"
		byteVal := []byte("hello")
		intVal := 55
		strIntVal := greddis.StrInt(56)
		tests := []testAdd{
			{"string", strVal, []byte("*1\r\n$5\r\nhello\r\n")},
			{"*string", &strVal, []byte("*1\r\n$5\r\nhello\r\n")},
			{"[]byte", byteVal, []byte("*1\r\n$5\r\nhello\r\n")},
			{"*[]byte", &byteVal, []byte("*1\r\n$5\r\nhello\r\n")},
			{"int", intVal, []byte("*1\r\n:55\r\n")},
			{"*int", &intVal, []byte("*1\r\n:55\r\n")},
			{"StrInt", strIntVal, []byte("*1\r\n$2\r\n56\r\n")},
			{"*StrInt", &strIntVal, []byte("*1\r\n$2\r\n56\r\n")},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
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
		t.Run("driver.Valuer wrong return type", func(t *testing.T) {
			var ctrl = gomock.NewController(t)
			defer ctrl.Finish()
			var mockValuer = mock_driver.NewMockValuer(ctrl)
			mockValuer.EXPECT().Value().Return("55", nil)
			out := &bytes.Buffer{}
			arrw := greddis.NewArrayWriter(bufio.NewWriter(out))
			err := arrw.Init(1).Add(mockValuer)
			require.Error(t, err)
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
	})
}

func TestArrayReader(t *testing.T) {
	t.Run("Len", func(t *testing.T) {
		t.Run("not initialized", func(t *testing.T) {
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString("")))
			ar := greddis.NewArrayReader(r)
			l := ar.Len()
			require.Equal(t, 0, l)
		})
		t.Run("initialized length 1", func(t *testing.T) {
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(greddis.TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanBulkString)
			require.Equal(t, 1, ar.Len())
		})
	})
	t.Run("Next", func(t *testing.T) {
		t.Run("different than Init()", func(t *testing.T) {
			TEST_ARRAY := "*2\r\n$3\r\nGET\r\n+testkey\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanBulkString)
			var get string
			err := ar.Scan(&get)
			require.NoError(t, err)
			require.Equal(t, "GET", get)
			var str string
			err = ar.Next(greddis.ScanSimpleString).Scan(&str)
			require.NoError(t, err)
			require.Equal(t, "testkey", str)
		})
		t.Run("multiple different", func(t *testing.T) {
			TEST_ARRAY := "*2\r\n$3\r\nGET\r\n+testkey\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init()
			ar.Next(greddis.ScanBulkString, greddis.ScanSimpleString)
			var get string
			err := ar.Scan(&get)
			require.NoError(t, err)
			require.Equal(t, "GET", get)
			var str string
			err = ar.Scan(&str)
			require.NoError(t, err)
			require.Equal(t, "testkey", str)
		})
		t.Run("consecutive calls", func(t *testing.T) {
			TEST_ARRAY := "*2\r\n$3\r\nGET\r\n+testkey\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init()
			var get string
			err := ar.Next(greddis.ScanBulkString).Scan(&get)
			require.NoError(t, err)
			require.Equal(t, "GET", get)
			var str string
			err = ar.Next(greddis.ScanSimpleString).Scan(&str)
			require.NoError(t, err)
			require.Equal(t, "testkey", str)
		})
	})
	t.Run("Scan", func(t *testing.T) {
		t.Run("bulkstrings", func(t *testing.T) {
			TEST_ARRAY := "*2\r\n$3\r\nGET\r\n$7\r\ntestkey\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanBulkString)
			var get string
			var str string
			err := ar.Scan(&get, &str)
			require.NoError(t, err)
			require.Equal(t, "GET", get)
			require.Equal(t, "testkey", str)
		})
		t.Run("simplestring(int)", func(t *testing.T) {
			TEST_ARRAY := "*1\r\n:42\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanInteger)
			var i int
			err := ar.Scan(&i)
			require.NoError(t, err)
			require.Equal(t, 42, i)
		})
		t.Run("multiple consecutive values", func(t *testing.T) {
			TEST_ARRAY := "*3\r\n+teststring\r\n+teststring2\r\n+teststring3\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanSimpleString)
			var str string
			err := ar.Scan(&str)
			require.NoError(t, err)
			require.Equal(t, "teststring", str)
			err = ar.Scan(&str)
			require.NoError(t, err)
			require.Equal(t, "teststring2", str)
			err = ar.Scan(&str)
			require.NoError(t, err)
			require.Equal(t, "teststring3", str)
		})
		t.Run("errorstring", func(t *testing.T) {
			TEST_ARRAY := "*1\r\n-testerror\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanSimpleString)
			var str string
			err := ar.Scan(&str)
			require.Error(t, err)
			require.EqualError(t, err, "Redis error: testerror")
		})
		t.Run("unexpected prefix", func(t *testing.T) {
			TEST_ARRAY := "*1\r\n!testerror\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanSimpleString)
			var str string
			err := ar.Scan(&str)
			require.Error(t, err)
			require.Equal(t, "", str)
		})
		t.Run("passing more variables to Scan than is in array", func(t *testing.T) {
			TEST_ARRAY := "*1\r\n+testerror\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanSimpleString)
			var str string
			var str2 string
			err := ar.Scan(&str, &str2)
			require.Error(t, err)
			require.Equal(t, greddis.ErrNoMoreRows, err)
			require.Equal(t, "testerror", str)
		})
		t.Run("error on result scan", func(t *testing.T) {
			TEST_ARRAY := "*1\r\n:12fffff\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanInteger)
			var i int
			err := ar.Scan(&i)
			require.Error(t, err)
			require.Equal(t, 0, i)
		})
	})
	t.Run("SwitchOnNext", func(t *testing.T) {
		t.Run("error on next()", func(t *testing.T) {
			TEST_ARRAY := "*1\r\n:12fffff\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanSimpleString)
			res := ar.SwitchOnNext()
			require.Equal(t, "", res)
		})
		t.Run("success (1 var)", func(t *testing.T) {
			TEST_ARRAY := "*1\r\n+teststring\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanSimpleString)
			str := ar.SwitchOnNext()
			require.Equal(t, "teststring", str)
		})
		t.Run("success (2 vars)", func(t *testing.T) {
			TEST_ARRAY := "*2\r\n+teststring\r\n+teststring2\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanSimpleString)
			str := ar.SwitchOnNext()
			require.Equal(t, "teststring", str)
			str = ar.SwitchOnNext()
			require.Equal(t, "teststring2", str)
		})
	})
	t.Run("Expect", func(t *testing.T) {
		t.Run("match found (1 var) and read next", func(t *testing.T) {
			TEST_ARRAY := "*2\r\n+teststring\r\n+teststring2\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanSimpleString)
			err := ar.Expect("teststring")
			require.NoError(t, err)
			var str string
			err = ar.Scan(&str)
			require.NoError(t, err)
			require.Equal(t, "teststring2", str)
		})
		t.Run("match found (1 var)", func(t *testing.T) {
			TEST_ARRAY := "*1\r\n+teststring\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanSimpleString)
			err := ar.Expect("teststring")
			require.NoError(t, err)
		})
		t.Run("match found (3 vars)", func(t *testing.T) {
			TEST_ARRAY := "*1\r\n+teststring\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanSimpleString)
			err := ar.Expect("blah", "teststring", "beheh")
			require.NoError(t, err)
		})
		t.Run("match found 2 times", func(t *testing.T) {
			TEST_ARRAY := "*2\r\n+teststring\r\n+teststring2\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanSimpleString)
			err := ar.Expect("teststring")
			require.NoError(t, err)
			err = ar.Expect("teststring2")
			require.NoError(t, err)
		})
		t.Run("match not found (1 var)", func(t *testing.T) {
			TEST_ARRAY := "*1\r\n+teststring2\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanSimpleString)
			err := ar.Expect("teststring")
			require.Error(t, err)
		})
		t.Run("match not found (1 var) and read next", func(t *testing.T) {
			TEST_ARRAY := "*2\r\n+teststring2\r\n+teststring\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanSimpleString)
			err := ar.Expect("teststring")
			require.Error(t, err)
			var str string
			err = ar.Scan(&str)
			require.NoError(t, err)
			require.Equal(t, "teststring", str)
		})
		t.Run("match not found (3 vars)", func(t *testing.T) {
			TEST_ARRAY := "*1\r\n+teststring2\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanSimpleString)
			err := ar.Expect("blah", "teststring", "beheh")
			require.Error(t, err)
		})
		t.Run("match not found (0 vars)", func(t *testing.T) {
			TEST_ARRAY := "*1\r\n+teststring2\r\n"
			r := greddis.NewReader(bufio.NewReader(bytes.NewBufferString(TEST_ARRAY)))
			ar := greddis.NewArrayReader(r)
			ar.Init(greddis.ScanSimpleString)
			err := ar.Expect()
			require.Error(t, err)
		})
	})
}
