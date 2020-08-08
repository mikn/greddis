package greddis_test

import (
	"bufio"
	"bytes"
	"errors"
	"strings"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/mikn/greddis"
	"github.com/mikn/greddis/mocks/mock_driver"
	"github.com/mikn/greddis/mocks/mock_io"
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
			buf := make([]byte, 0, 4096)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, &strings.Reader{}, res)
			l := ar.Len()
			require.Equal(t, 0, l)
		})
		t.Run("initialized length 5", func(t *testing.T) {
			buf := make([]byte, 0, 4096)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, &strings.Reader{}, res)
			ar.Init(buf, 5)
			l := ar.Len()
			require.Equal(t, 5, l)
		})
	})
	t.Run("ResetReader", func(t *testing.T) {
		t.Run("assign reader", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			buf := make([]byte, 0, 10)
			eofError := errors.New("EOF")
			r := &strings.Reader{}
			mockReader := mock_io.NewMockReader(ctrl)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			mockReader.EXPECT().Read(gomock.Any()).Return(0, eofError)
			ar.Init(buf, 1)
			var str string
			ar.ResetReader(mockReader)
			err := ar.Scan(&str)
			require.Equal(t, eofError, err)
		})
	})
	t.Run("Scan", func(t *testing.T) {
		t.Run("small bulkstring", func(t *testing.T) {
			TEST_ARRAY := "$3\r\nGET\r\n$7\r\ntestkey\r\n"
			r := strings.NewReader(TEST_ARRAY)
			buf := make([]byte, 0, 4096)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			ar.Init(buf, 2)
			var get string
			var str string
			err := ar.Scan(&get, &str)
			require.NoError(t, err)
			require.Equal(t, "GET", get)
			require.Equal(t, "testkey", str)
		})
		t.Run("large bulkstring", func(t *testing.T) {
			TEST_ARRAY := "$3\r\nGET\r\n$16\r\ntestkey123456789\r\n"
			r := strings.NewReader(TEST_ARRAY)
			buf := make([]byte, 0, 10)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			ar.Init(buf, 2)
			var get string
			var str string
			err := ar.Scan(&get)
			require.NoError(t, err)
			require.Equal(t, "GET", get)
			err = ar.Scan(&str)
			require.NoError(t, err)
			require.Equal(t, "testkey123456789", str)
		})
		t.Run("simplestring(int)", func(t *testing.T) {
			TEST_ARRAY := ":teststring\r\n"
			r := strings.NewReader(TEST_ARRAY)
			buf := make([]byte, 0, 10)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			ar.Init(buf, 1)
			var str string
			err := ar.Scan(&str)
			require.NoError(t, err)
			require.Equal(t, "teststring", str)
		})
		t.Run("multiple consecutive values", func(t *testing.T) {
			TEST_ARRAY := ":teststring\r\n:teststring2\r\n:teststring3\r\n"
			r := strings.NewReader(TEST_ARRAY)
			buf := make([]byte, 0, 10)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			ar.Init(buf, 3)
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
			TEST_ARRAY := "-testerror\r\n"
			r := strings.NewReader(TEST_ARRAY)
			buf := make([]byte, 0, 10)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			ar.Init(buf, 1)
			var str string
			err := ar.Scan(&str)
			require.NoError(t, err)
			require.Equal(t, "testerror", str)
		})
		t.Run("unexpected prefix", func(t *testing.T) {
			TEST_ARRAY := "!testerror\r\n"
			r := strings.NewReader(TEST_ARRAY)
			buf := make([]byte, 0, 10)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			ar.Init(buf, 1)
			var str string
			err := ar.Scan(&str)
			require.Error(t, err)
			require.Equal(t, "", str)
		})
		t.Run("passing more variables to Scan than is in array", func(t *testing.T) {
			TEST_ARRAY := ":testerror\r\n"
			r := strings.NewReader(TEST_ARRAY)
			buf := make([]byte, 0, 10)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			ar.Init(buf, 1)
			var str string
			var str2 string
			err := ar.Scan(&str, &str2)
			require.Error(t, err)
			require.Equal(t, greddis.ErrNoMoreRows, err)
			require.Equal(t, "testerror", str)
		})
		t.Run("trigger truncate to 0", func(t *testing.T) {
			TEST_ARRAY := ":"
			r := strings.NewReader(TEST_ARRAY)
			buf := make([]byte, 0, 10)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			ar.Init(buf, 1)
			var str string
			err := ar.Scan(&str)
			require.Error(t, err)
		})
		t.Run("error on read", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockReader := mock_io.NewMockReader(ctrl)
			buf := make([]byte, 0, 10)
			eofError := errors.New("EOF")
			mockReader.EXPECT().Read(gomock.Any()).Return(0, eofError)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, mockReader, res)
			ar.Init(buf, 1)
			var str string
			err := ar.Scan(&str)
			require.Equal(t, eofError, err)
		})
		t.Run("error on result scan", func(t *testing.T) {
			TEST_ARRAY := ":12fffff\r\n"
			r := strings.NewReader(TEST_ARRAY)
			buf := make([]byte, 0, 10)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			ar.Init(buf, 1)
			var i int
			err := ar.Scan(&i)
			require.Error(t, err)
			require.Equal(t, 0, i)
		})
	})
	t.Run("SwitchOnNext", func(t *testing.T) {
		t.Run("error on next()", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockReader := mock_io.NewMockReader(ctrl)
			buf := make([]byte, 0, 10)
			eofError := errors.New("EOF")
			mockReader.EXPECT().Read(gomock.Any()).Return(0, eofError)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, mockReader, res)
			ar.Init(buf, 1)
			r := ar.SwitchOnNext()
			require.Equal(t, "", r)
		})
		t.Run("success (1 var)", func(t *testing.T) {
			TEST_ARRAY := ":teststring\r\n"
			r := strings.NewReader(TEST_ARRAY)
			buf := make([]byte, 0, 10)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			ar.Init(buf, 1)
			str := ar.SwitchOnNext()
			require.Equal(t, "teststring", str)
		})
		t.Run("success (3 vars)", func(t *testing.T) {
			TEST_ARRAY := ":teststring\r\n:teststring2\r\n:teststring3\r\n"
			r := strings.NewReader(TEST_ARRAY)
			buf := make([]byte, 0, 10)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			ar.Init(buf, 3)
			str := ar.SwitchOnNext()
			require.Equal(t, "teststring", str)
			str = ar.SwitchOnNext()
			require.Equal(t, "teststring2", str)
		})
	})
	t.Run("Expect", func(t *testing.T) {
		t.Run("match found (1 var) and read next", func(t *testing.T) {
			TEST_ARRAY := ":teststring\r\n:teststring2\r\n"
			r := strings.NewReader(TEST_ARRAY)
			buf := make([]byte, 0, 10)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			ar.Init(buf, 2)
			err := ar.Expect("teststring")
			require.NoError(t, err)
			var str string
			err = ar.Scan(&str)
			require.NoError(t, err)
			require.Equal(t, "teststring2", str)
		})
		t.Run("match found (1 var)", func(t *testing.T) {
			TEST_ARRAY := ":teststring\r\n"
			r := strings.NewReader(TEST_ARRAY)
			buf := make([]byte, 0, 10)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			ar.Init(buf, 1)
			err := ar.Expect("teststring")
			require.NoError(t, err)
		})
		t.Run("match found (3 vars)", func(t *testing.T) {
			TEST_ARRAY := ":teststring\r\n"
			r := strings.NewReader(TEST_ARRAY)
			buf := make([]byte, 0, 10)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			ar.Init(buf, 1)
			err := ar.Expect("blah", "teststring", "beheh")
			require.NoError(t, err)
		})
		t.Run("match found 2 times", func(t *testing.T) {
			TEST_ARRAY := ":teststring\r\n:teststring2\r\n"
			r := strings.NewReader(TEST_ARRAY)
			buf := make([]byte, 0, 10)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			ar.Init(buf, 2)
			err := ar.Expect("teststring")
			require.NoError(t, err)
			err = ar.Expect("teststring2")
			require.NoError(t, err)
		})
		t.Run("match not found (1 var)", func(t *testing.T) {
			TEST_ARRAY := ":teststring2\r\n"
			r := strings.NewReader(TEST_ARRAY)
			buf := make([]byte, 0, 10)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			ar.Init(buf, 1)
			err := ar.Expect("teststring")
			require.Error(t, err)
		})
		t.Run("match not found (1 var) and read next", func(t *testing.T) {
			TEST_ARRAY := ":teststring2\r\n:teststring\r\n"
			r := strings.NewReader(TEST_ARRAY)
			buf := make([]byte, 0, 10)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			ar.Init(buf, 2)
			err := ar.Expect("teststring")
			require.Error(t, err)
			var str string
			err = ar.Scan(&str)
			require.NoError(t, err)
			require.Equal(t, "teststring", str)
		})
		t.Run("match not found (3 vars)", func(t *testing.T) {
			TEST_ARRAY := ":teststring2\r\n"
			r := strings.NewReader(TEST_ARRAY)
			buf := make([]byte, 0, 10)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			ar.Init(buf, 1)
			err := ar.Expect("blah", "teststring", "beheh")
			require.Error(t, err)
		})
		t.Run("match not found (0 vars)", func(t *testing.T) {
			TEST_ARRAY := ":teststring2\r\n"
			r := strings.NewReader(TEST_ARRAY)
			buf := make([]byte, 0, 10)
			res := greddis.NewResult(buf)
			ar := greddis.NewArrayReader(buf, r, res)
			ar.Init(buf, 1)
			err := ar.Expect()
			require.Error(t, err)
		})
	})
}
