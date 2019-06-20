package greddis

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteCommand(t *testing.T) {
	var out = &bytes.Buffer{}
	var origBuf = make([]byte, 1000)
	var cmd = &command{}
	cmd.array = &respArray{}
	cmd.array.reset(origBuf)
	cmd.add("GET")
	cmd.add("testkey")
	cmd.writeTo(out)
	require.Equal(t, "*2\r\n$3\r\nGET\r\n$7\r\ntestkey\r\n", out.String())
}
