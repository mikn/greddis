package greddis

import (
	"bytes"
	"context"
	"fmt"
)

func ping(ctx context.Context, conn *conn) (err error) {
	conn.cmd.start(conn.buf, 1).add("PING")
	conn.buf, err = conn.cmd.flush()
	reply, err := readSimpleString(conn.conn, conn.buf)
	if err != nil {
		return err
	}
	if bytes.Compare(reply, []byte("PONG")) != 0 {
		return fmt.Errorf("Got wrong reply! Expected 'PONG', received '%s'", reply)
	}
	return nil
}
