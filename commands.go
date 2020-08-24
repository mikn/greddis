package greddis

import (
	"context"
	"fmt"
)

func ping(ctx context.Context, conn *conn) (err error) {
	conn.arrw.Init(1).Add("PING")
	err = conn.arrw.Flush()
	if err != nil {
		return err
	}
	err = conn.r.Next(ScanSimpleString)
	if err != nil {
		return err
	}
	if conn.r.String() != "PONG" {
		return fmt.Errorf("Got wrong reply! Expected 'PONG', received '%s'", conn.r.String())
	}
	return nil
}
