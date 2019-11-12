package greddis

import (
	"bytes"
	"context"
	"database/sql/driver"
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

func publish(ctx context.Context, conn *conn, topic string, value driver.Value) (i int, err error) {
	err = conn.cmd.start(conn.buf, 3).add("PUBLISH").add(topic).addUnsafe(value)
	if err != nil {
		conn.cmd.reset(conn.conn)
		return
	}
	conn.buf, err = conn.cmd.flush()
	if err != nil {
		conn.cmd.reset(conn.conn)
		return
	}
	i, err = readInteger(conn.conn, conn.buf)
	return
}

func unsubscribe(ctx context.Context, conn *conn, topics ...interface{}) error {
	c := conn.cmd
	var subType int
	for _, topic := range topics {
		switch d := topic.(type) {
		case RedisPattern:
			if subType == 0 {
				subType = subPattern
				c = c.start(c.buf, len(topics)+1).add("PUNSUBSCRIBE")
			} else if subType != 0 && subType != subPattern {
				return ErrMixedTopicTypes
			}
			c.add(string(d))
		case string:
			if subType == 0 {
				subType = subTopic
				c = c.start(c.buf, len(topics)+1).add("UNSUBSCRIBE")
			} else if subType != 0 && subType != subTopic {
				return ErrMixedTopicTypes
			}
			c.add(d)
		default:
			return fmt.Errorf("Wrong type! Expected RedisPattern or string, but received: %T", d)
		}
	}
	var err error
	c.buf, err = c.flush()
	if err != nil {
		return err
	}
	return nil
}
