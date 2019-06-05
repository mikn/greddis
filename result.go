package redis

import (
	"fmt"
	"io"
	"strconv"
)

// Scanner is an interface that allows you to implement custom logic with direct control over
// the byte buffer being used by the connection. Do not modify it and do not return it.
// As soon as your Scan function exits, the connection will start using the buffer again.
type Scanner interface {
	Scan(dst interface{}) error
}

// Result is what is returned from the Redis client if a response is expected
type Result interface {
	// Scan is called with a pointer to a value of a concrete type that you want to cast to,
	// alternatively with an io.Writer or an implementation of the Scanner interface which
	// accepts a []byte type input.
	Scan(dst interface{}) error
}

type result struct {
	value []byte
	pool  internalPool
	conn  *conn
}

// Scan on result allows us to read with zero-copy into Scanner and io.Writer
// implementations as well as into string, int, other []byte slices (but with copy).
// However since Redis always returns a []byte slice, if you were to implement a scanner
// you would need to have a value switch that takes []byte slice and does something
// productive with it. This implementation can be simpler and forego the source
// switch since it is custom made for Redis.
func (r *result) Scan(dst interface{}) error {
	switch d := dst.(type) {
	case *string:
		*d = string(r.value)
		r.pool.Put(r.conn)
		return nil
	case *int:
		var val, err = strconv.ParseInt(string(r.value), 10, 64)
		r.pool.Put(r.conn)
		if err != nil {
			return err
		}
		*d = int(val)
		return nil
	case *[]byte:
		copy(*d, r.value)
		r.pool.Put(r.conn)
		return nil
	case io.Writer:
		var _, err = d.Write(r.value)
		r.pool.Put(r.conn)
		if err != nil {
			return err
		}
		return nil
	case Scanner:
		var err = d.Scan(r.value)
		r.pool.Put(r.conn)
		return err
	default:
		r.pool.Put(r.conn)
		return fmt.Errorf("dst is not of any supported type. Is of type %s", d)
	}
}
