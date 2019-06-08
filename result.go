//go:generate mockgen -source result.go -destination ./mocks/mock_greddis/mock_result.go
package greddis

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
type Result struct {
	value  []byte
	finish func()
}

func NewResult(buf []byte) *Result {
	return &Result{
		value:  buf,
		finish: func() {},
	}
}

// Scan on result allows us to read with zero-copy into Scanner and io.Writer
// implementations as well as into string, int, other []byte slices (but with copy).
// However since Redis always returns a []byte slice, if you were to implement a scanner
// you would need to have a value switch that takes []byte slice and does something
// productive with it. This implementation can be simpler and forego the source
// switch since it is custom made for Redis.
func (r *Result) Scan(dst interface{}) error {
	switch d := dst.(type) {
	case *string:
		*d = string(r.value)
		r.finish()
		return nil
	case *int:
		var val, err = strconv.Atoi(string(r.value))
		r.finish()
		if err != nil {
			return err
		}
		*d = int(val)
		return nil
	case *[]byte:
		copy(*d, r.value)
		r.finish()
		return nil
	case io.Writer:
		var _, err = d.Write(r.value)
		r.finish()
		if err != nil {
			return err
		}
		return nil
	case Scanner:
		var err = d.Scan(r.value)
		r.finish()
		return err
	default:
		r.finish()
		return fmt.Errorf("dst is not of any supported type. Is of type %s", d)
	}
}
