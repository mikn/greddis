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

// Result is what is returned from the Redis client if a single response is expected
type Result struct {
	r      *Reader
	finish func()
}

func NewResult(r *Reader) *Result {
	return &Result{
		r:      r,
		finish: func() {},
	}
}

// Scan on result allows us to read with zero-copy into Scanner and io.Writer
// implementations as well as into string, int, other []byte slices (but with copy).
// However since Redis always returns a []byte slice, if you were to implement a scanner
// you would need to have a value switch that takes []byte slice and does something
// productive with it. This implementation can be simpler and forego the source
// switch since it is custom made for Redis.
func (r *Result) Scan(dst interface{}) (err error) {
	err = scan(r.r, dst)
	r.finish()
	return err
}

func scan(r *Reader, dst interface{}) (err error) {
	switch d := dst.(type) {
	case *string:
		*d = string(r.Bytes())
	case *int:
		var val int
		val, err = strconv.Atoi(r.String())
		if err == nil {
			*d = int(val)
		}
	case *[]byte:
		b := *d
		*d = b[:r.Len()]
		copy(*d, r.Bytes())
	case io.Writer:
		_, err = r.WriteTo(d)
	case Scanner:
		err = d.Scan(r.Bytes())
	default:
		err = fmt.Errorf("dst is not of any supported type. Is of type %T", d)
	}
	return err
}
