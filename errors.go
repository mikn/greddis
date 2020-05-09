package greddis

import (
	"errors"
	"fmt"
)

// ErrTimeout error when waiting for data from Redis
var ErrTimeout = errors.New("Timed out whilst waiting for data")

// ErrMalformedString received expected length, but it isn't terminated properly
var ErrMalformedString = errors.New("Expected CRLF terminated string, but did not receive one")

// ErrWrongPrefix returns when we expected a certain type prefix, but received another
var ErrWrongPrefix = errors.New("Wrong prefix on string")

// ErrConnRead received error when reading from connection
var ErrConnRead = errors.New("Received error whilst reading from connection")

// ErrConnWrite error when writing to connection
var ErrConnWrite = errors.New("Received error whilst writing to connection")

// ErrConnDial error during dial
var ErrConnDial = errors.New("Received error whilst establishing connection")

// ErrOptsDialAndURL cannot combine both dial and URL for pool options
var ErrOptsDialAndURL = errors.New("Both Dial and URL is set, can only set one")

// ErrNoMoreRows is returned when an ArrayResult does not contain any more entries when Next() is called
var ErrNoMoreRows = errors.New("No more rows")

// ErrNoData is returned when a Read() call read no data
var ErrNoData = errors.New("No data was read")

// ErrMixedTopicTypes is given when you pass in arguments of both RedisPattern and String
var ErrMixedTopicTypes = errors.New("All the topics need to be either of type string or of RedisPattern, but not of both")

// ErrWrongType is returned when the function receives an unsupported type
func ErrWrongType(v interface{}, expected string) error {
	return fmt.Errorf("Received an unsupported type of %t, expected %s", v, expected)
}

type ErrRetry struct {
	Err error
}

func (e ErrRetry) Unwrap() error {
	return e.Err
}

func (e ErrRetry) Error() string {
	return "Temporary failure, please retry"
}

type errProxy struct {
	Err     error
	proxied error
}

func (e *errProxy) Unwrap() error {
	return e.Err
}

func (e *errProxy) Error() string {
	return e.proxied.Error()
}
