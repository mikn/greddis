package redis

import "errors"

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
