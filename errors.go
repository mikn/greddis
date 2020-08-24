package greddis

import (
	"errors"
	"fmt"
)

// ErrRetryable is an error that can be retried
var ErrRetryable = errors.New("Temporary error, please retry!")

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

// ErrMixedTopicTypes is given when you pass in arguments of both RedisPattern and String
var ErrMixedTopicTypes = errors.New("All the topics need to be either of type string or of RedisPattern, but not of both")

// ErrWrongType is returned when the function receives an unsupported type
func ErrWrongType(v interface{}, expected string) error {
	return fmt.Errorf("Received an unsupported type of %t, expected %s", v, expected)
}

// ErrWrongToken is used internally in the Redis Reader when it checks whether the token expected and this is not the case
func ErrWrongToken(expToken byte, token byte) error {
	return fmt.Errorf("Expected token: %s but received %s", string(expToken), string(token))
}
