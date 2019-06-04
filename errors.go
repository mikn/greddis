package redis

import "errors"

var ErrNoData = errors.New("No data on connection but we did not reach end of data")
var ErrMalformedString = errors.New("Expected CRLF terminated string, but did not receive one")
var ErrWrongPrefix = errors.New("Wrong prefix on string")

var ErrConnRead = errors.New("Received error whilst reading from connection")
var ErrConnWrite = errors.New("Received error whilst writing to connection")
var ErrConnDial = errors.New("Received error whilst establishing connection")
