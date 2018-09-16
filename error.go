package beanstalkd

import (
	"errors"
	"fmt"
	"net"
)

var (
	ErrOutOfMemory    = errors.New("out of memory")
	ErrInternalError  = errors.New("internal error")
	ErrBadFormat      = errors.New("bad format")
	ErrUnknownCommand = errors.New("unknown command")
	ErrBuried         = errors.New("buried")
	ErrExpectedCrlf   = errors.New("expected CRLF")
	ErrJobTooBig      = errors.New("job too big")
	ErrDraining       = errors.New("draining")
	ErrDeadlineSoon   = errors.New("deadline soon")
	ErrTimedOut       = errors.New("timed out")
	ErrNotFound       = errors.New("not found")
)


var errorTable = map[string]error{

	"DEADLINE_SOON\r\n": ErrDeadlineSoon,
	"TIMED_OUT\r\n":     ErrTimedOut,
	"EXPECTED_CRLF\r\n": ErrExpectedCrlf,
	"JOB_TOO_BIG\r\n":   ErrJobTooBig,
	"DRAINING\r\n":      ErrDraining,
	"BURIED\r\n":        ErrBuried,
	"NOT_FOUND\r\n":     ErrNotFound,

	// common error
	"OUT_OF_MEMORY\r\n":   ErrOutOfMemory,
	"INTERNAL_ERROR\r\n":  ErrInternalError,
	"BAD_FORMAT\r\n":      ErrBadFormat,
	"UNKNOWN_COMMAND\r\n": ErrUnknownCommand,
}

// parse for Common Error
func parseError(str string) error {
	if v, ok := errorTable[str]; ok {
		return v
	}
	return fmt.Errorf("unknown error: %v", str)
}

//Check if it is temporary network error
func isNetTempErr(err error) bool {
	if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
		return true
	}
	return false
}


