// Package grpcx implements the gRPC protocol (HTTP/2, length-prefixed
// protobuf messages, grpc-status trailers) on top of Go's net/http, for
// unary, server-streaming and client-streaming calls. It interoperates with
// standard gRPC implementations such as grpc-go and grpcurl.
package grpcx

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Code is a gRPC status code.
type Code uint32

// Status codes, as defined by the gRPC specification.
const (
	OK                 Code = 0
	Canceled           Code = 1
	Unknown            Code = 2
	InvalidArgument    Code = 3
	DeadlineExceeded   Code = 4
	NotFound           Code = 5
	AlreadyExists      Code = 6
	PermissionDenied   Code = 7
	ResourceExhausted  Code = 8
	FailedPrecondition Code = 9
	Aborted            Code = 10
	OutOfRange         Code = 11
	Unimplemented      Code = 12
	Internal           Code = 13
	Unavailable        Code = 14
	DataLoss           Code = 15
	Unauthenticated    Code = 16
)

var codeNames = [...]string{
	"OK", "Canceled", "Unknown", "InvalidArgument", "DeadlineExceeded", "NotFound",
	"AlreadyExists", "PermissionDenied", "ResourceExhausted", "FailedPrecondition",
	"Aborted", "OutOfRange", "Unimplemented", "Internal", "Unavailable", "DataLoss",
	"Unauthenticated",
}

func (c Code) String() string {
	if int(c) < len(codeNames) {
		return codeNames[c]
	}
	return "Code(" + strconv.Itoa(int(c)) + ")"
}

// Status is a gRPC status. A non-OK *Status is used as an error.
type Status struct {
	Code    Code
	Message string
}

func (s *Status) Error() string {
	return "rpc error: code = " + s.Code.String() + " desc = " + s.Message
}

// Errorf returns a *Status error.
func Errorf(c Code, format string, args ...any) error {
	return &Status{Code: c, Message: fmt.Sprintf(format, args...)}
}

// FromError converts err to a Status. Context errors map to Canceled and
// DeadlineExceeded; other errors map to Unknown.
func FromError(err error) *Status {
	if err == nil {
		return &Status{Code: OK}
	}
	var s *Status
	if errors.As(err, &s) {
		return s
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return &Status{Code: DeadlineExceeded, Message: err.Error()}
	case errors.Is(err, context.Canceled):
		return &Status{Code: Canceled, Message: err.Error()}
	}
	return &Status{Code: Unknown, Message: err.Error()}
}

// CodeOf returns the status code of err (OK for nil).
func CodeOf(err error) Code { return FromError(err).Code }

// encodeMessage percent-encodes a grpc-message value: bytes outside the
// printable ASCII range and '%' are escaped.
func encodeMessage(msg string) string {
	var b strings.Builder
	for i := 0; i < len(msg); i++ {
		c := msg[i]
		if c >= 0x20 && c <= 0x7e && c != '%' {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}

func decodeMessage(s string) string {
	if !strings.Contains(s, "%") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+3], 16, 8); err == nil {
				b.WriteByte(byte(v))
				i += 2
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
