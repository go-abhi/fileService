package grpcx

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// DefaultMaxMessageSize is the default limit for received messages.
const DefaultMaxMessageSize = 8 << 20

// frame prefixes an encoded message with the gRPC length-prefix header.
func frame(msg []byte) []byte {
	out := make([]byte, 5+len(msg))
	binary.BigEndian.PutUint32(out[1:5], uint32(len(msg)))
	copy(out[5:], msg)
	return out
}

type frameReader struct {
	r    io.Reader
	max  int
	gzip bool
}

// next returns the next message, or io.EOF at a clean end of stream.
func (fr *frameReader) next() ([]byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(fr.r, hdr[:]); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		if err == io.ErrUnexpectedEOF {
			return nil, Errorf(Internal, "grpc: truncated message header")
		}
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	if int64(n) > int64(fr.max) {
		return nil, Errorf(ResourceExhausted, "grpc: received message larger than max (%d vs. %d)", n, fr.max)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(fr.r, buf); err != nil {
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			return nil, Errorf(Internal, "grpc: truncated message")
		}
		return nil, err
	}
	switch hdr[0] {
	case 0:
		return buf, nil
	case 1:
		if !fr.gzip {
			return nil, Errorf(Internal, "grpc: compressed flag set without grpc-encoding")
		}
		zr, err := gzip.NewReader(bytes.NewReader(buf))
		if err != nil {
			return nil, Errorf(Internal, "grpc: invalid gzip message: %v", err)
		}
		out, err := io.ReadAll(io.LimitReader(zr, int64(fr.max)+1))
		if err != nil {
			return nil, Errorf(Internal, "grpc: invalid gzip message: %v", err)
		}
		if len(out) > fr.max {
			return nil, Errorf(ResourceExhausted, "grpc: decompressed message larger than max (%d)", fr.max)
		}
		return out, nil
	default:
		return nil, Errorf(Internal, "grpc: invalid compression flag %d", hdr[0])
	}
}

func isGRPCContentType(ct string) bool {
	ct = strings.ToLower(strings.TrimSpace(ct))
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	return ct == "application/grpc" || ct == "application/grpc+proto"
}

// encodeTimeout formats d as a grpc-timeout header value (at most 8 digits).
func encodeTimeout(d time.Duration) string {
	if d <= 0 {
		return "1n"
	}
	units := []struct {
		unit string
		size time.Duration
	}{{"n", time.Nanosecond}, {"u", time.Microsecond}, {"m", time.Millisecond}, {"S", time.Second}, {"M", time.Minute}, {"H", time.Hour}}
	for _, u := range units {
		v := (d + u.size - 1) / u.size
		if v < 1e8 {
			return strconv.FormatInt(int64(v), 10) + u.unit
		}
	}
	return "99999999H"
}

func parseTimeout(s string) (time.Duration, error) {
	if len(s) < 2 || len(s) > 9 {
		return 0, errors.New("malformed grpc-timeout")
	}
	v, err := strconv.ParseInt(s[:len(s)-1], 10, 64)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("malformed grpc-timeout %q", s)
	}
	var unit time.Duration
	switch s[len(s)-1] {
	case 'H':
		unit = time.Hour
	case 'M':
		unit = time.Minute
	case 'S':
		unit = time.Second
	case 'm':
		unit = time.Millisecond
	case 'u':
		unit = time.Microsecond
	case 'n':
		unit = time.Nanosecond
	default:
		return 0, fmt.Errorf("malformed grpc-timeout unit %q", s)
	}
	const maxHours = int64(1<<63-1) / int64(time.Hour)
	if unit == time.Hour && v > maxHours {
		return 1<<63 - 1, nil
	}
	return time.Duration(v) * unit, nil
}
