package grpcx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/go-abhi/fileService/internal/pbwire"
)

// Message is a protobuf message.
type Message = pbwire.Message

// UnaryHandler handles a unary call. decode reads the request.
type UnaryHandler func(ctx context.Context, decode func(Message) error) (Message, error)

// ServerStreamHandler handles a server-streaming call.
type ServerStreamHandler func(ctx context.Context, decode func(Message) error, send func(Message) error) error

// ClientStreamHandler handles a client-streaming call. recv returns io.EOF
// after the last request message.
type ClientStreamHandler func(ctx context.Context, recv func(Message) error) (Message, error)

// CallInfo describes an incoming call.
type CallInfo struct {
	// FullMethod is "/package.Service/Method".
	FullMethod string
	// Header holds the request metadata (HTTP/2 headers).
	Header     http.Header
	RemoteAddr string
}

// Interceptor runs before every handler. It may enrich the context (e.g.
// with an authenticated principal) or reject the call by returning an error.
type Interceptor func(ctx context.Context, info *CallInfo) (context.Context, error)

type kind int

const (
	unary kind = iota
	serverStream
	clientStream
)

type method struct {
	kind    kind
	unary   UnaryHandler
	sstream ServerStreamHandler
	cstream ClientStreamHandler
}

// Server is an http.Handler serving gRPC. Serve it over HTTP/2: TLS, or
// cleartext (h2c) with http.Server.Protocols.SetUnencryptedHTTP2(true).
type Server struct {
	methods      map[string]method
	interceptors []Interceptor
	maxRecv      int
	log          *slog.Logger
}

// ServerOption configures a Server.
type ServerOption func(*Server)

// WithInterceptor adds an interceptor; interceptors run in order.
func WithInterceptor(ic Interceptor) ServerOption {
	return func(s *Server) { s.interceptors = append(s.interceptors, ic) }
}

// WithMaxRecvMsgSize sets the largest accepted request message.
func WithMaxRecvMsgSize(n int) ServerOption { return func(s *Server) { s.maxRecv = n } }

// WithLogger enables per-call access logs.
func WithLogger(l *slog.Logger) ServerOption { return func(s *Server) { s.log = l } }

// NewServer returns a Server with no methods.
func NewServer(opts ...ServerOption) *Server {
	s := &Server{methods: map[string]method{}, maxRecv: DefaultMaxMessageSize}
	for _, o := range opts {
		o(s)
	}
	return s
}

// HandleUnary registers a unary method.
func (s *Server) HandleUnary(fullMethod string, h UnaryHandler) {
	s.methods[fullMethod] = method{kind: unary, unary: h}
}

// HandleServerStream registers a server-streaming method.
func (s *Server) HandleServerStream(fullMethod string, h ServerStreamHandler) {
	s.methods[fullMethod] = method{kind: serverStream, sstream: h}
}

// HandleClientStream registers a client-streaming method.
func (s *Server) HandleClientStream(fullMethod string, h ClientStreamHandler) {
	s.methods[fullMethod] = method{kind: clientStream, cstream: h}
}

type callInfoKey struct{}

// CallInfoFrom returns the CallInfo of the current call.
func CallInfoFrom(ctx context.Context) (*CallInfo, bool) {
	ci, ok := ctx.Value(callInfoKey{}).(*CallInfo)
	return ci, ok
}

type responseWriter struct {
	w           http.ResponseWriter
	wroteHeader bool
}

func (rw *responseWriter) writeHeader() {
	if !rw.wroteHeader {
		rw.wroteHeader = true
		rw.w.WriteHeader(http.StatusOK)
	}
}

func (rw *responseWriter) writeMessage(m Message) error {
	rw.writeHeader()
	if _, err := rw.w.Write(frame(pbwire.Marshal(m))); err != nil {
		return Errorf(Unavailable, "grpc: writing response: %v", err)
	}
	if err := http.NewResponseController(rw.w).Flush(); err != nil {
		return Errorf(Unavailable, "grpc: flushing response: %v", err)
	}
	return nil
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "gRPC requires POST", http.StatusMethodNotAllowed)
		return
	}
	if !isGRPCContentType(r.Header.Get("Content-Type")) {
		http.Error(w, "unsupported content type (expected application/grpc)", http.StatusUnsupportedMediaType)
		return
	}
	if r.ProtoMajor != 2 {
		http.Error(w, "gRPC requires HTTP/2", http.StatusHTTPVersionNotSupported)
		return
	}
	start := time.Now()
	h := w.Header()
	h.Set("Content-Type", "application/grpc")
	h.Set("Grpc-Accept-Encoding", "gzip")
	rw := &responseWriter{w: w}

	st := s.serve(rw, r)

	rw.writeHeader()
	h.Set(http.TrailerPrefix+"Grpc-Status", strconv.Itoa(int(st.Code)))
	if st.Message != "" {
		h.Set(http.TrailerPrefix+"Grpc-Message", encodeMessage(st.Message))
	}
	if s.log != nil {
		level := slog.LevelInfo
		switch st.Code {
		case Internal, Unknown, DataLoss:
			level = slog.LevelError
		}
		attrs := []any{"method", r.URL.Path, "code", st.Code.String(), "duration", time.Since(start).Round(time.Microsecond), "peer", r.RemoteAddr}
		if st.Code != OK {
			attrs = append(attrs, "error", st.Message)
		}
		s.log.Log(r.Context(), level, "grpc call", attrs...)
	}
}

func (s *Server) serve(rw *responseWriter, r *http.Request) (st *Status) {
	defer func() {
		if p := recover(); p != nil {
			if s.log != nil {
				s.log.Error("grpc handler panic", "method", r.URL.Path, "panic", fmt.Sprint(p), "stack", string(debug.Stack()))
			}
			st = &Status{Code: Internal, Message: "internal error"}
		}
	}()
	m, ok := s.methods[r.URL.Path]
	if !ok {
		return &Status{Code: Unimplemented, Message: "unknown method " + r.URL.Path}
	}
	enc := r.Header.Get("Grpc-Encoding")
	if enc != "" && enc != "identity" && enc != "gzip" {
		rw.w.Header().Set("Grpc-Accept-Encoding", "gzip")
		return &Status{Code: Unimplemented, Message: "grpc: unsupported compression " + enc}
	}
	ctx := r.Context()
	if t := r.Header.Get("Grpc-Timeout"); t != "" {
		d, err := parseTimeout(t)
		if err != nil {
			return &Status{Code: Internal, Message: err.Error()}
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d)
		defer cancel()
	}
	info := &CallInfo{FullMethod: r.URL.Path, Header: r.Header, RemoteAddr: r.RemoteAddr}
	ctx = context.WithValue(ctx, callInfoKey{}, info)
	for _, ic := range s.interceptors {
		var err error
		if ctx, err = ic(ctx, info); err != nil {
			return FromError(err)
		}
	}

	fr := &frameReader{r: r.Body, max: s.maxRecv, gzip: enc == "gzip"}
	read := func(msg Message) error {
		data, err := fr.next()
		if err != nil {
			if err == io.EOF {
				return io.EOF
			}
			return readError(ctx, err)
		}
		if err := pbwire.Unmarshal(data, msg); err != nil {
			return Errorf(Internal, "grpc: failed to unmarshal the received message: %v", err)
		}
		return nil
	}
	decodeOne := func(msg Message) error {
		if err := read(msg); err != nil {
			if err == io.EOF {
				return Errorf(Internal, "grpc: missing request message")
			}
			return err
		}
		return nil
	}

	switch m.kind {
	case unary:
		resp, err := m.unary(ctx, decodeOne)
		if err != nil {
			return FromError(err)
		}
		if err := rw.writeMessage(resp); err != nil {
			return FromError(err)
		}
	case serverStream:
		if err := m.sstream(ctx, decodeOne, rw.writeMessage); err != nil {
			return FromError(err)
		}
	case clientStream:
		resp, err := m.cstream(ctx, read)
		if err != nil {
			return FromError(err)
		}
		if err := rw.writeMessage(resp); err != nil {
			return FromError(err)
		}
	}
	return &Status{Code: OK}
}

// readError maps a request body read error (usually a client reset).
func readError(ctx context.Context, err error) error {
	var st *Status
	if errors.As(err, &st) {
		return st
	}
	if ctx.Err() != nil {
		return FromError(ctx.Err())
	}
	return Errorf(Canceled, "grpc: reading request: %v", err)
}

// Methods returns the registered method names (for logging / reflection).
func (s *Server) Methods() []string {
	out := make([]string, 0, len(s.methods))
	for k := range s.methods {
		out = append(out, k)
	}
	return out
}

// IsGRPCRequest reports whether r is a gRPC request, to route it on a
// server that also serves other HTTP handlers.
func IsGRPCRequest(r *http.Request) bool {
	return r.ProtoMajor == 2 && strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/grpc")
}
