package grpcx

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-abhi/fileService/internal/pbwire"
)

// ClientConn is a gRPC client connection pool to one server. It is safe
// for concurrent use; HTTP/2 multiplexes calls over shared connections.
type ClientConn struct {
	base    string
	hc      *http.Client
	md      http.Header
	ua      string
	maxRecv int
}

type dialOptions struct {
	insecure bool
	tlsCfg   *tls.Config
	md       http.Header
	hc       *http.Client
	ua       string
	maxRecv  int
}

// DialOption configures Dial.
type DialOption func(*dialOptions)

// WithInsecure uses cleartext HTTP/2 (h2c).
func WithInsecure() DialOption { return func(o *dialOptions) { o.insecure = true } }

// WithTLSConfig sets the TLS configuration.
func WithTLSConfig(cfg *tls.Config) DialOption { return func(o *dialOptions) { o.tlsCfg = cfg } }

// WithMetadata adds a header sent with every call (e.g. authorization).
func WithMetadata(key, value string) DialOption {
	return func(o *dialOptions) { o.md.Add(key, value) }
}

// WithUserAgent sets the user agent.
func WithUserAgent(ua string) DialOption { return func(o *dialOptions) { o.ua = ua } }

// WithMaxResponseSize sets the largest accepted response message.
func WithMaxResponseSize(n int) DialOption { return func(o *dialOptions) { o.maxRecv = n } }

// WithHTTPClient uses a custom HTTP client; it must speak HTTP/2.
func WithHTTPClient(hc *http.Client) DialOption { return func(o *dialOptions) { o.hc = hc } }

// Dial prepares a connection to target, which is "host:port",
// "http://host:port" (cleartext) or "https://host:port" (TLS). With a bare
// host:port, TLS is used unless WithInsecure is given. Connections are
// established lazily.
func Dial(target string, opts ...DialOption) (*ClientConn, error) {
	o := dialOptions{md: http.Header{}, ua: "grpcx-go/1", maxRecv: DefaultMaxMessageSize}
	for _, opt := range opts {
		opt(&o)
	}
	scheme := "https"
	host := target
	switch {
	case strings.HasPrefix(target, "http://"):
		scheme, host = "http", strings.TrimPrefix(target, "http://")
	case strings.HasPrefix(target, "https://"):
		host = strings.TrimPrefix(target, "https://")
	case o.insecure:
		scheme = "http"
	}
	host = strings.TrimRight(host, "/")
	if host == "" {
		return nil, errors.New("grpcx: empty target")
	}
	hc := o.hc
	if hc == nil {
		var p http.Protocols
		t := &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			DialContext:         (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			TLSClientConfig:     o.tlsCfg,
			TLSHandshakeTimeout: 10 * time.Second,
			IdleConnTimeout:     5 * time.Minute,
			HTTP2: &http.HTTP2Config{
				MaxReceiveBufferPerStream:     2 << 20,
				MaxReceiveBufferPerConnection: 3 << 20,
				SendPingTimeout:               30 * time.Second,
				PingTimeout:                   15 * time.Second,
			},
		}
		if scheme == "http" {
			p.SetUnencryptedHTTP2(true)
		} else {
			p.SetHTTP2(true)
		}
		t.Protocols = &p
		hc = &http.Client{Transport: t}
	}
	return &ClientConn{base: scheme + "://" + host, hc: hc, md: o.md, ua: o.ua, maxRecv: o.maxRecv}, nil
}

// Close releases idle connections.
func (c *ClientConn) Close() error {
	c.hc.CloseIdleConnections()
	return nil
}

type mdKey struct{}

// AppendOutgoingMetadata returns a context carrying extra request headers
// for calls made with it.
func AppendOutgoingMetadata(ctx context.Context, kv ...string) context.Context {
	old, _ := ctx.Value(mdKey{}).(http.Header)
	md := old.Clone()
	if md == nil {
		md = http.Header{}
	}
	for i := 0; i+1 < len(kv); i += 2 {
		md.Add(kv[i], kv[i+1])
	}
	return context.WithValue(ctx, mdKey{}, md)
}

func (c *ClientConn) do(ctx context.Context, method string, body io.Reader, length int64) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+method, body)
	if err != nil {
		return nil, Errorf(Internal, "grpc: %v", err)
	}
	req.ContentLength = length
	h := req.Header
	for k, vs := range c.md {
		h[k] = append(h[k], vs...)
	}
	if md, ok := ctx.Value(mdKey{}).(http.Header); ok {
		for k, vs := range md {
			h[k] = append(h[k], vs...)
		}
	}
	h.Set("Content-Type", "application/grpc")
	h.Set("Te", "trailers")
	h.Set("User-Agent", c.ua)
	if dl, ok := ctx.Deadline(); ok {
		h.Set("Grpc-Timeout", encodeTimeout(time.Until(dl)))
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, FromError(ctx.Err())
		}
		return nil, Errorf(Unavailable, "grpc: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, Errorf(httpToCode(resp.StatusCode), "grpc: unexpected HTTP status %d", resp.StatusCode)
	}
	if !isGRPCContentType(resp.Header.Get("Content-Type")) {
		resp.Body.Close()
		return nil, Errorf(Unknown, "grpc: unexpected content-type %q", resp.Header.Get("Content-Type"))
	}
	return resp, nil
}

// httpToCode maps HTTP status codes per the gRPC specification.
func httpToCode(status int) Code {
	switch status {
	case 400:
		return Internal
	case 401:
		return Unauthenticated
	case 403:
		return PermissionDenied
	case 404:
		return Unimplemented
	case 429, 502, 503, 504:
		return Unavailable
	}
	return Unknown
}

func statusFrom(h http.Header) (*Status, bool) {
	v := h.Get("Grpc-Status")
	if v == "" {
		return nil, false
	}
	code, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return &Status{Code: Internal, Message: "grpc: malformed grpc-status " + v}, true
	}
	return &Status{Code: Code(code), Message: decodeMessage(h.Get("Grpc-Message"))}, true
}

// finalStatus returns the call status from trailers (or headers for
// trailers-only responses). It must be called after the body hit EOF.
func finalStatus(resp *http.Response) *Status {
	if st, ok := statusFrom(resp.Trailer); ok {
		return st
	}
	if st, ok := statusFrom(resp.Header); ok {
		return st
	}
	return &Status{Code: Internal, Message: "grpc: server closed the stream without sending trailers"}
}

func bodyError(ctx context.Context, err error) error {
	var st *Status
	if errors.As(err, &st) {
		return st
	}
	if ctx.Err() != nil {
		return FromError(ctx.Err())
	}
	return Errorf(Unavailable, "grpc: reading response: %v", err)
}

func asError(st *Status) error {
	if st.Code == OK {
		return nil
	}
	return st
}

// Invoke performs a unary call.
func (c *ClientConn) Invoke(ctx context.Context, method string, req, resp Message) error {
	body := frame(pbwire.Marshal(req))
	httpResp, err := c.do(ctx, method, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()
	fr := &frameReader{r: httpResp.Body, max: c.maxRecv, gzip: httpResp.Header.Get("Grpc-Encoding") == "gzip"}
	data, err := fr.next()
	if err == io.EOF {
		if err := asError(finalStatus(httpResp)); err != nil {
			return err
		}
		return Errorf(Internal, "grpc: no response message")
	}
	if err != nil {
		return bodyError(ctx, err)
	}
	if _, err := fr.next(); err != io.EOF {
		if err == nil {
			return Errorf(Internal, "grpc: too many response messages for a unary call")
		}
		return bodyError(ctx, err)
	}
	if err := asError(finalStatus(httpResp)); err != nil {
		return err
	}
	if err := pbwire.Unmarshal(data, resp); err != nil {
		return Errorf(Internal, "grpc: failed to unmarshal the response: %v", err)
	}
	return nil
}

// ServerStream is the client side of a server-streaming call.
type ServerStream struct {
	ctx  context.Context
	resp *http.Response
	fr   *frameReader
	done error
}

// NewServerStream starts a server-streaming call.
func (c *ClientConn) NewServerStream(ctx context.Context, method string, req Message) (*ServerStream, error) {
	body := frame(pbwire.Marshal(req))
	resp, err := c.do(ctx, method, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, err
	}
	return &ServerStream{ctx: ctx, resp: resp, fr: &frameReader{r: resp.Body, max: c.maxRecv, gzip: resp.Header.Get("Grpc-Encoding") == "gzip"}}, nil
}

// Recv reads the next message into m. It returns io.EOF when the stream
// ended successfully, or the call's error.
func (s *ServerStream) Recv(m Message) error {
	if s.done != nil {
		return s.done
	}
	data, err := s.fr.next()
	if err != nil {
		if err == io.EOF {
			s.done = asError(finalStatus(s.resp))
			if s.done == nil {
				s.done = io.EOF
			}
		} else {
			s.done = bodyError(s.ctx, err)
		}
		s.resp.Body.Close()
		return s.done
	}
	if err := pbwire.Unmarshal(data, m); err != nil {
		s.done = Errorf(Internal, "grpc: failed to unmarshal the response: %v", err)
		s.resp.Body.Close()
		return s.done
	}
	return nil
}

// Close cancels the stream if it is still open.
func (s *ServerStream) Close() error {
	if s.done == nil {
		s.done = Errorf(Canceled, "grpc: stream closed by client")
	}
	return s.resp.Body.Close()
}

// ClientStream is the client side of a client-streaming call.
type ClientStream struct {
	ctx     context.Context
	pw      *io.PipeWriter
	c       *ClientConn
	done    chan struct{}
	resp    *http.Response
	respErr error
}

// NewClientStream starts a client-streaming call.
func (c *ClientConn) NewClientStream(ctx context.Context, method string) (*ClientStream, error) {
	pr, pw := io.Pipe()
	cs := &ClientStream{ctx: ctx, pw: pw, c: c, done: make(chan struct{})}
	go func() {
		defer close(cs.done)
		cs.resp, cs.respErr = c.do(ctx, method, pr, -1)
		if cs.respErr != nil {
			pr.CloseWithError(cs.respErr)
		}
	}()
	return cs, nil
}

// Send sends a message. It returns io.EOF if the server already ended the
// call; CloseAndRecv then returns the call status.
func (cs *ClientStream) Send(m Message) error {
	if _, err := cs.pw.Write(frame(pbwire.Marshal(m))); err != nil {
		return io.EOF
	}
	return nil
}

// CloseAndRecv half-closes the stream and reads the response into m.
func (cs *ClientStream) CloseAndRecv(m Message) error {
	cs.pw.Close()
	<-cs.done
	if cs.respErr != nil {
		return cs.respErr
	}
	defer cs.resp.Body.Close()
	fr := &frameReader{r: cs.resp.Body, max: cs.c.maxRecv, gzip: cs.resp.Header.Get("Grpc-Encoding") == "gzip"}
	data, err := fr.next()
	if err == io.EOF {
		if err := asError(finalStatus(cs.resp)); err != nil {
			return err
		}
		return Errorf(Internal, "grpc: no response message")
	}
	if err != nil {
		return bodyError(cs.ctx, err)
	}
	if _, err := fr.next(); err != io.EOF {
		if err == nil {
			return Errorf(Internal, "grpc: too many response messages")
		}
		return bodyError(cs.ctx, err)
	}
	if err := asError(finalStatus(cs.resp)); err != nil {
		return err
	}
	if err := pbwire.Unmarshal(data, m); err != nil {
		return Errorf(Internal, "grpc: failed to unmarshal the response: %v", err)
	}
	return nil
}

// Abort cancels a client stream without waiting for a response.
func (cs *ClientStream) Abort(err error) {
	if err == nil {
		err = errors.New("grpcx: stream aborted")
	}
	cs.pw.CloseWithError(err)
	go func() {
		<-cs.done
		if cs.resp != nil {
			cs.resp.Body.Close()
		}
	}()
}
