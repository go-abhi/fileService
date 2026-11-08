package grpcx

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-abhi/fileService/internal/pbwire"
)

type testMsg struct {
	Text string
	Data []byte
	N    int64
}

func (m *testMsg) Reset() { *m = testMsg{} }
func (m *testMsg) AppendProto(b []byte) []byte {
	b = pbwire.String(b, 1, m.Text)
	b = pbwire.BytesField(b, 2, m.Data)
	return pbwire.Int64(b, 3, m.N)
}
func (m *testMsg) MergeProto(b []byte) error {
	return pbwire.Parse(b, func(f pbwire.Field) error {
		switch f.Num {
		case 1:
			return f.StringTo(&m.Text)
		case 2:
			return f.BytesTo(&m.Data)
		case 3:
			return f.Int64To(&m.N)
		}
		return nil
	})
}

type ctxKey struct{}

func startServer(t *testing.T, s *Server) string {
	t.Helper()
	var p http.Protocols
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	srv := &http.Server{Handler: s, Protocols: &p}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return ln.Addr().String()
}

var serverCanceled atomic.Bool

func newTestServer(t *testing.T) (*ClientConn, string) {
	s := NewServer(WithMaxRecvMsgSize(1<<20), WithInterceptor(func(ctx context.Context, info *CallInfo) (context.Context, error) {
		if strings.HasPrefix(info.FullMethod, "/grpc.health.v1.") {
			return ctx, nil
		}
		if info.Header.Get("Authorization") != "Bearer good" {
			return ctx, Errorf(Unauthenticated, "missing or invalid API key")
		}
		return context.WithValue(ctx, ctxKey{}, "alice"), nil
	}))
	s.HandleUnary("/test.Svc/Echo", func(ctx context.Context, decode func(Message) error) (Message, error) {
		var req testMsg
		if err := decode(&req); err != nil {
			return nil, err
		}
		switch req.Text {
		case "fail":
			return nil, Errorf(FailedPrecondition, "nope: ünïcode 100%% sure\n")
		case "panic":
			panic("boom")
		case "slow":
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(5 * time.Second):
			}
		case "plain-error":
			return nil, errors.New("something broke")
		}
		user, _ := ctx.Value(ctxKey{}).(string)
		return &testMsg{Text: req.Text + "/" + user, Data: req.Data, N: int64(len(req.Data))}, nil
	})
	s.HandleServerStream("/test.Svc/Count", func(ctx context.Context, decode func(Message) error, send func(Message) error) error {
		var req testMsg
		if err := decode(&req); err != nil {
			return err
		}
		for i := int64(1); i <= req.N; i++ {
			if req.Text == "forever" {
				select {
				case <-ctx.Done():
					serverCanceled.Store(true)
					return ctx.Err()
				case <-time.After(10 * time.Millisecond):
				}
			}
			if err := send(&testMsg{N: i, Data: bytes.Repeat([]byte{byte(i)}, 1000)}); err != nil {
				return err
			}
			if req.Text == "fail-after-3" && i == 3 {
				return Errorf(DataLoss, "lost it")
			}
		}
		return nil
	})
	s.HandleClientStream("/test.Svc/Sum", func(ctx context.Context, recv func(Message) error) (Message, error) {
		var total int64
		var n int64
		for {
			var m testMsg
			err := recv(&m)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			if m.Text == "reject" {
				return nil, Errorf(InvalidArgument, "rejected at message %d", n)
			}
			n++
			total += int64(len(m.Data))
		}
		return &testMsg{N: total, Text: fmt.Sprint(n)}, nil
	})
	RegisterHealth(s, func(ctx context.Context, svc string) int32 {
		if svc == "" || svc == "test.Svc" {
			return HealthServing
		}
		return HealthServiceUnknown
	})
	addr := startServer(t, s)
	cc, err := Dial("http://"+addr, WithMetadata("Authorization", "Bearer good"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return cc, addr
}

func TestUnary(t *testing.T) {
	cc, addr := newTestServer(t)
	ctx := context.Background()
	var resp testMsg
	if err := cc.Invoke(ctx, "/test.Svc/Echo", &testMsg{Text: "hi", Data: []byte("abc")}, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Text != "hi/alice" || resp.N != 3 || string(resp.Data) != "abc" {
		t.Fatalf("resp = %+v", resp)
	}

	err := cc.Invoke(ctx, "/test.Svc/Echo", &testMsg{Text: "fail"}, &resp)
	if st := FromError(err); st.Code != FailedPrecondition || st.Message != "nope: ünïcode 100% sure\n" {
		t.Fatalf("err = %#v", err)
	}
	if c := CodeOf(cc.Invoke(ctx, "/test.Svc/Echo", &testMsg{Text: "panic"}, &resp)); c != Internal {
		t.Fatalf("panic code = %v", c)
	}
	if c := CodeOf(cc.Invoke(ctx, "/test.Svc/Echo", &testMsg{Text: "plain-error"}, &resp)); c != Unknown {
		t.Fatalf("plain error code = %v", c)
	}
	if c := CodeOf(cc.Invoke(ctx, "/test.Svc/Nope", &testMsg{}, &resp)); c != Unimplemented {
		t.Fatalf("unknown method code = %v", c)
	}

	// Interceptor rejection (trailers-only style response).
	bad, _ := Dial("http://"+addr, WithMetadata("Authorization", "Bearer bad"))
	if c := CodeOf(bad.Invoke(ctx, "/test.Svc/Echo", &testMsg{Text: "x"}, &resp)); c != Unauthenticated {
		t.Fatalf("auth code = %v", c)
	}

	// Per-call metadata from context.
	anon, _ := Dial(addr, WithInsecure())
	if err := anon.Invoke(AppendOutgoingMetadata(ctx, "Authorization", "Bearer good"), "/test.Svc/Echo", &testMsg{Text: "ctx"}, &resp); err != nil || resp.Text != "ctx/alice" {
		t.Fatalf("ctx metadata: %v %+v", err, resp)
	}
}

func TestDeadlinePropagates(t *testing.T) {
	cc, _ := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := cc.Invoke(ctx, "/test.Svc/Echo", &testMsg{Text: "slow"}, &testMsg{})
	if c := CodeOf(err); c != DeadlineExceeded {
		t.Fatalf("code = %v (%v)", c, err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("deadline not enforced")
	}
}

func TestServerStream(t *testing.T) {
	cc, _ := newTestServer(t)
	ctx := context.Background()
	st, err := cc.NewServerStream(ctx, "/test.Svc/Count", &testMsg{N: 50})
	if err != nil {
		t.Fatal(err)
	}
	var got int64
	for {
		var m testMsg
		err := st.Recv(&m)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got++
		if m.N != got || len(m.Data) != 1000 {
			t.Fatalf("message %d = %+v", got, m.N)
		}
	}
	if got != 50 {
		t.Fatalf("got %d messages", got)
	}

	st, _ = cc.NewServerStream(ctx, "/test.Svc/Count", &testMsg{N: 10, Text: "fail-after-3"})
	var n int
	for {
		var m testMsg
		if err = st.Recv(&m); err != nil {
			break
		}
		n++
	}
	if n != 3 || CodeOf(err) != DataLoss {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestServerStreamCancel(t *testing.T) {
	cc, _ := newTestServer(t)
	serverCanceled.Store(false)
	ctx, cancel := context.WithCancel(context.Background())
	st, err := cc.NewServerStream(ctx, "/test.Svc/Count", &testMsg{N: 1000, Text: "forever"})
	if err != nil {
		t.Fatal(err)
	}
	var m testMsg
	if err := st.Recv(&m); err != nil {
		t.Fatal(err)
	}
	cancel()
	for err == nil {
		err = st.Recv(&m)
	}
	if CodeOf(err) != Canceled {
		t.Fatalf("err = %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !serverCanceled.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !serverCanceled.Load() {
		t.Fatal("server handler did not observe cancellation")
	}
}

func TestClientStream(t *testing.T) {
	cc, _ := newTestServer(t)
	ctx := context.Background()
	cs, err := cc.NewClientStream(ctx, "/test.Svc/Sum")
	if err != nil {
		t.Fatal(err)
	}
	chunk := bytes.Repeat([]byte("x"), 256<<10)
	for i := 0; i < 40; i++ { // 10 MiB total, well above flow-control windows
		if err := cs.Send(&testMsg{Data: chunk}); err != nil {
			t.Fatal(err)
		}
	}
	var resp testMsg
	if err := cs.CloseAndRecv(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.N != 40*256<<10 || resp.Text != "40" {
		t.Fatalf("resp = %+v", resp)
	}

	// The server rejects early; the client must get the status, not hang.
	cs, _ = cc.NewClientStream(ctx, "/test.Svc/Sum")
	cs.Send(&testMsg{Data: chunk})
	cs.Send(&testMsg{Text: "reject"})
	for i := 0; i < 100; i++ {
		if err := cs.Send(&testMsg{Data: chunk}); err != nil {
			if err != io.EOF {
				t.Fatalf("send err = %v", err)
			}
			break
		}
	}
	err = cs.CloseAndRecv(&resp)
	if st := FromError(err); st.Code != InvalidArgument || !strings.Contains(st.Message, "rejected at message 1") {
		t.Fatalf("err = %v", err)
	}
}

func TestMaxMessageSize(t *testing.T) {
	cc, _ := newTestServer(t)
	err := cc.Invoke(context.Background(), "/test.Svc/Echo", &testMsg{Data: make([]byte, 2<<20)}, &testMsg{})
	if c := CodeOf(err); c != ResourceExhausted {
		t.Fatalf("code = %v (%v)", c, err)
	}
}

func TestHealth(t *testing.T) {
	cc, _ := newTestServer(t)
	var resp HealthCheckResponse
	if err := cc.Invoke(context.Background(), HealthCheckMethod, &HealthCheckRequest{}, &resp); err != nil || resp.Status != HealthServing {
		t.Fatalf("health: %v %+v", err, resp)
	}
	if c := CodeOf(cc.Invoke(context.Background(), HealthCheckMethod, &HealthCheckRequest{Service: "x"}, &resp)); c != NotFound {
		t.Fatalf("unknown service code = %v", c)
	}
	ctx, cancel := context.WithCancel(context.Background())
	st, err := cc.NewServerStream(ctx, HealthWatchMethod, &HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Recv(&resp); err != nil || resp.Status != HealthServing {
		t.Fatalf("watch: %v %+v", err, resp)
	}
	cancel()
	st.Close()
}

// rawCall sends a hand-built request so we can exercise protocol details
// (compression, malformed frames) that our client never produces.
func rawCall(t *testing.T, addr string, hdr http.Header, body []byte) (*http.Response, []byte) {
	t.Helper()
	var p http.Protocols
	p.SetUnencryptedHTTP2(true)
	hc := &http.Client{Transport: &http.Transport{Protocols: &p}}
	req, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/test.Svc/Echo", bytes.NewReader(body))
	req.Header = hdr
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp, out
}

func TestGzipRequestAndProtocolErrors(t *testing.T) {
	_, addr := newTestServer(t)
	var zbuf bytes.Buffer
	zw := gzip.NewWriter(&zbuf)
	zw.Write(pbwire.Marshal(&testMsg{Text: "zipped"}))
	zw.Close()
	f := frame(zbuf.Bytes())
	f[0] = 1
	hdr := http.Header{"Content-Type": {"application/grpc+proto"}, "Te": {"trailers"}, "Grpc-Encoding": {"gzip"}, "Authorization": {"Bearer good"}}
	resp, body := rawCall(t, addr, hdr, f)
	if resp.Trailer.Get("Grpc-Status") != "0" {
		t.Fatalf("status %q %q", resp.Trailer.Get("Grpc-Status"), resp.Trailer.Get("Grpc-Message"))
	}
	var m testMsg
	if err := pbwire.Unmarshal(body[5:], &m); err != nil || m.Text != "zipped/alice" {
		t.Fatalf("decoded %+v %v", m, err)
	}

	hdr.Set("Grpc-Encoding", "snappy")
	resp, _ = rawCall(t, addr, hdr, f)
	if resp.Trailer.Get("Grpc-Status") != "12" {
		t.Fatalf("unsupported encoding status %q", resp.Trailer.Get("Grpc-Status"))
	}

	hdr.Del("Grpc-Encoding")
	resp, _ = rawCall(t, addr, hdr, []byte{0, 0, 0})
	if resp.Trailer.Get("Grpc-Status") != "13" {
		t.Fatalf("truncated frame status %q", resp.Trailer.Get("Grpc-Status"))
	}

	// HTTP/1.1 and non-gRPC content types are rejected at the HTTP layer.
	r1, err := http.Post("http://"+addr+"/test.Svc/Echo", "application/grpc", bytes.NewReader(f))
	if err != nil {
		t.Fatal(err)
	}
	r1.Body.Close()
	if r1.StatusCode != http.StatusHTTPVersionNotSupported {
		t.Fatalf("HTTP/1 status = %d", r1.StatusCode)
	}
	hdr.Set("Content-Type", "application/json")
	resp, _ = rawCall(t, addr, hdr, f)
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("json status = %d", resp.StatusCode)
	}
}

func TestTimeoutEncoding(t *testing.T) {
	for _, d := range []time.Duration{time.Nanosecond, 99 * time.Millisecond, 3 * time.Second, 2 * time.Hour, 10000 * time.Hour} {
		enc := encodeTimeout(d)
		got, err := parseTimeout(enc)
		if err != nil || len(enc) > 9 || got < d {
			t.Errorf("%v -> %q -> %v %v", d, enc, got, err)
		}
	}
	if _, err := parseTimeout("12x"); err == nil {
		t.Error("expected error")
	}
	if m := decodeMessage(encodeMessage("a%b\nü")); m != "a%b\nü" {
		t.Errorf("message round trip = %q", m)
	}
}
