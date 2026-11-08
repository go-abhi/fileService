package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "github.com/go-abhi/fileService/api/fileservice/v1"
	"github.com/go-abhi/fileService/internal/core"
	"github.com/go-abhi/fileService/internal/grpcx"
	"github.com/go-abhi/fileService/pkg/client"
)

const (
	acmeKey  = "acme-key-0123456789abcdef"
	otherKey = "other-key-0123456789abcdef"
)

func startApp(t *testing.T, mods ...func(*Config)) *App {
	t.Helper()
	cfg := Config{
		HTTPAddr: "127.0.0.1:0", GRPCAddr: "127.0.0.1:0",
		Dev: true, DevS3Addr: "127.0.0.1:0",
		APIKeys:         "acme:" + acmeKey + ",other:" + otherKey,
		MetadataStore:   "s3",
		Core:            core.Config{DefaultPartSize: 5 * core.MiB, StreamPartSize: 5 * core.MiB},
		JanitorInterval: time.Hour, StartupRetryLimit: time.Second,
	}
	for _, m := range mods {
		m(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	a, err := New(ctx, cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	})
	return a
}

func newClient(t *testing.T, addr, key string) *client.Client {
	t.Helper()
	c, err := client.New(addr, client.WithAPIKey(key), client.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

type memWriterAt struct {
	mu  sync.Mutex
	buf []byte
}

func (m *memWriterAt) WriteAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if need := int(off) + len(p); need > len(m.buf) {
		m.buf = append(m.buf, make([]byte, need-len(m.buf))...)
	}
	copy(m.buf[off:], p)
	return len(p), nil
}

func (m *memWriterAt) Write(p []byte) (int, error) { return m.WriteAt(p, int64(len(m.buf))) }

// onlyReader hides io.ReaderAt / io.Seeker to exercise sequential paths.
type onlyReader struct{ r io.Reader }

func (o onlyReader) Read(p []byte) (int, error) { return o.r.Read(p) }

type onlyWriter struct{ w io.Writer }

func (o onlyWriter) Write(p []byte) (int, error) { return o.w.Write(p) }

func TestE2EDirectUploadAndDownload(t *testing.T) {
	a := startApp(t)
	c := newClient(t, a.GRPCAddr(), acmeKey)
	ctx := context.Background()
	data := randBytes(23*int(core.MiB) + 517)

	var progress int64
	f, err := c.Upload(ctx, bytes.NewReader(data), int64(len(data)), client.UploadOptions{
		Name: "dataset.parquet", Concurrency: 4, Metadata: map[string]string{"source": "e2e"},
		OnProgress: func(done, total int64) { progress = done },
	})
	if err != nil {
		t.Fatal(err)
	}
	if f.Status != v1.FileStatusAvailable || f.Size != int64(len(data)) || f.Metadata["source"] != "e2e" || f.Namespace != "acme" {
		t.Fatalf("file = %+v", f)
	}
	if progress != int64(len(data)) {
		t.Fatalf("progress ended at %d", progress)
	}

	// Parallel ranged download into a WriterAt.
	var wa memWriterAt
	if _, err := c.Download(ctx, f.ID, &wa, client.DownloadOptions{ChunkSize: 4 << 20, Concurrency: 3}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wa.buf, data) {
		t.Fatal("parallel download mismatch")
	}
	// Sequential download into a plain writer.
	var buf bytes.Buffer
	if _, err := c.Download(ctx, f.ID, onlyWriter{&buf}, client.DownloadOptions{}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatal("sequential download mismatch")
	}
	// Byte range streamed through the service.
	buf.Reset()
	if _, err := c.DownloadStream(ctx, f.ID, &buf, 7<<20, 1<<20); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), data[7<<20:8<<20]) {
		t.Fatal("ranged stream mismatch")
	}
	// The signed URL works for any HTTP client.
	u, err := c.URL(ctx, f.ID, time.Minute, true)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(u.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(body, data) || !strings.Contains(resp.Header.Get("Content-Disposition"), "dataset.parquet") {
		t.Fatalf("signed URL download: %d bytes, %q", len(body), resp.Header.Get("Content-Disposition"))
	}

	if err := c.Delete(ctx, f.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stat(ctx, f.ID); client.CodeOf(err) != client.NotFound {
		t.Fatalf("after delete: %v", err)
	}
}

func TestE2ESequentialSourceAndEmptyFile(t *testing.T) {
	a := startApp(t)
	c := newClient(t, a.GRPCAddr(), acmeKey)
	ctx := context.Background()
	data := randBytes(11 * int(core.MiB))
	f, err := c.Upload(ctx, onlyReader{bytes.NewReader(data)}, int64(len(data)), client.UploadOptions{Name: "pipe.bin", Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	var out memWriterAt
	if _, err := c.Download(ctx, f.ID, &out, client.DownloadOptions{ChunkSize: 1 << 20}); err != nil || !bytes.Equal(out.buf, data) {
		t.Fatalf("download: %v", err)
	}

	empty, err := c.Upload(ctx, bytes.NewReader(nil), 0, client.UploadOptions{Name: "empty.txt"})
	if err != nil || empty.Status != v1.FileStatusAvailable || empty.Size != 0 {
		t.Fatalf("empty upload: %v %+v", err, empty)
	}
	var e bytes.Buffer
	if _, err := c.Download(ctx, empty.ID, &e, client.DownloadOptions{}); err != nil || e.Len() != 0 {
		t.Fatalf("empty download: %v %d", err, e.Len())
	}
}

// flakyReaderAt fails reads beyond limit until healed.
type flakyReaderAt struct {
	data  []byte
	limit int64
}

func (f *flakyReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off+int64(len(p)) > f.limit {
		return 0, errors.New("disk read error")
	}
	return copy(p, f.data[off:]), nil
}

func (f *flakyReaderAt) Read(p []byte) (int, error) { return 0, errors.New("use ReadAt") }

func TestE2EResumeUpload(t *testing.T) {
	a := startApp(t)
	c := newClient(t, a.GRPCAddr(), acmeKey)
	ctx := context.Background()
	data := randBytes(16 * int(core.MiB))
	src := &flakyReaderAt{data: data, limit: 10 << 20} // parts 1-2 readable, 3-4 fail

	_, err := c.Upload(ctx, src, int64(len(data)), client.UploadOptions{Name: "big.iso", Concurrency: 1, KeepOnError: true})
	var ue *client.UploadError
	if !errors.As(err, &ue) || ue.Aborted || ue.UploadID == "" {
		t.Fatalf("expected resumable UploadError, got %v", err)
	}
	st, err := c.GetUpload(ctx, &v1.GetUploadRequest{UploadID: ue.UploadID})
	if err != nil || len(st.UploadedParts) != 2 || st.PartCount != 4 {
		t.Fatalf("state after failure: %v %+v", err, st)
	}
	f, err := c.ResumeUpload(ctx, ue.UploadID, bytes.NewReader(data), client.UploadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var out memWriterAt
	if _, err := c.Download(ctx, f.ID, &out, client.DownloadOptions{}); err != nil || !bytes.Equal(out.buf, data) {
		t.Fatalf("resumed content mismatch: %v", err)
	}

	// Without KeepOnError a failed upload is aborted.
	_, err = c.Upload(ctx, &flakyReaderAt{data: data, limit: 1}, int64(len(data)), client.UploadOptions{Name: "x"})
	if !errors.As(err, &ue) || !ue.Aborted {
		t.Fatalf("expected aborted upload, got %v", err)
	}
	if _, err := c.Stat(ctx, ue.UploadID); client.CodeOf(err) != client.NotFound {
		t.Fatalf("aborted upload still exists: %v", err)
	}
}

func TestE2EStreamingOverHTTPPort(t *testing.T) {
	a := startApp(t)
	// gRPC also works on the HTTP port (h2c, routed by content type).
	c := newClient(t, "http://"+a.HTTPAddr(), acmeKey)
	ctx := context.Background()
	data := randBytes(12*int(core.MiB) + 3)
	f, err := c.UploadStream(ctx, onlyReader{bytes.NewReader(data)}, client.StreamOptions{Name: "stream.bin", Size: -1})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if f.SHA256 != hex.EncodeToString(sum[:]) || f.Size != int64(len(data)) {
		t.Fatalf("stream file = %+v", f)
	}
	var out bytes.Buffer
	if _, err := c.DownloadStream(ctx, f.ID, &out, 0, 0); err != nil || !bytes.Equal(out.Bytes(), data) {
		t.Fatalf("download stream: %v", err)
	}
	// Sequential URL downloads verify the SHA-256 known from the stream upload.
	out.Reset()
	if _, err := c.Download(ctx, f.ID, onlyWriter{&out}, client.DownloadOptions{}); err != nil || !bytes.Equal(out.Bytes(), data) {
		t.Fatalf("verified download: %v", err)
	}

	_, err = c.UploadStream(ctx, bytes.NewReader(data), client.StreamOptions{Name: "bad.bin", Size: int64(len(data)) + 10})
	if client.CodeOf(err) != client.InvalidArgument {
		t.Fatalf("size mismatch: %v", err)
	}
	_, err = c.DownloadStream(ctx, f.ID, io.Discard, int64(len(data))+1, 0)
	if client.CodeOf(err) != client.OutOfRange {
		t.Fatalf("offset beyond end: %v", err)
	}
}

func TestE2EAuthIsolationAndREST(t *testing.T) {
	a := startApp(t)
	ctx := context.Background()
	acme := newClient(t, a.GRPCAddr(), acmeKey)
	other := newClient(t, a.GRPCAddr(), otherKey)
	bad := newClient(t, a.GRPCAddr(), "wrong-key-0123456789abcdef")

	f, err := acme.UploadStream(ctx, strings.NewReader("hello"), client.StreamOptions{Name: "hello.txt", Size: 5})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bad.Stat(ctx, f.ID); client.CodeOf(err) != client.Unauthenticated {
		t.Fatalf("bad key: %v", err)
	}
	if _, err := other.Stat(ctx, f.ID); client.CodeOf(err) != client.NotFound {
		t.Fatalf("other namespace: %v", err)
	}
	list, err := other.ListFiles(ctx, &v1.ListFilesRequest{})
	if err != nil || len(list.Files) != 0 {
		t.Fatalf("other list: %v %d", err, len(list.Files))
	}

	// Health checks need no credentials.
	cc, _ := grpcx.Dial("http://" + a.GRPCAddr())
	var hr grpcx.HealthCheckResponse
	if err := cc.Invoke(ctx, grpcx.HealthCheckMethod, &grpcx.HealthCheckRequest{}, &hr); err != nil || hr.Status != grpcx.HealthServing {
		t.Fatalf("health: %v %+v", err, hr)
	}

	// The same file through REST.
	req, _ := http.NewRequest("GET", "http://"+a.HTTPAddr()+"/v1/files/"+f.ID, nil)
	req.Header.Set("X-Api-Key", acmeKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var rf struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	}
	json.NewDecoder(resp.Body).Decode(&rf)
	resp.Body.Close()
	if resp.StatusCode != 200 || rf.Name != "hello.txt" || rf.Status != "available" {
		t.Fatalf("REST: %d %+v", resp.StatusCode, rf)
	}
	resp, err = http.Get("http://" + a.HTTPAddr() + "/readyz")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("readyz: %v %v", err, resp.Status)
	}
	resp.Body.Close()
}

func TestE2EConcurrentClients(t *testing.T) {
	a := startApp(t)
	c := newClient(t, a.GRPCAddr(), acmeKey)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := randBytes(100_000 + i*1000)
			var f *v1.File
			var err error
			if i%2 == 0 {
				f, err = c.Upload(ctx, bytes.NewReader(data), int64(len(data)), client.UploadOptions{Name: fmt.Sprintf("f%d.bin", i)})
			} else {
				f, err = c.UploadStream(ctx, bytes.NewReader(data), client.StreamOptions{Name: fmt.Sprintf("f%d.bin", i), Size: -1})
			}
			if err != nil {
				errs <- err
				return
			}
			var out bytes.Buffer
			if _, err := c.Download(ctx, f.ID, &out, client.DownloadOptions{}); err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(out.Bytes(), data) {
				errs <- fmt.Errorf("file %d mismatch", i)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	var total int
	token := ""
	for {
		resp, err := c.ListFiles(ctx, &v1.ListFilesRequest{PageSize: 5, PageToken: token})
		if err != nil {
			t.Fatal(err)
		}
		total += len(resp.Files)
		if resp.NextPageToken == "" {
			break
		}
		token = resp.NextPageToken
	}
	if total != 16 {
		t.Fatalf("listed %d files", total)
	}
}

func TestConfigFromEnv(t *testing.T) {
	env := map[string]string{
		"FILESERVICE_S3_BUCKET":             "my-bucket",
		"FILESERVICE_API_KEYS":              "acme:" + acmeKey,
		"AWS_REGION":                        "eu-west-1",
		"FILESERVICE_PART_SIZE":             "64MiB",
		"FILESERVICE_MAX_DOWNLOAD_URL_TTL":  "2d",
		"FILESERVICE_ALLOWED_CONTENT_TYPES": "image/*, application/pdf",
		"FILESERVICE_GRPC_ADDR":             "off",
	}
	cfg, err := ConfigFromEnv(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.S3Region != "eu-west-1" || cfg.Core.DefaultPartSize != 64*core.MiB || cfg.Core.MaxDownloadURLTTL != 48*time.Hour ||
		len(cfg.Core.AllowedContentTypes) != 2 || cfg.GRPCAddr != "" || cfg.HTTPAddr != ":8080" || cfg.S3PathStyle {
		t.Fatalf("cfg = %+v", cfg)
	}

	dev, err := ConfigFromEnv(func(k string) string { return map[string]string{"FILESERVICE_DEV": "true"}[k] })
	if err != nil || dev.HTTPAddr != "127.0.0.1:8080" || dev.GRPCAddr != "127.0.0.1:9090" {
		t.Fatalf("dev mode must default to loopback: %v %q %q", err, dev.HTTPAddr, dev.GRPCAddr)
	}

	_, err = ConfigFromEnv(func(k string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "FILESERVICE_S3_BUCKET") || !strings.Contains(err.Error(), "FILESERVICE_API_KEYS") {
		t.Fatalf("missing config: %v", err)
	}
	env["FILESERVICE_PART_SIZE"] = "1MiB"
	env["FILESERVICE_UPLOAD_EXPIRY"] = "soon"
	_, err = ConfigFromEnv(func(k string) string { return env[k] })
	if err == nil || !strings.Contains(err.Error(), "FILESERVICE_UPLOAD_EXPIRY") {
		t.Fatalf("bad values: %v", err)
	}
}

func TestParseSize(t *testing.T) {
	cases := map[string]int64{"1024": 1024, "16MiB": 16 << 20, "16M": 16 << 20, "1.5GiB": 3 << 29, "100MB": 100e6, "5TiB": 5 << 40, "64k": 64 << 10}
	for in, want := range cases {
		if got, err := ParseSize(in); err != nil || got != want {
			t.Errorf("ParseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	if _, err := ParseSize("lots"); err == nil {
		t.Error("expected error")
	}
}
