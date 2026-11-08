package s3_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-abhi/fileService/internal/s3"
	"github.com/go-abhi/fileService/internal/s3/sigv4"
	"github.com/go-abhi/fileService/internal/s3fake"
)

const (
	testAK     = "AKIDTEST"
	testSK     = "secret/key+test"
	testBucket = "files"
)

func newClient(t *testing.T, handler http.Handler, mod ...func(*s3.Config)) *s3.Client {
	t.Helper()
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	cfg := s3.Config{
		Bucket:      testBucket,
		Region:      "us-east-1",
		Endpoint:    ts.URL,
		PathStyle:   true,
		Credentials: s3.StaticCredentials{AccessKeyID: testAK, SecretAccessKey: testSK},
	}
	for _, m := range mod {
		m(&cfg)
	}
	c, err := s3.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func randBytes(t *testing.T, n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestObjectLifecycle(t *testing.T) {
	ctx := context.Background()
	fake := s3fake.New(s3fake.WithCredentials(testAK, testSK))
	c := newClient(t, fake)

	if err := c.EnsureBucket(ctx); err != nil {
		t.Fatalf("EnsureBucket: %v", err)
	}
	if err := c.EnsureBucket(ctx); err != nil {
		t.Fatalf("EnsureBucket (existing): %v", err)
	}

	data := []byte("hello, object storage")
	etag, err := c.PutObject(ctx, "ns/dir/hello world.txt", data, s3.ObjectHeaders{
		ContentType:        "text/plain",
		ContentDisposition: `inline; filename="hello.txt"`,
		Metadata:           map[string]string{"file-id": "abc"},
	})
	if err != nil || etag == "" {
		t.Fatalf("PutObject: %v %q", err, etag)
	}

	info, err := c.HeadObject(ctx, "ns/dir/hello world.txt")
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if info.Size != int64(len(data)) || info.ContentType != "text/plain" || info.ETag != etag ||
		info.ContentDisposition != `inline; filename="hello.txt"` || info.Metadata["file-id"] != "abc" {
		t.Fatalf("unexpected info: %+v", info)
	}

	out, err := c.GetObject(ctx, "ns/dir/hello world.txt", 7, 6)
	if err != nil {
		t.Fatalf("GetObject range: %v", err)
	}
	got, _ := io.ReadAll(out.Body)
	out.Body.Close()
	if string(got) != "object" || out.Info.Size != int64(len(data)) || out.ContentLength != 6 {
		t.Fatalf("range read = %q size=%d len=%d", got, out.Info.Size, out.ContentLength)
	}

	out, err = c.GetObject(ctx, "ns/dir/hello world.txt", 7, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, _ = io.ReadAll(out.Body)
	out.Body.Close()
	if string(got) != "object storage" {
		t.Fatalf("open-ended range = %q", got)
	}

	if _, err := c.HeadObject(ctx, "missing"); !s3.IsNotFound(err) {
		t.Fatalf("HeadObject(missing) err = %v", err)
	}
	if _, err := c.GetObject(ctx, "missing", 0, 0); !s3.IsNotFound(err) {
		t.Fatalf("GetObject(missing) err = %v", err)
	}
	if err := c.DeleteObject(ctx, "ns/dir/hello world.txt"); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteObject(ctx, "ns/dir/hello world.txt"); err != nil {
		t.Fatalf("second delete: %v", err)
	}
	if _, err := c.HeadObject(ctx, "ns/dir/hello world.txt"); !s3.IsNotFound(err) {
		t.Fatalf("after delete err = %v", err)
	}
}

func TestEmptyObject(t *testing.T) {
	ctx := context.Background()
	fake := s3fake.New(s3fake.WithCredentials(testAK, testSK))
	fake.CreateBucket(testBucket)
	c := newClient(t, fake)
	if _, err := c.PutObject(ctx, "empty", nil, s3.ObjectHeaders{}); err != nil {
		t.Fatal(err)
	}
	info, err := c.HeadObject(ctx, "empty")
	if err != nil || info.Size != 0 {
		t.Fatalf("head empty: %v %+v", err, info)
	}
}

func TestListObjectsPaginates(t *testing.T) {
	ctx := context.Background()
	fake := s3fake.New(s3fake.WithCredentials(testAK, testSK), s3fake.WithPageSize(2))
	fake.CreateBucket(testBucket)
	c := newClient(t, fake)
	for _, k := range []string{"a/1", "a/2", "a/3", "a/4", "a/5", "b/1"} {
		if _, err := c.PutObject(ctx, k, []byte(k), s3.ObjectHeaders{}); err != nil {
			t.Fatal(err)
		}
	}
	var keys []string
	in := s3.ListObjectsInput{Prefix: "a/", StartAfter: "a/1"}
	for {
		out, err := c.ListObjects(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range out.Objects {
			keys = append(keys, o.Key)
		}
		if !out.IsTruncated {
			break
		}
		in.ContinuationToken = out.NextContinuationToken
	}
	if strings.Join(keys, ",") != "a/2,a/3,a/4,a/5" {
		t.Fatalf("keys = %v", keys)
	}
}

func TestMultipartUpload(t *testing.T) {
	ctx := context.Background()
	fake := s3fake.New(s3fake.WithCredentials(testAK, testSK), s3fake.WithPageSize(2))
	fake.CreateBucket(testBucket)
	c := newClient(t, fake)

	id, err := c.CreateMultipartUpload(ctx, "big", s3.ObjectHeaders{ContentType: "application/zip"})
	if err != nil {
		t.Fatal(err)
	}
	p1, p2, p3 := randBytes(t, 5<<20), randBytes(t, 5<<20), randBytes(t, 1234)
	var parts []s3.CompletedPart
	for i, p := range [][]byte{p1, p2, p3} {
		etag, err := c.UploadPart(ctx, "big", id, i+1, p)
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i+1, err)
		}
		parts = append(parts, s3.CompletedPart{PartNumber: i + 1, ETag: etag})
	}
	listed, err := c.ListParts(ctx, "big", id) // paginated (page size 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 3 || listed[2].PartNumber != 3 || listed[2].Size != 1234 || listed[0].ETag != parts[0].ETag {
		t.Fatalf("ListParts = %+v", listed)
	}
	etag, err := c.CompleteMultipartUpload(ctx, "big", id, parts)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if !strings.HasSuffix(strings.Trim(etag, `"`), "-3") {
		t.Fatalf("multipart etag = %q", etag)
	}
	stored, _ := fake.Object(testBucket, "big")
	if !bytes.Equal(stored, append(append(append([]byte{}, p1...), p2...), p3...)) {
		t.Fatal("assembled object mismatch")
	}
	info, err := c.HeadObject(ctx, "big")
	if err != nil || info.ContentType != "application/zip" {
		t.Fatalf("head after complete: %v %+v", err, info)
	}
	if _, err := c.ListParts(ctx, "big", id); !s3.IsNoSuchUpload(err) {
		t.Fatalf("ListParts after complete: %v", err)
	}
}

func TestMultipartRejectsSmallPartsAndAbort(t *testing.T) {
	ctx := context.Background()
	fake := s3fake.New(s3fake.WithCredentials(testAK, testSK))
	fake.CreateBucket(testBucket)
	c := newClient(t, fake)
	id, err := c.CreateMultipartUpload(ctx, "k", s3.ObjectHeaders{})
	if err != nil {
		t.Fatal(err)
	}
	e1, _ := c.UploadPart(ctx, "k", id, 1, []byte("tiny"))
	e2, _ := c.UploadPart(ctx, "k", id, 2, []byte("tiny"))
	_, err = c.CompleteMultipartUpload(ctx, "k", id, []s3.CompletedPart{{1, e1}, {2, e2}})
	if s3.ErrorCode(err) != "EntityTooSmall" {
		t.Fatalf("expected EntityTooSmall, got %v", err)
	}
	if err := c.AbortMultipartUpload(ctx, "k", id); err != nil {
		t.Fatal(err)
	}
	if err := c.AbortMultipartUpload(ctx, "k", id); !s3.IsNoSuchUpload(err) {
		t.Fatalf("second abort: %v", err)
	}
	if fake.UploadCount(testBucket) != 0 {
		t.Fatal("upload not removed")
	}
}

func TestPresignedUploadPartAndDownload(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	clock := func() time.Time { return now }
	fake := s3fake.New(s3fake.WithCredentials(testAK, testSK), s3fake.WithClock(func() time.Time { return clock() }))
	fake.CreateBucket(testBucket)
	c := newClient(t, fake)

	id, err := c.CreateMultipartUpload(ctx, "ns/f", s3.ObjectHeaders{ContentType: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("presigned part body")
	u, exp, err := c.PresignUploadPart(ctx, "ns/f", id, 1, int64(len(data)), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if d := exp.Sub(now); d < 59*time.Minute || d > time.Hour+time.Second {
		t.Fatalf("expiry %v", d)
	}

	put := func(body []byte) *http.Response {
		req, _ := http.NewRequest(http.MethodPut, u, bytes.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}
	if resp := put([]byte("wrong size")); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("wrong-size PUT status = %d, want 403", resp.StatusCode)
	}
	resp := put(data)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("ETag") == "" {
		t.Fatalf("presigned PUT status = %d", resp.StatusCode)
	}
	parts, err := c.ListParts(ctx, "ns/f", id)
	if err != nil || len(parts) != 1 {
		t.Fatalf("ListParts: %v %+v", err, parts)
	}
	if _, err := c.CompleteMultipartUpload(ctx, "ns/f", id, []s3.CompletedPart{{1, parts[0].ETag}}); err != nil {
		t.Fatal(err)
	}

	get, _, err := c.PresignGetObject(ctx, "ns/f", 10*time.Minute, url.Values{"response-content-disposition": {`attachment; filename="f.txt"`}})
	if err != nil {
		t.Fatal(err)
	}
	r, err := http.Get(get)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if r.StatusCode != 200 || string(body) != string(data) || r.Header.Get("Content-Disposition") != `attachment; filename="f.txt"` {
		t.Fatalf("presigned GET: %d %q %q", r.StatusCode, body, r.Header.Get("Content-Disposition"))
	}

	// After expiry the URL must be rejected.
	now = now.Add(11 * time.Minute)
	r, err = http.Get(get)
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.StatusCode != http.StatusForbidden {
		t.Fatalf("expired URL status = %d", r.StatusCode)
	}
}

func TestPresignCappedByCredentialExpiry(t *testing.T) {
	now := time.Now()
	c := newClient(t, s3fake.New(), func(cfg *s3.Config) {
		cfg.Credentials = s3.StaticCredentials{AccessKeyID: "a", SecretAccessKey: "b", SessionToken: "t", Expires: now.Add(20 * time.Minute)}
		cfg.Now = func() time.Time { return now }
	})
	u, exp, err := c.PresignGetObject(context.Background(), "k", time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	if exp.Sub(now) > 20*time.Minute || !strings.Contains(u, "X-Amz-Expires=1200") || !strings.Contains(u, "X-Amz-Security-Token=t") {
		t.Fatalf("presign not capped: exp=%v url=%s", exp.Sub(now), u)
	}
}

func TestWrongSecretIsRejected(t *testing.T) {
	fake := s3fake.New(s3fake.WithCredentials(testAK, testSK))
	fake.CreateBucket(testBucket)
	c := newClient(t, fake, func(cfg *s3.Config) {
		cfg.Credentials = s3.StaticCredentials{AccessKeyID: testAK, SecretAccessKey: "nope"}
	})
	_, err := c.PutObject(context.Background(), "k", []byte("x"), s3.ObjectHeaders{})
	if s3.ErrorCode(err) != "SignatureDoesNotMatch" {
		t.Fatalf("err = %v", err)
	}
}

func TestRetriesThrottling(t *testing.T) {
	fake := s3fake.New()
	fake.CreateBucket(testBucket)
	var calls atomic.Int32
	flaky := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			io.WriteString(w, `<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>`)
			return
		}
		fake.ServeHTTP(w, r)
	})
	c := newClient(t, flaky)
	if _, err := c.PutObject(context.Background(), "k", []byte("x"), s3.ObjectHeaders{}); err != nil {
		t.Fatalf("expected success after retries: %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d", calls.Load())
	}

	calls.Store(0)
	c1 := newClient(t, flaky, func(cfg *s3.Config) { cfg.MaxAttempts = 1 })
	if _, err := c1.PutObject(context.Background(), "k", []byte("x"), s3.ObjectHeaders{}); s3.ErrorCode(err) != "SlowDown" {
		t.Fatalf("expected SlowDown, got %v", err)
	}
}

func TestCompleteWith200ErrorBody(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "\n  <?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>InvalidPart</Code><Message>bad</Message></Error>")
	})
	c := newClient(t, h, func(cfg *s3.Config) { cfg.MaxAttempts = 1 })
	_, err := c.CompleteMultipartUpload(context.Background(), "k", "id", []s3.CompletedPart{{1, `"x"`}})
	if s3.ErrorCode(err) != "InvalidPart" {
		t.Fatalf("err = %v", err)
	}
}

func TestEndpoints(t *testing.T) {
	creds := s3.StaticCredentials{AccessKeyID: "a", SecretAccessKey: "b"}
	ctx := context.Background()
	cases := []struct {
		cfg  s3.Config
		want string
	}{
		{s3.Config{Bucket: "my-bucket", Region: "eu-west-1", Credentials: creds}, "https://my-bucket.s3.eu-west-1.amazonaws.com/a/b.txt?"},
		{s3.Config{Bucket: "my.bucket", Region: "eu-west-1", Credentials: creds}, "https://s3.eu-west-1.amazonaws.com/my.bucket/a/b.txt?"},
		{s3.Config{Bucket: "b", Region: "cn-north-1", Credentials: creds}, "https://b.s3.cn-north-1.amazonaws.com.cn/a/b.txt?"},
		{s3.Config{Bucket: "b", Endpoint: "http://minio:9000", PresignEndpoint: "http://localhost:9000", PathStyle: true, Credentials: creds}, "http://localhost:9000/b/a/b.txt?"},
	}
	for _, tc := range cases {
		c, err := s3.New(tc.cfg)
		if err != nil {
			t.Fatal(err)
		}
		u, _, err := c.PresignGetObject(ctx, "a/b.txt", time.Minute, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(u, tc.want) {
			t.Errorf("got %s, want prefix %s", u, tc.want)
		}
	}

	acc, err := s3.New(s3.Config{Bucket: "fast", Region: "us-west-2", Accelerate: true, Credentials: creds})
	if err != nil {
		t.Fatal(err)
	}
	u, _, _ := acc.PresignUploadPart(ctx, "k", "id", 1, 10, time.Minute)
	if !strings.HasPrefix(u, "https://fast.s3-accelerate.amazonaws.com/k?") || !strings.Contains(u, "X-Amz-SignedHeaders=content-length%3Bhost") {
		t.Errorf("accelerate URL = %s", u)
	}
	if _, err := s3.New(s3.Config{Bucket: "b", Endpoint: "http://x", Accelerate: true, Credentials: creds}); err == nil {
		t.Error("accelerate with custom endpoint should fail")
	}
}

func TestSignerMatchesPresignedVerification(t *testing.T) {
	// Sanity check that a presigned URL from the client carries a scope the
	// fake verifies (region mismatch must be rejected).
	fake := s3fake.New(s3fake.WithCredentials(testAK, testSK), s3fake.WithRegion("eu-central-1"))
	fake.CreateBucket(testBucket)
	c := newClient(t, fake) // signs for us-east-1
	_, err := c.PutObject(context.Background(), "k", []byte("x"), s3.ObjectHeaders{})
	if s3.ErrorCode(err) != "AuthorizationHeaderMalformed" {
		t.Fatalf("err = %v", err)
	}
	_ = sigv4.Algorithm
}
