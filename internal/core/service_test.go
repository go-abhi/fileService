package core_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-abhi/fileService/internal/core"
	"github.com/go-abhi/fileService/internal/metastore"
	"github.com/go-abhi/fileService/internal/s3"
	"github.com/go-abhi/fileService/internal/s3fake"
	"github.com/go-abhi/fileService/internal/storage"
)

const bucket = "files"

type recorder struct {
	mu     sync.Mutex
	events []core.Event
}

func (r *recorder) Publish(_ context.Context, e core.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recorder) types() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, e := range r.events {
		out = append(out, e.Type)
	}
	return out
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type env struct {
	svc    *core.Service
	fake   *s3fake.Server
	events *recorder
	clock  *clock
	ctx    context.Context
}

func newEnv(t *testing.T, s3meta bool, mods ...func(*core.Config)) *env {
	t.Helper()
	clk := &clock{t: time.Now()}
	fake := s3fake.New(s3fake.WithCredentials("ak", "sk"), s3fake.WithClock(clk.Now))
	fake.CreateBucket(bucket)
	ts := httptest.NewServer(fake)
	t.Cleanup(ts.Close)
	client, err := s3.New(s3.Config{Bucket: bucket, Endpoint: ts.URL, PathStyle: true,
		Credentials: s3.StaticCredentials{AccessKeyID: "ak", SecretAccessKey: "sk"}, Now: clk.Now})
	if err != nil {
		t.Fatal(err)
	}
	var meta core.MetadataStore = metastore.NewMemory()
	if s3meta {
		meta = metastore.NewS3(client, "files")
	}
	cfg := core.Config{KeyPrefix: "files", DefaultPartSize: 5 * core.MiB, StreamPartSize: 5 * core.MiB}
	for _, m := range mods {
		m(&cfg)
	}
	rec := &recorder{}
	svc, err := core.New(cfg, core.Deps{Storage: storage.NewS3(client), Metadata: meta, Events: rec, Now: clk.Now})
	if err != nil {
		t.Fatal(err)
	}
	return &env{svc: svc, fake: fake, events: rec, clock: clk,
		ctx: core.WithPrincipal(context.Background(), core.Principal{Namespace: "acme", Subject: "test"})}
}

func randData(t *testing.T, n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func put(t *testing.T, u core.PartURL, data []byte) int {
	t.Helper()
	req, _ := http.NewRequest(u.Method, u.URL, bytes.NewReader(data))
	for k, v := range u.Headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

func download(t *testing.T, url string) (*http.Response, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func wantKind(t *testing.T, err error, k core.Kind, contains string) {
	t.Helper()
	if err == nil || core.KindOf(err) != k || !strings.Contains(err.Error(), contains) {
		t.Fatalf("want %v error containing %q, got %v", k, contains, err)
	}
}

func TestDirectUploadLifecycle(t *testing.T) {
	for _, s3meta := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "s3meta"}[s3meta], func(t *testing.T) {
			e := newEnv(t, s3meta)
			data := randData(t, 12*int(core.MiB)+123)
			up, err := e.svc.CreateUpload(e.ctx, core.CreateUploadInput{Name: "dir/Report Q1 ü.pdf", Size: int64(len(data)), Metadata: map[string]string{"owner": "42"}})
			if err != nil {
				t.Fatal(err)
			}
			f := up.File
			if f.Name != "Report Q1 ü.pdf" || f.ContentType != "application/pdf" || f.Status != core.StatusPending ||
				up.PartCount != 3 || up.PartSize != 5*core.MiB || len(up.Parts) != 3 {
				t.Fatalf("unexpected upload: %+v %+v", up, f)
			}
			if up.Parts[2].Offset != 10*core.MiB || up.Parts[2].Size != 2*core.MiB+123 {
				t.Fatalf("last part = %+v", up.Parts[2])
			}
			// Upload out of order, as parallel clients do.
			for i := len(up.Parts) - 1; i >= 0; i-- {
				p := up.Parts[i]
				if st := put(t, p, data[p.Offset:p.Offset+p.Size]); st != http.StatusOK {
					t.Fatalf("part %d status %d", p.PartNumber, st)
				}
			}
			st, err := e.svc.GetUpload(e.ctx, f.ID)
			if err != nil || len(st.Uploaded) != 3 || st.PartCount != 3 {
				t.Fatalf("GetUpload: %v %+v", err, st)
			}
			done, err := e.svc.CompleteUpload(e.ctx, f.ID)
			if err != nil {
				t.Fatal(err)
			}
			if done.Status != core.StatusAvailable || done.Size != int64(len(data)) || done.ETag == "" || done.Upload != nil {
				t.Fatalf("completed file = %+v", done)
			}
			again, err := e.svc.CompleteUpload(e.ctx, f.ID)
			if err != nil || again.Status != core.StatusAvailable {
				t.Fatalf("idempotent complete: %v", err)
			}
			if got := e.events.types(); len(got) != 1 || got[0] != core.EventFileUploaded {
				t.Fatalf("events = %v", got)
			}
			stored, _ := e.fake.Object(bucket, "files/acme/"+f.ID)
			if !bytes.Equal(stored, data) {
				t.Fatal("stored object differs")
			}

			dl, err := e.svc.DownloadURL(e.ctx, core.DownloadURLInput{FileID: f.ID, Attachment: true})
			if err != nil {
				t.Fatal(err)
			}
			resp, body := download(t, dl.URL)
			if resp.StatusCode != 200 || !bytes.Equal(body, data) || dl.CDN {
				t.Fatalf("download status %d len %d", resp.StatusCode, len(body))
			}
			if cd := resp.Header.Get("Content-Disposition"); !strings.HasPrefix(cd, "attachment;") || !strings.Contains(cd, "Report%20Q1%20%C3%BC.pdf") {
				t.Fatalf("Content-Disposition = %q", cd)
			}
			if d := dl.ExpiresAt.Sub(e.clock.Now()); d < 14*time.Minute || d > 15*time.Minute {
				t.Fatalf("default ttl = %v", d)
			}

			got, err := e.svc.GetFile(e.ctx, f.ID)
			if err != nil || got.Metadata["owner"] != "42" {
				t.Fatalf("GetFile: %v %+v", err, got)
			}
			if err := e.svc.DeleteFile(e.ctx, f.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := e.svc.GetFile(e.ctx, f.ID); !core.IsNotFound(err) {
				t.Fatalf("after delete: %v", err)
			}
			if _, ok := e.fake.Object(bucket, "files/acme/"+f.ID); ok {
				t.Fatal("object not deleted")
			}
			if got := e.events.types(); len(got) != 2 || got[1] != core.EventFileDeleted {
				t.Fatalf("events = %v", got)
			}
		})
	}
}

func TestCompleteRequiresAllParts(t *testing.T) {
	e := newEnv(t, false)
	data := randData(t, 11*int(core.MiB))
	up, err := e.svc.CreateUpload(e.ctx, core.CreateUploadInput{Name: "a.bin", Size: int64(len(data))})
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, 2} {
		p := up.Parts[i]
		put(t, p, data[p.Offset:p.Offset+p.Size])
	}
	_, err = e.svc.CompleteUpload(e.ctx, up.File.ID)
	wantKind(t, err, core.KindFailedPrecondition, "missing parts: 2")

	// Wrong-size bodies are rejected by storage (Content-Length is signed).
	p := up.Parts[1]
	if st := put(t, p, data[:10]); st != http.StatusForbidden {
		t.Fatalf("wrong size status = %d", st)
	}
	// Fresh URLs (resume) work.
	urls, err := e.svc.PartURLs(e.ctx, up.File.ID, []int{2, 2})
	if err != nil || len(urls) != 1 {
		t.Fatalf("PartURLs: %v %+v", err, urls)
	}
	put(t, urls[0], data[p.Offset:p.Offset+p.Size])
	if _, err := e.svc.CompleteUpload(e.ctx, up.File.ID); err != nil {
		t.Fatal(err)
	}
	_, err = e.svc.PartURLs(e.ctx, up.File.ID, []int{1})
	wantKind(t, err, core.KindFailedPrecondition, "already complete")
}

func TestPartURLValidation(t *testing.T) {
	e := newEnv(t, false)
	up, _ := e.svc.CreateUpload(e.ctx, core.CreateUploadInput{Name: "a.bin", Size: 6 * core.MiB})
	_, err := e.svc.PartURLs(e.ctx, up.File.ID, nil)
	wantKind(t, err, core.KindInvalidArgument, "part_numbers")
	_, err = e.svc.PartURLs(e.ctx, up.File.ID, []int{3})
	wantKind(t, err, core.KindInvalidArgument, "out of range")
	_, err = e.svc.PartURLs(e.ctx, "not-an-id", []int{1})
	wantKind(t, err, core.KindInvalidArgument, "invalid id")
}

func TestManyPartsAreBatched(t *testing.T) {
	e := newEnv(t, false, func(c *core.Config) { c.InitialPartURLs = 4 })
	up, err := e.svc.CreateUpload(e.ctx, core.CreateUploadInput{Name: "big.iso", Size: 100 * core.GiB})
	if err != nil {
		t.Fatal(err)
	}
	// 5 MiB parts would need 20,480 parts: the size is raised to 11 MiB.
	if up.PartCount != 9310 || up.PartSize != 11*core.MiB || len(up.Parts) != 4 {
		t.Fatalf("parts %d of %d / urls %d", up.PartCount, up.PartSize, len(up.Parts))
	}
	// 5 TiB fits in 10,000 parts by raising the part size.
	up, err = e.svc.CreateUpload(e.ctx, core.CreateUploadInput{Name: "huge", Size: 5 * core.TiB, PartSize: 5 * core.MiB})
	if err != nil || up.PartCount > 10000 || up.PartSize < 5*core.TiB/10000 {
		t.Fatalf("huge: %v %d %d", err, up.PartCount, up.PartSize)
	}
}

func TestZeroByteFile(t *testing.T) {
	e := newEnv(t, false)
	up, err := e.svc.CreateUpload(e.ctx, core.CreateUploadInput{Name: "empty.txt", Size: 0})
	if err != nil {
		t.Fatal(err)
	}
	if up.File.Status != core.StatusAvailable || len(up.Parts) != 0 || up.File.SHA256 != hex.EncodeToString(sha256.New().Sum(nil)) {
		t.Fatalf("empty upload = %+v", up.File)
	}
	dl, err := e.svc.DownloadURL(e.ctx, core.DownloadURLInput{FileID: up.File.ID})
	if err != nil {
		t.Fatal(err)
	}
	resp, body := download(t, dl.URL)
	if resp.StatusCode != 200 || len(body) != 0 || resp.Header.Get("Content-Type") != "text/plain; charset=utf-8" {
		t.Fatalf("download empty: %d %d %q", resp.StatusCode, len(body), resp.Header.Get("Content-Type"))
	}
	_, rc, n, err := e.svc.OpenFile(e.ctx, up.File.ID, 0, 0)
	if err != nil || n != 0 {
		t.Fatalf("open empty: %v %d", err, n)
	}
	rc.Close()
}

func TestAbortAndDeletePending(t *testing.T) {
	e := newEnv(t, true)
	up, _ := e.svc.CreateUpload(e.ctx, core.CreateUploadInput{Name: "a.bin", Size: 6 * core.MiB})
	if err := e.svc.AbortUpload(e.ctx, up.File.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.GetFile(e.ctx, up.File.ID); !core.IsNotFound(err) {
		t.Fatalf("after abort: %v", err)
	}
	up, _ = e.svc.CreateUpload(e.ctx, core.CreateUploadInput{Name: "b.bin", Size: 6 * core.MiB})
	if err := e.svc.DeleteFile(e.ctx, up.File.ID); err != nil {
		t.Fatal(err)
	}
	if e.fake.UploadCount(bucket) != 0 {
		t.Fatalf("%d multipart uploads left", e.fake.UploadCount(bucket))
	}
	for _, k := range e.fake.ObjectKeys(bucket) {
		t.Errorf("leftover object %s", k)
	}
}

func TestNamespaceIsolation(t *testing.T) {
	e := newEnv(t, false)
	f, err := e.svc.UploadStream(e.ctx, core.StreamInput{Name: "secret.txt", Size: -1}, strings.NewReader("top secret"))
	if err != nil {
		t.Fatal(err)
	}
	other := core.WithPrincipal(context.Background(), core.Principal{Namespace: "other"})
	if _, err := e.svc.GetFile(other, f.ID); !core.IsNotFound(err) {
		t.Fatalf("GetFile: %v", err)
	}
	if _, err := e.svc.DownloadURL(other, core.DownloadURLInput{FileID: f.ID}); !core.IsNotFound(err) {
		t.Fatalf("DownloadURL: %v", err)
	}
	if err := e.svc.DeleteFile(other, f.ID); !core.IsNotFound(err) {
		t.Fatalf("DeleteFile: %v", err)
	}
	files, _, err := e.svc.ListFiles(other, 10, "", true)
	if err != nil || len(files) != 0 {
		t.Fatalf("ListFiles: %v %d", err, len(files))
	}
	if _, err := e.svc.GetFile(context.Background(), f.ID); core.KindOf(err) != core.KindUnauthenticated {
		t.Fatalf("no principal: %v", err)
	}
}

func TestValidation(t *testing.T) {
	e := newEnv(t, false, func(c *core.Config) {
		c.MaxFileSize = 100 * core.MiB
		c.AllowedContentTypes = []string{"image/*", "application/pdf"}
	})
	cases := []struct {
		in   core.CreateUploadInput
		kind core.Kind
		msg  string
	}{
		{core.CreateUploadInput{Name: "  ", Size: 1}, core.KindInvalidArgument, "name is required"},
		{core.CreateUploadInput{Name: "a\x00b.png", Size: 1}, core.KindInvalidArgument, "control"},
		{core.CreateUploadInput{Name: "a.png", Size: -1}, core.KindInvalidArgument, "negative"},
		{core.CreateUploadInput{Name: "a.png", Size: 101 * core.MiB}, core.KindTooLarge, "exceeds"},
		{core.CreateUploadInput{Name: "a.exe", Size: 1}, core.KindInvalidArgument, "not allowed"},
		{core.CreateUploadInput{Name: "a", ContentType: "bad type", Size: 1}, core.KindInvalidArgument, "invalid content type"},
		{core.CreateUploadInput{Name: "a.png", Size: 1, Metadata: map[string]string{"bad key": "v"}}, core.KindInvalidArgument, "metadata key"},
	}
	for _, tc := range cases {
		_, err := e.svc.CreateUpload(e.ctx, tc.in)
		wantKind(t, err, tc.kind, tc.msg)
	}
	up, err := e.svc.CreateUpload(e.ctx, core.CreateUploadInput{Name: "photo.JPG", Size: 10})
	if err != nil || up.File.ContentType != "image/jpeg" {
		t.Fatalf("detected type: %v %+v", err, up)
	}
}

func TestStreamUpload(t *testing.T) {
	e := newEnv(t, false)
	cases := []struct {
		name string
		size int
	}{
		{"small", 1000},
		{"exact-part", 5 * int(core.MiB)},
		{"multipart", 23*int(core.MiB) + 7},
		{"exact-multiple", 15 * int(core.MiB)},
	}
	for _, tc := range cases {
		for _, declared := range []bool{false, true} {
			data := randData(t, tc.size)
			size := int64(-1)
			if declared {
				size = int64(len(data))
			}
			f, err := e.svc.UploadStream(e.ctx, core.StreamInput{Name: tc.name + ".bin", Size: size}, iotestHalfReader{bytes.NewReader(data)})
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			sum := sha256.Sum256(data)
			if f.Size != int64(len(data)) || f.SHA256 != hex.EncodeToString(sum[:]) || f.Status != core.StatusAvailable {
				t.Fatalf("%s: file = %+v", tc.name, f)
			}
			stored, _ := e.fake.Object(bucket, "files/acme/"+f.ID)
			if !bytes.Equal(stored, data) {
				t.Fatalf("%s: stored content differs", tc.name)
			}
		}
	}
	if e.fake.UploadCount(bucket) != 0 {
		t.Fatal("multipart uploads left behind")
	}
}

// iotestHalfReader returns short reads to exercise buffering.
type iotestHalfReader struct{ r io.Reader }

func (h iotestHalfReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:(len(p)+1)/2]
	}
	return h.r.Read(p)
}

type failingReader struct {
	r     io.Reader
	after int
	read  int
}

func (f *failingReader) Read(p []byte) (int, error) {
	if f.read >= f.after {
		return 0, errors.New("client went away")
	}
	n, err := f.r.Read(p[:min(len(p), f.after-f.read)])
	f.read += n
	return n, err
}

func TestStreamUploadFailures(t *testing.T) {
	e := newEnv(t, false, func(c *core.Config) { c.MaxFileSize = 12 * core.MiB })
	data := randData(t, 11*int(core.MiB))
	countKeys := func() int { return len(e.fake.ObjectKeys(bucket)) }

	_, err := e.svc.UploadStream(e.ctx, core.StreamInput{Name: "a", Size: int64(len(data)) + 1}, bytes.NewReader(data))
	wantKind(t, err, core.KindInvalidArgument, "declared size")
	_, err = e.svc.UploadStream(e.ctx, core.StreamInput{Name: "a", Size: int64(len(data)) - 1}, bytes.NewReader(data))
	wantKind(t, err, core.KindInvalidArgument, "larger than the declared size")
	_, err = e.svc.UploadStream(e.ctx, core.StreamInput{Name: "a", Size: 10}, strings.NewReader("short"))
	wantKind(t, err, core.KindInvalidArgument, "declared size")
	_, err = e.svc.UploadStream(e.ctx, core.StreamInput{Name: "a", Size: -1}, bytes.NewReader(randData(t, 13*int(core.MiB))))
	wantKind(t, err, core.KindTooLarge, "exceeds the limit")
	_, err = e.svc.UploadStream(e.ctx, core.StreamInput{Name: "a", Size: -1}, &failingReader{r: bytes.NewReader(data), after: 7 << 20})
	if err == nil || !strings.Contains(err.Error(), "client went away") {
		t.Fatalf("reader failure: %v", err)
	}
	if countKeys() != 0 || e.fake.UploadCount(bucket) != 0 {
		t.Fatalf("failed uploads left %d objects and %d multipart uploads", countKeys(), e.fake.UploadCount(bucket))
	}
	if len(e.events.types()) != 0 {
		t.Fatal("events published for failed uploads")
	}
}

func TestOpenFileRanges(t *testing.T) {
	e := newEnv(t, false)
	data := []byte("0123456789abcdefghij")
	f, err := e.svc.UploadStream(e.ctx, core.StreamInput{Name: "r.txt", Size: -1}, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		off, length int64
		want        string
	}{{0, 0, string(data)}, {5, 0, string(data[5:])}, {5, 3, "567"}, {18, 10, "ij"}, {20, 0, ""}} {
		_, rc, n, err := e.svc.OpenFile(e.ctx, f.ID, tc.off, tc.length)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if string(got) != tc.want || n != int64(len(tc.want)) {
			t.Errorf("OpenFile(%d,%d) = %q (%d), want %q", tc.off, tc.length, got, n, tc.want)
		}
	}
	_, _, _, err = e.svc.OpenFile(e.ctx, f.ID, 21, 0)
	wantKind(t, err, core.KindOutOfRange, "beyond")
	up, _ := e.svc.CreateUpload(e.ctx, core.CreateUploadInput{Name: "p.bin", Size: 6 * core.MiB})
	_, _, _, err = e.svc.OpenFile(e.ctx, up.File.ID, 0, 0)
	wantKind(t, err, core.KindFailedPrecondition, "still uploading")
}

func TestListFiles(t *testing.T) {
	e := newEnv(t, true)
	var ids []string
	for i := 0; i < 5; i++ {
		f, err := e.svc.UploadStream(e.ctx, core.StreamInput{Name: "f.txt", Size: -1}, strings.NewReader("x"))
		if err != nil {
			t.Fatal(err)
		}
		ids = append([]string{f.ID}, ids...)
		time.Sleep(2 * time.Millisecond) // distinct UUIDv7 timestamps
	}
	e.svc.CreateUpload(e.ctx, core.CreateUploadInput{Name: "pending.bin", Size: 6 * core.MiB})
	var got []string
	token := ""
	for {
		files, next, err := e.svc.ListFiles(e.ctx, 2, token, false)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			got = append(got, f.ID)
		}
		if next == "" {
			break
		}
		token = next
	}
	if strings.Join(got, ",") != strings.Join(ids, ",") {
		t.Fatalf("got %v\nwant %v", got, ids)
	}
	all, _, _ := e.svc.ListFiles(e.ctx, 100, "", true)
	if len(all) != 6 {
		t.Fatalf("with pending: %d", len(all))
	}
	_, _, err := e.svc.ListFiles(e.ctx, 10, "garbage!", false)
	wantKind(t, err, core.KindInvalidArgument, "page_token")
}

func TestJanitorRemovesExpiredUploads(t *testing.T) {
	e := newEnv(t, true, func(c *core.Config) { c.UploadExpiry = time.Hour })
	up, err := e.svc.CreateUpload(e.ctx, core.CreateUploadInput{Name: "a.bin", Size: 6 * core.MiB})
	if err != nil {
		t.Fatal(err)
	}
	// Part URLs never outlive the session.
	e.clock.Advance(50 * time.Minute)
	urls, err := e.svc.PartURLs(e.ctx, up.File.ID, []int{1})
	if err != nil || urls[0].ExpiresAt.After(up.ExpiresAt) {
		t.Fatalf("urls: %v %+v", err, urls)
	}
	if n, _ := e.svc.CleanupExpiredUploads(e.ctx); n != 0 {
		t.Fatalf("cleaned %d before expiry", n)
	}
	e.clock.Advance(11 * time.Minute)
	_, err = e.svc.PartURLs(e.ctx, up.File.ID, []int{1})
	wantKind(t, err, core.KindFailedPrecondition, "expired")
	n, err := e.svc.CleanupExpiredUploads(e.ctx)
	if err != nil || n != 1 {
		t.Fatalf("cleanup: %v %d", err, n)
	}
	if _, err := e.svc.GetFile(e.ctx, up.File.ID); !core.IsNotFound(err) {
		t.Fatalf("after cleanup: %v", err)
	}
	if e.fake.UploadCount(bucket) != 0 || len(e.fake.ObjectKeys(bucket)) != 0 {
		t.Fatalf("leftovers: %d uploads, keys %v", e.fake.UploadCount(bucket), e.fake.ObjectKeys(bucket))
	}
}

func TestCompleteRecoversFromConcurrentCompletion(t *testing.T) {
	e := newEnv(t, false)
	data := randData(t, 6*int(core.MiB))
	up, _ := e.svc.CreateUpload(e.ctx, core.CreateUploadInput{Name: "a.bin", Size: int64(len(data))})
	for _, p := range up.Parts {
		put(t, p, data[p.Offset:p.Offset+p.Size])
	}
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = e.svc.CompleteUpload(e.ctx, up.File.ID)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("concurrent complete: %v", err)
		}
	}
	f, _ := e.svc.GetFile(e.ctx, up.File.ID)
	if f.Status != core.StatusAvailable {
		t.Fatalf("status = %s", f.Status)
	}
}

func TestDownloadURLTTL(t *testing.T) {
	e := newEnv(t, false, func(c *core.Config) { c.MaxDownloadURLTTL = time.Hour })
	f, _ := e.svc.UploadStream(e.ctx, core.StreamInput{Name: "a.txt", Size: -1}, strings.NewReader("hello"))
	dl, err := e.svc.DownloadURL(e.ctx, core.DownloadURLInput{FileID: f.ID, TTL: 48 * time.Hour})
	if err != nil || dl.ExpiresAt.Sub(e.clock.Now()) > time.Hour {
		t.Fatalf("ttl not capped: %v %v", err, dl.ExpiresAt.Sub(e.clock.Now()))
	}
	_, err = e.svc.DownloadURL(e.ctx, core.DownloadURLInput{FileID: f.ID, TTL: -time.Second})
	wantKind(t, err, core.KindInvalidArgument, "ttl")
}
