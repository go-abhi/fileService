package rest_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-abhi/fileService/internal/auth"
	"github.com/go-abhi/fileService/internal/core"
	"github.com/go-abhi/fileService/internal/metastore"
	"github.com/go-abhi/fileService/internal/s3"
	"github.com/go-abhi/fileService/internal/s3fake"
	"github.com/go-abhi/fileService/internal/storage"
	"github.com/go-abhi/fileService/internal/transport/rest"
)

const apiKey = "test-api-key-0123456789"

func newServer(t *testing.T) string {
	t.Helper()
	fake := s3fake.New(s3fake.WithCredentials("ak", "sk"))
	fake.CreateBucket("b")
	s3srv := httptest.NewServer(fake)
	t.Cleanup(s3srv.Close)
	c, _ := s3.New(s3.Config{Bucket: "b", Endpoint: s3srv.URL, PathStyle: true, Credentials: s3.StaticCredentials{AccessKeyID: "ak", SecretAccessKey: "sk"}})
	svc, err := core.New(core.Config{DefaultPartSize: 5 * core.MiB, StreamPartSize: 5 * core.MiB},
		core.Deps{Storage: storage.NewS3(c), Metadata: metastore.NewMemory()})
	if err != nil {
		t.Fatal(err)
	}
	keys, _ := auth.ParseAPIKeys("acme:" + apiKey)
	h := rest.NewHandler(svc, rest.Options{Auth: keys, CORSOrigins: []string{"https://app.example.com"},
		Ready: func(context.Context) error { return nil }})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

type call struct {
	t    *testing.T
	base string
}

func (c call) do(method, path string, body io.Reader, hdr map[string]string) (*http.Response, []byte) {
	c.t.Helper()
	req, _ := http.NewRequest(method, c.base+path, body)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func (c call) json(method, path string, in any, want int, out any) {
	c.t.Helper()
	var body io.Reader
	if in != nil {
		b, _ := json.Marshal(in)
		body = bytes.NewReader(b)
	}
	resp, b := c.do(method, path, body, map[string]string{"Content-Type": "application/json"})
	if resp.StatusCode != want {
		c.t.Fatalf("%s %s: status %d, want %d: %s", method, path, resp.StatusCode, want, b)
	}
	if out != nil {
		if err := json.Unmarshal(b, out); err != nil {
			c.t.Fatalf("%s %s: decoding %s: %v", method, path, b, err)
		}
	}
}

func TestDirectUploadFlow(t *testing.T) {
	c := call{t, newServer(t)}
	data := make([]byte, 7<<20)
	rand.Read(data)

	var up rest.Upload
	c.json("POST", "/v1/uploads", map[string]any{"name": "video.mp4", "size": len(data), "metadata": map[string]string{"k": "v"}}, 201, &up)
	if up.PartCount != 2 || len(up.Parts) != 2 || up.File.Status != "pending" || up.File.ContentType != "video/mp4" || up.ExpiresAt == nil {
		t.Fatalf("upload = %+v", up)
	}
	// Browser-style direct upload of each part.
	for _, p := range up.Parts {
		req, _ := http.NewRequest(p.Method, p.URL, bytes.NewReader(data[p.Offset:p.Offset+p.Size]))
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("part %d: %v %v", p.PartNumber, err, resp.Status)
		}
		resp.Body.Close()
	}
	var state rest.Upload
	c.json("GET", "/v1/uploads/"+up.UploadID, nil, 200, &state)
	if len(state.UploadedParts) != 2 {
		t.Fatalf("state = %+v", state)
	}
	var more struct{ Parts []rest.PartURL }
	c.json("POST", "/v1/uploads/"+up.UploadID+"/parts", map[string]any{"part_numbers": []int{2}}, 200, &more)
	if len(more.Parts) != 1 || more.Parts[0].PartNumber != 2 {
		t.Fatalf("parts = %+v", more)
	}
	var f rest.File
	c.json("POST", "/v1/uploads/"+up.UploadID+"/complete", nil, 200, &f)
	if f.Status != "available" || f.Size != int64(len(data)) || f.Metadata["k"] != "v" {
		t.Fatalf("file = %+v", f)
	}

	var dl rest.DownloadURL
	c.json("GET", "/v1/files/"+f.ID+"/url?ttl=5m&disposition=attachment", nil, 200, &dl)
	resp, err := http.Get(dl.URL)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(got, data) || !strings.HasPrefix(resp.Header.Get("Content-Disposition"), "attachment") {
		t.Fatalf("download mismatch (%d bytes, %q)", len(got), resp.Header.Get("Content-Disposition"))
	}

	// /content redirects to the signed URL.
	resp, _ = c.do("GET", "/v1/files/"+f.ID+"/content", nil, nil)
	if resp.StatusCode != http.StatusFound || resp.Header.Get("Location") == "" {
		t.Fatalf("content redirect: %d", resp.StatusCode)
	}
	// ...or streams it with Range support.
	resp, body := c.do("GET", "/v1/files/"+f.ID+"/content?proxy=true", nil, map[string]string{"Range": "bytes=100-199"})
	if resp.StatusCode != 206 || !bytes.Equal(body, data[100:200]) || resp.Header.Get("Content-Range") != fmt.Sprintf("bytes 100-199/%d", len(data)) ||
		resp.Header.Get("X-Content-Type-Options") != "nosniff" || resp.Header.Get("Content-Security-Policy") != "sandbox" {
		t.Fatalf("proxy range: %d %d %q", resp.StatusCode, len(body), resp.Header.Get("Content-Range"))
	}
	resp, body = c.do("GET", "/v1/files/"+f.ID+"/content?proxy=1", nil, map[string]string{"Range": "bytes=-10"})
	if resp.StatusCode != 206 || !bytes.Equal(body, data[len(data)-10:]) {
		t.Fatalf("suffix range: %d", resp.StatusCode)
	}
	resp, _ = c.do("GET", "/v1/files/"+f.ID+"/content?proxy=1", nil, map[string]string{"Range": "bytes=99999999-"})
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("bad range: %d", resp.StatusCode)
	}
	resp, body = c.do("HEAD", "/v1/files/"+f.ID+"/content?proxy=1", nil, nil)
	if resp.StatusCode != 200 || resp.Header.Get("Content-Length") != fmt.Sprint(len(data)) || len(body) != 0 {
		t.Fatalf("HEAD: %d %q", resp.StatusCode, resp.Header.Get("Content-Length"))
	}

	c.json("DELETE", "/v1/files/"+f.ID, nil, 204, nil)
	var e rest.ErrorBody
	c.json("GET", "/v1/files/"+f.ID, nil, 404, &e)
	if e.Error.Code != "not_found" {
		t.Fatalf("error = %+v", e)
	}
}

func TestStreamUploadAndList(t *testing.T) {
	c := call{t, newServer(t)}
	resp, body := c.do("POST", "/v1/files?name=notes.txt&meta.team=infra", strings.NewReader("hello world"), map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
	if resp.StatusCode != 201 {
		t.Fatalf("stream upload: %d %s", resp.StatusCode, body)
	}
	var f rest.File
	json.Unmarshal(body, &f)
	if f.Name != "notes.txt" || f.ContentType != "text/plain; charset=utf-8" || f.Size != 11 || f.SHA256 == "" || f.Metadata["team"] != "infra" {
		t.Fatalf("file = %+v", f)
	}
	if resp.Header.Get("Location") != "/v1/files/"+f.ID {
		t.Fatalf("Location = %q", resp.Header.Get("Location"))
	}
	resp, body = c.do("POST", "/v1/files", strings.NewReader("x"), map[string]string{"X-File-Name": "r%C3%A9sum%C3%A9.pdf"})
	if resp.StatusCode != 201 {
		t.Fatalf("X-File-Name upload: %d %s", resp.StatusCode, body)
	}
	json.Unmarshal(body, &f)
	if f.Name != "résumé.pdf" {
		t.Fatalf("name = %q", f.Name)
	}
	var list struct {
		Files         []rest.File `json:"files"`
		NextPageToken string      `json:"next_page_token"`
	}
	c.json("GET", "/v1/files?page_size=1", nil, 200, &list)
	if len(list.Files) != 1 || list.NextPageToken == "" {
		t.Fatalf("page 1 = %+v", list)
	}
	token := list.NextPageToken
	list.Files, list.NextPageToken = nil, ""
	c.json("GET", "/v1/files?page_size=1&page_token="+token, nil, 200, &list)
	if len(list.Files) != 1 || list.Files[0].Name != "notes.txt" || list.NextPageToken != "" {
		t.Fatalf("page 2 = %+v", list)
	}
	resp, body = c.do("POST", "/v1/files", strings.NewReader("x"), nil)
	if resp.StatusCode != 400 || !strings.Contains(string(body), "name is required") {
		t.Fatalf("missing name: %d %s", resp.StatusCode, body)
	}
}

func TestAuthErrorsAndCORS(t *testing.T) {
	base := newServer(t)
	resp, err := http.Get(base + "/v1/files")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 401 || !strings.Contains(string(b), `"unauthenticated"`) || resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatalf("unauthenticated: %d %s", resp.StatusCode, b)
	}
	req, _ := http.NewRequest("GET", base+"/v1/files", nil)
	req.Header.Set("X-Api-Key", "wrong-key-0123456789")
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("wrong key: %d", resp.StatusCode)
	}
	for _, p := range []string{"/healthz", "/readyz"} {
		resp, _ := http.Get(base + p)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("%s: %d", p, resp.StatusCode)
		}
	}
	req, _ = http.NewRequest("OPTIONS", base+"/v1/uploads", nil)
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 204 || resp.Header.Get("Access-Control-Allow-Origin") != "https://app.example.com" ||
		!strings.Contains(resp.Header.Get("Access-Control-Allow-Headers"), "Authorization") {
		t.Fatalf("preflight: %d %v", resp.StatusCode, resp.Header)
	}
	req.Header.Set("Origin", "https://evil.example.com")
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.Header.Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("disallowed origin got CORS headers")
	}

	c := call{t, base}
	var e rest.ErrorBody
	resp, body := c.do("POST", "/v1/uploads", strings.NewReader("{not json"), nil)
	json.Unmarshal(body, &e)
	if resp.StatusCode != 400 || e.Error.Code != "invalid_argument" {
		t.Fatalf("bad json: %d %s", resp.StatusCode, body)
	}
	c.json("POST", "/v1/uploads", map[string]any{"name": "a", "size": int64(6) << 40}, 413, &e)
	c.json("GET", "/v1/files/not-a-uuid", nil, 400, &e)
	c.json("GET", "/v1/nope", nil, 404, &e)
}
