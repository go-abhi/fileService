package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	v1 "github.com/go-abhi/fileService/api/fileservice/v1"
	"github.com/go-abhi/fileService/internal/core"
	"github.com/go-abhi/fileService/pkg/client"
)

// TestIntegrationS3 runs the full flow against a real S3-compatible store
// (AWS S3, MinIO...). It is skipped unless FILESERVICE_TEST_S3_ENDPOINT is
// set; credentials come from the standard AWS environment variables. CI runs
// it against MinIO:
//
//	FILESERVICE_TEST_S3_ENDPOINT=http://localhost:9000 \
//	AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
//	go test -run Integration ./internal/app/
func TestIntegrationS3(t *testing.T) {
	endpoint := os.Getenv("FILESERVICE_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set FILESERVICE_TEST_S3_ENDPOINT to run against a real S3-compatible store")
	}
	suffix := make([]byte, 4)
	rand.Read(suffix)
	bucket := os.Getenv("FILESERVICE_TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "fileservice-it-" + hex.EncodeToString(suffix)
	}
	a := startApp(t, func(c *Config) {
		c.Dev = false
		c.S3Bucket = bucket
		c.S3Endpoint = endpoint
		c.S3PathStyle = true
		c.S3CreateBucket = true
		c.S3Region = os.Getenv("AWS_REGION")
		if c.S3Region == "" {
			c.S3Region = "us-east-1"
		}
		c.Core.KeyPrefix = "it-" + hex.EncodeToString(suffix)
		c.StartupRetryLimit = 30 * time.Second
	})
	ctx := context.Background()
	c := newClient(t, a.GRPCAddr(), acmeKey)

	data := randBytes(13*int(core.MiB) + 99)
	f, err := c.Upload(ctx, bytes.NewReader(data), int64(len(data)), client.UploadOptions{Name: "integration.bin", PartSize: 5 * core.MiB})
	if err != nil {
		t.Fatalf("direct upload: %v", err)
	}
	var out memWriterAt
	if _, err := c.Download(ctx, f.ID, &out, client.DownloadOptions{ChunkSize: 4 << 20}); err != nil || !bytes.Equal(out.buf, data) {
		t.Fatalf("parallel download: %v", err)
	}
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
	if resp.StatusCode != 200 || !bytes.Equal(body, data) {
		t.Fatalf("presigned GET: %d, %d bytes", resp.StatusCode, len(body))
	}

	s, err := c.UploadStream(ctx, bytes.NewReader(data), client.StreamOptions{Name: "streamed.bin", Size: -1})
	if err != nil {
		t.Fatalf("stream upload: %v", err)
	}
	var sb bytes.Buffer
	if _, err := c.DownloadStream(ctx, s.ID, &sb, 0, 0); err != nil || !bytes.Equal(sb.Bytes(), data) {
		t.Fatalf("stream download: %v", err)
	}

	list, err := c.ListFiles(ctx, &v1.ListFilesRequest{PageSize: 10})
	if err != nil || len(list.Files) != 2 || list.Files[0].ID != s.ID {
		t.Fatalf("list: %v %+v", err, list)
	}
	for _, id := range []string{f.ID, s.ID} {
		if err := c.Delete(ctx, id); err != nil {
			t.Fatalf("delete: %v", err)
		}
	}
}
