package metastore_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-abhi/fileService/internal/core"
	"github.com/go-abhi/fileService/internal/metastore"
	"github.com/go-abhi/fileService/internal/metastore/storetest"
	"github.com/go-abhi/fileService/internal/s3"
	"github.com/go-abhi/fileService/internal/s3fake"
)

func TestMemory(t *testing.T) {
	storetest.Run(t, func(*testing.T) core.MetadataStore { return metastore.NewMemory() })
}

func newS3Store(t *testing.T, pageSize int) (*metastore.S3, *s3fake.Server) {
	fake := s3fake.New(s3fake.WithCredentials("ak", "sk"), s3fake.WithPageSize(pageSize))
	fake.CreateBucket("b")
	ts := httptest.NewServer(fake)
	t.Cleanup(ts.Close)
	c, err := s3.New(s3.Config{Bucket: "b", Endpoint: ts.URL, PathStyle: true, Credentials: s3.StaticCredentials{AccessKeyID: "ak", SecretAccessKey: "sk"}})
	if err != nil {
		t.Fatal(err)
	}
	return metastore.NewS3(c, "files"), fake
}

func TestS3(t *testing.T) {
	storetest.Run(t, func(t *testing.T) core.MetadataStore {
		s, _ := newS3Store(t, 1000)
		return s
	})
}

// Small S3 list pages exercise the store's continuation logic.
func TestS3SmallPages(t *testing.T) {
	storetest.Run(t, func(t *testing.T) core.MetadataStore {
		s, _ := newS3Store(t, 2)
		return s
	})
}

func TestS3Layout(t *testing.T) {
	s, fake := newS3Store(t, 1000)
	f := &core.File{ID: storetest.IDAt(1_700_000_000_000, 1), Namespace: "ns", Name: "a", Status: core.StatusPending, Upload: &core.UploadSession{StorageUploadID: "u"}}
	if err := s.Create(t.Context(), f); err != nil {
		t.Fatal(err)
	}
	keys := strings.Join(fake.ObjectKeys("b"), " ")
	if !strings.Contains(keys, "files/_meta/ns/") || !strings.Contains(keys, "files/_pending/ns/"+f.ID) {
		t.Fatalf("unexpected layout: %s", keys)
	}
	f.Status, f.Upload = core.StatusAvailable, nil
	if err := s.Put(t.Context(), f); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(fake.ObjectKeys("b"), " "), "_pending") {
		t.Fatal("pending marker not removed")
	}
}
