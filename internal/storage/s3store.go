// Package storage adapts the S3 client to core.ObjectStorage.
package storage

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/go-abhi/fileService/internal/core"
	"github.com/go-abhi/fileService/internal/s3"
)

// S3 implements core.ObjectStorage on an S3 bucket.
type S3 struct{ c *s3.Client }

// NewS3 wraps an S3 client.
func NewS3(c *s3.Client) *S3 { return &S3{c: c} }

// Client returns the underlying client.
func (s *S3) Client() *s3.Client { return s.c }

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	switch code := s3.ErrorCode(err); {
	case s3.IsNotFound(err):
		return &core.Error{Kind: core.KindNotFound, Msg: "object not found", Err: err}
	case s3.IsNoSuchUpload(err):
		return fmt.Errorf("%w: %v", core.ErrNoSuchUpload, err)
	case code == "InvalidRange":
		return &core.Error{Kind: core.KindOutOfRange, Msg: "invalid range", Err: err}
	case code == "SlowDown" || code == "ServiceUnavailable" || code == "RequestTimeout":
		return &core.Error{Kind: core.KindUnavailable, Msg: "storage is temporarily unavailable", Err: err}
	}
	return err
}

func headers(a core.ObjectAttrs) s3.ObjectHeaders {
	return s3.ObjectHeaders{ContentType: a.ContentType, ContentDisposition: a.ContentDisposition, CacheControl: a.CacheControl, Metadata: a.Metadata}
}

// CreateMultipartUpload implements core.ObjectStorage.
func (s *S3) CreateMultipartUpload(ctx context.Context, key string, attrs core.ObjectAttrs) (string, error) {
	id, err := s.c.CreateMultipartUpload(ctx, key, headers(attrs))
	return id, mapErr(err)
}

// PresignUploadPart implements core.ObjectStorage.
func (s *S3) PresignUploadPart(ctx context.Context, key, uploadID string, part int, size int64, ttl time.Duration) (core.SignedURL, error) {
	u, exp, err := s.c.PresignUploadPart(ctx, key, uploadID, part, size, ttl)
	if err != nil {
		return core.SignedURL{}, err
	}
	return core.SignedURL{URL: u, Method: http.MethodPut, ExpiresAt: exp}, nil
}

// UploadPart implements core.ObjectStorage.
func (s *S3) UploadPart(ctx context.Context, key, uploadID string, part int, data []byte) (string, error) {
	etag, err := s.c.UploadPart(ctx, key, uploadID, part, data)
	return etag, mapErr(err)
}

// ListParts implements core.ObjectStorage.
func (s *S3) ListParts(ctx context.Context, key, uploadID string) ([]core.StoredPart, error) {
	parts, err := s.c.ListParts(ctx, key, uploadID)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]core.StoredPart, len(parts))
	for i, p := range parts {
		out[i] = core.StoredPart{Number: p.PartNumber, Size: p.Size, ETag: p.ETag}
	}
	return out, nil
}

// CompleteMultipartUpload implements core.ObjectStorage.
func (s *S3) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []core.StoredPart) error {
	cp := make([]s3.CompletedPart, len(parts))
	for i, p := range parts {
		cp[i] = s3.CompletedPart{PartNumber: p.Number, ETag: p.ETag}
	}
	_, err := s.c.CompleteMultipartUpload(ctx, key, uploadID, cp)
	return mapErr(err)
}

// AbortMultipartUpload implements core.ObjectStorage.
func (s *S3) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	return mapErr(s.c.AbortMultipartUpload(ctx, key, uploadID))
}

// PutObject implements core.ObjectStorage.
func (s *S3) PutObject(ctx context.Context, key string, data []byte, attrs core.ObjectAttrs) (string, error) {
	etag, err := s.c.PutObject(ctx, key, data, headers(attrs))
	return etag, mapErr(err)
}

// HeadObject implements core.ObjectStorage.
func (s *S3) HeadObject(ctx context.Context, key string) (core.ObjectInfo, error) {
	info, err := s.c.HeadObject(ctx, key)
	if err != nil {
		return core.ObjectInfo{}, mapErr(err)
	}
	return core.ObjectInfo{Size: info.Size, ETag: info.ETag, ContentType: info.ContentType, LastModified: info.LastModified}, nil
}

// GetObject implements core.ObjectStorage.
func (s *S3) GetObject(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	out, err := s.c.GetObject(ctx, key, offset, length)
	if err != nil {
		return nil, mapErr(err)
	}
	return out.Body, nil
}

// DeleteObject implements core.ObjectStorage.
func (s *S3) DeleteObject(ctx context.Context, key string) error {
	return mapErr(s.c.DeleteObject(ctx, key))
}

// PresignGetObject implements core.ObjectStorage.
func (s *S3) PresignGetObject(ctx context.Context, key string, ttl time.Duration, opts core.DownloadOptions) (core.SignedURL, error) {
	var params url.Values
	if opts.ContentDisposition != "" {
		params = url.Values{"response-content-disposition": {opts.ContentDisposition}}
	}
	u, exp, err := s.c.PresignGetObject(ctx, key, ttl, params)
	if err != nil {
		return core.SignedURL{}, err
	}
	return core.SignedURL{URL: u, Method: http.MethodGet, ExpiresAt: exp}, nil
}

// Ping checks that the bucket is reachable (readiness probes).
func (s *S3) Ping(ctx context.Context) error { return s.c.HeadBucket(ctx) }
