package core

import (
	"context"
	"io"
	"time"
)

// ObjectAttrs are stored with an object.
type ObjectAttrs struct {
	ContentType        string
	ContentDisposition string
	CacheControl       string
	// Metadata must be ASCII (it is stored as S3 user metadata).
	Metadata map[string]string
}

// ObjectInfo describes a stored object.
type ObjectInfo struct {
	Size         int64
	ETag         string
	ContentType  string
	LastModified time.Time
}

// StoredPart is an uploaded part of a multipart upload.
type StoredPart struct {
	Number int
	Size   int64
	ETag   string
}

// SignedURL is a presigned or CDN-signed request.
type SignedURL struct {
	URL    string
	Method string
	// Headers that must accompany the request (besides Content-Length).
	Headers   map[string]string
	ExpiresAt time.Time
}

// DownloadOptions customize a download URL.
type DownloadOptions struct {
	// ContentDisposition overrides the stored Content-Disposition.
	ContentDisposition string
}

// ObjectStorage is the blob store (S3 or compatible). Missing objects are
// reported as KindNotFound errors and missing multipart uploads as errors
// wrapping ErrNoSuchUpload.
type ObjectStorage interface {
	CreateMultipartUpload(ctx context.Context, key string, attrs ObjectAttrs) (uploadID string, err error)
	PresignUploadPart(ctx context.Context, key, uploadID string, part int, size int64, ttl time.Duration) (SignedURL, error)
	UploadPart(ctx context.Context, key, uploadID string, part int, data []byte) (etag string, err error)
	ListParts(ctx context.Context, key, uploadID string) ([]StoredPart, error)
	CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []StoredPart) error
	AbortMultipartUpload(ctx context.Context, key, uploadID string) error

	PutObject(ctx context.Context, key string, data []byte, attrs ObjectAttrs) (etag string, err error)
	HeadObject(ctx context.Context, key string) (ObjectInfo, error)
	// GetObject returns length bytes from offset (length 0: to the end).
	GetObject(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
	DeleteObject(ctx context.Context, key string) error
	PresignGetObject(ctx context.Context, key string, ttl time.Duration, opts DownloadOptions) (SignedURL, error)
}

// URLSigner produces download URLs, e.g. CloudFront signed URLs.
type URLSigner interface {
	SignURL(ctx context.Context, key string, ttl time.Duration, opts DownloadOptions) (SignedURL, error)
	// CDN reports whether URLs are served by a CDN.
	CDN() bool
}

// ListQuery selects files to list.
type ListQuery struct {
	Namespace string
	Limit     int
	// After is the ID of the last file of the previous page.
	After          string
	IncludePending bool
}

// MetadataStore persists file records. Implementations must return copies
// (callers may modify the returned files) and report missing records as
// KindNotFound errors.
type MetadataStore interface {
	// Create stores a new record; KindAlreadyExists if the ID is taken.
	Create(ctx context.Context, f *File) error
	Get(ctx context.Context, namespace, id string) (*File, error)
	// Put creates or replaces a record.
	Put(ctx context.Context, f *File) error
	// Delete removes a record; deleting a missing record is not an error.
	Delete(ctx context.Context, namespace, id string) error
	// List returns files newest first and the cursor for the next page ("" at the end).
	List(ctx context.Context, q ListQuery) ([]*File, string, error)
	// ListExpiredUploads returns pending uploads whose session expired before now.
	ListExpiredUploads(ctx context.Context, now time.Time, limit int) ([]*File, error)
}

// EventPublisher delivers events to other services. Implementations should
// not block; delivery is at-least-once.
type EventPublisher interface {
	Publish(ctx context.Context, e Event)
}

// StorageSigner adapts ObjectStorage presigned GETs to URLSigner.
type StorageSigner struct{ Storage ObjectStorage }

// SignURL implements URLSigner.
func (s StorageSigner) SignURL(ctx context.Context, key string, ttl time.Duration, opts DownloadOptions) (SignedURL, error) {
	return s.Storage.PresignGetObject(ctx, key, ttl, opts)
}

// CDN implements URLSigner.
func (StorageSigner) CDN() bool { return false }

type noopPublisher struct{}

func (noopPublisher) Publish(context.Context, Event) {}
