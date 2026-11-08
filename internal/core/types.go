// Package core implements the file service's business logic independently
// of transports (gRPC, REST) and infrastructure (S3, metadata stores, CDN),
// which plug in through the interfaces in ports.go.
package core

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"
)

// Status is the lifecycle state of a file.
type Status string

// File states.
const (
	StatusPending   Status = "pending"
	StatusAvailable Status = "available"
)

// File is a stored (or in-progress) file.
type File struct {
	ID          string            `json:"id"`
	Namespace   string            `json:"namespace"`
	Name        string            `json:"name"`
	ContentType string            `json:"content_type"`
	Size        int64             `json:"size"`
	SHA256      string            `json:"sha256,omitempty"`
	ETag        string            `json:"etag,omitempty"`
	Status      Status            `json:"status"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	// Upload is set while the file is pending a direct upload.
	Upload *UploadSession `json:"upload,omitempty"`
}

// UploadSession tracks a direct-to-storage multipart upload.
type UploadSession struct {
	StorageUploadID string    `json:"storage_upload_id"`
	PartSize        int64     `json:"part_size"`
	PartCount       int       `json:"part_count"`
	ExpiresAt       time.Time `json:"expires_at"`
}

// Clone returns a deep copy of f.
func (f *File) Clone() *File {
	if f == nil {
		return nil
	}
	c := *f
	c.Metadata = maps.Clone(f.Metadata)
	if f.Upload != nil {
		u := *f.Upload
		c.Upload = &u
	}
	return &c
}

// Principal is the authenticated caller. All operations are scoped to its
// namespace.
type Principal struct {
	Namespace string
	// Subject identifies the caller (API key name, user...) for audit logs.
	Subject string
}

type principalKey struct{}

// WithPrincipal returns a context carrying p.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// PrincipalFrom returns the principal carried by ctx.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

// Event types published to EventPublisher.
const (
	EventFileUploaded = "file.uploaded"
	EventFileDeleted  = "file.deleted"
)

// Event notifies other services about file changes.
type Event struct {
	ID   string    `json:"id"`
	Type string    `json:"type"`
	Time time.Time `json:"time"`
	File *File     `json:"file"`
}

// --- errors ------------------------------------------------------------------------

// Kind classifies errors so transports can map them to status codes.
type Kind int

// Error kinds.
const (
	KindInternal Kind = iota
	KindInvalidArgument
	KindNotFound
	KindAlreadyExists
	KindFailedPrecondition
	KindOutOfRange
	KindTooLarge
	KindUnauthenticated
	KindPermissionDenied
	KindUnavailable
)

func (k Kind) String() string {
	switch k {
	case KindInvalidArgument:
		return "invalid_argument"
	case KindNotFound:
		return "not_found"
	case KindAlreadyExists:
		return "already_exists"
	case KindFailedPrecondition:
		return "failed_precondition"
	case KindOutOfRange:
		return "out_of_range"
	case KindTooLarge:
		return "too_large"
	case KindUnauthenticated:
		return "unauthenticated"
	case KindPermissionDenied:
		return "permission_denied"
	case KindUnavailable:
		return "unavailable"
	}
	return "internal"
}

// Error is a classified error.
type Error struct {
	Kind Kind
	Msg  string
	Err  error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return e.Msg + ": " + e.Err.Error()
	}
	return e.Msg
}

func (e *Error) Unwrap() error { return e.Err }

// Errorf returns a classified error.
func Errorf(kind Kind, format string, args ...any) *Error {
	return &Error{Kind: kind, Msg: fmt.Sprintf(format, args...)}
}

// KindOf returns the kind of err (KindInternal for unclassified errors).
func KindOf(err error) Kind {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return KindInternal
}

// IsNotFound reports whether err is a not-found error.
func IsNotFound(err error) bool { return err != nil && KindOf(err) == KindNotFound }

// ErrNoSuchUpload is returned (wrapped) by ObjectStorage when a multipart
// upload does not exist any more.
var ErrNoSuchUpload = errors.New("multipart upload does not exist")
