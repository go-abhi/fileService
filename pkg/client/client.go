// Package client is the Go SDK for the file service. It speaks gRPC to the
// service and transfers file content directly to/from storage or the CDN:
//
//	c, err := client.New("fileservice:9090", client.WithAPIKey(key), client.WithInsecure())
//	f, err := c.UploadFile(ctx, "video.mp4", client.UploadOptions{})   // parallel, resumable
//	_, err = c.DownloadFile(ctx, f.ID, "copy.mp4", client.DownloadOptions{}) // parallel ranges
//
// The typed RPC methods (CreateUpload, GetFile, ...) are also available for
// custom flows.
package client

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"time"

	v1 "github.com/go-abhi/fileService/api/fileservice/v1"
	"github.com/go-abhi/fileService/internal/grpcx"
	"github.com/go-abhi/fileService/internal/s3"
)

// Code is a gRPC status code.
type Code = grpcx.Code

// Status is a gRPC status error returned by calls.
type Status = grpcx.Status

// Status codes returned by the service.
const (
	OK                 = grpcx.OK
	Canceled           = grpcx.Canceled
	Unknown            = grpcx.Unknown
	InvalidArgument    = grpcx.InvalidArgument
	DeadlineExceeded   = grpcx.DeadlineExceeded
	NotFound           = grpcx.NotFound
	AlreadyExists      = grpcx.AlreadyExists
	PermissionDenied   = grpcx.PermissionDenied
	ResourceExhausted  = grpcx.ResourceExhausted
	FailedPrecondition = grpcx.FailedPrecondition
	OutOfRange         = grpcx.OutOfRange
	Unimplemented      = grpcx.Unimplemented
	Internal           = grpcx.Internal
	Unavailable        = grpcx.Unavailable
	Unauthenticated    = grpcx.Unauthenticated
)

// CodeOf returns the status code of err (OK for nil).
func CodeOf(err error) Code { return grpcx.CodeOf(err) }

type options struct {
	apiKey     string
	insecure   bool
	tlsConfig  *tls.Config
	transfer   *http.Client
	userAgent  string
	maxRetries int
}

// Option configures a Client.
type Option func(*options)

// WithAPIKey authenticates calls with an API key.
func WithAPIKey(key string) Option { return func(o *options) { o.apiKey = key } }

// WithInsecure connects without TLS (h2c), e.g. inside a cluster.
func WithInsecure() Option { return func(o *options) { o.insecure = true } }

// WithTLSConfig sets TLS options for the gRPC connection.
func WithTLSConfig(cfg *tls.Config) Option { return func(o *options) { o.tlsConfig = cfg } }

// WithTransferClient sets the HTTP client used to move file content to and
// from storage / the CDN.
func WithTransferClient(hc *http.Client) Option { return func(o *options) { o.transfer = hc } }

// WithUserAgent sets the user agent.
func WithUserAgent(ua string) Option { return func(o *options) { o.userAgent = ua } }

// WithMaxRetries sets how many times a failed part or chunk transfer is
// retried (default 5).
func WithMaxRetries(n int) Option { return func(o *options) { o.maxRetries = n } }

// Client is a file service client. It is safe for concurrent use.
type Client struct {
	RPC
	conn     *grpcx.ClientConn
	transfer *http.Client
	retries  int
}

// New returns a client for the service at addr ("host:port",
// "http://host:port" or "https://host:port").
func New(addr string, opts ...Option) (*Client, error) {
	o := options{userAgent: "fileservice-go/1", maxRetries: 5}
	for _, fn := range opts {
		fn(&o)
	}
	dial := []grpcx.DialOption{grpcx.WithUserAgent(o.userAgent)}
	if o.insecure {
		dial = append(dial, grpcx.WithInsecure())
	}
	if o.tlsConfig != nil {
		dial = append(dial, grpcx.WithTLSConfig(o.tlsConfig))
	}
	if o.apiKey != "" {
		dial = append(dial, grpcx.WithMetadata("Authorization", "Bearer "+o.apiKey))
	}
	conn, err := grpcx.Dial(addr, dial...)
	if err != nil {
		return nil, err
	}
	if o.transfer == nil {
		o.transfer = &http.Client{Transport: s3.DefaultTransport()}
	}
	return &Client{RPC: RPC{cc: conn}, conn: conn, transfer: o.transfer, retries: max(o.maxRetries, 1)}, nil
}

// Close releases idle connections.
func (c *Client) Close() error {
	c.transfer.CloseIdleConnections()
	return c.conn.Close()
}

// RPC exposes the raw FileService RPCs.
type RPC struct{ cc *grpcx.ClientConn }

func invoke[Resp any, P interface {
	*Resp
	grpcx.Message
}](ctx context.Context, cc *grpcx.ClientConn, method string, req grpcx.Message) (P, error) {
	resp := P(new(Resp))
	if err := cc.Invoke(ctx, method, req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// CreateUpload starts a direct upload.
func (r RPC) CreateUpload(ctx context.Context, req *v1.CreateUploadRequest) (*v1.CreateUploadResponse, error) {
	return invoke[v1.CreateUploadResponse](ctx, r.cc, v1.MethodCreateUpload, req)
}

// GetUploadPartURLs returns presigned URLs for parts.
func (r RPC) GetUploadPartURLs(ctx context.Context, req *v1.GetUploadPartURLsRequest) (*v1.GetUploadPartURLsResponse, error) {
	return invoke[v1.GetUploadPartURLsResponse](ctx, r.cc, v1.MethodGetUploadPartURLs, req)
}

// GetUpload returns the state of an upload.
func (r RPC) GetUpload(ctx context.Context, req *v1.GetUploadRequest) (*v1.GetUploadResponse, error) {
	return invoke[v1.GetUploadResponse](ctx, r.cc, v1.MethodGetUpload, req)
}

// CompleteUpload assembles an upload.
func (r RPC) CompleteUpload(ctx context.Context, req *v1.CompleteUploadRequest) (*v1.CompleteUploadResponse, error) {
	return invoke[v1.CompleteUploadResponse](ctx, r.cc, v1.MethodCompleteUpload, req)
}

// AbortUpload cancels an upload.
func (r RPC) AbortUpload(ctx context.Context, req *v1.AbortUploadRequest) (*v1.AbortUploadResponse, error) {
	return invoke[v1.AbortUploadResponse](ctx, r.cc, v1.MethodAbortUpload, req)
}

// GetDownloadURL returns a signed download URL.
func (r RPC) GetDownloadURL(ctx context.Context, req *v1.GetDownloadURLRequest) (*v1.GetDownloadURLResponse, error) {
	return invoke[v1.GetDownloadURLResponse](ctx, r.cc, v1.MethodGetDownloadURL, req)
}

// GetFile returns file metadata.
func (r RPC) GetFile(ctx context.Context, req *v1.GetFileRequest) (*v1.GetFileResponse, error) {
	return invoke[v1.GetFileResponse](ctx, r.cc, v1.MethodGetFile, req)
}

// ListFiles lists files.
func (r RPC) ListFiles(ctx context.Context, req *v1.ListFilesRequest) (*v1.ListFilesResponse, error) {
	return invoke[v1.ListFilesResponse](ctx, r.cc, v1.MethodListFiles, req)
}

// DeleteFile deletes a file.
func (r RPC) DeleteFile(ctx context.Context, req *v1.DeleteFileRequest) (*v1.DeleteFileResponse, error) {
	return invoke[v1.DeleteFileResponse](ctx, r.cc, v1.MethodDeleteFile, req)
}

// Stat returns the metadata of a file.
func (c *Client) Stat(ctx context.Context, id string) (*v1.File, error) {
	resp, err := c.GetFile(ctx, &v1.GetFileRequest{FileID: id})
	if err != nil {
		return nil, err
	}
	return resp.File, nil
}

// Delete deletes a file.
func (c *Client) Delete(ctx context.Context, id string) error {
	_, err := c.DeleteFile(ctx, &v1.DeleteFileRequest{FileID: id})
	return err
}

// URL returns a signed download URL valid for ttl (0: server default).
func (c *Client) URL(ctx context.Context, id string, ttl time.Duration, attachment bool) (*v1.GetDownloadURLResponse, error) {
	d := v1.ContentDispositionInline
	if attachment {
		d = v1.ContentDispositionAttachment
	}
	var resp *v1.GetDownloadURLResponse
	err := retryRPC(ctx, func() (err error) {
		resp, err = c.GetDownloadURL(ctx, &v1.GetDownloadURLRequest{FileID: id, TTL: ttl, Disposition: d})
		return err
	})
	return resp, err
}

// retryRPC retries idempotent calls on transient failures.
func retryRPC(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if err = fn(); err == nil || CodeOf(err) != Unavailable {
			return err
		}
		if err := sleep(ctx, time.Duration(200<<attempt)*time.Millisecond); err != nil {
			return err
		}
	}
	return err
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// UploadError reports a failed direct upload. Unless the upload was
// aborted, it can be resumed with ResumeUpload(UploadID).
type UploadError struct {
	UploadID string
	Aborted  bool
	Err      error
}

func (e *UploadError) Error() string {
	if e.Aborted {
		return "upload " + e.UploadID + " failed (aborted): " + e.Err.Error()
	}
	return "upload " + e.UploadID + " failed (resumable): " + e.Err.Error()
}

func (e *UploadError) Unwrap() error { return e.Err }

var errStop = errors.New("stopped")
