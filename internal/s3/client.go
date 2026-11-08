// Package s3 is a small, dependency-free Amazon S3 client covering the
// operations the file service needs: multipart uploads, ranged downloads,
// listing, presigned URLs and bucket bootstrap. It works with AWS S3 and
// S3-compatible stores such as MinIO.
package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-abhi/fileService/internal/s3/sigv4"
)

const userAgent = "fileservice/1 (+https://github.com/go-abhi/fileService)"

// Config configures a Client.
type Config struct {
	Bucket string
	Region string
	// Endpoint overrides the AWS endpoint, e.g. "http://minio:9000".
	Endpoint string
	// PresignEndpoint, when set, is used for presigned URLs instead of
	// Endpoint. Use it when clients reach storage at a different address than
	// the service does (e.g. docker-compose: service -> minio:9000, browser ->
	// localhost:9000).
	PresignEndpoint string
	// PathStyle addresses objects as endpoint/bucket/key instead of
	// bucket.endpoint/key. Required by most S3-compatible stores.
	PathStyle bool
	// Accelerate makes presigned upload URLs use S3 Transfer Acceleration
	// (bucket.s3-accelerate.amazonaws.com). The bucket must have it enabled.
	Accelerate bool
	// ServerSideEncryption is "", "AES256" or "aws:kms".
	ServerSideEncryption string
	SSEKMSKeyID          string

	Credentials CredentialsProvider
	HTTPClient  *http.Client
	// MaxAttempts bounds retries of throttled/failed requests (default 4).
	MaxAttempts int
	Now         func() time.Time
}

// Client is an S3 client bound to one bucket. It is safe for concurrent use.
type Client struct {
	cfg       Config
	signer    sigv4.Signer
	hc        *http.Client
	api       endpoint
	presign   endpoint
	uploadURL endpoint
	attempts  int
	now       func() time.Time
}

// New validates cfg and returns a Client.
func New(cfg Config) (*Client, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3: bucket is required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.Credentials == nil {
		return nil, errors.New("s3: credentials provider is required")
	}
	c := &Client{
		cfg:      cfg,
		signer:   sigv4.Signer{Service: "s3", Region: cfg.Region},
		attempts: cfg.MaxAttempts,
		now:      cfg.Now,
	}
	if c.attempts <= 0 {
		c.attempts = 4
	}
	if c.now == nil {
		c.now = time.Now
	}

	base := cfg.HTTPClient
	if base == nil {
		base = &http.Client{Transport: DefaultTransport()}
	}
	hc := *base
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c.hc = &hc

	// Dotted bucket names break TLS certificate matching with virtual hosts.
	pathStyle := cfg.PathStyle || strings.Contains(cfg.Bucket, ".")
	var err error
	if cfg.Endpoint != "" {
		if c.api, err = parseEndpoint(cfg.Endpoint, pathStyle); err != nil {
			return nil, err
		}
	} else {
		c.api = endpoint{scheme: "https", host: "s3." + cfg.Region + "." + dnsSuffix(cfg.Region), pathStyle: pathStyle}
	}
	c.presign = c.api
	if cfg.PresignEndpoint != "" {
		if c.presign, err = parseEndpoint(cfg.PresignEndpoint, pathStyle); err != nil {
			return nil, err
		}
	}
	c.uploadURL = c.presign
	if cfg.Accelerate {
		if cfg.Endpoint != "" || cfg.PresignEndpoint != "" || strings.Contains(cfg.Bucket, ".") {
			return nil, errors.New("s3: transfer acceleration requires the AWS endpoint and a bucket name without dots")
		}
		c.uploadURL = endpoint{scheme: "https", host: "s3-accelerate.amazonaws.com"}
	}
	return c, nil
}

// DefaultTransport returns an HTTP transport tuned for many parallel
// requests to the same host.
func DefaultTransport() *http.Transport {
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   128,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 2 * time.Minute,
		ExpectContinueTimeout: time.Second,
		ForceAttemptHTTP2:     true,
	}
}

// Bucket returns the bucket name.
func (c *Client) Bucket() string { return c.cfg.Bucket }

func dnsSuffix(region string) string {
	if strings.HasPrefix(region, "cn-") {
		return "amazonaws.com.cn"
	}
	return "amazonaws.com"
}

type endpoint struct {
	scheme, host, basePath string
	pathStyle              bool
}

func parseEndpoint(raw string, pathStyle bool) (endpoint, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return endpoint{}, fmt.Errorf("s3: invalid endpoint %q (want scheme://host[:port])", raw)
	}
	return endpoint{scheme: u.Scheme, host: u.Host, basePath: strings.TrimRight(u.Path, "/"), pathStyle: pathStyle}, nil
}

// url returns the URL of key in bucket; an empty key addresses the bucket.
func (e endpoint) url(bucket, key string) *url.URL {
	var host, p string
	if e.pathStyle {
		host = e.host
		p = e.basePath + "/" + bucket
		if key != "" {
			p += "/" + key
		}
	} else {
		host = bucket + "." + e.host
		p = e.basePath + "/" + key
	}
	return &url.URL{Scheme: e.scheme, Host: host, Path: p, RawPath: sigv4.EscapePath(p)}
}

// --- errors -------------------------------------------------------------------

// Error is an error returned by S3.
type Error struct {
	StatusCode int
	Code       string
	Message    string
	RequestID  string
}

func (e *Error) Error() string {
	msg := "s3: " + e.Code
	if e.Message != "" {
		msg += ": " + e.Message
	}
	return msg + " (status " + strconv.Itoa(e.StatusCode) + ")"
}

// ErrorCode returns the S3 error code of err, or "".
func ErrorCode(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

// IsNotFound reports whether err means the object does not exist.
func IsNotFound(err error) bool {
	c := ErrorCode(err)
	return c == "NoSuchKey" || c == "NotFound"
}

// IsNoSuchUpload reports whether err means the multipart upload does not exist.
func IsNoSuchUpload(err error) bool { return ErrorCode(err) == "NoSuchUpload" }

func parseError(resp *http.Response) *Error {
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	return decodeError(resp.StatusCode, resp.Header, body)
}

func decodeError(status int, h http.Header, body []byte) *Error {
	e := &Error{StatusCode: status, RequestID: h.Get("X-Amz-Request-Id")}
	var x struct {
		Code      string `xml:"Code"`
		Message   string `xml:"Message"`
		RequestID string `xml:"RequestId"`
	}
	if len(bytes.TrimSpace(body)) > 0 && xml.Unmarshal(body, &x) == nil {
		e.Code, e.Message = x.Code, x.Message
		if x.RequestID != "" {
			e.RequestID = x.RequestID
		}
	}
	if e.Code == "" {
		switch status {
		case http.StatusNotFound:
			e.Code = "NotFound"
		case http.StatusForbidden:
			e.Code = "AccessDenied"
		case http.StatusPreconditionFailed:
			e.Code = "PreconditionFailed"
		case http.StatusMovedPermanently:
			e.Code = "PermanentRedirect"
		default:
			e.Code = strings.ReplaceAll(http.StatusText(status), " ", "")
		}
	}
	if r := h.Get("X-Amz-Bucket-Region"); r != "" && status == http.StatusMovedPermanently {
		e.Message = strings.TrimSpace(e.Message + " (bucket is in region " + r + ")")
	}
	return e
}

func retryable(e *Error) bool {
	if e.StatusCode >= 500 || e.StatusCode == http.StatusTooManyRequests {
		return true
	}
	switch e.Code {
	case "RequestTimeout", "SlowDown", "InternalError", "ServiceUnavailable":
		return true
	}
	return false
}

// --- request execution --------------------------------------------------------

type call struct {
	method string
	key    string // "" addresses the bucket
	query  url.Values
	header http.Header
	body   []byte // nil means no body
}

func (c *Client) do(ctx context.Context, cl call) (*http.Response, error) {
	payloadHash := sigv4.EmptyPayloadHash
	if len(cl.body) > 0 {
		payloadHash = sigv4.HashHex(cl.body)
	}
	var lastErr error
	for attempt := 0; attempt < c.attempts; attempt++ {
		if attempt > 0 {
			if err := sleep(ctx, backoff(attempt)); err != nil {
				return nil, fmt.Errorf("%w (last error: %v)", err, lastErr)
			}
		}
		creds, err := c.cfg.Credentials.Retrieve(ctx)
		if err != nil {
			return nil, err
		}
		u := c.api.url(c.cfg.Bucket, cl.key)
		u.RawQuery = sigv4.CanonicalQuery(cl.query)
		var body io.Reader = http.NoBody
		if cl.body != nil {
			body = bytes.NewReader(cl.body)
		}
		req, err := http.NewRequestWithContext(ctx, cl.method, u.String(), body)
		if err != nil {
			return nil, err
		}
		for k, vs := range cl.header {
			req.Header[k] = append([]string(nil), vs...)
		}
		req.Header.Set("User-Agent", userAgent)
		if err := c.signer.Sign(req, payloadHash, creds, c.now()); err != nil {
			return nil, err
		}
		resp, err := c.hc.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = fmt.Errorf("s3: %s %s: %w", cl.method, u.Path, err)
			continue
		}
		if resp.StatusCode < 300 {
			return resp, nil
		}
		apiErr := parseError(resp)
		if !retryable(apiErr) {
			return nil, apiErr
		}
		lastErr = apiErr
	}
	return nil, lastErr
}

func backoff(attempt int) time.Duration {
	d := 100 * time.Millisecond << (attempt - 1)
	if d > 5*time.Second {
		d = 5 * time.Second
	}
	return time.Duration(rand.Int64N(int64(d))) + d/2
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

func decodeXMLBody(resp *http.Response, v any) error {
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return fmt.Errorf("s3: reading response: %w", err)
	}
	// Some operations (CompleteMultipartUpload) can return 200 with an error document.
	if e := embeddedError(resp, body); e != nil {
		return e
	}
	if err := xml.Unmarshal(body, v); err != nil {
		return fmt.Errorf("s3: decoding response: %w", err)
	}
	return nil
}

func embeddedError(resp *http.Response, body []byte) *Error {
	trimmed := bytes.TrimSpace(body)
	if bytes.HasPrefix(trimmed, []byte("<?xml")) {
		if i := bytes.Index(trimmed, []byte("?>")); i >= 0 {
			trimmed = bytes.TrimSpace(trimmed[i+2:])
		}
	}
	if bytes.HasPrefix(trimmed, []byte("<Error>")) || bytes.HasPrefix(trimmed, []byte("<Error ")) {
		e := decodeError(resp.StatusCode, resp.Header, body)
		if e.StatusCode < 400 {
			e.StatusCode = http.StatusInternalServerError
		}
		return e
	}
	return nil
}

// --- object attributes -------------------------------------------------------

// ObjectHeaders are attributes stored with an object.
type ObjectHeaders struct {
	ContentType        string
	ContentDisposition string
	CacheControl       string
	// Metadata is stored as x-amz-meta-* headers (ASCII values only).
	Metadata map[string]string
}

func (c *Client) objectHeaders(h ObjectHeaders) http.Header {
	out := http.Header{}
	if h.ContentType != "" {
		out.Set("Content-Type", h.ContentType)
	}
	if h.ContentDisposition != "" {
		out.Set("Content-Disposition", h.ContentDisposition)
	}
	if h.CacheControl != "" {
		out.Set("Cache-Control", h.CacheControl)
	}
	for k, v := range h.Metadata {
		out.Set("X-Amz-Meta-"+k, v)
	}
	if c.cfg.ServerSideEncryption != "" {
		out.Set("X-Amz-Server-Side-Encryption", c.cfg.ServerSideEncryption)
		if c.cfg.SSEKMSKeyID != "" {
			out.Set("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id", c.cfg.SSEKMSKeyID)
		}
	}
	return out
}

func contentMD5(b []byte) string {
	sum := md5.Sum(b)
	return base64.StdEncoding.EncodeToString(sum[:])
}

// ObjectInfo describes a stored object.
type ObjectInfo struct {
	Key                string
	Size               int64
	ContentType        string
	ContentDisposition string
	ETag               string
	LastModified       time.Time
	Metadata           map[string]string
}

func objectInfo(key string, h http.Header, size int64) ObjectInfo {
	info := ObjectInfo{
		Key:                key,
		Size:               size,
		ContentType:        h.Get("Content-Type"),
		ContentDisposition: h.Get("Content-Disposition"),
		ETag:               h.Get("ETag"),
	}
	if t, err := http.ParseTime(h.Get("Last-Modified")); err == nil {
		info.LastModified = t
	}
	for k, vs := range h {
		if lk := strings.ToLower(k); strings.HasPrefix(lk, "x-amz-meta-") && len(vs) > 0 {
			if info.Metadata == nil {
				info.Metadata = map[string]string{}
			}
			info.Metadata[strings.TrimPrefix(lk, "x-amz-meta-")] = vs[0]
		}
	}
	return info
}

// --- object operations --------------------------------------------------------

// PutObject stores data under key and returns the ETag.
func (c *Client) PutObject(ctx context.Context, key string, data []byte, h ObjectHeaders) (string, error) {
	hdr := c.objectHeaders(h)
	hdr.Set("Content-Md5", contentMD5(data))
	if data == nil {
		data = []byte{}
	}
	resp, err := c.do(ctx, call{method: http.MethodPut, key: key, header: hdr, body: data})
	if err != nil {
		return "", err
	}
	resp.Body.Close()
	return resp.Header.Get("ETag"), nil
}

// HeadObject returns the attributes of key.
func (c *Client) HeadObject(ctx context.Context, key string) (*ObjectInfo, error) {
	resp, err := c.do(ctx, call{method: http.MethodHead, key: key})
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	size := resp.ContentLength
	if size < 0 {
		size, _ = strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	}
	info := objectInfo(key, resp.Header, size)
	return &info, nil
}

// GetObjectOutput is the result of GetObject. The caller must close Body.
type GetObjectOutput struct {
	Body io.ReadCloser
	// Info.Size is the size of the whole object.
	Info ObjectInfo
	// ContentLength is the number of bytes in Body.
	ContentLength int64
}

// GetObject reads key. If length > 0 only bytes [offset, offset+length) are
// returned; if length == 0 the object is read from offset to the end.
func (c *Client) GetObject(ctx context.Context, key string, offset, length int64) (*GetObjectOutput, error) {
	hdr := http.Header{}
	if offset > 0 || length > 0 {
		r := "bytes=" + strconv.FormatInt(offset, 10) + "-"
		if length > 0 {
			r += strconv.FormatInt(offset+length-1, 10)
		}
		hdr.Set("Range", r)
	}
	resp, err := c.do(ctx, call{method: http.MethodGet, key: key, header: hdr})
	if err != nil {
		return nil, err
	}
	total := resp.ContentLength
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		if i := strings.LastIndexByte(cr, '/'); i >= 0 {
			if n, err := strconv.ParseInt(cr[i+1:], 10, 64); err == nil {
				total = n
			}
		}
	}
	return &GetObjectOutput{Body: resp.Body, Info: objectInfo(key, resp.Header, total), ContentLength: resp.ContentLength}, nil
}

// DeleteObject deletes key. Deleting a missing key is not an error.
func (c *Client) DeleteObject(ctx context.Context, key string) error {
	resp, err := c.do(ctx, call{method: http.MethodDelete, key: key})
	if err != nil {
		if IsNotFound(err) {
			return nil
		}
		return err
	}
	resp.Body.Close()
	return nil
}

// ObjectSummary is an entry of ListObjects.
type ObjectSummary struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
}

// ListObjectsInput selects objects to list.
type ListObjectsInput struct {
	Prefix            string
	StartAfter        string
	ContinuationToken string
	MaxKeys           int
}

// ListObjectsOutput is one page of ListObjects.
type ListObjectsOutput struct {
	Objects               []ObjectSummary
	IsTruncated           bool
	NextContinuationToken string
}

// ListObjects lists one page of objects (ListObjectsV2) in key order.
func (c *Client) ListObjects(ctx context.Context, in ListObjectsInput) (*ListObjectsOutput, error) {
	q := url.Values{"list-type": {"2"}}
	if in.Prefix != "" {
		q.Set("prefix", in.Prefix)
	}
	if in.StartAfter != "" {
		q.Set("start-after", in.StartAfter)
	}
	if in.ContinuationToken != "" {
		q.Set("continuation-token", in.ContinuationToken)
	}
	if in.MaxKeys > 0 {
		q.Set("max-keys", strconv.Itoa(in.MaxKeys))
	}
	resp, err := c.do(ctx, call{method: http.MethodGet, query: q})
	if err != nil {
		return nil, err
	}
	var x struct {
		IsTruncated           bool   `xml:"IsTruncated"`
		NextContinuationToken string `xml:"NextContinuationToken"`
		Contents              []struct {
			Key          string    `xml:"Key"`
			Size         int64     `xml:"Size"`
			ETag         string    `xml:"ETag"`
			LastModified time.Time `xml:"LastModified"`
		} `xml:"Contents"`
	}
	if err := decodeXMLBody(resp, &x); err != nil {
		return nil, err
	}
	out := &ListObjectsOutput{IsTruncated: x.IsTruncated, NextContinuationToken: x.NextContinuationToken}
	for _, o := range x.Contents {
		out.Objects = append(out.Objects, ObjectSummary{Key: o.Key, Size: o.Size, ETag: o.ETag, LastModified: o.LastModified})
	}
	return out, nil
}

// --- multipart uploads ---------------------------------------------------------

// CreateMultipartUpload starts a multipart upload and returns its upload ID.
func (c *Client) CreateMultipartUpload(ctx context.Context, key string, h ObjectHeaders) (string, error) {
	resp, err := c.do(ctx, call{method: http.MethodPost, key: key, query: url.Values{"uploads": {""}}, header: c.objectHeaders(h), body: []byte{}})
	if err != nil {
		return "", err
	}
	var x struct {
		UploadID string `xml:"UploadId"`
	}
	if err := decodeXMLBody(resp, &x); err != nil {
		return "", err
	}
	if x.UploadID == "" {
		return "", errors.New("s3: CreateMultipartUpload returned no upload id")
	}
	return x.UploadID, nil
}

// UploadPart uploads one part and returns its ETag.
func (c *Client) UploadPart(ctx context.Context, key, uploadID string, partNumber int, data []byte) (string, error) {
	q := url.Values{"partNumber": {strconv.Itoa(partNumber)}, "uploadId": {uploadID}}
	hdr := http.Header{}
	hdr.Set("Content-Md5", contentMD5(data))
	if data == nil {
		data = []byte{}
	}
	resp, err := c.do(ctx, call{method: http.MethodPut, key: key, query: q, header: hdr, body: data})
	if err != nil {
		return "", err
	}
	resp.Body.Close()
	etag := resp.Header.Get("ETag")
	if etag == "" {
		return "", errors.New("s3: UploadPart returned no ETag")
	}
	return etag, nil
}

// Part is an uploaded part of a multipart upload.
type Part struct {
	PartNumber   int
	ETag         string
	Size         int64
	LastModified time.Time
}

// ListParts returns all uploaded parts of a multipart upload, in order.
func (c *Client) ListParts(ctx context.Context, key, uploadID string) ([]Part, error) {
	var parts []Part
	marker := ""
	for {
		q := url.Values{"uploadId": {uploadID}, "max-parts": {"1000"}}
		if marker != "" {
			q.Set("part-number-marker", marker)
		}
		resp, err := c.do(ctx, call{method: http.MethodGet, key: key, query: q})
		if err != nil {
			return nil, err
		}
		var x struct {
			IsTruncated          bool   `xml:"IsTruncated"`
			NextPartNumberMarker string `xml:"NextPartNumberMarker"`
			Parts                []struct {
				PartNumber   int       `xml:"PartNumber"`
				ETag         string    `xml:"ETag"`
				Size         int64     `xml:"Size"`
				LastModified time.Time `xml:"LastModified"`
			} `xml:"Part"`
		}
		if err := decodeXMLBody(resp, &x); err != nil {
			return nil, err
		}
		for _, p := range x.Parts {
			parts = append(parts, Part{PartNumber: p.PartNumber, ETag: p.ETag, Size: p.Size, LastModified: p.LastModified})
		}
		if !x.IsTruncated || x.NextPartNumberMarker == "" || x.NextPartNumberMarker == marker {
			return parts, nil
		}
		marker = x.NextPartNumberMarker
	}
}

// CompletedPart identifies a part to assemble.
type CompletedPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// CompleteMultipartUpload assembles parts (in ascending order) into the
// final object and returns its ETag.
func (c *Client) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []CompletedPart) (string, error) {
	body, err := xml.Marshal(struct {
		XMLName xml.Name        `xml:"CompleteMultipartUpload"`
		Xmlns   string          `xml:"xmlns,attr"`
		Parts   []CompletedPart `xml:"Part"`
	}{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Parts: parts})
	if err != nil {
		return "", err
	}
	hdr := http.Header{}
	hdr.Set("Content-Type", "application/xml")
	resp, err := c.do(ctx, call{method: http.MethodPost, key: key, query: url.Values{"uploadId": {uploadID}}, header: hdr, body: body})
	if err != nil {
		return "", err
	}
	var x struct {
		ETag string `xml:"ETag"`
	}
	if err := decodeXMLBody(resp, &x); err != nil {
		return "", err
	}
	return x.ETag, nil
}

// AbortMultipartUpload discards a multipart upload and its parts.
func (c *Client) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	resp, err := c.do(ctx, call{method: http.MethodDelete, key: key, query: url.Values{"uploadId": {uploadID}}})
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// --- bucket operations ---------------------------------------------------------

// HeadBucket checks that the bucket exists and is accessible.
func (c *Client) HeadBucket(ctx context.Context) error {
	resp, err := c.do(ctx, call{method: http.MethodHead})
	if err != nil {
		var e *Error
		if errors.As(err, &e) && e.StatusCode == http.StatusNotFound {
			e.Code = "NoSuchBucket"
		}
		return err
	}
	resp.Body.Close()
	return nil
}

// EnsureBucket creates the bucket if it does not exist.
func (c *Client) EnsureBucket(ctx context.Context) error {
	err := c.HeadBucket(ctx)
	if err == nil || ErrorCode(err) != "NoSuchBucket" {
		return err
	}
	body := []byte{}
	if c.cfg.Region != "us-east-1" {
		body = []byte(`<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>` +
			c.cfg.Region + `</LocationConstraint></CreateBucketConfiguration>`)
	}
	resp, err := c.do(ctx, call{method: http.MethodPut, body: body})
	if err != nil {
		if c := ErrorCode(err); c == "BucketAlreadyOwnedByYou" {
			return nil
		}
		return err
	}
	resp.Body.Close()
	return nil
}

// --- presigned URLs ------------------------------------------------------------

// PresignGetObject returns a URL that downloads key until the returned time.
// params may carry S3 response overrides such as response-content-disposition.
func (c *Client) PresignGetObject(ctx context.Context, key string, expires time.Duration, params url.Values) (string, time.Time, error) {
	return c.presignURL(ctx, http.MethodGet, c.presign, key, params, nil, expires)
}

// PresignUploadPart returns a URL that accepts exactly one PUT of size bytes
// for the given part. Content-Length is part of the signature, so S3 rejects
// a body of any other size.
func (c *Client) PresignUploadPart(ctx context.Context, key, uploadID string, partNumber int, size int64, expires time.Duration) (string, time.Time, error) {
	q := url.Values{"partNumber": {strconv.Itoa(partNumber)}, "uploadId": {uploadID}}
	h := http.Header{}
	h.Set("Content-Length", strconv.FormatInt(size, 10))
	return c.presignURL(ctx, http.MethodPut, c.uploadURL, key, q, h, expires)
}

func (c *Client) presignURL(ctx context.Context, method string, e endpoint, key string, q url.Values, h http.Header, expires time.Duration) (string, time.Time, error) {
	creds, err := c.cfg.Credentials.Retrieve(ctx)
	if err != nil {
		return "", time.Time{}, err
	}
	now := c.now()
	// A presigned URL stops working when temporary credentials expire.
	if !creds.Expires.IsZero() {
		if left := creds.Expires.Sub(now); left < expires {
			expires = left
		}
	}
	expires = expires.Truncate(time.Second)
	if expires < time.Second {
		return "", time.Time{}, errors.New("s3: credentials expire too soon to presign")
	}
	if expires > sigv4.MaxPresignExpiry {
		expires = sigv4.MaxPresignExpiry
	}
	u := e.url(c.cfg.Bucket, key)
	u.RawQuery = sigv4.CanonicalQuery(q)
	s, err := c.signer.Presign(method, u, h, creds, now, expires)
	if err != nil {
		return "", time.Time{}, err
	}
	return s, now.Add(expires), nil
}
