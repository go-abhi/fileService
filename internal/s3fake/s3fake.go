// Package s3fake is an in-memory, S3-compatible HTTP server for tests and
// local development. It implements the subset of the S3 REST API used by the
// file service (objects, multipart uploads, ranged reads, listing, presigned
// URLs) with path-style addressing, and verifies AWS SigV4 signatures —
// both Authorization headers and presigned query strings — when credentials
// are configured.
package s3fake

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-abhi/fileService/internal/s3/sigv4"
)

const xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"

// Server is an in-memory S3 server. Create it with New.
type Server struct {
	region   string
	creds    map[string]string
	minPart  int64
	pageSize int
	now      func() time.Time

	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	objects map[string]*object
	uploads map[string]*upload
}

type attrs struct {
	contentType, contentDisposition, cacheControl string
	meta                                          map[string]string
}

type object struct {
	data     []byte
	etag     string
	modified time.Time
	attrs
}

type upload struct {
	key       string
	id        string
	initiated time.Time
	attrs
	parts map[int]*part
}

type part struct {
	data     []byte
	etag     string
	modified time.Time
}

// Option configures a Server.
type Option func(*Server)

// WithCredentials requires requests to be signed with this key pair.
// Without credentials the server accepts anonymous requests.
func WithCredentials(accessKey, secret string) Option {
	return func(s *Server) { s.creds[accessKey] = secret }
}

// WithRegion sets the region signatures must be scoped to (default us-east-1).
func WithRegion(region string) Option { return func(s *Server) { s.region = region } }

// WithMinPartSize sets the minimum size of all but the last part of a
// multipart upload (default 5 MiB, like S3).
func WithMinPartSize(n int64) Option { return func(s *Server) { s.minPart = n } }

// WithPageSize caps list results per page (default 1000), to exercise
// pagination in tests.
func WithPageSize(n int) Option { return func(s *Server) { s.pageSize = n } }

// WithClock overrides the clock used for expiry checks and timestamps.
func WithClock(now func() time.Time) Option { return func(s *Server) { s.now = now } }

// New returns a server with no buckets.
func New(opts ...Option) *Server {
	s := &Server{
		region:   "us-east-1",
		creds:    map[string]string{},
		minPart:  5 << 20,
		pageSize: 1000,
		now:      time.Now,
		buckets:  map[string]*bucket{},
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// CreateBucket creates a bucket if it does not exist.
func (s *Server) CreateBucket(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.buckets[name] == nil {
		s.buckets[name] = &bucket{objects: map[string]*object{}, uploads: map[string]*upload{}}
	}
}

// ObjectKeys returns the keys stored in a bucket, sorted.
func (s *Server) ObjectKeys(bucketName string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.buckets[bucketName]
	if b == nil {
		return nil
	}
	keys := make([]string, 0, len(b.objects))
	for k := range b.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Object returns a copy of an object's content.
func (s *Server) Object(bucketName, key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.buckets[bucketName]
	if b == nil || b.objects[key] == nil {
		return nil, false
	}
	return bytes.Clone(b.objects[key].data), true
}

// PutObject stores an object directly (test helper).
func (s *Server) PutObject(bucketName, key string, data []byte) {
	s.CreateBucket(bucketName)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buckets[bucketName].objects[key] = &object{data: bytes.Clone(data), etag: md5ETag(data), modified: s.now().UTC()}
}

// DeleteObject removes an object directly (test helper).
func (s *Server) DeleteObject(bucketName, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b := s.buckets[bucketName]; b != nil {
		delete(b.objects, key)
	}
}

// UploadCount returns the number of in-progress multipart uploads.
func (s *Server) UploadCount(bucketName string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b := s.buckets[bucketName]; b != nil {
		return len(b.uploads)
	}
	return 0
}

// ServeHTTP implements the S3 REST API (path-style addressing).
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Expose-Headers", "ETag, Content-Length, Content-Range")
	if r.Method == http.MethodOptions {
		h.Set("Access-Control-Allow-Methods", "GET, HEAD, PUT, POST, DELETE")
		h.Set("Access-Control-Allow-Headers", "*")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "IncompleteBody", err.Error())
		return
	}
	if e := s.authenticate(r, body); e != nil {
		writeError(w, r, e.status, e.code, e.msg)
		return
	}

	bucketName, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if bucketName == "" {
		writeError(w, r, http.StatusNotImplemented, "NotImplemented", "ListBuckets is not supported")
		return
	}
	q := r.URL.Query()
	if key == "" {
		s.serveBucket(w, r, bucketName, q, body)
		return
	}

	switch {
	case r.Method == http.MethodPut && q.Has("uploadId"):
		s.uploadPart(w, r, bucketName, key, q, body)
	case r.Method == http.MethodPut:
		s.putObject(w, r, bucketName, key, body)
	case r.Method == http.MethodPost && q.Has("uploads"):
		s.createMultipart(w, r, bucketName, key)
	case r.Method == http.MethodPost && q.Has("uploadId"):
		s.completeMultipart(w, r, bucketName, key, q.Get("uploadId"), body)
	case r.Method == http.MethodGet && q.Has("uploadId"):
		s.listParts(w, r, bucketName, key, q)
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		s.getObject(w, r, bucketName, key, q)
	case r.Method == http.MethodDelete && q.Has("uploadId"):
		s.abortMultipart(w, r, bucketName, key, q.Get("uploadId"))
	case r.Method == http.MethodDelete:
		s.deleteObject(w, bucketName, key)
	default:
		writeError(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "unsupported operation")
	}
}

// --- authentication -------------------------------------------------------------

type authError struct {
	status    int
	code, msg string
}

func denied(code, msg string) *authError { return &authError{http.StatusForbidden, code, msg} }

func (s *Server) authenticate(r *http.Request, body []byte) *authError {
	if len(s.creds) == 0 {
		return nil
	}
	if auth := r.Header.Get("Authorization"); auth != "" {
		return s.verifyHeaderAuth(r, body, auth)
	}
	if r.URL.Query().Get("X-Amz-Algorithm") != "" {
		return s.verifyPresigned(r)
	}
	return denied("AccessDenied", "Anonymous access is not allowed")
}

func (s *Server) checkScope(credential string, t time.Time) (string, *authError) {
	parts := strings.Split(credential, "/")
	if len(parts) != 5 || parts[4] != "aws4_request" || parts[3] != "s3" {
		return "", denied("AuthorizationQueryParametersError", "malformed credential "+credential)
	}
	secret, ok := s.creds[parts[0]]
	if !ok {
		return "", denied("InvalidAccessKeyId", "unknown access key "+parts[0])
	}
	if parts[1] != t.UTC().Format("20060102") {
		return "", denied("SignatureDoesNotMatch", "credential date does not match X-Amz-Date")
	}
	if parts[2] != s.region {
		return "", &authError{http.StatusBadRequest, "AuthorizationHeaderMalformed", "region must be " + s.region}
	}
	return secret, nil
}

func (s *Server) verifyHeaderAuth(r *http.Request, body []byte, auth string) *authError {
	rest, ok := strings.CutPrefix(auth, sigv4.Algorithm+" ")
	if !ok {
		return denied("AccessDenied", "unsupported authorization scheme")
	}
	fields := map[string]string{}
	for _, f := range strings.Split(rest, ",") {
		k, v, _ := strings.Cut(strings.TrimSpace(f), "=")
		fields[k] = v
	}
	t, err := time.Parse(sigv4.TimeFormat, r.Header.Get("X-Amz-Date"))
	if err != nil {
		return denied("AccessDenied", "missing or invalid X-Amz-Date")
	}
	if d := s.now().Sub(t); d > 15*time.Minute || d < -15*time.Minute {
		return denied("RequestTimeTooSkewed", "request time too skewed")
	}
	secret, aerr := s.checkScope(fields["Credential"], t)
	if aerr != nil {
		return aerr
	}
	payload := r.Header.Get("X-Amz-Content-Sha256")
	if payload == "" {
		return &authError{http.StatusBadRequest, "InvalidRequest", "missing x-amz-content-sha256"}
	}
	if payload != sigv4.UnsignedPayload && payload != sigv4.HashHex(body) {
		return &authError{http.StatusBadRequest, "XAmzContentSHA256Mismatch", "payload hash mismatch"}
	}
	signed := strings.Split(fields["SignedHeaders"], ";")
	return s.compare(r, secret, t, signed, fields["SignedHeaders"], payload, r.URL.Query(), fields["Signature"])
}

func (s *Server) verifyPresigned(r *http.Request) *authError {
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") != sigv4.Algorithm {
		return denied("AuthorizationQueryParametersError", "unsupported algorithm")
	}
	t, err := time.Parse(sigv4.TimeFormat, q.Get("X-Amz-Date"))
	if err != nil {
		return denied("AuthorizationQueryParametersError", "invalid X-Amz-Date")
	}
	expires, err := strconv.Atoi(q.Get("X-Amz-Expires"))
	if err != nil || expires <= 0 || expires > 604800 {
		return denied("AuthorizationQueryParametersError", "invalid X-Amz-Expires")
	}
	if s.now().After(t.Add(time.Duration(expires) * time.Second)) {
		return denied("AccessDenied", "Request has expired")
	}
	secret, aerr := s.checkScope(q.Get("X-Amz-Credential"), t)
	if aerr != nil {
		return aerr
	}
	sig := q.Get("X-Amz-Signature")
	q.Del("X-Amz-Signature")
	signedStr := q.Get("X-Amz-SignedHeaders")
	payload := sigv4.UnsignedPayload
	if p := q.Get("X-Amz-Content-Sha256"); p != "" {
		payload = p
	}
	return s.compare(r, secret, t, strings.Split(signedStr, ";"), signedStr, payload, q, sig)
}

func (s *Server) compare(r *http.Request, secret string, t time.Time, signed []string, signedStr, payload string, q url.Values, got string) *authError {
	hasHost := false
	for _, h := range signed {
		if h == "host" {
			hasHost = true
		}
	}
	if !hasHost {
		return denied("AccessDenied", "host must be a signed header")
	}
	canonURI := "/"
	if r.URL.Path != "" {
		canonURI = sigv4.EscapePath(r.URL.Path)
	}
	creq := sigv4.CanonicalRequest(r.Method, canonURI, sigv4.CanonicalQuery(q),
		sigv4.CanonicalHeaders(signed, r.Header, r.Host, r.ContentLength), signedStr, payload)
	scope := sigv4.Scope(t, s.region, "s3")
	want := sigv4.Signature(sigv4.SigningKey(secret, t, s.region, "s3"), sigv4.StringToSign(t, scope, creq))
	if !hmac.Equal([]byte(want), []byte(got)) {
		return denied("SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided")
	}
	return nil
}

// --- bucket-level operations -----------------------------------------------------

func (s *Server) serveBucket(w http.ResponseWriter, r *http.Request, name string, q url.Values, body []byte) {
	s.mu.Lock()
	b := s.buckets[name]
	s.mu.Unlock()
	switch r.Method {
	case http.MethodHead:
		if b == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodPut:
		if b != nil {
			writeError(w, r, http.StatusConflict, "BucketAlreadyOwnedByYou", "bucket exists")
			return
		}
		s.CreateBucket(name)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		if b == nil {
			writeError(w, r, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
			return
		}
		switch {
		case q.Has("uploads"):
			s.listUploads(w, name, q)
		case q.Get("list-type") == "2":
			s.listObjects(w, r, name, q)
		default:
			writeError(w, r, http.StatusNotImplemented, "NotImplemented", "only ListObjectsV2 is supported")
		}
	default:
		writeError(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "unsupported bucket operation")
	}
}

func (s *Server) bucket(w http.ResponseWriter, r *http.Request, name string) *bucket {
	b := s.buckets[name]
	if b == nil {
		writeError(w, r, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
	}
	return b
}

func (s *Server) listObjects(w http.ResponseWriter, r *http.Request, name string, q url.Values) {
	s.mu.Lock()
	b := s.buckets[name]
	prefix := q.Get("prefix")
	after := q.Get("start-after")
	if tok := q.Get("continuation-token"); tok != "" {
		dec, err := base64.RawURLEncoding.DecodeString(tok)
		if err != nil {
			s.mu.Unlock()
			writeError(w, r, http.StatusBadRequest, "InvalidArgument", "invalid continuation token")
			return
		}
		after = string(dec)
	}
	maxKeys := s.pageSize
	if mk := q.Get("max-keys"); mk != "" {
		if n, err := strconv.Atoi(mk); err == nil && n >= 0 && n < maxKeys {
			maxKeys = n
		}
	}
	keys := make([]string, 0)
	for k := range b.objects {
		if strings.HasPrefix(k, prefix) && k > after {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	type content struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
		StorageClass string `xml:"StorageClass"`
	}
	res := struct {
		XMLName               xml.Name  `xml:"ListBucketResult"`
		Xmlns                 string    `xml:"xmlns,attr"`
		Name                  string    `xml:"Name"`
		Prefix                string    `xml:"Prefix"`
		KeyCount              int       `xml:"KeyCount"`
		MaxKeys               int       `xml:"MaxKeys"`
		IsTruncated           bool      `xml:"IsTruncated"`
		Contents              []content `xml:"Contents"`
		NextContinuationToken string    `xml:"NextContinuationToken,omitempty"`
	}{Xmlns: xmlns, Name: name, Prefix: prefix, MaxKeys: maxKeys}
	if len(keys) > maxKeys {
		res.IsTruncated = true
		keys = keys[:maxKeys]
		if len(keys) > 0 {
			res.NextContinuationToken = base64.RawURLEncoding.EncodeToString([]byte(keys[len(keys)-1]))
		}
	}
	for _, k := range keys {
		o := b.objects[k]
		res.Contents = append(res.Contents, content{Key: k, LastModified: xmlTime(o.modified), ETag: o.etag, Size: int64(len(o.data)), StorageClass: "STANDARD"})
	}
	res.KeyCount = len(res.Contents)
	s.mu.Unlock()
	writeXML(w, http.StatusOK, res)
}

func (s *Server) listUploads(w http.ResponseWriter, name string, q url.Values) {
	s.mu.Lock()
	b := s.buckets[name]
	prefix := q.Get("prefix")
	type up struct {
		Key       string `xml:"Key"`
		UploadID  string `xml:"UploadId"`
		Initiated string `xml:"Initiated"`
	}
	ups := []*upload{}
	for _, u := range b.uploads {
		if strings.HasPrefix(u.key, prefix) {
			ups = append(ups, u)
		}
	}
	sort.Slice(ups, func(i, j int) bool {
		if ups[i].key != ups[j].key {
			return ups[i].key < ups[j].key
		}
		return ups[i].initiated.Before(ups[j].initiated)
	})
	res := struct {
		XMLName     xml.Name `xml:"ListMultipartUploadsResult"`
		Xmlns       string   `xml:"xmlns,attr"`
		Bucket      string   `xml:"Bucket"`
		Prefix      string   `xml:"Prefix"`
		IsTruncated bool     `xml:"IsTruncated"`
		Uploads     []up     `xml:"Upload"`
	}{Xmlns: xmlns, Bucket: name, Prefix: prefix}
	for _, u := range ups {
		res.Uploads = append(res.Uploads, up{Key: u.key, UploadID: u.id, Initiated: xmlTime(u.initiated)})
	}
	s.mu.Unlock()
	writeXML(w, http.StatusOK, res)
}

// --- object operations ------------------------------------------------------------

func readAttrs(h http.Header) attrs {
	a := attrs{
		contentType:        h.Get("Content-Type"),
		contentDisposition: h.Get("Content-Disposition"),
		cacheControl:       h.Get("Cache-Control"),
	}
	for k, vs := range h {
		if lk := strings.ToLower(k); strings.HasPrefix(lk, "x-amz-meta-") && len(vs) > 0 {
			if a.meta == nil {
				a.meta = map[string]string{}
			}
			a.meta[strings.TrimPrefix(lk, "x-amz-meta-")] = vs[0]
		}
	}
	if a.contentType == "" {
		a.contentType = "binary/octet-stream"
	}
	return a
}

func checkMD5(r *http.Request, body []byte) bool {
	want := r.Header.Get("Content-Md5")
	if want == "" {
		return true
	}
	sum := md5.Sum(body)
	return want == base64.StdEncoding.EncodeToString(sum[:])
}

func (s *Server) putObject(w http.ResponseWriter, r *http.Request, name, key string, body []byte) {
	if !checkMD5(r, body) {
		writeError(w, r, http.StatusBadRequest, "BadDigest", "The Content-MD5 you specified did not match what we received.")
		return
	}
	s.mu.Lock()
	b := s.bucket(w, r, name)
	if b == nil {
		s.mu.Unlock()
		return
	}
	o := &object{data: body, etag: md5ETag(body), modified: s.now().UTC(), attrs: readAttrs(r.Header)}
	b.objects[key] = o
	s.mu.Unlock()
	w.Header().Set("ETag", o.etag)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request, name, key string, q url.Values) {
	s.mu.Lock()
	b := s.bucket(w, r, name)
	if b == nil {
		s.mu.Unlock()
		return
	}
	o := b.objects[key]
	s.mu.Unlock()
	if o == nil {
		writeError(w, r, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
		return
	}
	h := w.Header()
	h.Set("Content-Type", o.contentType)
	if o.contentDisposition != "" {
		h.Set("Content-Disposition", o.contentDisposition)
	}
	if o.cacheControl != "" {
		h.Set("Cache-Control", o.cacheControl)
	}
	for k, v := range o.meta {
		h.Set("X-Amz-Meta-"+k, v)
	}
	for param, header := range map[string]string{
		"response-content-type":        "Content-Type",
		"response-content-disposition": "Content-Disposition",
		"response-cache-control":       "Cache-Control",
	} {
		if v := q.Get(param); v != "" {
			h.Set(header, v)
		}
	}
	h.Set("ETag", o.etag)
	h.Set("Last-Modified", o.modified.Format(http.TimeFormat))
	h.Set("Accept-Ranges", "bytes")

	size := int64(len(o.data))
	start, end, status := int64(0), size-1, http.StatusOK
	if rng := r.Header.Get("Range"); rng != "" {
		var ok bool
		start, end, ok = parseRange(rng, size)
		if !ok {
			h.Set("Content-Range", "bytes */"+strconv.FormatInt(size, 10))
			writeError(w, r, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "The requested range is not satisfiable")
			return
		}
		status = http.StatusPartialContent
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
	}
	h.Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.WriteHeader(status)
	if r.Method != http.MethodHead && size > 0 {
		w.Write(o.data[start : end+1])
	}
}

// parseRange parses a single-range "bytes=" header.
func parseRange(rng string, size int64) (int64, int64, bool) {
	spec, ok := strings.CutPrefix(rng, "bytes=")
	if !ok || strings.Contains(spec, ",") {
		return 0, 0, false
	}
	a, b, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, false
	}
	if a == "" { // suffix range: last n bytes
		n, err := strconv.ParseInt(b, 10, 64)
		if err != nil || n <= 0 || size == 0 {
			return 0, 0, false
		}
		if n > size {
			n = size
		}
		return size - n, size - 1, true
	}
	start, err := strconv.ParseInt(a, 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false
	}
	end := size - 1
	if b != "" {
		e, err := strconv.ParseInt(b, 10, 64)
		if err != nil || e < start {
			return 0, 0, false
		}
		if e < end {
			end = e
		}
	}
	return start, end, true
}

func (s *Server) deleteObject(w http.ResponseWriter, name, key string) {
	s.mu.Lock()
	if b := s.buckets[name]; b != nil {
		delete(b.objects, key)
	}
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// --- multipart uploads -------------------------------------------------------------

func (s *Server) createMultipart(w http.ResponseWriter, r *http.Request, name, key string) {
	var raw [16]byte
	rand.Read(raw[:])
	id := hex.EncodeToString(raw[:])
	s.mu.Lock()
	b := s.bucket(w, r, name)
	if b == nil {
		s.mu.Unlock()
		return
	}
	b.uploads[id] = &upload{key: key, id: id, initiated: s.now().UTC(), attrs: readAttrs(r.Header), parts: map[int]*part{}}
	s.mu.Unlock()
	writeXML(w, http.StatusOK, struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Xmlns    string   `xml:"xmlns,attr"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		UploadID string   `xml:"UploadId"`
	}{Xmlns: xmlns, Bucket: name, Key: key, UploadID: id})
}

func (s *Server) findUpload(w http.ResponseWriter, r *http.Request, name, key, id string) *upload {
	b := s.bucket(w, r, name)
	if b == nil {
		return nil
	}
	u := b.uploads[id]
	if u == nil || u.key != key {
		writeError(w, r, http.StatusNotFound, "NoSuchUpload", "The specified upload does not exist.")
		return nil
	}
	return u
}

func (s *Server) uploadPart(w http.ResponseWriter, r *http.Request, name, key string, q url.Values, body []byte) {
	n, err := strconv.Atoi(q.Get("partNumber"))
	if err != nil || n < 1 || n > 10000 {
		writeError(w, r, http.StatusBadRequest, "InvalidArgument", "Part number must be an integer between 1 and 10000")
		return
	}
	if !checkMD5(r, body) {
		writeError(w, r, http.StatusBadRequest, "BadDigest", "The Content-MD5 you specified did not match what we received.")
		return
	}
	s.mu.Lock()
	u := s.findUpload(w, r, name, key, q.Get("uploadId"))
	if u == nil {
		s.mu.Unlock()
		return
	}
	p := &part{data: body, etag: md5ETag(body), modified: s.now().UTC()}
	u.parts[n] = p
	s.mu.Unlock()
	w.Header().Set("ETag", p.etag)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) listParts(w http.ResponseWriter, r *http.Request, name, key string, q url.Values) {
	marker, _ := strconv.Atoi(q.Get("part-number-marker"))
	maxParts := s.pageSize
	if mp, err := strconv.Atoi(q.Get("max-parts")); err == nil && mp > 0 && mp < maxParts {
		maxParts = mp
	}
	s.mu.Lock()
	u := s.findUpload(w, r, name, key, q.Get("uploadId"))
	if u == nil {
		s.mu.Unlock()
		return
	}
	nums := []int{}
	for n := range u.parts {
		if n > marker {
			nums = append(nums, n)
		}
	}
	sort.Ints(nums)
	type xp struct {
		PartNumber   int    `xml:"PartNumber"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
	}
	res := struct {
		XMLName              xml.Name `xml:"ListPartsResult"`
		Xmlns                string   `xml:"xmlns,attr"`
		Bucket               string   `xml:"Bucket"`
		Key                  string   `xml:"Key"`
		UploadID             string   `xml:"UploadId"`
		PartNumberMarker     int      `xml:"PartNumberMarker"`
		NextPartNumberMarker int      `xml:"NextPartNumberMarker"`
		MaxParts             int      `xml:"MaxParts"`
		IsTruncated          bool     `xml:"IsTruncated"`
		Parts                []xp     `xml:"Part"`
	}{Xmlns: xmlns, Bucket: name, Key: key, UploadID: u.id, PartNumberMarker: marker, MaxParts: maxParts}
	if len(nums) > maxParts {
		nums = nums[:maxParts]
		res.IsTruncated = true
	}
	for _, n := range nums {
		p := u.parts[n]
		res.Parts = append(res.Parts, xp{PartNumber: n, LastModified: xmlTime(p.modified), ETag: p.etag, Size: int64(len(p.data))})
		res.NextPartNumberMarker = n
	}
	s.mu.Unlock()
	writeXML(w, http.StatusOK, res)
}

func (s *Server) completeMultipart(w http.ResponseWriter, r *http.Request, name, key, id string, body []byte) {
	var req struct {
		Parts []struct {
			PartNumber int    `xml:"PartNumber"`
			ETag       string `xml:"ETag"`
		} `xml:"Part"`
	}
	if err := xml.Unmarshal(body, &req); err != nil || len(req.Parts) == 0 {
		writeError(w, r, http.StatusBadRequest, "MalformedXML", "The XML you provided was not well-formed")
		return
	}
	s.mu.Lock()
	u := s.findUpload(w, r, name, key, id)
	if u == nil {
		s.mu.Unlock()
		return
	}
	var data []byte
	var sums []byte
	for i, rp := range req.Parts {
		if i > 0 && rp.PartNumber <= req.Parts[i-1].PartNumber {
			s.mu.Unlock()
			writeError(w, r, http.StatusBadRequest, "InvalidPartOrder", "The list of parts was not in ascending order.")
			return
		}
		p := u.parts[rp.PartNumber]
		if p == nil || strings.Trim(rp.ETag, `"`) != strings.Trim(p.etag, `"`) {
			s.mu.Unlock()
			writeError(w, r, http.StatusBadRequest, "InvalidPart", fmt.Sprintf("part %d not found or ETag mismatch", rp.PartNumber))
			return
		}
		if i < len(req.Parts)-1 && int64(len(p.data)) < s.minPart {
			s.mu.Unlock()
			writeError(w, r, http.StatusBadRequest, "EntityTooSmall", "Your proposed upload is smaller than the minimum allowed object size.")
			return
		}
		data = append(data, p.data...)
		sum := md5.Sum(p.data)
		sums = append(sums, sum[:]...)
	}
	all := md5.Sum(sums)
	etag := fmt.Sprintf(`"%s-%d"`, hex.EncodeToString(all[:]), len(req.Parts))
	b := s.buckets[name]
	b.objects[key] = &object{data: data, etag: etag, modified: s.now().UTC(), attrs: u.attrs}
	delete(b.uploads, id)
	s.mu.Unlock()
	writeXML(w, http.StatusOK, struct {
		XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
		Xmlns    string   `xml:"xmlns,attr"`
		Location string   `xml:"Location"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		ETag     string   `xml:"ETag"`
	}{Xmlns: xmlns, Location: "/" + name + "/" + key, Bucket: name, Key: key, ETag: etag})
}

func (s *Server) abortMultipart(w http.ResponseWriter, r *http.Request, name, key, id string) {
	s.mu.Lock()
	u := s.findUpload(w, r, name, key, id)
	if u == nil {
		s.mu.Unlock()
		return
	}
	delete(s.buckets[name].uploads, id)
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// --- helpers --------------------------------------------------------------------

func md5ETag(b []byte) string {
	sum := md5.Sum(b)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func xmlTime(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }

func writeXML(w http.ResponseWriter, status int, v any) {
	b, err := xml.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	io.WriteString(w, xml.Header)
	w.Write(b)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}
	writeXML(w, status, struct {
		XMLName   xml.Name `xml:"Error"`
		Code      string   `xml:"Code"`
		Message   string   `xml:"Message"`
		Resource  string   `xml:"Resource"`
		RequestID string   `xml:"RequestId"`
	}{Code: code, Message: msg, Resource: r.URL.Path, RequestID: "fake"})
}
