// Package sigv4 implements AWS Signature Version 4 signing for HTTP requests
// and presigned URLs, as used by Amazon S3 and other AWS services.
//
// The exported building blocks (CanonicalRequest, StringToSign, SigningKey...)
// are also used to verify signatures, e.g. by the fake S3 server in tests.
package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// Algorithm is the SigV4 signing algorithm identifier.
	Algorithm = "AWS4-HMAC-SHA256"
	// UnsignedPayload marks a request whose body is not covered by the signature.
	UnsignedPayload = "UNSIGNED-PAYLOAD"
	// EmptyPayloadHash is the hex SHA-256 of an empty body.
	EmptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	// TimeFormat is the format of X-Amz-Date.
	TimeFormat = "20060102T150405Z"
	dateFormat = "20060102"

	// MaxPresignExpiry is the longest validity S3 accepts for a presigned URL.
	MaxPresignExpiry = 7 * 24 * time.Hour
)

// Credentials are AWS access credentials.
type Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	// Expires is when temporary credentials stop working. Zero means never.
	Expires time.Time
	// Source describes where the credentials came from (for logs).
	Source string
}

// Valid reports whether the credentials have the required fields.
func (c Credentials) Valid() bool { return c.AccessKeyID != "" && c.SecretAccessKey != "" }

// ExpiresWithin reports whether the credentials expire within d of now.
func (c Credentials) ExpiresWithin(now time.Time, d time.Duration) bool {
	return !c.Expires.IsZero() && !now.Add(d).Before(c.Expires)
}

// Signer signs requests for one AWS service in one region.
type Signer struct {
	Service string
	Region  string
}

// headers never included in the signature: they are added or rewritten by
// proxies and HTTP stacks.
var unsignedHeaders = map[string]bool{
	"authorization":     true,
	"user-agent":        true,
	"x-amzn-trace-id":   true,
	"expect":            true,
	"connection":        true,
	"content-length":    true,
	"transfer-encoding": true,
	"accept-encoding":   true,
}

// Sign signs req in place with an Authorization header. payloadHash is the
// hex SHA-256 of the request body or UnsignedPayload. For S3 the
// X-Amz-Content-Sha256 header is set as the service requires it.
//
// Every header present on req (except a few hop-by-hop ones) is signed, so
// set all headers before calling Sign.
func (s Signer) Sign(req *http.Request, payloadHash string, creds Credentials, now time.Time) error {
	if !creds.Valid() {
		return errors.New("sigv4: missing credentials")
	}
	t := now.UTC()
	req.Header.Set("X-Amz-Date", t.Format(TimeFormat))
	if creds.SessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", creds.SessionToken)
	}
	if s.Service == "s3" {
		req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	}

	names := make([]string, 0, len(req.Header)+1)
	values := make(map[string]string, len(req.Header)+1)
	names = append(names, "host")
	values["host"] = hostOf(req)
	for k, vs := range req.Header {
		lk := strings.ToLower(k)
		if unsignedHeaders[lk] || lk == "host" {
			continue
		}
		names = append(names, lk)
		values[lk] = canonicalHeaderValue(vs)
	}
	canonHeaders, signed := buildCanonicalHeaders(names, values)

	scope := Scope(t, s.Region, s.Service)
	creq := CanonicalRequest(req.Method, CanonicalURI(req.URL), CanonicalQuery(req.URL.Query()),
		canonHeaders, signed, payloadHash)
	sig := Signature(SigningKey(creds.SecretAccessKey, t, s.Region, s.Service), StringToSign(t, scope, creq))

	req.Header.Set("Authorization", Algorithm+" Credential="+creds.AccessKeyID+"/"+scope+
		", SignedHeaders="+signed+", Signature="+sig)
	return nil
}

// Presign returns a presigned URL for method and u. extraHeaders are signed
// in addition to Host; the caller of the URL must send them with exactly the
// same values (S3 uses this to pin e.g. Content-Length of an upload part).
// The returned URL keeps u's path and adds the X-Amz-* query parameters.
func (s Signer) Presign(method string, u *url.URL, extraHeaders http.Header, creds Credentials, now time.Time, expires time.Duration) (string, error) {
	if !creds.Valid() {
		return "", errors.New("sigv4: missing credentials")
	}
	if expires <= 0 || expires > MaxPresignExpiry {
		return "", errors.New("sigv4: presign expiry must be between 1s and 7 days")
	}
	t := now.UTC()
	scope := Scope(t, s.Region, s.Service)

	names := []string{"host"}
	values := map[string]string{"host": u.Host}
	for k, vs := range extraHeaders {
		lk := strings.ToLower(k)
		if lk == "host" {
			continue
		}
		names = append(names, lk)
		values[lk] = canonicalHeaderValue(vs)
	}
	canonHeaders, signed := buildCanonicalHeaders(names, values)

	q := u.Query()
	q.Set("X-Amz-Algorithm", Algorithm)
	q.Set("X-Amz-Credential", creds.AccessKeyID+"/"+scope)
	q.Set("X-Amz-Date", t.Format(TimeFormat))
	q.Set("X-Amz-Expires", strconv.FormatInt(int64(expires/time.Second), 10))
	q.Set("X-Amz-SignedHeaders", signed)
	if creds.SessionToken != "" {
		q.Set("X-Amz-Security-Token", creds.SessionToken)
	}
	q.Del("X-Amz-Signature")
	cq := CanonicalQuery(q)

	creq := CanonicalRequest(method, CanonicalURI(u), cq, canonHeaders, signed, UnsignedPayload)
	sig := Signature(SigningKey(creds.SecretAccessKey, t, s.Region, s.Service), StringToSign(t, scope, creq))

	out := *u
	out.RawQuery = cq + "&X-Amz-Signature=" + sig
	return out.String(), nil
}

func hostOf(req *http.Request) string {
	if req.Host != "" {
		return req.Host
	}
	return req.URL.Host
}

// buildCanonicalHeaders returns the canonical header block (each line ends
// with "\n") and the signed-headers list.
func buildCanonicalHeaders(names []string, values map[string]string) (string, string) {
	sort.Strings(names)
	var b strings.Builder
	for _, n := range names {
		b.WriteString(n)
		b.WriteByte(':')
		b.WriteString(values[n])
		b.WriteByte('\n')
	}
	return b.String(), strings.Join(names, ";")
}

// CanonicalHeaders builds the canonical header block for the given signed
// header names, reading values from h. "host" is taken from host. It is used
// to verify signatures.
func CanonicalHeaders(signedHeaders []string, h http.Header, host string, contentLength int64) string {
	names := make([]string, 0, len(signedHeaders))
	values := make(map[string]string, len(signedHeaders))
	for _, n := range signedHeaders {
		n = strings.ToLower(n)
		names = append(names, n)
		switch n {
		case "host":
			values[n] = host
		case "content-length":
			values[n] = strconv.FormatInt(contentLength, 10)
		default:
			values[n] = canonicalHeaderValue(h.Values(n))
		}
	}
	block, _ := buildCanonicalHeaders(names, values)
	return block
}

func canonicalHeaderValue(vs []string) string {
	parts := make([]string, len(vs))
	for i, v := range vs {
		parts[i] = strings.Join(strings.Fields(v), " ")
	}
	return strings.Join(parts, ",")
}

// CanonicalRequest assembles the canonical request string.
func CanonicalRequest(method, canonicalURI, canonicalQuery, canonicalHeaders, signedHeaders, payloadHash string) string {
	return method + "\n" + canonicalURI + "\n" + canonicalQuery + "\n" + canonicalHeaders + "\n" + signedHeaders + "\n" + payloadHash
}

// CanonicalURI returns the S3-style canonical URI of u: the decoded path,
// URI-encoded once with '/' preserved. An empty path becomes "/".
func CanonicalURI(u *url.URL) string {
	p := u.Path
	if p == "" {
		return "/"
	}
	return EscapePath(p)
}

// CanonicalQuery returns the canonical query string: parameters sorted by
// name then value, each URI-encoded.
func CanonicalQuery(v url.Values) string {
	if len(v) == 0 {
		return ""
	}
	type kv struct{ k, v string }
	pairs := make([]kv, 0, len(v))
	for k, vs := range v {
		ek := Escape(k)
		if len(vs) == 0 {
			pairs = append(pairs, kv{ek, ""})
			continue
		}
		for _, val := range vs {
			pairs = append(pairs, kv{ek, Escape(val)})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	var b strings.Builder
	for i, p := range pairs {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(p.k)
		b.WriteByte('=')
		b.WriteString(p.v)
	}
	return b.String()
}

// Scope returns the credential scope date/region/service/aws4_request.
func Scope(t time.Time, region, service string) string {
	return t.UTC().Format(dateFormat) + "/" + region + "/" + service + "/aws4_request"
}

// StringToSign builds the string to sign for a canonical request.
func StringToSign(t time.Time, scope, canonicalRequest string) string {
	h := sha256.Sum256([]byte(canonicalRequest))
	return Algorithm + "\n" + t.UTC().Format(TimeFormat) + "\n" + scope + "\n" + hex.EncodeToString(h[:])
}

// SigningKey derives the SigV4 signing key.
func SigningKey(secret string, t time.Time, region, service string) []byte {
	k := hmacSHA256([]byte("AWS4"+secret), t.UTC().Format(dateFormat))
	k = hmacSHA256(k, region)
	k = hmacSHA256(k, service)
	return hmacSHA256(k, "aws4_request")
}

// Signature returns the hex signature of stringToSign.
func Signature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(hmacSHA256(signingKey, stringToSign))
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

// HashHex returns the hex SHA-256 of b.
func HashHex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// Escape URI-encodes s per AWS rules: every byte except the unreserved
// characters A-Z a-z 0-9 - _ . ~ is percent-encoded with upper-case hex.
func Escape(s string) string { return escape(s, false) }

// EscapePath is like Escape but keeps '/' unencoded.
func EscapePath(s string) string { return escape(s, true) }

func escape(s string, keepSlash bool) string {
	const hexChars = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s) + 8)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case 'A' <= c && c <= 'Z', 'a' <= c && c <= 'z', '0' <= c && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && keepSlash:
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexChars[c>>4])
			b.WriteByte(hexChars[c&15])
		}
	}
	return b.String()
}
