// Package cdn signs download URLs for a CDN in front of the bucket.
package cdn

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-abhi/fileService/internal/core"
	"github.com/go-abhi/fileService/internal/s3/sigv4"
)

// CloudFront produces CloudFront signed URLs (canned policy) for objects
// served through a distribution whose origin is the bucket (with Origin
// Access Control) and that restricts viewer access to a trusted key group.
type CloudFront struct {
	base      string
	keyPairID string
	key       *rsa.PrivateKey
	now       func() time.Time
}

// NewCloudFront returns a signer. domain is the distribution domain or URL
// (e.g. "d111111abcdef8.cloudfront.net" or "https://cdn.example.com"),
// keyPairID the ID of the public key in the trusted key group, and
// privateKeyPEM the matching RSA private key (PKCS#1 or PKCS#8 PEM).
func NewCloudFront(domain, keyPairID string, privateKeyPEM []byte) (*CloudFront, error) {
	if domain == "" || keyPairID == "" {
		return nil, errors.New("cdn: domain and key pair ID are required")
	}
	if !strings.Contains(domain, "://") {
		domain = "https://" + domain
	}
	u, err := url.Parse(domain)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("cdn: invalid domain %q", domain)
	}
	key, err := ParseRSAPrivateKey(privateKeyPEM)
	if err != nil {
		return nil, err
	}
	return &CloudFront{base: u.Scheme + "://" + u.Host + strings.TrimRight(u.Path, "/"), keyPairID: keyPairID, key: key, now: time.Now}, nil
}

// ParseRSAPrivateKey parses a PEM-encoded RSA key in PKCS#1 or PKCS#8 form.
func ParseRSAPrivateKey(pemBytes []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("cdn: private key is not PEM encoded")
	}
	if k, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return k, nil
	}
	k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("cdn: parsing private key: %w", err)
	}
	rk, ok := k.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("cdn: CloudFront requires an RSA private key")
	}
	return rk, nil
}

// CDN implements core.URLSigner.
func (c *CloudFront) CDN() bool { return true }

// SignURL implements core.URLSigner.
func (c *CloudFront) SignURL(_ context.Context, key string, ttl time.Duration, opts core.DownloadOptions) (core.SignedURL, error) {
	resource := c.base + "/" + sigv4.EscapePath(key)
	if opts.ContentDisposition != "" {
		// Honoured when the distribution forwards this query string to S3.
		resource += "?response-content-disposition=" + sigv4.Escape(opts.ContentDisposition)
	}
	expires := c.now().Add(ttl).Truncate(time.Second)
	signed, err := c.sign(resource, expires)
	if err != nil {
		return core.SignedURL{}, err
	}
	return core.SignedURL{URL: signed, Method: "GET", ExpiresAt: expires}, nil
}

// sign appends Expires, Signature and Key-Pair-Id to resource.
func (c *CloudFront) sign(resource string, expires time.Time) (string, error) {
	exp := strconv.FormatInt(expires.Unix(), 10)
	policy := CannedPolicy(resource, exp)
	h := sha1.Sum([]byte(policy))
	sig, err := rsa.SignPKCS1v15(rand.Reader, c.key, crypto.SHA1, h[:])
	if err != nil {
		return "", fmt.Errorf("cdn: signing: %w", err)
	}
	sep := "?"
	if strings.Contains(resource, "?") {
		sep = "&"
	}
	return resource + sep + "Expires=" + exp + "&Signature=" + EncodeSignature(sig) + "&Key-Pair-Id=" + url.QueryEscape(c.keyPairID), nil
}

// CannedPolicy returns the canned policy CloudFront reconstructs to verify a
// signed URL. The resource is inserted verbatim (URLs never contain '"' or
// '\'), exactly as CloudFront does.
func CannedPolicy(resource, expires string) string {
	return `{"Statement":[{"Resource":"` + resource + `","Condition":{"DateLessThan":{"AWS:EpochTime":` + expires + `}}}]}`
}

// EncodeSignature applies CloudFront's URL-safe base64 variant.
func EncodeSignature(sig []byte) string {
	return strings.NewReplacer("+", "-", "=", "_", "/", "~").Replace(base64.StdEncoding.EncodeToString(sig))
}
