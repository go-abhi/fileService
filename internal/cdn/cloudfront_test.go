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
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-abhi/fileService/internal/core"
)

func testKey(t *testing.T) (*rsa.PrivateKey, []byte, []byte) {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pkcs1 := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})
	der, _ := x509.MarshalPKCS8PrivateKey(k)
	pkcs8 := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	return k, pkcs1, pkcs8
}

// verify checks a signed URL the way CloudFront does for canned policies.
func verify(t *testing.T, pub *rsa.PublicKey, signed string) (resource string, expires string) {
	t.Helper()
	i := strings.LastIndex(signed, "Expires=")
	if i < 1 {
		t.Fatalf("no Expires in %s", signed)
	}
	resource = signed[:i-1]
	q, err := url.ParseQuery(signed[i:])
	if err != nil {
		t.Fatal(err)
	}
	expires = q.Get("Expires")
	sigStr := strings.NewReplacer("-", "+", "_", "=", "~", "/").Replace(q.Get("Signature"))
	sig, err := base64.StdEncoding.DecodeString(sigStr)
	if err != nil {
		t.Fatal(err)
	}
	h := sha1.Sum([]byte(CannedPolicy(resource, expires)))
	if err := rsa.VerifyPKCS1v15(pub, crypto.SHA1, h[:], sig); err != nil {
		t.Fatalf("signature does not verify: %v", err)
	}
	if q.Get("Key-Pair-Id") != "K2JCJMDEHXQW5F" {
		t.Fatalf("key pair id = %q", q.Get("Key-Pair-Id"))
	}
	return resource, expires
}

func TestSignURL(t *testing.T) {
	k, pkcs1, pkcs8 := testKey(t)
	for _, pemKey := range [][]byte{pkcs1, pkcs8} {
		c, err := NewCloudFront("d111111abcdef8.cloudfront.net", "K2JCJMDEHXQW5F", pemKey)
		if err != nil {
			t.Fatal(err)
		}
		now := time.Unix(1_700_000_000, 0)
		c.now = func() time.Time { return now }
		u, err := c.SignURL(context.Background(), "files/acme/0190f0a1-2b3c-7d4e-8f50-123456789abc", 10*time.Minute, core.DownloadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		res, exp := verify(t, &k.PublicKey, u.URL)
		if res != "https://d111111abcdef8.cloudfront.net/files/acme/0190f0a1-2b3c-7d4e-8f50-123456789abc" || exp != "1700000600" {
			t.Fatalf("resource %s expires %s", res, exp)
		}
		if !u.ExpiresAt.Equal(now.Add(10*time.Minute)) || !c.CDN() {
			t.Fatalf("expires at %v", u.ExpiresAt)
		}
	}
}

func TestSignURLWithDisposition(t *testing.T) {
	k, pkcs1, _ := testKey(t)
	c, err := NewCloudFront("https://cdn.example.com/", "K2JCJMDEHXQW5F", pkcs1)
	if err != nil {
		t.Fatal(err)
	}
	u, err := c.SignURL(context.Background(), "ns/id", time.Hour, core.DownloadOptions{ContentDisposition: core.ContentDisposition("attachment", "résumé.pdf")})
	if err != nil {
		t.Fatal(err)
	}
	res, _ := verify(t, &k.PublicKey, u.URL)
	pu, _ := url.Parse(res)
	if pu.Host != "cdn.example.com" || pu.Path != "/ns/id" || !strings.HasPrefix(pu.Query().Get("response-content-disposition"), "attachment; filename*=utf-8''r%C3%A9sum%C3%A9.pdf") {
		t.Fatalf("resource = %s", res)
	}
	if strings.Count(u.URL, "?") != 1 {
		t.Fatalf("malformed query: %s", u.URL)
	}
}

func TestBadKeys(t *testing.T) {
	if _, err := NewCloudFront("cdn", "id", []byte("not pem")); err == nil {
		t.Fatal("expected error for non-PEM key")
	}
	if _, err := NewCloudFront("", "id", nil); err == nil {
		t.Fatal("expected error for missing domain")
	}
}
