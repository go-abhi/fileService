package s3

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-abhi/fileService/internal/s3/sigv4"
)

func envMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestChainEnv(t *testing.T) {
	p := DefaultCredentials(ChainOptions{Getenv: envMap(map[string]string{
		"AWS_ACCESS_KEY_ID": "AK", "AWS_SECRET_ACCESS_KEY": "SK", "AWS_SESSION_TOKEN": "TOK",
	})})
	c, err := p.Retrieve(context.Background())
	if err != nil || c.AccessKeyID != "AK" || c.SecretAccessKey != "SK" || c.SessionToken != "TOK" {
		t.Fatalf("got %+v %v", c, err)
	}
}

func TestChainSharedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials")
	os.WriteFile(path, []byte("[default]\naws_access_key_id = D\naws_secret_access_key = DS\n\n# comment\n[prod]\naws_access_key_id=P\naws_secret_access_key=PS\naws_session_token = PT\n"), 0o600)
	p := DefaultCredentials(ChainOptions{Getenv: envMap(map[string]string{
		"AWS_SHARED_CREDENTIALS_FILE": path, "AWS_PROFILE": "prod",
	})})
	c, err := p.Retrieve(context.Background())
	if err != nil || c.AccessKeyID != "P" || c.SessionToken != "PT" {
		t.Fatalf("got %+v %v", c, err)
	}
}

func TestChainWebIdentity(t *testing.T) {
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	os.WriteFile(tokenFile, []byte("jwt-token\n"), 0o600)
	exp := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	var calls atomic.Int32
	sts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		r.ParseForm()
		if r.Form.Get("Action") != "AssumeRoleWithWebIdentity" || r.Form.Get("WebIdentityToken") != "jwt-token" ||
			r.Form.Get("RoleArn") != "arn:aws:iam::1:role/r" || r.Header.Get("Authorization") != "" {
			http.Error(w, "bad request", 400)
			return
		}
		io.WriteString(w, `<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithWebIdentityResult><Credentials>
    <AccessKeyId>ASIA</AccessKeyId><SecretAccessKey>S</SecretAccessKey><SessionToken>T</SessionToken><Expiration>`+exp+`</Expiration>
  </Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`)
	}))
	defer sts.Close()
	p := DefaultCredentials(ChainOptions{Getenv: envMap(map[string]string{
		"AWS_WEB_IDENTITY_TOKEN_FILE": tokenFile, "AWS_ROLE_ARN": "arn:aws:iam::1:role/r", "AWS_ENDPOINT_URL_STS": sts.URL,
	})})
	for i := 0; i < 3; i++ {
		c, err := p.Retrieve(context.Background())
		if err != nil || c.AccessKeyID != "ASIA" || c.SessionToken != "T" || c.Expires.IsZero() {
			t.Fatalf("got %+v %v", c, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("expected cached credentials, sts called %d times", calls.Load())
	}
}

func TestChainContainerFullURIWithTokenFile(t *testing.T) {
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	os.WriteFile(tokenFile, []byte("pod-identity-token"), 0o600)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "pod-identity-token" {
			http.Error(w, "unauthorized", 401)
			return
		}
		io.WriteString(w, `{"AccessKeyId":"C","SecretAccessKey":"CS","Token":"CT","Expiration":"`+time.Now().Add(time.Hour).UTC().Format(time.RFC3339)+`"}`)
	}))
	defer srv.Close()
	p := DefaultCredentials(ChainOptions{Getenv: envMap(map[string]string{
		"AWS_CONTAINER_CREDENTIALS_FULL_URI": srv.URL + "/creds", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE": tokenFile,
	})})
	c, err := p.Retrieve(context.Background())
	if err != nil || c.AccessKeyID != "C" || c.SessionToken != "CT" {
		t.Fatalf("got %+v %v", c, err)
	}
}

func TestChainIMDSv2(t *testing.T) {
	const token = "imds-token"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/latest/api/token":
			if r.Header.Get("X-Aws-Ec2-Metadata-Token-Ttl-Seconds") == "" {
				http.Error(w, "missing ttl", 400)
				return
			}
			io.WriteString(w, token)
		case r.Header.Get("X-Aws-Ec2-Metadata-Token") != token:
			http.Error(w, "unauthorized", 401)
		case r.URL.Path == "/latest/meta-data/iam/security-credentials/":
			io.WriteString(w, "my-role\n")
		case r.URL.Path == "/latest/meta-data/iam/security-credentials/my-role":
			io.WriteString(w, `{"Code":"Success","AccessKeyId":"I","SecretAccessKey":"IS","Token":"IT","Expiration":"`+time.Now().Add(time.Hour).UTC().Format(time.RFC3339)+`"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	p := DefaultCredentials(ChainOptions{Getenv: envMap(map[string]string{"AWS_EC2_METADATA_SERVICE_ENDPOINT": srv.URL})})
	c, err := p.Retrieve(context.Background())
	if err != nil || c.AccessKeyID != "I" || c.Source != "imds" {
		t.Fatalf("got %+v %v", c, err)
	}
}

func TestChainNothingConfigured(t *testing.T) {
	p := DefaultCredentials(ChainOptions{Getenv: envMap(map[string]string{"AWS_EC2_METADATA_DISABLED": "true"})})
	if _, err := p.Retrieve(context.Background()); err == nil || !strings.Contains(err.Error(), "no AWS credentials") {
		t.Fatalf("err = %v", err)
	}
}

func TestCachingRefreshesBeforeExpiry(t *testing.T) {
	now := time.Now()
	var n atomic.Int32
	p := &cachingProvider{now: func() time.Time { return now }, fetch: func(context.Context) (sigv4.Credentials, error) {
		i := n.Add(1)
		return sigv4.Credentials{AccessKeyID: string(rune('A' + i)), SecretAccessKey: "s", Expires: now.Add(10 * time.Minute)}, nil
	}}
	c1, _ := p.Retrieve(context.Background())
	c2, _ := p.Retrieve(context.Background())
	if c1.AccessKeyID != c2.AccessKeyID {
		t.Fatal("should be cached")
	}
	now = now.Add(6 * time.Minute) // within the 5 minute refresh window
	c3, _ := p.Retrieve(context.Background())
	if c3.AccessKeyID == c1.AccessKeyID {
		t.Fatal("should have refreshed")
	}
}
