package s3

import (
	"bufio"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-abhi/fileService/internal/s3/sigv4"
)

// CredentialsProvider supplies AWS credentials.
type CredentialsProvider interface {
	Retrieve(ctx context.Context) (sigv4.Credentials, error)
}

// StaticCredentials always returns the same credentials.
type StaticCredentials sigv4.Credentials

// Retrieve implements CredentialsProvider.
func (s StaticCredentials) Retrieve(context.Context) (sigv4.Credentials, error) {
	c := sigv4.Credentials(s)
	if !c.Valid() {
		return c, errors.New("s3: static credentials are empty")
	}
	return c, nil
}

// refreshWindow is how long before expiry temporary credentials are renewed.
const refreshWindow = 5 * time.Minute

// cachingProvider caches temporary credentials until shortly before expiry.
type cachingProvider struct {
	fetch func(ctx context.Context) (sigv4.Credentials, error)
	now   func() time.Time

	mu     sync.Mutex
	cached sigv4.Credentials
}

func (p *cachingProvider) Retrieve(ctx context.Context) (sigv4.Credentials, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cached.Valid() && !p.cached.ExpiresWithin(p.now(), refreshWindow) {
		return p.cached, nil
	}
	c, err := p.fetch(ctx)
	if err != nil {
		// Keep serving still-valid credentials if a refresh fails.
		if p.cached.Valid() && !p.cached.ExpiresWithin(p.now(), 0) {
			return p.cached, nil
		}
		return sigv4.Credentials{}, err
	}
	p.cached = c
	return c, nil
}

// ChainOptions configures DefaultCredentials. Zero values use the process
// environment and AWS defaults.
type ChainOptions struct {
	Region     string
	Getenv     func(string) string
	HTTPClient *http.Client
	Now        func() time.Time
}

// DefaultCredentials returns the standard AWS credential chain:
//
//  1. Environment: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
//  2. Shared credentials file (AWS_SHARED_CREDENTIALS_FILE or ~/.aws/credentials,
//     profile AWS_PROFILE or "default"), static keys only
//  3. Web identity / EKS IRSA: AWS_WEB_IDENTITY_TOKEN_FILE + AWS_ROLE_ARN
//  4. Container credentials: ECS task roles and EKS Pod Identity
//     (AWS_CONTAINER_CREDENTIALS_RELATIVE_URI / _FULL_URI)
//  5. EC2 instance profile via IMDSv2 (unless AWS_EC2_METADATA_DISABLED=true)
//
// The first source that is configured is used for the life of the provider.
func DefaultCredentials(opts ChainOptions) CredentialsProvider {
	if opts.Getenv == nil {
		opts.Getenv = os.Getenv
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &chainProvider{opts: opts}
}

type chainProvider struct {
	opts ChainOptions

	mu       sync.Mutex
	resolved CredentialsProvider
}

func (c *chainProvider) Retrieve(ctx context.Context) (sigv4.Credentials, error) {
	c.mu.Lock()
	p := c.resolved
	c.mu.Unlock()
	if p != nil {
		return p.Retrieve(ctx)
	}
	p, err := c.resolve(ctx)
	if err != nil {
		return sigv4.Credentials{}, err
	}
	c.mu.Lock()
	c.resolved = p
	c.mu.Unlock()
	return p.Retrieve(ctx)
}

func (c *chainProvider) resolve(ctx context.Context) (CredentialsProvider, error) {
	env := c.opts.Getenv
	// 1. Environment.
	if ak, sk := env("AWS_ACCESS_KEY_ID"), env("AWS_SECRET_ACCESS_KEY"); ak != "" && sk != "" {
		return StaticCredentials{AccessKeyID: ak, SecretAccessKey: sk, SessionToken: env("AWS_SESSION_TOKEN"), Source: "env"}, nil
	}
	// 2. Shared credentials file.
	if creds, ok, err := sharedFileCredentials(env); err != nil {
		return nil, err
	} else if ok {
		return StaticCredentials(creds), nil
	}
	// 3. Web identity (EKS IRSA).
	if tokenFile, role := env("AWS_WEB_IDENTITY_TOKEN_FILE"), env("AWS_ROLE_ARN"); tokenFile != "" && role != "" {
		w := &webIdentity{
			tokenFile:   tokenFile,
			roleARN:     role,
			sessionName: env("AWS_ROLE_SESSION_NAME"),
			endpoint:    stsEndpoint(env, c.opts.Region),
			hc:          c.opts.HTTPClient,
		}
		return &cachingProvider{fetch: w.fetch, now: c.opts.Now}, nil
	}
	// 4. Container credentials (ECS, EKS Pod Identity).
	if cp := newContainerProvider(env, c.opts.HTTPClient); cp != nil {
		return &cachingProvider{fetch: cp.fetch, now: c.opts.Now}, nil
	}
	// 5. EC2 instance metadata.
	if !strings.EqualFold(env("AWS_EC2_METADATA_DISABLED"), "true") {
		endpoint := env("AWS_EC2_METADATA_SERVICE_ENDPOINT")
		if endpoint == "" {
			endpoint = "http://169.254.169.254"
		}
		im := &imds{endpoint: strings.TrimRight(endpoint, "/"), hc: &http.Client{Timeout: 2 * time.Second}}
		cp := &cachingProvider{fetch: im.fetch, now: c.opts.Now}
		if _, err := cp.Retrieve(ctx); err == nil {
			return cp, nil
		}
	}
	return nil, errors.New("s3: no AWS credentials found (set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, a profile, or run with an IAM role)")
}

// --- shared credentials file -------------------------------------------------

func sharedFileCredentials(env func(string) string) (sigv4.Credentials, bool, error) {
	path := env("AWS_SHARED_CREDENTIALS_FILE")
	if path == "" {
		home := env("HOME")
		if home == "" {
			return sigv4.Credentials{}, false, nil
		}
		path = filepath.Join(home, ".aws", "credentials")
	}
	profile := env("AWS_PROFILE")
	if profile == "" {
		profile = "default"
	}
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return sigv4.Credentials{}, false, nil
		}
		return sigv4.Credentials{}, false, fmt.Errorf("s3: reading %s: %w", path, err)
	}
	defer f.Close()
	values, err := parseINISection(f, profile)
	if err != nil {
		return sigv4.Credentials{}, false, fmt.Errorf("s3: parsing %s: %w", path, err)
	}
	creds := sigv4.Credentials{
		AccessKeyID:     values["aws_access_key_id"],
		SecretAccessKey: values["aws_secret_access_key"],
		SessionToken:    values["aws_session_token"],
		Source:          "shared-file:" + profile,
	}
	return creds, creds.Valid(), nil
}

func parseINISection(r io.Reader, section string) (map[string]string, error) {
	out := map[string]string{}
	in := false
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || line[0] == '#' || line[0] == ';' {
			continue
		}
		if line[0] == '[' && line[len(line)-1] == ']' {
			in = strings.TrimSpace(line[1:len(line)-1]) == section
			continue
		}
		if !in {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		out[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
	}
	return out, sc.Err()
}

// --- web identity (STS AssumeRoleWithWebIdentity) ----------------------------

func stsEndpoint(env func(string) string, region string) string {
	if e := env("AWS_ENDPOINT_URL_STS"); e != "" {
		return e
	}
	if region == "" {
		region = env("AWS_REGION")
	}
	if region == "" {
		region = env("AWS_DEFAULT_REGION")
	}
	if region == "" {
		return "https://sts.amazonaws.com"
	}
	return "https://sts." + region + "." + dnsSuffix(region)
}

type webIdentity struct {
	tokenFile, roleARN, sessionName, endpoint string
	hc                                        *http.Client
}

func (w *webIdentity) fetch(ctx context.Context) (sigv4.Credentials, error) {
	token, err := os.ReadFile(w.tokenFile)
	if err != nil {
		return sigv4.Credentials{}, fmt.Errorf("s3: reading web identity token: %w", err)
	}
	session := w.sessionName
	if session == "" {
		session = fmt.Sprintf("fileservice-%d", time.Now().UnixNano())
	}
	form := url.Values{
		"Action":           {"AssumeRoleWithWebIdentity"},
		"Version":          {"2011-06-15"},
		"RoleArn":          {w.roleARN},
		"RoleSessionName":  {session},
		"WebIdentityToken": {strings.TrimSpace(string(token))},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return sigv4.Credentials{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := w.hc.Do(req)
	if err != nil {
		return sigv4.Credentials{}, fmt.Errorf("s3: sts AssumeRoleWithWebIdentity: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return sigv4.Credentials{}, fmt.Errorf("s3: sts AssumeRoleWithWebIdentity: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out struct {
		Result struct {
			Credentials struct {
				AccessKeyID     string `xml:"AccessKeyId"`
				SecretAccessKey string `xml:"SecretAccessKey"`
				SessionToken    string `xml:"SessionToken"`
				Expiration      string `xml:"Expiration"`
			} `xml:"Credentials"`
		} `xml:"AssumeRoleWithWebIdentityResult"`
	}
	if err := xml.Unmarshal(body, &out); err != nil {
		return sigv4.Credentials{}, fmt.Errorf("s3: decoding sts response: %w", err)
	}
	c := out.Result.Credentials
	exp, _ := time.Parse(time.RFC3339, c.Expiration)
	creds := sigv4.Credentials{AccessKeyID: c.AccessKeyID, SecretAccessKey: c.SecretAccessKey, SessionToken: c.SessionToken, Expires: exp, Source: "web-identity"}
	if !creds.Valid() {
		return sigv4.Credentials{}, errors.New("s3: sts returned empty credentials")
	}
	return creds, nil
}

// --- container credentials (ECS / EKS Pod Identity) ---------------------------

type containerProvider struct {
	uri       string
	token     string
	tokenFile string
	hc        *http.Client
}

func newContainerProvider(env func(string) string, hc *http.Client) *containerProvider {
	p := &containerProvider{
		token:     env("AWS_CONTAINER_AUTHORIZATION_TOKEN"),
		tokenFile: env("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"),
		hc:        hc,
	}
	switch {
	case env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") != "":
		p.uri = "http://169.254.170.2" + env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
	case env("AWS_CONTAINER_CREDENTIALS_FULL_URI") != "":
		p.uri = env("AWS_CONTAINER_CREDENTIALS_FULL_URI")
	default:
		return nil
	}
	return p
}

type jsonCredentials struct {
	AccessKeyID     string `json:"AccessKeyId"`
	SecretAccessKey string `json:"SecretAccessKey"`
	Token           string `json:"Token"`
	Expiration      string `json:"Expiration"`
}

func (j jsonCredentials) toCreds(source string) (sigv4.Credentials, error) {
	exp, _ := time.Parse(time.RFC3339, j.Expiration)
	c := sigv4.Credentials{AccessKeyID: j.AccessKeyID, SecretAccessKey: j.SecretAccessKey, SessionToken: j.Token, Expires: exp, Source: source}
	if !c.Valid() {
		return c, fmt.Errorf("s3: %s returned empty credentials", source)
	}
	return c, nil
}

func (p *containerProvider) fetch(ctx context.Context) (sigv4.Credentials, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.uri, nil)
	if err != nil {
		return sigv4.Credentials{}, err
	}
	token := p.token
	if p.tokenFile != "" {
		b, err := os.ReadFile(p.tokenFile)
		if err != nil {
			return sigv4.Credentials{}, fmt.Errorf("s3: reading container auth token: %w", err)
		}
		token = strings.TrimSpace(string(b))
	}
	if token != "" {
		req.Header.Set("Authorization", token)
	}
	resp, err := p.hc.Do(req)
	if err != nil {
		return sigv4.Credentials{}, fmt.Errorf("s3: container credentials: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return sigv4.Credentials{}, fmt.Errorf("s3: container credentials: status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var j jsonCredentials
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&j); err != nil {
		return sigv4.Credentials{}, fmt.Errorf("s3: decoding container credentials: %w", err)
	}
	return j.toCreds("container")
}

// --- EC2 instance metadata (IMDSv2) -------------------------------------------

type imds struct {
	endpoint string
	hc       *http.Client
}

func (m *imds) fetch(ctx context.Context) (sigv4.Credentials, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, m.endpoint+"/latest/api/token", nil)
	if err != nil {
		return sigv4.Credentials{}, err
	}
	req.Header.Set("X-Aws-Ec2-Metadata-Token-Ttl-Seconds", "21600")
	token, err := m.text(req)
	if err != nil {
		return sigv4.Credentials{}, fmt.Errorf("s3: imds token: %w", err)
	}
	get := func(path string) (string, error) {
		r, err := http.NewRequestWithContext(ctx, http.MethodGet, m.endpoint+path, nil)
		if err != nil {
			return "", err
		}
		r.Header.Set("X-Aws-Ec2-Metadata-Token", token)
		return m.text(r)
	}
	roles, err := get("/latest/meta-data/iam/security-credentials/")
	if err != nil {
		return sigv4.Credentials{}, fmt.Errorf("s3: imds role: %w", err)
	}
	role := strings.TrimSpace(strings.SplitN(roles, "\n", 2)[0])
	if role == "" {
		return sigv4.Credentials{}, errors.New("s3: imds: no instance role attached")
	}
	body, err := get("/latest/meta-data/iam/security-credentials/" + role)
	if err != nil {
		return sigv4.Credentials{}, fmt.Errorf("s3: imds credentials: %w", err)
	}
	var j jsonCredentials
	if err := json.Unmarshal([]byte(body), &j); err != nil {
		return sigv4.Credentials{}, fmt.Errorf("s3: decoding imds credentials: %w", err)
	}
	return j.toCreds("imds")
}

func (m *imds) text(req *http.Request) (string, error) {
	resp, err := m.hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	return string(b), nil
}
