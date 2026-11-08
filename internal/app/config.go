package app

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-abhi/fileService/internal/core"
)

// Config is the server configuration, read from FILESERVICE_* environment
// variables (see ConfigFromEnv and the README).
type Config struct {
	HTTPAddr    string // REST + gRPC (h2c) listener
	GRPCAddr    string // gRPC-only listener ("" disables)
	TLSCertFile string
	TLSKeyFile  string
	LogLevel    string
	LogFormat   string

	S3Bucket          string
	S3Region          string
	S3Endpoint        string
	S3PublicEndpoint  string
	S3PathStyle       bool
	S3Accelerate      bool
	S3SSE             string
	S3SSEKMSKeyID     string
	S3CreateBucket    bool
	MetadataStore     string // "s3" or "memory"
	StartupRetryLimit time.Duration

	APIKeys          string
	AuthDisabled     bool
	DefaultNamespace string

	CDNDomain         string
	CDNKeyPairID      string
	CDNPrivateKey     string
	CDNPrivateKeyFile string

	WebhookURL    string
	WebhookSecret string
	CORSOrigins   []string

	Core            core.Config
	JanitorInterval time.Duration

	// Dev runs an embedded in-memory S3 on DevS3Addr; nothing else is needed.
	Dev       bool
	DevS3Addr string
}

// ConfigFromEnv builds a Config from environment variables.
func ConfigFromEnv(getenv func(string) string) (Config, error) {
	e := env{get: getenv}
	c := Config{
		HTTPAddr:          e.str("FILESERVICE_HTTP_ADDR", ":8080"),
		GRPCAddr:          e.str("FILESERVICE_GRPC_ADDR", ":9090"),
		TLSCertFile:       e.str("FILESERVICE_TLS_CERT_FILE", ""),
		TLSKeyFile:        e.str("FILESERVICE_TLS_KEY_FILE", ""),
		LogLevel:          e.str("FILESERVICE_LOG_LEVEL", "info"),
		LogFormat:         e.str("FILESERVICE_LOG_FORMAT", "json"),
		S3Bucket:          e.str("FILESERVICE_S3_BUCKET", ""),
		S3Region:          e.str("FILESERVICE_S3_REGION", e.str("AWS_REGION", e.str("AWS_DEFAULT_REGION", "us-east-1"))),
		S3Endpoint:        e.str("FILESERVICE_S3_ENDPOINT", ""),
		S3PublicEndpoint:  e.str("FILESERVICE_S3_PUBLIC_ENDPOINT", ""),
		S3SSE:             e.str("FILESERVICE_S3_SSE", ""),
		S3SSEKMSKeyID:     e.str("FILESERVICE_S3_SSE_KMS_KEY_ID", ""),
		MetadataStore:     e.str("FILESERVICE_METADATA_STORE", "s3"),
		APIKeys:           e.str("FILESERVICE_API_KEYS", ""),
		DefaultNamespace:  e.str("FILESERVICE_DEFAULT_NAMESPACE", "default"),
		CDNDomain:         e.str("FILESERVICE_CDN_DOMAIN", ""),
		CDNKeyPairID:      e.str("FILESERVICE_CDN_KEY_PAIR_ID", ""),
		CDNPrivateKey:     e.str("FILESERVICE_CDN_PRIVATE_KEY", ""),
		CDNPrivateKeyFile: e.str("FILESERVICE_CDN_PRIVATE_KEY_FILE", ""),
		WebhookURL:        e.str("FILESERVICE_WEBHOOK_URL", ""),
		WebhookSecret:     e.str("FILESERVICE_WEBHOOK_SECRET", ""),
		CORSOrigins:       e.list("FILESERVICE_CORS_ORIGINS"),
		DevS3Addr:         e.str("FILESERVICE_DEV_S3_ADDR", "127.0.0.1:9000"),
	}
	c.S3PathStyle = e.bool("FILESERVICE_S3_PATH_STYLE", c.S3Endpoint != "")
	c.S3Accelerate = e.bool("FILESERVICE_S3_ACCELERATE", false)
	c.S3CreateBucket = e.bool("FILESERVICE_S3_CREATE_BUCKET", false)
	c.AuthDisabled = e.bool("FILESERVICE_AUTH_DISABLED", false)
	c.Dev = e.bool("FILESERVICE_DEV", false)
	if c.Dev { // dev mode may run without auth: stay on loopback unless told otherwise
		c.HTTPAddr = e.str("FILESERVICE_HTTP_ADDR", "127.0.0.1:8080")
		c.GRPCAddr = e.str("FILESERVICE_GRPC_ADDR", "127.0.0.1:9090")
	}
	c.StartupRetryLimit = e.dur("FILESERVICE_STARTUP_TIMEOUT", time.Minute)
	c.JanitorInterval = e.dur("FILESERVICE_JANITOR_INTERVAL", 10*time.Minute)
	c.Core = core.Config{
		KeyPrefix:           e.str("FILESERVICE_S3_PREFIX", ""),
		DefaultPartSize:     e.size("FILESERVICE_PART_SIZE", 16*core.MiB),
		MaxFileSize:         e.size("FILESERVICE_MAX_FILE_SIZE", 5*core.TiB),
		UploadURLTTL:        e.dur("FILESERVICE_UPLOAD_URL_TTL", time.Hour),
		UploadExpiry:        e.dur("FILESERVICE_UPLOAD_EXPIRY", 24*time.Hour),
		DownloadURLTTL:      e.dur("FILESERVICE_DOWNLOAD_URL_TTL", 15*time.Minute),
		MaxDownloadURLTTL:   e.dur("FILESERVICE_MAX_DOWNLOAD_URL_TTL", 7*24*time.Hour),
		StreamPartSize:      e.size("FILESERVICE_STREAM_PART_SIZE", 16*core.MiB),
		StreamConcurrency:   e.int("FILESERVICE_STREAM_CONCURRENCY", 4),
		StreamBufferBudget:  e.size("FILESERVICE_STREAM_BUFFER_BUDGET", 64*core.MiB),
		AllowedContentTypes: e.list("FILESERVICE_ALLOWED_CONTENT_TYPES"),
		CacheControl:        e.str("FILESERVICE_CACHE_CONTROL", ""),
	}
	if c.GRPCAddr == "off" {
		c.GRPCAddr = ""
	}
	if len(e.errs) > 0 {
		return c, errors.Join(e.errs...)
	}
	return c, c.Validate()
}

// Validate checks the configuration.
func (c Config) Validate() error {
	var errs []error
	if c.S3Bucket == "" && !c.Dev {
		errs = append(errs, errors.New("FILESERVICE_S3_BUCKET is required (or set FILESERVICE_DEV=true to run with an in-memory S3)"))
	}
	if c.APIKeys == "" && !c.AuthDisabled && !c.Dev {
		errs = append(errs, errors.New("FILESERVICE_API_KEYS is required (namespace:key,...), or set FILESERVICE_AUTH_DISABLED=true"))
	}
	if c.MetadataStore != "s3" && c.MetadataStore != "memory" {
		errs = append(errs, fmt.Errorf("FILESERVICE_METADATA_STORE must be s3 or memory, got %q", c.MetadataStore))
	}
	if c.CDNDomain != "" && (c.CDNKeyPairID == "" || (c.CDNPrivateKey == "" && c.CDNPrivateKeyFile == "")) {
		errs = append(errs, errors.New("FILESERVICE_CDN_DOMAIN requires FILESERVICE_CDN_KEY_PAIR_ID and FILESERVICE_CDN_PRIVATE_KEY(_FILE)"))
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		errs = append(errs, errors.New("FILESERVICE_TLS_CERT_FILE and FILESERVICE_TLS_KEY_FILE must be set together"))
	}
	if c.HTTPAddr == "" && c.GRPCAddr == "" {
		errs = append(errs, errors.New("at least one of FILESERVICE_HTTP_ADDR and FILESERVICE_GRPC_ADDR must be set"))
	}
	if c.Core.DefaultPartSize > 0 && c.Core.DefaultPartSize < 5*core.MiB {
		errs = append(errs, errors.New("FILESERVICE_PART_SIZE must be at least 5MiB (S3 minimum)"))
	}
	return errors.Join(errs...)
}

type env struct {
	get  func(string) string
	errs []error
}

func (e *env) str(key, def string) string {
	if v := strings.TrimSpace(e.get(key)); v != "" {
		return v
	}
	return def
}

func (e *env) list(key string) []string {
	var out []string
	for _, p := range strings.Split(e.get(key), ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (e *env) bool(key string, def bool) bool {
	v := e.str(key, "")
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: invalid boolean %q", key, v))
	}
	return b
}

func (e *env) int(key string, def int) int {
	v := e.str(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		e.errs = append(e.errs, fmt.Errorf("%s: invalid number %q", key, v))
	}
	return n
}

func (e *env) dur(key string, def time.Duration) time.Duration {
	v := e.str(key, "")
	if v == "" {
		return def
	}
	d, err := ParseDuration(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %v", key, err))
	}
	return d
}

func (e *env) size(key string, def int64) int64 {
	v := e.str(key, "")
	if v == "" {
		return def
	}
	n, err := ParseSize(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %v", key, err))
	}
	return n
}

// ParseDuration is time.ParseDuration plus a "d" (days) unit, e.g. "7d".
func ParseDuration(s string) (time.Duration, error) {
	if days, ok := strings.CutSuffix(s, "d"); ok {
		n, err := strconv.ParseFloat(days, 64)
		if err != nil || n < 0 {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
		return time.Duration(n * 24 * float64(time.Hour)), nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("invalid duration %q (use e.g. 90s, 15m, 24h, 7d)", s)
	}
	return d, nil
}

// ParseSize parses byte sizes like "16MiB", "5GiB", "100MB", "1024" or "64k".
// KiB/MiB/GiB/TiB and K/M/G/T are powers of 1024; KB/MB/GB/TB powers of 1000.
func ParseSize(s string) (int64, error) {
	t := strings.TrimSpace(s)
	units := []struct {
		suffix string
		mult   int64
	}{
		{"KiB", core.KiB}, {"MiB", core.MiB}, {"GiB", core.GiB}, {"TiB", core.TiB},
		{"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"TB", 1e12},
		{"K", core.KiB}, {"M", core.MiB}, {"G", core.GiB}, {"T", core.TiB},
		{"k", core.KiB}, {"B", 1},
	}
	mult := int64(1)
	for _, u := range units {
		if num, ok := strings.CutSuffix(t, u.suffix); ok {
			t, mult = strings.TrimSpace(num), u.mult
			break
		}
	}
	n, err := strconv.ParseFloat(t, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid size %q (use e.g. 16MiB, 5GiB)", s)
	}
	return int64(n * float64(mult)), nil
}

// readFileOr returns the content of path if set, else value.
func readFileOr(value, path string) ([]byte, error) {
	if path != "" {
		return os.ReadFile(path)
	}
	return []byte(value), nil
}
