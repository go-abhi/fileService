package core

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"mime"
	"path"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Size units.
const (
	KiB int64 = 1 << 10
	MiB int64 = 1 << 20
	GiB int64 = 1 << 30
	TiB int64 = 1 << 40
)

// Config tunes the service. Zero values take the defaults documented on
// each field (see DefaultConfig).
type Config struct {
	// KeyPrefix is prepended to every storage key (e.g. "files/").
	KeyPrefix string
	// DefaultPartSize for direct uploads (16 MiB).
	DefaultPartSize int64
	// MinPartSize is the smallest part except the last (5 MiB, the S3 minimum).
	MinPartSize int64
	// MaxPartSize is the largest part (5 GiB, the S3 maximum).
	MaxPartSize int64
	// MaxParts per upload (10,000, the S3 maximum).
	MaxParts int
	// MaxFileSize accepted (5 TiB).
	MaxFileSize int64
	// UploadURLTTL is the lifetime of presigned part URLs (1h).
	UploadURLTTL time.Duration
	// UploadExpiry is how long a direct upload may stay incomplete (24h).
	UploadExpiry time.Duration
	// InitialPartURLs returned by CreateUpload (100).
	InitialPartURLs int
	// MaxPartURLsPerRequest for GetUploadPartURLs (1000).
	MaxPartURLsPerRequest int
	// DownloadURLTTL is the default lifetime of download URLs (15m).
	DownloadURLTTL time.Duration
	// MaxDownloadURLTTL caps requested download URL lifetimes (7 days).
	MaxDownloadURLTTL time.Duration
	// StreamPartSize is the part size used for streamed uploads (16 MiB).
	StreamPartSize int64
	// StreamBufferBudget bounds the memory buffered per streamed upload (64 MiB).
	StreamBufferBudget int64
	// StreamConcurrency is the max parts uploaded in parallel per stream (4).
	StreamConcurrency int
	// AllowedContentTypes restricts uploads, e.g. ["image/*", "application/pdf"].
	// Empty allows everything.
	AllowedContentTypes []string
	// CacheControl is stored on objects and served by S3/CDN (empty: none).
	CacheControl string
	// JanitorBatch is how many expired uploads are cleaned per pass (100).
	JanitorBatch int
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	return Config{
		DefaultPartSize:       16 * MiB,
		MinPartSize:           5 * MiB,
		MaxPartSize:           5 * GiB,
		MaxParts:              10000,
		MaxFileSize:           5 * TiB,
		UploadURLTTL:          time.Hour,
		UploadExpiry:          24 * time.Hour,
		InitialPartURLs:       100,
		MaxPartURLsPerRequest: 1000,
		DownloadURLTTL:        15 * time.Minute,
		MaxDownloadURLTTL:     7 * 24 * time.Hour,
		StreamPartSize:        16 * MiB,
		StreamBufferBudget:    64 * MiB,
		StreamConcurrency:     4,
		JanitorBatch:          100,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	set64 := func(v *int64, def int64) {
		if *v <= 0 {
			*v = def
		}
	}
	setInt := func(v *int, def int) {
		if *v <= 0 {
			*v = def
		}
	}
	setDur := func(v *time.Duration, def time.Duration) {
		if *v <= 0 {
			*v = def
		}
	}
	set64(&c.DefaultPartSize, d.DefaultPartSize)
	set64(&c.MinPartSize, d.MinPartSize)
	set64(&c.MaxPartSize, d.MaxPartSize)
	setInt(&c.MaxParts, d.MaxParts)
	set64(&c.MaxFileSize, d.MaxFileSize)
	setDur(&c.UploadURLTTL, d.UploadURLTTL)
	setDur(&c.UploadExpiry, d.UploadExpiry)
	setInt(&c.InitialPartURLs, d.InitialPartURLs)
	setInt(&c.MaxPartURLsPerRequest, d.MaxPartURLsPerRequest)
	setDur(&c.DownloadURLTTL, d.DownloadURLTTL)
	setDur(&c.MaxDownloadURLTTL, d.MaxDownloadURLTTL)
	set64(&c.StreamPartSize, d.StreamPartSize)
	set64(&c.StreamBufferBudget, d.StreamBufferBudget)
	setInt(&c.StreamConcurrency, d.StreamConcurrency)
	setInt(&c.JanitorBatch, d.JanitorBatch)
	if c.KeyPrefix != "" && !strings.HasSuffix(c.KeyPrefix, "/") {
		c.KeyPrefix += "/"
	}
	if c.StreamPartSize < c.MinPartSize {
		c.StreamPartSize = c.MinPartSize
	}
	if c.DefaultPartSize < c.MinPartSize {
		c.DefaultPartSize = c.MinPartSize
	}
	return c
}

// planParts picks the part size and count for a file of size bytes.
func (c Config) planParts(size, requested int64) (int64, int, error) {
	if size <= 0 {
		return 0, 0, nil
	}
	ps := requested
	if ps <= 0 {
		ps = c.DefaultPartSize
	}
	ps = max(ps, c.MinPartSize)
	ps = min(ps, c.MaxPartSize)
	if need := ceilDiv(size, int64(c.MaxParts)); ps < need {
		ps = ceilDiv(need, MiB) * MiB // round up to whole MiB
	}
	if ps > c.MaxPartSize {
		return 0, 0, Errorf(KindTooLarge, "file of %d bytes exceeds the maximum of %d parts of %d bytes", size, c.MaxParts, c.MaxPartSize)
	}
	return ps, int(ceilDiv(size, ps)), nil
}

func ceilDiv(a, b int64) int64 { return (a + b - 1) / b }

// partRange returns the byte range of 1-based part n.
func partRange(n int, partSize, size int64) (offset, length int64) {
	offset = int64(n-1) * partSize
	length = min(partSize, size-offset)
	return offset, length
}

// --- identifiers ----------------------------------------------------------------

// NewID returns a UUIDv7 (time-ordered) string.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	ms := uint64(time.Now().UnixMilli())
	b[0], b[1], b[2], b[3], b[4], b[5] = byte(ms>>40), byte(ms>>32), byte(ms>>24), byte(ms>>16), byte(ms>>8), byte(ms)
	b[6] = b[6]&0x0f | 0x70
	b[8] = b[8]&0x3f | 0x80
	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:], b[10:])
	return string(out[:])
}

// ValidID reports whether id is a canonical lowercase UUID.
func ValidID(id string) bool {
	if len(id) != 36 {
		return false
	}
	for i := 0; i < 36; i++ {
		c := id[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
				return false
			}
		}
	}
	return true
}

// ValidNamespace reports whether ns is usable as a namespace (and storage
// key segment): 1-63 chars of [a-z0-9_-], starting with a letter or digit.
func ValidNamespace(ns string) bool {
	if len(ns) == 0 || len(ns) > 63 {
		return false
	}
	for i := 0; i < len(ns); i++ {
		c := ns[i]
		ok := 'a' <= c && c <= 'z' || '0' <= c && c <= '9' || (i > 0 && (c == '-' || c == '_'))
		if !ok {
			return false
		}
	}
	return true
}

// --- input validation ------------------------------------------------------------

const (
	maxNameBytes      = 1024
	maxMetadataKeys   = 32
	maxMetadataKey    = 64
	maxMetadataValue  = 1024
	maxMetadataTotal  = 8 * 1024
	defaultMIME       = "application/octet-stream"
	dispositionInline = "inline"
)

// cleanName validates a file name and strips any directory components.
func cleanName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if i := strings.LastIndexAny(name, `/\`); i >= 0 {
		name = name[i+1:]
	}
	switch {
	case name == "" || name == "." || name == "..":
		return "", Errorf(KindInvalidArgument, "name is required")
	case len(name) > maxNameBytes:
		return "", Errorf(KindInvalidArgument, "name is longer than %d bytes", maxNameBytes)
	case !utf8.ValidString(name):
		return "", Errorf(KindInvalidArgument, "name is not valid UTF-8")
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return "", Errorf(KindInvalidArgument, "name contains control characters")
		}
	}
	return name, nil
}

// contentType validates ct, or detects it from the file name.
func (c Config) contentType(ct, name string) (string, error) {
	if ct == "" {
		ct = mime.TypeByExtension(strings.ToLower(path.Ext(name)))
		if ct == "" {
			ct = defaultMIME
		}
	}
	mt, params, err := mime.ParseMediaType(ct)
	if err != nil {
		return "", Errorf(KindInvalidArgument, "invalid content type %q", ct)
	}
	if !c.contentTypeAllowed(mt) {
		return "", Errorf(KindInvalidArgument, "content type %q is not allowed", mt)
	}
	return mime.FormatMediaType(mt, params), nil
}

func (c Config) contentTypeAllowed(mt string) bool {
	if len(c.AllowedContentTypes) == 0 {
		return true
	}
	for _, pat := range c.AllowedContentTypes {
		pat = strings.ToLower(strings.TrimSpace(pat))
		if pat == "*" || pat == "*/*" || pat == mt {
			return true
		}
		if prefix, ok := strings.CutSuffix(pat, "/*"); ok && strings.HasPrefix(mt, prefix+"/") {
			return true
		}
	}
	return false
}

func validateMetadata(md map[string]string) error {
	if len(md) > maxMetadataKeys {
		return Errorf(KindInvalidArgument, "metadata has more than %d entries", maxMetadataKeys)
	}
	total := 0
	for k, v := range md {
		if k == "" || len(k) > maxMetadataKey {
			return Errorf(KindInvalidArgument, "metadata key %q must be 1-%d characters", k, maxMetadataKey)
		}
		for i := 0; i < len(k); i++ {
			c := k[i]
			if !('a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '-' || c == '_' || c == '.') {
				return Errorf(KindInvalidArgument, "metadata key %q may only contain letters, digits, '-', '_' and '.'", k)
			}
		}
		if len(v) > maxMetadataValue || !utf8.ValidString(v) {
			return Errorf(KindInvalidArgument, "metadata value for %q must be valid UTF-8 of at most %d bytes", k, maxMetadataValue)
		}
		total += len(k) + len(v)
	}
	if total > maxMetadataTotal {
		return Errorf(KindInvalidArgument, "metadata is larger than %d bytes", maxMetadataTotal)
	}
	return nil
}

// ContentDisposition formats a Content-Disposition header ("inline" or
// "attachment") carrying name, RFC 6266 / 2231 encoded when not ASCII.
func ContentDisposition(kind, name string) string {
	if v := mime.FormatMediaType(kind, map[string]string{"filename": name}); v != "" {
		return v
	}
	return kind
}

func describeParts(nums []int) string {
	const show = 10
	var b strings.Builder
	for i, n := range nums {
		if i == show {
			fmt.Fprintf(&b, " ... (%d total)", len(nums))
			break
		}
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprint(&b, n)
	}
	return b.String()
}
