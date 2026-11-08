// Package auth authenticates callers and maps them to a namespace.
package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-abhi/fileService/internal/core"
)

// Authenticator resolves a credential (API key, token...) to a principal.
// Implement it to plug in JWT/OIDC or an external auth service.
type Authenticator interface {
	Authenticate(ctx context.Context, credential string) (core.Principal, error)
}

// TokenFromHeader extracts the credential from "Authorization: Bearer <t>"
// or "X-Api-Key: <t>".
func TokenFromHeader(h http.Header) string {
	if v := h.Get("Authorization"); v != "" {
		if scheme, tok, ok := strings.Cut(v, " "); ok && strings.EqualFold(scheme, "Bearer") {
			return strings.TrimSpace(tok)
		}
	}
	return strings.TrimSpace(h.Get("X-Api-Key"))
}

// APIKeys authenticates static API keys, each bound to a namespace. Keys are
// kept only as SHA-256 hashes.
type APIKeys struct {
	keys map[[32]byte]core.Principal
}

// ParseAPIKeys parses "namespace:key[,namespace:key...]". A namespace may
// have several keys (for rotation); keys must be unique and at least 16
// characters long.
func ParseAPIKeys(spec string) (*APIKeys, error) {
	a := &APIKeys{keys: map[[32]byte]core.Principal{}}
	for _, entry := range strings.Split(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		ns, key, ok := strings.Cut(entry, ":")
		ns, key = strings.TrimSpace(ns), strings.TrimSpace(key)
		if !ok || key == "" {
			return nil, fmt.Errorf("auth: invalid API key entry %q (want namespace:key)", redact(entry))
		}
		if !core.ValidNamespace(ns) {
			return nil, fmt.Errorf("auth: invalid namespace %q (use 1-63 of a-z, 0-9, '-', '_')", ns)
		}
		if len(key) < 16 {
			return nil, fmt.Errorf("auth: API key for namespace %q is shorter than 16 characters", ns)
		}
		h := sha256.Sum256([]byte(key))
		if _, dup := a.keys[h]; dup {
			return nil, fmt.Errorf("auth: duplicate API key for namespace %q", ns)
		}
		a.keys[h] = core.Principal{Namespace: ns, Subject: ns + "/key-" + hex.EncodeToString(h[:4])}
	}
	if len(a.keys) == 0 {
		return nil, fmt.Errorf("auth: no API keys configured")
	}
	return a, nil
}

func redact(s string) string {
	if ns, _, ok := strings.Cut(s, ":"); ok {
		return ns + ":***"
	}
	return "***"
}

// Len returns the number of keys.
func (a *APIKeys) Len() int { return len(a.keys) }

// Authenticate implements Authenticator.
func (a *APIKeys) Authenticate(_ context.Context, credential string) (core.Principal, error) {
	if credential == "" {
		return core.Principal{}, core.Errorf(core.KindUnauthenticated, "missing API key (send 'Authorization: Bearer <key>' or 'X-Api-Key')")
	}
	p, ok := a.keys[sha256.Sum256([]byte(credential))]
	if !ok {
		return core.Principal{}, core.Errorf(core.KindUnauthenticated, "invalid API key")
	}
	return p, nil
}

// Anonymous accepts every caller into one namespace. For development only.
type Anonymous struct{ Namespace string }

// Authenticate implements Authenticator.
func (a Anonymous) Authenticate(context.Context, string) (core.Principal, error) {
	return core.Principal{Namespace: a.Namespace, Subject: "anonymous"}, nil
}
