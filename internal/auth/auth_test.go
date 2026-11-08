package auth

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/go-abhi/fileService/internal/core"
)

func TestParseAndAuthenticate(t *testing.T) {
	a, err := ParseAPIKeys(" orders:orders-key-0123456789 , media:media-key-0123456789, media:media-rotated-012345 ")
	if err != nil {
		t.Fatal(err)
	}
	if a.Len() != 3 {
		t.Fatalf("len = %d", a.Len())
	}
	p, err := a.Authenticate(context.Background(), "media-rotated-012345")
	if err != nil || p.Namespace != "media" || !strings.HasPrefix(p.Subject, "media/key-") {
		t.Fatalf("got %+v %v", p, err)
	}
	if _, err := a.Authenticate(context.Background(), "nope"); core.KindOf(err) != core.KindUnauthenticated {
		t.Fatalf("bad key err = %v", err)
	}
	if _, err := a.Authenticate(context.Background(), ""); core.KindOf(err) != core.KindUnauthenticated {
		t.Fatalf("empty key err = %v", err)
	}
}

func TestParseErrors(t *testing.T) {
	for _, spec := range []string{"", "nokey", "Bad NS:0123456789abcdef", "ns:short", "a:0123456789abcdef,b:0123456789abcdef"} {
		_, err := ParseAPIKeys(spec)
		if err == nil {
			t.Errorf("expected error for %q", spec)
		} else if strings.Contains(err.Error(), "0123456789abcdef") {
			t.Errorf("error leaks the key: %v", err)
		}
	}
}

func TestTokenFromHeader(t *testing.T) {
	cases := map[string]http.Header{
		"tok1": {"Authorization": {"Bearer tok1"}},
		"tok2": {"Authorization": {"bearer  tok2 "}},
		"tok3": {"X-Api-Key": {"tok3"}},
		"":     {"Authorization": {"Basic abc"}},
	}
	for want, h := range cases {
		if got := TokenFromHeader(h); got != want {
			t.Errorf("TokenFromHeader(%v) = %q, want %q", h, got, want)
		}
	}
}
