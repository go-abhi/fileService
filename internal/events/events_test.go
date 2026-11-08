package events

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-abhi/fileService/internal/core"
)

func event(id string) core.Event {
	return core.Event{ID: id, Type: core.EventFileUploaded, Time: time.Now(), File: &core.File{
		ID: "f1", Namespace: "acme", Name: "a.txt", Status: core.StatusAvailable,
		Upload: &core.UploadSession{StorageUploadID: "secret-upload-id"},
	}}
}

func TestWebhookDeliversSignedEventsWithRetries(t *testing.T) {
	secret := []byte("s3cr3t")
	var calls atomic.Int32
	var mu sync.Mutex
	var got []Payload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !Verify(secret, r.Header.Get("X-FileService-Signature"), body, time.Now(), time.Minute) {
			http.Error(w, "bad signature", http.StatusUnauthorized)
			return
		}
		if calls.Add(1) == 1 {
			http.Error(w, "try again", http.StatusServiceUnavailable)
			return
		}
		var p Payload
		json.Unmarshal(body, &p)
		if r.Header.Get("X-FileService-Delivery") != p.ID || r.Header.Get("X-FileService-Event") != p.Type {
			http.Error(w, "header mismatch", 400)
			return
		}
		mu.Lock()
		got = append(got, p)
		mu.Unlock()
	}))
	defer srv.Close()

	w := NewWebhook(srv.URL, WebhookOptions{Secret: string(secret), Workers: 1, Backoff: time.Millisecond})
	w.Publish(context.Background(), event("e1"))
	w.Publish(context.Background(), event("e2"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != "e1" || got[1].File.ID != "f1" {
		t.Fatalf("delivered %+v", got)
	}
	if got[0].File.Upload != nil {
		t.Fatal("internal upload session leaked in webhook payload")
	}
	w.Publish(context.Background(), event("late")) // after Close: ignored, must not panic
}

func TestWebhookGivesUpOnClientErrors(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "gone", http.StatusGone)
	}))
	defer srv.Close()
	w := NewWebhook(srv.URL, WebhookOptions{Backoff: time.Millisecond})
	w.Publish(context.Background(), event("e1"))
	w.Close(context.Background())
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1 (4xx is permanent)", calls.Load())
	}
}

func TestVerifyRejectsTamperingAndReplays(t *testing.T) {
	secret := []byte("k")
	now := time.Now()
	h := Sign(secret, now, []byte("body"))
	if !Verify(secret, h, []byte("body"), now, time.Minute) {
		t.Fatal("valid signature rejected")
	}
	if Verify(secret, h, []byte("body2"), now, time.Minute) {
		t.Fatal("tampered body accepted")
	}
	if Verify(secret, h, []byte("body"), now.Add(10*time.Minute), time.Minute) {
		t.Fatal("replayed signature accepted")
	}
	if Verify([]byte("other"), h, []byte("body"), now, time.Minute) {
		t.Fatal("wrong secret accepted")
	}
}
