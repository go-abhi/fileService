// Package events delivers file events to other services.
package events

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-abhi/fileService/internal/core"
)

// Log writes events to a logger.
type Log struct{ Logger *slog.Logger }

// Publish implements core.EventPublisher.
func (l Log) Publish(ctx context.Context, e core.Event) {
	l.Logger.InfoContext(ctx, "event", "type", e.Type, "event_id", e.ID, "file_id", e.File.ID, "namespace", e.File.Namespace)
}

// Multi fans out to several publishers.
type Multi []core.EventPublisher

// Publish implements core.EventPublisher.
func (m Multi) Publish(ctx context.Context, e core.Event) {
	for _, p := range m {
		p.Publish(ctx, e)
	}
}

// Webhook POSTs events as JSON to a URL, asynchronously, with retries.
//
// Each request carries:
//
//	X-FileService-Event:     event type (file.uploaded, file.deleted)
//	X-FileService-Delivery:  event ID (use it to de-duplicate retries)
//	X-FileService-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256(secret, "<t>.<body>")>
//
// Delivery is at-least-once; events are dropped (and logged) if the queue
// is full or every attempt fails.
type Webhook struct {
	url     string
	secret  []byte
	hc      *http.Client
	log     *slog.Logger
	queue   chan core.Event
	wg      sync.WaitGroup
	backoff time.Duration

	mu     sync.Mutex
	closed bool
}

// WebhookOptions configure a Webhook.
type WebhookOptions struct {
	Secret     string
	Workers    int
	QueueSize  int
	HTTPClient *http.Client
	Logger     *slog.Logger
	// Backoff is the first retry delay (doubles per attempt, 5 attempts).
	Backoff time.Duration
}

// NewWebhook starts delivery workers.
func NewWebhook(url string, o WebhookOptions) *Webhook {
	if o.Workers <= 0 {
		o.Workers = 4
	}
	if o.QueueSize <= 0 {
		o.QueueSize = 10000
	}
	if o.HTTPClient == nil {
		o.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.Backoff <= 0 {
		o.Backoff = 500 * time.Millisecond
	}
	w := &Webhook{url: url, secret: []byte(o.Secret), hc: o.HTTPClient, log: o.Logger, queue: make(chan core.Event, o.QueueSize), backoff: o.Backoff}
	for i := 0; i < o.Workers; i++ {
		w.wg.Add(1)
		go w.worker()
	}
	return w
}

// Publish implements core.EventPublisher. It never blocks.
func (w *Webhook) Publish(_ context.Context, e core.Event) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	select {
	case w.queue <- e:
	default:
		w.log.Error("webhook queue full, dropping event", "event_id", e.ID, "type", e.Type)
	}
}

// Close stops accepting events and waits (until ctx is done) for queued
// events to be delivered.
func (w *Webhook) Close(ctx context.Context) error {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.queue)
	}
	w.mu.Unlock()
	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Webhook) worker() {
	defer w.wg.Done()
	for e := range w.queue {
		w.deliver(e)
	}
}

// Payload is the JSON body of a webhook request.
type Payload struct {
	ID   string     `json:"id"`
	Type string     `json:"type"`
	Time time.Time  `json:"time"`
	File *core.File `json:"file"`
}

func (w *Webhook) deliver(e core.Event) {
	f := e.File.Clone()
	f.Upload = nil
	body, err := json.Marshal(Payload{ID: e.ID, Type: e.Type, Time: e.Time, File: f})
	if err != nil {
		w.log.Error("webhook: encoding event", "error", err)
		return
	}
	delay := w.backoff
	for attempt := 1; attempt <= 5; attempt++ {
		err := w.send(e, body)
		if err == nil {
			return
		}
		var perm permanentError
		if errors.As(err, &perm) || attempt == 5 {
			w.log.Error("webhook delivery failed", "event_id", e.ID, "type", e.Type, "attempts", attempt, "error", err)
			return
		}
		time.Sleep(delay)
		delay *= 2
	}
}

type permanentError struct{ error }

func (w *Webhook) send(e core.Event, body []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(body))
	if err != nil {
		return permanentError{err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "fileservice-webhook/1")
	req.Header.Set("X-FileService-Event", e.Type)
	req.Header.Set("X-FileService-Delivery", e.ID)
	if len(w.secret) > 0 {
		req.Header.Set("X-FileService-Signature", Sign(w.secret, time.Now(), body))
	}
	resp, err := w.hc.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	switch {
	case resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return errors.New("webhook: status " + strconv.Itoa(resp.StatusCode))
	default:
		return permanentError{errors.New("webhook: status " + strconv.Itoa(resp.StatusCode))}
	}
}

// Sign returns the X-FileService-Signature header value for body.
func Sign(secret []byte, t time.Time, body []byte) string {
	ts := strconv.FormatInt(t.Unix(), 10)
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(ts + "."))
	m.Write(body)
	return "t=" + ts + ",v1=" + hex.EncodeToString(m.Sum(nil))
}

// Verify checks a signature header against body, rejecting signatures older
// than tolerance. Receivers can use it (or reimplement it) to authenticate
// webhooks.
func Verify(secret []byte, header string, body []byte, now time.Time, tolerance time.Duration) bool {
	var ts, sig string
	for _, part := range bytes.Split([]byte(header), []byte(",")) {
		k, v, _ := bytes.Cut(bytes.TrimSpace(part), []byte("="))
		switch string(k) {
		case "t":
			ts = string(v)
		case "v1":
			sig = string(v)
		}
	}
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil || sig == "" {
		return false
	}
	if d := now.Sub(time.Unix(sec, 0)); d > tolerance || d < -tolerance {
		return false
	}
	want := Sign(secret, time.Unix(sec, 0), body)
	return hmac.Equal([]byte(want), []byte("t="+ts+",v1="+sig))
}
