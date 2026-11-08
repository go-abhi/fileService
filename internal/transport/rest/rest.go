// Package rest exposes the core service as an HTTP/JSON API, for browsers
// and clients without gRPC. It mirrors the gRPC API:
//
//	POST   /v1/uploads                  start a direct (presigned multipart) upload
//	GET    /v1/uploads/{id}             upload state (uploaded parts, for resuming)
//	POST   /v1/uploads/{id}/parts       presigned URLs for parts {"part_numbers":[...]}
//	POST   /v1/uploads/{id}/complete    assemble the uploaded parts
//	DELETE /v1/uploads/{id}             abort
//	POST   /v1/files?name=...           upload by streaming the request body
//	GET    /v1/files                    list (?page_size, page_token, include_pending)
//	GET    /v1/files/{id}               metadata
//	DELETE /v1/files/{id}               delete
//	GET    /v1/files/{id}/url           signed download URL (?ttl=15m&disposition=attachment)
//	GET    /v1/files/{id}/content       302 to the CDN / storage URL (?proxy=true streams, with Range)
//	GET    /healthz, /readyz            probes
package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/go-abhi/fileService/internal/auth"
	"github.com/go-abhi/fileService/internal/core"
	"github.com/go-abhi/fileService/internal/grpcx"
)

// Options configure the handler.
type Options struct {
	Auth   auth.Authenticator
	Logger *slog.Logger
	// CORSOrigins lists allowed browser origins ("*" allows any).
	CORSOrigins []string
	// Ready reports readiness for /readyz.
	Ready func(ctx context.Context) error
	// GRPC, if set, receives gRPC requests arriving on the same port.
	GRPC http.Handler
}

type handler struct {
	svc  *core.Service
	opts Options
	log  *slog.Logger
}

// NewHandler returns the HTTP handler.
func NewHandler(svc *core.Service, o Options) http.Handler {
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	h := &handler{svc: svc, opts: o, log: o.Logger}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/uploads", h.createUpload)
	mux.HandleFunc("GET /v1/uploads/{id}", h.getUpload)
	mux.HandleFunc("POST /v1/uploads/{id}/parts", h.partURLs)
	mux.HandleFunc("POST /v1/uploads/{id}/complete", h.completeUpload)
	mux.HandleFunc("DELETE /v1/uploads/{id}", h.abortUpload)
	mux.HandleFunc("POST /v1/files", h.streamUpload)
	mux.HandleFunc("GET /v1/files", h.listFiles)
	mux.HandleFunc("GET /v1/files/{id}", h.getFile)
	mux.HandleFunc("DELETE /v1/files/{id}", h.deleteFile)
	mux.HandleFunc("GET /v1/files/{id}/url", h.downloadURL)
	mux.HandleFunc("GET /v1/files/{id}/content", h.content)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", h.ready)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, core.Errorf(core.KindNotFound, "no route for %s %s", r.Method, r.URL.Path), h.log)
	})
	return h.middleware(mux)
}

// --- middleware ---------------------------------------------------------------------

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (h *handler) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.opts.GRPC != nil && grpcx.IsGRPCRequest(r) {
			h.opts.GRPC.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		defer func() {
			if p := recover(); p != nil {
				h.log.Error("http handler panic", "path", r.URL.Path, "panic", fmt.Sprint(p), "stack", string(debug.Stack()))
				if sw.status == 0 {
					writeError(sw, errors.New("panic"), h.log)
				}
			}
			if r.URL.Path != "/healthz" && r.URL.Path != "/readyz" {
				h.log.Info("http request", "method", r.Method, "path", r.URL.Path, "status", sw.status,
					"bytes", sw.bytes, "duration", time.Since(start).Round(time.Microsecond), "remote", r.RemoteAddr)
			}
		}()
		if h.cors(sw, r) {
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v1/") {
			if h.opts.Auth == nil {
				writeError(sw, core.Errorf(core.KindUnauthenticated, "authentication is not configured"), h.log)
				return
			}
			p, err := h.opts.Auth.Authenticate(r.Context(), auth.TokenFromHeader(r.Header))
			if err != nil {
				writeError(sw, err, h.log)
				return
			}
			r = r.WithContext(core.WithPrincipal(r.Context(), p))
		}
		next.ServeHTTP(sw, r)
	})
}

// cors sets CORS headers and answers preflight requests (returns true).
func (h *handler) cors(w http.ResponseWriter, r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" || len(h.opts.CORSOrigins) == 0 {
		return false
	}
	allowed := false
	for _, o := range h.opts.CORSOrigins {
		if o == "*" || strings.EqualFold(o, origin) {
			allowed = true
			break
		}
	}
	if !allowed {
		return false
	}
	hd := w.Header()
	hd.Set("Access-Control-Allow-Origin", origin)
	hd.Add("Vary", "Origin")
	hd.Set("Access-Control-Expose-Headers", "Location, Content-Range, Content-Length, Content-Disposition, ETag")
	if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
		hd.Set("Access-Control-Allow-Methods", "GET, HEAD, POST, DELETE, OPTIONS")
		hd.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Api-Key, X-File-Name, Range")
		hd.Set("Access-Control-Max-Age", "600")
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	return false
}

func (h *handler) ready(w http.ResponseWriter, r *http.Request) {
	if h.opts.Ready != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err := h.opts.Ready(ctx); err != nil {
			h.log.Warn("readiness check failed", "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "error": err.Error()})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- JSON representation ------------------------------------------------------------------

// File is the JSON representation of a file.
type File struct {
	ID          string            `json:"id"`
	Namespace   string            `json:"namespace"`
	Name        string            `json:"name"`
	ContentType string            `json:"content_type"`
	Size        int64             `json:"size"`
	SHA256      string            `json:"sha256,omitempty"`
	ETag        string            `json:"etag,omitempty"`
	Status      string            `json:"status"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// PartURL is a presigned part upload request.
type PartURL struct {
	PartNumber int               `json:"part_number"`
	URL        string            `json:"url"`
	Method     string            `json:"method"`
	Offset     int64             `json:"offset"`
	Size       int64             `json:"size"`
	Headers    map[string]string `json:"headers,omitempty"`
	ExpiresAt  time.Time         `json:"expires_at"`
}

// UploadedPart is a stored part.
type UploadedPart struct {
	PartNumber int    `json:"part_number"`
	Size       int64  `json:"size"`
	ETag       string `json:"etag"`
}

// Upload is the JSON representation of an upload.
type Upload struct {
	UploadID      string         `json:"upload_id"`
	File          File           `json:"file"`
	PartSize      int64          `json:"part_size,omitempty"`
	PartCount     int            `json:"part_count,omitempty"`
	Parts         []PartURL      `json:"parts,omitempty"`
	UploadedParts []UploadedPart `json:"uploaded_parts,omitempty"`
	ExpiresAt     *time.Time     `json:"expires_at,omitempty"`
}

// DownloadURL is the response of GET /v1/files/{id}/url.
type DownloadURL struct {
	URL       string    `json:"url"`
	ExpiresAt time.Time `json:"expires_at"`
	CDN       bool      `json:"cdn"`
	File      File      `json:"file"`
}

func toFile(f *core.File) File {
	return File{ID: f.ID, Namespace: f.Namespace, Name: f.Name, ContentType: f.ContentType, Size: f.Size, SHA256: f.SHA256,
		ETag: f.ETag, Status: string(f.Status), Metadata: f.Metadata, CreatedAt: f.CreatedAt, UpdatedAt: f.UpdatedAt}
}

func toParts(ps []core.PartURL) []PartURL {
	out := make([]PartURL, len(ps))
	for i, p := range ps {
		out[i] = PartURL{PartNumber: p.PartNumber, URL: p.URL, Method: p.Method, Offset: p.Offset, Size: p.Size, Headers: p.Headers, ExpiresAt: p.ExpiresAt}
	}
	return out
}

func toUpload(u *core.Upload) Upload {
	out := Upload{UploadID: u.File.ID, File: toFile(u.File), PartSize: u.PartSize, PartCount: u.PartCount, Parts: toParts(u.Parts)}
	if len(out.Parts) == 0 {
		out.Parts = nil
	}
	for _, p := range u.Uploaded {
		out.UploadedParts = append(out.UploadedParts, UploadedPart{PartNumber: p.Number, Size: p.Size, ETag: p.ETag})
	}
	if !u.ExpiresAt.IsZero() {
		t := u.ExpiresAt
		out.ExpiresAt = &t
	}
	return out
}

// --- helpers --------------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.Encode(v)
}

// ErrorBody is the JSON error envelope.
type ErrorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func httpStatus(k core.Kind) int {
	switch k {
	case core.KindInvalidArgument:
		return http.StatusBadRequest
	case core.KindNotFound:
		return http.StatusNotFound
	case core.KindAlreadyExists, core.KindFailedPrecondition:
		return http.StatusConflict
	case core.KindOutOfRange:
		return http.StatusRequestedRangeNotSatisfiable
	case core.KindTooLarge:
		return http.StatusRequestEntityTooLarge
	case core.KindUnauthenticated:
		return http.StatusUnauthorized
	case core.KindPermissionDenied:
		return http.StatusForbidden
	case core.KindUnavailable:
		return http.StatusServiceUnavailable
	}
	return http.StatusInternalServerError
}

func writeError(w http.ResponseWriter, err error, log *slog.Logger) {
	var body ErrorBody
	var ce *core.Error
	var mbe *http.MaxBytesError
	status := http.StatusInternalServerError
	switch {
	case errors.As(err, &ce) && ce.Kind != core.KindInternal:
		status = httpStatus(ce.Kind)
		body.Error.Code, body.Error.Message = ce.Kind.String(), ce.Msg
	case errors.As(err, &mbe):
		status = http.StatusRequestEntityTooLarge
		body.Error.Code, body.Error.Message = core.KindTooLarge.String(), "request body too large"
	case errors.Is(err, context.Canceled):
		status = 499 // client closed request
		body.Error.Code, body.Error.Message = "canceled", "request canceled"
	default:
		log.Error("internal error", "error", err)
		body.Error.Code, body.Error.Message = core.KindInternal.String(), "internal error"
	}
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="fileservice"`)
	}
	writeJSON(w, status, body)
}

func decodeJSON(r *http.Request, w http.ResponseWriter, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return err
		}
		return core.Errorf(core.KindInvalidArgument, "invalid JSON body: %v", err)
	}
	return nil
}

// --- handlers ---------------------------------------------------------------------------------

func (h *handler) createUpload(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name        string            `json:"name"`
		ContentType string            `json:"content_type"`
		Size        int64             `json:"size"`
		PartSize    int64             `json:"part_size"`
		Metadata    map[string]string `json:"metadata"`
	}
	if err := decodeJSON(r, w, &req); err != nil {
		writeError(w, err, h.log)
		return
	}
	up, err := h.svc.CreateUpload(r.Context(), core.CreateUploadInput{Name: req.Name, ContentType: req.ContentType, Size: req.Size, PartSize: req.PartSize, Metadata: req.Metadata})
	if err != nil {
		writeError(w, err, h.log)
		return
	}
	w.Header().Set("Location", "/v1/uploads/"+up.File.ID)
	writeJSON(w, http.StatusCreated, toUpload(up))
}

func (h *handler) getUpload(w http.ResponseWriter, r *http.Request) {
	up, err := h.svc.GetUpload(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err, h.log)
		return
	}
	writeJSON(w, http.StatusOK, toUpload(up))
}

func (h *handler) partURLs(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PartNumbers []int `json:"part_numbers"`
	}
	if err := decodeJSON(r, w, &req); err != nil {
		writeError(w, err, h.log)
		return
	}
	parts, err := h.svc.PartURLs(r.Context(), r.PathValue("id"), req.PartNumbers)
	if err != nil {
		writeError(w, err, h.log)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"parts": toParts(parts)})
}

func (h *handler) completeUpload(w http.ResponseWriter, r *http.Request) {
	f, err := h.svc.CompleteUpload(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err, h.log)
		return
	}
	writeJSON(w, http.StatusOK, toFile(f))
}

func (h *handler) abortUpload(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.AbortUpload(r.Context(), r.PathValue("id")); err != nil {
		writeError(w, err, h.log)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) streamUpload(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	name := q.Get("name")
	if name == "" {
		if v := r.Header.Get("X-File-Name"); v != "" {
			if dec, err := url.PathUnescape(v); err == nil {
				name = dec
			} else {
				name = v
			}
		}
	}
	if name == "" {
		if _, params, err := mime.ParseMediaType(r.Header.Get("Content-Disposition")); err == nil {
			name = params["filename"]
		}
	}
	ct := q.Get("content_type")
	if ct == "" {
		ct = r.Header.Get("Content-Type")
		// curl --data-binary sends this by default; it is never the file type.
		if strings.HasPrefix(ct, "application/x-www-form-urlencoded") {
			ct = ""
		}
	}
	md := map[string]string{}
	for k, vs := range q {
		if key, ok := strings.CutPrefix(k, "meta."); ok && len(vs) > 0 {
			md[key] = vs[0]
		}
	}
	size := r.ContentLength // -1 when unknown (chunked)
	f, err := h.svc.UploadStream(r.Context(), core.StreamInput{Name: name, ContentType: ct, Size: size, Metadata: md}, r.Body)
	if err != nil {
		writeError(w, err, h.log)
		return
	}
	w.Header().Set("Location", "/v1/files/"+f.ID)
	writeJSON(w, http.StatusCreated, toFile(f))
}

func (h *handler) listFiles(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	size := 0
	if v := q.Get("page_size"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, core.Errorf(core.KindInvalidArgument, "invalid page_size"), h.log)
			return
		}
		size = n
	}
	pending, _ := strconv.ParseBool(q.Get("include_pending"))
	files, next, err := h.svc.ListFiles(r.Context(), size, q.Get("page_token"), pending)
	if err != nil {
		writeError(w, err, h.log)
		return
	}
	out := struct {
		Files         []File `json:"files"`
		NextPageToken string `json:"next_page_token,omitempty"`
	}{Files: make([]File, len(files)), NextPageToken: next}
	for i, f := range files {
		out.Files[i] = toFile(f)
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *handler) getFile(w http.ResponseWriter, r *http.Request) {
	f, err := h.svc.GetFile(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err, h.log)
		return
	}
	writeJSON(w, http.StatusOK, toFile(f))
}

func (h *handler) deleteFile(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.DeleteFile(r.Context(), r.PathValue("id")); err != nil {
		writeError(w, err, h.log)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) signedURL(r *http.Request) (*core.DownloadURL, error) {
	q := r.URL.Query()
	var ttl time.Duration
	if v := q.Get("ttl"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, core.Errorf(core.KindInvalidArgument, "invalid ttl %q (use e.g. 15m)", v)
		}
		ttl = d
	}
	return h.svc.DownloadURL(r.Context(), core.DownloadURLInput{FileID: r.PathValue("id"), TTL: ttl, Attachment: q.Get("disposition") == "attachment"})
}

func (h *handler) downloadURL(w http.ResponseWriter, r *http.Request) {
	u, err := h.signedURL(r)
	if err != nil {
		writeError(w, err, h.log)
		return
	}
	writeJSON(w, http.StatusOK, DownloadURL{URL: u.URL, ExpiresAt: u.ExpiresAt, CDN: u.CDN, File: toFile(u.File)})
}

func (h *handler) content(w http.ResponseWriter, r *http.Request) {
	if proxy, _ := strconv.ParseBool(r.URL.Query().Get("proxy")); !proxy {
		u, err := h.signedURL(r)
		if err != nil {
			writeError(w, err, h.log)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		http.Redirect(w, r, u.URL, http.StatusFound)
		return
	}
	h.proxyContent(w, r)
}

// proxyContent streams the file through the service, honouring a single
// byte range.
func (h *handler) proxyContent(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	f, err := h.svc.GetFile(r.Context(), id)
	if err != nil {
		writeError(w, err, h.log)
		return
	}
	if f.Status != core.StatusAvailable {
		writeError(w, core.Errorf(core.KindFailedPrecondition, "file %s is still uploading", id), h.log)
		return
	}
	hd := w.Header()
	hd.Set("Accept-Ranges", "bytes")
	hd.Set("Content-Type", f.ContentType)
	// User content is served from the API origin: never sniff or run it.
	hd.Set("X-Content-Type-Options", "nosniff")
	hd.Set("Content-Security-Policy", "sandbox")
	disp := "inline"
	if r.URL.Query().Get("disposition") == "attachment" {
		disp = "attachment"
	}
	hd.Set("Content-Disposition", core.ContentDisposition(disp, f.Name))
	if f.ETag != "" {
		hd.Set("ETag", f.ETag)
	}
	hd.Set("Last-Modified", f.UpdatedAt.UTC().Format(http.TimeFormat))

	offset, length, status := int64(0), f.Size, http.StatusOK
	if rng := r.Header.Get("Range"); rng != "" && f.Size > 0 {
		start, n, ok := parseRange(rng, f.Size)
		switch {
		case !ok:
			hd.Set("Content-Range", "bytes */"+strconv.FormatInt(f.Size, 10))
			writeError(w, core.Errorf(core.KindOutOfRange, "unsatisfiable range %q", rng), h.log)
			return
		case n >= 0:
			offset, length, status = start, n, http.StatusPartialContent
			hd.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+n-1, f.Size))
		}
	}
	hd.Set("Content-Length", strconv.FormatInt(length, 10))
	if r.Method == http.MethodHead || length == 0 {
		w.WriteHeader(status)
		return
	}
	_, rc, _, err := h.svc.OpenFile(r.Context(), id, offset, length)
	if err != nil {
		hd.Del("Content-Length")
		hd.Del("Content-Range")
		writeError(w, err, h.log)
		return
	}
	defer rc.Close()
	w.WriteHeader(status)
	if _, err := io.Copy(w, rc); err != nil && r.Context().Err() == nil {
		h.log.Warn("proxy download interrupted", "file_id", id, "error", err)
	}
}

// parseRange parses a single "bytes=" range. It returns length -1 to ignore
// the header (multiple ranges are served as the full content).
func parseRange(rng string, size int64) (start, length int64, ok bool) {
	spec, found := strings.CutPrefix(rng, "bytes=")
	if !found {
		return 0, -1, true
	}
	if strings.Contains(spec, ",") {
		return 0, -1, true
	}
	a, b, found := strings.Cut(strings.TrimSpace(spec), "-")
	if !found {
		return 0, 0, false
	}
	if a == "" {
		n, err := strconv.ParseInt(b, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		n = min(n, size)
		return size - n, n, true
	}
	s, err := strconv.ParseInt(a, 10, 64)
	if err != nil || s < 0 || s >= size {
		return 0, 0, false
	}
	e := size - 1
	if b != "" {
		v, err := strconv.ParseInt(b, 10, 64)
		if err != nil || v < s {
			return 0, 0, false
		}
		e = min(v, size-1)
	}
	return s, e - s + 1, true
}
