package core

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"
)

// Service implements the file service use cases.
type Service struct {
	cfg     Config
	storage ObjectStorage
	meta    MetadataStore
	signer  URLSigner
	events  EventPublisher
	log     *slog.Logger
	now     func() time.Time
}

// Deps are the service's collaborators. Storage and Metadata are required.
type Deps struct {
	Storage  ObjectStorage
	Metadata MetadataStore
	// Signer produces download URLs; defaults to presigned storage URLs.
	Signer URLSigner
	// Events receives file events; defaults to a no-op.
	Events EventPublisher
	Logger *slog.Logger
	// Now overrides the clock (tests).
	Now func() time.Time
}

// New returns a Service.
func New(cfg Config, d Deps) (*Service, error) {
	if d.Storage == nil || d.Metadata == nil {
		return nil, errors.New("core: storage and metadata store are required")
	}
	s := &Service{cfg: cfg.withDefaults(), storage: d.Storage, meta: d.Metadata, signer: d.Signer, events: d.Events, log: d.Logger, now: d.Now}
	if s.signer == nil {
		s.signer = StorageSigner{Storage: d.Storage}
	}
	if s.events == nil {
		s.events = noopPublisher{}
	}
	if s.log == nil {
		s.log = slog.New(slog.DiscardHandler)
	}
	if s.now == nil {
		s.now = time.Now
	}
	return s, nil
}

// Config returns the effective configuration.
func (s *Service) Config() Config { return s.cfg }

// Key returns the storage key of a file.
func (s *Service) Key(f *File) string { return s.cfg.KeyPrefix + f.Namespace + "/" + f.ID }

func (s *Service) principal(ctx context.Context) (Principal, error) {
	p, ok := PrincipalFrom(ctx)
	if !ok || p.Namespace == "" {
		return Principal{}, Errorf(KindUnauthenticated, "unauthenticated")
	}
	return p, nil
}

// owned loads a file of the caller's namespace. Files of other namespaces
// are reported as not found.
func (s *Service) owned(ctx context.Context, id string) (*File, error) {
	p, err := s.principal(ctx)
	if err != nil {
		return nil, err
	}
	if !ValidID(id) {
		return nil, Errorf(KindInvalidArgument, "invalid id %q", id)
	}
	f, err := s.meta.Get(ctx, p.Namespace, id)
	if err != nil {
		if IsNotFound(err) {
			return nil, Errorf(KindNotFound, "file %s not found", id)
		}
		return nil, err
	}
	return f, nil
}

func (s *Service) available(ctx context.Context, id string) (*File, error) {
	f, err := s.owned(ctx, id)
	if err != nil {
		return nil, err
	}
	if f.Status != StatusAvailable {
		return nil, Errorf(KindFailedPrecondition, "file %s is still uploading", id)
	}
	return f, nil
}

func (s *Service) attrs(f *File) ObjectAttrs {
	return ObjectAttrs{
		ContentType:        f.ContentType,
		ContentDisposition: ContentDisposition(dispositionInline, f.Name),
		CacheControl:       s.cfg.CacheControl,
		Metadata:           map[string]string{"file-id": f.ID, "namespace": f.Namespace},
	}
}

func (s *Service) publish(ctx context.Context, typ string, f *File) {
	s.events.Publish(context.WithoutCancel(ctx), Event{ID: NewID(), Type: typ, Time: s.now().UTC(), File: f.Clone()})
}

// cleanup runs best-effort compensation after a failure, detached from the
// (possibly cancelled) request context.
func (s *Service) cleanup(ctx context.Context, what string, fn func(context.Context) error) {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if err := fn(cctx); err != nil {
		s.log.Warn("cleanup failed", "what", what, "error", err)
	}
}

// --- direct (presigned multipart) uploads ---------------------------------------------

// CreateUploadInput describes a new direct upload.
type CreateUploadInput struct {
	Name        string
	ContentType string
	Size        int64
	PartSize    int64
	Metadata    map[string]string
}

// PartURL is a presigned URL for one part.
type PartURL struct {
	PartNumber int
	Offset     int64
	Size       int64
	SignedURL
}

// Upload is the state of a direct upload.
type Upload struct {
	File      *File
	PartSize  int64
	PartCount int
	// Parts holds presigned URLs (CreateUpload).
	Parts []PartURL
	// Uploaded holds the parts already stored (GetUpload).
	Uploaded  []StoredPart
	ExpiresAt time.Time
}

func (s *Service) newFile(ctx context.Context, name, contentType string, metadata map[string]string) (*File, error) {
	p, err := s.principal(ctx)
	if err != nil {
		return nil, err
	}
	if name, err = cleanName(name); err != nil {
		return nil, err
	}
	if contentType, err = s.cfg.contentType(contentType, name); err != nil {
		return nil, err
	}
	if err := validateMetadata(metadata); err != nil {
		return nil, err
	}
	now := s.now().UTC()
	f := &File{ID: NewID(), Namespace: p.Namespace, Name: name, ContentType: contentType, CreatedAt: now, UpdatedAt: now}
	if len(metadata) > 0 {
		f.Metadata = make(map[string]string, len(metadata))
		for k, v := range metadata {
			f.Metadata[k] = v
		}
	}
	return f, nil
}

// CreateUpload starts a direct upload: the client PUTs parts to presigned
// URLs and then calls CompleteUpload.
func (s *Service) CreateUpload(ctx context.Context, in CreateUploadInput) (*Upload, error) {
	f, err := s.newFile(ctx, in.Name, in.ContentType, in.Metadata)
	if err != nil {
		return nil, err
	}
	if in.Size < 0 {
		return nil, Errorf(KindInvalidArgument, "size must not be negative")
	}
	if in.Size > s.cfg.MaxFileSize {
		return nil, Errorf(KindTooLarge, "file size %d exceeds the limit of %d bytes", in.Size, s.cfg.MaxFileSize)
	}
	partSize, count, err := s.cfg.planParts(in.Size, in.PartSize)
	if err != nil {
		return nil, err
	}
	f.Size = in.Size
	key := s.Key(f)

	if in.Size == 0 { // nothing to upload: store the empty object directly
		etag, err := s.storage.PutObject(ctx, key, nil, s.attrs(f))
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(nil)
		f.Status, f.ETag, f.SHA256 = StatusAvailable, etag, hex.EncodeToString(sum[:])
		if err := s.meta.Create(ctx, f); err != nil {
			s.cleanup(ctx, "delete object", func(c context.Context) error { return s.storage.DeleteObject(c, key) })
			return nil, err
		}
		s.publish(ctx, EventFileUploaded, f)
		return &Upload{File: f}, nil
	}

	uploadID, err := s.storage.CreateMultipartUpload(ctx, key, s.attrs(f))
	if err != nil {
		return nil, err
	}
	f.Status = StatusPending
	f.Upload = &UploadSession{StorageUploadID: uploadID, PartSize: partSize, PartCount: count, ExpiresAt: f.CreatedAt.Add(s.cfg.UploadExpiry)}
	if err := s.meta.Create(ctx, f); err != nil {
		s.cleanup(ctx, "abort upload", func(c context.Context) error { return s.storage.AbortMultipartUpload(c, key, uploadID) })
		return nil, err
	}
	nums := make([]int, min(count, s.cfg.InitialPartURLs))
	for i := range nums {
		nums[i] = i + 1
	}
	parts, err := s.presignParts(ctx, f, nums)
	if err != nil {
		s.cleanup(ctx, "abort upload", func(c context.Context) error {
			s.storage.AbortMultipartUpload(c, key, uploadID)
			return s.meta.Delete(c, f.Namespace, f.ID)
		})
		return nil, err
	}
	s.log.Info("upload created", "file_id", f.ID, "namespace", f.Namespace, "size", f.Size, "parts", count)
	return &Upload{File: f, PartSize: partSize, PartCount: count, Parts: parts, ExpiresAt: f.Upload.ExpiresAt}, nil
}

func (s *Service) presignParts(ctx context.Context, f *File, nums []int) ([]PartURL, error) {
	u := f.Upload
	ttl := s.cfg.UploadURLTTL
	if left := u.ExpiresAt.Sub(s.now()); left < ttl {
		ttl = left
	}
	if ttl < time.Second {
		return nil, Errorf(KindFailedPrecondition, "upload %s has expired", f.ID)
	}
	key := s.Key(f)
	out := make([]PartURL, 0, len(nums))
	for _, n := range nums {
		off, size := partRange(n, u.PartSize, f.Size)
		su, err := s.storage.PresignUploadPart(ctx, key, u.StorageUploadID, n, size, ttl)
		if err != nil {
			return nil, err
		}
		out = append(out, PartURL{PartNumber: n, Offset: off, Size: size, SignedURL: su})
	}
	return out, nil
}

func (s *Service) pending(ctx context.Context, uploadID string) (*File, error) {
	f, err := s.owned(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	if f.Status != StatusPending || f.Upload == nil {
		return nil, Errorf(KindFailedPrecondition, "upload %s is already complete", uploadID)
	}
	return f, nil
}

// PartURLs returns fresh presigned URLs for the given parts.
func (s *Service) PartURLs(ctx context.Context, uploadID string, partNumbers []int) ([]PartURL, error) {
	f, err := s.pending(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	if len(partNumbers) == 0 {
		return nil, Errorf(KindInvalidArgument, "part_numbers is required")
	}
	if len(partNumbers) > s.cfg.MaxPartURLsPerRequest {
		return nil, Errorf(KindInvalidArgument, "at most %d part numbers per request", s.cfg.MaxPartURLsPerRequest)
	}
	seen := make(map[int]bool, len(partNumbers))
	nums := make([]int, 0, len(partNumbers))
	for _, n := range partNumbers {
		if n < 1 || n > f.Upload.PartCount {
			return nil, Errorf(KindInvalidArgument, "part number %d out of range 1-%d", n, f.Upload.PartCount)
		}
		if !seen[n] {
			seen[n] = true
			nums = append(nums, n)
		}
	}
	return s.presignParts(ctx, f, nums)
}

// GetUpload returns the state of an upload, including stored parts.
func (s *Service) GetUpload(ctx context.Context, uploadID string) (*Upload, error) {
	f, err := s.owned(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	if f.Status == StatusAvailable {
		return &Upload{File: f}, nil
	}
	parts, err := s.storage.ListParts(ctx, s.Key(f), f.Upload.StorageUploadID)
	if errors.Is(err, ErrNoSuchUpload) {
		if done, err := s.recoverCompleted(ctx, f); err == nil {
			return &Upload{File: done}, nil
		}
		return nil, Errorf(KindFailedPrecondition, "upload %s no longer exists (aborted or expired)", uploadID)
	}
	if err != nil {
		return nil, err
	}
	return &Upload{File: f, PartSize: f.Upload.PartSize, PartCount: f.Upload.PartCount, Uploaded: parts, ExpiresAt: f.Upload.ExpiresAt}, nil
}

// CompleteUpload checks that every part is stored with the expected size
// and assembles the file. It is idempotent.
func (s *Service) CompleteUpload(ctx context.Context, uploadID string) (*File, error) {
	f, err := s.owned(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	if f.Status == StatusAvailable {
		return f, nil
	}
	u := f.Upload
	key := s.Key(f)
	stored, err := s.storage.ListParts(ctx, key, u.StorageUploadID)
	if errors.Is(err, ErrNoSuchUpload) {
		return s.recoverCompleted(ctx, f)
	}
	if err != nil {
		return nil, err
	}
	byNum := make(map[int]StoredPart, len(stored))
	for _, p := range stored {
		byNum[p.Number] = p
	}
	var missing []int
	parts := make([]StoredPart, 0, u.PartCount)
	for n := 1; n <= u.PartCount; n++ {
		p, ok := byNum[n]
		if !ok {
			missing = append(missing, n)
			continue
		}
		if _, want := partRange(n, u.PartSize, f.Size); p.Size != want {
			return nil, Errorf(KindFailedPrecondition, "part %d has %d bytes, expected %d", n, p.Size, want)
		}
		parts = append(parts, p)
	}
	if len(missing) > 0 {
		return nil, Errorf(KindFailedPrecondition, "upload is incomplete, missing parts: %s", describeParts(missing))
	}
	if err := s.storage.CompleteMultipartUpload(ctx, key, u.StorageUploadID, parts); err != nil {
		if errors.Is(err, ErrNoSuchUpload) {
			return s.recoverCompleted(ctx, f)
		}
		return nil, err
	}
	return s.finalize(ctx, f)
}

// finalize marks a pending file available once its object exists.
func (s *Service) finalize(ctx context.Context, f *File) (*File, error) {
	key := s.Key(f)
	info, err := s.storage.HeadObject(ctx, key)
	if err != nil {
		return nil, err
	}
	if info.Size != f.Size {
		s.cleanup(ctx, "delete corrupt object", func(c context.Context) error {
			s.storage.DeleteObject(c, key)
			return s.meta.Delete(c, f.Namespace, f.ID)
		})
		return nil, Errorf(KindInternal, "stored object has %d bytes, expected %d", info.Size, f.Size)
	}
	f.Status, f.ETag, f.Upload, f.UpdatedAt = StatusAvailable, info.ETag, nil, s.now().UTC()
	if err := s.meta.Put(ctx, f); err != nil {
		return nil, err
	}
	s.log.Info("upload completed", "file_id", f.ID, "namespace", f.Namespace, "size", f.Size)
	s.publish(ctx, EventFileUploaded, f)
	return f, nil
}

// recoverCompleted handles a multipart upload that no longer exists: a
// concurrent CompleteUpload may have won, or it was aborted / expired.
func (s *Service) recoverCompleted(ctx context.Context, f *File) (*File, error) {
	if cur, err := s.meta.Get(ctx, f.Namespace, f.ID); err == nil && cur.Status == StatusAvailable {
		return cur, nil
	}
	if info, err := s.storage.HeadObject(ctx, s.Key(f)); err == nil && info.Size == f.Size {
		return s.finalize(ctx, f)
	}
	return nil, Errorf(KindFailedPrecondition, "upload %s no longer exists (aborted or expired)", f.ID)
}

// AbortUpload cancels a direct upload.
func (s *Service) AbortUpload(ctx context.Context, uploadID string) error {
	f, err := s.pending(ctx, uploadID)
	if err != nil {
		return err
	}
	return s.abort(ctx, f)
}

func (s *Service) abort(ctx context.Context, f *File) error {
	if f.Upload != nil {
		if err := s.storage.AbortMultipartUpload(ctx, s.Key(f), f.Upload.StorageUploadID); err != nil && !errors.Is(err, ErrNoSuchUpload) {
			return err
		}
	}
	return s.meta.Delete(ctx, f.Namespace, f.ID)
}

// --- streamed uploads ----------------------------------------------------------------------

// StreamInput describes a streamed upload.
type StreamInput struct {
	Name        string
	ContentType string
	// Size is the expected size, or -1 if unknown.
	Size     int64
	Metadata map[string]string
}

// UploadStream stores the content read from r. Memory use is bounded by
// StreamBufferBudget; parts are uploaded concurrently while reading.
func (s *Service) UploadStream(ctx context.Context, in StreamInput, r io.Reader) (*File, error) {
	f, err := s.newFile(ctx, in.Name, in.ContentType, in.Metadata)
	if err != nil {
		return nil, err
	}
	if in.Size > s.cfg.MaxFileSize {
		return nil, Errorf(KindTooLarge, "file size %d exceeds the limit of %d bytes", in.Size, s.cfg.MaxFileSize)
	}
	partSize := s.cfg.StreamPartSize
	limit := min(s.cfg.MaxFileSize, partSize*int64(s.cfg.MaxParts))
	if in.Size >= 0 {
		if partSize, _, err = s.cfg.planParts(in.Size, s.cfg.StreamPartSize); err != nil {
			return nil, err
		}
		partSize = max(partSize, s.cfg.MinPartSize)
		limit = in.Size
	}
	conc := int(min(int64(s.cfg.StreamConcurrency), max(1, s.cfg.StreamBufferBudget/partSize)))

	key := s.Key(f)
	h := sha256.New()
	tooBig := func(total int64) error {
		if total <= limit {
			return nil
		}
		if in.Size >= 0 {
			return Errorf(KindInvalidArgument, "stream is larger than the declared size of %d bytes", in.Size)
		}
		return Errorf(KindTooLarge, "stream exceeds the limit of %d bytes", limit)
	}
	lr := io.LimitReader(r, limit+1)

	// Read the first part with a growing buffer so small files stay cheap.
	first, rerr := readChunk(lr, int(partSize))
	if rerr != nil && rerr != io.EOF {
		return nil, fmt.Errorf("reading upload stream: %w", rerr)
	}
	h.Write(first)
	total := int64(len(first))
	if err := tooBig(total); err != nil {
		return nil, err
	}

	var etag string
	if rerr == io.EOF { // the whole file fit in one part
		if in.Size >= 0 && total != in.Size {
			return nil, Errorf(KindInvalidArgument, "stream has %d bytes, declared size is %d", total, in.Size)
		}
		if etag, err = s.storage.PutObject(ctx, key, first, s.attrs(f)); err != nil {
			return nil, err
		}
	} else {
		uploadID, err := s.storage.CreateMultipartUpload(ctx, key, s.attrs(f))
		if err != nil {
			return nil, err
		}
		abort := func() {
			s.cleanup(ctx, "abort upload", func(c context.Context) error { return s.storage.AbortMultipartUpload(c, key, uploadID) })
		}
		parts, n, err := s.streamParts(ctx, key, uploadID, first, lr, partSize, conc, h, tooBig)
		total = n
		if err == nil && in.Size >= 0 && total != in.Size {
			err = Errorf(KindInvalidArgument, "stream has %d bytes, declared size is %d", total, in.Size)
		}
		if err != nil {
			abort()
			return nil, err
		}
		if err := s.storage.CompleteMultipartUpload(ctx, key, uploadID, parts); err != nil {
			abort()
			return nil, err
		}
	}

	info, err := s.storage.HeadObject(ctx, key)
	if err != nil {
		return nil, err
	}
	if info.Size != total {
		s.cleanup(ctx, "delete corrupt object", func(c context.Context) error { return s.storage.DeleteObject(c, key) })
		return nil, Errorf(KindInternal, "stored object has %d bytes, expected %d", info.Size, total)
	}
	if etag == "" {
		etag = info.ETag
	}
	f.Size, f.ETag, f.SHA256, f.Status = total, etag, hex.EncodeToString(h.Sum(nil)), StatusAvailable
	f.UpdatedAt = s.now().UTC()
	if err := s.meta.Create(ctx, f); err != nil {
		s.cleanup(ctx, "delete object", func(c context.Context) error { return s.storage.DeleteObject(c, key) })
		return nil, err
	}
	s.log.Info("stream upload completed", "file_id", f.ID, "namespace", f.Namespace, "size", f.Size)
	s.publish(ctx, EventFileUploaded, f)
	return f, nil
}

// streamParts uploads first and the rest of r as parts, conc at a time.
func (s *Service) streamParts(ctx context.Context, key, uploadID string, first []byte, r io.Reader,
	partSize int64, conc int, h hash.Hash, check func(int64) error) ([]StoredPart, int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		mu       sync.Mutex
		parts    []StoredPart
		firstErr error
		wg       sync.WaitGroup
	)
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		mu.Unlock()
	}
	free := make(chan []byte, conc)
	allocated := 1
	getBuf := func() ([]byte, error) {
		select {
		case b := <-free:
			return b, nil
		default:
		}
		if allocated < conc {
			allocated++
			return make([]byte, partSize), nil
		}
		select {
		case b := <-free:
			return b, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	upload := func(num int, buf []byte, n int) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			etag, err := s.storage.UploadPart(ctx, key, uploadID, num, buf[:n])
			free <- buf
			if err != nil {
				fail(fmt.Errorf("uploading part %d: %w", num, err))
				return
			}
			mu.Lock()
			parts = append(parts, StoredPart{Number: num, Size: int64(n), ETag: etag})
			mu.Unlock()
		}()
	}

	total := int64(len(first))
	num := 1
	upload(num, first, len(first))
	for {
		buf, err := getBuf()
		if err != nil {
			fail(err)
			break
		}
		n, rerr := io.ReadFull(r, buf)
		if n == 0 {
			free <- buf
		} else {
			h.Write(buf[:n])
			total += int64(n)
			if err := check(total); err != nil {
				free <- buf
				fail(err)
				break
			}
			if num++; num > s.cfg.MaxParts {
				free <- buf
				fail(Errorf(KindTooLarge, "stream needs more than %d parts", s.cfg.MaxParts))
				break
			}
			upload(num, buf, n)
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			fail(fmt.Errorf("reading upload stream: %w", rerr))
			break
		}
	}
	wg.Wait()
	if firstErr != nil {
		return nil, total, firstErr
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Number < parts[j].Number })
	return parts, total, nil
}

// --- downloads and file management ----------------------------------------------------------

// OpenFile returns the content of a file from offset; length 0 reads to the
// end. The returned length is the number of bytes the reader yields.
func (s *Service) OpenFile(ctx context.Context, id string, offset, length int64) (*File, io.ReadCloser, int64, error) {
	f, err := s.available(ctx, id)
	if err != nil {
		return nil, nil, 0, err
	}
	if offset < 0 || length < 0 {
		return nil, nil, 0, Errorf(KindInvalidArgument, "offset and length must not be negative")
	}
	if offset > f.Size {
		return nil, nil, 0, Errorf(KindOutOfRange, "offset %d is beyond the end of the file (%d bytes)", offset, f.Size)
	}
	if length == 0 || length > f.Size-offset {
		length = f.Size - offset
	}
	if length == 0 {
		return f, io.NopCloser(strings.NewReader("")), 0, nil
	}
	rc, err := s.storage.GetObject(ctx, s.Key(f), offset, length)
	if err != nil {
		return nil, nil, 0, err
	}
	return f, rc, length, nil
}

// DownloadURLInput requests a download URL.
type DownloadURLInput struct {
	FileID     string
	TTL        time.Duration
	Attachment bool
}

// DownloadURL is a signed URL to download a file.
type DownloadURL struct {
	URL       string
	ExpiresAt time.Time
	CDN       bool
	File      *File
}

// DownloadURL returns a CDN or presigned storage URL for a file.
func (s *Service) DownloadURL(ctx context.Context, in DownloadURLInput) (*DownloadURL, error) {
	f, err := s.available(ctx, in.FileID)
	if err != nil {
		return nil, err
	}
	ttl := in.TTL
	if ttl < 0 {
		return nil, Errorf(KindInvalidArgument, "ttl must not be negative")
	}
	if ttl == 0 {
		ttl = s.cfg.DownloadURLTTL
	}
	ttl = min(ttl, s.cfg.MaxDownloadURLTTL)
	var opts DownloadOptions
	if in.Attachment {
		opts.ContentDisposition = ContentDisposition("attachment", f.Name)
	}
	u, err := s.signer.SignURL(ctx, s.Key(f), ttl, opts)
	if err != nil {
		return nil, err
	}
	return &DownloadURL{URL: u.URL, ExpiresAt: u.ExpiresAt, CDN: s.signer.CDN(), File: f}, nil
}

// GetFile returns a file of the caller's namespace.
func (s *Service) GetFile(ctx context.Context, id string) (*File, error) { return s.owned(ctx, id) }

// ListFiles lists the caller's files newest first. The page token is opaque.
func (s *Service) ListFiles(ctx context.Context, pageSize int, pageToken string, includePending bool) ([]*File, string, error) {
	p, err := s.principal(ctx)
	if err != nil {
		return nil, "", err
	}
	switch {
	case pageSize < 0:
		return nil, "", Errorf(KindInvalidArgument, "page_size must not be negative")
	case pageSize == 0:
		pageSize = 50
	case pageSize > 1000:
		pageSize = 1000
	}
	var after string
	if pageToken != "" {
		b, err := base64.RawURLEncoding.DecodeString(pageToken)
		if err != nil || !ValidID(string(b)) {
			return nil, "", Errorf(KindInvalidArgument, "invalid page_token")
		}
		after = string(b)
	}
	files, next, err := s.meta.List(ctx, ListQuery{Namespace: p.Namespace, Limit: pageSize, After: after, IncludePending: includePending})
	if err != nil {
		return nil, "", err
	}
	if next != "" {
		next = base64.RawURLEncoding.EncodeToString([]byte(next))
	}
	return files, next, nil
}

// DeleteFile deletes a file, or aborts it if its upload is in progress.
func (s *Service) DeleteFile(ctx context.Context, id string) error {
	f, err := s.owned(ctx, id)
	if err != nil {
		return err
	}
	if f.Status == StatusPending {
		return s.abort(ctx, f)
	}
	// Object first: if the record delete fails the call can simply be retried.
	if err := s.storage.DeleteObject(ctx, s.Key(f)); err != nil {
		return err
	}
	if err := s.meta.Delete(ctx, f.Namespace, f.ID); err != nil {
		return err
	}
	s.log.Info("file deleted", "file_id", f.ID, "namespace", f.Namespace)
	s.publish(ctx, EventFileDeleted, f)
	return nil
}

// --- janitor ----------------------------------------------------------------------------------

// CleanupExpiredUploads aborts direct uploads whose session expired and
// returns how many were removed.
func (s *Service) CleanupExpiredUploads(ctx context.Context) (int, error) {
	files, err := s.meta.ListExpiredUploads(ctx, s.now(), s.cfg.JanitorBatch)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, f := range files {
		if err := s.abort(ctx, f); err != nil {
			s.log.Warn("janitor: abort failed", "file_id", f.ID, "error", err)
			continue
		}
		n++
	}
	if n > 0 {
		s.log.Info("janitor: removed expired uploads", "count", n)
	}
	return n, nil
}

// RunJanitor cleans expired uploads every interval until ctx is done.
func (s *Service) RunJanitor(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for {
				n, err := s.CleanupExpiredUploads(ctx)
				if err != nil {
					s.log.Warn("janitor failed", "error", err)
				}
				if err != nil || n < s.cfg.JanitorBatch {
					break
				}
			}
		}
	}
}

// readChunk reads up to max bytes from r, growing its buffer as data
// arrives. It returns io.EOF (with the data read) when r ended before max
// bytes, and a nil error when the buffer is full.
func readChunk(r io.Reader, max int) ([]byte, error) {
	buf := make([]byte, 0, min(max, 64<<10))
	for len(buf) < max {
		if len(buf) == cap(buf) {
			grown := make([]byte, len(buf), min(max, 2*cap(buf)))
			copy(grown, buf)
			buf = grown
		}
		n, err := r.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if err != nil {
			return buf, err
		}
	}
	return buf, nil
}
