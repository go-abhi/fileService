package client

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	v1 "github.com/go-abhi/fileService/api/fileservice/v1"
)

// UploadOptions configure Upload.
type UploadOptions struct {
	// Name of the file (defaults to the base name for UploadFile).
	Name string
	// ContentType; detected from the name by the server when empty.
	ContentType string
	Metadata    map[string]string
	// PartSize in bytes (server default 16 MiB, min 5 MiB).
	PartSize int64
	// Concurrency is the number of parts uploaded in parallel (default 4).
	Concurrency int
	// OnProgress is called with the number of bytes stored so far.
	OnProgress func(done, total int64)
	// KeepOnError leaves a failed upload in place so it can be resumed with
	// ResumeUpload; by default it is aborted.
	KeepOnError bool
}

// UploadFile uploads a local file directly to storage.
func (c *Client) UploadFile(ctx context.Context, path string, o UploadOptions) (*v1.File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if o.Name == "" {
		o.Name = filepath.Base(path)
	}
	return c.Upload(ctx, f, st.Size(), o)
}

// Upload uploads size bytes from r directly to storage with presigned
// multipart URLs: parts go in parallel straight to S3, never through the
// service. If r implements io.ReaderAt (like *os.File) parts are read
// concurrently; otherwise r is read sequentially into part buffers.
func (c *Client) Upload(ctx context.Context, r io.Reader, size int64, o UploadOptions) (*v1.File, error) {
	if size < 0 {
		return nil, errors.New("client: size is required for Upload; use UploadStream for streams of unknown size")
	}
	created, err := c.CreateUpload(ctx, &v1.CreateUploadRequest{Name: o.Name, ContentType: o.ContentType, Size: size, PartSize: o.PartSize, Metadata: o.Metadata})
	if err != nil {
		return nil, err
	}
	if created.File.Status == v1.FileStatusAvailable { // empty file: nothing to send
		return created.File, nil
	}
	up := c.newUploader(created.UploadID, created.PartSize, int(created.PartCount), size, o)
	up.urls.add(created.Parts)
	return c.finish(ctx, up, r, nil, o)
}

// ResumeUpload continues an upload created by Upload (with KeepOnError) or
// any client, sending only the parts that are not stored yet.
func (c *Client) ResumeUpload(ctx context.Context, uploadID string, r io.ReaderAt, o UploadOptions) (*v1.File, error) {
	st, err := c.GetUpload(ctx, &v1.GetUploadRequest{UploadID: uploadID})
	if err != nil {
		return nil, err
	}
	if st.File.Status == v1.FileStatusAvailable {
		return st.File, nil
	}
	up := c.newUploader(uploadID, st.PartSize, int(st.PartCount), st.File.Size, o)
	done := map[int]bool{}
	for _, p := range st.UploadedParts {
		if _, size := up.partRange(int(p.PartNumber)); p.Size == size {
			done[int(p.PartNumber)] = true
			up.progress(size)
		}
	}
	return c.finish(ctx, up, io.NewSectionReader(r, 0, st.File.Size), done, o)
}

func (c *Client) finish(ctx context.Context, up *uploader, r io.Reader, done map[int]bool, o UploadOptions) (*v1.File, error) {
	if err := up.run(ctx, r, done); err != nil {
		uerr := &UploadError{UploadID: up.id, Err: err}
		if !o.KeepOnError {
			actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if _, aerr := c.AbortUpload(actx, &v1.AbortUploadRequest{UploadID: up.id}); aerr == nil {
				uerr.Aborted = true
			}
		}
		return nil, uerr
	}
	var resp *v1.CompleteUploadResponse
	err := retryRPC(ctx, func() (err error) {
		resp, err = c.CompleteUpload(ctx, &v1.CompleteUploadRequest{UploadID: up.id})
		return err
	})
	if err != nil {
		return nil, &UploadError{UploadID: up.id, Err: err}
	}
	return resp.File, nil
}

type uploader struct {
	c           *Client
	id          string
	partSize    int64
	partCount   int
	size        int64
	concurrency int
	urls        *urlCache

	mu         sync.Mutex
	done       int64
	onProgress func(done, total int64)
}

func (c *Client) newUploader(id string, partSize int64, partCount int, size int64, o UploadOptions) *uploader {
	conc := o.Concurrency
	if conc <= 0 {
		conc = 4
	}
	u := &uploader{c: c, id: id, partSize: partSize, partCount: partCount, size: size, concurrency: min(conc, max(partCount, 1)), onProgress: o.OnProgress}
	u.urls = &urlCache{urls: map[int]*v1.UploadPartURL{}, partCount: partCount, fetch: func(ctx context.Context, nums []int32) ([]*v1.UploadPartURL, error) {
		var resp *v1.GetUploadPartURLsResponse
		err := retryRPC(ctx, func() (err error) {
			resp, err = c.GetUploadPartURLs(ctx, &v1.GetUploadPartURLsRequest{UploadID: id, PartNumbers: nums})
			return err
		})
		if err != nil {
			return nil, err
		}
		return resp.Parts, nil
	}}
	return u
}

func (u *uploader) partRange(n int) (int64, int64) {
	off := int64(n-1) * u.partSize
	return off, min(u.partSize, u.size-off)
}

func (u *uploader) progress(n int64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.done += n
	if u.onProgress != nil {
		u.onProgress(u.done, u.size)
	}
}

type partJob struct {
	num  int
	body io.ReadSeeker
	size int64
	buf  []byte // returned to the pool when done (sequential readers)
}

// run uploads every part not in done.
func (u *uploader) run(ctx context.Context, r io.Reader, done map[int]bool) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan partJob)
	errc := make(chan error, u.concurrency+1)
	ra, isReaderAt := r.(io.ReaderAt)
	var pool chan []byte
	if !isReaderAt {
		n := min(u.concurrency+1, u.partCount)
		pool = make(chan []byte, n)
		for i := 0; i < n; i++ {
			pool <- make([]byte, min(u.partSize, u.size))
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < u.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				err := u.sendPart(ctx, j)
				if j.buf != nil {
					pool <- j.buf
				}
				if err != nil {
					errc <- err
					cancel()
					return
				}
				u.progress(j.size)
			}
		}()
	}

	// Producer: hand parts to the workers.
	produce := func() error {
		defer close(jobs)
		for n := 1; n <= u.partCount; n++ {
			off, size := u.partRange(n)
			var j partJob
			if isReaderAt {
				if done[n] {
					continue
				}
				j = partJob{num: n, body: io.NewSectionReader(ra, off, size), size: size}
			} else {
				var buf []byte
				select {
				case buf = <-pool:
				case <-ctx.Done():
					return errStop
				}
				if _, err := io.ReadFull(r, buf[:size]); err != nil {
					pool <- buf
					return fmt.Errorf("reading part %d: %w", n, err)
				}
				if done[n] {
					pool <- buf
					continue
				}
				j = partJob{num: n, body: bytes.NewReader(buf[:size]), size: size, buf: buf}
			}
			select {
			case jobs <- j:
			case <-ctx.Done():
				if j.buf != nil {
					pool <- j.buf
				}
				return errStop
			}
		}
		return nil
	}
	perr := produce()
	wg.Wait()
	close(errc)
	if err := <-errc; err != nil {
		return err
	}
	if perr != nil && perr != errStop {
		return perr
	}
	return ctx.Err()
}

// sendPart PUTs one part to its presigned URL, retrying transient failures
// and refreshing expired URLs.
func (u *uploader) sendPart(ctx context.Context, j partJob) error {
	h := md5.New()
	if _, err := io.Copy(h, j.body); err != nil {
		return fmt.Errorf("reading part %d: %w", j.num, err)
	}
	sum := base64.StdEncoding.EncodeToString(h.Sum(nil))
	refresh := false
	var lastErr error
	for attempt := 0; attempt < u.c.retries; attempt++ {
		if attempt > 0 {
			if err := sleep(ctx, time.Duration(250<<min(attempt, 5))*time.Millisecond); err != nil {
				return err
			}
		}
		pu, err := u.urls.get(ctx, j.num, refresh)
		if err != nil {
			return fmt.Errorf("part %d: getting upload URL: %w", j.num, err)
		}
		if _, err := j.body.Seek(0, io.SeekStart); err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, pu.Method, pu.URL, io.NopCloser(j.body))
		if err != nil {
			return err
		}
		req.ContentLength = j.size
		for k, v := range pu.Headers {
			req.Header.Set(k, v)
		}
		req.Header.Set("Content-Md5", sum)
		resp, err := u.c.transfer.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = err
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		switch {
		case resp.StatusCode == http.StatusOK:
			return nil
		case resp.StatusCode == http.StatusForbidden:
			refresh = true // expired or invalidated URL
		case resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusRequestTimeout:
		default:
			return fmt.Errorf("part %d: storage returned %s: %s", j.num, resp.Status, body)
		}
		lastErr = fmt.Errorf("storage returned %s: %s", resp.Status, bytes.TrimSpace(body))
	}
	return fmt.Errorf("part %d: giving up after %d attempts: %w", j.num, u.c.retries, lastErr)
}

// urlCache holds presigned part URLs, fetching them in batches.
type urlCache struct {
	mu        sync.Mutex
	urls      map[int]*v1.UploadPartURL
	partCount int
	fetch     func(ctx context.Context, nums []int32) ([]*v1.UploadPartURL, error)
}

const urlBatch = 100

func (uc *urlCache) add(ps []*v1.UploadPartURL) {
	for _, p := range ps {
		uc.urls[int(p.PartNumber)] = p
	}
}

func fresh(p *v1.UploadPartURL) bool {
	return p != nil && (p.ExpireTime.IsZero() || time.Until(p.ExpireTime) > 2*time.Minute)
}

func (uc *urlCache) get(ctx context.Context, n int, refresh bool) (*v1.UploadPartURL, error) {
	uc.mu.Lock()
	defer uc.mu.Unlock()
	if p := uc.urls[n]; !refresh && fresh(p) {
		return p, nil
	}
	nums := []int32{int32(n)}
	for k := n + 1; k <= uc.partCount && len(nums) < urlBatch; k++ {
		if !fresh(uc.urls[k]) {
			nums = append(nums, int32(k))
		}
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	ps, err := uc.fetch(ctx, nums)
	if err != nil {
		return nil, err
	}
	uc.add(ps)
	if p := uc.urls[n]; p != nil {
		return p, nil
	}
	return nil, fmt.Errorf("server returned no URL for part %d", n)
}

// StreamOptions configure UploadStream.
type StreamOptions struct {
	Name        string
	ContentType string
	Metadata    map[string]string
	// Size is the total size if known, or -1. The server rejects streams
	// that differ from a known size.
	Size int64
	// ChunkSize of each gRPC message (default 256 KiB).
	ChunkSize int
}

// UploadStream uploads r by streaming it through the service. Use it when
// the size is unknown or storage is not reachable from the caller.
func (c *Client) UploadStream(ctx context.Context, r io.Reader, o StreamOptions) (*v1.File, error) {
	cs, err := c.conn.NewClientStream(ctx, v1.MethodUploadStream)
	if err != nil {
		return nil, err
	}
	hdr := &v1.UploadStreamHeader{Name: o.Name, ContentType: o.ContentType, Metadata: o.Metadata}
	if o.Size >= 0 {
		size := o.Size
		hdr.Size = &size
	}
	chunk := o.ChunkSize
	if chunk <= 0 {
		chunk = 256 << 10
	}
	var resp v1.UploadStreamResponse
	if cs.Send(&v1.UploadStreamRequest{Header: hdr}) == nil {
		buf := make([]byte, chunk)
		for {
			n, rerr := io.ReadFull(r, buf)
			if n > 0 {
				if cs.Send(&v1.UploadStreamRequest{Chunk: buf[:n]}) != nil {
					break // the server ended the call; CloseAndRecv reports why
				}
			}
			if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
				break
			}
			if rerr != nil {
				cs.Abort(rerr)
				return nil, fmt.Errorf("reading upload source: %w", rerr)
			}
		}
	}
	if err := cs.CloseAndRecv(&resp); err != nil {
		return nil, err
	}
	return resp.File, nil
}
