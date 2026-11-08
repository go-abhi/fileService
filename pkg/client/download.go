package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	v1 "github.com/go-abhi/fileService/api/fileservice/v1"
)

// DownloadOptions configure Download.
type DownloadOptions struct {
	// Concurrency is the number of ranges fetched in parallel when the
	// destination supports WriteAt (default 4).
	Concurrency int
	// ChunkSize of each ranged request (default 16 MiB).
	ChunkSize int64
	// OnProgress is called with the number of bytes written so far.
	OnProgress func(done, total int64)
}

// DownloadFile downloads a file to path, writing to a temporary file first.
func (c *Client) DownloadFile(ctx context.Context, id, path string, o DownloadOptions) (*v1.File, error) {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".partial-*")
	if err != nil {
		return nil, err
	}
	f, err := c.Download(ctx, id, tmp, o)
	if err == nil {
		err = tmp.Chmod(0o644)
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), path)
	}
	if err != nil {
		os.Remove(tmp.Name())
		return nil, err
	}
	return f, nil
}

// Download writes a file's content to w, fetching it from the CDN or storage
// with signed URLs. When w implements io.WriterAt (like *os.File) ranges are
// fetched in parallel; otherwise the content is streamed sequentially,
// resuming from the last byte after transient errors. When the service knows
// the file's SHA-256, sequential downloads are verified against it.
func (c *Client) Download(ctx context.Context, id string, w io.Writer, o DownloadOptions) (*v1.File, error) {
	d := &downloader{c: c, id: id, onProgress: o.OnProgress}
	if err := d.refresh(ctx); err != nil {
		return nil, err
	}
	f := d.file
	d.size = f.Size
	if f.Size == 0 {
		return f, nil
	}
	chunk := o.ChunkSize
	if chunk <= 0 {
		chunk = 16 << 20
	}
	conc := o.Concurrency
	if conc <= 0 {
		conc = 4
	}
	if wa, ok := w.(io.WriterAt); ok && f.Size > chunk && conc > 1 {
		return f, d.parallel(ctx, wa, chunk, conc)
	}
	return f, d.sequential(ctx, w)
}

type downloader struct {
	c          *Client
	id         string
	onProgress func(done, total int64)

	size int64 // fixed after the first URL request

	mu      sync.Mutex
	file    *v1.File
	url     string
	expires time.Time
	done    int64
}

func (d *downloader) refresh(ctx context.Context) error {
	resp, err := d.c.URL(ctx, d.id, 0, false)
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.file, d.url, d.expires = resp.File, resp.URL, resp.ExpireTime
	d.mu.Unlock()
	return nil
}

// signedURL returns a URL valid for at least another minute.
func (d *downloader) signedURL(ctx context.Context, force bool) (string, error) {
	d.mu.Lock()
	u, exp := d.url, d.expires
	d.mu.Unlock()
	if !force && (exp.IsZero() || time.Until(exp) > time.Minute) {
		return u, nil
	}
	if err := d.refresh(ctx); err != nil {
		return "", err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.url, nil
}

func (d *downloader) progress(n int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.done += n
	if d.onProgress != nil {
		d.onProgress(d.done, d.size)
	}
}

// get issues a ranged GET for [off, off+n) (n < 0: to the end).
func (d *downloader) get(ctx context.Context, off, n int64, force bool) (*http.Response, error) {
	u, err := d.signedURL(ctx, force)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	size := d.size
	if off > 0 || (n >= 0 && n < size) {
		end := ""
		if n >= 0 {
			end = strconv.FormatInt(off+n-1, 10)
		}
		req.Header.Set("Range", "bytes="+strconv.FormatInt(off, 10)+"-"+end)
	}
	resp, err := d.c.transfer.Do(req)
	if err != nil {
		return nil, err
	}
	want := http.StatusPartialContent
	if req.Header.Get("Range") == "" {
		want = http.StatusOK
	}
	if resp.StatusCode != want {
		resp.Body.Close()
		return nil, &httpError{status: resp.StatusCode}
	}
	return resp, nil
}

type httpError struct{ status int }

func (e *httpError) Error() string { return "storage returned HTTP " + strconv.Itoa(e.status) }

// retry runs fn with backoff, refreshing the URL after 403s (expiry).
func (d *downloader) retry(ctx context.Context, fn func(force bool) error) error {
	force := false
	var err error
	for attempt := 0; attempt < d.c.retries; attempt++ {
		if attempt > 0 {
			if serr := sleep(ctx, time.Duration(250<<min(attempt, 5))*time.Millisecond); serr != nil {
				return serr
			}
		}
		if err = fn(force); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var he *httpError
		if errors.As(err, &he) {
			switch {
			case he.status == http.StatusForbidden:
				force = true
				continue
			case he.status >= 500 || he.status == http.StatusTooManyRequests:
				continue
			default:
				return err
			}
		}
		if CodeOf(err) != OK && CodeOf(err) != Unknown {
			return err // RPC error while refreshing the URL
		}
	}
	return err
}

func (d *downloader) parallel(ctx context.Context, w io.WriterAt, chunk int64, conc int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	size := d.size
	offsets := make(chan int64)
	var once sync.Once
	var firstErr error
	var wg sync.WaitGroup
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for off := range offsets {
				n := min(chunk, size-off)
				err := d.retry(ctx, func(force bool) error {
					resp, err := d.get(ctx, off, n, force)
					if err != nil {
						return err
					}
					defer resp.Body.Close()
					got, err := io.CopyN(io.NewOffsetWriter(w, off), resp.Body, n)
					if err == nil && got != n {
						err = io.ErrUnexpectedEOF
					}
					return err
				})
				if err != nil {
					once.Do(func() { firstErr = fmt.Errorf("downloading bytes %d-%d: %w", off, off+n-1, err); cancel() })
					return
				}
				d.progress(n)
			}
		}()
	}
feed:
	for off := int64(0); off < size; off += chunk {
		select {
		case offsets <- off:
		case <-ctx.Done():
			break feed
		}
	}
	close(offsets)
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

func (d *downloader) sequential(ctx context.Context, w io.Writer) error {
	size := d.size
	d.mu.Lock()
	want := d.file.SHA256
	d.mu.Unlock()
	var h hash.Hash
	if want != "" {
		h = sha256.New()
	}
	var written int64
	err := d.retry(ctx, func(force bool) error {
		resp, err := d.get(ctx, written, -1, force)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		dst := io.Writer(w)
		if h != nil {
			dst = io.MultiWriter(w, h)
		}
		buf := make([]byte, 256<<10)
		for written < size {
			n, rerr := resp.Body.Read(buf[:min(int64(len(buf)), size-written)])
			if n > 0 {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return werr
				}
				written += int64(n)
				d.progress(int64(n))
			}
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				return rerr
			}
		}
		if written < size {
			return io.ErrUnexpectedEOF
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("downloading %s after %d bytes: %w", d.id, written, err)
	}
	if h != nil && hex.EncodeToString(h.Sum(nil)) != want {
		return fmt.Errorf("downloading %s: SHA-256 mismatch", d.id)
	}
	return nil
}

// DownloadStream streams [offset, offset+length) of a file through the
// service (length 0: to the end). Use it when the CDN / storage is not
// reachable from the caller.
func (c *Client) DownloadStream(ctx context.Context, id string, w io.Writer, offset, length int64) (*v1.File, error) {
	st, err := c.conn.NewServerStream(ctx, v1.MethodDownloadStream, &v1.DownloadStreamRequest{FileID: id, Offset: offset, Length: length})
	if err != nil {
		return nil, err
	}
	defer st.Close()
	var first v1.DownloadStreamResponse
	if err := st.Recv(&first); err != nil {
		if err == io.EOF {
			return nil, errors.New("client: empty download stream")
		}
		return nil, err
	}
	if first.File == nil {
		return nil, errors.New("client: download stream did not start with file metadata")
	}
	for {
		var m v1.DownloadStreamResponse
		err := st.Recv(&m)
		if err == io.EOF {
			return first.File, nil
		}
		if err != nil {
			return nil, err
		}
		if _, err := w.Write(m.Chunk); err != nil {
			return nil, err
		}
	}
}
