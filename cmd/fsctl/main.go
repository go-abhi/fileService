// Command fsctl is a command-line client for the file service.
//
//	fsctl upload ./video.mp4                 parallel direct upload to S3
//	fsctl upload -stream - < data.bin        stream stdin through the service
//	fsctl download -o copy.mp4 <id>          parallel ranged download from the CDN / S3
//	fsctl url -ttl 1h <id>                   print a signed download URL
//	fsctl ls | info <id> | rm <id> | resume <upload-id> <file> | abort <upload-id>
//
// The server address and API key come from -addr / -api-key or the
// FILESERVICE_ADDR / FILESERVICE_API_KEY environment variables.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	v1 "github.com/go-abhi/fileService/api/fileservice/v1"
	"github.com/go-abhi/fileService/internal/app"
	"github.com/go-abhi/fileService/pkg/client"
)

const usage = `fsctl - file service client

Usage:
  fsctl [global flags] <command> [flags] [args]

Commands:
  upload    [-name N] [-type T] [-part-size S] [-concurrency N] [-meta k=v] [-stream] [-keep] FILE|-
  resume    [-concurrency N] UPLOAD_ID FILE
  download  [-o PATH|-] [-concurrency N] [-stream] [-offset N -length N] ID
  url       [-ttl 15m] [-attachment] ID
  info      ID
  ls        [-limit N] [-pending] [-all]
  rm        ID...
  abort     UPLOAD_ID

Global flags:
`

func main() {
	global := flag.NewFlagSet("fsctl", flag.ExitOnError)
	addr := global.String("addr", envOr("FILESERVICE_ADDR", "localhost:9090"), "server address (host:port, http://… or https://…)")
	apiKey := global.String("api-key", os.Getenv("FILESERVICE_API_KEY"), "API key")
	useTLS := global.Bool("tls", false, "use TLS for host:port addresses")
	quiet := global.Bool("q", false, "no progress output")
	global.Usage = func() {
		fmt.Fprint(os.Stderr, usage)
		global.PrintDefaults()
	}
	global.Parse(os.Args[1:])
	if global.NArg() == 0 {
		global.Usage()
		os.Exit(2)
	}

	opts := []client.Option{client.WithAPIKey(*apiKey)}
	if !*useTLS && !strings.HasPrefix(*addr, "https://") {
		opts = append(opts, client.WithInsecure())
	}
	c, err := client.New(*addr, opts...)
	if err != nil {
		fatal(err)
	}
	defer c.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cmd, args := global.Arg(0), global.Args()[1:]
	r := runner{c: c, progress: !*quiet && isTerminal(os.Stderr)}
	switch cmd {
	case "upload":
		err = r.upload(ctx, args)
	case "resume":
		err = r.resume(ctx, args)
	case "download":
		err = r.download(ctx, args)
	case "url":
		err = r.url(ctx, args)
	case "info":
		err = r.info(ctx, args)
	case "ls":
		err = r.ls(ctx, args)
	case "rm":
		err = r.rm(ctx, args)
	case "abort":
		err = r.abort(ctx, args)
	default:
		global.Usage()
		os.Exit(2)
	}
	if err != nil {
		fatal(err)
	}
}

type runner struct {
	c        *client.Client
	progress bool
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	var ue *client.UploadError
	if errors.As(err, &ue) && !ue.Aborted {
		fmt.Fprintf(os.Stderr, "resume with: fsctl resume %s <file>\n", ue.UploadID)
	}
	os.Exit(1)
}

func isTerminal(f *os.File) bool {
	st, err := f.Stat()
	return err == nil && st.Mode()&os.ModeCharDevice != 0
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	enc.Encode(v)
}

// fileJSON is the CLI's JSON view of a file.
type fileJSON struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace"`
	ContentType string            `json:"content_type"`
	Size        int64             `json:"size"`
	Status      string            `json:"status"`
	SHA256      string            `json:"sha256,omitempty"`
	ETag        string            `json:"etag,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
}

func view(f *v1.File) fileJSON {
	st := "pending"
	if f.Status == v1.FileStatusAvailable {
		st = "available"
	}
	return fileJSON{ID: f.ID, Name: f.Name, Namespace: f.Namespace, ContentType: f.ContentType, Size: f.Size, Status: st,
		SHA256: f.SHA256, ETag: f.ETag, Metadata: f.Metadata, CreatedAt: f.CreateTime}
}

// progressBar prints transfer progress to stderr, at most a few times a second.
func (r runner) progressBar(label string) func(done, total int64) {
	if !r.progress {
		return nil
	}
	start := time.Now()
	var last time.Time
	return func(done, total int64) {
		if now := time.Now(); now.Sub(last) < 200*time.Millisecond && done < total {
			return
		} else {
			last = now
		}
		pct := 100.0
		if total > 0 {
			pct = float64(done) * 100 / float64(total)
		}
		rate := float64(done) / time.Since(start).Seconds() / (1 << 20)
		fmt.Fprintf(os.Stderr, "\r%s %6.2f%%  %s / %s  %.1f MiB/s ", label, pct, human(done), human(total), rate)
		if done >= total {
			fmt.Fprintln(os.Stderr)
		}
	}
}

func human(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

type metaFlags map[string]string

func (m metaFlags) String() string { return "" }
func (m metaFlags) Set(v string) error {
	k, val, ok := strings.Cut(v, "=")
	if !ok {
		return fmt.Errorf("want key=value, got %q", v)
	}
	m[k] = val
	return nil
}

func (r runner) upload(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("upload", flag.ExitOnError)
	name := fs.String("name", "", "file name (default: base name of FILE)")
	ctype := fs.String("type", "", "content type (default: detected from the name)")
	partSize := fs.String("part-size", "", "part size, e.g. 64MiB (default: server default)")
	conc := fs.Int("concurrency", 4, "parallel part uploads")
	stream := fs.Bool("stream", false, "stream through the service instead of uploading directly to storage")
	keep := fs.Bool("keep", false, "keep a failed upload so it can be resumed")
	meta := metaFlags{}
	fs.Var(meta, "meta", "metadata key=value (repeatable)")
	fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("upload needs exactly one FILE (or - for stdin)")
	}
	path := fs.Arg(0)
	if *name == "" && path != "-" {
		*name = filepath.Base(path)
	}
	if *name == "" {
		return errors.New("-name is required when reading stdin")
	}
	var ps int64
	if *partSize != "" {
		n, err := app.ParseSize(*partSize)
		if err != nil {
			return err
		}
		ps = n
	}

	var f *v1.File
	var err error
	if path == "-" || *stream {
		in := io.Reader(os.Stdin)
		size := int64(-1)
		if path != "-" {
			fh, err := os.Open(path)
			if err != nil {
				return err
			}
			defer fh.Close()
			if st, err := fh.Stat(); err == nil {
				size = st.Size()
			}
			in = fh
		}
		f, err = r.c.UploadStream(ctx, in, client.StreamOptions{Name: *name, ContentType: *ctype, Metadata: meta, Size: size})
	} else {
		f, err = r.c.UploadFile(ctx, path, client.UploadOptions{Name: *name, ContentType: *ctype, Metadata: meta, PartSize: ps,
			Concurrency: *conc, KeepOnError: *keep, OnProgress: r.progressBar("upload")})
	}
	if err != nil {
		return err
	}
	printJSON(view(f))
	return nil
}

func (r runner) resume(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("resume", flag.ExitOnError)
	conc := fs.Int("concurrency", 4, "parallel part uploads")
	fs.Parse(args)
	if fs.NArg() != 2 {
		return errors.New("resume needs UPLOAD_ID and FILE")
	}
	fh, err := os.Open(fs.Arg(1))
	if err != nil {
		return err
	}
	defer fh.Close()
	f, err := r.c.ResumeUpload(ctx, fs.Arg(0), fh, client.UploadOptions{Concurrency: *conc, KeepOnError: true, OnProgress: r.progressBar("resume")})
	if err != nil {
		return err
	}
	printJSON(view(f))
	return nil
}

func (r runner) download(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("download", flag.ExitOnError)
	out := fs.String("o", "", "output path, or - for stdout (default: the file's name)")
	conc := fs.Int("concurrency", 4, "parallel ranged requests")
	stream := fs.Bool("stream", false, "stream through the service instead of the CDN / storage")
	offset := fs.Int64("offset", 0, "start offset (with -stream)")
	length := fs.Int64("length", 0, "bytes to read, 0 = to the end (with -stream)")
	fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("download needs exactly one ID")
	}
	id := fs.Arg(0)
	path := *out
	if path == "" {
		f, err := r.c.Stat(ctx, id)
		if err != nil {
			return err
		}
		path = filepath.Base(f.Name)
	}
	if *stream || *offset > 0 || *length > 0 {
		w := io.Writer(os.Stdout)
		if path != "-" {
			fh, err := os.Create(path)
			if err != nil {
				return err
			}
			defer fh.Close()
			w = fh
		}
		_, err := r.c.DownloadStream(ctx, id, w, *offset, *length)
		return err
	}
	o := client.DownloadOptions{Concurrency: *conc, OnProgress: r.progressBar("download")}
	if path == "-" {
		o.OnProgress = nil
		_, err := r.c.Download(ctx, id, os.Stdout, o)
		return err
	}
	f, err := r.c.DownloadFile(ctx, id, path, o)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "saved %s (%s) to %s\n", f.Name, human(f.Size), path)
	return nil
}

func (r runner) url(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("url", flag.ExitOnError)
	ttl := fs.Duration("ttl", 0, "URL lifetime (default: server default)")
	attach := fs.Bool("attachment", false, "make browsers save the file instead of displaying it")
	fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("url needs exactly one ID")
	}
	u, err := r.c.URL(ctx, fs.Arg(0), *ttl, *attach)
	if err != nil {
		return err
	}
	fmt.Println(u.URL)
	fmt.Fprintf(os.Stderr, "expires %s (cdn: %v)\n", u.ExpireTime.Local().Format(time.RFC3339), u.CDN)
	return nil
}

func (r runner) info(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return errors.New("info needs exactly one ID")
	}
	f, err := r.c.Stat(ctx, args[0])
	if err != nil {
		return err
	}
	printJSON(view(f))
	return nil
}

func (r runner) ls(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("ls", flag.ExitOnError)
	limit := fs.Int("limit", 50, "files per page")
	pending := fs.Bool("pending", false, "include uploads in progress")
	all := fs.Bool("all", false, "list every page")
	fs.Parse(args)
	token := ""
	var files []fileJSON
	for {
		resp, err := r.c.ListFiles(ctx, &v1.ListFilesRequest{PageSize: int32(*limit), PageToken: token, IncludePending: *pending})
		if err != nil {
			return err
		}
		for _, f := range resp.Files {
			files = append(files, view(f))
		}
		if !*all || resp.NextPageToken == "" {
			if resp.NextPageToken != "" {
				fmt.Fprintln(os.Stderr, "more files available; use -all to list everything")
			}
			break
		}
		token = resp.NextPageToken
	}
	if files == nil {
		files = []fileJSON{}
	}
	printJSON(files)
	return nil
}

func (r runner) rm(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("rm needs at least one ID")
	}
	for _, id := range args {
		if err := r.c.Delete(ctx, id); err != nil {
			return fmt.Errorf("%s: %w", id, err)
		}
		fmt.Fprintln(os.Stderr, "deleted", id)
	}
	return nil
}

func (r runner) abort(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return errors.New("abort needs exactly one UPLOAD_ID")
	}
	_, err := r.c.AbortUpload(ctx, &v1.AbortUploadRequest{UploadID: args[0]})
	return err
}
