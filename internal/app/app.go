// Package app wires the file service together from its configuration.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-abhi/fileService/internal/auth"
	"github.com/go-abhi/fileService/internal/cdn"
	"github.com/go-abhi/fileService/internal/core"
	"github.com/go-abhi/fileService/internal/events"
	"github.com/go-abhi/fileService/internal/grpcx"
	"github.com/go-abhi/fileService/internal/metastore"
	"github.com/go-abhi/fileService/internal/s3"
	"github.com/go-abhi/fileService/internal/s3fake"
	"github.com/go-abhi/fileService/internal/storage"
	"github.com/go-abhi/fileService/internal/transport/grpcapi"
	"github.com/go-abhi/fileService/internal/transport/rest"
)

// Dev-mode S3 credentials (the embedded server only listens locally).
const (
	devAccessKey = "fileservice-dev"
	devSecretKey = "fileservice-dev-secret"
	devBucket    = "fileservice-dev"
)

// NewLogger returns the process logger for cfg.
func NewLogger(cfg Config) *slog.Logger {
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}
	if strings.EqualFold(cfg.LogFormat, "text") {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}

// App is a configured, listening file service.
type App struct {
	cfg     Config
	log     *slog.Logger
	svc     *core.Service
	servers []*http.Server
	lns     []net.Listener
	devS3   *http.Server
	devLn   net.Listener
	webhook *events.Webhook
	ready   func(context.Context) error
}

type pinger interface{ Ping(context.Context) error }

// New builds the service and binds its listeners (use ":0" for random
// ports; see HTTPAddr / GRPCAddr). Call Run to serve.
func New(ctx context.Context, cfg Config, log *slog.Logger) (*App, error) {
	a := &App{cfg: cfg, log: log}
	ok := false
	defer func() {
		if !ok {
			a.closeListeners()
		}
	}()

	if cfg.Dev {
		if err := a.startDevS3(); err != nil {
			return nil, err
		}
	}

	client, err := a.s3Client()
	if err != nil {
		return nil, err
	}
	store := storage.NewS3(client)
	if err := a.waitForBucket(ctx, client); err != nil {
		return nil, err
	}

	var meta core.MetadataStore
	if cfg.MetadataStore == "memory" {
		log.Warn("using the in-memory metadata store: file records are lost on restart")
		meta = metastore.NewMemory()
	} else {
		meta = metastore.NewS3(client, cfg.Core.KeyPrefix)
	}

	var signer core.URLSigner
	if cfg.CDNDomain != "" {
		pemKey, err := readFileOr(cfg.CDNPrivateKey, cfg.CDNPrivateKeyFile)
		if err != nil {
			return nil, fmt.Errorf("reading CDN private key: %w", err)
		}
		if !strings.Contains(string(pemKey), "\n") { // PEM passed through a single-line env var
			pemKey = []byte(strings.ReplaceAll(string(pemKey), `\n`, "\n"))
		}
		cf, err := cdn.NewCloudFront(cfg.CDNDomain, cfg.CDNKeyPairID, pemKey)
		if err != nil {
			return nil, err
		}
		signer = cf
		log.Info("downloads served by CloudFront signed URLs", "domain", cfg.CDNDomain)
	} else {
		log.Info("downloads served by presigned S3 URLs (set FILESERVICE_CDN_DOMAIN to use CloudFront)")
	}

	pubs := events.Multi{events.Log{Logger: log}}
	if cfg.WebhookURL != "" {
		a.webhook = events.NewWebhook(cfg.WebhookURL, events.WebhookOptions{Secret: cfg.WebhookSecret, Logger: log})
		pubs = append(pubs, a.webhook)
	}

	a.svc, err = core.New(cfg.Core, core.Deps{Storage: store, Metadata: meta, Signer: signer, Events: pubs, Logger: log})
	if err != nil {
		return nil, err
	}

	authn, err := a.authenticator()
	if err != nil {
		return nil, err
	}

	checks := []pinger{store}
	if p, ok := meta.(pinger); ok && cfg.MetadataStore == "memory" {
		checks = append(checks, p)
	}
	a.ready = cachedCheck(func(ctx context.Context) error {
		for _, c := range checks {
			if err := c.Ping(ctx); err != nil {
				return err
			}
		}
		return nil
	}, 5*time.Second)

	g := grpcx.NewServer(grpcx.WithLogger(log), grpcx.WithInterceptor(grpcapi.AuthInterceptor(authn, log)))
	grpcapi.Register(g, a.svc, log)
	grpcx.RegisterHealth(g, func(ctx context.Context, svc string) int32 {
		if svc != "" && svc != "fileservice.v1.FileService" {
			return grpcx.HealthServiceUnknown
		}
		if a.ready(ctx) != nil {
			return grpcx.HealthNotServing
		}
		return grpcx.HealthServing
	})
	h := rest.NewHandler(a.svc, rest.Options{Auth: authn, Logger: log, CORSOrigins: cfg.CORSOrigins, Ready: a.ready, GRPC: g})

	if cfg.HTTPAddr != "" {
		if err := a.listen(cfg.HTTPAddr, h, "http"); err != nil {
			return nil, err
		}
	}
	if cfg.GRPCAddr != "" {
		if err := a.listen(cfg.GRPCAddr, g, "grpc"); err != nil {
			return nil, err
		}
	}
	ok = true
	return a, nil
}

// Service returns the core service.
func (a *App) Service() *core.Service { return a.svc }

// HTTPAddr returns the address of the HTTP (REST + gRPC) listener.
func (a *App) HTTPAddr() string { return a.addr(0, a.cfg.HTTPAddr) }

// GRPCAddr returns the address of the gRPC listener.
func (a *App) GRPCAddr() string {
	if a.cfg.HTTPAddr == "" {
		return a.addr(0, a.cfg.GRPCAddr)
	}
	return a.addr(1, a.cfg.GRPCAddr)
}

// DevS3Addr returns the address of the embedded S3 server (dev mode).
func (a *App) DevS3Addr() string {
	if a.devLn == nil {
		return ""
	}
	return a.devLn.Addr().String()
}

func (a *App) addr(i int, configured string) string {
	if configured == "" || i >= len(a.lns) {
		return ""
	}
	return a.lns[i].Addr().String()
}

func (a *App) s3Client() (*s3.Client, error) {
	cfg := a.cfg
	sc := s3.Config{
		Bucket:               cfg.S3Bucket,
		Region:               cfg.S3Region,
		Endpoint:             cfg.S3Endpoint,
		PresignEndpoint:      cfg.S3PublicEndpoint,
		PathStyle:            cfg.S3PathStyle,
		Accelerate:           cfg.S3Accelerate,
		ServerSideEncryption: cfg.S3SSE,
		SSEKMSKeyID:          cfg.S3SSEKMSKeyID,
		Credentials:          s3.DefaultCredentials(s3.ChainOptions{Region: cfg.S3Region}),
	}
	if cfg.Dev {
		if sc.Bucket == "" {
			sc.Bucket = devBucket
		}
		sc.Endpoint = "http://" + a.devLn.Addr().String()
		sc.PresignEndpoint = ""
		sc.PathStyle = true
		sc.Region = "us-east-1"
		sc.Credentials = s3.StaticCredentials{AccessKeyID: devAccessKey, SecretAccessKey: devSecretKey}
	}
	return s3.New(sc)
}

func (a *App) startDevS3() error {
	fake := s3fake.New(s3fake.WithCredentials(devAccessKey, devSecretKey))
	bucket := a.cfg.S3Bucket
	if bucket == "" {
		bucket = devBucket
	}
	fake.CreateBucket(bucket)
	ln, err := net.Listen("tcp", a.cfg.DevS3Addr)
	if err != nil {
		return fmt.Errorf("dev S3 listener: %w", err)
	}
	a.devLn = ln
	a.devS3 = &http.Server{Handler: fake, ReadHeaderTimeout: 10 * time.Second}
	a.log.Warn("DEV MODE: using an embedded in-memory S3; data is lost on exit", "s3_addr", ln.Addr().String())
	return nil
}

// waitForBucket checks (or creates) the bucket, retrying while storage
// starts up (e.g. MinIO in docker-compose).
func (a *App) waitForBucket(ctx context.Context, c *s3.Client) error {
	if a.cfg.Dev {
		return nil
	}
	deadline := time.Now().Add(a.cfg.StartupRetryLimit)
	delay := 500 * time.Millisecond
	for {
		var err error
		if a.cfg.S3CreateBucket {
			err = c.EnsureBucket(ctx)
		} else {
			err = c.HeadBucket(ctx)
		}
		if err == nil {
			a.log.Info("connected to storage", "bucket", c.Bucket())
			return nil
		}
		if s3.ErrorCode(err) == "NoSuchBucket" && !a.cfg.S3CreateBucket {
			return fmt.Errorf("bucket %q does not exist (create it, or set FILESERVICE_S3_CREATE_BUCKET=true): %w", c.Bucket(), err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("storage not reachable: %w", err)
		}
		a.log.Warn("waiting for storage", "error", err, "retry_in", delay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, 5*time.Second)
	}
}

func (a *App) authenticator() (auth.Authenticator, error) {
	if a.cfg.APIKeys != "" {
		keys, err := auth.ParseAPIKeys(a.cfg.APIKeys)
		if err != nil {
			return nil, err
		}
		a.log.Info("API key authentication enabled", "keys", keys.Len())
		return keys, nil
	}
	ns := a.cfg.DefaultNamespace
	if !core.ValidNamespace(ns) {
		return nil, fmt.Errorf("invalid FILESERVICE_DEFAULT_NAMESPACE %q", ns)
	}
	a.log.Warn("AUTHENTICATION DISABLED: every caller uses one namespace", "namespace", ns)
	return auth.Anonymous{Namespace: ns}, nil
}

func (a *App) listen(addr string, h http.Handler, name string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("%s listener: %w", name, err)
	}
	var p http.Protocols
	p.SetHTTP1(true)
	p.SetHTTP2(true)
	p.SetUnencryptedHTTP2(true)
	srv := &http.Server{
		Handler:           h,
		Protocols:         &p,
		ReadHeaderTimeout: 15 * time.Second,
		IdleTimeout:       5 * time.Minute,
		HTTP2: &http.HTTP2Config{
			MaxConcurrentStreams:          1000,
			MaxReceiveBufferPerStream:     2 << 20,
			MaxReceiveBufferPerConnection: 3 << 20,
		},
		ErrorLog: slog.NewLogLogger(a.log.Handler(), slog.LevelWarn),
	}
	a.servers = append(a.servers, srv)
	a.lns = append(a.lns, ln)
	return nil
}

func (a *App) closeListeners() {
	for _, ln := range a.lns {
		ln.Close()
	}
	if a.devLn != nil {
		a.devLn.Close()
	}
}

// Run serves until ctx is cancelled, then shuts down gracefully.
func (a *App) Run(ctx context.Context) error {
	errc := make(chan error, len(a.servers)+1)
	serve := func(srv *http.Server, ln net.Listener) {
		var err error
		if a.cfg.TLSCertFile != "" && srv != a.devS3 {
			err = srv.ServeTLS(ln, a.cfg.TLSCertFile, a.cfg.TLSKeyFile)
		} else {
			err = srv.Serve(ln)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}
	if a.devS3 != nil {
		go serve(a.devS3, a.devLn)
	}
	for i, srv := range a.servers {
		go serve(srv, a.lns[i])
	}
	jctx, stopJanitor := context.WithCancel(context.Background())
	go a.svc.RunJanitor(jctx, a.cfg.JanitorInterval)
	a.log.Info("file service started", "http", a.HTTPAddr(), "grpc", a.GRPCAddr(), "tls", a.cfg.TLSCertFile != "")

	var err error
	select {
	case <-ctx.Done():
	case err = <-errc:
		a.log.Error("server failed", "error", err)
	}
	a.log.Info("shutting down")
	stopJanitor()
	sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, srv := range a.servers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if serr := srv.Shutdown(sctx); serr != nil {
				srv.Close()
			}
		}()
	}
	wg.Wait()
	if a.webhook != nil {
		if werr := a.webhook.Close(sctx); werr != nil {
			a.log.Warn("webhook queue not fully delivered", "error", werr)
		}
	}
	if a.devS3 != nil {
		a.devS3.Close()
	}
	a.log.Info("stopped")
	return err
}

// cachedCheck caches the result of check for ttl, so frequent probes do not
// hammer storage.
func cachedCheck(check func(context.Context) error, ttl time.Duration) func(context.Context) error {
	var mu sync.Mutex
	var last time.Time
	var lastErr error
	return func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		if !last.IsZero() && time.Since(last) < ttl {
			return lastErr
		}
		lastErr = check(ctx)
		last = time.Now()
		return lastErr
	}
}
