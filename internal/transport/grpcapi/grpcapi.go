// Package grpcapi exposes the core service as the fileservice.v1.FileService
// gRPC API.
package grpcapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"

	v1 "github.com/go-abhi/fileService/api/fileservice/v1"
	"github.com/go-abhi/fileService/internal/auth"
	"github.com/go-abhi/fileService/internal/core"
	"github.com/go-abhi/fileService/internal/grpcx"
)

// ChunkSize is the payload size of DownloadStream messages.
const ChunkSize = 256 << 10

type api struct {
	svc *core.Service
	log *slog.Logger
}

// AuthInterceptor authenticates every call except health checks.
func AuthInterceptor(a auth.Authenticator, log *slog.Logger) grpcx.Interceptor {
	return func(ctx context.Context, info *grpcx.CallInfo) (context.Context, error) {
		if strings.HasPrefix(info.FullMethod, "/grpc.health.v1.Health/") {
			return ctx, nil
		}
		p, err := a.Authenticate(ctx, auth.TokenFromHeader(info.Header))
		if err != nil {
			return ctx, toStatus(err, log)
		}
		return core.WithPrincipal(ctx, p), nil
	}
}

// Register adds the FileService methods to s.
func Register(s *grpcx.Server, svc *core.Service, log *slog.Logger) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	a := &api{svc: svc, log: log}
	s.HandleUnary(v1.MethodCreateUpload, unary(a, a.createUpload))
	s.HandleUnary(v1.MethodGetUploadPartURLs, unary(a, a.getUploadPartURLs))
	s.HandleUnary(v1.MethodGetUpload, unary(a, a.getUpload))
	s.HandleUnary(v1.MethodCompleteUpload, unary(a, a.completeUpload))
	s.HandleUnary(v1.MethodAbortUpload, unary(a, a.abortUpload))
	s.HandleUnary(v1.MethodGetDownloadURL, unary(a, a.getDownloadURL))
	s.HandleUnary(v1.MethodGetFile, unary(a, a.getFile))
	s.HandleUnary(v1.MethodListFiles, unary(a, a.listFiles))
	s.HandleUnary(v1.MethodDeleteFile, unary(a, a.deleteFile))
	s.HandleClientStream(v1.MethodUploadStream, a.uploadStream)
	s.HandleServerStream(v1.MethodDownloadStream, a.downloadStream)
}

// unary adapts a typed handler to grpcx.
func unary[Req any, PReq interface {
	*Req
	grpcx.Message
}, Resp grpcx.Message](a *api, fn func(context.Context, PReq) (Resp, error)) grpcx.UnaryHandler {
	return func(ctx context.Context, decode func(grpcx.Message) error) (grpcx.Message, error) {
		req := PReq(new(Req))
		if err := decode(req); err != nil {
			return nil, err
		}
		resp, err := fn(ctx, req)
		if err != nil {
			return nil, toStatus(err, a.log)
		}
		return resp, nil
	}
}

// toStatus maps errors to gRPC statuses without leaking internal details.
func toStatus(err error, log *slog.Logger) error {
	var st *grpcx.Status
	if errors.As(err, &st) {
		return st
	}
	switch {
	case errors.Is(err, context.Canceled):
		return grpcx.Errorf(grpcx.Canceled, "canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return grpcx.Errorf(grpcx.DeadlineExceeded, "deadline exceeded")
	}
	var ce *core.Error
	if errors.As(err, &ce) && ce.Kind != core.KindInternal {
		return &grpcx.Status{Code: codeOf(ce.Kind), Message: ce.Msg}
	}
	log.Error("internal error", "error", err)
	return grpcx.Errorf(grpcx.Internal, "internal error")
}

func codeOf(k core.Kind) grpcx.Code {
	switch k {
	case core.KindInvalidArgument, core.KindTooLarge:
		return grpcx.InvalidArgument
	case core.KindNotFound:
		return grpcx.NotFound
	case core.KindAlreadyExists:
		return grpcx.AlreadyExists
	case core.KindFailedPrecondition:
		return grpcx.FailedPrecondition
	case core.KindOutOfRange:
		return grpcx.OutOfRange
	case core.KindUnauthenticated:
		return grpcx.Unauthenticated
	case core.KindPermissionDenied:
		return grpcx.PermissionDenied
	case core.KindUnavailable:
		return grpcx.Unavailable
	}
	return grpcx.Internal
}

// ToProto converts a core file to its API representation.
func ToProto(f *core.File) *v1.File {
	if f == nil {
		return nil
	}
	st := v1.FileStatusPending
	if f.Status == core.StatusAvailable {
		st = v1.FileStatusAvailable
	}
	return &v1.File{
		ID: f.ID, Namespace: f.Namespace, Name: f.Name, ContentType: f.ContentType, Size: f.Size,
		SHA256: f.SHA256, ETag: f.ETag, Status: st, Metadata: f.Metadata,
		CreateTime: f.CreatedAt, UpdateTime: f.UpdatedAt,
	}
}

func partsToProto(ps []core.PartURL) []*v1.UploadPartURL {
	out := make([]*v1.UploadPartURL, len(ps))
	for i, p := range ps {
		out[i] = &v1.UploadPartURL{PartNumber: int32(p.PartNumber), URL: p.URL, Method: p.Method, Offset: p.Offset,
			Size: p.Size, Headers: p.Headers, ExpireTime: p.ExpiresAt}
	}
	return out
}

func (a *api) createUpload(ctx context.Context, req *v1.CreateUploadRequest) (*v1.CreateUploadResponse, error) {
	up, err := a.svc.CreateUpload(ctx, core.CreateUploadInput{Name: req.Name, ContentType: req.ContentType, Size: req.Size, PartSize: req.PartSize, Metadata: req.Metadata})
	if err != nil {
		return nil, err
	}
	return &v1.CreateUploadResponse{UploadID: up.File.ID, File: ToProto(up.File), PartSize: up.PartSize,
		PartCount: int32(up.PartCount), Parts: partsToProto(up.Parts), ExpireTime: up.ExpiresAt}, nil
}

func (a *api) getUploadPartURLs(ctx context.Context, req *v1.GetUploadPartURLsRequest) (*v1.GetUploadPartURLsResponse, error) {
	nums := make([]int, len(req.PartNumbers))
	for i, n := range req.PartNumbers {
		nums[i] = int(n)
	}
	parts, err := a.svc.PartURLs(ctx, req.UploadID, nums)
	if err != nil {
		return nil, err
	}
	return &v1.GetUploadPartURLsResponse{Parts: partsToProto(parts)}, nil
}

func (a *api) getUpload(ctx context.Context, req *v1.GetUploadRequest) (*v1.GetUploadResponse, error) {
	up, err := a.svc.GetUpload(ctx, req.UploadID)
	if err != nil {
		return nil, err
	}
	resp := &v1.GetUploadResponse{File: ToProto(up.File), PartSize: up.PartSize, PartCount: int32(up.PartCount), ExpireTime: up.ExpiresAt}
	for _, p := range up.Uploaded {
		resp.UploadedParts = append(resp.UploadedParts, &v1.UploadedPart{PartNumber: int32(p.Number), Size: p.Size, ETag: p.ETag})
	}
	return resp, nil
}

func (a *api) completeUpload(ctx context.Context, req *v1.CompleteUploadRequest) (*v1.CompleteUploadResponse, error) {
	f, err := a.svc.CompleteUpload(ctx, req.UploadID)
	if err != nil {
		return nil, err
	}
	return &v1.CompleteUploadResponse{File: ToProto(f)}, nil
}

func (a *api) abortUpload(ctx context.Context, req *v1.AbortUploadRequest) (*v1.AbortUploadResponse, error) {
	if err := a.svc.AbortUpload(ctx, req.UploadID); err != nil {
		return nil, err
	}
	return &v1.AbortUploadResponse{}, nil
}

func (a *api) getDownloadURL(ctx context.Context, req *v1.GetDownloadURLRequest) (*v1.GetDownloadURLResponse, error) {
	u, err := a.svc.DownloadURL(ctx, core.DownloadURLInput{FileID: req.FileID, TTL: req.TTL, Attachment: req.Disposition == v1.ContentDispositionAttachment})
	if err != nil {
		return nil, err
	}
	return &v1.GetDownloadURLResponse{URL: u.URL, ExpireTime: u.ExpiresAt, CDN: u.CDN, File: ToProto(u.File)}, nil
}

func (a *api) getFile(ctx context.Context, req *v1.GetFileRequest) (*v1.GetFileResponse, error) {
	f, err := a.svc.GetFile(ctx, req.FileID)
	if err != nil {
		return nil, err
	}
	return &v1.GetFileResponse{File: ToProto(f)}, nil
}

func (a *api) listFiles(ctx context.Context, req *v1.ListFilesRequest) (*v1.ListFilesResponse, error) {
	files, next, err := a.svc.ListFiles(ctx, int(req.PageSize), req.PageToken, req.IncludePending)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListFilesResponse{NextPageToken: next, Files: make([]*v1.File, len(files))}
	for i, f := range files {
		resp.Files[i] = ToProto(f)
	}
	return resp, nil
}

func (a *api) deleteFile(ctx context.Context, req *v1.DeleteFileRequest) (*v1.DeleteFileResponse, error) {
	if err := a.svc.DeleteFile(ctx, req.FileID); err != nil {
		return nil, err
	}
	return &v1.DeleteFileResponse{}, nil
}

// chunkReader turns UploadStream messages into an io.Reader.
type chunkReader struct {
	recv func(grpcx.Message) error
	buf  []byte
	err  error
}

func (r *chunkReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		var m v1.UploadStreamRequest
		if err := r.recv(&m); err != nil {
			r.err = err
			continue
		}
		if m.Header != nil {
			r.err = grpcx.Errorf(grpcx.InvalidArgument, "the header must only be sent in the first message")
			continue
		}
		r.buf = m.Chunk
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (a *api) uploadStream(ctx context.Context, recv func(grpcx.Message) error) (grpcx.Message, error) {
	var first v1.UploadStreamRequest
	if err := recv(&first); err != nil {
		if err == io.EOF {
			return nil, grpcx.Errorf(grpcx.InvalidArgument, "empty stream: the first message must carry the header")
		}
		return nil, err
	}
	h := first.Header
	if h == nil {
		return nil, grpcx.Errorf(grpcx.InvalidArgument, "the first message must carry the header")
	}
	size := int64(-1)
	if h.Size != nil {
		if *h.Size < 0 {
			return nil, grpcx.Errorf(grpcx.InvalidArgument, "size must not be negative")
		}
		size = *h.Size
	}
	f, err := a.svc.UploadStream(ctx, core.StreamInput{Name: h.Name, ContentType: h.ContentType, Size: size, Metadata: h.Metadata}, &chunkReader{recv: recv})
	if err != nil {
		return nil, toStatus(err, a.log)
	}
	return &v1.UploadStreamResponse{File: ToProto(f)}, nil
}

func (a *api) downloadStream(ctx context.Context, decode func(grpcx.Message) error, send func(grpcx.Message) error) error {
	var req v1.DownloadStreamRequest
	if err := decode(&req); err != nil {
		return err
	}
	f, rc, n, err := a.svc.OpenFile(ctx, req.FileID, req.Offset, req.Length)
	if err != nil {
		return toStatus(err, a.log)
	}
	defer rc.Close()
	if err := send(&v1.DownloadStreamResponse{File: ToProto(f)}); err != nil {
		return err
	}
	buf := make([]byte, min(int64(ChunkSize), max(n, 1)))
	var sent int64
	for sent < n {
		k, err := io.ReadFull(rc, buf[:min(int64(len(buf)), n-sent)])
		if k > 0 {
			if err := send(&v1.DownloadStreamResponse{Chunk: buf[:k]}); err != nil {
				return err
			}
			sent += int64(k)
		}
		if err != nil {
			if ctx.Err() != nil {
				return toStatus(ctx.Err(), a.log)
			}
			a.log.Error("download stream read failed", "file_id", f.ID, "error", err)
			return grpcx.Errorf(grpcx.Unavailable, "reading file content failed after %d bytes", sent)
		}
	}
	return nil
}
