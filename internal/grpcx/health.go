package grpcx

import (
	"context"
	"time"

	"github.com/go-abhi/fileService/internal/pbwire"
)

// Health serving states (grpc.health.v1.HealthCheckResponse.ServingStatus).
const (
	HealthUnknown        int32 = 0
	HealthServing        int32 = 1
	HealthNotServing     int32 = 2
	HealthServiceUnknown int32 = 3
)

// HealthCheckRequest is grpc.health.v1.HealthCheckRequest.
type HealthCheckRequest struct {
	Service string // 1
}

func (m *HealthCheckRequest) Reset()                      { *m = HealthCheckRequest{} }
func (m *HealthCheckRequest) AppendProto(b []byte) []byte { return pbwire.String(b, 1, m.Service) }
func (m *HealthCheckRequest) MergeProto(b []byte) error {
	return pbwire.Parse(b, func(f pbwire.Field) error {
		if f.Num == 1 {
			return f.StringTo(&m.Service)
		}
		return nil
	})
}

// HealthCheckResponse is grpc.health.v1.HealthCheckResponse.
type HealthCheckResponse struct {
	Status int32 // 1 (enum ServingStatus)
}

func (m *HealthCheckResponse) Reset()                      { *m = HealthCheckResponse{} }
func (m *HealthCheckResponse) AppendProto(b []byte) []byte { return pbwire.Int32(b, 1, m.Status) }
func (m *HealthCheckResponse) MergeProto(b []byte) error {
	return pbwire.Parse(b, func(f pbwire.Field) error {
		if f.Num == 1 {
			return f.Int32To(&m.Status)
		}
		return nil
	})
}

// Health method names.
const (
	HealthCheckMethod = "/grpc.health.v1.Health/Check"
	HealthWatchMethod = "/grpc.health.v1.Health/Watch"
)

// RegisterHealth registers the standard grpc.health.v1.Health service
// (used by Kubernetes gRPC probes and grpc-health-probe). check reports the
// status of a service; "" means the whole server. It must return
// HealthServiceUnknown for services it does not know.
func RegisterHealth(s *Server, check func(ctx context.Context, service string) int32) {
	s.HandleUnary(HealthCheckMethod, func(ctx context.Context, decode func(Message) error) (Message, error) {
		var req HealthCheckRequest
		if err := decode(&req); err != nil {
			return nil, err
		}
		st := check(ctx, req.Service)
		if st == HealthServiceUnknown {
			return nil, Errorf(NotFound, "unknown service %q", req.Service)
		}
		return &HealthCheckResponse{Status: st}, nil
	})
	s.HandleServerStream(HealthWatchMethod, func(ctx context.Context, decode func(Message) error, send func(Message) error) error {
		var req HealthCheckRequest
		if err := decode(&req); err != nil {
			return err
		}
		last := int32(-1)
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			if st := check(ctx, req.Service); st != last {
				if err := send(&HealthCheckResponse{Status: st}); err != nil {
					return err
				}
				last = st
			}
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
			}
		}
	})
}
