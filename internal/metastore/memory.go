// Package metastore provides core.MetadataStore implementations: an
// in-memory store for tests and development, and a durable store that keeps
// records as JSON objects in the S3 bucket next to the data.
package metastore

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/go-abhi/fileService/internal/core"
)

// Memory is an in-memory MetadataStore. Data is lost on restart.
type Memory struct {
	mu    sync.RWMutex
	files map[string]*core.File
}

// NewMemory returns an empty store.
func NewMemory() *Memory { return &Memory{files: map[string]*core.File{}} }

func memKey(ns, id string) string { return ns + "/" + id }

// Create implements core.MetadataStore.
func (m *Memory) Create(_ context.Context, f *core.File) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := memKey(f.Namespace, f.ID)
	if _, ok := m.files[k]; ok {
		return core.Errorf(core.KindAlreadyExists, "file %s already exists", f.ID)
	}
	m.files[k] = f.Clone()
	return nil
}

// Get implements core.MetadataStore.
func (m *Memory) Get(_ context.Context, ns, id string) (*core.File, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	f, ok := m.files[memKey(ns, id)]
	if !ok {
		return nil, core.Errorf(core.KindNotFound, "file %s not found", id)
	}
	return f.Clone(), nil
}

// Put implements core.MetadataStore.
func (m *Memory) Put(_ context.Context, f *core.File) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[memKey(f.Namespace, f.ID)] = f.Clone()
	return nil
}

// Delete implements core.MetadataStore.
func (m *Memory) Delete(_ context.Context, ns, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, memKey(ns, id))
	return nil
}

// List implements core.MetadataStore (newest first, by UUIDv7 order).
func (m *Memory) List(_ context.Context, q core.ListQuery) ([]*core.File, string, error) {
	m.mu.RLock()
	var all []*core.File
	for _, f := range m.files {
		if f.Namespace != q.Namespace || (!q.IncludePending && f.Status != core.StatusAvailable) {
			continue
		}
		if q.After != "" && f.ID >= q.After {
			continue
		}
		all = append(all, f)
	}
	m.mu.RUnlock()
	sort.Slice(all, func(i, j int) bool { return all[i].ID > all[j].ID })
	next := ""
	if q.Limit > 0 && len(all) > q.Limit {
		all = all[:q.Limit]
		next = all[len(all)-1].ID
	}
	out := make([]*core.File, len(all))
	for i, f := range all {
		out[i] = f.Clone()
	}
	return out, next, nil
}

// ListExpiredUploads implements core.MetadataStore.
func (m *Memory) ListExpiredUploads(_ context.Context, now time.Time, limit int) ([]*core.File, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*core.File
	for _, f := range m.files {
		if f.Status == core.StatusPending && f.Upload != nil && f.Upload.ExpiresAt.Before(now) {
			out = append(out, f.Clone())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Upload.ExpiresAt.Before(out[j].Upload.ExpiresAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// Ping implements readiness checks.
func (m *Memory) Ping(context.Context) error { return nil }
