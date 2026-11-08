// Package storetest is a conformance suite for core.MetadataStore
// implementations. Plug in a new store by calling Run from its tests.
package storetest

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/go-abhi/fileService/internal/core"
)

// IDAt returns a valid UUIDv7 whose timestamp is ms (for ordering tests).
func IDAt(ms int64, seq int) string {
	return fmt.Sprintf("%08x-%04x-7000-8000-%012x", uint64(ms)>>16, uint64(ms)&0xffff, seq)
}

var base = time.Date(2026, 1, 2, 3, 4, 5, 600, time.UTC)

func file(ns string, i int, status core.Status) *core.File {
	f := &core.File{
		ID:          IDAt(1_700_000_000_000+int64(i)*1000, i),
		Namespace:   ns,
		Name:        fmt.Sprintf("file-%d.bin", i),
		ContentType: "application/octet-stream",
		Size:        int64(i) * 100,
		Status:      status,
		Metadata:    map[string]string{"i": fmt.Sprint(i)},
		CreatedAt:   base.Add(time.Duration(i) * time.Second),
		UpdatedAt:   base.Add(time.Duration(i) * time.Second),
	}
	if status == core.StatusPending {
		f.Upload = &core.UploadSession{StorageUploadID: fmt.Sprintf("upload-%d", i), PartSize: 5 << 20, PartCount: 3, ExpiresAt: base.Add(time.Duration(i) * time.Hour)}
	} else {
		f.ETag = `"etag"`
		f.SHA256 = "abc"
	}
	return f
}

func ids(fs []*core.File) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.ID
	}
	return out
}

// Run runs the suite against stores returned by newStore (one per subtest).
func Run(t *testing.T, newStore func(t *testing.T) core.MetadataStore) {
	ctx := context.Background()

	t.Run("CreateGet", func(t *testing.T) {
		s := newStore(t)
		f := file("ns1", 1, core.StatusPending)
		if err := s.Create(ctx, f); err != nil {
			t.Fatal(err)
		}
		got, err := s.Get(ctx, "ns1", f.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, f) {
			t.Fatalf("got %+v\nwant %+v", got, f)
		}
		got.Metadata["i"] = "changed"
		again, _ := s.Get(ctx, "ns1", f.ID)
		if again.Metadata["i"] != "1" {
			t.Fatal("store returned a shared reference")
		}
		if _, err := s.Get(ctx, "ns2", f.ID); !core.IsNotFound(err) {
			t.Fatalf("other namespace: err = %v", err)
		}
		if _, err := s.Get(ctx, "ns1", IDAt(1, 99)); !core.IsNotFound(err) {
			t.Fatalf("missing: err = %v", err)
		}
	})

	t.Run("PutAndDelete", func(t *testing.T) {
		s := newStore(t)
		f := file("ns1", 2, core.StatusPending)
		if err := s.Create(ctx, f); err != nil {
			t.Fatal(err)
		}
		f.Status, f.Upload, f.ETag = core.StatusAvailable, nil, `"x"`
		if err := s.Put(ctx, f); err != nil {
			t.Fatal(err)
		}
		got, err := s.Get(ctx, "ns1", f.ID)
		if err != nil || got.Status != core.StatusAvailable || got.Upload != nil || got.ETag != `"x"` {
			t.Fatalf("after put: %v %+v", err, got)
		}
		expired, err := s.ListExpiredUploads(ctx, base.Add(1000*time.Hour), 10)
		if err != nil || len(expired) != 0 {
			t.Fatalf("available file listed as expired upload: %v %v", err, ids(expired))
		}
		if err := s.Delete(ctx, "ns1", f.ID); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, "ns1", f.ID); err != nil {
			t.Fatalf("second delete: %v", err)
		}
		if _, err := s.Get(ctx, "ns1", f.ID); !core.IsNotFound(err) {
			t.Fatalf("after delete: %v", err)
		}
	})

	t.Run("ListNewestFirstPaginated", func(t *testing.T) {
		s := newStore(t)
		var want []string
		for i := 1; i <= 7; i++ {
			st := core.StatusAvailable
			if i == 4 {
				st = core.StatusPending
			}
			f := file("ns1", i, st)
			if err := s.Create(ctx, f); err != nil {
				t.Fatal(err)
			}
			if st == core.StatusAvailable {
				want = append([]string{f.ID}, want...)
			}
		}
		if err := s.Create(ctx, file("other", 9, core.StatusAvailable)); err != nil {
			t.Fatal(err)
		}
		var got []string
		after := ""
		for pages := 0; pages < 10; pages++ {
			fs, next, err := s.List(ctx, core.ListQuery{Namespace: "ns1", Limit: 2, After: after})
			if err != nil {
				t.Fatal(err)
			}
			if len(fs) > 2 {
				t.Fatalf("page too large: %d", len(fs))
			}
			got = append(got, ids(fs)...)
			if next == "" {
				break
			}
			after = next
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("listing\n got %v\nwant %v", got, want)
		}
		all, next, err := s.List(ctx, core.ListQuery{Namespace: "ns1", Limit: 100, IncludePending: true})
		if err != nil || len(all) != 7 || next != "" {
			t.Fatalf("include pending: %v %d %q", err, len(all), next)
		}
		if all[0].Metadata["i"] != "7" {
			t.Fatalf("records not fully loaded: %+v", all[0])
		}
	})

	t.Run("ListExpiredUploads", func(t *testing.T) {
		s := newStore(t)
		for i := 1; i <= 4; i++ {
			if err := s.Create(ctx, file("ns1", i, core.StatusPending)); err != nil {
				t.Fatal(err)
			}
		}
		s.Create(ctx, file("ns2", 5, core.StatusPending))
		s.Create(ctx, file("ns1", 6, core.StatusAvailable))
		// Uploads i expire at base+i hours.
		got, err := s.ListExpiredUploads(ctx, base.Add(3*time.Hour+time.Minute), 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 {
			t.Fatalf("expired = %v", ids(got))
		}
		for _, f := range got {
			if f.Upload == nil || f.Upload.StorageUploadID == "" {
				t.Fatalf("expired upload lacks session: %+v", f)
			}
		}
		limited, err := s.ListExpiredUploads(ctx, base.Add(100*time.Hour), 2)
		if err != nil || len(limited) != 2 {
			t.Fatalf("limit: %v %d", err, len(limited))
		}
	})
}
