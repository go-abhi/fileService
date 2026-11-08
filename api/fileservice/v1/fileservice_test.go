package fileservicev1

import (
	"bufio"
	"encoding/hex"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-abhi/fileService/internal/pbwire"
)

var constructors = map[string]func() pbwire.Message{
	"File":                      func() pbwire.Message { return new(File) },
	"UploadPartURL":             func() pbwire.Message { return new(UploadPartURL) },
	"CreateUploadRequest":       func() pbwire.Message { return new(CreateUploadRequest) },
	"CreateUploadResponse":      func() pbwire.Message { return new(CreateUploadResponse) },
	"GetUploadPartURLsRequest":  func() pbwire.Message { return new(GetUploadPartURLsRequest) },
	"GetUploadPartURLsResponse": func() pbwire.Message { return new(GetUploadPartURLsResponse) },
	"GetUploadRequest":          func() pbwire.Message { return new(GetUploadRequest) },
	"UploadedPart":              func() pbwire.Message { return new(UploadedPart) },
	"GetUploadResponse":         func() pbwire.Message { return new(GetUploadResponse) },
	"CompleteUploadRequest":     func() pbwire.Message { return new(CompleteUploadRequest) },
	"CompleteUploadResponse":    func() pbwire.Message { return new(CompleteUploadResponse) },
	"AbortUploadRequest":        func() pbwire.Message { return new(AbortUploadRequest) },
	"AbortUploadResponse":       func() pbwire.Message { return new(AbortUploadResponse) },
	"UploadStreamHeader":        func() pbwire.Message { return new(UploadStreamHeader) },
	"UploadStreamRequest":       func() pbwire.Message { return new(UploadStreamRequest) },
	"UploadStreamResponse":      func() pbwire.Message { return new(UploadStreamResponse) },
	"DownloadStreamRequest":     func() pbwire.Message { return new(DownloadStreamRequest) },
	"DownloadStreamResponse":    func() pbwire.Message { return new(DownloadStreamResponse) },
	"GetDownloadURLRequest":     func() pbwire.Message { return new(GetDownloadURLRequest) },
	"GetDownloadURLResponse":    func() pbwire.Message { return new(GetDownloadURLResponse) },
	"GetFileRequest":            func() pbwire.Message { return new(GetFileRequest) },
	"GetFileResponse":           func() pbwire.Message { return new(GetFileResponse) },
	"ListFilesRequest":          func() pbwire.Message { return new(ListFilesRequest) },
	"ListFilesResponse":         func() pbwire.Message { return new(ListFilesResponse) },
	"DeleteFileRequest":         func() pbwire.Message { return new(DeleteFileRequest) },
	"DeleteFileResponse":        func() pbwire.Message { return new(DeleteFileResponse) },
}

func loadGolden(t *testing.T) map[string][]byte {
	t.Helper()
	f, err := os.Open("testdata/golden.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string][]byte{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, hx, _ := strings.Cut(line, " ")
		b, err := hex.DecodeString(hx)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		out[name] = b
	}
	return out
}

// TestGoldenRoundTrip decodes encodings produced by the official protobuf
// runtime, re-encodes them and checks nothing is lost.
func TestGoldenRoundTrip(t *testing.T) {
	golden := loadGolden(t)
	seen := map[string]bool{}
	for name, data := range golden {
		typ := strings.SplitN(name, ".", 2)[0]
		ctor := constructors[typ]
		if ctor == nil {
			t.Fatalf("no Go type for %s", typ)
		}
		seen[typ] = true
		m1 := ctor()
		if err := pbwire.Unmarshal(data, m1); err != nil {
			t.Fatalf("%s: decode: %v", name, err)
		}
		m2 := ctor()
		if err := pbwire.Unmarshal(pbwire.Marshal(m1), m2); err != nil {
			t.Fatalf("%s: decode re-encoded: %v", name, err)
		}
		if !reflect.DeepEqual(m1, m2) {
			t.Errorf("%s: round trip mismatch:\n%#v\n%#v", name, m1, m2)
		}
	}
	for typ := range constructors {
		if !seen[typ] {
			t.Errorf("no golden sample for %s", typ)
		}
	}
}

func TestGoldenValues(t *testing.T) {
	golden := loadGolden(t)
	var f File
	if err := pbwire.Unmarshal(golden["File.full0"], &f); err != nil {
		t.Fatal(err)
	}
	wantTime := time.Unix(1700000000, 123456789).UTC()
	if f.ID != "val-id-ü✓" || f.Size != 1234567890123 || f.Status != FileStatusAvailable ||
		f.Metadata["ünï"] != "çödé €" || f.Metadata[""] != "" || len(f.Metadata) != 4 ||
		!f.CreateTime.Equal(wantTime) || !f.UpdateTime.Equal(wantTime) {
		t.Fatalf("File decoded wrong: %+v", f)
	}

	var unk File
	if err := pbwire.Unmarshal(golden["File.unknown"], &unk); err != nil || !reflect.DeepEqual(unk, f) {
		t.Fatalf("unknown fields not skipped: %v %+v", err, unk)
	}
	var wrong File
	if err := pbwire.Unmarshal(golden["File.wrongtype"], &wrong); err != nil || wrong.ID != "a" || wrong.Size != 0 {
		t.Fatalf("wrong wire type should be ignored: %v %+v", err, wrong)
	}

	var req GetUploadPartURLsRequest
	if err := pbwire.Unmarshal(golden["GetUploadPartURLsRequest.unpacked"], &req); err != nil ||
		!reflect.DeepEqual(req.PartNumbers, []int32{5, 7, -1, 9}) || req.UploadID != "abc" {
		t.Fatalf("unpacked/negative int32: %v %+v", err, req)
	}

	var h UploadStreamHeader
	if err := pbwire.Unmarshal(golden["UploadStreamHeader.optzero"], &h); err != nil || h.Size == nil || *h.Size != 0 {
		t.Fatalf("optional presence lost: %v %+v", err, h)
	}
	var chunk UploadStreamRequest
	if err := pbwire.Unmarshal(golden["UploadStreamRequest.emptychunk"], &chunk); err != nil || chunk.Chunk == nil || len(chunk.Chunk) != 0 || chunk.Header != nil {
		t.Fatalf("empty oneof chunk lost: %v %+v", err, chunk)
	}
	var dl DownloadStreamResponse
	if err := pbwire.Unmarshal(golden["DownloadStreamResponse.emptyfile"], &dl); err != nil || dl.File == nil || dl.Chunk != nil {
		t.Fatalf("empty oneof message lost: %v %+v", err, dl)
	}

	var u GetDownloadURLRequest
	if err := pbwire.Unmarshal(golden["GetDownloadURLRequest.full0"], &u); err != nil || u.TTL != 90*time.Second+500 || u.Disposition != ContentDispositionAttachment {
		t.Fatalf("duration/enum: %v %+v", err, u)
	}
}

func TestOneofEncoding(t *testing.T) {
	m := &UploadStreamRequest{Chunk: []byte{}}
	if got := hex.EncodeToString(pbwire.Marshal(m)); got != "1200" {
		t.Fatalf("empty chunk encoding = %s, want 1200", got)
	}
	m = &UploadStreamRequest{Header: &UploadStreamHeader{Name: "a"}, Chunk: []byte("ignored")}
	var back UploadStreamRequest
	if err := pbwire.Unmarshal(pbwire.Marshal(m), &back); err != nil || back.Header == nil || back.Chunk != nil {
		t.Fatalf("oneof should keep only the header: %+v", back)
	}
}

func TestMalformed(t *testing.T) {
	for _, b := range [][]byte{
		{0x0a, 0x05, 'a'},        // truncated string
		{0x08},                   // truncated varint
		{0x00, 0x01},             // field number 0
		{0x0a, 0x02, 0xff, 0xfe}, // invalid UTF-8 in string field
		{0x0f},                   // invalid wire type 7
		{0x08, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}, // varint overflow
	} {
		var f File
		if err := pbwire.Unmarshal(b, &f); err == nil {
			t.Errorf("expected error for %x", b)
		}
	}
}
