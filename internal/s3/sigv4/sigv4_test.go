package sigv4

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// Test vectors from the AWS documentation:
// "Signature Calculations for the Authorization Header (AWS Signature Version 4)"
// and "Authenticating Requests: Using Query Parameters".
var (
	docCreds = Credentials{
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}
	docTime   = time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)
	docSigner = Signer{Service: "s3", Region: "us-east-1"}
)

func signatureOf(t *testing.T, req *http.Request) string {
	t.Helper()
	auth := req.Header.Get("Authorization")
	i := strings.Index(auth, "Signature=")
	if i < 0 {
		t.Fatalf("no signature in %q", auth)
	}
	return auth[i+len("Signature="):]
}

func TestSignAWSDocExamples(t *testing.T) {
	cases := []struct {
		name    string
		method  string
		url     string
		headers map[string]string
		payload string
		want    string
		signed  string
	}{
		{
			name:    "GET object with range",
			method:  "GET",
			url:     "https://examplebucket.s3.amazonaws.com/test.txt",
			headers: map[string]string{"Range": "bytes=0-9"},
			want:    "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
			signed:  "host;range;x-amz-content-sha256;x-amz-date",
		},
		{
			name:   "PUT object",
			method: "PUT",
			url:    "https://examplebucket.s3.amazonaws.com/test$file.text",
			headers: map[string]string{
				"Date":                "Fri, 24 May 2013 00:00:00 GMT",
				"X-Amz-Storage-Class": "REDUCED_REDUNDANCY",
			},
			payload: "Welcome to Amazon S3.",
			want:    "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd",
			signed:  "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class",
		},
		{
			name:   "GET bucket lifecycle",
			method: "GET",
			url:    "https://examplebucket.s3.amazonaws.com/?lifecycle",
			want:   "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543",
			signed: "host;x-amz-content-sha256;x-amz-date",
		},
		{
			name:   "list objects",
			method: "GET",
			url:    "https://examplebucket.s3.amazonaws.com/?max-keys=2&prefix=J",
			want:   "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7",
			signed: "host;x-amz-content-sha256;x-amz-date",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, tc.url, strings.NewReader(tc.payload))
			if err != nil {
				t.Fatal(err)
			}
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			if err := docSigner.Sign(req, HashHex([]byte(tc.payload)), docCreds, docTime); err != nil {
				t.Fatal(err)
			}
			if got := signatureOf(t, req); got != tc.want {
				t.Errorf("signature = %s, want %s", got, tc.want)
			}
			if !strings.Contains(req.Header.Get("Authorization"), "SignedHeaders="+tc.signed+",") {
				t.Errorf("signed headers mismatch: %s", req.Header.Get("Authorization"))
			}
		})
	}
}

func TestPresignAWSDocExample(t *testing.T) {
	u, _ := url.Parse("https://examplebucket.s3.amazonaws.com/test.txt")
	got, err := docSigner.Presign("GET", u, nil, docCreds, docTime, 86400*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	const wantSig = "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
	if !strings.HasSuffix(got, "&X-Amz-Signature="+wantSig) {
		t.Fatalf("presigned URL %s does not end with expected signature %s", got, wantSig)
	}
	pu, err := url.Parse(got)
	if err != nil {
		t.Fatal(err)
	}
	q := pu.Query()
	if q.Get("X-Amz-Credential") != "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request" ||
		q.Get("X-Amz-Expires") != "86400" || q.Get("X-Amz-SignedHeaders") != "host" {
		t.Fatalf("unexpected query: %v", q)
	}
}

// "get-vanilla" from the AWS SigV4 test suite (non-S3 service).
func TestSignGetVanilla(t *testing.T) {
	req, _ := http.NewRequest("GET", "https://example.amazonaws.com/", nil)
	creds := Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"}
	s := Signer{Service: "service", Region: "us-east-1"}
	if err := s.Sign(req, EmptyPayloadHash, creds, time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	const want = "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
	if got := req.Header.Get("Authorization"); got != want {
		t.Fatalf("Authorization =\n%s\nwant\n%s", got, want)
	}
}

func TestPresignSignsExtraHeadersAndToken(t *testing.T) {
	u, _ := url.Parse("http://localhost:9000/bucket/key?partNumber=2&uploadId=abc")
	creds := docCreds
	creds.SessionToken = "tok/en+="
	h := http.Header{}
	h.Set("Content-Length", "5242880")
	got, err := docSigner.Presign("PUT", u, h, creds, docTime, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	pu, _ := url.Parse(got)
	q := pu.Query()
	if q.Get("X-Amz-SignedHeaders") != "content-length;host" {
		t.Fatalf("signed headers = %q", q.Get("X-Amz-SignedHeaders"))
	}
	if q.Get("X-Amz-Security-Token") != "tok/en+=" || q.Get("partNumber") != "2" || q.Get("uploadId") != "abc" {
		t.Fatalf("query lost parameters: %v", q)
	}

	// Recompute the signature the way a server would and compare.
	sig := q.Get("X-Amz-Signature")
	q.Del("X-Amz-Signature")
	canon := CanonicalHeaders([]string{"content-length", "host"}, http.Header{}, pu.Host, 5242880)
	creq := CanonicalRequest("PUT", CanonicalURI(pu), CanonicalQuery(q), canon, "content-length;host", UnsignedPayload)
	scope := Scope(docTime, "us-east-1", "s3")
	if want := Signature(SigningKey(creds.SecretAccessKey, docTime, "us-east-1", "s3"), StringToSign(docTime, scope, creq)); want != sig {
		t.Fatalf("server-side recomputation mismatch: %s vs %s", want, sig)
	}
}

func TestEscape(t *testing.T) {
	cases := map[string]string{
		"abc-_.~XYZ019": "abc-_.~XYZ019",
		"a b":           "a%20b",
		"a/b":           "a%2Fb",
		"ü":             "%C3%BC",
		"$&+,:;=@":      "%24%26%2B%2C%3A%3B%3D%40",
	}
	for in, want := range cases {
		if got := Escape(in); got != want {
			t.Errorf("Escape(%q) = %q, want %q", in, got, want)
		}
	}
	if got := EscapePath("/a b/c$"); got != "/a%20b/c%24" {
		t.Errorf("EscapePath = %q", got)
	}
}

func TestPresignRejectsBadExpiry(t *testing.T) {
	u, _ := url.Parse("https://b.s3.amazonaws.com/k")
	if _, err := docSigner.Presign("GET", u, nil, docCreds, docTime, 8*24*time.Hour); err == nil {
		t.Fatal("expected error for >7 day expiry")
	}
	if _, err := docSigner.Presign("GET", u, nil, Credentials{}, docTime, time.Hour); err == nil {
		t.Fatal("expected error for missing credentials")
	}
}
