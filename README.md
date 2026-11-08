# fileService

A file upload / download service on **Amazon S3** (or any S3-compatible store) with **CDN downloads** (CloudFront signed URLs), built for **large files** and for **plugging into other services** over **gRPC** or REST.

- **Large uploads go straight to S3.** Clients get presigned multipart URLs and upload parts in parallel, directly to the bucket; bytes never pass through the service. Uploads are resumable and parts are size-pinned and MD5-checked. Up to 5 TiB per file.
- **Downloads come from the CDN.** The service hands out short-lived CloudFront signed URLs (or presigned S3 URLs without a CDN). Both support HTTP `Range`, so clients download in parallel and resume.
- **Streaming fallback.** Callers that can't reach S3 can stream through the service (`UploadStream` / `DownloadStream`, or `POST /v1/files` and `?proxy=true`) with bounded memory and server-side SHA-256.
- **Pluggable.** gRPC API (`proto/fileservice/v1/fileservice.proto`) for any language, a Go SDK (`pkg/client`), a REST/JSON API for browsers, per-service namespaces via API keys, and signed webhooks (`file.uploaded`, `file.deleted`).
- **Stateless and simple to run.** File records live as small JSON objects in the same bucket — no database. Run as many replicas as you like.
- **Zero third-party dependencies.** Go standard library only; one static binary.

## Contents

- [How it works](#how-it-works)
- [Quick start](#quick-start)
- [Using it from other services](#using-it-from-other-services)
- [API reference](#api-reference)
- [Configuration](#configuration)
- [Production on AWS](#production-on-aws)
- [Webhooks](#webhooks)
- [Operations](#operations)
- [Development](#development)
- [Design notes](#design-notes)

## How it works

```mermaid
sequenceDiagram
    autonumber
    participant C as Client / service
    participant F as fileService
    participant S as S3 bucket
    participant CDN as CloudFront

    C->>F: CreateUpload(name, size)
    F->>S: CreateMultipartUpload
    F-->>C: upload_id + presigned part URLs
    par parts in parallel
        C->>S: PUT part 1..N (presigned, Content-Length pinned)
    end
    C->>F: CompleteUpload(upload_id)
    F->>S: ListParts (verify sizes) + CompleteMultipartUpload
    F-->>C: File (available) and webhook file.uploaded
    C->>F: GetDownloadURL(file_id)
    F-->>C: CloudFront signed URL (or presigned S3 URL)
    C->>CDN: GET (Range requests in parallel)
    CDN->>S: origin fetch (Origin Access Control)
```

Missing a part? `CompleteUpload` reports exactly which ones; `GetUpload` lists what is stored so a client can resume. Incomplete uploads expire (24h by default) and are cleaned up by a background janitor.

## Quick start

### Run locally, nothing to install

`-dev` starts the service with an embedded in-memory S3 and no authentication:

```bash
go run ./cmd/fileservice -dev
```

In another terminal, use the CLI (gRPC on `localhost:9090`):

```bash
go build -o fsctl ./cmd/fsctl
./fsctl upload ./big-video.mp4             # parallel direct upload
./fsctl ls
./fsctl download -o copy.mp4 <id>          # parallel ranged download
./fsctl url -ttl 1h <id>                   # signed URL for anything else
cat log.txt | ./fsctl upload -name log.txt -   # stream stdin through the service
```

### With MinIO (S3-compatible) in Docker

```bash
docker compose up --build
export FILESERVICE_API_KEY=dev-api-key-change-me
./fsctl upload ./big-video.mp4
curl -H "Authorization: Bearer $FILESERVICE_API_KEY" localhost:8080/v1/files
```

## Using it from other services

Each calling service gets an API key bound to a **namespace**; it only sees its own files. Send the key as `authorization: Bearer <key>` (gRPC metadata or HTTP header) or `x-api-key: <key>`.

### Go SDK

```go
import "github.com/go-abhi/fileService/pkg/client"

c, err := client.New("fileservice:9090", client.WithAPIKey(key), client.WithInsecure())

// Upload: parts go straight to S3, 4 in parallel; resumable on failure.
f, err := c.UploadFile(ctx, "/data/video.mp4", client.UploadOptions{
	Metadata:   map[string]string{"order-id": "1234"},
	OnProgress: func(done, total int64) { log.Printf("%d/%d", done, total) },
})

// Download: parallel Range requests to the CDN, written to a file.
_, err = c.DownloadFile(ctx, f.ID, "/tmp/video.mp4", client.DownloadOptions{})

// Or hand a URL to a browser / another system.
u, err := c.URL(ctx, f.ID, 15*time.Minute, true /* attachment */)

// Unknown size, or no network path to S3: stream through the service.
f, err = c.UploadStream(ctx, reader, client.StreamOptions{Name: "export.csv", Size: -1})
_, err = c.DownloadStream(ctx, f.ID, w, 0, 0)
```

A failed upload returns `*client.UploadError` with the `UploadID`; with `KeepOnError: true` it can be finished later with `c.ResumeUpload(ctx, id, file, opts)` — only missing parts are sent. Errors carry gRPC codes: `client.CodeOf(err) == client.NotFound`.

### Any other language (gRPC)

Generate a client from [`proto/fileservice/v1/fileservice.proto`](proto/fileservice/v1/fileservice.proto) with `protoc` or `buf generate` and connect to port 9090 (plaintext h2c, or TLS when configured). The flow is the same: `CreateUpload` → PUT each part to its `url` with exactly `size` bytes → `CompleteUpload`; `GetDownloadURL` → GET with `Range`.

```bash
grpcurl -plaintext -import-path proto -proto fileservice/v1/fileservice.proto \
  -H 'authorization: Bearer dev-api-key-change-me' \
  -d '{"name":"report.pdf","size":"1048576"}' \
  localhost:9090 fileservice.v1.FileService/CreateUpload
```

### Browsers (REST)

Enable CORS on the service (`FILESERVICE_CORS_ORIGINS`) and on the bucket ([`deploy/aws/s3-cors.json`](deploy/aws/s3-cors.json)), then upload straight to S3:

```js
async function upload(file, api, key) {
  const headers = { Authorization: `Bearer ${key}`, "Content-Type": "application/json" };
  const post = (path, body) => fetch(api + path, { method: "POST", headers, body: JSON.stringify(body) }).then(r => r.json());

  const up = await post("/v1/uploads", { name: file.name, size: file.size, content_type: file.type });
  if (up.file.status === "available") return up.file; // empty file

  const parts = up.parts;          // URLs for the first 100 parts
  for (let n = parts.length + 1; n <= up.part_count; n += 1000) {
    const nums = Array.from({ length: Math.min(1000, up.part_count - n + 1) }, (_, i) => n + i);
    parts.push(...(await post(`/v1/uploads/${up.upload_id}/parts`, { part_numbers: nums })).parts);
  }
  let next = 0;                    // 4 parts in flight
  await Promise.all([0, 1, 2, 3].map(async () => {
    while (next < parts.length) {
      const p = parts[next++];
      const res = await fetch(p.url, { method: "PUT", body: file.slice(p.offset, p.offset + p.size) });
      if (!res.ok) throw new Error(`part ${p.part_number}: HTTP ${res.status}`);
    }
  }));
  return post(`/v1/uploads/${up.upload_id}/complete`, {});
}
```

Part URLs expire after an hour (`FILESERVICE_UPLOAD_URL_TTL`); for very large uploads request them lazily and re-request on HTTP 403.

To show or download a file, point the browser at `GET /v1/files/{id}/content` — it redirects to the CDN — or fetch `GET /v1/files/{id}/url` and use the URL.

## API reference

### gRPC — `fileservice.v1.FileService`

| RPC | Purpose |
|---|---|
| `CreateUpload` | Start a direct upload; returns part size/count and presigned URLs for the first 100 parts |
| `GetUploadPartURLs` | Fresh presigned URLs for given parts (≤ 1000 per call) |
| `GetUpload` | Upload state and stored parts (for resuming) |
| `CompleteUpload` | Verify all parts and assemble the file (idempotent) |
| `AbortUpload` | Cancel an upload |
| `UploadStream` | Client stream: header message, then content chunks |
| `DownloadStream` | Server stream: file metadata, then content chunks (optional byte range) |
| `GetDownloadURL` | CDN / presigned URL with TTL and inline/attachment disposition |
| `GetFile`, `ListFiles`, `DeleteFile` | Metadata, newest-first listing with page tokens, deletion |

The standard `grpc.health.v1.Health` service is also served (no credentials needed).

Errors use standard codes: `INVALID_ARGUMENT` (bad input, too large), `NOT_FOUND`, `FAILED_PRECONDITION` (e.g. missing parts, upload expired, file still uploading), `OUT_OF_RANGE`, `UNAUTHENTICATED`, `UNAVAILABLE`.

### REST

| Method & path | Purpose |
|---|---|
| `POST /v1/uploads` | `{"name","size","content_type","part_size","metadata"}` → upload with part URLs |
| `GET /v1/uploads/{id}` | Upload state with `uploaded_parts` |
| `POST /v1/uploads/{id}/parts` | `{"part_numbers":[…]}` → fresh part URLs |
| `POST /v1/uploads/{id}/complete` | Assemble → file |
| `DELETE /v1/uploads/{id}` | Abort |
| `POST /v1/files?name=…` | Upload the request body by streaming (`meta.<key>=value` for metadata; `X-File-Name` header also works) |
| `GET /v1/files` | List (`page_size`, `page_token`, `include_pending`) |
| `GET /v1/files/{id}` / `DELETE /v1/files/{id}` | Metadata / delete |
| `GET /v1/files/{id}/url` | Signed URL (`ttl=15m`, `disposition=attachment`) |
| `GET /v1/files/{id}/content` | `302` to the signed URL; `?proxy=true` streams through the service with `Range` support |
| `GET /healthz`, `GET /readyz` | Liveness / readiness (checks the bucket) |

Errors: `{"error":{"code":"not_found","message":"…"}}` with 400/401/404/409/413/416/503 status codes. Port 8080 also accepts gRPC (routed by `content-type`), so a single port is enough behind load balancers that support HTTP/2.

## Configuration

All settings are environment variables. AWS credentials use the standard chain: `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, `~/.aws/credentials` (`AWS_PROFILE`), EKS IRSA (web identity), ECS task roles / EKS Pod Identity, then the EC2 instance profile (IMDSv2).

| Variable | Default | Description |
|---|---|---|
| `FILESERVICE_S3_BUCKET` | — | Bucket (required) |
| `FILESERVICE_S3_REGION` | `AWS_REGION` or `us-east-1` | Bucket region |
| `FILESERVICE_S3_ENDPOINT` | AWS | S3-compatible endpoint (MinIO, Cloudflare R2, …) |
| `FILESERVICE_S3_PUBLIC_ENDPOINT` | endpoint | Endpoint used inside presigned URLs, when clients reach storage at another address |
| `FILESERVICE_S3_PATH_STYLE` | `true` with a custom endpoint | Path-style addressing |
| `FILESERVICE_S3_ACCELERATE` | `false` | Use S3 Transfer Acceleration for upload URLs (enable it on the bucket) |
| `FILESERVICE_S3_SSE` / `_SSE_KMS_KEY_ID` | — | Server-side encryption (`AES256` or `aws:kms`) |
| `FILESERVICE_S3_PREFIX` | — | Key prefix for everything the service writes |
| `FILESERVICE_S3_CREATE_BUCKET` | `false` | Create the bucket if missing |
| `FILESERVICE_API_KEYS` | — | `namespace:key[,namespace:key…]` (keys ≥ 16 chars; several keys per namespace allow rotation) |
| `FILESERVICE_AUTH_DISABLED` | `false` | Accept everyone into `FILESERVICE_DEFAULT_NAMESPACE` (development only) |
| `FILESERVICE_CDN_DOMAIN` | — | CloudFront domain, e.g. `d111.cloudfront.net` or `cdn.example.com`; enables CDN URLs |
| `FILESERVICE_CDN_KEY_PAIR_ID` | — | Public key ID from the distribution's trusted key group |
| `FILESERVICE_CDN_PRIVATE_KEY` / `_FILE` | — | Matching RSA private key (PEM; `\n` escapes accepted) |
| `FILESERVICE_WEBHOOK_URL` / `_SECRET` | — | POST signed events to this URL |
| `FILESERVICE_CORS_ORIGINS` | — | Allowed browser origins (`*` for any) |
| `FILESERVICE_HTTP_ADDR` | `:8080` | REST + health + gRPC |
| `FILESERVICE_GRPC_ADDR` | `:9090` | gRPC only (`off` to disable) |
| `FILESERVICE_TLS_CERT_FILE` / `_KEY_FILE` | — | Serve TLS (HTTP/2 via ALPN) |
| `FILESERVICE_PART_SIZE` | `16MiB` | Default part size (raised automatically to fit 10,000 parts) |
| `FILESERVICE_MAX_FILE_SIZE` | `5TiB` | Upload size limit |
| `FILESERVICE_ALLOWED_CONTENT_TYPES` | any | e.g. `image/*,application/pdf` |
| `FILESERVICE_UPLOAD_URL_TTL` | `1h` | Lifetime of part URLs |
| `FILESERVICE_UPLOAD_EXPIRY` | `24h` | Incomplete uploads are aborted after this |
| `FILESERVICE_DOWNLOAD_URL_TTL` / `MAX_DOWNLOAD_URL_TTL` | `15m` / `7d` | Download URL lifetime: default / maximum |
| `FILESERVICE_STREAM_PART_SIZE` / `_CONCURRENCY` / `_BUFFER_BUDGET` | `16MiB` / `4` / `64MiB` | Streamed uploads: part size, parallel parts, memory per stream |
| `FILESERVICE_CACHE_CONTROL` | — | `Cache-Control` stored on objects (files are immutable) |
| `FILESERVICE_METADATA_STORE` | `s3` | `s3` (durable, in the bucket) or `memory` |
| `FILESERVICE_JANITOR_INTERVAL` | `10m` | Expired-upload cleanup interval |
| `FILESERVICE_LOG_LEVEL` / `_LOG_FORMAT` | `info` / `json` | `debug…error` / `json`, `text` |
| `FILESERVICE_DEV` / `_DEV_S3_ADDR` | `false` / `127.0.0.1:9000` | Embedded in-memory S3 (same as `-dev`) |

Durations accept `90s`, `15m`, `24h`, `7d`; sizes accept `16MiB`, `5GiB`, `100MB`.

## Production on AWS

1. **Bucket.** Create it (block public access stays on). Add the lifecycle rule in [`deploy/aws/s3-lifecycle.json`](deploy/aws/s3-lifecycle.json) as a safety net for abandoned multipart uploads, and the CORS rules in [`deploy/aws/s3-cors.json`](deploy/aws/s3-cors.json) if browsers upload directly:
   ```bash
   aws s3api put-bucket-lifecycle-configuration --bucket my-files --lifecycle-configuration file://deploy/aws/s3-lifecycle.json
   aws s3api put-bucket-cors --bucket my-files --cors-configuration file://deploy/aws/s3-cors.json
   ```
2. **IAM.** Give the service's role [`deploy/aws/iam-policy.json`](deploy/aws/iam-policy.json) (plus `kms:GenerateDataKey`/`kms:Decrypt` with SSE-KMS). Presigned URLs act with the service's permissions; when the service runs with temporary role credentials, URLs stop working when those credentials expire, and the service shortens `expire_time` accordingly.
3. **CloudFront** (recommended for downloads):
   - Create a distribution with the bucket as origin and **Origin Access Control**; apply the bucket policy CloudFront suggests (`s3:GetObject` for the distribution).
   - Generate a key pair and register the public key in a **key group**:
     ```bash
     openssl genrsa -out cf_private.pem 2048
     openssl rsa -pubout -in cf_private.pem -out cf_public.pem
     ```
   - On the default behavior set **Restrict viewer access → trusted key groups** and use the `CachingOptimized` cache policy. To let `disposition=attachment` work through the CDN, also forward the `response-content-disposition` query string (origin request policy) and include it in the cache key.
   - Configure `FILESERVICE_CDN_DOMAIN`, `FILESERVICE_CDN_KEY_PAIR_ID` and `FILESERVICE_CDN_PRIVATE_KEY_FILE`.
4. **Uploads from far away?** Enable Transfer Acceleration on the bucket and set `FILESERVICE_S3_ACCELERATE=true`.
5. **Deploy.** The container is a static distroless image running as non-root; [`deploy/kubernetes/fileservice.yaml`](deploy/kubernetes/fileservice.yaml) is a starting point for EKS (IRSA, gRPC readiness probe, two replicas).

## Webhooks

With `FILESERVICE_WEBHOOK_URL` set, the service POSTs JSON events asynchronously with retries (at-least-once; de-duplicate on the event `id`):

```json
{"id":"01a1…","type":"file.uploaded","time":"2026-11-08T00:25:33Z",
 "file":{"id":"01a1…","namespace":"orders","name":"invoice.pdf","content_type":"application/pdf","size":48213,"status":"available",…}}
```

Headers: `X-FileService-Event`, `X-FileService-Delivery` (event id), and `X-FileService-Signature: t=<unix>,v1=<hex>` where `v1 = HMAC-SHA256(secret, "<t>." + body)`. Reject requests whose `t` is more than a few minutes old. Go receivers can use `events.Verify` from `internal/events` as a reference.

## Operations

- **Scaling.** The service is stateless: records live in the bucket (strongly consistent), so replicas need no coordination. Direct uploads/downloads cost the service a few small requests per file regardless of size; only streamed transfers use its bandwidth (memory per stream is bounded by `FILESERVICE_STREAM_BUFFER_BUDGET`).
- **Bucket layout.**
  ```
  <prefix><namespace>/<file-id>                        file content
  <prefix>_meta/<namespace>/<sort-key>_<file-id>.json  file record (newest-first listing)
  <prefix>_pending/<namespace>/<file-id>               marker while an upload is in progress
  ```
- **Health.** `/healthz` (liveness), `/readyz` and gRPC `grpc.health.v1.Health/Check` (bucket reachable; cached 5s).
- **Logs.** One structured line per request/call (method, code, duration, peer) plus upload/delete events.
- **Shutdown.** `SIGTERM` stops accepting connections and lets in-flight calls finish (30s), then flushes webhooks.
- **Limits.** Parts are 5 MiB–5 GiB with at most 10,000 per upload (S3 rules); the part size grows automatically for big files. Streamed uploads of unknown size are limited to `STREAM_PART_SIZE × 10,000` (156 GiB by default) — use direct uploads for larger files.

## Development

```bash
make test         # unit + end-to-end tests; no services needed (embedded fake S3)
make race         # with the race detector
make lint         # gofmt + go vet
make integration  # against MinIO on localhost:9000 (docker compose up minio)
make run-dev      # local server with the in-memory S3
```

```
api/fileservice/v1/   Go types of the API (protobuf wire format)
cmd/fileservice/      server
cmd/fsctl/            CLI
pkg/client/           Go SDK
proto/                API definition (.proto)
internal/core/        business logic and the ports (ObjectStorage, MetadataStore, URLSigner, EventPublisher)
internal/s3/          S3 client: SigV4, multipart, presigning, AWS credential chain
internal/s3fake/      in-memory S3 server for tests and -dev (verifies SigV4 signatures)
internal/metastore/   file records: S3 JSON store, in-memory store, conformance suite
internal/grpcx/       gRPC protocol on net/http (unary, client/server streaming, health)
internal/transport/   gRPC and REST handlers
internal/cdn/         CloudFront signed URLs
internal/events/      webhooks
internal/auth/        API keys
```

Extension points are interfaces in `internal/core/ports.go` and `internal/auth`: add a metadata store (e.g. Postgres or DynamoDB) and run `internal/metastore/storetest` against it; swap the URL signer for another CDN; plug JWT/OIDC in as an `auth.Authenticator`.

## Design notes

- **Why presigned multipart uploads?** The service never becomes the bottleneck: N clients upload in parallel at S3's speed, each part can be retried on its own, and an interrupted upload resumes where it stopped. `Content-Length` is part of each URL's signature, so S3 rejects wrong-sized parts; the service still verifies every part before assembling.
- **Why CloudFront signed URLs?** Edge caching for popular files, Range support for parallel downloads, and access control without proxying bytes. Without a CDN, the same API returns presigned S3 URLs.
- **No dependencies, standard protocols.** Everything is built on the Go standard library: the S3 client implements SigV4 (checked against AWS's published signature examples), the gRPC server and client run on `net/http`'s native HTTP/2 (h2c and TLS), and the API types encode the standard protobuf wire format. The encoding is checked against the official protobuf runtime (golden data in `api/fileservice/v1/testdata`), and the gRPC transport was exercised with an independent HTTP/2 client. Any standard gRPC client generated from the `.proto` works against the server.
