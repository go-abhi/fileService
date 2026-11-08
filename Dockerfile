# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS build
WORKDIR /src
# The module has no third-party dependencies: nothing to download.
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/fileservice ./cmd/fileservice \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/fsctl ./cmd/fsctl

# distroless/static ships CA certificates (needed for HTTPS to S3) and runs as non-root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/fileservice /out/fsctl /usr/local/bin/
EXPOSE 8080 9090
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/fileservice"]
