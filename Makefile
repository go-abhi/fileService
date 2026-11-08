VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: all build test race lint fmt run-dev docker compose-up integration proto-lint clean

all: lint test build

build: ## Build bin/fileservice and bin/fsctl
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o bin/fileservice ./cmd/fileservice
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/fsctl ./cmd/fsctl

test: ## Unit + end-to-end tests (embedded fake S3, no services needed)
	go test -count=1 ./...

race: ## Tests with the race detector
	go test -race -count=1 ./...

lint: ## gofmt + go vet
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)
	go vet ./...

fmt:
	gofmt -w .

run-dev: build ## Run locally with an embedded in-memory S3 (no setup)
	FILESERVICE_LOG_FORMAT=text ./bin/fileservice -dev

docker: ## Build the container image
	docker build --build-arg VERSION=$(VERSION) -t fileservice:$(VERSION) .

compose-up: ## Run the service with MinIO
	docker compose up --build

integration: ## Integration tests against MinIO on localhost:9000
	FILESERVICE_TEST_S3_ENDPOINT=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
		go test -count=1 -run Integration -v ./internal/app/

proto-lint: ## Lint the API definition (requires buf)
	buf lint

clean:
	rm -rf bin dist
