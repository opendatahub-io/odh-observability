# Build the manager binary
# Build context must be the parent directory (RHOAI-dev/) so that
# odh-platform-utilities is accessible alongside odh-observability.
# Use: docker build -f odh-observability/Dockerfile -t <img> <parent-dir>
# Or:  make docker-build (Makefile sets the context to ..)
FROM golang:1.25 AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
COPY odh-observability/go.mod go.mod
COPY odh-observability/go.sum go.sum

# Local odh-platform-utilities sibling directory
COPY odh-platform-utilities/ /odh-platform-utilities/

RUN go mod download

COPY odh-observability/cmd/main.go cmd/main.go
COPY odh-observability/api/ api/
COPY odh-observability/internal/ internal/

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o manager cmd/main.go

# Use distroless as minimal base image to package the manager binary
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]
