ARG GOLANG_VERSION=1.25

ARG BUILDPLATFORM
ARG TARGETPLATFORM

FROM --platform=$BUILDPLATFORM registry.access.redhat.com/ubi9/go-toolset:$GOLANG_VERSION AS builder
ARG CGO_ENABLED=1
ARG GOEXPERIMENT=strictfipsruntime
ARG TARGETOS
ARG TARGETARCH
USER root
WORKDIR /workspace
COPY odh-observability/go.mod go.mod
COPY odh-observability/go.sum go.sum

# Local odh-platform-utilities sibling directory
COPY odh-platform-utilities/ /odh-platform-utilities/

RUN go mod download

COPY odh-observability/cmd/main.go cmd/main.go
COPY odh-observability/api/ api/
COPY odh-observability/internal/ internal/

RUN CGO_ENABLED=${CGO_ENABLED} GOEXPERIMENT=${GOEXPERIMENT} GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -trimpath -ldflags="-s -w" -o manager cmd/main.go

FROM --platform=$TARGETPLATFORM registry.access.redhat.com/ubi9/ubi-minimal:latest
WORKDIR /
COPY --from=builder /workspace/manager .
USER 1001

ENTRYPOINT ["/manager"]
