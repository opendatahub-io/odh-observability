ARG GOLANG_VERSION=1.26

ARG BUILDPLATFORM
ARG TARGETPLATFORM

FROM --platform=$BUILDPLATFORM registry.access.redhat.com/ubi9/go-toolset:$GOLANG_VERSION AS builder
ARG CGO_ENABLED=1
ARG GOEXPERIMENT=strictfipsruntime
ARG TARGETOS
ARG TARGETARCH
USER root
WORKDIR /workspace
COPY go.mod go.sum ./

RUN go mod download

COPY cmd/ cmd/
COPY api/ api/
COPY internal/ internal/

RUN CGO_ENABLED=${CGO_ENABLED} GOEXPERIMENT=${GOEXPERIMENT} GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -trimpath -ldflags="-s -w" -o manager ./cmd

FROM --platform=$TARGETPLATFORM registry.access.redhat.com/ubi9/ubi-minimal@sha256:dfa0eb04abc59221544e74862432496add1f491f915c9b531880f91affc0e12c
WORKDIR /
COPY --from=builder /workspace/manager .
USER 1001

ENTRYPOINT ["/manager"]
