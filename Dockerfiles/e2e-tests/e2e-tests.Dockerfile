ARG BUILDER_IMAGE=registry.access.redhat.com/ubi9/go-toolset:latest
ARG BASE_IMAGE=registry.access.redhat.com/ubi9/ubi-minimal:latest

FROM ${BUILDER_IMAGE} AS builder
ARG TARGETOS=linux
ARG TARGETARCH
ARG TARGETPLATFORM

USER root
WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY api/ api/
COPY internal/ internal/
COPY tests/ tests/

ENV GOEXPERIMENT=strictfipsruntime
RUN CGO_ENABLED=1 GOEXPERIMENT=strictfipsruntime \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go test -c -tags=e2e -o /e2e-tests ./tests/e2e/

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o /gotestsum gotest.tools/gotestsum

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o /test2json $(go env GOROOT)/src/cmd/test2json

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-X 'github.com/opendatahub-io/odh-observability/tests/e2e/runner.TestPackages=e2e=/e2e/e2e-tests'" \
    -o /e2e-run ./tests/e2e/cmd/run/

FROM --platform=$TARGETPLATFORM ${BASE_IMAGE}
ARG USER_ID=65532
WORKDIR /e2e
COPY --from=builder /e2e-tests .
COPY --from=builder /gotestsum /usr/local/bin/gotestsum
COPY --from=builder /test2json /usr/local/bin/test2json
COPY --from=builder /e2e-run .
RUN mkdir -p /artifacts && chown ${USER_ID}:${USER_ID} /artifacts

USER ${USER_ID}:${USER_ID}

LABEL com.redhat.component=odh-observability
LABEL description="ODH Observability e2e tests"
LABEL io.k8s.description="ODH Observability e2e tests"
LABEL io.k8s.display-name="ODH Observability e2e tests"
LABEL io.openshift.tags="openshift,odh,observability,operator,e2e"
LABEL name="odh-observability/odh-observability-e2e-rhel9"
LABEL summary="ODH Observability e2e tests"

ENTRYPOINT ["/e2e/e2e-run"]
CMD []
