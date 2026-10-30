# ODH Observability Operator

A Kubernetes operator for the [Open Data Hub](https://opendatahub.io/) observability module. It manages a single cluster-scoped `Monitoring` custom resource that declaratively provisions a full metrics, traces, and dashboarding stack on OpenShift.

## Overview

The operator watches a singleton `Monitoring` CR (`services.platform.opendatahub.io/v1alpha1`) and dynamically detects which prerequisite operators are installed — deploying only the subsystems whose CRDs are present on the cluster.

### Managed Subsystems

| Subsystem | Prerequisite | Purpose |
|-----------|-------------|---------|
| MonitoringStack + ThanosQuerier | Cluster Observability Operator | Prometheus metrics collection |
| TempoMonolithic / TempoStack | Tempo Operator | Distributed tracing |
| OpenTelemetry Collector | OpenTelemetry Operator | Telemetry pipeline |
| Perses | Perses Operator | Dashboarding |
| Alerting (PrometheusRules) | COO + metrics storage | Alert definitions |
| Mutating Webhook | cert-manager | Label injection on ServiceMonitor/PodMonitor |

## Getting Started

### Prerequisites

- Go 1.26+
- An OpenShift cluster with `KUBECONFIG` configured
- OpenTelemetry Operator for metrics; Cluster Observability Operator for built-in metrics storage
- OpenTelemetry Operator for traces; Tempo Operator for built-in trace storage
- OpenTelemetry Operator and Loki Operator for usage logs
- cert-manager for the mutating webhook and metrics TargetAllocator mTLS

### Build

```bash
make build
```

### Run Locally

```bash
POD_NAMESPACE=opendatahub make run
```

### Deploy to a Cluster

```bash
make deploy NAMESPACE=opendatahub IMG=quay.io/opendatahub/odh-observability:odh-stable
```

### Remove from Cluster

```bash
make undeploy
```

## Compatibility matrix

Known-good and known-bad COO / Tempo / related dependency versions for RHOAI releases are documented in [docs/compatibility-matrix.md](docs/compatibility-matrix.md) (machine-readable: [docs/compatibility-matrix.yaml](docs/compatibility-matrix.yaml)). The automated golden-pair gate is `make e2e-test-coo-compatibility` — see [tests/e2e/README.md](tests/e2e/README.md).

## Custom Resource

Create a `Monitoring` CR to configure the observability stack:

```yaml
apiVersion: services.platform.opendatahub.io/v1alpha1
kind: Monitoring
metadata:
  name: default-monitoring
spec:
  namespace: opendatahub
  metrics:
    storage:
      size: 10Gi
      retention: 15d
    replicas: 2
  traces:
    storage:
      backend: pv
      size: 10Gi
      retention: 48h
    sampleRatio: "0.1"
  alerting: {}
```

### Spec Reference

| Field | Description |
|-------|-------------|
| `managementState` | `Managed` (default) or `Removed` |
| `namespace` | Target namespace for monitoring resources (default: `opendatahub`, immutable) |
| `metrics` | Prometheus metrics — storage size/retention, replicas, OTel exporters |
| `traces` | Distributed tracing — storage backend (`pv`/`s3`/`gcs`), sample ratio, TLS, exporters |
| `usageLogs` | Usage logging via LokiStack |
| `alerting` | Enables PrometheusRules (requires `metrics.storage`) |
| `collectorReplicas` | OTel Collector replica count (requires metrics or traces) |

## Development

### Testing

```bash
make test              # Full pipeline: codegen + fmt + vet + unit tests
make unit-test         # Unit tests only (fastest iteration)
make test-verbose      # Unit tests with -v

# Single test
go test ./internal/controller/ -run TestBuildTemplateData -v
```

### E2E Tests

Requires a live cluster with `KUBECONFIG` set:

```bash
make e2e-test-monitoring
```

Or with flags:

```bash
go test ./tests/e2e/ -v -timeout 120m -count=1 -run '^TestMonitoring$' \
  -monitoring-namespace=opendatahub \
  -install-operators=true \
  -eventually-timeout=5m
```

### Code Generation

```bash
make manifests         # Regenerate CRDs, RBAC, webhook manifests
make generate          # Regenerate DeepCopy implementations
make helm-update-crds  # Sync generated CRDs into the Helm chart
```

### Container Image

```bash
make docker-build      # Build with podman
make docker-push       # Push to registry
```

### Helm Chart

The operator is deployed via the Helm chart in `charts/odh-observability/`.

```bash
make helm-lint         # Lint the chart
make helm-template     # Render templates locally
```

## Architecture

```text
cmd/                            Operator entrypoint
api/                            CRD type definitions (Monitoring)
internal/controller/            Reconciliation loop, actions, templates
internal/webhook/               Mutating admission webhook
charts/odh-observability/       Helm chart for deployment
config/                         Generated manifests (CRDs, RBAC, webhook)
tests/e2e/                      End-to-end test suite
```

### Reconciliation Flow

1. **Precondition checks** — verifies prerequisite operators via OLM OperatorCondition resources
2. **Action functions** — each subsystem checks CRD existence and collects template sources
3. **Template rendering** — Go templates rendered against cluster-derived data
4. **Server-Side Apply** — resources applied via odh-platform-utilities with SSA and caching
5. **Garbage collection** — stale owned resources (by label) not in the desired set are deleted
6. **Post-deploy syncs** — TLS CA sync and status URL discovery

### Key Design Patterns

- **Label-based ownership** — the cluster-scoped CR cannot use OwnerReferences for namespace-scoped resources, so `platform.opendatahub.io/part-of=monitoring` labels are used instead
- **Embedded templates** — resource templates are embedded via `//go:embed`
- **Condition aggregation** — per-feature conditions are aggregated into top-level Ready/Degraded status
- **TLS-only collector scraping** — the OTel Collector's internal telemetry is re-exported through a TLS-enabled pipeline exporter on `:8890` (service `data-science-collector-monitoring`, cert via OpenShift service-CA) because the Go SDK Prometheus pull exporter does not support TLS natively. Prometheus scrapes this endpoint over HTTPS.
- **TLS-protected OTLP receivers** — both the main collector and the optional Usage Logs collector serve OTLP gRPC (`:4317`) and HTTP (`:4318`) with OpenShift service-CA certificates. In-cluster clients should use each collector's generated headless Service DNS name over HTTPS/gRPC TLS and trust `/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt`.
- **NetworkPolicy-guarded collector ingress** — `data-science-collector-monitoring-ingress` is the only policy selecting the collector pods, so it acts as the collector's sole ingress gate on clusters with NetworkPolicy enforcement. It allows the Prometheus pod selector to scrape the active TLS endpoints on `:8889`/`:8890`; the operator-created `:8888` Service is not an active pod-IP listener and is not admitted. OTLP `:4317`/`:4318` uses TLS with an OpenShift service-CA certificate and requires an administrator-enrolled namespace plus the KServe inference or scheduler pod labels. The TargetAllocator has a separate policy allowing only collector pods on `:8080` and discovered Kubernetes API endpoint egress.

### Enrolling inference namespaces for OTLP

Before upgrading, install the enrollment guard from the new checkout and audit
existing enrollment labels. Admission validates future changes; a label set
before the guard existed is not evidence of administrator approval.

```sh
oc apply -f internal/controller/resources/monitoring-admission-policies.tmpl.yaml
oc get namespaces -l monitoring.opendatahub.io/otlp --show-labels
```

A monitoring administrator must remove any unapproved enrollment labels before
deploying the new collector policy. Then enroll every approved namespace hosting
supported inference or scheduler pods that export to the main collector. Enroll
new inference namespaces as part of their provisioning:

```sh
oc label namespace <inference-namespace> monitoring.opendatahub.io/otlp=true
```

The admission policy requires cluster-level permission to update
`monitorings.services.platform.opendatahub.io` to change this label. It protects
Namespace mutations, their subresources, and the OpenShift Project API. Ordinary
metadata updates that preserve enrollment remain permitted. Remove the label to
revoke access; allow time for the CNI to reconcile NetworkPolicy changes.

Enrollment trusts the namespace to submit telemetry to the shared collector.
The pod labels are an additional filter and can be copied by users with pod
creation rights inside an enrolled namespace. The module does not automatically
enroll namespaces based on pods or tenant-created inference resources. Prometheus
scraping and ClusterLogForwarder-to-Loki delivery do not require this label.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `POD_NAMESPACE` | Operator's own namespace (required for local run) |
| `GATEWAY_NAMESPACE` | Optional namespace containing the router or gateway that reaches monitoring proxy routes |
| `OPERATOR_VERSION` | Stamped into release status |
| `RELATED_IMAGE_*` | Container image overrides for managed components |

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.
