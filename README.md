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

- Go 1.25+
- An OpenShift cluster with `KUBECONFIG` configured
- One or more of: Cluster Observability Operator, Tempo Operator, OpenTelemetry Operator, cert-manager

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
make deploy NAMESPACE=opendatahub IMG=quay.io/opendatahub/odh-observability@sha256:3dce083e5446feac95246eb1bfd46d53df47a1a253874ea3d2f5167ccccbe330
```

### Remove from Cluster

```bash
make undeploy
```

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
make e2e-test
```

Or with flags:

```bash
go test ./tests/e2e/ -v -timeout 120m -count=1 \
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
cmd/main.go                     Operator entrypoint
api/v1alpha1/                   CRD type definitions (Monitoring)
internal/
  controller/
    monitoring_reconciler.go    Main reconciliation loop
    actions.go                  Per-subsystem deploy functions
    templatedata.go             Template data builder + validation
    helpers.go                  CRD detection, GVK registry
    conditions/                 Status condition management
    resources/*.tmpl.yaml       Go templates for operand manifests
  webhook/                      Mutating admission webhook
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

## Environment Variables

| Variable | Description |
|----------|-------------|
| `POD_NAMESPACE` | Operator's own namespace (required for local run) |
| `OPERATOR_VERSION` | Stamped into release status |
| `RELATED_IMAGE_*` | Container image overrides for managed components |

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.
