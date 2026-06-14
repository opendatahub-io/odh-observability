# Monitoring E2E Tests

End-to-end tests for the odh-observability monitoring module. These tests run against a live OpenShift cluster and verify operand health, CR reconciliation, webhook logic, and negative condition reporting.

## Prerequisites

- An OpenShift cluster with `KUBECONFIG` set
- The odh-observability operator deployed and running
- A `Monitoring` CR must exist before running tests. In a full RHOAI deployment the platform operator creates this via SSA from the DSCI. For standalone testing, create one manually:
  ```bash
  oc apply -f - <<EOF
  apiVersion: services.platform.opendatahub.io/v1alpha1
  kind: Monitoring
  metadata:
    name: default-monitoring
  spec:
    managementState: Managed
  EOF
  ```
- Dependent operators installed via OLM:
  - Cluster Observability Operator (provides MonitoringStack, ThanosQuerier)
  - Tempo Operator (provides TempoMonolithic, TempoStack)
  - OpenTelemetry Operator (provides OpenTelemetryCollector, Instrumentation)
  - Perses Operator (provides Perses, PersesDatasource)

## Running

Log in to your cluster (`oc login`) or ensure `KUBECONFIG` points at it, then:

```bash
make e2e-test
```

To pass additional flags:

```bash
make e2e-test E2E_TEST_FLAGS="-run TestMonitoring"
```

## Configuration

All configuration is via test flags, passed through `E2E_TEST_FLAGS`:

| Flag | Default | Description |
|------|---------|-------------|
| `-monitoring-namespace` | `redhat-ods-monitoring` | Namespace where monitoring operands are deployed |
| `-monitoring-cr-name` | `default-monitoring` | Name of the Monitoring CR |
| `-eventually-timeout` | `5m` | Default timeout for Eventually assertions |
| `-eventually-poll-interval` | `2s` | Default poll interval for Eventually assertions |
| `-consistently-timeout` | `30s` | Default timeout for Consistently assertions |
| `-consistently-poll-interval` | `2s` | Default poll interval for Consistently assertions |

Example:

```bash
make e2e-test E2E_TEST_FLAGS="-monitoring-namespace my-namespace -eventually-timeout 10m"
```

## Test Groups

| Group | File | Description |
|-------|------|-------------|
| 1 | `monitoring_test.go` | Base configuration defaults |
| 2 | `monitoring_test.go` | Metrics and MonitoringStack |
| 3 | `monitoring_test.go` | OpenTelemetry Collector |
| 4 | `monitoring_test.go` | Target Allocator |
| 5 | `monitoring_test.go` | Thanos Querier |
| 6 | `monitoring_test.go` | Traces with PV backend |
| 7 | `monitoring_test.go` | Traces with cloud storage (S3, GCS) |
| 8 | `monitoring_test.go` | Perses dashboards and datasources |
| 9 | `monitoring_test.go` | Networking and RBAC |
| 10 | `monitoring_webhook_test.go` | Admission webhook tests |
| 12 | `monitoring_negative_test.go` | Negative condition tests |
| -- | `monitoring_test.go` | Disabled/cleanup validation |

Tests run sequentially within groups and groups run in order. Many tests have sequential dependencies via shared cluster state.

## Architecture

- **`e2e_test.go`** — Entry point, flag registration
- **`test_context_test.go`** — `TestContext` with resource lifecycle helpers (`EnsureResourceExists`, `EventuallyResourcePatched`, `DeleteResource`, etc.)
- **`resource_options_test.go`** — Functional options pattern for configuring resource operations
- **`helper_test.go`** — Setup, cleanup, and jq transform helpers
- **`config_test.go`** — Test configuration and flag defaults
- **`matchers/jq/`** — Gomega matcher wrapping `gojq` for asserting on unstructured k8s objects

## Notes

- Tests operate against the `Monitoring` CR directly, not via DSCInitialization
- Cloud storage tests (Group 7, S3/GCS) require dummy secrets — these are created by the test suite but the TempoStack reconciliation may not fully succeed without real storage backends
- The full suite can take 30-60 minutes depending on cluster performance
