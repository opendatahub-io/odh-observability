# Monitoring E2E Tests

`TestMonitoring` exercises the odh-observability operator against a live OpenShift cluster. It changes monitoring configuration, waits for operands, tests webhook behavior, and restores the original Monitoring or DSCI monitoring spec when it finishes. Run it on a test cluster because the suite temporarily disables and recreates monitoring operands.

`TestLLMInferenceService` is a separate RHOAI integration suite. The monitoring commands below select `TestMonitoring` so they do not provision inference resources.

## Prerequisites

- A reachable OpenShift cluster with `KUBECONFIG` set and permissions to manage the tested resources.
- The odh-observability operator deployed and running, with its Monitoring CRD installed.
- A default StorageClass for the MonitoringStack, Tempo, Perses, and Loki operands.
- Cluster Observability Operator, Tempo Operator, OpenTelemetry Operator, and Loki Operator. With `-install-operators=true`, the suite installs missing dependencies through OLM from the `redhat-operators` catalog; Loki uses the catalog's default channel. With `-install-operators=false`, the suite checks that the required CRDs are present.
- Access to the pinned SeaweedFS, fake GCS, and curl images used by the cloud storage fixtures.
- For `-api-mode=dsc`, a real DSCI and platform operator installation. The suite updates the existing DSCI's `spec.monitoring`; it does not create a DSCI.

## Run only monitoring tests

Standalone module mode:

```bash
make e2e-test-monitoring
```

DSC mode on a cluster with the platform operator and dependent operators already installed:

```bash
make e2e-test-monitoring E2E_TEST_FLAGS="-api-mode=dsc -install-operators=false"
```

For containerized runs, use `make e2e-test-container` or `make e2e-test-container-dsc`. Both targets select `TestMonitoring` and write JUnit and JSON results under `E2E_ARTIFACTS` (default `./e2e-artifacts`). `make e2e-test` retains its package-wide behavior and also runs the inference test.

To focus on a monitoring group while debugging, use Go's subtest selector:

```bash
go test ./tests/e2e/ -v -timeout 120m -count=1 -run 'TestMonitoring/Traces_with_Cloud_Storage' -api-mode=dsc -install-operators=false
```

The full monitoring suite takes roughly 30–60 minutes depending on cluster performance. Storage and OLM installation can take longer; adjust the timeout flags below if needed.

## Configuration

Pass test flags through `E2E_TEST_FLAGS` for local Make targets. The container runner maps the corresponding `E2E_TEST_*` environment variables to these flags.

| Flag | Default | Description |
|------|---------|-------------|
| `-api-mode` | `module` | `module` updates the Monitoring CR; `dsc` updates the existing DSCI |
| `-dsci-cr-name` | `default-dsci` | DSCI name in DSC mode |
| `-monitoring-namespace` | auto-detected | Namespace from the operator when creating a Monitoring CR, or from an existing Monitoring CR |
| `-monitoring-cr-name` | `default-monitoring` | Monitoring CR name |
| `-install-operators` | `true` | Install dependent OLM operators when needed |
| `-olm-timeout` | `5m` | Timeout for OLM installation |
| `-eventually-timeout` | `5m` | Default wait timeout; specific slow resources use longer waits |
| `-eventually-poll-interval` | `2s` | Default wait polling interval |
| `-consistently-timeout` | `30s` | Default consistency check duration |
| `-consistently-poll-interval` | `2s` | Default consistency polling interval |

## Test groups

The 14 top-level groups run in order. Subtests share cluster state within a group, so a focused subtest may still need its group's setup.

| Group | Coverage |
|-------|----------|
| 1. Base Configuration | Monitoring defaults |
| 2. Metrics & MonitoringStack | Metrics, rules, replicas, reconciliation stability |
| 3. Korrel8r | TLS query service and network policy |
| 4. OpenTelemetry Collector | Collector configuration and TLS |
| 5. Target Allocator | Deployment, lifecycle, and RBAC |
| 6. Thanos Querier | Deployment and networking |
| 7. Traces with PV Backend | TempoMonolithic and trace integration |
| 8. Traces with Cloud Storage | TempoStack on S3 and GCS, Perses TLS |
| 9. Perses | Dashboards, datasources, and lifecycle |
| 10. Networking and RBAC | Proxy, node metrics, and access rules |
| 11. Webhooks | Admission behavior and label injection |
| 12. Usage Logs Collection | LokiStack and collector configuration and lifecycle |
| 13. Negative Conditions | Status when features are not configured |
| 14. Disabled | Operand removal when monitoring is disabled |

The S3 cases deploy SeaweedFS in the monitoring namespace and create a local bucket for Tempo or Loki. The GCS case deploys fake-gcs-server and points TempoStack at its local endpoint. Fixture pods, services, and secrets are removed after their groups. The suite restores the pre-test Monitoring spec in module mode or `DSCI.spec.monitoring` in DSC mode. If it created a Monitoring CR in module mode, it deletes that CR at the end. Operators installed through OLM remain installed.
