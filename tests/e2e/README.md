# Monitoring E2E Tests

`TestMonitoring` exercises the odh-observability operator against a live OpenShift cluster. It changes monitoring configuration, waits for operands, tests webhook behavior, and restores the original Monitoring or DSCI monitoring spec when it finishes. Run it on a test cluster because the suite temporarily disables and recreates monitoring operands.

`TestLLMInferenceService` is a separate RHOAI integration suite. The monitoring commands below select `TestMonitoring` so they do not provision inference resources.

## Prerequisites

- A reachable OpenShift cluster with `KUBECONFIG` set and permissions to manage the tested resources.
- The odh-observability operator deployed and running, with its Monitoring CRD installed.
- A default StorageClass for the MonitoringStack, Tempo, Perses, and Loki operands. Both suites
  detect it and fail immediately if there is none; the inference suite also substitutes it into
  `prerequisites/inference/*.yaml`, which carry `gp3-csi` only as a placeholder.
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
go test ./tests/e2e/ -v -timeout 120m -count=1 -run 'TestMonitoring/Metrics_ready/Thanos_Querier' -api-mode=dsc -install-operators=false
```

The selected phase performs its own setup and cleanup. To run an off/on case in isolation, select `TestMonitoring/Lifecycle/Group_13:_Lifecycle/Target_Allocator`. The full monitoring suite takes roughly 30–60 minutes depending on cluster performance. Storage and OLM installation can take longer; adjust the timeout flags below if needed.

## Configuration

Pass test flags through `E2E_TEST_FLAGS` for local Make targets. The container runner maps the corresponding `E2E_TEST_*` environment variables to these flags.

| Flag | Default | Description |
|------|---------|-------------|
| `-api-mode` | `module` | `module` updates the Monitoring CR; `dsc` updates the existing DSCI |
| `-dsci-cr-name` | `default-dsci` | DSCI name in DSC mode |
| `-monitoring-namespace` | auto-detected | Namespace from the operator when creating a Monitoring CR, or from an existing Monitoring CR. The inference test cannot auto-detect (nothing is installed yet) and uses `redhat-ods-monitoring` |
| `-applications-namespace` | `redhat-ods-applications` | Inference test only: DSCI `spec.applicationsNamespace` |
| `-monitoring-cr-name` | `default-monitoring` | Monitoring CR name |
| `-install-operators` | `true` | Install dependent OLM operators when needed |
| `-apply-monitoring-manifest` | `true` | Inference test only: overwrite the DSCI-created Monitoring CR spec from `prerequisites/inference/monitoring.yaml`. Set `false` to test DSCI → Monitoring propagation instead |
| `-cleanup-test-state` | `true` | Inference test only: after the suite, delete the SeaweedFS fixtures and the Loki S3 Secret and restore MaaS `spec.usageLogging`. See [What the inference suite leaves behind](#what-the-inference-suite-leaves-behind) |
| `-olm-timeout` | `5m` | Timeout for OLM installation |
| `-eventually-timeout` | `5m` | Default wait timeout; specific slow resources use longer waits |
| `-eventually-poll-interval` | `2s` | Default wait polling interval |
| `-consistently-timeout` | `30s` | Default consistency check duration |
| `-consistently-poll-interval` | `2s` | Default consistency polling interval |

### Inference test namespaces

`prerequisites/inference/*.yaml` are written for the RHOAI layout, and the two namespace flags
rewrite those literals wherever they appear — including inside the exporter endpoints
(`http://lgtm.<monitoring-namespace>.svc.cluster.local:4317`). For the ODH layout, which is what
all three CRDs default to:

```sh
go test ./tests/e2e/ -run '^TestLLMInferenceService$' \
    -monitoring-namespace=opendatahub -applications-namespace=opendatahub
```

Pick once per cluster. DSCI `applicationsNamespace`, DSCI `monitoring.namespace`, and Monitoring
`spec.namespace` are all immutable (`self == oldSelf`), so running with different values against a
cluster that already has these CRs is rejected. Delete the DSCI, DSC, and Monitoring CRs first —
the suite checks for the mismatch and says so rather than letting the CEL error surface.

The namespace must also be one the opendatahub-operator knew about when its pod started: it builds
a namespace-scoped cache at startup, and a namespace created afterwards fails every module deploy
with `unknown namespace for the cache`. Restart the operator after creating a new one.

### What the inference suite leaves behind

The suite splits its cluster changes by who declares them.

Reverted when the suite finishes, unless `-cleanup-test-state=false`
(`E2E_TEST_CLEANUP_STATE=false`):

- The SeaweedFS pod, its bucket-creator pod, and its Service.
- The `test-loki-s3-secret` Secret. The `test-` prefix matters: the suite overwrites this Secret
  and then deletes it, so it must not be a name a real S3 backend would plausibly use.
- MaaS `Config/default.spec.usageLogging`, restored to whatever it was before the run. Nothing in
  this repo declares that field, so leaving it on would enable MaaS usage logging for whatever
  runs next.

Left in place, by design:

- Everything in `prerequisites/inference/` — the DSCI, DSC, UIPlugin, Kuadrant, LWSOperator and
  LGTM. Applying them is idempotent; reverting them would mean rebuilding the platform every run.
- The Monitoring CR spec and its operands, including the LokiStack and its PVCs. The suite writes
  that spec from a manifest, standing in for the DSCI propagation that does not exist yet, so it
  is treated as declared configuration rather than a mutation.
- Operators installed through OLM.

After teardown the LokiStack points at an S3 endpoint that no longer exists. That is expected: the
next run's SeaweedFS comes back at the same Service DNS name with the same Secret, and the stack
recovers. Pass `-cleanup-test-state=false` to keep the backend alive — useful for reading the
usage logs a failed run actually produced, since SeaweedFS stores them in an `emptyDir` that the
teardown destroys along with the pod.

This flag is independent of `NO_CLEANUP=1`, and deliberately so — the two cover different things
and the useful combinations need both knobs:

| | `LLMInferenceService` + OAuth proxy | SeaweedFS, `test-loki-s3-secret`, MaaS `usageLogging` |
|---|---|---|
| (neither) | deleted | reverted |
| `NO_CLEANUP=1` | kept | reverted |
| `-cleanup-test-state=false` | deleted | kept |
| both | kept | kept |

`NO_CLEANUP=1` on its own still destroys the usage logs the run produced, because the SeaweedFS
`emptyDir` goes with the pod. To inspect them, you want `-cleanup-test-state=false` — with or
without `NO_CLEANUP=1`, since the model server is not needed to read what it already wrote.

`NO_CLEANUP=1` also skips a test step, not just teardown: deleting the `LLMInferenceService` and
asserting its `PodMonitor` is garbage-collected within one reconciliation cycle is the last phase
of each topology subtest, and the flag returns before it. A run with `NO_CLEANUP=1` covers less
than a clean one. `-cleanup-test-state` changes no assertions.

## Test groups

The phases run in order. The no-metrics phase checks defaults and absent components. The metrics phase starts Prometheus once, runs the steady-state integrations, then removes the operands. Transition checks that disable or recreate metrics run later in Lifecycle. A phase waits for old Prometheus pods to leave before enabling metrics and for the new StatefulSet and pods to become ready before dependent checks; each wait allows up to 12 minutes for a slow termination or rollout.

| Phase | Coverage |
|-------|----------|
| Baseline without metrics | Monitoring defaults, negative conditions, absent Target Allocator, Thanos, and Perses |
| Metrics ready | MonitoringStack, Korrel8r, Collector, Target Allocator, Thanos route and Prometheus networking, RBAC |
| Traces with PV Backend | TempoMonolithic and trace integration |
| Traces with Cloud Storage | TempoStack on S3 and GCS, Perses TLS |
| Perses | Dashboards, datasources, and Thanos ingress connectivity using a ready Perses pod |
| Webhooks | Admission behavior and label injection |
| Usage Logs Collection | LokiStack and collector configuration and lifecycle |
| Lifecycle | Metrics replicas, Target Allocator, Perses, datasource, Prometheus rules, and disabled webhook transitions |
| Disabled | Operand removal when monitoring is disabled |

The S3 cases deploy SeaweedFS in the monitoring namespace and create a local bucket for Tempo or Loki. The GCS case deploys fake-gcs-server and points TempoStack at its local endpoint. Fixture pods, services, and secrets are removed after their groups. The suite restores the pre-test Monitoring spec in module mode or `DSCI.spec.monitoring` in DSC mode. If it created a Monitoring CR in module mode, it deletes that CR at the end. Operators installed through OLM remain installed.
