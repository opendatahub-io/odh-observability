# Monitoring E2E Tests

`TestMonitoring` exercises the odh-observability operator against a live OpenShift cluster. It changes monitoring configuration, waits for operands, tests webhook behavior, and restores the original Monitoring or DSCI monitoring spec when it finishes. Run it on a test cluster because the suite temporarily disables and recreates monitoring operands.

`TestLLMInferenceService` is a separate RHOAI integration suite. The monitoring commands below select `TestMonitoring` so they do not provision inference resources.

`TestCOOVersionCompatibility` is the RHOAI 3.6 / COO 1.5 release gate. It validates fixed operator CSV lines (`cluster-observability-operator.v1.5`, `rhods-operator.3.6`), the RHOAI-pinned Perses image, Perses operand health, and mandatory dashboard paths after metrics are enabled: Prometheus dashboard Route host, namespace-proxy PromQL (`up` with `namespace=`), Perses health and projects APIs, and the Dashboards UIPlugin when installed. COO and RHOAI CSV prefixes and dashboard probes cannot be overridden through flags or `E2E_TEST_*` environment variables. It restores monitoring configuration like `TestMonitoring` but does not run the full regression matrix.

`TestOLMUpgradeSurvivability` exercises in-place OLM upgrades for Cluster Observability Operator and Tempo Operator without rewriting the existing Monitoring or DSCI configuration. The cluster must already have a healthy metrics stack and the requested operators installed, so run with `-install-operators=false`. When Tempo is selected, traces must also be configured: the suite verifies Tempo readiness and performs an OTLP ingest/query round trip before and after the upgrade. The inseparable workflow validates the baseline, patches subscription channels, approves only a Manual InstallPlan resolved for the target operator CSV, waits for the Subscription and CSV to converge on that plan, rejects stale Replacing/Deleting CSVs and blocking operand finalizers, then reruns the Perses and Prometheus dashboard checks. Opt in with `-upgrade-enabled=true` (`make e2e-test-olm-upgrade-survivability` sets this); the make target uses `-count=1`.

```bash
make e2e-test-coo-compatibility
```

```bash
E2E_TEST_API_MODE=dsc E2E_TEST_INSTALL_OPERATORS=false \
  E2E_TEST_UPGRADE_COO_CHANNEL=<target> E2E_TEST_UPGRADE_TEMPO_CHANNEL=<target> \
  make e2e-test-olm-upgrade-survivability
```

DSC mode on a cluster with RHOAI already installed:

```bash
make e2e-test-coo-compatibility E2E_TEST_FLAGS="-api-mode=dsc -install-operators=false"
```

Optional COO catalog channel and compatibility report path:

```bash
go test ./tests/e2e/ -v -timeout 90m -count=1 -run '^TestCOOVersionCompatibility$' \
  -coo-channel=stable \
  -compatibility-report=coo-compatibility.json
```

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
| `-coo-channel` | `stable` | OLM channel for Cluster Observability Operator when `-install-operators=true` |
| `-compatibility-report` | _(empty)_ | Optional JSON diagnostics filename or relative path under `ARTIFACTS` / `E2E_ARTIFACTS` / `e2e-artifacts` (report is always logged when the test fails) |
| `-upgrade-enabled` | `false` | Enable `TestOLMUpgradeSurvivability` (required for the upgrade survivability make target) |
| `-upgrade-operators` | `coo,tempo` | Comma-separated operators to upgrade (`coo`, `tempo`) |
| `-upgrade-coo-channel` | _(empty)_ | Target OLM channel for COO; empty skips the COO upgrade step |
| `-upgrade-tempo-channel` | _(empty)_ | Target OLM channel for Tempo Operator; empty skips the Tempo upgrade step |
| `-upgrade-report` | _(empty)_ | Optional JSON report path under `ARTIFACTS` / `E2E_ARTIFACTS` / `e2e-artifacts` (always logged when the test finishes) |

`TestCOOVersionCompatibility` always requires COO CSV prefix `cluster-observability-operator.v1.5`, RHOAI CSV prefix `rhods-operator.3.6`, the RHOAI-pinned Perses image digest, and Perses plus Prometheus dashboard proxy checks. Those requirements are not configurable.

### Bumping the release gate (e.g. RHOAI 3.7 / COO 1.6)

Edit the `releaseGateContract` struct in `tests/e2e/config_test.go` (three fields only). In the same PR: update `TestReleaseGateCOOCSVVersions` / `TestReleaseGateRHOAICSVVersions` expected pass/fail examples if the minor version changes, align `PersesImage` with the operator’s pinned Perses default when that moves, run `make e2e-test-coo-compatibility` on a clean target cluster, and link the PR to the release milestone. No flags or `E2E_TEST_*` overrides—reviewers see the version bump in one diff hunk.

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
