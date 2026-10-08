# Managing observability deployment modes

Red Hat OpenShift AI provides centralized platform observability for metrics, traces, and logs. You configure observability on the `DataScienceClusterInitialization` (`DSCInitialization`, DSCI) custom resource at `spec.monitoring`.

This document covers **Red Hat OpenShift AI 3.6**. It describes four deployment modes and how to install them with the `rhai-on-openshift-chart` Helm chart. For dashboard usage, workload scraping, and tracing procedures already published for 3.5, see [Managing observability](https://docs.redhat.com/en/documentation/red_hat_openshift_ai_self-managed/3.5/html/managing_openshift_ai/managing-observability_managing-rhoai) (extend that chapter for 3.6; do not replace it).

---

## 1. Choose a deployment mode

| Mode | Intent | Required operators |
|------|--------|-------------------|
| **All built-in** | Full stack: metrics, traces, logs, and dashboards in OpenShift AI | Cluster Observability Operator (COO), Red Hat build of OpenTelemetry, Tempo Operator, Loki Operator, Red Hat OpenShift Logging (CLO) |
| **External only** | Forward telemetry to your existing backends; no built-in storage or dashboards | Red Hat build of OpenTelemetry; Red Hat OpenShift Logging (CLO) if you forward logs |
| **None** | No observability module | None |
| **Custom** | Mix built-in and external options per capability | See [Custom](#6-custom) |

**All built-in** — Use when you want Prometheus, Tempo, dashboards in OpenShift AI, and log aggregation in the platform.

**External only** — Use when you already have enterprise observability. OpenShift AI deploys collectors only (and optional log forwarding via CLO). There is no built-in Prometheus, Tempo, Loki, or Observe & monitor dashboards.

**None** — Use when you do not need platform observability. No observability operator prerequisites.

**Custom** — Use when you need only some capabilities (for example metrics only, or traces only). Install the operators that match those capabilities.

**OpenShift AI web application:** Observability settings do not enable the OpenShift AI console by themselves. Set `components.dashboard.dsc.managementState: Managed` so the application appears in the OpenShift application launcher. Built-in observability dashboards appear in OpenShift AI under **Observe & monitor → Dashboard** when the observability stack provides them (typically All built-in and some Custom configurations).

### Operator glossary

| Short name | Full name | Role |
|------------|-----------|------|
| COO | Cluster Observability Operator | Deploys and manages Prometheus, Alertmanager, and Perses dashboards for metrics |
| OpenTelemetry / OTel | Red Hat build of OpenTelemetry | Deploys the OpenTelemetry Collector for collecting and exporting telemetry |
| Tempo | Tempo Operator | Provides the Tempo backend for distributed tracing |
| Loki | Loki Operator | Provides LokiStack for log storage |
| CLO | Red Hat OpenShift Logging | Collects and forwards cluster logs (ClusterLogForwarder) |
| OLM | Operator Lifecycle Manager | Installs Operators from OperatorHub / catalog subscriptions |

---

## 2. Install with Helm

The chart installs the Red Hat OpenShift AI operator, dependency operators (COO, OpenTelemetry, Tempo, and Loki when enabled), and writes monitoring settings to `DSCInitialization` through `services.monitoring.*` values. Install **Red Hat OpenShift Logging (CLO)** from OperatorHub when your mode requires cluster log collection; CLO is not managed by the chart.

The chart uses the same dependency model for **services** (monitoring) as for **components**: monitoring operator dependencies are controlled under `services.monitoring.dependencies.*`, and monitoring configuration is written under `services.monitoring.dsci.*`.

### Chart location

| Source | Registry / path | Login |
|--------|-----------------|-------|
| Pre-release / early access | `oci://quay.io/rhoai/rhai-on-openshift-chart` | `helm registry login quay.io` |
| Generally available (when published) | `oci://registry.redhat.io/rhai/rhai-on-openshift-chart` | `helm registry login registry.redhat.io` |

```bash
# Replace CHART_VERSION with the chart version that matches your OpenShift AI 3.6 release.
export CHART_VERSION=<chart-version>
helm registry login quay.io
helm upgrade --install rhoai oci://quay.io/rhoai/rhai-on-openshift-chart \
  --version "${CHART_VERSION}" \
  -n rhoai-gitops --create-namespace \
  --set operator.type=rhoai \
  -f values-builtin.yaml
```

Copy a values file from [`docs/examples/`](examples/) in this repository (`values-builtin.yaml`, `values-external.yaml`, `values-none.yaml`, `values-custom-metrics.yaml`, `values-custom-traces.yaml`), or paste the matching YAML block below into a local file and pass it with `-f`. Match the chart version and OperatorHub channel to your OpenShift AI version.

### How monitoring dependencies work

| Layer | Values | Effect |
|-------|--------|--------|
| Operators | `services.monitoring.dependencies.*` | Whether the chart installs COO, OpenTelemetry, Tempo, Loki for monitoring |
| Monitoring config | `services.monitoring.dsci.*` | Writes `spec.monitoring` on `default-dsci` |

Leave top-level `dependencies.*.enabled` at **`auto`** (default) unless you intentionally force operators on or off globally.

### Two-phase install

The first `helm upgrade --install` installs Operator Lifecycle Manager (OLM) subscriptions. After the required operator ClusterServiceVersions show `Succeeded`, run the same Helm command again so `DSCInitialization` and related custom resources are applied. Avoid `helm install --wait` (it can time out). See [Automating Red Hat OpenShift AI installations with Helm and GitOps](https://developers.redhat.com/articles/2026/08/26/automating-red-hat-openshift-ai-installations-with-helm-and-gitops).

Include the OpenShift AI application in every example values file:

```yaml
components:
  dashboard:
    dsc:
      managementState: Managed
```

### Mode values files

#### All built-in

Example values file [`docs/examples/values-builtin.yaml`](examples/values-builtin.yaml):

```yaml
components:
  dashboard:
    dsc:
      managementState: Managed
services:
  monitoring:
    dependencies:
      clusterObservability: true
      opentelemetry: true
      tempo: true
      loki: true
    dsci:
      managementState: Managed
      metrics:
        storage:
          size: 5Gi
          retention: 90d
      traces:
        sampleRatio: "0.1"
        storage:
          backend: pv
          size: 5Gi
          retention: 2160h
      logs:
        storage:
          type: s3
          secretName: rhoai-logs-s3
          credentialMode: static
          storageClassName: gp3-csi  # API default; change if your cluster uses a different StorageClass
        inferenceNamespaces: []
```

Also install **CLO** from OperatorHub. Create the S3 secret in `redhat-ods-monitoring` before enabling logs. Helm writes `dsci.logs` onto DSCI; the operator copies that setting to the Monitoring CR when `logs.storage` is set. `storageClassName` defaults to `gp3-csi`; change it if your cluster uses a different StorageClass. See [Enabling built-in logs (Loki)](#enabling-built-in-logs-loki) for the secret keys, PSA labels, and verification.

#### External only

Example values file [`docs/examples/values-external.yaml`](examples/values-external.yaml):

```yaml
components:
  dashboard:
    dsc:
      managementState: Managed
services:
  monitoring:
    dependencies:
      clusterObservability: false
      opentelemetry: true
      tempo: false
      loki: false
    dsci:
      managementState: Managed
      metrics:
        exporters:
          otlp/external:
            endpoint: https://metrics.example.com:4317
            tls:
              insecure: false
      traces:
        exporters:
          otlp/external:
            endpoint: https://traces.example.com:4317
```

If you forward cluster logs to an external system, also install **CLO** from OperatorHub. Do **not** enable COO, Tempo, or Loki (`loki: false` overrides the chart default of `true`).

You can omit built-in `traces.storage` when you use exporters only. For setup only, you may add a temporary OpenTelemetry `debug` exporter to confirm traffic in collector logs. Treat that as sensitive: debug output can include request attributes and other telemetry payloads, so restrict who can read collector logs, redact anything you share, and remove the `debug` exporter as soon as verification is done. Do not leave it enabled in production.

#### None

Example values file [`docs/examples/values-none.yaml`](examples/values-none.yaml):

```yaml
components:
  dashboard:
    dsc:
      managementState: Managed
services:
  monitoring:
    dependencies:
      clusterObservability: false
      opentelemetry: false
      tempo: false
    dsci:
      managementState: Removed
```

#### Custom

**Metrics only** — example values file [`docs/examples/values-custom-metrics.yaml`](examples/values-custom-metrics.yaml):

```yaml
components:
  dashboard:
    dsc:
      managementState: Managed
services:
  monitoring:
    dependencies:
      clusterObservability: true
      opentelemetry: true
      tempo: false
      loki: false
    dsci:
      managementState: Managed
      metrics:
        storage:
          size: 10Gi
          retention: 30d
```

**Traces only** — example values file [`docs/examples/values-custom-traces.yaml`](examples/values-custom-traces.yaml):

```yaml
components:
  dashboard:
    dsc:
      managementState: Managed
services:
  monitoring:
    dependencies:
      clusterObservability: false
      opentelemetry: true
      tempo: true
      loki: false
    dsci:
      managementState: Managed
      traces:
        storage:
          backend: pv
          size: 5Gi
```

**Metrics plus remote write** — under `dsci.metrics`:

```yaml
exporters:
  prometheusremotewrite/external:
    endpoint: https://prometheus.example.com/api/v1/write
```

### Common verification commands

```bash
oc get csv -n redhat-ods-operator
oc get dsci default-dsci -o jsonpath='{.spec.monitoring.managementState}{"\n"}'
oc get pods -n redhat-ods-monitoring
oc get monitoring default-monitoring
```

---

## 3. All built-in

### Prerequisites

- You have cluster administrator privileges for your OpenShift cluster.
- You installed OpenShift AI with the All built-in example values file ([`docs/examples/values-builtin.yaml`](examples/values-builtin.yaml)). See [Install with Helm](#2-install-with-helm).
- You installed **Red Hat OpenShift Logging** from OperatorHub. For install details, see [Installing Red Hat OpenShift Logging](https://docs.redhat.com/en/documentation/red_hat_openshift_logging/6.6/html/install/index).
- The chart installed COO, OpenTelemetry, Tempo, and Loki when monitoring dependencies are enabled.

### Procedure

Equivalent `DSCInitialization` fragment:

```yaml
spec:
  monitoring:
    managementState: Managed
    namespace: redhat-ods-monitoring
    metrics:
      storage:
        size: 5Gi
        retention: 90d
    traces:
      sampleRatio: "0.1"
      storage:
        backend: pv
        size: 5Gi
        retention: 2160h
```

### Enabling built-in logs (Loki)

Metrics, traces, and `dsci.logs` can be set from Helm / `DSCInitialization`. Log forwarding still needs object storage, the Loki secret, and CLO.

1. Provide S3-compatible object storage and a bucket for Loki.
2. Create a secret in the monitoring namespace (`redhat-ods-monitoring`) with **all** of these keys (required by the Monitoring API):

   | Key | Purpose |
   |-----|---------|
   | `access_key_id` | S3 access key |
   | `access_key_secret` | S3 secret key |
   | `bucketnames` | Loki bucket name |
   | `endpoint` | S3 endpoint URL |
   | `region` | S3 region |
   | `insecure` | `true` or `false` (skip TLS verify / HTTP) |
   | `s3ForcePathStyle` | `true` or `false` |

   Example:

   ```yaml
   apiVersion: v1
   kind: Secret
   metadata:
     name: rhoai-logs-s3
     namespace: redhat-ods-monitoring
   stringData:
     access_key_id: "<id>"
     access_key_secret: "<secret>"
     bucketnames: "<bucket>"
     endpoint: "https://s3.example.com"
     region: "us-east-1"
     insecure: "false"
     s3ForcePathStyle: "false"
   ```

3. Configure logs on DSCI / Monitoring (Helm `services.monitoring.dsci.logs` or the Monitoring resource):

```yaml
spec:
  logs:
    storage:
      type: s3
      secretName: <your-s3-secret>
      credentialMode: static
      storageClassName: gp3-csi  # API default; change if your cluster uses a different StorageClass
```

4. Confirm `LokiStackAvailable` and `ClusterLogForwarderAvailable` are `True` on the Monitoring resource (`oc get monitoring default-monitoring -o yaml`).
5. Label the monitoring namespace for privileged Pod Security so OpenShift Logging collectors can use `hostPath` volumes:

```bash
oc label ns redhat-ods-monitoring \
  pod-security.kubernetes.io/enforce=privileged \
  pod-security.kubernetes.io/enforce-version=latest \
  pod-security.kubernetes.io/audit=privileged \
  pod-security.kubernetes.io/audit-version=latest \
  pod-security.kubernetes.io/warn=privileged \
  pod-security.kubernetes.io/warn-version=latest \
  security.openshift.io/scc.podSecurityLabelSync=false \
  --overwrite
```

> These labels apply to **every** Pod in `redhat-ods-monitoring`, not only the Logging collector. Limit who can create Pods in that namespace, and make sure only the Logging collector service account can use the privileged / `hostPath` SCC.

### Verification

1. In the OpenShift web console, go to **Workloads → Pods** and select the `redhat-ods-monitoring` project. Confirm pods similar to the following are Running:

```text
alertmanager-data-science-monitoringstack-#      2/2   Running
data-science-collector-collector-#               1/1   Running
prometheus-data-science-monitoringstack-#        2/2   Running
tempo-data-science-tempomonolithic-#             1/1   Running
thanos-querier-data-science-thanos-querier-#     2/2   Running
```

   Perses pods may also be present when observability dashboards are available.
2. Confirm the Monitoring resource is Ready: `oc get monitoring default-monitoring`.
3. In OpenShift AI, open **Observe & monitor → Dashboard** and confirm the **Cluster** and **Models** tabs load. Tabs may show “No data” until workloads produce metrics; that is different from a missing dashboard experience.
4. If you enabled logs, confirm LokiStack and ClusterLogForwarder conditions are available on the Monitoring resource.

---

## 4. External only

### Prerequisites

- You have cluster administrator privileges for your OpenShift cluster.
- You installed OpenShift AI with the External only example values file ([`docs/examples/values-external.yaml`](examples/values-external.yaml)). See [Install with Helm](#2-install-with-helm).
- You have **Red Hat build of OpenTelemetry** (installed by the chart).
- If you forward cluster logs externally, you installed **Red Hat OpenShift Logging** from OperatorHub. For install and forwarder configuration, see [Installing Red Hat OpenShift Logging](https://docs.redhat.com/en/documentation/red_hat_openshift_logging/6.6/html/install/index).

Do not install COO, Tempo, or Loki for this mode. If you forward cluster logs externally, use OpenShift Logging (not the OpenTelemetry Collector) and configure it to send logs to your chosen system. OpenShift AI does not create a ClusterLogForwarder for this mode from the exporters-only monitoring configuration.

### Procedure

Forward metrics and traces with OpenTelemetry exporters (example):

```yaml
spec:
  monitoring:
    managementState: Managed
    namespace: redhat-ods-monitoring
    metrics:
      exporters:
        otlp/external:
          endpoint: https://metrics.example.com:4317
          tls:
            insecure: false
    traces:
      exporters:
        otlp/external:
          endpoint: https://traces.example.com:4317
```

### Verification

1. Confirm OpenTelemetry Collector pods are Running in `redhat-ods-monitoring`, for example:

```text
data-science-collector-collector-#               1/1   Running
```

2. Confirm there is no built-in MonitoringStack, Tempo, or Perses stack for platform storage/dashboards.
3. In OpenShift AI, confirm **Observe & monitor** is not available (no built-in observability dashboards).
4. Confirm metrics and traces reach your external backend. If you use a temporary `debug` exporter for setup, treat collector logs as sensitive (they can include telemetry payloads), restrict log access, redact anything you share, and remove the exporter immediately after testing.

---

## 5. None

### Prerequisites

- You have cluster administrator privileges for your OpenShift cluster.
- You installed OpenShift AI with the None example values file ([`docs/examples/values-none.yaml`](examples/values-none.yaml)). See [Install with Helm](#2-install-with-helm).
- No observability operator prerequisites.

### Procedure

```yaml
spec:
  monitoring:
    managementState: Removed
```

### Trade-offs

OpenShift AI does not deploy platform metrics, traces, or observability dashboards. Individual components may still expose their own metrics. To enable observability later, switch to All built-in or Custom.

### Verification

1. Confirm `spec.monitoring.managementState` is `Removed` on `default-dsci`.
2. Confirm monitoring operands are not running in `redhat-ods-monitoring`.
3. In OpenShift AI, confirm **Observe & monitor** is not in the navigation. The OpenShift AI application can remain available when the dashboard component is Managed.

---

## 6. Custom

### Prerequisites

- You have cluster administrator privileges for your OpenShift cluster.
- You installed OpenShift AI with a Custom example values file from [Install with Helm](#2-install-with-helm).
- You installed the operators required for the capabilities you enable (see the capability matrix).

### Procedure

Use the Custom example values files in [Install with Helm](#2-install-with-helm).

### Capability matrix

| Capability | Built-in option | Required operators |
|------------|-----------------|-------------------|
| **Metrics** | MonitoringStack and OpenTelemetry Collector | COO, OpenTelemetry |
| **Tracing** | Tempo and OpenTelemetry Collector | OpenTelemetry, Tempo |
| **Logs** | Loki-based forwarding | CLO, Loki |
| **Metrics dashboards** | Cluster, Models, Usage, and LLM dashboards in OpenShift AI (Perses via COO) | COO, OpenTelemetry |
| **Trace views** | Tempo Perses dashboards | OpenTelemetry, Tempo |

### Examples

**Metrics with remote write:**

```yaml
spec:
  monitoring:
    managementState: Managed
    metrics:
      storage:
        size: 10Gi
        retention: 30d
      exporters:
        prometheusremotewrite/external:
          endpoint: https://prometheus.example.com/api/v1/write
```

**Traces only (persistent volume):**

```yaml
spec:
  monitoring:
    managementState: Managed
    traces:
      storage:
        backend: pv
        size: 5Gi
```

### Verification

**Metrics only**

1. In `redhat-ods-monitoring`, confirm pods similar to the following are Running, and that Tempo is not deployed:

```text
alertmanager-data-science-monitoringstack-#      2/2   Running
data-science-collector-collector-#               1/1   Running
prometheus-data-science-monitoringstack-#        2/2   Running
thanos-querier-data-science-thanos-querier-#     2/2   Running
```

2. In OpenShift AI, open **Observe & monitor → Dashboard** and confirm Cluster / Models tabs are available.

**Traces only**

1. In `redhat-ods-monitoring`, confirm pods similar to the following are Running, and that MonitoringStack / Prometheus is not deployed:

```text
data-science-collector-collector-#               1/1   Running
tempo-data-science-tempomonolithic-#             1/1   Running
```

2. Perses may still be present for Tempo-related views. **Observe & monitor** can remain visible; Cluster and Models panels that require Prometheus show datasource errors until metrics are enabled. Enable metrics (or use All built-in) if you need those dashboards.

---

## 7. Operator reference

| Operator | Package (OperatorHub) | Typical namespace | Helm chart value |
|----------|----------------------|-------------------|------------------|
| Cluster Observability Operator | `cluster-observability-operator` | `openshift-cluster-observability-operator` | `services.monitoring.dependencies.clusterObservability` |
| Red Hat build of OpenTelemetry | `opentelemetry-product` | `openshift-opentelemetry-operator` | `services.monitoring.dependencies.opentelemetry` |
| Tempo Operator | `tempo-product` | `openshift-tempo-operator` | `services.monitoring.dependencies.tempo` |
| Loki Operator | `loki-operator` | `openshift-operators-redhat` | `services.monitoring.dependencies.loki` |
| Red Hat OpenShift Logging | `cluster-logging` | `openshift-logging` | OperatorHub (not in chart) |

Example subscription for COO (adapt channel for your OpenShift version; use the same pattern for OpenTelemetry and Tempo when installing from OperatorHub):

```yaml
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: cluster-observability-operator
  namespace: openshift-cluster-observability-operator
spec:
  channel: stable
  name: cluster-observability-operator
  source: redhat-operators
  sourceNamespace: openshift-marketplace
  installPlanApproval: Automatic
```

Dashboards use Perses custom resources managed through COO. There is no separate Perses Operator prerequisite.

---

## 8. Release notes — Observability dashboards general availability

In Red Hat OpenShift AI **3.6**, observability dashboards that were Technology Preview in earlier releases are **generally available**. Documentation also describes four observability deployment modes and Helm-based configuration.

This table is the **complete** set of built-in **Observe & monitor** dashboards for OpenShift AI 3.6 (same six dashboards documented in [Managing observability](https://docs.redhat.com/en/documentation/red_hat_openshift_ai_self-managed/3.5/html/managing_openshift_ai/managing-observability_managing-rhoai)).

| Dashboard | Access |
|-----------|--------|
| Cluster | Cluster administrators |
| Models | All users (project-scoped) |
| Usage | Models-as-a-Service usage metrics |
| LLM Traffic | Distributed Inference with llm-d |
| LLM Performance | Distributed Inference with llm-d |
| LLM Utilization | Distributed Inference with llm-d |

**What's new**

- Observability dashboards generally available
- Four deployment modes with operator prerequisites
- Per-mode Helm values (`services.monitoring`) and verification guidance
- Built-in log forwarding to Loki via the Monitoring resource (object storage and OpenShift Logging required)

---

## Additional resources

- [Managing observability (OpenShift AI 3.5 product docs — base chapter to extend for 3.6)](https://docs.redhat.com/en/documentation/red_hat_openshift_ai_self-managed/3.5/html/managing_openshift_ai/managing-observability_managing-rhoai)
- [Installing Red Hat OpenShift Logging](https://docs.redhat.com/en/documentation/red_hat_openshift_logging/6.6/html/install/index)
- [Automating Red Hat OpenShift AI installations with Helm and GitOps](https://developers.redhat.com/articles/2026/08/26/automating-red-hat-openshift-ai-installations-with-helm-and-gitops)
