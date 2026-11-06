---
name: observability-endpoint-discovery
description: Discovers the live query, dashboard, correlation, and telemetry endpoints exposed by the RHOAI observability stack on an OpenShift cluster. Use when another skill needs Prometheus-compatible metrics, Loki logs, Tempo traces, Perses, Korrel8r, or collector endpoint details before gathering observability, troubleshooting, or usage information.
license: Apache-2.0
compatibility: Requires an OpenShift kubeconfig with read access to the Monitoring, DSCI, Route, Service, EndpointSlice, and observability custom resources; jq is recommended for structured inspection.
metadata:
  issue: RHAI-15148
  version: "1.0"
---

# Observability endpoint discovery

Discover the endpoints that are actually configured and resource-ready in the
current RHOAI cluster. This skill is a discovery step for downstream skills; it
does not perform broad signal queries, change cluster resources, or expose
credentials.

For a deterministic inventory, run the read-only helper and validate its JSON
handoff:

```bash
set -o pipefail
python3 scripts/discover_endpoints.py --pretty \
  | tee observability-endpoints.json \
  | python3 scripts/validate_inventory.py
```

The helper may return partial results when optional APIs or permissions are
missing. Do not treat a non-empty endpoint list as proof that the current caller
can reach every endpoint; consume the readiness state and warnings.

## Rules

- Use the current `oc` context. Never assume `opendatahub`,
  `redhat-ods-monitoring`, or any other namespace.
- Never invent a Route host, Service, port, or endpoint from a product default.
  Confirm the resource exists and preserve an endpoint as `unknown` or omit it
  when its host, port, or ready EndpointSlice cannot be confirmed.
- Treat `spec.metrics.storage`, `spec.traces.storage`, `spec.logs.storage`, and
  `spec.usageLogs.storage` as separate capabilities. Exporters-only configuration
  forwards telemetry but does not create a local query backend.
- Prefer an authorized proxy or published status endpoint over a raw backend.
  Never send a user query directly to the RHOAI Thanos Querier on `:10902` or
  the Prometheus server on `:9090`; those are internal implementation paths.
- Preserve the caller's bearer token when an endpoint requires caller
  authorization. Do not request, print, decode, or store service-account tokens.
- Do not read Secrets, object-storage credentials, or exporter credentials. A
  configured exporter URL is a sink, not evidence of a query endpoint.
- Reject endpoint URLs containing credentials or sensitive query parameters such
  as `password`, `api_key`, `secret`, or `token`; never write those values to the
  inventory or diagnostic output.
- Use HTTPS and the cluster/service CA for TLS endpoints. Do not use `curl -k`
  as a normal discovery or verification method.
- Do not start a long-running port-forward automatically. Report the
  port-forward command as a fallback when the caller is outside the cluster.
- Return partial results when an optional component is absent. Explain whether
  the component is disabled, not ready, not installed, or not discoverable with
  the caller's RBAC.

## Workflow

### 1. Verify cluster context and read access

Resolve the operand namespace before checking namespaced permissions. The
Monitoring resource is cluster-scoped, but Routes, Services, EndpointSlices,
and NetworkPolicies are namespaced:

```bash
oc whoami --show-server
oc whoami
oc auth can-i get monitorings.services.platform.opendatahub.io
MONITORING_NAMESPACE=$(oc get monitoring default-monitoring \
  -o jsonpath='{.spec.namespace}')
test -n "$MONITORING_NAMESPACE" || {
  echo "Monitoring.spec.namespace is empty" >&2
  exit 1
}
oc auth can-i get routes.route.openshift.io -n "$MONITORING_NAMESPACE"
oc auth can-i get services -n "$MONITORING_NAMESPACE"
oc auth can-i get endpointslices.discovery.k8s.io -n "$MONITORING_NAMESPACE"
oc auth can-i get networkpolicies.networking.k8s.io -n "$MONITORING_NAMESPACE"
```

Do not use `oc whoami -t` in discovery output. If there is no active cluster,
the context is wrong, or the required reads are denied, stop and return a
discovery error with the missing prerequisite instead of guessing endpoints.

### 2. Resolve the active Monitoring resource and operand namespace

The `Monitoring` resource is cluster-scoped and normally named
`default-monitoring`. Fetch it first:

```bash
oc get monitoring default-monitoring -o json
```

Read these fields:

- `.spec.managementState` — only `Managed` provisions the stack.
- `.spec.namespace` — the operand namespace; it is immutable but may be
  `opendatahub`, `redhat-ods-monitoring`, or another valid namespace.
- `.spec.metrics`, `.spec.traces`, `.spec.logs`, and `.spec.usageLogs` — desired
  capability gates.
- `.status.url` — corroborating operator-published metadata for the Thanos query
  Route; it may be empty while the Route is not admitted yet and can remain
  stale, so current Route admission is authoritative.
- `.status.logsEndpoint` — the ready log-forwarding Loki query base URL.
- `.status.usageLogsEndpoint` — an OTLP **write** endpoint, never a log query
  endpoint.
- `.status.conditions[]` — readiness and degradation evidence.

If the RHOAI installation is DSCI-managed, cross-check the selected namespace:

```bash
oc get dsci -o json
```

Select exactly one active DSCI. Prefer `default-dsci` when it is the only
non-Removed DSCI; otherwise fail closed when multiple non-Removed DSCI
resources exist. Use its `.spec.monitoring.namespace` and compare it with
`Monitoring.spec.namespace`. Report a mismatch; do not silently choose the
first item returned by the API.
If no DSCI exists, use the standalone `Monitoring` resource. If monitoring is
`Removed`, report that no RHOAI-managed query endpoints are expected.

Capture relevant conditions without discarding their reason or message:

```bash
oc get monitoring default-monitoring -o json \
  | jq '{name: .metadata.name, spec: .spec, status: {
      url: .status.url,
      logsEndpoint: .status.logsEndpoint,
      usageLogsEndpoint: .status.usageLogsEndpoint,
      conditions: (.conditions // .status.conditions // [])
    }}'
```

The exact condition location depends on the PlatformObject status shape. Use the
actual field present in the object; do not report a component ready merely
because its spec field exists.

### 3. Discover Routes, Services, and ready endpoints

For the operand namespace, inspect Routes and Services rather than constructing
hosts blindly:

```bash
oc -n "$MONITORING_NAMESPACE" get routes.route.openshift.io -o json
oc -n "$MONITORING_NAMESPACE" get services -o json
oc -n "$MONITORING_NAMESPACE" get endpointslices.discovery.k8s.io -o json
```

For each candidate Service, confirm at least one ready EndpointSlice address and
confirm that the EndpointSlice port matches the requested Service port and
targetPort. An endpoint with no explicit `ready` condition is normally treated
as ready by the EndpointSlice API; terminating or explicitly non-ready endpoints
must not be used.
Record both the Service reference (`service/name:port`) and its cluster DNS name
when it is useful to an in-cluster caller:

```text
https://SERVICE.NAMESPACE.svc.cluster.local:PORT
```

For a ready Route, require an admitted ingress entry with an `Admitted=True`
condition. Use that entry's `.status.ingress[].host`; `.spec.host` is only a
configured/pending hint and must not be reported as a ready URL. The normal URL
is `https://HOST`; retain the Route name, target Service, TLS termination, and
admission state in the inventory.

Inspect NetworkPolicies in the operand namespace. A ready Service or admitted
Route proves resource readiness, not reachability from the downstream caller's
namespace. Record the relevant policy names and classify caller reachability as
unknown unless the policy selectors and ports have been evaluated.

A Service existing without a ready EndpointSlice is `configured/not-ready`, not
`available`. A Route existing without an admitted host is `configured/pending`.

### 4. Discover metrics endpoints

Only create RHOAI Data Science query records when built-in metrics storage is
configured and the corresponding conditions/resources support it.

#### Data Science metrics

The preferred user-query lane is the authorized Thanos proxy:

- Route: `data-science-thanos-querier-route`.
- Service: `data-science-thanos-querier-proxy`, HTTPS port `8443`.
- Query API: Prometheus-compatible `/api/v1/*` paths such as `query`,
  `query_range`, `series`, `labels`, and `label/<name>/values`.
- Required URL parameter: `namespace=<workload-namespace>` on every query.
- Authorization: caller bearer token; the proxy performs a namespace SAR and
  constrains the PromQL namespace label. The caller must be authorized for the
  selected namespace.
- TLS: Route re-encryption or the OpenShift service CA for the Service.

The operator also has a namespace-enforcing Prometheus proxy compatibility lane:

- Route: `data-science-prometheus-route`.
- Service: `data-science-prometheus-namespace-proxy`, HTTPS port `8443`.
- Alternate Service: `data-science-prometheus-namespace-proxy-service`, port
  `9090` when present; its target is still the secure proxy.
- It is GET/HEAD-only and requires `namespace` in the URL. Treat it as an
  alternate, not as a reason to bypass the Thanos proxy.

Discover the actual Route admission and Service readiness. Use `.status.url`
only as corroborating status metadata: the controller can preserve an older
value while a current Route exists without an admitted host. The current Route
must be the source of truth for a ready URL, and the inventory must retain the
Route and Service evidence.

For the Thanos proxy, `namespace` must be URL query data even when using a
form-encoded POST. Never put `namespace` in the POST body. The Thanos proxy
accepts form POSTs only for its read-only query/label paths; the Prometheus
namespace proxy is GET/HEAD-only.

#### OpenShift platform metrics lanes

The Perses integration may expose two caller-token lanes through the COO
`thanos-querier` Service. Discover the Service across namespaces when RBAC
permits and retain its observed namespace and actual ports before reporting it.
Require an available OpenShift platform Prometheus resource and the
cluster-monitoring-operator-managed Service identity labels before accepting
the Service as the platform Thanos endpoint. If either source cannot be
verified, fail closed and omit the platform endpoint:

- `:9091` — cluster-wide OpenShift Monitoring metrics; caller RBAC applies.
- `:9092` — namespace-tenanted OpenShift Monitoring metrics; `namespace=` is
  required and caller authorization applies.

These are not Data Science Thanos endpoints. Label each record as
`openshift-platform` and do not combine a PromQL query across the Data Science
`:8443` lane and either OpenShift lane. A missing `thanos-querier` Service or
denied cross-namespace read is an unavailable optional lane, not a failure of
the RHOAI stack.

The Service `data-science-prometheus-cluster-proxy` and its Route are restricted
to the `/metrics` endpoint for node metrics. Record it, if needed for a
diagnostic skill, as `diagnostic/metrics-scrape-only`; it is not a PromQL query
endpoint.

### 5. Discover Loki log endpoints

RHOAI log forwarding and usage logs are separate LokiStack instances.

#### Forwarded application logs — query lane

When `.status.logsEndpoint` is non-empty and the `LokiStackAvailable` condition
is `True`, record it as the base query endpoint for the `application` tenant.
Normally it points to the `data-science-lokistack` gateway in the monitoring
namespace. Confirm the gateway Service and ready EndpointSlice using the
published URL and/or the Service labels; do not read its storage Secret.

The Loki HTTP API is below that base URL, for example:

```text
<logsEndpoint>/loki/api/v1/query_range
<logsEndpoint>/loki/api/v1/query
<logsEndpoint>/loki/api/v1/labels
```

Use the caller token and tenant `application` (normally the
`X-Scope-OrgID: application` header) as required by the gateway. Keep the base
URL exactly as published; do not strip or duplicate the
`/api/logs/v1/application` prefix.

If `LokiStackAvailable` is false, report the condition and do not use a stale
`.status.logsEndpoint` value as an available query endpoint. If only
`ClusterLogForwarderAvailable` is false, retain a ready Loki gateway endpoint
but mark historical data as incomplete or unavailable; the gateway and the
forwarding pipeline have independent readiness. Korrel8r's `direct: true`
fallback can access current pod logs through the Kubernetes API, but that is not
equivalent to historical Loki storage.

#### Usage logs — ingestion lane only

`.status.usageLogsEndpoint` belongs to the
`data-science-usage-logs-lokistack` OTLP receiver and is for writing usage logs.
It is not a query endpoint. Report it only under `telemetry_ingest` with the
OTLP HTTP path and never pass it to a downstream log-query skill. If usage-log
queries are needed, discover an explicitly configured Perses/usage-tenancy
proxy; do not infer one from the OTLP URL.

### 6. Discover Tempo trace endpoints

Tracing has a local query backend only when `.spec.traces.storage` is present.
Exporter-only traces have no RHOAI-managed query endpoint.

Determine which CR exists and is ready:

```bash
oc -n "$MONITORING_NAMESPACE" get tempomonolithic data-science-tempomonolithic -o json
oc -n "$MONITORING_NAMESPACE" get tempostack data-science-tempostack -o json
```

Require the matching Tempo resource to report `status.conditions[type=Ready,
status=True]`. A `TempoAvailable` Monitoring condition alone is insufficient;
if the matching CR cannot be read, report that readiness could not be verified
and do not report its gateway as ready.

Then discover the ready gateway Service. The operator-managed candidates are:

- `tempo-data-science-tempomonolithic-gateway` for `TempoMonolithic`.
- `tempo-data-science-tempostack-gateway` for `TempoStack`.

Confirm the Service and port `8080` before constructing its cluster DNS name.
The RHOAI query base is:

```text
https://GATEWAY-SERVICE.MONITORING_NAMESPACE.svc.cluster.local:8080/api/traces/v1/MONITORING_NAMESPACE/tempo
```

Use the exact namespace value as the Tempo tenant. Common query paths include
`/api/search` below that base and the Tempo trace lookup APIs. For direct gateway
requests, preserve the tenant path and send `X-Scope-OrgID: MONITORING_NAMESPACE`
when the gateway requires the tenant header. Use the service CA and caller
authorization. There is no Route created by this operator for Tempo; use a
permitted in-cluster caller or report a port-forward fallback.

### 7. Discover Perses and its configured datasources

Perses is a dashboard/API service, not a replacement for the signal backends.
When `PersesAvailable` is `True`, verify the `data-science-perses` Service and
ready endpoints. Its in-cluster API base is normally:

```text
http://data-science-perses.MONITORING_NAMESPACE.svc.cluster.local:8080
```

Confirm the actual Service port. The read-only health and project API paths are
`/api/v1/health` and `/api/v1/projects`.

An external Perses Route is not owned by this operator. If Route read access is
available, search for a Route whose target Service is `data-science-perses` and
record it only when the host is admitted. Otherwise report `in-cluster-only`;
do not invent a console or gateway URL.

Inspect the deployed datasources to preserve the actual dashboard routing:

```bash
oc -n "$MONITORING_NAMESPACE" get persesdatasources.perses.dev -o json \
  | jq '[.items[] | {
      name: .metadata.name,
      display: .spec.config.display,
      default: .spec.config.default,
      url: .spec.config.plugin.spec.proxy.spec.url,
      queryParams: .spec.config.plugin.spec.queryParams
    }]'
```

The expected RHOAI datasource roles are:

- `data-science-prometheus-datasource` — default Data Science metrics via the
  namespace-enforcing Thanos proxy; it requires `namespace=`.
- `cluster-prometheus-tenancy-datasource` — OpenShift metrics via `:9092`; it
  requires `namespace=`.
- `cluster-prometheus-datasource` — cluster-wide OpenShift metrics via `:9091`;
  caller RBAC applies.
- `tempo-datasource` — RHOAI Tempo query gateway with the monitoring namespace
  tenant.

Treat deployed datasource URLs as corroboration and dashboard routing. The
direct, authorized endpoint records discovered in the previous steps remain the
preferred handoff for query skills.

### 8. Discover Korrel8r

When any of metrics, traces, or forwarded logs is configured, check the
`Korrel8rAvailable` condition and the `korrel8r` Deployment/Service. The REST
endpoint is:

```text
https://korrel8r.MONITORING_NAMESPACE.svc.cluster.local:8443
```

Korrel8r is a TLS-protected ClusterIP Service. It has no Route, no MCP endpoint
(`--mcp=false`), and no writable runtime configuration endpoint. Record these
read-only REST paths, then probe them only with a bounded, authenticated request
when the caller has a suitable token:

- `/api/v1alpha1/domains`
- `/api/v1alpha1/objects`
- `/api/v1alpha1/graphs/neighbors`
- `/api/v1alpha1/graphs/goals`

Graph endpoints are Korrel8r-build dependent. Treat HTTP `404` or `501` as an
unavailable optional path, not as a service failure. `/api/v1alpha1/config` is
intentionally disabled and should return `404`.

Use the caller's bearer token and service CA. Graph requests must be bounded:
include a known `start` query, a time window for telemetry, `depth` (normally
`2`), and conservative `limit`/`queryLimit` values. Never use an unbounded graph
request as an endpoint health check or probe with a caller token that was not
explicitly provided for this task. Never try to modify backend configuration.

If the caller is outside the cluster, provide this fallback without launching
it automatically:

```bash
oc -n "$MONITORING_NAMESPACE" port-forward svc/korrel8r 8443:8443
```

The TLS hostname remains `korrel8r.MONITORING_NAMESPACE.svc`; a port-forward
client must preserve that hostname with `--resolve` and validate the service CA.

### 9. Record collector and other non-query endpoints separately

Downstream troubleshooting skills may need telemetry pipeline endpoints. Include
them in a separate `telemetry_ingest` or `diagnostic` section, never alongside
query endpoints:

- `data-science-collector-collector-headless`: OTLP gRPC `4317` and OTLP HTTP
  `4318`, TLS-protected ingestion.
- `data-science-collector-monitoring`: TLS collector monitoring endpoint on
  `8890`.
- `data-science-collector-prometheus`: TLS collector Prometheus exporter on
  `8889`.
- `data-science-prometheus-cluster-proxy`: `/metrics` only, for node metrics.
- Alertmanager Services exposing port `9093`: `/api/v2/alerts` and
  `/api/v2/status` are diagnostic APIs only; discover the actual Service and
  authentication requirements rather than assuming a shared token.

Confirm each Service and ready EndpointSlice before reporting it. Collector
endpoints accept or expose telemetry; they do not replace Prometheus, Loki,
Tempo, or Korrel8r query APIs. The helper also places Perses datasource URLs and
Alertmanager endpoints in `diagnostic_endpoints`, not in the direct signal
query handoff.

## Handoff format

Return a concise JSON object first, followed by warnings and the evidence used.
Use one record per confirmed endpoint and use `null` rather than guessed values:

```json
{
  "schema_version": "1",
  "cluster": {"server": "...", "user": "..."},
  "monitoring": {
    "resource": "default-monitoring",
    "namespace": "...",
    "management_state": "Managed",
    "conditions": {}
  },
  "query_endpoints": [
    {
      "signal": "metrics|logs|traces|correlation|dashboards",
      "name": "...",
      "url": "...",
      "access": "route|cluster-service|port-forward",
      "scope": "...",
      "auth": "caller-bearer|none|see-notes",
       "tls": "service-ca|route-edge|route-reencrypt|route-passthrough|none|see-notes",
      "paths": ["..."],
      "required_query_parameters": [],
      "state": "ready|configured|pending|not-ready|unavailable",
      "source": "Kind/namespace/name"
    }
  ],
  "telemetry_ingest": [],
  "diagnostic_endpoints": [],
  "unavailable": [],
  "warnings": []
}
```

The handoff must explicitly state:

1. The operand namespace and whether DSCI and Monitoring agree.
2. Which metrics lane owns the requested series: Data Science, OpenShift
   tenancy, or OpenShift cluster-wide.
3. Whether `namespace=` is mandatory and which namespace authorization applies.
4. Whether logs are historical Loki data, direct current pod logs, or only an
   ingestion path.
5. Whether traces, Perses, and Korrel8r are ready, pending, or absent.
6. Any RBAC, NetworkPolicy, TLS, or cross-namespace limitation that a query
   skill must account for.

Downstream skills should consume only records in `query_endpoints` whose state
is `ready` and whose source resource was observed. They should use the
`telemetry_ingest` and `diagnostic_endpoints` sections only for their stated
purpose.
