# Korrel8r

When DSCI monitoring is `Managed` and at least one of metrics, traces, or
logs is configured, the monitoring module deploys one RHOAI-owned Korrel8r in
the monitoring namespace. It has a TLS-protected ClusterIP Service only; it
does not create a Route, console link, UIPlugin, MCP endpoint, or writable
runtime config endpoint.

The Deployment uses one replica with requests of `50m` CPU and `64Mi` memory,
and limits of `200m` CPU and `512Mi` memory. The operator includes the stock
Korrel8r rules and pins the stores to RHOAI backends:

The default container is the Korrel8r image from Cluster Observability Operator
1.5.3. This build supports traversal-wide object and query limits and cancels
searches under memory pressure. Releases can override the image through
`RELATED_IMAGE_KORREL8R_IMAGE`; an override must support these tuning settings.

- metrics: the RHOAI `ThanosQuerier` on port `10902`;
- traces: the RHOAI Tempo gateway and the monitoring-namespace tenant;
- logs: the RHOAI LokiStack `application` tenant, which is the RHOAI
  inference-log tenant in OpenShift logging mode. When that store is
  unavailable, `direct: true` makes Korrel8r read the requested container logs
  from the Kubernetes API; it never forwards or writes those logs to Loki.

Configured remote stores are rendered independently of backend Service
readiness, so they remain available when their asynchronously-created Service
appears. A missing Loki/CLO backend therefore does not remove the Deployment
or prevent metrics and traces from being used. Direct pod logs are kept enabled
whenever logs are configured.

Korrel8r queries telemetry; it does not collect it. RHOAI workload metrics and
traces are forwarded through the `data-science-collector-collector` pods to
Thanos and Tempo. Container logs are collected separately by the OpenShift
Cluster Logging Operator: the `ClusterLogForwarder` and its service account are
reconciled in `openshift-logging`, where CLO creates the Vector DaemonSet, and
the output continues to target the RHOAI LokiStack in the monitoring namespace.
The forwarder uses the namespace-local `openshift-service-ca.crt` ConfigMap to
trust the Loki gateway; it does not move or alter the LokiStack. Thus, CLO is
the path for stored, historical Loki correlation; the direct fallback provides
only current Kubernetes container-log access while that path is unavailable.

## REST verification

The DSCI selects the monitoring operand namespace. Do not assume it is
`opendatahub` or `redhat-ods-monitoring`; both are valid deployment values.
`default-monitoring` is the generated Monitoring CR name, not a namespace.
Discover the active DSCI and verify that the Monitoring CR points to the same
operand namespace before forwarding the ClusterIP Service:

```bash
DSCI_NAME=$(oc get dsci -o jsonpath='{.items[0].metadata.name}')
test -n "$DSCI_NAME" || {
  echo "No DSCInitialization resource found" >&2
  exit 1
}

STORE_NS=$(oc get dsci "$DSCI_NAME" \
  -o jsonpath='{.spec.monitoring.namespace}')
test -n "$STORE_NS" || {
  echo "DSCI monitoring namespace is empty" >&2
  exit 1
}

MONITORING_CR=$(oc get monitoring \
  -o jsonpath='{.items[0].metadata.name}')
CR_STORE_NS=$(oc get monitoring "$MONITORING_CR" \
  -o jsonpath='{.spec.namespace}')
test "$CR_STORE_NS" = "$STORE_NS" || {
  echo "DSCI and Monitoring namespace mismatch: $STORE_NS != $CR_STORE_NS" >&2
  exit 1
}

echo "DSCI: $DSCI_NAME"
echo "Monitoring CR: $MONITORING_CR"
echo "Monitoring namespace: $STORE_NS"

oc -n "$STORE_NS" port-forward svc/korrel8r 8443:8443
```

Before testing authenticated requests, verify the service account has the
required cluster-scoped permissions and that the NetworkPolicy permits the
Kubernetes API path. TokenReview and the direct pod-log fallback use the
Kubernetes API, not only the RHOAI backend Services:

```bash
oc get clusterrole korrel8r-query
oc get clusterrolebinding korrel8r-query korrel8r-auth-delegator
oc auth can-i --as="system:serviceaccount:$STORE_NS:korrel8r" \
  create tokenreviews.authentication.k8s.io
oc -n default get endpointslice \
  -l kubernetes.io/service-name=kubernetes -o yaml
oc -n "$STORE_NS" get networkpolicy korrel8r -o yaml
oc -n openshift-logging get clusterlogforwarder data-science-cluster-log-forwarder
oc -n openshift-logging get daemonset data-science-cluster-log-forwarder
```

On OpenShift clusters where the `kubernetes` Service resolves to a
host-network API endpoint, a namespace-only TCP/443 rule may not be sufficient
for TokenReview or direct pod-log traffic. Treat an authenticated request that
hangs or times out as a NetworkPolicy validation failure; do not record the
expected `200` until the actual API-server path is allow-listed.

In a second terminal, use the caller’s token. Korrel8r forwards the bearer
token to Loki and Tempo; it does not use only its ServiceAccount token as a
cross-tenant proxy. The service certificate is valid for the service DNS name,
not `127.0.0.1`, so preserve that name with `--resolve` while testing the
port-forwarded TLS endpoint.

```bash
TOKEN=$(oc whoami -t)
CA_FILE=$(mktemp)
AUTH_HEADER_FILE=$(mktemp)
chmod 600 "$AUTH_HEADER_FILE"
printf 'Authorization: Bearer %s\n' "$TOKEN" > "$AUTH_HEADER_FILE"
trap 'rm -f "$CA_FILE" "$AUTH_HEADER_FILE"' EXIT
oc -n openshift-service-ca get configmap signing-cabundle \
  -o jsonpath='{.data.ca-bundle\.crt}' > "$CA_FILE"
KORREL8R_HOST="korrel8r.$STORE_NS.svc"

POD_NS="replace-with-workload-namespace"
POD_NAME="replace-with-known-pod"
START='k8s:Pod.v1:{"namespace":"'"$POD_NS"'","name":"'"$POD_NAME"'"}'
if ! WINDOW_START=$(date -u -v-1H '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null); then
  if ! WINDOW_START=$(date -u -d '1 hour ago' '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null); then
    echo "Unable to calculate a portable UTC start time" >&2
    exit 1
  fi
fi
test -n "$WINDOW_START" || {
  echo "UTC start time is empty" >&2
  exit 1
}
WINDOW_END=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
NEIGHBORS_BODY=$(jq -n --arg start "$START" --arg windowStart "$WINDOW_START" --arg windowEnd "$WINDOW_END" '{depth:2,start:{queries:[$start],constraint:{limit:50,queryLimit:10,start:$windowStart,end:$windowEnd}}}')
GOALS_BODY=$(jq -n --arg start "$START" --arg windowStart "$WINDOW_START" --arg windowEnd "$WINDOW_END" '{goals:["metric:metric","trace:span","log:application"],start:{queries:[$start],constraint:{limit:50,queryLimit:10,start:$windowStart,end:$windowEnd}}}')
```

Check authentication, configured domains, and the disabled runtime config
endpoint:

```bash
curl --noproxy '*' --cacert "$CA_FILE" --resolve "$KORREL8R_HOST:8443:127.0.0.1" \
  -i -H "@$AUTH_HEADER_FILE" \
  "https://$KORREL8R_HOST:8443/api/v1alpha1/domains"

curl --noproxy '*' --cacert "$CA_FILE" --resolve "$KORREL8R_HOST:8443:127.0.0.1" \
  -i -H "@$AUTH_HEADER_FILE" \
  "https://$KORREL8R_HOST:8443/api/v1alpha1/config"
# Expected: HTTP 404; callers cannot change store URLs or tenant headers.
```

Use bounded requests. `depth: 2` is the normal sign-off path; never omit the
start constraint from graph requests. `limit` applies per query and `queryLimit`
per class; the server also caps each traversal at 3,000 objects and 300 queries.
Set a one-hour time window (replace the timestamps with the desired UTC window).
For a partial signal configuration, remove unavailable domains from `GOALS_BODY`.

```bash
curl --noproxy '*' --cacert "$CA_FILE" --resolve "$KORREL8R_HOST:8443:127.0.0.1" \
  -sS -H "@$AUTH_HEADER_FILE" \
  -H 'Content-Type: application/json' \
  -d "$NEIGHBORS_BODY" \
  "https://$KORREL8R_HOST:8443/api/v1alpha1/graphs/neighbors"

curl --noproxy '*' --cacert "$CA_FILE" --resolve "$KORREL8R_HOST:8443:127.0.0.1" \
  -sS -H "@$AUTH_HEADER_FILE" \
  -H 'Content-Type: application/json' \
  -d "$GOALS_BODY" \
  "https://$KORREL8R_HOST:8443/api/v1alpha1/graphs/goals"

curl --noproxy '*' --cacert "$CA_FILE" --resolve "$KORREL8R_HOST:8443:127.0.0.1" \
  -sS -H "@$AUTH_HEADER_FILE" --get \
  --data-urlencode "query=$START" \
  --data-urlencode 'constraint.limit=20' \
  --data-urlencode 'constraint.queryLimit=10' \
  --data-urlencode "constraint.start=$WINDOW_START" \
  --data-urlencode "constraint.end=$WINDOW_END" \
  "https://$KORREL8R_HOST:8443/api/v1alpha1/objects"
```

Acceptance results from a known RHOAI pod are:

- the goal graph contains `metric:metric` backed by RHOAI Thanos series;
- `trace:span` is present when traces are enabled and the workload emits
  traces;
- `log:application` is reached through RHOAI Loki after CLO forwarding, or
  through the direct pod-log fallback before CLO/Loki is ready.

For a complete three-edge end-to-end check, enable all of metrics, traces, and
logs, then use a RHOAI-owned workload that exposes a scraped metric, emits OTLP
traces to the data-science collector, and writes to stdout. Verify the metric
independently in Thanos using
`exported_namespace`/`exported_pod`, then send the bounded Korrel8r goals
request. The response must contain the three edges from `k8s:Pod.v1` to
`metric:metric`, `trace:span`, and `log:application`. Check that the
ClusterLogForwarder is `Ready=True` and its Vector DaemonSet has ready pods
before treating the Loki result as a Korrel8r failure. For a partial signal
configuration, validate only the edges for enabled stores. If the response has
`truncation` metadata, narrow the request and rerun it before treating a missing
edge as a missing signal.

The `512Mi` memory limit is paired with traversal-wide caps and a memory-pressure
guard. Record the Korrel8r pod's last termination state while exercising trace
and log goals:

```bash
oc -n "$STORE_NS" get pod -l app.kubernetes.io/name=korrel8r -o json \
  | jq '.items[0].status.containerStatuses[]
      | select(.name == "korrel8r")
      | {ready,restarts,lastState}'
```

If a bounded trace or log request causes `OOMKilled`, the deployment must not
be considered ready for sign-off.

For a broad generated query, retain the same limits and time range. If a hop
cannot safely execute the generated selector, Korrel8r should return an error
for that hop rather than retrying it without bounds.

## Readiness and graceful degradation

When Loki or CLO is unavailable, record the feature condition rather than
treating Korrel8r as failed:

```bash
oc get monitoring "$MONITORING_CR" -o json \
  | jq -r '(.status.conditions // [])[]
      | select(.type == "LokiStackAvailable" or
               .type == "ClusterLogForwarderAvailable" or
               .type == "Korrel8rAvailable")
      | "\(.type)=\(.status) reason=\(.reason) severity=\(.severity // "") message=\(.message // "")"'
```

`LokiStackAvailable=False` and/or `ClusterLogForwarderAvailable=False` with a
not-ready or missing-operator reason is the expected graceful-degradation
record while the optional backend is unavailable. `Korrel8rAvailable` should
remain `True`, and metrics/traces must remain configured independently.

The ServiceAccount has read access needed for stock Kubernetes correlation,
direct pod logs, and the pinned RHOAI Loki/Tempo tenant resources. A separate
`system:auth-delegator` binding grants the required TokenReview delegation.
Cluster Loki, platform Tempo, and the
namespace-restricted Prometheus proxy are not configured stores.

Producer-side trace/session instrumentation, COO troubleshooting UI, Perses or
console links, Routes, and MCP integration are outside this story.
