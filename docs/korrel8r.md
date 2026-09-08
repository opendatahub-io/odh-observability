# Korrel8r

When DSCI monitoring is `Managed` and at least one of metrics, traces, or
logs is configured, the monitoring module deploys one RHOAI-owned Korrel8r in
the monitoring namespace. It has a ClusterIP Service only; it does not create
a Route, console link, UIPlugin, MCP endpoint, or writable runtime config
endpoint.

The Deployment uses one replica with requests of `50m` CPU and `64Mi` memory,
and limits of `200m` CPU and `256Mi` memory. The operator includes the stock
Korrel8r rules and pins the stores to RHOAI backends:

The default container is the pinned Korrel8r image shipped with the supported
Cluster Observability Operator release. The older upstream `0.7.x` image does
not accept the top-level timeout configuration required here; releases can
override the image through `RELATED_IMAGE_KORREL8R_IMAGE`.

- metrics: the RHOAI `ThanosQuerier` on port `10902`;
- traces: the RHOAI Tempo gateway and the monitoring-namespace tenant;
- logs: the RHOAI LokiStack `application` tenant, which is the RHOAI
  inference-log tenant in OpenShift logging mode, plus direct Kubernetes pod
  logs as a fallback.

Optional remote stores are enabled independently after their backend Service
is available. A missing Loki/CLO backend therefore does not remove the
Deployment or prevent metrics and traces from being used. Direct pod logs are
kept enabled whenever logs are configured.

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

oc -n "$STORE_NS" port-forward svc/korrel8r 8080:8080
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
oc -n default get endpoints kubernetes -o yaml
oc -n "$STORE_NS" get networkpolicy korrel8r -o yaml
```

On OpenShift clusters where the `kubernetes` Service resolves to a
host-network API endpoint, a namespace-only TCP/443 rule may not be sufficient
for TokenReview or direct pod-log traffic. Treat an authenticated request that
hangs or times out as a NetworkPolicy validation failure; do not record the
expected `200` until the actual API-server path is allow-listed.

In a second terminal, use the caller’s token. Korrel8r forwards the bearer
token to Loki and Tempo; it does not use only its ServiceAccount token as a
cross-tenant proxy.

```bash
TOKEN=$(oc whoami -t)
POD_NS=<inference-or-workload-namespace>
POD_NAME=<known-pod>
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
GOALS_BODY=$(jq -n --arg start "$START" --arg windowStart "$WINDOW_START" --arg windowEnd "$WINDOW_END" '{goals:["metric:metric"],start:{queries:[$start],constraint:{limit:50,queryLimit:10,start:$windowStart,end:$windowEnd}}}')
```

Check authentication, configured domains, and the disabled runtime config
endpoint:

```bash
curl -i -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:8080/api/v1alpha1/domains

curl -i -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:8080/api/v1alpha1/config
# Expected: HTTP 404; callers cannot change store URLs or tenant headers.
```

Use bounded requests. `depth: 2` is the normal sign-off path; never omit the
start constraint from graph requests. Set both a result limit and a one-hour
time window (replace the timestamps with the desired UTC window):

```bash
curl -sS -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d "$NEIGHBORS_BODY" \
  http://127.0.0.1:8080/api/v1alpha1/graphs/neighbors

curl -sS -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d "$GOALS_BODY" \
  http://127.0.0.1:8080/api/v1alpha1/graphs/goals

curl -sS -H "Authorization: Bearer $TOKEN" --get \
  --data-urlencode "query=$START" \
  --data-urlencode 'constraint.limit=20' \
  --data-urlencode 'constraint.queryLimit=10' \
  --data-urlencode "constraint.start=$WINDOW_START" \
  --data-urlencode "constraint.end=$WINDOW_END" \
  http://127.0.0.1:8080/api/v1alpha1/objects
```

Acceptance results from a known RHOAI pod are:

- the goal graph contains `metric:metric` backed by RHOAI Thanos series;
- `trace:span` is present when traces are enabled and the workload emits
  traces;
- `log:application` is reached through RHOAI Loki after CLO forwarding, or
  through the direct pod-log fallback before CLO/Loki is ready.

The `256Mi` memory limit is the required spike baseline. Record the Korrel8r
pod's last termination state while exercising trace and log goals:

```bash
oc -n "$STORE_NS" get pod -l app.kubernetes.io/name=korrel8r -o json \
  | jq '.items[0].status.containerStatuses[]
      | select(.name == "korrel8r")
      | {ready,restarts,lastState}'
```

If a bounded trace or log request causes `OOMKilled`, the baseline-sizing
acceptance criterion is not met. Raising the limit temporarily can confirm a
memory-related diagnosis, but a larger limit must not be recorded as passing
the story's required sizing.

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

The ServiceAccount has only read access needed for stock Kubernetes
correlation and direct pod logs, TokenReview delegation, and the pinned RHOAI
Loki/Tempo tenant resources. Cluster Loki, platform Tempo, and the
namespace-restricted Prometheus proxy are not configured stores.

Producer-side trace/session instrumentation, COO troubleshooting UI, Perses or
console links, Routes, and MCP integration are outside this story.
