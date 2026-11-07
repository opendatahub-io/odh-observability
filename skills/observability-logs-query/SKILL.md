---
name: observability-logs-query
description: Queries RHOAI forwarded application logs through a ready Loki endpoint returned by observability-endpoint-discovery. Use when an agent needs bounded LogQL results without hardcoding endpoints, querying usage-log ingestion, or exposing credentials.
license: Apache-2.0
compatibility: Requires a validated observability-endpoint-discovery JSON inventory, Python 3.11+, network access to the selected Loki gateway, and caller-provided authentication when the endpoint requires a bearer token.
metadata:
  issue: RHAI-15151
  version: "1.1"
---

# Observability logs query

Query RHOAI forwarded application logs only through a Loki endpoint that the
`observability-endpoint-discovery` skill has discovered and marked `ready`.
The discovery inventory is the source of truth for the Loki gateway URL,
tenant, TLS, authentication, and supported paths.

This skill is read-only. It does not ingest, modify, delete, or configure logs;
query the usage-log OTLP receiver; access raw Loki storage; or read current pod
logs through the Kubernetes API.

## Prerequisites

The log-forwarding backend must already be configured by the platform and
reported as ready. This skill does not install operators, configure Loki,
configure ClusterLogForwarder, or provision object storage.

The caller needs:

- Python 3.11 or newer;
- a validated inventory from `observability-endpoint-discovery`;
- network access to the selected ready Loki gateway;
- an already-authorized bearer token; and
- the service CA bundle for the application logs lane.

The skill accepts the token only through `--bearer-token-env` and the CA only
through `--ca-file`. It never obtains credentials or disables TLS verification.
For a `tls: "service-ca"` endpoint, `--ca-file` is mandatory; the helper
fails before making a request when it is omitted.

## Workflow

1. From the repository root, run and validate endpoint discovery:

   ```bash
   set -o pipefail
   python3 skills/observability-endpoint-discovery/scripts/discover_endpoints.py \
     --pretty | tee observability-endpoints.json \
     | python3 skills/observability-endpoint-discovery/scripts/validate_inventory.py
   ```

2. Select one `query_endpoints` record with `signal: "logs"`,
   `scope: "application"`, `tenant: "application"`, `auth: "caller-bearer"`,
   `tls: "service-ca"`, `state: "ready"`, a non-null URL, and
   `/loki/api/v1/query_range` in its paths. If more than one logs endpoint is
   ready, pass `--endpoint` explicitly.
   If the selected record lists `required_query_parameters`, supply them with
   repeated `--param KEY=VALUE` options.

   3. Run a bounded LogQL range query:

   ```bash
   set -o pipefail
   python3 skills/observability-logs-query/scripts/query_logs.py \
     --inventory observability-endpoints.json \
     --endpoint data-science-loki \
     --query '{kubernetes_namespace_name="my-namespace"} |= "error"' \
     --start 2026-01-01T00:00:00Z \
     --end 2026-01-01T00:05:00Z \
     --limit 100 \
     --direction backward \
     --bearer-token-env RHOAI_LOGS_BEARER_TOKEN \
     --ca-file /path/to/service-ca.crt \
     --pretty \
     | tee logs-result.json \
     | python3 skills/observability-logs-query/scripts/validate_result.py
   ```

4. Treat a non-zero exit status or `status: "error"` as a failed query. Do
   not retry by selecting a raw Loki service, using the usage-log OTLP
   endpoint, disabling TLS verification, or broadening the time window.

## Live-cluster validation setup

Normal use does not create Kubernetes resources. Use this opt-in procedure
when the skill itself must be exercised against a live RHOAI cluster. It was
validated against the RHOAI `data-science-loki` gateway and intentionally keeps
the test caller separate from operator-owned workloads.

The temporary caller needs permission to create a namespace, ServiceAccount,
ConfigMaps, Pod, two narrowly scoped ClusterRoles and bindings, and one
NetworkPolicy. A cluster administrator or an appropriately delegated test
operator must perform the setup. The query helper does not perform these
privileged operations.

### Live authorization contract

For the forwarded application-log lane, the gateway authorizes the bearer
identity with delegated Kubernetes authorization. The temporary caller needs
only:

- `get` on the virtual `loki.grafana.com/application` resource named `logs`;
- read-only `get`, `list`, and `watch` access to core `namespaces`, which the
  gateway uses for the `kubernetes_namespace_name` selector; and
- a projected ServiceAccount bearer token.

The caller does **not** need `system:auth-delegator`; TokenReview and
SubjectAccessReview permissions belong to the operator-managed gateway
ServiceAccount. Do not grant the caller write access to Loki, access to
Secrets, pod-log permissions, or a broad `view`/`cluster-admin` role. A
different cluster version may use a different gateway contract; treat a 403 as
an authorization/setup failure and investigate the gateway's documented SAR
requirements rather than broadening access blindly.

The current live service CA is published in the non-secret ConfigMap
`data-science-lokistack-gateway-ca-bundle` in `redhat-ods-monitoring`, under
`service-ca.crt`. Never copy the gateway's token Secret. The gateway's existing
NetworkPolicy is operator-owned and admits only OpenShift Logging, Korrel8r,
and Prometheus peers. Add a separate policy for the temporary test pod; do not
edit or delete `data-science-lokistack-gateway-ingress`.

### Setup and run

Run discovery first and keep its validated inventory for the test workload:

```bash
set -euo pipefail
MONITORING_NAMESPACE=redhat-ods-monitoring
RUN_ID="$(date +%s)-$$"
TEST_NAMESPACE="rhai-15151-logs-query-${RUN_ID}"
TEST_SERVICE_ACCOUNT=logs-query-test
TEST_READER_ROLE="rhai-15151-logs-query-reader-${RUN_ID}"
TEST_NAMESPACE_ROLE="rhai-15151-logs-query-namespace-reader-${RUN_ID}"
TEST_READER_BINDING=$TEST_READER_ROLE
TEST_NAMESPACE_BINDING=$TEST_NAMESPACE_ROLE
TEST_NETWORK_POLICY="rhai-15151-logs-query-gateway-ingress-${RUN_ID}"
INVENTORY=observability-endpoints.json
CA_FILE="${TMPDIR:-/tmp}/rhai-15151-service-ca.crt"

python3 skills/observability-endpoint-discovery/scripts/discover_endpoints.py \
  --pretty > "$INVENTORY"
python3 skills/observability-endpoint-discovery/scripts/validate_inventory.py \
  < "$INVENTORY"

# This reads a public service-CA ConfigMap, not a Secret.
oc get configmap data-science-lokistack-gateway-ca-bundle \
  -n "$MONITORING_NAMESPACE" \
  -o jsonpath='{.data.service-ca\.crt}' > "$CA_FILE"
test -s "$CA_FILE"

oc create namespace "$TEST_NAMESPACE"

# Install cleanup before creating any additional temporary resources. The
# unique names above prevent this trap from deleting another test's resources.
cleanup() {
  set +e
  oc delete pod/logs-query-test -n "$TEST_NAMESPACE" --ignore-not-found
  oc delete configmap/logs-query-skill configmap/loki-gateway-ca \
    -n "$TEST_NAMESPACE" --ignore-not-found
  oc delete networkpolicy "$TEST_NETWORK_POLICY" \
    -n "$MONITORING_NAMESPACE" --ignore-not-found
  oc delete clusterrolebinding "$TEST_READER_BINDING" \
    "$TEST_NAMESPACE_BINDING" --ignore-not-found
  oc delete clusterrole "$TEST_READER_ROLE" "$TEST_NAMESPACE_ROLE" \
    --ignore-not-found
  oc delete namespace "$TEST_NAMESPACE" --ignore-not-found
  rm -f "$CA_FILE" logs-result.json
}
trap cleanup EXIT

oc create serviceaccount "$TEST_SERVICE_ACCOUNT" -n "$TEST_NAMESPACE"
```

Apply the two read-only ClusterRoles and their bindings. The virtual Loki
resource is deliberately restricted to query authorization (`get`), not log
ingestion (`create`):

```bash
cat <<EOF | oc apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ${TEST_READER_ROLE}
  labels:
    app.kubernetes.io/part-of: observability-logs-query-live-test
    app.kubernetes.io/managed-by: manual-test
rules:
- apiGroups: [loki.grafana.com]
  resources: [application]
  resourceNames: [logs]
  verbs: [get]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ${TEST_READER_BINDING}
  labels:
    app.kubernetes.io/part-of: observability-logs-query-live-test
    app.kubernetes.io/managed-by: manual-test
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: ${TEST_READER_ROLE}
subjects:
- kind: ServiceAccount
  name: ${TEST_SERVICE_ACCOUNT}
  namespace: ${TEST_NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ${TEST_NAMESPACE_ROLE}
  labels:
    app.kubernetes.io/part-of: observability-logs-query-live-test
    app.kubernetes.io/managed-by: manual-test
rules:
- apiGroups: [""]
  resources: [namespaces]
  verbs: [get, list, watch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ${TEST_NAMESPACE_BINDING}
  labels:
    app.kubernetes.io/part-of: observability-logs-query-live-test
    app.kubernetes.io/managed-by: manual-test
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: ${TEST_NAMESPACE_ROLE}
subjects:
- kind: ServiceAccount
  name: ${TEST_SERVICE_ACCOUNT}
  namespace: ${TEST_NAMESPACE}
EOF
```

Copy the service CA and the already discovered/query helper inputs into
temporary ConfigMaps. These ConfigMaps contain no bearer token or Secret:

```bash
oc create configmap loki-gateway-ca -n "$TEST_NAMESPACE" \
  --from-file=service-ca.crt="$CA_FILE"
oc create configmap logs-query-skill -n "$TEST_NAMESPACE" \
  --from-file=query_logs.py=skills/observability-logs-query/scripts/query_logs.py \
  --from-file=validate_inventory.py=skills/observability-endpoint-discovery/scripts/validate_inventory.py \
  --from-file=validate_result.py=skills/observability-logs-query/scripts/validate_result.py \
  --from-file=inventory.json="$INVENTORY"
```

Add only the temporary pod as an ingress peer. The combined selectors mean
that both the temporary namespace and the temporary pod labels must match:

```bash
cat <<EOF | oc apply -f -
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: ${TEST_NETWORK_POLICY}
  namespace: ${MONITORING_NAMESPACE}
  labels:
    app.kubernetes.io/part-of: observability-logs-query-live-test
    app.kubernetes.io/managed-by: manual-test
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/component: lokistack-gateway
      app.kubernetes.io/instance: data-science-lokistack
      app.kubernetes.io/name: lokistack
  policyTypes: [Ingress]
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: ${TEST_NAMESPACE}
      podSelector:
        matchLabels:
          app.kubernetes.io/component: logs-query-test
          app.kubernetes.io/name: observability-logs-query
    ports:
    - protocol: TCP
      port: 8080
EOF
```

Create a short-lived, non-root test pod. The wrapper reads the projected token
only to place it in the explicit environment variable consumed by the helper;
the helper itself never discovers or reads that token. The token is never
printed or placed in the result:

```bash
END=$(date +%s)
START=$((END - 300))
cat <<EOF | oc apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: logs-query-test
  namespace: ${TEST_NAMESPACE}
  labels:
    app.kubernetes.io/component: logs-query-test
    app.kubernetes.io/name: observability-logs-query
spec:
  serviceAccountName: ${TEST_SERVICE_ACCOUNT}
  restartPolicy: Never
  securityContext:
    seccompProfile:
      type: RuntimeDefault
  containers:
  - name: runner
    image: registry.access.redhat.com/ubi9/python-311:latest
    imagePullPolicy: IfNotPresent
    command: ["/bin/sh", "-c"]
    args:
    - >-
      export LOGS_TOKEN="\$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)";
      python3 /opt/logs-query/query_logs.py
      --inventory /opt/logs-query/inventory.json
      --endpoint data-science-loki
      --query '{kubernetes_namespace_name="${MONITORING_NAMESPACE}"}'
      --start "${START}"
      --end "${END}"
      --limit 5
      --direction backward
      --bearer-token-env LOGS_TOKEN
      --ca-file /etc/loki-ca/service-ca.crt
      --max-response-bytes 1048576
      --pretty
    securityContext:
      allowPrivilegeEscalation: false
      readOnlyRootFilesystem: true
      runAsNonRoot: true
    volumeMounts:
    - name: logs-query
      mountPath: /opt/logs-query
      readOnly: true
    - name: loki-ca
      mountPath: /etc/loki-ca
      readOnly: true
    - name: kube-api-access
      mountPath: /var/run/secrets/kubernetes.io/serviceaccount
      readOnly: true
  volumes:
  - name: logs-query
    configMap:
      name: logs-query-skill
  - name: loki-ca
    configMap:
      name: loki-gateway-ca
  - name: kube-api-access
    projected:
      sources:
      - serviceAccountToken:
          path: token
          expirationSeconds: 3600
      - configMap:
          name: kube-root-ca.crt
          items:
          - key: ca.crt
            path: ca.crt
      - downwardAPI:
          items:
          - path: namespace
            fieldRef:
              fieldPath: metadata.namespace
EOF

oc wait --for=jsonpath='{.status.phase}'=Succeeded \
  pod/logs-query-test -n "$TEST_NAMESPACE" --timeout=180s
oc logs pod/logs-query-test -n "$TEST_NAMESPACE" > logs-result.json
python3 skills/observability-logs-query/scripts/validate_result.py \
  < logs-result.json
python3 skills/observability-logs-query/scripts/validate_live_setup.py \
  --monitoring-namespace "$MONITORING_NAMESPACE" \
  --test-namespace "$TEST_NAMESPACE" \
  --service-account "$TEST_SERVICE_ACCOUNT" \
  --reader-role "$TEST_READER_ROLE" \
  --reader-binding "$TEST_READER_BINDING" \
  --namespace-reader-role "$TEST_NAMESPACE_ROLE" \
  --namespace-reader-binding "$TEST_NAMESPACE_BINDING" \
  --network-policy "$TEST_NETWORK_POLICY" \
  --result logs-result.json
```

The expected successful result is `status: "success"`, endpoint
`data-science-loki`, `auth: "caller-bearer"`, `tls: "service-ca"`, tenant
`application`, and Loki `streams` data (possibly an empty list). A 403 means
the caller's gateway authorization is incomplete; a timeout usually means the
temporary NetworkPolicy selector or pod labels are wrong.

### Live cleanup

The setup block installs an `EXIT` trap that removes the temporary resources,
including the cluster-scoped RBAC, even when validation fails or the shell is
interrupted. If the shell was restarted before the trap ran, execute:

```bash
oc delete pod/logs-query-test -n "$TEST_NAMESPACE" --ignore-not-found
oc delete configmap/logs-query-skill configmap/loki-gateway-ca \
  -n "$TEST_NAMESPACE" --ignore-not-found
oc delete networkpolicy "$TEST_NETWORK_POLICY" \
  -n "$MONITORING_NAMESPACE" --ignore-not-found
oc delete clusterrolebinding "$TEST_READER_BINDING" \
  "$TEST_NAMESPACE_BINDING" --ignore-not-found
oc delete clusterrole "$TEST_READER_ROLE" "$TEST_NAMESPACE_ROLE" \
  --ignore-not-found
oc delete namespace "$TEST_NAMESPACE" --ignore-not-found
rm -f "$CA_FILE" logs-result.json
```

Do not delete or patch the operator-owned
`data-science-lokistack-gateway-ingress` policy. The checked-in
`validate_live_setup.py` script and `test_live_setup.py` tests enforce the
temporary RBAC and NetworkPolicy shape used above.

## Query behavior

- `--query` is a LogQL expression and is required. The helper does not rewrite
  or infer a query.
- `--start` and `--end` are required Unix-second or timezone-qualified RFC3339
  timestamps. Requests serialize them as integer Unix nanoseconds, rounding
  outward when necessary so the requested range remains covered.
- The default maximum search window is 24 hours. Results are limited to at
  most 500 entries, with a default of 100. The default direction is
  `backward`; use `forward` for oldest-first results.
- The helper enforces a 120-second maximum request timeout and a 5 MiB maximum
  response by default.
- The normal RHOAI logs lane uses tenant `application` and the discovered
  `X-Scope-OrgID: application` header. The helper rejects any other tenant or
  scope; do not substitute a tenant from user input.
- A successful query with zero streams is valid data, not evidence that Loki
  is unavailable.

## Rules

- Run and validate endpoint discovery before every logs query.
- Use only a ready `signal: "logs"` query endpoint from the validated inventory.
  Do not invent or rewrite the endpoint URL.
- Query only the discovered Loki `query_range` path with GET. Do not use write,
  delete, tail, status, rule, or administration endpoints.
- Use only the forwarded application lane: `scope` and tenant must be
  `application`, authentication must be `caller-bearer`, and TLS must be
  `service-ca`.
- Preserve every non-Loki `required_query_parameters` value advertised by the
  inventory; fail rather than silently omit one.
- Use the supplied service CA for the application logs endpoint. Never use
  `curl -k` or an equivalent insecure TLS bypass.
- Do not follow redirects; this prevents bearer tokens from reaching an
  untrusted target.
- The helper does not read Secrets, service-account token files, kubeconfig
  credentials, or object-storage credentials. A caller-controlled in-cluster
  wrapper may read its own projected ServiceAccount token to populate the
  explicit bearer-token environment variable, but must not print or persist
  that token.
- Return structured JSON on errors without including bearer tokens,
  authorization headers, or credentials in the result.
- If Loki is ready but ClusterLogForwarder is not ready, treat historical logs
  as potentially incomplete and preserve the discovery warning or condition.
  Do not silently switch to direct pod logs.
- The usage-log OTLP endpoint is an ingestion endpoint and must never be passed
  to this skill.

## Result format

The helper returns a deterministic JSON object with:

- `schema_version`: currently `1`;
- `status`: `success` or `error`;
- `endpoint`: selected endpoint identity and safe URL metadata;
- `query`: LogQL, bounded time window, limit, direction, and parameters;
- `data`: Loki response data containing `streams` or `matrix` results on
  success;
- `warnings`: server or client warnings; and
- `error`: a typed error object on failure.

Validate a saved result before handing it to another skill:

```bash
python3 skills/observability-logs-query/scripts/validate_result.py < logs-result.json
```

For endpoint selection, readiness states, tenant authorization, TLS, and
forwarding evidence, see the
[`observability-endpoint-discovery`](../observability-endpoint-discovery/SKILL.md)
skill. Do not duplicate its discovery logic here.
