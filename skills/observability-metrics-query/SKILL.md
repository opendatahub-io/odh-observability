---
name: observability-metrics-query
description: Queries RHOAI Prometheus-compatible metrics through a ready endpoint returned by observability-endpoint-discovery. Use when an agent needs bounded instant or range PromQL results from RHOAI or OpenShift metrics without hardcoding endpoints, bypassing namespace isolation, or exposing credentials.
license: Apache-2.0
compatibility: Requires a validated observability-endpoint-discovery JSON inventory, Python 3.11+, network access to the selected endpoint, and caller-provided authentication when the endpoint requires a bearer token.
metadata:
  issue: RHAI-15149
  version: "1.0"
---

# Observability metrics query

Query RHOAI metrics only through an endpoint that the
`observability-endpoint-discovery` skill has discovered and marked `ready`.
The discovery inventory is the source of truth for endpoint selection,
namespace isolation, TLS, and authentication requirements.

This skill is read-only. It does not change cluster resources, discover
credentials, or query an internal Prometheus or Thanos backend directly.

## Workflow

1. From the repository root, run and validate endpoint discovery:

   ```bash
   set -o pipefail
   python3 skills/observability-endpoint-discovery/scripts/discover_endpoints.py \
     --pretty | tee observability-endpoints.json \
     | python3 skills/observability-endpoint-discovery/scripts/validate_inventory.py
   ```

2. Select one `query_endpoints` record with `signal: "metrics"`,
   `state: "ready"`, a non-null URL, and a path for the requested operation.
   If more than one metrics lane is ready, pass `--endpoint` explicitly; do
   not combine Data Science and OpenShift platform lanes in one query.

3. Supply every value in `required_query_parameters`. For RHOAI namespace
   proxies, pass `--namespace` and use the authorized workload namespace.
   Never put the namespace in a POST body or bypass the proxy.

4. If the endpoint has `auth: caller-bearer`, provide a token through an
   explicitly populated environment variable with `--bearer-token-env`. The
   script uses it only in the request and never prints, stores, or returns it.
   The script does not obtain tokens or read Kubernetes Secrets.

5. Run a bounded query:

   ```bash
   python3 skills/observability-metrics-query/scripts/query_metrics.py \
     --inventory observability-endpoints.json \
     --endpoint data-science-thanos-route \
     --namespace <authorized-workload-namespace> \
     --query 'up' \
     --bearer-token-env RHOAI_METRICS_BEARER_TOKEN \
     --pretty \
     | tee metrics-result.json \
     | python3 skills/observability-metrics-query/scripts/validate_result.py
   ```

   Use `--start`, `--end`, and `--step` together for a range query. The
   helper enforces a 24-hour maximum range, a 15-second minimum step, a
   20-second default request timeout (never more than 120 seconds), and a
   5 MiB response limit.

6. Treat a non-zero exit status or `status: "error"` as a failed query. Do not
   retry by selecting a raw backend, removing required query parameters,
   disabling TLS verification, or broadening the time range.

## Rules

- Use only ready metrics records from the validated discovery inventory.
- Do not accept or invent a metrics URL outside the inventory.
- Use GET requests to the Prometheus HTTP API. Do not send namespace form data
  or use write, admin, configuration, or reload endpoints.
- Keep instant and range queries bounded. Reject missing or ambiguous endpoint
  selection, unparseable timestamps, excessive ranges, small steps, oversized
  responses, and sensitive endpoint query parameters.
- Preserve the endpoint's required query parameters and caller authorization.
- Use the supplied CA file when service-CA trust is required. Never use an
  insecure TLS bypass such as `curl -k` or an equivalent SSL context.
- Do not follow endpoint redirects; this prevents caller bearer tokens from
  being sent to an untrusted redirect target.
- Do not read Secrets, service-account token files, kubeconfig credentials, or
  print authorization headers.
- Return the structured JSON result even on query errors, but never include the
  bearer token or authorization header in it.
- An empty successful result is valid data, not evidence that the endpoint is
  unavailable.

## Result format

The helper returns a deterministic JSON object with:

- `schema_version`: currently `1`.
- `status`: `success` or `error`.
- `endpoint`: selected endpoint identity and safe base URL metadata.
- `query`: operation, PromQL, and bounded time parameters.
- `data`: the Prometheus response data on success.
- `warnings`: server or client warnings.
- `error`: a typed error object on failure.

Validate a saved result before handing it to another skill:

```bash
python3 skills/observability-metrics-query/scripts/validate_result.py < metrics-result.json
```

For endpoint selection, readiness states, RBAC limitations, and service/route
evidence, see the [observability-endpoint-discovery](../observability-endpoint-discovery/SKILL.md)
skill. Do not duplicate its endpoint-discovery logic here.
