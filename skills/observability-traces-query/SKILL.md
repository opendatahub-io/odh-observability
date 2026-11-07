---
name: observability-traces-query
description: Queries RHOAI Tempo traces through a ready endpoint returned by observability-endpoint-discovery. Use when an agent needs bounded trace searches or a specific trace from RHOAI without hardcoding Tempo endpoints, bypassing tenant authorization, or exposing credentials.
license: Apache-2.0
compatibility: Requires a validated observability-endpoint-discovery JSON inventory, Python 3.11+, network access to the selected Tempo gateway, and caller-provided authentication when the endpoint requires a bearer token.
metadata:
  issue: RHAI-15150
  version: "1.0"
---

# Observability traces query

Query RHOAI traces only through an endpoint that the
`observability-endpoint-discovery` skill has discovered and marked `ready`.
The discovery inventory is the source of truth for the Tempo gateway URL,
tenant, TLS, authentication, and supported paths.

This skill is read-only. It does not configure tracing, ingest or delete
traces, discover credentials, or query a raw Tempo backend directly.

## Prerequisites

The tracing backend must already be configured by the platform and reported as
ready. This skill does not install operators, configure storage, or provision
Tempo.

The caller needs:

- Python 3.11 or newer;
- a validated inventory from `observability-endpoint-discovery`;
- network access to the selected ready Tempo gateway;
- an already-authorized bearer token when the inventory reports
  `auth: "caller-bearer"`; and
- the service CA bundle when the inventory reports `tls: "service-ca"`.

The skill accepts the token only through `--bearer-token-env` and the CA only
through `--ca-file`. It never obtains credentials or disables TLS verification.

## Workflow

1. From the repository root, run and validate endpoint discovery:

   ```bash
   set -o pipefail
   python3 skills/observability-endpoint-discovery/scripts/discover_endpoints.py \
     --pretty | tee observability-endpoints.json \
     | python3 skills/observability-endpoint-discovery/scripts/validate_inventory.py
   ```

2. Select one `query_endpoints` record with `signal: "traces"`,
   `state: "ready"`, a non-null URL, and the path needed for the operation.
   If more than one trace endpoint is ready, pass `--endpoint` explicitly. If
   the selected record lists `required_query_parameters`, supply them with
   repeated `--param KEY=VALUE` options.

3. Search a bounded time window or retrieve a trace by ID:

   ```bash
   python3 skills/observability-traces-query/scripts/query_traces.py \
     --inventory observability-endpoints.json \
     --endpoint tempo-data-science-tempomonolithic-gateway \
     --start 2026-01-01T00:00:00Z \
     --end 2026-01-01T00:05:00Z \
      --service my-service \
      --limit 100 \
      --bearer-token-env RHOAI_TRACES_BEARER_TOKEN \
      --ca-file /path/to/service-ca.crt \
      --pretty \
     | tee traces-result.json \
     | python3 skills/observability-traces-query/scripts/validate_result.py
   ```

   Or retrieve one trace:

   ```bash
   python3 skills/observability-traces-query/scripts/query_traces.py \
     --inventory observability-endpoints.json \
      --trace-id <hex-trace-id> \
      --bearer-token-env RHOAI_TRACES_BEARER_TOKEN \
      --ca-file /path/to/service-ca.crt \
      --pretty \
     | tee trace-result.json \
     | python3 skills/observability-traces-query/scripts/validate_result.py
   ```

4. Treat a non-zero exit status or `status: "error"` as a failed query. Do
   not retry by selecting a raw Tempo service, removing the tenant header,
   disabling TLS verification, or broadening the search window.

## Query behavior

- Search requires explicit `--start` and `--end` values as Unix seconds or
  timezone-qualified RFC3339 timestamps. Requests round the start down and end
  up to integer Unix seconds so the requested window remains covered. The
  default maximum window is 24 hours.
- Search supports a TraceQL query (`--query`), service, operation, status,
  tags, minimum/maximum duration, and a bounded result limit.
- `--status` is represented in Tempo's TraceQL `q` expression and cannot be
  combined with `--query`. When a TraceQL query is used, service and operation
  filters are included in that same expression so they are not lost.
- `--service`, `--operation`, and `--tag KEY=VALUE` use Tempo's search filters
  when they are not incorporated into TraceQL. Tag pairs are logfmt encoded,
  separated by spaces, and values containing spaces, quotes, or backslashes are
  quoted and escaped. Commas in values are valid.
- Duration filters use Go duration units: `ns`, `us`, `µs`, `ms`, `s`, `m`, or
  `h`. Endpoint-specific non-Tempo parameters use `--param KEY=VALUE`.
- Trace IDs must be 16- or 32-character hexadecimal values. Trace lookup does
  not require a time window.
- Requests use GET and the endpoint's discovered tenant header, normally
  `X-Scope-OrgID`. Caller bearer tokens are supplied only through an explicit
  environment variable.
- The helper enforces a 120-second maximum timeout and a 5 MiB maximum
  response by default. Search results are limited to at most 500 traces.

## Rules

- Run and validate endpoint discovery before every trace query.
- Use only a ready `signal: "traces"` query endpoint from the validated
  inventory. Do not invent or rewrite the endpoint URL.
- Require the discovered tenant and required headers. Do not substitute a
  namespace or tenant from user input.
- Preserve every non-Tempo `required_query_parameters` value advertised by the
  inventory; fail rather than silently omit one.
- Use the supplied service CA for TLS endpoints. Never use `curl -k` or an
  equivalent insecure TLS bypass.
- Do not follow redirects; this prevents bearer tokens from reaching an
  untrusted target.
- Do not read Secrets, service-account token files, kubeconfig credentials,
  or print authorization headers.
- Reject sensitive query parameter names, control characters, malformed trace
  IDs, invalid timestamps, excessive windows, and unbounded searches.
- Return structured JSON on errors without including bearer tokens,
  authorization headers, or credentials in the result.
- A successful search with zero traces is valid data, not evidence that Tempo
  is unavailable.
- Exporter-only tracing has no local query backend. If discovery reports no
  ready trace endpoint, return the discovery reason instead of querying the
  collector or an internal Tempo port.

## Result format

The helper returns a deterministic JSON object with:

- `schema_version`: currently `1`;
- `status`: `success` or `error`;
- `endpoint`: selected endpoint identity and safe URL metadata;
- `query`: operation, filters, trace ID or bounded time window;
- `data`: Tempo search results or the retrieved trace document on success;
- `warnings`: server or client warnings; and
- `error`: a typed error object on failure.

Validate a saved result before handing it to another skill:

```bash
python3 skills/observability-traces-query/scripts/validate_result.py < traces-result.json
```

For endpoint selection, readiness states, tenant authorization, TLS, and
service evidence, see the
[observability-endpoint-discovery](../observability-endpoint-discovery/SKILL.md)
skill. Do not duplicate its discovery logic here.
