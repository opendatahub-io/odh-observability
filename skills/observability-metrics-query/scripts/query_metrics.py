#!/usr/bin/env python3
"""Run bounded, read-only Prometheus API queries from a discovery inventory."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import ssl
import sys
from datetime import datetime, timezone
from http.client import HTTPException
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import HTTPSHandler, HTTPRedirectHandler, Request, build_opener


sys.dont_write_bytecode = True

DISCOVERY_SCRIPTS = Path(__file__).resolve().parents[2] / "observability-endpoint-discovery" / "scripts"
if str(DISCOVERY_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(DISCOVERY_SCRIPTS))

from validate_inventory import validate_inventory as validate_discovery_inventory  # noqa: E402


SCHEMA_VERSION = "1"
MAX_RANGE_SECONDS = 24 * 60 * 60
MIN_STEP_SECONDS = 15.0
DEFAULT_TIMEOUT_SECONDS = 20.0
DEFAULT_MAX_RESPONSE_BYTES = 5 * 1024 * 1024
MAX_QUERY_LENGTH = 16 * 1024
SENSITIVE_QUERY_MARKERS = (
    "password",
    "passwd",
    "apikey",
    "secret",
    "token",
    "credential",
    "privatekey",
)
VALID_AUTH = {"none", "caller-bearer"}
RESERVED_QUERY_PARAMETERS = {"query", "time", "start", "end", "step"}
NAMESPACE_PATTERN = re.compile(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?")
DURATION_PATTERN = re.compile(r"(?P<value>[0-9]+(?:\.[0-9]+)?)(?P<unit>ms|s|m|h|d|w|y)")
TOKEN_ENV_NAME_PATTERN = re.compile(r"[A-Z][A-Z0-9_]{0,63}")


class QueryError(RuntimeError):
    """A safe, user-facing query failure."""

    def __init__(
        self, kind: str, message: str, exit_code: int = 2, warnings: list[str] | None = None
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.message = message
        self.exit_code = exit_code
        self.warnings = warnings or []


class NoRedirectHandler(HTTPRedirectHandler):
    """Prevent bearer tokens from being sent to an untrusted redirect target."""

    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        raise QueryError("redirect_not_allowed", "metrics endpoint returned a redirect", 4)


def has_sensitive_key(key: str) -> bool:
    normalized = "".join(character for character in key.lower() if character.isalnum())
    return any(marker in normalized for marker in SENSITIVE_QUERY_MARKERS)


def parse_safe_url(value: Any) -> Any:
    if not isinstance(value, str) or not value:
        raise QueryError("invalid_inventory", "metrics endpoint URL must be a non-empty string")
    try:
        parsed = urlsplit(value)
        parsed.port
    except (TypeError, ValueError) as error:
        raise QueryError("invalid_inventory", "metrics endpoint URL is malformed") from error
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise QueryError("invalid_inventory", "metrics endpoint URL must use HTTP or HTTPS")
    if parsed.username or parsed.password or parsed.fragment:
        raise QueryError("invalid_inventory", "metrics endpoint URL contains credentials or a fragment")
    if any(has_sensitive_key(key) for key, _ in parse_qsl(parsed.query, keep_blank_values=True)):
        raise QueryError("invalid_inventory", "metrics endpoint URL contains a sensitive query parameter")
    if any(marker in value.lower() for marker in ("bearer", "token=")):
        raise QueryError("invalid_inventory", "metrics endpoint URL appears to contain credentials")
    if (
        parsed.port == 10902
        or "prometheus-operated" in parsed.hostname
        or "thanos-querier-data-science-thanos-querier" in parsed.hostname
    ):
        raise QueryError("invalid_inventory", "raw Prometheus or Thanos backends are not query endpoints")
    return parsed


def load_inventory(path: str) -> dict[str, Any]:
    try:
        if path == "-":
            inventory = json.load(sys.stdin)
        else:
            with Path(path).open(encoding="utf-8") as stream:
                inventory = json.load(stream)
    except (OSError, json.JSONDecodeError) as error:
        raise QueryError("invalid_inventory", f"could not read inventory: {error}") from error
    if not isinstance(inventory, dict) or inventory.get("schema_version") != SCHEMA_VERSION:
        raise QueryError("invalid_inventory", "inventory schema_version must be '1'")
    error = validate_discovery_inventory(inventory)
    if error:
        raise QueryError("invalid_inventory", error)
    for endpoint in inventory["query_endpoints"]:
        if endpoint.get("url") is not None:
            parse_safe_url(endpoint["url"])
    return inventory


def path_supported(endpoint: dict[str, Any], path: str) -> bool:
    return any(str(candidate).split("?", 1)[0].rstrip("/") == path for candidate in endpoint["paths"])


def trusted_endpoint_host(endpoint: dict[str, Any], parsed: Any) -> str | None:
    """Return a host verified by endpoint-discovery provenance, if available."""
    hostname = getattr(parsed, "hostname", None)
    source = endpoint.get("source")
    access = endpoint.get("access")
    if endpoint.get("state") != "ready" or not isinstance(hostname, str) or not isinstance(source, str):
        return None
    source_parts = source.split("/")
    if len(source_parts) != 3 or any(not part for part in source_parts):
        return None
    source_kind, namespace, name = source_parts
    if access == "route":
        return hostname if source_kind == "Route" and endpoint.get("route_admitted") is True else None
    if access == "cluster-service" and source_kind == "Service":
        expected_host = f"{name}.{namespace}.svc.cluster.local"
        return hostname if hostname == expected_host else None
    return None


def select_endpoint(
    inventory: dict[str, Any], endpoint_name: str | None, scope: str | None
) -> dict[str, Any]:
    endpoints = [
        endpoint
        for endpoint in inventory["query_endpoints"]
        if endpoint.get("signal") == "metrics"
        and endpoint.get("state") == "ready"
        and endpoint.get("url")
        and path_supported(endpoint, "/api/v1/query")
    ]
    if endpoint_name:
        matches = [endpoint for endpoint in inventory["query_endpoints"] if endpoint.get("name") == endpoint_name]
        if not matches:
            raise QueryError("endpoint_not_found", f"metrics endpoint '{endpoint_name}' was not found", 3)
        endpoint = matches[0]
        if endpoint.get("signal") != "metrics":
            raise QueryError("invalid_endpoint", f"endpoint '{endpoint_name}' is not a metrics endpoint", 3)
        if endpoint.get("state") != "ready":
            raise QueryError(
                "endpoint_not_ready",
                f"endpoint '{endpoint_name}' is {endpoint.get('state')}, not ready",
                3,
            )
        if not endpoint.get("url") or not path_supported(endpoint, "/api/v1/query"):
            raise QueryError(
                "endpoint_not_queryable",
                f"endpoint '{endpoint_name}' does not expose the Prometheus query API",
                3,
            )
        endpoints = [endpoint]
    if scope:
        endpoints = [endpoint for endpoint in endpoints if endpoint.get("scope") == scope]
    if not endpoints:
        raise QueryError("no_ready_endpoint", "no ready Prometheus-compatible metrics endpoint is available", 3)
    if len(endpoints) > 1:
        names = ", ".join(str(endpoint.get("name")) for endpoint in endpoints)
        raise QueryError("ambiguous_endpoint", f"multiple ready metrics endpoints match; choose --endpoint: {names}", 3)
    return endpoints[0]


def parse_duration(value: str, option: str) -> float:
    if not isinstance(value, str) or not value:
        raise QueryError("invalid_argument", f"{option} must not be empty")
    try:
        numeric = float(value)
    except ValueError:
        numeric = None
    if numeric is not None:
        if not math.isfinite(numeric) or numeric <= 0:
            raise QueryError("invalid_argument", f"{option} must be greater than zero")
        return numeric
    position = 0
    seconds = 0.0
    units = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0, "w": 604800.0, "y": 31536000.0}
    for match in DURATION_PATTERN.finditer(value):
        if match.start() != position:
            raise QueryError("invalid_argument", f"{option} is not a valid duration")
        seconds += float(match.group("value")) * units[match.group("unit")]
        position = match.end()
    if position != len(value) or seconds <= 0:
        raise QueryError("invalid_argument", f"{option} is not a valid duration")
    return seconds


def parse_timestamp(value: str, option: str) -> float:
    try:
        timestamp = float(value)
    except ValueError:
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as error:
            raise QueryError("invalid_argument", f"{option} must be Unix seconds or RFC3339") from error
        if parsed.tzinfo is None:
            raise QueryError("invalid_argument", f"{option} must include a timezone")
        timestamp = parsed.astimezone(timezone.utc).timestamp()
    if not math.isfinite(timestamp) or timestamp < 0:
        raise QueryError("invalid_argument", f"{option} must not be negative")
    return timestamp


def parse_key_value(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise QueryError("invalid_argument", "--param values must use KEY=VALUE")
    key, item = value.split("=", 1)
    if not key or has_sensitive_key(key):
        raise QueryError("invalid_argument", "sensitive or empty query parameter names are not allowed")
    return key, item


def query_metadata(args: argparse.Namespace, mode: str) -> dict[str, Any]:
    metadata: dict[str, Any] = {"mode": mode, "promql": args.query}
    if mode == "instant":
        if args.time is not None:
            parse_timestamp(args.time, "--time")
            metadata["time"] = args.time
    else:
        start = parse_timestamp(args.start, "--start")
        end = parse_timestamp(args.end, "--end")
        if end < start:
            raise QueryError("invalid_argument", "--end must be greater than or equal to --start")
        step = parse_duration(args.step, "--step")
        maximum = parse_duration(args.max_range, "--max-range")
        minimum_step = parse_duration(args.min_step, "--min-step")
        if maximum > MAX_RANGE_SECONDS:
            raise QueryError("invalid_argument", "--max-range cannot exceed 24 hours")
        if minimum_step < MIN_STEP_SECONDS:
            raise QueryError("invalid_argument", "--min-step cannot be less than 15 seconds")
        if end - start > maximum:
            raise QueryError("invalid_argument", "range query exceeds the configured maximum range")
        if step < minimum_step:
            raise QueryError("invalid_argument", "range query step is below the configured minimum")
        metadata.update({"start": args.start, "end": args.end, "step": args.step})
    return metadata


def build_request(
    endpoint: dict[str, Any], args: argparse.Namespace, metadata: dict[str, Any]
) -> tuple[str, dict[str, str]]:
    parsed = parse_safe_url(endpoint["url"])
    if parsed.scheme != "https" and endpoint.get("tls") != "none":
        raise QueryError("invalid_endpoint", "metrics endpoint requires HTTPS for its declared TLS mode", 3)
    if parsed.scheme == "http" and endpoint.get("auth") == "caller-bearer":
        raise QueryError("invalid_endpoint", "caller-bearer metrics endpoints require HTTPS", 3)
    mode = metadata["mode"]
    path = "/api/v1/query_range" if mode == "range" else "/api/v1/query"
    if not path_supported(endpoint, path):
        raise QueryError("unsupported_operation", f"endpoint does not expose {path}", 3)

    required_parameters = endpoint.get("required_query_parameters", [])
    if "namespace" in required_parameters and not args.namespace:
        raise QueryError(
            "missing_query_parameter",
            "namespace-isolated metrics endpoints require an explicit --namespace",
            3,
        )
    metadata["required_query_parameters"] = sorted(required_parameters)
    if args.namespace is not None:
        metadata["namespace"] = args.namespace

    base_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    params: dict[str, str] = {}
    for key, value in base_pairs:
        if has_sensitive_key(key):
            raise QueryError("invalid_inventory", "endpoint URL contains a sensitive query parameter")
        if key == "namespace":
            raise QueryError(
                "invalid_inventory",
                "namespace must be supplied by the caller, not embedded in the endpoint URL",
            )
        if key in RESERVED_QUERY_PARAMETERS:
            raise QueryError("invalid_inventory", f"endpoint URL must not predefine Prometheus parameter '{key}'")
        if key in params and params[key] != value:
            raise QueryError("invalid_inventory", "endpoint URL contains conflicting query parameters")
        params[key] = value

    provided: dict[str, str] = {}
    if args.namespace is not None:
        if not args.namespace:
            raise QueryError("invalid_argument", "--namespace must not be empty")
        if len(args.namespace) > 63 or not NAMESPACE_PATTERN.fullmatch(args.namespace):
            raise QueryError("invalid_argument", "--namespace must be a valid Kubernetes namespace")
        provided["namespace"] = args.namespace
    for item in args.param:
        key, value = parse_key_value(item)
        if key == "namespace":
            raise QueryError("invalid_argument", "namespace must be supplied with --namespace")
        if key in RESERVED_QUERY_PARAMETERS:
            raise QueryError("invalid_argument", f"--param cannot override Prometheus parameter '{key}'")
        if key in provided and provided[key] != value:
            raise QueryError("invalid_argument", f"conflicting values supplied for --param {key}")
        provided[key] = value
    for key in required_parameters:
        if not isinstance(key, str) or not key:
            raise QueryError("invalid_inventory", "endpoint has an invalid required query parameter")
        if key not in provided and key not in params:
            raise QueryError("missing_query_parameter", f"endpoint requires query parameter '{key}'", 3)
    for key, value in provided.items():
        if key in params and params[key] != value:
            raise QueryError("invalid_argument", f"query parameter '{key}' conflicts with the endpoint URL")
        params[key] = value

    params["query"] = args.query
    if mode == "instant" and args.time is not None:
        params["time"] = args.time
    if mode == "range":
        params.update({"start": args.start, "end": args.end, "step": args.step})

    base_path = parsed.path.rstrip("/")
    request_path = f"{base_path}{path}" if base_path else path
    request_url = urlunsplit((parsed.scheme, parsed.netloc, request_path, urlencode(params), ""))
    headers = {"Accept": "application/json", "User-Agent": "odh-observability-metrics-query/1"}
    auth = endpoint.get("auth")
    if auth not in VALID_AUTH:
        raise QueryError("unsupported_auth", f"unsupported metrics endpoint auth mode: {auth}", 3)
    if auth == "caller-bearer":
        if not args.bearer_token_env:
            raise QueryError("missing_auth", "endpoint requires caller bearer authentication", 3)
        if not isinstance(args.bearer_token_env, str) or not TOKEN_ENV_NAME_PATTERN.fullmatch(
            args.bearer_token_env
        ):
            raise QueryError("invalid_auth", "bearer token environment variable name is invalid", 3)
        if trusted_endpoint_host(endpoint, parsed) is None:
            raise QueryError(
                "invalid_auth",
                "caller-bearer authorization requires a host verified by endpoint discovery",
                3,
            )
        token = os.environ.get(args.bearer_token_env)
        if not token:
            raise QueryError("missing_auth", f"bearer token environment variable '{args.bearer_token_env}' is empty", 3)
        if any(character in token for character in "\r\n"):
            raise QueryError("invalid_auth", "bearer token contains invalid characters", 3)
        headers["Authorization"] = f"Bearer {token}"
    return request_url, headers


def read_limited(response: Any, maximum: int) -> bytes:
    content_length = None
    headers = getattr(response, "headers", None)
    if headers is not None:
        value = headers.get("Content-Length")
        if value:
            try:
                content_length = int(value)
            except ValueError:
                content_length = None
    if content_length is not None and content_length > maximum:
        raise QueryError("response_too_large", "metrics response exceeds the configured size limit", 4)
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = response.read(min(64 * 1024, maximum - total + 1))
        if not chunk:
            break
        total += len(chunk)
        if total > maximum:
            raise QueryError("response_too_large", "metrics response exceeds the configured size limit", 4)
        chunks.append(chunk)
    return b"".join(chunks)


def response_warnings(payload: dict[str, Any]) -> list[str]:
    warnings = payload.get("warnings", [])
    if not isinstance(warnings, list) or any(not isinstance(warning, str) for warning in warnings):
        raise QueryError("invalid_response", "metrics response warnings must be strings", 4)
    return warnings


def validate_prometheus_data(data: Any) -> None:
    if not isinstance(data, dict):
        raise QueryError("invalid_response", "successful metrics response has no data object", 4)
    result_type = data.get("resultType")
    result = data.get("result")
    if result_type not in {"matrix", "vector", "scalar", "string"}:
        raise QueryError("invalid_response", "metrics response has an invalid result type", 4)
    if not isinstance(result, list):
        raise QueryError("invalid_response", "metrics response result must be a list", 4)

    def valid_sample(sample: Any) -> bool:
        return (
            isinstance(sample, list)
            and len(sample) == 2
            and isinstance(sample[0], (int, float))
            and not isinstance(sample[0], bool)
            and math.isfinite(sample[0])
            and isinstance(sample[1], str)
        )

    if result_type in {"scalar", "string"}:
        if not valid_sample(result):
            raise QueryError("invalid_response", "scalar and string results must contain one valid sample", 4)
        return
    for item in result:
        if not isinstance(item, dict) or not isinstance(item.get("metric"), dict):
            raise QueryError("invalid_response", "vector and matrix results must include metric objects", 4)
        if result_type == "vector":
            if not valid_sample(item.get("value")):
                raise QueryError("invalid_response", "vector results must include valid value samples", 4)
        elif not isinstance(item.get("values"), list) or any(
            not valid_sample(sample) for sample in item["values"]
        ):
            raise QueryError("invalid_response", "matrix results must include valid values samples", 4)


def request_metrics(
    request_url: str,
    headers: dict[str, str],
    args: argparse.Namespace,
    endpoint: dict[str, Any] | None = None,
) -> dict[str, Any]:
    authorization_key = next(
        (key for key in headers if key.lower() == "authorization"), None
    )
    if authorization_key is not None:
        try:
            request_host = urlsplit(request_url).hostname
        except ValueError as error:
            raise QueryError("invalid_auth", "authorization target URL is malformed", 3) from error
        trusted_host = None
        if endpoint is not None:
            trusted_host = trusted_endpoint_host(endpoint, parse_safe_url(endpoint.get("url")))
        if trusted_host is None or request_host != trusted_host:
            raise QueryError(
                "invalid_auth",
                "caller-bearer authorization target is not trusted",
                3,
            )
    try:
        context = ssl.create_default_context()
        if args.ca_file:
            context.load_verify_locations(cafile=args.ca_file)
    except (OSError, ssl.SSLError) as error:
        raise QueryError("tls_error", f"could not configure TLS verification: {error}", 4) from error
    try:
        request = Request(request_url, headers=headers, method="GET")
        opener = build_opener(NoRedirectHandler(), HTTPSHandler(context=context))
        with opener.open(request, timeout=args.timeout) as response:
            raw = read_limited(response, args.max_response_bytes)
    except HTTPError as error:
        message = str(error.reason)
        warnings: list[str] = []
        try:
            payload = json.loads(read_limited(error, args.max_response_bytes))
            if isinstance(payload, dict):
                message = str(payload.get("error") or message)
                warnings = response_warnings(payload)
        except (
            QueryError,
            HTTPException,
            URLError,
            TimeoutError,
            OSError,
            ValueError,
            UnicodeDecodeError,
            json.JSONDecodeError,
        ):
            pass
        raise QueryError(
            "http_error",
            f"metrics endpoint returned HTTP {error.code}: {message}",
            4,
            warnings,
        ) from error
    except (HTTPException, URLError, TimeoutError, OSError, ValueError) as error:
        raise QueryError("transport_error", f"metrics request failed: {error}", 4) from error
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise QueryError("invalid_response", "metrics endpoint returned invalid JSON", 4) from error
    if not isinstance(payload, dict) or payload.get("status") not in {"success", "error"}:
        raise QueryError("invalid_response", "metrics endpoint returned an invalid Prometheus response", 4)
    if payload["status"] == "error":
        message = payload.get("error") or "Prometheus returned an unspecified query error"
        error_type = payload.get("errorType") or "query_error"
        raise QueryError(str(error_type), str(message), 4, response_warnings(payload))
    try:
        validate_prometheus_data(payload.get("data"))
    except QueryError as error:
        error.warnings = response_warnings(payload)
        raise
    return payload


def endpoint_metadata(endpoint: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": endpoint["name"],
        "signal": endpoint["signal"],
        "scope": endpoint["scope"],
        "access": endpoint.get("access"),
        "auth": endpoint["auth"],
        "tls": endpoint["tls"],
        "state": endpoint["state"],
        "source": endpoint["source"],
        "url": endpoint["url"],
    }


def emit(payload: dict[str, Any], pretty: bool) -> None:
    print(json.dumps(payload, indent=2 if pretty else None, sort_keys=True, separators=None if pretty else (",", ":")))


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", required=True, help="validated discovery inventory JSON, or - for stdin")
    parser.add_argument("--query", required=True, help="PromQL expression")
    parser.add_argument("--endpoint", help="exact ready metrics endpoint name")
    parser.add_argument("--scope", help="filter ready metrics endpoints by discovery scope")
    parser.add_argument("--namespace", help="namespace value for namespace-isolated endpoints")
    parser.add_argument("--param", action="append", default=[], help="additional required query parameter KEY=VALUE")
    parser.add_argument("--time", help="instant query evaluation time as Unix seconds or RFC3339")
    parser.add_argument("--start", help="range query start as Unix seconds or RFC3339")
    parser.add_argument("--end", help="range query end as Unix seconds or RFC3339")
    parser.add_argument("--step", help="range query step, for example 1m")
    parser.add_argument("--max-range", default="24h", help="maximum range duration (default: 24h)")
    parser.add_argument("--min-step", default="15s", help="minimum range step (default: 15s)")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="request timeout seconds")
    parser.add_argument("--max-response-bytes", type=int, default=DEFAULT_MAX_RESPONSE_BYTES)
    parser.add_argument("--bearer-token-env", help="environment variable containing an already-provided bearer token")
    parser.add_argument("--ca-file", help="additional CA bundle for endpoint TLS verification")
    parser.add_argument("--pretty", action="store_true", help="pretty-print JSON")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    endpoint: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    try:
        if not args.query or len(args.query) > MAX_QUERY_LENGTH or "\x00" in args.query:
            raise QueryError("invalid_argument", "--query must be non-empty and within the maximum length")
        if not math.isfinite(args.timeout) or args.timeout <= 0 or args.timeout > 120:
            raise QueryError("invalid_argument", "--timeout must be greater than zero and no more than 120 seconds")
        if args.max_response_bytes <= 0 or args.max_response_bytes > DEFAULT_MAX_RESPONSE_BYTES:
            raise QueryError(
                "invalid_argument",
                "--max-response-bytes must be greater than zero and no more than 5 MiB",
            )
        range_options = [args.start, args.end, args.step]
        if any(option is not None for option in range_options) and not all(option is not None for option in range_options):
            raise QueryError("invalid_argument", "--start, --end, and --step must be supplied together")
        mode = "range" if args.start is not None else "instant"
        if mode == "range" and args.time is not None:
            raise QueryError("invalid_argument", "--time cannot be combined with a range query")
        metadata = query_metadata(args, mode)
        inventory = load_inventory(args.inventory)
        endpoint = select_endpoint(inventory, args.endpoint, args.scope)
        request_url, headers = build_request(endpoint, args, metadata)
        response = request_metrics(
            request_url,
            headers,
            args,
            endpoint=endpoint,
        )
        payload = {
            "schema_version": SCHEMA_VERSION,
            "status": "success",
            "endpoint": endpoint_metadata(endpoint),
            "query": metadata,
            "data": response["data"],
            "warnings": response.get("warnings", []),
        }
        emit(payload, args.pretty)
        return 0
    except QueryError as error:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "status": "error",
            "endpoint": endpoint_metadata(endpoint) if endpoint else None,
            "query": metadata,
            "error": {"type": error.kind, "message": error.message},
            "warnings": error.warnings,
        }
        emit(payload, args.pretty)
        return error.exit_code


if __name__ == "__main__":
    sys.exit(main())
