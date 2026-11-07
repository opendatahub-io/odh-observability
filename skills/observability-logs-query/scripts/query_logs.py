#!/usr/bin/env python3
"""Run bounded, read-only Loki log queries from a discovery inventory."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import ssl
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener


sys.dont_write_bytecode = True

DISCOVERY_SCRIPTS = Path(__file__).resolve().parents[2] / "observability-endpoint-discovery" / "scripts"
if str(DISCOVERY_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(DISCOVERY_SCRIPTS))

from validate_inventory import validate_inventory as validate_discovery_inventory  # noqa: E402


SCHEMA_VERSION = "1"
APPLICATION_TENANT = "application"
MAX_RANGE_SECONDS = 24 * 60 * 60
DEFAULT_TIMEOUT_SECONDS = 20.0
MAX_TIMEOUT_SECONDS = 120.0
DEFAULT_MAX_RESPONSE_BYTES = 5 * 1024 * 1024
MAX_QUERY_LENGTH = 16 * 1024
MAX_RESULT_LIMIT = 500
DEFAULT_RESULT_LIMIT = 100
MAX_PARAMETER_COUNT = 32
MAX_PARAMETER_LENGTH = 8192
MAX_WARNING_COUNT = 100
MAX_WARNING_LENGTH = 2048
MAX_ERROR_MESSAGE_LENGTH = 2048
NANOSECONDS_PER_SECOND = 1_000_000_000
VALID_AUTH = {"caller-bearer"}
QUERY_RANGE_PATH = "/loki/api/v1/query_range"
RAW_LOKI_PORTS = {3100}
RESERVED_QUERY_PARAMETERS = {
    "query",
    "start",
    "end",
    "limit",
    "direction",
    "since",
    "step",
    "interval",
}
SENSITIVE_QUERY_MARKERS = (
    "password",
    "passwd",
    "apikey",
    "secret",
    "token",
    "credential",
    "privatekey",
)
HEADER_NAME_PATTERN = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
DURATION_PATTERN = re.compile(r"(?P<value>[0-9]+(?:\.[0-9]+)?)(?P<unit>ms|s|m|h|d|w|y)")


class QueryError(RuntimeError):
    """A safe, user-facing query failure."""

    def __init__(
        self,
        kind: str,
        message: str,
        exit_code: int = 2,
        warnings: list[str] | None = None,
        endpoint: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.message = message
        self.exit_code = exit_code
        self.warnings = warnings or []
        self.endpoint = endpoint


class NoRedirectHandler(HTTPRedirectHandler):
    """Prevent bearer tokens from being sent to an untrusted redirect target."""

    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        raise QueryError("redirect_not_allowed", "Loki endpoint returned a redirect", 4)


def has_sensitive_key(key: str) -> bool:
    normalized = "".join(character for character in key.lower() if character.isalnum())
    return any(marker in normalized for marker in SENSITIVE_QUERY_MARKERS)


def parse_safe_url(value: Any, *, reject_raw_loki_ports: bool = False) -> Any:
    if not isinstance(value, str) or not value:
        raise QueryError("invalid_inventory", "logs endpoint URL must be a non-empty string")
    try:
        parsed = urlsplit(value)
        parsed.port
    except (TypeError, ValueError) as error:
        raise QueryError("invalid_inventory", "logs endpoint URL is malformed") from error
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise QueryError("invalid_inventory", "logs endpoint URL must use HTTP or HTTPS")
    if parsed.username or parsed.password or parsed.fragment:
        raise QueryError("invalid_inventory", "logs endpoint URL contains credentials or a fragment")
    if any(has_sensitive_key(key) for key, _ in parse_qsl(parsed.query, keep_blank_values=True)):
        raise QueryError("invalid_inventory", "logs endpoint URL contains a sensitive query parameter")
    if any(marker in value.lower() for marker in ("bearer", "token=")):
        raise QueryError("invalid_inventory", "logs endpoint URL appears to contain credentials")
    if reject_raw_loki_ports and parsed.port in RAW_LOKI_PORTS:
        raise QueryError("invalid_inventory", "raw Loki ports are not logs query endpoints")
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
    warnings = inventory.get("warnings", [])
    if (
        not isinstance(warnings, list)
        or len(warnings) > MAX_WARNING_COUNT
        or any(not isinstance(warning, str) or len(warning) > MAX_WARNING_LENGTH for warning in warnings)
    ):
        raise QueryError("invalid_inventory", "inventory warnings must be a bounded list of strings")
    error = validate_discovery_inventory(inventory)
    if error:
        raise QueryError("invalid_inventory", error)
    for endpoint in inventory["query_endpoints"]:
        if endpoint.get("url") is not None:
            parse_safe_url(endpoint["url"])
    return inventory


def merge_warnings(*warning_lists: list[str]) -> list[str]:
    merged: list[str] = []
    for warnings in warning_lists:
        for warning in warnings:
            if warning not in merged:
                merged.append(warning)
    return merged[:MAX_WARNING_COUNT]


def path_supported(endpoint: dict[str, Any], path: str) -> bool:
    return any(str(candidate).split("?", 1)[0].rstrip("/") == path for candidate in endpoint["paths"])


def select_endpoint(
    inventory: dict[str, Any], endpoint_name: str | None
) -> dict[str, Any]:
    endpoints = [
        endpoint
        for endpoint in inventory["query_endpoints"]
        if endpoint.get("signal") == "logs"
        and endpoint.get("scope") == APPLICATION_TENANT
        and endpoint.get("tenant") == APPLICATION_TENANT
        and endpoint.get("auth") == "caller-bearer"
        and endpoint.get("tls") == "service-ca"
        and endpoint.get("state") == "ready"
        and endpoint.get("url")
        and path_supported(endpoint, QUERY_RANGE_PATH)
    ]
    if endpoint_name:
        matches = [endpoint for endpoint in inventory["query_endpoints"] if endpoint.get("name") == endpoint_name]
        if not matches:
            raise QueryError("endpoint_not_found", f"logs endpoint '{endpoint_name}' was not found", 3)
        if len(matches) > 1:
            raise QueryError(
                "ambiguous_endpoint",
                f"multiple endpoints are named '{endpoint_name}'; inventory names must be unique",
                3,
            )
        endpoint = matches[0]
        if endpoint.get("signal") != "logs":
            raise QueryError(
                "invalid_endpoint", f"endpoint '{endpoint_name}' is not a logs endpoint", 3, endpoint=endpoint
            )
        if endpoint.get("scope") != APPLICATION_TENANT or endpoint.get("tenant") != APPLICATION_TENANT:
            raise QueryError(
                "invalid_endpoint",
                f"endpoint '{endpoint_name}' is not an application-tenant logs endpoint",
                3,
                endpoint=endpoint,
            )
        if endpoint.get("auth") != "caller-bearer" or endpoint.get("tls") != "service-ca":
            raise QueryError(
                "invalid_endpoint",
                f"endpoint '{endpoint_name}' does not provide the caller-bearer/service-ca application logs lane",
                3,
                endpoint=endpoint,
            )
        if endpoint.get("state") != "ready":
            raise QueryError(
                "endpoint_not_ready",
                f"endpoint '{endpoint_name}' is {endpoint.get('state')}, not ready",
                3,
                endpoint=endpoint,
            )
        if not endpoint.get("url") or not path_supported(endpoint, QUERY_RANGE_PATH):
            raise QueryError(
                "endpoint_not_queryable",
                f"endpoint '{endpoint_name}' does not expose the Loki query_range API",
                3,
                endpoint=endpoint,
            )
        endpoints = [endpoint]
    if not endpoints:
        details: list[str] = []
        for endpoint in inventory["query_endpoints"]:
            if endpoint.get("signal") == "logs":
                details.append(f"{endpoint.get('name')} is {endpoint.get('state')}")
        for item in inventory.get("unavailable", []):
            if isinstance(item, dict) and item.get("component") == "logs":
                details.append(str(item.get("reason") or "logs component unavailable"))
        conditions = (inventory.get("monitoring") or {}).get("conditions") or {}
        for condition_name in ("LokiStackAvailable", "ClusterLogForwarderAvailable"):
            condition = conditions.get(condition_name) or {}
            if condition.get("status") == "False" and (condition.get("message") or condition.get("reason")):
                details.append(f"{condition_name}: {condition.get('message') or condition.get('reason')}")
        suffix = f" ({'; '.join(details)})" if details else ""
        raise QueryError("no_ready_endpoint", f"no ready Loki logs query endpoint is available{suffix}", 3)
    if len(endpoints) > 1:
        names = ", ".join(str(endpoint.get("name")) for endpoint in endpoints)
        raise QueryError("ambiguous_endpoint", f"multiple ready logs endpoints match; choose --endpoint: {names}", 3)
    return endpoints[0]


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
        raise QueryError("invalid_argument", f"{option} must be a non-negative finite timestamp")
    return timestamp


def format_timestamp_nanoseconds(timestamp: float, *, round_up: bool = False) -> str:
    scaled = timestamp * NANOSECONDS_PER_SECOND
    if not math.isfinite(scaled):
        raise QueryError("invalid_argument", "timestamp is too large to serialize")
    rounded = math.ceil(scaled) if round_up else math.floor(scaled)
    return str(rounded)


def parse_duration(value: str, option: str) -> float:
    if not isinstance(value, str) or not value:
        raise QueryError("invalid_argument", f"{option} must not be empty")
    position = 0
    seconds = 0.0
    units = {
        "ms": 0.001,
        "s": 1.0,
        "m": 60.0,
        "h": 3600.0,
        "d": 86400.0,
        "w": 604800.0,
        "y": 31536000.0,
    }
    for match in DURATION_PATTERN.finditer(value):
        if match.start() != position:
            raise QueryError("invalid_argument", f"{option} is not a valid duration")
        seconds += float(match.group("value")) * units[match.group("unit")]
        position = match.end()
    if position != len(value) or seconds <= 0 or not math.isfinite(seconds):
        raise QueryError("invalid_argument", f"{option} is not a valid duration")
    return seconds


def validate_query(value: str) -> str:
    if not isinstance(value, str) or not value or len(value) > MAX_QUERY_LENGTH or "\x00" in value:
        raise QueryError("invalid_argument", "--query must be non-empty and within the maximum length")
    if any(character in value for character in "\r\n"):
        raise QueryError("invalid_argument", "--query must not contain control characters")
    return value


def parse_key_value(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise QueryError("invalid_argument", "--param values must use KEY=VALUE")
    key, item = value.split("=", 1)
    if not key or has_sensitive_key(key):
        raise QueryError("invalid_argument", "sensitive or empty query parameter names are not allowed")
    if any(character in value for character in "\x00\r\n"):
        raise QueryError("invalid_argument", "--param values must not contain control characters")
    return key, item


def parse_parameters(values: list[str]) -> dict[str, str]:
    if len(values) > MAX_PARAMETER_COUNT:
        raise QueryError("invalid_argument", f"at most {MAX_PARAMETER_COUNT} --param values are allowed")
    parameters: dict[str, str] = {}
    encoded_length = 0
    for value in values:
        key, item = parse_key_value(value)
        if key in RESERVED_QUERY_PARAMETERS:
            raise QueryError("invalid_argument", f"--param cannot override Loki parameter '{key}'")
        if key in parameters and parameters[key] != item:
            raise QueryError("invalid_argument", f"conflicting values supplied for --param {key}")
        parameters[key] = item
        encoded_length += len(key) + len(item) + 2
    if encoded_length > MAX_PARAMETER_LENGTH:
        raise QueryError("invalid_argument", "combined --param values exceed the maximum length")
    return parameters


def query_metadata(args: argparse.Namespace) -> dict[str, Any]:
    query = validate_query(args.query)
    if args.start is None or args.end is None:
        raise QueryError("invalid_argument", "log searches require --start and --end")
    start = parse_timestamp(args.start, "--start")
    end = parse_timestamp(args.end, "--end")
    maximum = parse_duration(args.max_range, "--max-range")
    if maximum > MAX_RANGE_SECONDS:
        raise QueryError("invalid_argument", "--max-range cannot exceed 24 hours")
    if end <= start:
        raise QueryError("invalid_argument", "--end must be greater than --start")
    if end - start > maximum or end - start > MAX_RANGE_SECONDS:
        raise QueryError("invalid_argument", "log search exceeds the configured maximum range")
    if args.limit < 1 or args.limit > MAX_RESULT_LIMIT:
        raise QueryError("invalid_argument", f"--limit must be between 1 and {MAX_RESULT_LIMIT}")
    if args.direction not in {"forward", "backward"}:
        raise QueryError("invalid_argument", "--direction must be forward or backward")
    metadata: dict[str, Any] = {
        "mode": "range",
        "logql": query,
        "start": args.start,
        "end": args.end,
        "range_seconds": end - start,
        "limit": args.limit,
        "direction": args.direction,
    }
    parameters = parse_parameters(args.param)
    if parameters:
        metadata["parameters"] = parameters
    return metadata


def parse_required_headers(endpoint: dict[str, Any]) -> dict[str, str]:
    if endpoint.get("scope") != APPLICATION_TENANT:
        raise QueryError("invalid_inventory", "logs endpoint scope must be application")
    raw_headers = endpoint.get("required_headers", [])
    if not isinstance(raw_headers, list) or any(not isinstance(item, str) for item in raw_headers):
        raise QueryError("invalid_inventory", "logs endpoint required_headers must contain strings")
    headers: dict[str, str] = {}
    for item in raw_headers:
        if ":" not in item:
            raise QueryError("invalid_inventory", "logs endpoint required headers must use NAME: VALUE")
        name, value = item.split(":", 1)
        name, value = name.strip(), value.strip()
        if not HEADER_NAME_PATTERN.fullmatch(name) or not value:
            raise QueryError("invalid_inventory", "logs endpoint contains an invalid required header")
        if name.lower() in {"authorization", "cookie", "proxy-authorization"}:
            raise QueryError("invalid_inventory", "logs endpoint must not provide credential headers")
        if any(character in value for character in "\r\n"):
            raise QueryError("invalid_inventory", "logs endpoint header contains control characters")
        existing = next((key for key in headers if key.lower() == name.lower()), None)
        if existing and headers[existing] != value:
            raise QueryError("invalid_inventory", "logs endpoint contains conflicting required headers")
        headers[name] = value
        if name.lower() != "x-scope-orgid":
            raise QueryError("invalid_inventory", "logs endpoints may only provide X-Scope-OrgID")
    tenant = endpoint.get("tenant")
    if tenant != APPLICATION_TENANT:
        raise QueryError(
            "invalid_inventory",
            f"logs endpoint tenant must be '{APPLICATION_TENANT}'",
        )
    scope_header = next((key for key in headers if key.lower() == "x-scope-orgid"), None)
    if scope_header is None or headers[scope_header] != APPLICATION_TENANT:
        raise QueryError(
            "invalid_inventory",
            f"logs endpoint must provide X-Scope-OrgID: {APPLICATION_TENANT}",
        )
    return headers


def build_request(
    endpoint: dict[str, Any], args: argparse.Namespace
) -> tuple[str, dict[str, str]]:
    parsed = parse_safe_url(endpoint["url"], reject_raw_loki_ports=True)
    if parsed.scheme != "https":
        raise QueryError("invalid_endpoint", "application logs endpoints require HTTPS", 3)
    if endpoint.get("tls") != "service-ca":
        raise QueryError("invalid_endpoint", "application logs endpoints require service-ca TLS", 3)
    if not args.ca_file:
        raise QueryError("missing_ca", "service-ca logs endpoints require --ca-file", 3)
    if not path_supported(endpoint, QUERY_RANGE_PATH):
        raise QueryError("unsupported_operation", f"endpoint does not expose {QUERY_RANGE_PATH}", 3)

    params: dict[str, str] = {}
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if has_sensitive_key(key):
            raise QueryError("invalid_inventory", "endpoint URL contains a sensitive query parameter")
        if key in RESERVED_QUERY_PARAMETERS:
            raise QueryError("invalid_inventory", f"endpoint URL must not predefine Loki parameter '{key}'")
        if key in params and params[key] != value:
            raise QueryError("invalid_inventory", "endpoint URL contains conflicting query parameters")
        params[key] = value

    required_parameters = endpoint.get("required_query_parameters", [])
    if not isinstance(required_parameters, list) or any(
        not isinstance(parameter, str) or not parameter for parameter in required_parameters
    ):
        raise QueryError("invalid_inventory", "logs endpoint has invalid required query parameters")
    provided_parameters = parse_parameters(args.param)
    for key, value in provided_parameters.items():
        if key in params and params[key] != value:
            raise QueryError("invalid_argument", f"query parameter '{key}' conflicts with the endpoint URL")
        params[key] = value
    for key in required_parameters:
        if key not in params:
            raise QueryError("missing_query_parameter", f"endpoint requires query parameter '{key}'", 3)

    start = format_timestamp_nanoseconds(parse_timestamp(args.start, "--start"))
    end = format_timestamp_nanoseconds(parse_timestamp(args.end, "--end"), round_up=True)
    params.update(
        {
            "query": validate_query(args.query),
            "start": start,
            "end": end,
            "limit": str(args.limit),
            "direction": args.direction,
        }
    )
    base_path = parsed.path.rstrip("/")
    request_url = urlunsplit(
        (parsed.scheme, parsed.netloc, f"{base_path}{QUERY_RANGE_PATH}", urlencode(params), "")
    )
    headers = {"Accept": "application/json", "User-Agent": "odh-observability-logs-query/1"}
    headers.update(parse_required_headers(endpoint))
    auth = endpoint.get("auth")
    if auth not in VALID_AUTH:
        raise QueryError("unsupported_auth", f"unsupported logs endpoint auth mode: {auth}", 3)
    if auth == "caller-bearer":
        if not args.bearer_token_env:
            raise QueryError("missing_auth", "endpoint requires caller bearer authentication", 3)
        token = os.environ.get(args.bearer_token_env)
        if not token:
            raise QueryError(
                "missing_auth",
                f"bearer token environment variable '{args.bearer_token_env}' is empty",
                3,
            )
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
        raise QueryError("response_too_large", "Loki response exceeds the configured size limit", 4)
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = response.read(min(64 * 1024, maximum - total + 1))
        if not chunk:
            break
        total += len(chunk)
        if total > maximum:
            raise QueryError("response_too_large", "Loki response exceeds the configured size limit", 4)
        chunks.append(chunk)
    return b"".join(chunks)


def response_warnings(payload: dict[str, Any]) -> list[str]:
    warnings = payload.get("warnings", [])
    if (
        not isinstance(warnings, list)
        or len(warnings) > MAX_WARNING_COUNT
        or any(not isinstance(warning, str) or len(warning) > MAX_WARNING_LENGTH for warning in warnings)
    ):
        raise QueryError("invalid_response", "Loki response warnings must be strings", 4)
    return warnings


def response_message(value: Any) -> str:
    message = str(value)
    if len(message) > MAX_ERROR_MESSAGE_LENGTH:
        return message[:MAX_ERROR_MESSAGE_LENGTH] + "..."
    return message


def valid_log_value(value: Any) -> bool:
    return (
        isinstance(value, list)
        and len(value) in {2, 3}
        and isinstance(value[0], (str, int, float))
        and not isinstance(value[0], bool)
        and (not isinstance(value[0], float) or math.isfinite(value[0]))
        and isinstance(value[1], str)
        and (len(value) == 2 or isinstance(value[2], dict))
    )


def validate_loki_data(data: Any) -> None:
    if not isinstance(data, dict):
        raise QueryError("invalid_response", "successful Loki response must include a data object", 4)
    result_type = data.get("resultType")
    result = data.get("result")
    if result_type not in {"streams", "matrix"} or not isinstance(result, list):
        raise QueryError("invalid_response", "Loki response must include streams or matrix result data", 4)
    for item in result:
        if not isinstance(item, dict):
            raise QueryError("invalid_response", "Loki result entries must be objects", 4)
        label_key = "stream" if result_type == "streams" else "metric"
        if not isinstance(item.get(label_key), dict):
            raise QueryError("invalid_response", f"Loki {result_type} entries must include {label_key} objects", 4)
        if not isinstance(item.get("values"), list) or any(
            not valid_log_value(value) for value in item["values"]
        ):
            raise QueryError("invalid_response", "Loki result entries must include valid values", 4)


def validate_loki_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise QueryError("invalid_response", "Loki endpoint returned a non-object JSON response", 4)
    warnings = response_warnings(payload)
    if payload.get("status") == "error":
        raise QueryError(
            "loki_error",
            response_message(payload.get("error") or "Loki returned an unspecified error"),
            4,
            warnings,
        )
    if payload.get("status") != "success":
        raise QueryError("invalid_response", "Loki response status must be success", 4, warnings)
    data = payload.get("data")
    validate_loki_data(data)
    return {"data": data, "warnings": warnings}


def request_loki(
    request_url: str, headers: dict[str, str], args: argparse.Namespace
) -> dict[str, Any]:
    try:
        context = ssl.create_default_context()
        if args.ca_file:
            context.load_verify_locations(cafile=args.ca_file)
    except (OSError, ssl.SSLError) as error:
        raise QueryError("tls_error", f"could not configure TLS verification: {error}", 4) from error
    request = Request(request_url, headers=headers, method="GET")
    try:
        opener = build_opener(NoRedirectHandler(), HTTPSHandler(context=context))
        with opener.open(request, timeout=args.timeout) as response:
            raw = read_limited(response, args.max_response_bytes)
    except HTTPError as error:
        message = str(error.reason)
        warnings: list[str] = []
        try:
            payload = json.loads(read_limited(error, args.max_response_bytes))
            if isinstance(payload, dict):
                message = response_message(payload.get("error") or payload.get("message") or message)
                warnings = response_warnings(payload)
        except QueryError as response_error:
            if response_error.kind == "response_too_large":
                raise
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
        raise QueryError("http_error", f"Loki endpoint returned HTTP {error.code}: {message}", 4, warnings) from error
    except (URLError, TimeoutError, OSError) as error:
        raise QueryError("transport_error", f"Loki request failed: {error}", 4) from error
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise QueryError("invalid_response", "Loki endpoint returned invalid JSON", 4) from error
    validated = validate_loki_payload(payload)
    return {"data": validated["data"], "warnings": validated["warnings"]}


def endpoint_metadata(endpoint: dict[str, Any]) -> dict[str, Any]:
    metadata = {
        "name": endpoint["name"],
        "signal": endpoint["signal"],
        "scope": endpoint["scope"],
        "access": endpoint.get("access"),
        "auth": endpoint["auth"],
        "tls": endpoint["tls"],
        "state": endpoint["state"],
        "source": endpoint["source"],
        "url": endpoint.get("url"),
    }
    if endpoint.get("tenant") is not None:
        metadata["tenant"] = endpoint["tenant"]
    required_headers = endpoint.get("required_headers")
    if isinstance(required_headers, list) and all(isinstance(item, str) for item in required_headers):
        metadata["required_headers"] = [item.split(":", 1)[0].strip() for item in required_headers]
    required_parameters = endpoint.get("required_query_parameters")
    if isinstance(required_parameters, list) and all(isinstance(item, str) for item in required_parameters):
        metadata["required_query_parameters"] = list(required_parameters)
    return metadata


def emit(payload: dict[str, Any], pretty: bool) -> None:
    print(json.dumps(payload, indent=2 if pretty else None, sort_keys=True, separators=None if pretty else (",", ":")))


class JsonArgumentParser(argparse.ArgumentParser):
    """Keep argparse failures in the skill's structured result contract."""

    def error(self, message: str) -> None:
        emit(
            {
                "schema_version": SCHEMA_VERSION,
                "status": "error",
                "endpoint": None,
                "query": None,
                "error": {"type": "invalid_argument", "message": message},
                "warnings": [],
            },
            pretty=False,
        )
        raise SystemExit(2)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = JsonArgumentParser(description=__doc__)
    parser.add_argument("--inventory", required=True, help="validated discovery inventory JSON, or - for stdin")
    parser.add_argument("--endpoint", help="exact ready logs endpoint name")
    parser.add_argument("--query", required=True, help="LogQL query expression")
    parser.add_argument("--start", required=True, help="range start as Unix seconds or RFC3339")
    parser.add_argument("--end", required=True, help="range end as Unix seconds or RFC3339")
    parser.add_argument("--limit", type=int, default=DEFAULT_RESULT_LIMIT, help="maximum log entries to return")
    parser.add_argument("--direction", choices=("forward", "backward"), default="backward")
    parser.add_argument("--max-range", default="24h", help="maximum search window (default: 24h)")
    parser.add_argument("--param", action="append", default=[], help="additional endpoint parameter KEY=VALUE")
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
    discovery_warnings: list[str] = []
    try:
        if not math.isfinite(args.timeout) or args.timeout <= 0 or args.timeout > MAX_TIMEOUT_SECONDS:
            raise QueryError("invalid_argument", "--timeout must be greater than zero and no more than 120 seconds")
        if args.max_response_bytes <= 0 or args.max_response_bytes > DEFAULT_MAX_RESPONSE_BYTES:
            raise QueryError(
                "invalid_argument",
                "--max-response-bytes must be greater than zero and no more than 5 MiB",
            )
        metadata = query_metadata(args)
        inventory = load_inventory(args.inventory)
        discovery_warnings = inventory.get("warnings", [])
        endpoint = select_endpoint(inventory, args.endpoint)
        request_url, headers = build_request(endpoint, args)
        response = request_loki(request_url, headers, args)
        payload = {
            "schema_version": SCHEMA_VERSION,
            "status": "success",
            "endpoint": endpoint_metadata(endpoint),
            "query": metadata,
            "data": response["data"],
            "warnings": merge_warnings(discovery_warnings, response["warnings"]),
        }
        emit(payload, args.pretty)
        return 0
    except QueryError as error:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "status": "error",
            "endpoint": (
                endpoint_metadata(error.endpoint or endpoint)
                if (error.endpoint or endpoint)
                else None
            ),
            "query": metadata,
            "error": {"type": error.kind, "message": error.message},
            "warnings": merge_warnings(discovery_warnings, error.warnings),
        }
        emit(payload, args.pretty)
        return error.exit_code


if __name__ == "__main__":
    sys.exit(main())
