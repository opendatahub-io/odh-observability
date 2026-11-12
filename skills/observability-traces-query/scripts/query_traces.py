#!/usr/bin/env python3
"""Run bounded, read-only Tempo trace queries from a discovery inventory."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import signal
import ssl
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from http.client import HTTPException
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener


sys.dont_write_bytecode = True

DISCOVERY_SCRIPTS = Path(__file__).resolve().parents[2] / "observability-endpoint-discovery" / "scripts"
if str(DISCOVERY_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(DISCOVERY_SCRIPTS))

import discover_endpoints  # noqa: E402
from validate_inventory import validate_inventory as validate_discovery_inventory  # noqa: E402


SCHEMA_VERSION = "1"
MAX_RANGE_SECONDS = 24 * 60 * 60
DEFAULT_TIMEOUT_SECONDS = 20.0
MAX_TIMEOUT_SECONDS = 120.0
DEFAULT_MAX_RESPONSE_BYTES = 5 * 1024 * 1024
MAX_SEARCH_LIMIT = 500
DEFAULT_SEARCH_LIMIT = 100
MAX_FILTER_LENGTH = 2048
MAX_TAG_COUNT = 32
MAX_TAG_QUERY_LENGTH = 8192
MAX_PARAMETER_COUNT = 32
MAX_PARAMETER_QUERY_LENGTH = 8192
MAX_WARNING_COUNT = 100
MAX_WARNING_LENGTH = 2048
MAX_ERROR_MESSAGE_LENGTH = 2048
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
VERIFIED_ENDPOINT_FIELDS = (
    "name", "signal", "url", "access", "scope", "auth", "tls", "paths",
    "required_query_parameters", "required_headers", "tenant", "state", "source",
    "route_admitted", "target_service", "service_port",
)
SEARCH_PATH = "/api/search"
TRACE_PATH_TEMPLATE = "/api/traces/<trace-id>"
TRACE_ID_PATTERN = re.compile(r"^(?:[0-9a-fA-F]{16}|[0-9a-fA-F]{32})$")
HEADER_NAME_PATTERN = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
DURATION_PATTERN = re.compile(r"(?P<value>[0-9]+(?:\.[0-9]+)?)(?P<unit>ns|us|µs|ms|s|m|h)")
TAG_KEY_PATTERN = re.compile(r"^[A-Za-z0-9_.:-]+$")
RESERVED_QUERY_PARAMETERS = {
    "start",
    "end",
    "limit",
    "q",
    "tags",
    "minDuration",
    "maxDuration",
    "status",
}


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
        raise QueryError("redirect_not_allowed", "Tempo endpoint returned a redirect", 4)


def has_sensitive_key(key: str) -> bool:
    normalized = "".join(character for character in key.lower() if character.isalnum())
    return any(marker in normalized for marker in SENSITIVE_QUERY_MARKERS)


def parse_safe_url(value: Any, *, reject_raw_trace_ports: bool = False) -> Any:
    if not isinstance(value, str) or not value:
        raise QueryError("invalid_inventory", "trace endpoint URL must be a non-empty string")
    try:
        parsed = urlsplit(value)
        parsed.port
    except (TypeError, ValueError) as error:
        raise QueryError("invalid_inventory", "trace endpoint URL is malformed") from error
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise QueryError("invalid_inventory", "trace endpoint URL must use HTTP or HTTPS")
    if parsed.username or parsed.password or parsed.fragment:
        raise QueryError("invalid_inventory", "trace endpoint URL contains credentials or a fragment")
    if any(has_sensitive_key(key) for key, _ in parse_qsl(parsed.query, keep_blank_values=True)):
        raise QueryError("invalid_inventory", "trace endpoint URL contains a sensitive query parameter")
    if any(marker in value.lower() for marker in ("bearer", "token=")):
        raise QueryError("invalid_inventory", "trace endpoint URL appears to contain credentials")
    # Tempo's gateway is the only supported trace query lane. These are
    # ingestion or raw Tempo ports and must not be accepted for a selected
    # trace endpoint, but unrelated query endpoints may use other ports.
    if reject_raw_trace_ports and parsed.port in {3200, 4317, 4318}:
        raise QueryError("invalid_inventory", "raw Tempo or OTLP ports are not trace query endpoints")
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


def trace_path_supported(endpoint: dict[str, Any]) -> bool:
    return any(
        str(candidate).split("?", 1)[0].rstrip("/") == TRACE_PATH_TEMPLATE
        for candidate in endpoint["paths"]
    )


def verify_endpoint(endpoint: dict[str, Any]) -> None:
    """Bind the selected endpoint to fresh discovery before reading credentials."""
    try:
        discovered = discover_endpoints.discover()
        error = validate_discovery_inventory(discovered)
    except Exception as error:
        raise QueryError("invalid_auth", "could not verify endpoint with discovery", 3) from error
    if error:
        raise QueryError("invalid_auth", "could not verify endpoint with discovery", 3)
    matches = [item for item in discovered["query_endpoints"] if item.get("name") == endpoint.get("name")]
    if (
        len(matches) != 1
        or matches[0].get("signal") != "traces"
        or matches[0].get("state") != "ready"
        or matches[0].get("auth") not in VALID_AUTH
        or matches[0].get("access") not in {"route", "cluster-service"}
        or any(matches[0].get(field) != endpoint.get(field) for field in VERIFIED_ENDPOINT_FIELDS)
    ):
        raise QueryError("invalid_auth", "trace endpoint does not match current discovery evidence", 3)


def select_endpoint(
    inventory: dict[str, Any], endpoint_name: str | None, mode: str
) -> dict[str, Any]:
    required_path = SEARCH_PATH if mode == "search" else TRACE_PATH_TEMPLATE
    endpoints = [
        endpoint
        for endpoint in inventory["query_endpoints"]
        if endpoint.get("signal") == "traces"
        and endpoint.get("state") == "ready"
        and endpoint.get("url")
        and (path_supported(endpoint, required_path) if mode == "search" else trace_path_supported(endpoint))
    ]
    if endpoint_name:
        matches = [endpoint for endpoint in inventory["query_endpoints"] if endpoint.get("name") == endpoint_name]
        if not matches:
            raise QueryError("endpoint_not_found", f"trace endpoint '{endpoint_name}' was not found", 3)
        if len(matches) != 1:
            raise QueryError("ambiguous_endpoint", f"multiple endpoints named '{endpoint_name}'", 3)
        endpoint = matches[0]
        if endpoint.get("signal") != "traces":
            raise QueryError("invalid_endpoint", f"endpoint '{endpoint_name}' is not a traces endpoint", 3)
        if endpoint.get("state") != "ready":
            raise QueryError(
                "endpoint_not_ready",
                f"endpoint '{endpoint_name}' is {endpoint.get('state')}, not ready",
                3,
                endpoint=endpoint,
            )
        if not endpoint.get("url") or not (
            path_supported(endpoint, required_path) if mode == "search" else trace_path_supported(endpoint)
        ):
            raise QueryError(
                "endpoint_not_queryable",
                f"endpoint '{endpoint_name}' does not expose the Tempo {mode} API",
                3,
                endpoint=endpoint,
            )
        endpoints = [endpoint]
    if not endpoints:
        details: list[str] = []
        for endpoint in inventory["query_endpoints"]:
            if endpoint.get("signal") == "traces":
                details.append(f"{endpoint.get('name')} is {endpoint.get('state')}")
        for item in inventory.get("unavailable", []):
            if isinstance(item, dict) and item.get("component") == "traces":
                details.append(str(item.get("reason") or "trace component unavailable"))
        monitoring = inventory.get("monitoring")
        conditions = monitoring.get("conditions") if isinstance(monitoring, dict) else None
        if not isinstance(conditions, dict):
            conditions = {}
        for condition_name in ("TempoAvailable", "InstrumentationAvailable"):
            condition = conditions.get(condition_name)
            if isinstance(condition, dict) and condition.get("status") == "False" and (condition.get("message") or condition.get("reason")):
                details.append(
                    f"{condition_name}: {condition.get('message') or condition.get('reason')}"
                )
        suffix = f" ({'; '.join(details)})" if details else ""
        raise QueryError("no_ready_endpoint", f"no ready Tempo trace query endpoint is available{suffix}", 3)
    if len(endpoints) > 1:
        names = ", ".join(str(endpoint.get("name")) for endpoint in endpoints)
        raise QueryError("ambiguous_endpoint", f"multiple ready trace endpoints match; choose --endpoint: {names}", 3)
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
        raise QueryError("invalid_argument", f"{option} must not be negative")
    return timestamp


def format_timestamp(timestamp: float, *, round_up: bool = False) -> str:
    rounded = math.ceil(timestamp) if round_up else math.floor(timestamp)
    return str(rounded)


def parse_duration(value: str, option: str) -> float:
    if not isinstance(value, str) or not value:
        raise QueryError("invalid_argument", f"{option} must not be empty")
    position = 0
    seconds = 0.0
    units = {
        "ns": 1e-9,
        "us": 1e-6,
        "µs": 1e-6,
        "ms": 0.001,
        "s": 1.0,
        "m": 60.0,
        "h": 3600.0,
    }
    for match in DURATION_PATTERN.finditer(value):
        if match.start() != position:
            raise QueryError("invalid_argument", f"{option} is not a valid duration")
        seconds += float(match.group("value")) * units[match.group("unit")]
        position = match.end()
    if position != len(value) or seconds <= 0 or not math.isfinite(seconds):
        raise QueryError("invalid_argument", f"{option} is not a valid duration")
    if seconds > MAX_RANGE_SECONDS:
        raise QueryError("invalid_argument", f"{option} cannot exceed 24 hours")
    return seconds


def validate_filter_value(value: str, option: str) -> str:
    if not isinstance(value, str) or not value or len(value) > MAX_FILTER_LENGTH or "\x00" in value:
        raise QueryError("invalid_argument", f"{option} must be non-empty and within the maximum length")
    if any(character in value for character in "\r\n"):
        raise QueryError("invalid_argument", f"{option} must not contain control characters")
    return value


def parse_key_value(value: str, option: str) -> tuple[str, str]:
    if "=" not in value:
        raise QueryError("invalid_argument", f"{option} values must use KEY=VALUE")
    key, item = value.split("=", 1)
    validate_filter_value(key, f"{option} key")
    validate_filter_value(item, f"{option} value")
    if has_sensitive_key(key):
        raise QueryError("invalid_argument", f"sensitive {option} keys are not allowed")
    return key, item


def parse_tag(value: str) -> tuple[str, str]:
    key, item = parse_key_value(value, "--tag")
    if not TAG_KEY_PATTERN.fullmatch(key):
        raise QueryError("invalid_argument", "--tag keys must contain only letters, digits, '.', ':', '_' or '-'")
    return key, item


def parse_tags(values: list[str]) -> list[tuple[str, str]]:
    if len(values) > MAX_TAG_COUNT:
        raise QueryError("invalid_argument", f"at most {MAX_TAG_COUNT} --tag values are allowed")
    tags = [parse_tag(value) for value in values]
    encoded_length = sum(len(encode_logfmt_pair(key, value)) + 1 for key, value in tags)
    if encoded_length > MAX_TAG_QUERY_LENGTH:
        raise QueryError("invalid_argument", "combined --tag values exceed the maximum length")
    return tags


def parse_parameters(values: list[str]) -> dict[str, str]:
    if len(values) > MAX_PARAMETER_COUNT:
        raise QueryError("invalid_argument", f"at most {MAX_PARAMETER_COUNT} --param values are allowed")
    parameters: dict[str, str] = {}
    encoded_length = 0
    for value in values:
        key, item = parse_key_value(value, "--param")
        if key in RESERVED_QUERY_PARAMETERS:
            raise QueryError("invalid_argument", f"--param cannot override Tempo parameter '{key}'")
        if key in parameters and parameters[key] != item:
            raise QueryError("invalid_argument", f"conflicting values supplied for --param {key}")
        parameters[key] = item
        encoded_length += len(key) + len(item) + 2
    if encoded_length > MAX_PARAMETER_QUERY_LENGTH:
        raise QueryError("invalid_argument", "combined --param values exceed the maximum length")
    return parameters


def validate_tag_component(value: str, option: str) -> str:
    return validate_filter_value(value, option)


def encode_logfmt_pair(key: str, value: str) -> str:
    if value and not any(character.isspace() or character in {'"', "\\"} for character in value):
        return f"{key}={value}"
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'{key}="{escaped}"'


def traceql_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def traceql_filter_safe(query: str) -> bool:
    """Allow one simple spanset, ignoring operators and braces inside strings."""
    depth = 0
    brace_count = 0
    quote = None
    escaped = False
    for index, character in enumerate(query):
        if quote:
            if escaped:
                escaped = False
            elif character == "\\" and quote == '"':
                escaped = True
            elif character == quote:
                quote = None
            continue
        if character in {'"', '`'}:
            quote = character
        elif character == "|":
            return False
        elif character == "{":
            brace_count += 1
            depth += 1
            if depth != 1 or brace_count != 1 or index != 0:
                return False
        elif character == "}":
            depth -= 1
            if depth != 0 or index != len(query) - 1:
                return False
    return not quote and depth == 0


def build_traceql_query(args: argparse.Namespace) -> str | None:
    if args.status is not None and args.query is not None:
        raise QueryError("invalid_argument", "--status cannot be combined with --query")
    if args.query is None and args.status is None:
        return None
    conditions: list[str] = []
    if args.service is not None:
        conditions.append(
            f"resource.service.name = {traceql_string(validate_tag_component(args.service, '--service'))}"
        )
    if args.operation is not None:
        conditions.append(f"name = {traceql_string(validate_tag_component(args.operation, '--operation'))}")
    if args.status is not None:
        status = validate_filter_value(args.status, "--status").lower()
        if status not in {"ok", "error", "unset"}:
            raise QueryError("invalid_argument", "--status must be one of ok, error, or unset")
        conditions.insert(0, f"status = {status}")
    if args.query is not None:
        query = validate_filter_value(args.query, "--query")
        if not conditions:
            return query
        if not traceql_filter_safe(query):
            raise QueryError(
                "invalid_argument", "--service and --operation cannot be combined with a compound --query"
            )
        if query.startswith("{") and query.endswith("}"):
            inner = query[1:-1].strip()
            return "{ " + " && ".join([inner, *conditions]) + " }"
        return f"({query}) && {{ {' && '.join(conditions)} }}"
    if not conditions:
        return None
    return "{ " + " && ".join(conditions) + " }"


def query_metadata(args: argparse.Namespace, mode: str) -> dict[str, Any]:
    parameters = parse_parameters(args.param)
    if mode == "trace":
        if not TRACE_ID_PATTERN.fullmatch(args.trace_id or ""):
            raise QueryError("invalid_argument", "--trace-id must be 16 or 32 hexadecimal characters")
        metadata: dict[str, Any] = {"mode": "trace", "trace_id": args.trace_id.lower()}
        if parameters:
            metadata["parameters"] = parameters
        return metadata

    if args.start is None or args.end is None:
        raise QueryError("invalid_argument", "trace searches require --start and --end")
    start = parse_timestamp(args.start, "--start")
    end = parse_timestamp(args.end, "--end")
    maximum = parse_duration(args.max_range, "--max-range")
    if end <= start:
        raise QueryError("invalid_argument", "--end must be greater than --start")
    if end - start > maximum or end - start > MAX_RANGE_SECONDS:
        raise QueryError("invalid_argument", "trace search exceeds the configured maximum range")
    if args.limit < 1 or args.limit > MAX_SEARCH_LIMIT:
        raise QueryError("invalid_argument", f"--limit must be between 1 and {MAX_SEARCH_LIMIT}")

    filters: dict[str, Any] = {"limit": args.limit}
    if args.query is not None:
        filters["query"] = validate_filter_value(args.query, "--query")
    if args.service is not None:
        filters["service"] = validate_tag_component(args.service, "--service")
    if args.operation is not None:
        filters["operation"] = validate_tag_component(args.operation, "--operation")
    if args.status is not None:
        if args.query is not None:
            raise QueryError("invalid_argument", "--status cannot be combined with --query")
        status = validate_filter_value(args.status, "--status").lower()
        if status not in {"ok", "error", "unset"}:
            raise QueryError("invalid_argument", "--status must be one of ok, error, or unset")
        filters["status"] = status
    tags = parse_tags(args.tag)
    if tags:
        filters["tags"] = [{"key": key, "value": value} for key, value in tags]
    min_duration = None
    if args.min_duration is not None:
        min_duration = parse_duration(args.min_duration, "--min-duration")
        filters["min_duration"] = args.min_duration
    max_duration = None
    if args.max_duration is not None:
        max_duration = parse_duration(args.max_duration, "--max-duration")
        filters["max_duration"] = args.max_duration
    if min_duration is not None and max_duration is not None and min_duration > max_duration:
        raise QueryError("invalid_argument", "--min-duration must not exceed --max-duration")
    metadata = {
        "mode": "search",
        "start": args.start,
        "end": args.end,
        "range_seconds": end - start,
        "filters": filters,
    }
    if parameters:
        metadata["parameters"] = parameters
    return metadata


def parse_required_headers(endpoint: dict[str, Any]) -> dict[str, str]:
    raw_headers = endpoint.get("required_headers", [])
    if not isinstance(raw_headers, list) or any(not isinstance(item, str) for item in raw_headers):
        raise QueryError("invalid_inventory", "trace endpoint required_headers must contain strings")
    headers: dict[str, str] = {}
    for item in raw_headers:
        if ":" not in item:
            raise QueryError("invalid_inventory", "trace endpoint required headers must use NAME: VALUE")
        name, value = item.split(":", 1)
        name, value = name.strip(), value.strip()
        if not HEADER_NAME_PATTERN.fullmatch(name) or not value:
            raise QueryError("invalid_inventory", "trace endpoint contains an invalid required header")
        if name.lower() in {"authorization", "cookie", "proxy-authorization"}:
            raise QueryError("invalid_inventory", "trace endpoint must not provide credential headers")
        if any(character in value for character in "\r\n"):
            raise QueryError("invalid_inventory", "trace endpoint header contains control characters")
        existing = next((key for key in headers if key.lower() == name.lower()), None)
        if existing and headers[existing] != value:
            raise QueryError("invalid_inventory", "trace endpoint contains conflicting required headers")
        headers[name] = value
        if name.lower() != "x-scope-orgid":
            raise QueryError(
                "invalid_inventory",
                "trace endpoints may only provide the X-Scope-OrgID required header",
            )
    tenant = endpoint.get("tenant")
    if not isinstance(tenant, str) or not tenant or len(tenant) > 253:
        raise QueryError("invalid_inventory", "trace endpoint tenant is required and invalid")
    scope_header = next((key for key in headers if key.lower() == "x-scope-orgid"), None)
    if scope_header is None or headers[scope_header] != tenant:
        raise QueryError("invalid_inventory", "trace endpoint tenant does not match X-Scope-OrgID")
    return headers


def build_request(
    endpoint: dict[str, Any], args: argparse.Namespace, metadata: dict[str, Any]
) -> tuple[str, dict[str, str]]:
    parsed = parse_safe_url(endpoint["url"], reject_raw_trace_ports=True)
    if parsed.scheme != "https" or endpoint.get("tls") == "none":
        raise QueryError("invalid_endpoint", "Tempo trace endpoints must use HTTPS with TLS verification", 3)
    tenant = endpoint.get("tenant")
    if not isinstance(tenant, str) or not tenant or len(tenant) > 253:
        raise QueryError("invalid_inventory", "trace endpoint tenant is required and invalid")
    expected_base_path = f"/api/traces/v1/{tenant}/tempo"
    if parsed.path.rstrip("/") != expected_base_path:
        raise QueryError(
            "invalid_endpoint",
            "trace endpoint URL must contain the discovered Tempo tenant query base path",
            3,
        )
    mode = metadata["mode"]
    endpoint_path = SEARCH_PATH if mode == "search" else TRACE_PATH_TEMPLATE
    if not (path_supported(endpoint, SEARCH_PATH) if mode == "search" else trace_path_supported(endpoint)):
        raise QueryError("unsupported_operation", f"endpoint does not expose {endpoint_path}", 3)

    params: dict[str, str] = {}
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if has_sensitive_key(key):
            raise QueryError("invalid_inventory", "endpoint URL contains a sensitive query parameter")
        if key in RESERVED_QUERY_PARAMETERS:
            raise QueryError("invalid_inventory", f"endpoint URL must not predefine Tempo parameter '{key}'")
        if key in params and params[key] != value:
            raise QueryError("invalid_inventory", "endpoint URL contains conflicting query parameters")
        params[key] = value

    required_parameters = endpoint.get("required_query_parameters", [])
    if not isinstance(required_parameters, list) or any(
        not isinstance(parameter, str) or not parameter for parameter in required_parameters
    ):
        raise QueryError("invalid_inventory", "trace endpoint has invalid required query parameters")
    provided_parameters = parse_parameters(args.param)
    for key, value in provided_parameters.items():
        if key in params and params[key] != value:
            raise QueryError("invalid_argument", f"query parameter '{key}' conflicts with the endpoint URL")
        params[key] = value
    for key in required_parameters:
        if key not in params:
            raise QueryError("missing_query_parameter", f"endpoint requires query parameter '{key}'", 3)

    if mode == "search":
        start = format_timestamp(parse_timestamp(args.start, "--start"))
        end = format_timestamp(parse_timestamp(args.end, "--end"), round_up=True)
        params.update({"start": start, "end": end, "limit": str(args.limit)})
        traceql_query = build_traceql_query(args)
        if traceql_query is not None:
            params["q"] = traceql_query
        tag_pairs = []
        if traceql_query is None:
            if args.service is not None:
                tag_pairs.append(encode_logfmt_pair("service.name", validate_tag_component(args.service, "--service")))
            if args.operation is not None:
                tag_pairs.append(encode_logfmt_pair("name", validate_tag_component(args.operation, "--operation")))
        tag_pairs.extend(encode_logfmt_pair(key, value) for key, value in parse_tags(args.tag))
        if tag_pairs:
            params["tags"] = " ".join(tag_pairs)
        if args.min_duration is not None:
            parse_duration(args.min_duration, "--min-duration")
            params["minDuration"] = args.min_duration
        if args.max_duration is not None:
            parse_duration(args.max_duration, "--max-duration")
            params["maxDuration"] = args.max_duration
        request_path = SEARCH_PATH
    else:
        trace_id = args.trace_id.lower()
        if not TRACE_ID_PATTERN.fullmatch(trace_id):
            raise QueryError("invalid_argument", "--trace-id must be 16 or 32 hexadecimal characters")
        request_path = f"/api/traces/{trace_id}"

    base_path = parsed.path.rstrip("/")
    request_url = urlunsplit(
        (parsed.scheme, parsed.netloc, f"{base_path}{request_path}", urlencode(params), "")
    )
    headers = {"Accept": "application/json", "User-Agent": "odh-observability-traces-query/1"}
    headers.update(parse_required_headers(endpoint))
    auth = endpoint.get("auth")
    if auth not in VALID_AUTH:
        raise QueryError("unsupported_auth", f"unsupported trace endpoint auth mode: {auth}", 3)
    if auth == "caller-bearer":
        if not args.bearer_token_env:
            raise QueryError("missing_auth", "endpoint requires caller bearer authentication", 3)
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
        raise QueryError("response_too_large", "Tempo response exceeds the configured size limit", 4)
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = response.read(min(64 * 1024, maximum - total + 1))
        if not chunk:
            break
        total += len(chunk)
        if total > maximum:
            raise QueryError("response_too_large", "Tempo response exceeds the configured size limit", 4)
        chunks.append(chunk)
    return b"".join(chunks)


@contextmanager
def request_deadline(timeout: float) -> Iterator[None]:
    """Bound the entire request, including slow headers and response bodies."""
    if not hasattr(signal, "setitimer") or not hasattr(signal, "SIGALRM"):
        raise QueryError("transport_error", "request deadlines require POSIX interval timers", 4)
    previous_handler = signal.getsignal(signal.SIGALRM)
    if any(signal.getitimer(signal.ITIMER_REAL)):
        raise QueryError("transport_error", "cannot use request deadline while another timer is active", 4)

    def expire(signum: int, frame: Any) -> None:
        raise TimeoutError("Tempo request deadline exceeded")

    try:
        signal.signal(signal.SIGALRM, expire)
    except ValueError as error:
        raise QueryError("transport_error", "request deadlines require the main thread", 4) from error
    try:
        signal.setitimer(signal.ITIMER_REAL, timeout)
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def response_warnings(payload: dict[str, Any]) -> list[str]:
    warnings = payload.get("warnings", [])
    if (
        not isinstance(warnings, list)
        or len(warnings) > MAX_WARNING_COUNT
        or any(not isinstance(warning, str) or len(warning) > MAX_WARNING_LENGTH for warning in warnings)
    ):
        raise QueryError("invalid_response", "Tempo response warnings must be strings", 4)
    return warnings


def response_message(value: Any) -> str:
    message = str(value)
    if len(message) > MAX_ERROR_MESSAGE_LENGTH:
        return message[:MAX_ERROR_MESSAGE_LENGTH] + "..."
    return message


def validate_tempo_data(data: Any, mode: str) -> None:
    if not isinstance(data, dict):
        raise QueryError("invalid_response", "successful Tempo response must be a JSON object", 4)
    if mode == "search":
        traces = data.get("traces")
        if not isinstance(traces, list) or any(not isinstance(trace, dict) for trace in traces):
            raise QueryError("invalid_response", "Tempo search response must include a traces list", 4)
    else:
        batches = data.get("batches")
        if not isinstance(batches, list) or any(not isinstance(batch, dict) for batch in batches):
            raise QueryError("invalid_response", "Tempo trace response must include a batches list", 4)


def request_tempo(
    request_url: str, headers: dict[str, str], args: argparse.Namespace, mode: str
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
        with request_deadline(args.timeout):
            try:
                with opener.open(request, timeout=args.timeout) as response:
                    raw = read_limited(response, args.max_response_bytes)
            except HTTPError as error:
                message = str(error.reason)
                warnings: list[str] = []
                try:
                    with error:
                        payload = json.loads(read_limited(error, args.max_response_bytes))
                    if isinstance(payload, dict):
                        message = response_message(payload.get("error") or payload.get("message") or message)
                        warnings = response_warnings(payload)
                except (QueryError, UnicodeDecodeError, json.JSONDecodeError):
                    pass
                raise QueryError("http_error", f"Tempo endpoint returned HTTP {error.code}: {message}", 4, warnings) from error
    except (HTTPException, URLError, TimeoutError, OSError) as error:
        raise QueryError("transport_error", f"Tempo request failed: {error}", 4) from error
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise QueryError("invalid_response", "Tempo endpoint returned invalid JSON", 4) from error
    if not isinstance(payload, dict):
        raise QueryError("invalid_response", "Tempo endpoint returned a non-object JSON response", 4)
    if payload.get("status") == "error":
        raise QueryError(
            "tempo_error",
            response_message(payload.get("error") or "Tempo returned an unspecified error"),
            4,
            response_warnings(payload),
        )
    response_warnings(payload)
    validate_tempo_data(payload, mode)
    return payload


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
    parser.add_argument("--endpoint", help="exact ready trace endpoint name")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--trace-id", help="16- or 32-character hexadecimal Tempo trace ID")
    mode.add_argument("--search", action="store_true", help="search traces; requires --start and --end")
    parser.add_argument("--start", help="search start as Unix seconds or RFC3339")
    parser.add_argument("--end", help="search end as Unix seconds or RFC3339")
    parser.add_argument("--query", help="Tempo TraceQL search expression")
    parser.add_argument("--service", help="service.name search filter")
    parser.add_argument("--operation", help="span name search filter")
    parser.add_argument("--status", help="trace status: ok, error, or unset")
    parser.add_argument("--tag", action="append", default=[], help="Tempo tag filter KEY=VALUE; may be repeated")
    parser.add_argument("--param", action="append", default=[], help="additional endpoint parameter KEY=VALUE")
    parser.add_argument("--min-duration", help="minimum trace duration, for example 100ms")
    parser.add_argument("--max-duration", help="maximum trace duration, for example 5s")
    parser.add_argument(
        "--limit", type=int, default=DEFAULT_SEARCH_LIMIT, help="maximum traces to return (default: 100)"
    )
    parser.add_argument("--max-range", default="24h", help="maximum search window (default: 24h)")
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
        if not math.isfinite(args.timeout) or args.timeout <= 0 or args.timeout > MAX_TIMEOUT_SECONDS:
            raise QueryError("invalid_argument", "--timeout must be greater than zero and no more than 120 seconds")
        if args.max_response_bytes <= 0 or args.max_response_bytes > DEFAULT_MAX_RESPONSE_BYTES:
            raise QueryError(
                "invalid_argument",
                "--max-response-bytes must be greater than zero and no more than 5 MiB",
            )
        if args.search:
            mode = "search"
        else:
            mode = "trace"
            trace_options = (
                args.start,
                args.end,
                args.query,
                args.service,
                args.operation,
                args.status,
                args.min_duration,
                args.max_duration,
            )
            if any(value is not None for value in trace_options) or args.tag:
                raise QueryError("invalid_argument", "trace lookup options cannot be combined with --trace-id")
        metadata = query_metadata(args, mode)
        inventory = load_inventory(args.inventory)
        endpoint = select_endpoint(inventory, args.endpoint, mode)
        verify_endpoint(endpoint)
        request_url, headers = build_request(endpoint, args, metadata)
        response = request_tempo(request_url, headers, args, mode)
        payload = {
            "schema_version": SCHEMA_VERSION,
            "status": "success",
            "endpoint": endpoint_metadata(endpoint),
            "query": metadata,
            "data": response,
            "warnings": response.get("warnings", []),
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
            "warnings": error.warnings,
        }
        emit(payload, args.pretty)
        return error.exit_code


if __name__ == "__main__":
    sys.exit(main())
