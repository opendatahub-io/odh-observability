#!/usr/bin/env python3
"""Validate the structured result emitted by query_traces.py."""

from __future__ import annotations

import json
import math
import sys
from urllib.parse import parse_qsl, urlsplit


TRACE_ID_LENGTHS = {16, 32}
SENSITIVE_QUERY_MARKERS = {"password", "passwd", "apikey", "secret", "token", "credential", "privatekey"}
MAX_WARNING_COUNT = 100
MAX_WARNING_LENGTH = 2048
MAX_ERROR_MESSAGE_LENGTH = 2048


def fail(message: str) -> int:
    print(f"traces result validation failed: {message}", file=sys.stderr)
    return 1


def validate_endpoint(endpoint: object, *, require_ready: bool) -> str | None:
    if endpoint is None:
        return None
    if not isinstance(endpoint, dict):
        return "endpoint must be an object or null"
    for field in ("name", "signal", "scope", "auth", "tls", "state", "source"):
        if not endpoint.get(field):
            return f"endpoint is missing {field}"
    if endpoint.get("signal") != "traces":
        return "endpoint signal must be traces"
    if require_ready and endpoint.get("state") != "ready":
        return "endpoint state must be ready"
    if endpoint.get("auth") not in {"none", "caller-bearer"}:
        return "endpoint has an unsupported auth mode"
    if endpoint.get("tls") == "none":
        return "trace endpoint must use TLS"
    url = endpoint.get("url")
    if url is None and not require_ready:
        url = ""
    if not isinstance(url, str) or (not url and require_ready):
        return "endpoint URL must be a non-empty string"
    if not url:
        return None
    try:
        parsed = urlsplit(url)
        parsed.port
    except ValueError:
        return "endpoint URL is malformed"
    if parsed.scheme != "https" or not parsed.hostname:
        return "trace endpoint URL must use HTTPS"
    if parsed.username or parsed.password or parsed.fragment:
        return "endpoint URL contains credentials or a fragment"
    for key, _ in parse_qsl(parsed.query, keep_blank_values=True):
        normalized = "".join(character for character in key.lower() if character.isalnum())
        if any(marker in normalized for marker in SENSITIVE_QUERY_MARKERS):
            return "endpoint URL contains a sensitive query parameter"
    if parsed.port in {3200, 4317, 4318}:
        return "raw Tempo or OTLP ports are not trace query endpoints"
    tenant = endpoint.get("tenant")
    if not isinstance(tenant, str) or not tenant:
        return "trace endpoint must include a tenant"
    if parsed.path.rstrip("/") != f"/api/traces/v1/{tenant}/tempo":
        return "trace endpoint URL must contain the discovered Tempo tenant query base path"
    required_headers = endpoint.get("required_headers", [])
    if not isinstance(required_headers, list) or any(not isinstance(header, str) for header in required_headers):
        return "endpoint required_headers must be a list of names"
    if {header.lower() for header in required_headers} != {"x-scope-orgid"}:
        return "trace endpoint required_headers must contain only X-Scope-OrgID"
    return None


def validate_query(query: object) -> str | None:
    if not isinstance(query, dict):
        return "query must be an object"
    mode = query.get("mode")
    if mode == "trace":
        trace_id = query.get("trace_id")
        if not isinstance(trace_id, str) or len(trace_id) not in TRACE_ID_LENGTHS:
            return "trace query must include a 16- or 32-character trace_id"
        if any(character not in "0123456789abcdefABCDEF" for character in trace_id):
            return "trace_id must be hexadecimal"
        parameters = query.get("parameters", {})
        if not isinstance(parameters, dict) or any(
            not isinstance(key, str) or not isinstance(value, str) for key, value in parameters.items()
        ):
            return "trace query parameters must be string key/value pairs"
        return None
    if mode != "search":
        return "query mode must be search or trace"
    for field in ("start", "end"):
        if not isinstance(query.get(field), str) or not query[field]:
            return f"search query must include {field}"
    if (
        not isinstance(query.get("range_seconds"), (int, float))
        or isinstance(query["range_seconds"], bool)
        or not math.isfinite(query["range_seconds"])
        or query["range_seconds"] <= 0
        or query["range_seconds"] > 24 * 60 * 60
    ):
        return "search query must include a positive range_seconds"
    filters = query.get("filters")
    if (
        not isinstance(filters, dict)
        or not isinstance(filters.get("limit"), int)
        or isinstance(filters["limit"], bool)
    ):
        return "search query must include filters.limit"
    if filters["limit"] < 1 or filters["limit"] > 500:
        return "search query limit is outside the allowed range"
    parameters = query.get("parameters", {})
    if not isinstance(parameters, dict) or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in parameters.items()
    ):
        return "search query parameters must be string key/value pairs"
    return None


def main() -> int:
    try:
        result = json.load(sys.stdin)
    except json.JSONDecodeError as error:
        return fail(f"invalid JSON: {error}")
    if not isinstance(result, dict):
        return fail("top-level value must be an object")
    if result.get("schema_version") != "1":
        return fail("schema_version must be '1'")
    status = result.get("status")
    if status not in {"success", "error"}:
        return fail("status must be success or error")
    endpoint_error = validate_endpoint(result.get("endpoint"), require_ready=status == "success")
    if endpoint_error:
        return fail(endpoint_error)
    query = result.get("query")
    if query is not None:
        query_error = validate_query(query)
        if query_error:
            return fail(query_error)
    if status == "success" and (result.get("endpoint") is None or query is None):
        return fail("successful result must include endpoint and query metadata")
    if (
        not isinstance(result.get("warnings"), list)
        or len(result["warnings"]) > MAX_WARNING_COUNT
        or any(
            not isinstance(warning, str) or len(warning) > MAX_WARNING_LENGTH
            for warning in result["warnings"]
        )
    ):
        return fail("warnings must be a list")
    if status == "success":
        data = result.get("data")
        if not isinstance(data, dict):
            return fail("successful result must include a data object")
        if query["mode"] == "search":
            if not isinstance(data.get("traces"), list) or any(not isinstance(trace, dict) for trace in data["traces"]):
                return fail("search result must include a traces list of objects")
        elif not isinstance(data.get("batches"), list) or any(
            not isinstance(batch, dict) for batch in data["batches"]
        ):
            return fail("trace result must include a batches list of objects")
        if "error" in result:
            return fail("successful result must not include error")
    else:
        error = result.get("error")
        if (
            not isinstance(error, dict)
            or not isinstance(error.get("type"), str)
            or not isinstance(error.get("message"), str)
            or not error["type"]
            or not error["message"]
            or len(error["message"]) > MAX_ERROR_MESSAGE_LENGTH
        ):
            return fail("error result must include a typed error")
        if "data" in result:
            return fail("error result must not include data")
    print("traces result validation passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
