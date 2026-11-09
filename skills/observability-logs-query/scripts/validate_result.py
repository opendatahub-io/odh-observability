#!/usr/bin/env python3
"""Validate the structured result emitted by query_logs.py."""

from __future__ import annotations

import json
import math
import sys
from urllib.parse import parse_qsl, urlsplit


MAX_RANGE_SECONDS = 24 * 60 * 60
MAX_RESULT_LIMIT = 500
MAX_WARNING_COUNT = 100
MAX_WARNING_LENGTH = 2048
MAX_ERROR_MESSAGE_LENGTH = 2048
APPLICATION_TENANT = "application"
VALID_RESULT_TYPES = {"streams", "matrix"}
SENSITIVE_QUERY_MARKERS = {"password", "passwd", "apikey", "secret", "token", "credential", "privatekey"}


def fail(message: str) -> int:
    print(f"logs result validation failed: {message}", file=sys.stderr)
    return 1


def validate_endpoint(endpoint: object, *, require_ready: bool) -> str | None:
    if endpoint is None:
        return None
    if not isinstance(endpoint, dict):
        return "endpoint must be an object or null"
    for field in ("name", "signal", "scope", "auth", "tls", "state", "source"):
        if not endpoint.get(field):
            return f"endpoint is missing {field}"
    if endpoint.get("signal") != "logs":
        return "endpoint signal must be logs"
    if require_ready and endpoint.get("scope") != APPLICATION_TENANT:
        return "endpoint scope must be application"
    if require_ready and endpoint.get("state") != "ready":
        return "endpoint state must be ready"
    if require_ready and endpoint.get("auth") != "caller-bearer":
        return "logs endpoint must use caller-bearer auth"
    if require_ready and endpoint.get("tls") != "service-ca":
        return "logs endpoint must use service-ca TLS"
    url = endpoint.get("url")
    if url is None and not require_ready:
        return None
    if not isinstance(url, str) or (not url and require_ready):
        return "endpoint URL must be a non-empty string"
    if not url:
        return None
    try:
        parsed = urlsplit(url)
        parsed.port
    except (TypeError, ValueError):
        return "endpoint URL is malformed"
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return "endpoint URL must use HTTP or HTTPS"
    if parsed.username or parsed.password or parsed.fragment:
        return "endpoint URL contains credentials or a fragment"
    for key, _ in parse_qsl(parsed.query, keep_blank_values=True):
        normalized = "".join(character for character in key.lower() if character.isalnum())
        if any(marker in normalized for marker in SENSITIVE_QUERY_MARKERS):
            return "endpoint URL contains a sensitive query parameter"
    tenant = endpoint.get("tenant")
    if require_ready and tenant != APPLICATION_TENANT:
        return "logs endpoint tenant must be application"
    required_headers = endpoint.get("required_headers")
    if not isinstance(required_headers, list) or any(not isinstance(header, str) for header in required_headers):
        return "endpoint required_headers must be a list of names"
    if {header.lower() for header in required_headers} != {"x-scope-orgid"}:
        return "logs endpoint required_headers must contain only X-Scope-OrgID"
    if require_ready and parsed.scheme != "https":
        return "successful logs endpoint URL must use HTTPS"
    required_parameters = endpoint.get("required_query_parameters", [])
    if not isinstance(required_parameters, list) or any(
        not isinstance(parameter, str) or not parameter for parameter in required_parameters
    ):
        return "endpoint required_query_parameters must contain non-empty strings"
    return None


def validate_query(query: object) -> str | None:
    if not isinstance(query, dict):
        return "successful result must include query metadata"
    if query.get("mode") != "range":
        return "query mode must be range"
    if not isinstance(query.get("logql"), str) or not query["logql"]:
        return "query must include a non-empty LogQL expression"
    if not isinstance(query.get("start"), str) or not query["start"]:
        return "query must include a start timestamp"
    if not isinstance(query.get("end"), str) or not query["end"]:
        return "query must include an end timestamp"
    range_seconds = query.get("range_seconds")
    if (
        not isinstance(range_seconds, (int, float))
        or isinstance(range_seconds, bool)
        or not math.isfinite(range_seconds)
        or range_seconds <= 0
        or range_seconds > MAX_RANGE_SECONDS
    ):
        return "query range_seconds must be a positive bounded number"
    limit = query.get("limit")
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= MAX_RESULT_LIMIT:
        return "query limit is outside the allowed range"
    if query.get("direction") not in {"forward", "backward"}:
        return "query direction must be forward or backward"
    parameters = query.get("parameters", {})
    if not isinstance(parameters, dict) or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in parameters.items()
    ):
        return "query parameters must be string key/value pairs"
    return None


def valid_log_value(value: object) -> bool:
    return (
        isinstance(value, list)
        and len(value) in {2, 3}
        and isinstance(value[0], (str, int, float))
        and not isinstance(value[0], bool)
        and (not isinstance(value[0], float) or math.isfinite(value[0]))
        and isinstance(value[1], str)
        and (len(value) == 2 or isinstance(value[2], dict))
    )


def validate_data(data: object) -> str | None:
    if not isinstance(data, dict):
        return "successful result must include a data object"
    result_type = data.get("resultType")
    result = data.get("result")
    if result_type not in VALID_RESULT_TYPES or not isinstance(result, list):
        return "data must include a valid resultType and result list"
    label_key = "stream" if result_type == "streams" else "metric"
    for item in result:
        if not isinstance(item, dict) or not isinstance(item.get(label_key), dict):
            return f"{result_type} results must include {label_key} objects"
        values = item.get("values")
        if not isinstance(values, list) or any(not valid_log_value(value) for value in values):
            return "log results must include valid values"
    return None


def validation_error(result: object) -> str | None:
    if not isinstance(result, dict):
        return "top-level value must be an object"
    if result.get("schema_version") != "1":
        return "schema_version must be '1'"
    status = result.get("status")
    if status not in {"success", "error"}:
        return "status must be success or error"
    endpoint_error = validate_endpoint(result.get("endpoint"), require_ready=status == "success")
    if endpoint_error:
        return endpoint_error
    if status == "success" and result.get("endpoint") is None:
        return "successful result must include endpoint metadata"
    query = result.get("query")
    if query is not None:
        query_error = validate_query(query)
        if query_error:
            return query_error
    if status == "success" and query is None:
        return "successful result must include query metadata"
    warnings = result.get("warnings")
    if (
        not isinstance(warnings, list)
        or len(warnings) > MAX_WARNING_COUNT
        or any(not isinstance(warning, str) or len(warning) > MAX_WARNING_LENGTH for warning in warnings)
    ):
        return "warnings must be a bounded list of strings"
    if status == "success":
        data_error = validate_data(result.get("data"))
        if data_error:
            return data_error
        if "error" in result:
            return "successful result must not include error"
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
            return "error result must include a typed error"
        if "data" in result:
            return "error result must not include data"
    return None


def main() -> int:
    try:
        result = json.load(sys.stdin)
    except json.JSONDecodeError as error:
        return fail(f"invalid JSON: {error}")
    error = validation_error(result)
    if error:
        return fail(error)
    print("logs result validation passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
