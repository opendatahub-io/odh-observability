#!/usr/bin/env python3
"""Validate the structured result emitted by query_metrics.py."""

from __future__ import annotations

import json
import math
import sys
from urllib.parse import parse_qsl, urlsplit


VALID_RESULT_TYPES = {"matrix", "vector", "scalar", "string"}


def fail(message: str) -> int:
    print(f"metrics result validation failed: {message}", file=sys.stderr)
    return 1


def validate_endpoint(endpoint: object) -> str | None:
    if endpoint is None:
        return None
    if not isinstance(endpoint, dict):
        return "endpoint must be an object or null"
    for field in ("name", "signal", "scope", "auth", "tls", "state", "source"):
        if not endpoint.get(field):
            return f"endpoint is missing {field}"
    if endpoint.get("signal") != "metrics":
        return "endpoint signal must be metrics"
    if endpoint.get("state") != "ready":
        return "endpoint state must be ready"
    if endpoint.get("auth") not in {"none", "caller-bearer"}:
        return "endpoint has an unsupported auth mode"
    url = endpoint.get("url")
    if not isinstance(url, str) or not url:
        return "endpoint URL must be a non-empty string"
    try:
        parsed = urlsplit(url)
        parsed.port
    except ValueError:
        return "endpoint URL is malformed"
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return "endpoint URL must use HTTP or HTTPS"
    if parsed.username or parsed.password or parsed.fragment:
        return "endpoint URL contains credentials or a fragment"
    sensitive = {"password", "passwd", "apikey", "secret", "token", "credential", "privatekey"}
    for key, _ in parse_qsl(parsed.query, keep_blank_values=True):
        normalized = "".join(character for character in key.lower() if character.isalnum())
        if any(marker in normalized for marker in sensitive):
            return "endpoint URL contains a sensitive query parameter"
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
    endpoint_error = validate_endpoint(result.get("endpoint"))
    if endpoint_error:
        return fail(endpoint_error)
    if status == "success" and result.get("endpoint") is None:
        return fail("successful result must include endpoint metadata")
    query = result.get("query")
    if query is not None and (
        not isinstance(query, dict)
        or query.get("mode") not in {"instant", "range"}
        or not isinstance(query.get("promql"), str)
        or not query["promql"]
    ):
        return fail("query must be null or include a valid mode and PromQL")
    if status == "success" and query is None:
        return fail("successful result must include query metadata")
    if not isinstance(result.get("warnings"), list) or any(
        not isinstance(warning, str) for warning in result["warnings"]
    ):
        return fail("warnings must be a list")
    if status == "success":
        data = result.get("data")
        if not isinstance(data, dict):
            return fail("successful result must include a data object")
        result_type = data.get("resultType")
        values = data.get("result")
        if result_type not in VALID_RESULT_TYPES or not isinstance(values, list):
            return fail("successful result must include a valid resultType and result list")

        def valid_sample(sample: object) -> bool:
            return (
                isinstance(sample, list)
                and len(sample) == 2
                and isinstance(sample[0], (int, float))
                and not isinstance(sample[0], bool)
                and math.isfinite(sample[0])
                and isinstance(sample[1], str)
            )

        if result_type in {"scalar", "string"}:
            if not valid_sample(values):
                return fail("scalar and string results must contain one valid sample")
        else:
            for item in values:
                if not isinstance(item, dict) or not isinstance(item.get("metric"), dict):
                    return fail("vector and matrix results must include metric objects")
                if result_type == "vector":
                    if not valid_sample(item.get("value")):
                        return fail("vector results must include valid value samples")
                elif not isinstance(item.get("values"), list) or any(
                    not valid_sample(sample) for sample in item["values"]
                ):
                    return fail("matrix results must include valid values samples")
        if "error" in result:
            return fail("successful result must not include error")
    else:
        error = result.get("error")
        if not isinstance(error, dict) or not error.get("type") or not error.get("message"):
            return fail("error result must include a typed error")
    print("metrics result validation passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
