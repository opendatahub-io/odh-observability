#!/usr/bin/env python3
"""Validate the JSON handoff emitted by discover_endpoints.py."""

from __future__ import annotations

import json
import sys
from urllib.parse import parse_qsl, urlparse


ENDPOINT_COLLECTIONS = ("query_endpoints", "telemetry_ingest", "diagnostic_endpoints")
STATES = {"ready", "configured", "pending", "not-ready", "unavailable"}
SENSITIVE_QUERY_MARKERS = (
    "password",
    "passwd",
    "apikey",
    "secret",
    "token",
    "credential",
    "privatekey",
)


def fail(message: str) -> int:
    print(f"inventory validation failed: {message}", file=sys.stderr)
    return 1


def has_sensitive_query_parameter(query: str) -> bool:
    for key, _ in parse_qsl(query, keep_blank_values=True):
        normalized_key = "".join(character for character in key.lower() if character.isalnum())
        if any(marker in normalized_key for marker in SENSITIVE_QUERY_MARKERS):
            return True
    return False


def validate_endpoint(collection: str, index: int, endpoint: object) -> str | None:
    if not isinstance(endpoint, dict):
        return f"{collection}[{index}] must be an object"
    for field in ("signal", "name", "access", "scope", "auth", "tls", "state", "source"):
        if not endpoint.get(field):
            return f"{collection}[{index}] is missing {field}"
    state = endpoint["state"]
    if state not in STATES:
        return f"{collection}[{index}] has invalid state"
    url = endpoint.get("url")
    if url is not None:
        if not isinstance(url, str):
            return f"{collection}[{index}] URL must be a string or null"
        try:
            parsed = urlparse(url)
            hostname = parsed.hostname
            port = parsed.port
        except (TypeError, ValueError):
            return f"{collection}[{index}] URL is malformed"
        if parsed.scheme not in {"http", "https"}:
            return f"{collection}[{index}] has unsupported URL scheme"
        if not hostname:
            return f"{collection}[{index}] URL has no hostname"
        if parsed.username or parsed.password:
            return f"{collection}[{index}] URL contains credentials"
        if has_sensitive_query_parameter(parsed.query):
            return f"{collection}[{index}] URL contains a sensitive query parameter"
        if any(secret_word in url.lower() for secret_word in ("bearer", "token=")):
            return f"{collection}[{index}] URL appears to contain a credential"
        if collection == "query_endpoints" and (
            port == 10902
            or "thanos-querier-data-science-thanos-querier" in hostname
            or "prometheus-operated" in hostname
        ):
            return f"{collection}[{index}] uses an internal raw metrics backend"
    if state == "ready" and not url:
        return f"{collection}[{index}] is ready but has no URL"
    if not isinstance(endpoint.get("paths"), list):
        return f"{collection}[{index}] paths must be a list"
    return None


def main() -> int:
    try:
        inventory = json.load(sys.stdin)
    except json.JSONDecodeError as error:
        return fail(f"invalid JSON: {error}")
    if not isinstance(inventory, dict):
        return fail("top-level value must be an object")
    if inventory.get("schema_version") != "1":
        return fail("schema_version must be '1'")
    for collection in ENDPOINT_COLLECTIONS:
        if not isinstance(inventory.get(collection), list):
            return fail(f"{collection} must be a list")
        for index, endpoint in enumerate(inventory[collection]):
            error = validate_endpoint(collection, index, endpoint)
            if error:
                return fail(error)
    monitoring = inventory.get("monitoring")
    if not isinstance(monitoring, dict):
        return fail("monitoring must be an object")
    if monitoring.get("management_state") == "Managed" and not monitoring.get("namespace"):
        return fail("managed monitoring must include a namespace")
    print("inventory validation passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
