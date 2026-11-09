#!/usr/bin/env python3
"""Validate a live logs result and emit only a non-sensitive summary."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import validate_result


def emit_error() -> int:
    print(
        json.dumps(
            {
                "schema_version": "1",
                "status": "error",
                "error": {
                    "type": "live_result_invalid",
                    "message": "live query result failed validation",
                },
            },
            sort_keys=True,
        )
    )
    return 1


def sanitize(payload: object) -> tuple[dict[str, Any] | None, int]:
    if validate_result.validation_error(payload):
        return None, emit_error()
    if not isinstance(payload, dict) or payload.get("status") != "success":
        return None, emit_error()

    endpoint = payload["endpoint"]
    query = payload["query"]
    data = payload["data"]
    if not isinstance(endpoint, dict) or not isinstance(query, dict) or not isinstance(data, dict):
        return None, emit_error()

    results = data["result"]
    value_count = sum(len(item["values"]) for item in results)
    summary = {
        "schema_version": "1",
        "status": "success",
        "endpoint": {
            key: endpoint[key]
            for key in ("name", "signal", "scope", "auth", "tls", "state", "source", "tenant")
        },
        "query": {
            key: query[key]
            for key in ("mode", "start", "end", "range_seconds", "limit", "direction")
        },
        "result": {
            "result_type": data["resultType"],
            "stream_count": len(results),
            "value_count": value_count,
        },
        "warning_count": len(payload["warnings"]),
    }
    return summary, 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    args = parser.parse_args(argv)
    try:
        payload = json.loads(Path(args.input).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError, RecursionError):
        return emit_error()
    summary, status = sanitize(payload)
    if summary is not None:
        print(json.dumps(summary, sort_keys=True))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
