#!/usr/bin/env python3

from __future__ import annotations

import io
import json
import os
import sys
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit


SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts"))
sys.path.insert(0, SCRIPT_DIR)

import query_logs  # noqa: E402
import validate_result  # noqa: E402


def inventory(endpoint: dict | None = None) -> dict:
    return {
        "schema_version": "1",
        "monitoring": {
            "management_state": "Managed",
            "namespace": "redhat-ods-monitoring",
            "conditions": {
                "LokiStackAvailable": {"status": "True"},
                "ClusterLogForwarderAvailable": {"status": "True"},
            },
        },
        "query_endpoints": [
            endpoint
            or {
                "signal": "logs",
                "name": "data-science-loki",
                "url": "https://loki.example.test:8080/api/logs/v1/application",
                "access": "cluster-service",
                "scope": "application",
                "auth": "caller-bearer",
                "tls": "service-ca",
                "paths": [
                    "/loki/api/v1/query",
                    "/loki/api/v1/query_range",
                    "/loki/api/v1/labels",
                ],
                "required_query_parameters": [],
                "required_headers": ["X-Scope-OrgID: application"],
                "tenant": "application",
                "state": "ready",
                "source": "Monitoring/cluster/default-monitoring",
            }
        ],
        "telemetry_ingest": [],
        "diagnostic_endpoints": [],
        "unavailable": [],
    }


def search_args(**overrides: object) -> Namespace:
    values = {
        "query": '{kubernetes_namespace_name="redhat-ods-monitoring"}',
        "start": "2026-01-01T00:00:00Z",
        "end": "2026-01-01T00:05:00Z",
        "max_range": "24h",
        "limit": 100,
        "direction": "backward",
        "param": [],
        "bearer_token_env": "LOGS_TOKEN",
        "ca_file": "provided-service-ca.crt",
        "timeout": 20.0,
        "max_response_bytes": 1024,
    }
    values.update(overrides)
    return Namespace(**values)


class FakeResponse:
    def __init__(self, payload: dict) -> None:
        self.body = json.dumps(payload).encode()
        self.headers = {"Content-Length": str(len(self.body))}

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self.body)
        body, self.body = self.body[:size], self.body[size:]
        return body


class QueryLogsTests(unittest.TestCase):
    def test_selects_ready_logs_endpoint(self) -> None:
        selected = query_logs.select_endpoint(inventory(), None)
        self.assertEqual(selected["name"], "data-science-loki")

    def test_preserves_discovery_warnings_in_success_result(self) -> None:
        data = inventory()
        data["warnings"] = ["NetworkPolicy reachability still requires live validation"]
        with patch("query_logs.request_loki") as request:
            request.return_value = {"data": {"resultType": "streams", "result": []}, "warnings": []}
            args = search_args()
            with patch.dict(os.environ, {"LOGS_TOKEN": "test-token"}):
                with patch("query_logs.load_inventory", return_value=data):
                    with patch("query_logs.select_endpoint", return_value=data["query_endpoints"][0]):
                        with patch("query_logs.build_request", return_value=("https://example.test", {})):
                            output = io.StringIO()
                            with redirect_stdout(output):
                                self.assertEqual(query_logs.main([
                                    "--inventory", "inventory.json",
                                    "--query", args.query,
                                    "--start", args.start,
                                    "--end", args.end,
                                    "--bearer-token-env", "LOGS_TOKEN",
                                ]), 0)
        self.assertEqual(json.loads(output.getvalue())["warnings"], data["warnings"])

    def test_rejects_ambiguous_logs_endpoints(self) -> None:
        second = inventory()["query_endpoints"][0].copy()
        second["name"] = "second-loki"
        data = inventory()
        data["query_endpoints"].append(second)
        with self.assertRaises(query_logs.QueryError) as context:
            query_logs.select_endpoint(data, None)
        self.assertEqual(context.exception.kind, "ambiguous_endpoint")

    def test_builds_bounded_loki_request_with_tenant_and_auth(self) -> None:
        args = search_args()
        with patch.dict(os.environ, {"LOGS_TOKEN": "test-token"}):
            url, headers = query_logs.build_request(inventory()["query_endpoints"][0], args)
        parsed = urlsplit(url)
        self.assertEqual(parsed.path, "/api/logs/v1/application/loki/api/v1/query_range")
        params = parse_qs(parsed.query)
        self.assertEqual(params["query"], [args.query])
        self.assertEqual(params["start"], ["1767225600000000000"])
        self.assertEqual(params["end"], ["1767225900000000000"])
        self.assertEqual(params["limit"], ["100"])
        self.assertEqual(params["direction"], ["backward"])
        self.assertEqual(headers["X-Scope-OrgID"], "application")
        self.assertEqual(headers["Authorization"], "Bearer test-token")

    def test_serializes_fractional_timestamps_as_nanoseconds(self) -> None:
        args = search_args(start="1704067200.1", end="1704067200.9")
        with patch.dict(os.environ, {"LOGS_TOKEN": "test-token"}):
            url, _ = query_logs.build_request(inventory()["query_endpoints"][0], args)
        params = parse_qs(urlsplit(url).query)
        self.assertEqual(params["start"], ["1704067200100000000"])
        self.assertEqual(params["end"], ["1704067200900000000"])

    def test_preserves_required_endpoint_parameters(self) -> None:
        endpoint = inventory()["query_endpoints"][0].copy()
        endpoint["required_query_parameters"] = ["cluster"]
        args = search_args(param=["cluster=prod"])
        with patch.dict(os.environ, {"LOGS_TOKEN": "test-token"}):
            url, _ = query_logs.build_request(endpoint, args)
        self.assertEqual(parse_qs(urlsplit(url).query)["cluster"], ["prod"])

    def test_rejects_missing_required_parameter(self) -> None:
        endpoint = inventory()["query_endpoints"][0].copy()
        endpoint["required_query_parameters"] = ["cluster"]
        args = search_args()
        with self.assertRaises(query_logs.QueryError) as context:
            query_logs.build_request(endpoint, args)
        self.assertEqual(context.exception.kind, "missing_query_parameter")

    def test_rejects_missing_or_mismatched_tenant(self) -> None:
        endpoint = inventory()["query_endpoints"][0].copy()
        endpoint["required_headers"] = ["X-Scope-OrgID: other"]
        args = search_args()
        with self.assertRaises(query_logs.QueryError) as context:
            query_logs.build_request(endpoint, args)
        self.assertEqual(context.exception.kind, "invalid_inventory")

        endpoint["tenant"] = "infrastructure"
        endpoint["required_headers"] = ["X-Scope-OrgID: infrastructure"]
        with self.assertRaises(query_logs.QueryError) as context:
            query_logs.build_request(endpoint, args)
        self.assertEqual(context.exception.kind, "invalid_inventory")

        endpoint.pop("tenant")
        with self.assertRaises(query_logs.QueryError) as context:
            query_logs.build_request(endpoint, args)
        self.assertEqual(context.exception.kind, "invalid_inventory")

    def test_rejects_usage_endpoint_and_raw_loki_port(self) -> None:
        usage = inventory()["query_endpoints"][0].copy()
        usage["name"] = "usage-logs"
        usage["url"] = "https://loki.example.test:3100/otlp"
        usage["paths"] = ["/loki/api/v1/query_range"]
        with self.assertRaises(query_logs.QueryError) as context:
            query_logs.build_request(usage, search_args())
        self.assertEqual(context.exception.kind, "invalid_inventory")

    def test_requires_service_ca_and_https(self) -> None:
        endpoint = inventory()["query_endpoints"][0]
        args = search_args(ca_file=None)
        with self.assertRaises(query_logs.QueryError) as context:
            query_logs.build_request(endpoint, args)
        self.assertEqual(context.exception.kind, "missing_ca")

        endpoint = endpoint.copy()
        endpoint["url"] = "http://loki.example.test:8080/api/logs/v1/application"
        endpoint["tls"] = "none"
        endpoint["auth"] = "none"
        with self.assertRaises(query_logs.QueryError) as context:
            query_logs.build_request(endpoint, search_args())
        self.assertEqual(context.exception.kind, "invalid_endpoint")

    def test_rejects_duplicate_explicit_endpoint_names(self) -> None:
        data = inventory()
        data["query_endpoints"].append(data["query_endpoints"][0].copy())
        with self.assertRaises(query_logs.QueryError) as context:
            query_logs.select_endpoint(data, "data-science-loki")
        self.assertEqual(context.exception.kind, "ambiguous_endpoint")

    def test_rejects_unbounded_and_invalid_queries(self) -> None:
        with self.assertRaises(query_logs.QueryError):
            query_logs.query_metadata(search_args(start="0", end="90000"))
        with self.assertRaises(query_logs.QueryError):
            query_logs.query_metadata(search_args(max_range="48h"))
        with self.assertRaises(query_logs.QueryError):
            query_logs.query_metadata(search_args(query=""))
        with self.assertRaises(query_logs.QueryError):
            query_logs.query_metadata(search_args(limit=501))
        with self.assertRaises(query_logs.QueryError):
            query_logs.query_metadata(search_args(direction="invalid"))

    def test_accepts_empty_loki_result(self) -> None:
        payload = {"status": "success", "data": {"resultType": "streams", "result": []}}
        validated = query_logs.validate_loki_payload(payload)
        self.assertEqual(validated["data"]["result"], [])

    def test_rejects_malformed_loki_result(self) -> None:
        with self.assertRaises(query_logs.QueryError):
            query_logs.validate_loki_payload({"status": "success", "data": {"resultType": "streams"}})
        with self.assertRaises(query_logs.QueryError):
            query_logs.validate_loki_payload(
                {
                    "status": "success",
                    "data": {"resultType": "streams", "result": [{"stream": {}, "values": [["1"]]}]},
                }
            )
        with self.assertRaises(query_logs.QueryError):
            query_logs.validate_loki_payload(
                {
                    "status": "success",
                    "data": {"resultType": "streams", "result": [{"stream": {}, "values": [[float("nan"), "line"]]}]},
                }
            )

    @patch.dict(os.environ, {"LOGS_TOKEN": "test-token"})
    def test_queries_loki_and_returns_data(self) -> None:
        response = FakeResponse({"status": "success", "data": {"resultType": "streams", "result": []}})
        args = search_args(ca_file=None)
        with patch("query_logs.build_opener") as mock_build_opener:
            opener = mock_build_opener.return_value
            opener.open.return_value.__enter__.return_value = response
            result = query_logs.request_loki(
                "https://loki.example.test/query_range?query=%7B%7D",
                {"Accept": "application/json", "Authorization": "Bearer test-token"},
                args,
            )
        self.assertEqual(result["data"]["resultType"], "streams")
        request = opener.open.call_args.args[0]
        self.assertEqual(request.method, "GET")
        self.assertEqual(request.get_header("Authorization"), "Bearer test-token")

    def test_rejects_oversized_success_and_error_responses(self) -> None:
        args = search_args(max_response_bytes=1, ca_file=None)
        response = FakeResponse({"status": "success", "data": {"resultType": "streams", "result": []}})
        with patch("query_logs.build_opener") as mock_build_opener:
            mock_build_opener.return_value.open.return_value.__enter__.return_value = response
            with self.assertRaises(query_logs.QueryError) as context:
                query_logs.request_loki("https://loki.example.test/query_range", {}, args)
        self.assertEqual(context.exception.kind, "response_too_large")

        error = HTTPError(
            "https://loki.example.test/query_range",
            500,
            "server error",
            {},
            io.BytesIO(b"too large"),
        )
        with patch("query_logs.build_opener") as mock_build_opener:
            mock_build_opener.return_value.open.side_effect = error
            with self.assertRaises(query_logs.QueryError) as context:
                query_logs.request_loki("https://loki.example.test/query_range", {}, args)
        self.assertEqual(context.exception.kind, "response_too_large")

    def test_rejects_redirects(self) -> None:
        with self.assertRaises(query_logs.QueryError) as context:
            query_logs.NoRedirectHandler().redirect_request()
        self.assertEqual(context.exception.kind, "redirect_not_allowed")

    def test_argparse_errors_are_structured(self) -> None:
        output = io.StringIO()
        with redirect_stdout(output), self.assertRaises(SystemExit) as context:
            query_logs.parse_args(["--inventory", "inventory.json"])
        self.assertEqual(context.exception.code, 2)
        result = json.loads(output.getvalue())
        self.assertEqual(result["error"]["type"], "invalid_argument")

    def test_validates_structured_result(self) -> None:
        result = {
            "schema_version": "1",
            "status": "success",
            "endpoint": {
                "name": "data-science-loki",
                "signal": "logs",
                "scope": "application",
                "auth": "caller-bearer",
                "tls": "service-ca",
                "state": "ready",
                "source": "Monitoring/cluster/default-monitoring",
                "url": "https://loki.example.test:8080/api/logs/v1/application",
                "tenant": "application",
                "required_headers": ["X-Scope-OrgID"],
                "required_query_parameters": [],
            },
            "query": {
                "mode": "range",
                "logql": '{namespace="monitoring"}',
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-01-01T00:05:00Z",
                "range_seconds": 300,
                "limit": 100,
                "direction": "backward",
            },
            "data": {"resultType": "streams", "result": []},
            "warnings": [],
        }
        with patch("sys.stdin", io.StringIO(json.dumps(result))), patch("builtins.print"):
            self.assertEqual(validate_result.main(), 0)

        invalid = json.loads(json.dumps(result))
        invalid["endpoint"]["tenant"] = "infrastructure"
        with patch("sys.stdin", io.StringIO(json.dumps(invalid))), patch("builtins.print"):
            self.assertEqual(validate_result.main(), 1)


if __name__ == "__main__":
    unittest.main()
