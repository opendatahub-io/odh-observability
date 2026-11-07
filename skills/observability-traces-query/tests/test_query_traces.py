#!/usr/bin/env python3

from __future__ import annotations

import json
import io
import os
import sys
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit


SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts"))
sys.path.insert(0, SCRIPT_DIR)

import query_traces  # noqa: E402
import validate_result  # noqa: E402


def inventory(endpoint: dict | None = None) -> dict:
    return {
        "schema_version": "1",
        "monitoring": {"management_state": "Managed", "namespace": "redhat-ods-monitoring"},
        "query_endpoints": [
            endpoint
            or {
                "signal": "traces",
                "name": "tempo-data-science-tempomonolithic-gateway",
                "url": "https://tempo.example.test:8080/api/traces/v1/redhat-ods-monitoring/tempo",
                "access": "cluster-service",
                "scope": "redhat-ods-monitoring",
                "auth": "caller-bearer",
                "tls": "service-ca",
                "paths": ["/api/search", "/api/traces/<trace-id>"],
                "required_query_parameters": [],
                "required_headers": ["X-Scope-OrgID: redhat-ods-monitoring"],
                "tenant": "redhat-ods-monitoring",
                "state": "ready",
                "source": "Service/redhat-ods-monitoring/tempo-data-science-tempomonolithic-gateway",
            }
        ],
        "telemetry_ingest": [],
        "diagnostic_endpoints": [],
    }


def search_args(**overrides: object) -> Namespace:
    values = {
        "start": "2026-01-01T00:00:00Z",
        "end": "2026-01-01T00:05:00Z",
        "max_range": "24h",
        "limit": 100,
        "query": None,
        "service": None,
        "operation": None,
        "status": None,
        "tag": [],
        "param": [],
        "min_duration": None,
        "max_duration": None,
        "trace_id": None,
        "bearer_token_env": "TRACE_TOKEN",
    }
    values.update(overrides)
    return Namespace(**values)


def trace_args(**overrides: object) -> Namespace:
    values = search_args(**overrides).__dict__
    values.update({"trace_id": "0123456789abcdef0123456789abcdef", "start": None, "end": None})
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


class QueryTracesTests(unittest.TestCase):
    def test_selects_ready_trace_endpoint(self) -> None:
        selected = query_traces.select_endpoint(inventory(), None, "search")
        self.assertEqual(selected["name"], "tempo-data-science-tempomonolithic-gateway")

    def test_rejects_ambiguous_trace_endpoints(self) -> None:
        second = inventory()["query_endpoints"][0].copy()
        second["name"] = "second-tempo-gateway"
        data = inventory()
        data["query_endpoints"].append(second)
        with self.assertRaises(query_traces.QueryError) as context:
            query_traces.select_endpoint(data, None, "search")
        self.assertEqual(context.exception.kind, "ambiguous_endpoint")

    def test_preserves_explicit_endpoint_on_not_ready_error(self) -> None:
        endpoint = inventory()["query_endpoints"][0].copy()
        endpoint["state"] = "configured"
        with self.assertRaises(query_traces.QueryError) as context:
            query_traces.select_endpoint(inventory(endpoint), endpoint["name"], "trace")
        self.assertIs(context.exception.endpoint, endpoint)

    @patch.dict(os.environ, {"TRACE_TOKEN": "test-token"})
    def test_builds_search_request_with_tenant_and_filters(self) -> None:
        args = search_args(service="checkout", operation="GET /orders", status="error", tag=["http.method=GET"])
        metadata = query_traces.query_metadata(args, "search")
        url, headers = query_traces.build_request(inventory()["query_endpoints"][0], args, metadata)
        parsed = urlsplit(url)
        self.assertEqual(parsed.path, "/api/traces/v1/redhat-ods-monitoring/tempo/api/search")
        params = parse_qs(parsed.query)
        self.assertEqual(params["limit"], ["100"])
        self.assertEqual(params["status"], ["error"])
        self.assertEqual(params["tags"], ["service.name=checkout,name=GET /orders,http.method=GET"])
        self.assertEqual(headers["X-Scope-OrgID"], "redhat-ods-monitoring")
        self.assertEqual(headers["Authorization"], "Bearer test-token")

    @patch.dict(os.environ, {"TRACE_TOKEN": "test-token"})
    def test_builds_trace_lookup_request(self) -> None:
        args = trace_args()
        metadata = query_traces.query_metadata(args, "trace")
        url, _ = query_traces.build_request(inventory()["query_endpoints"][0], args, metadata)
        parsed = urlsplit(url)
        self.assertEqual(
            parsed.path,
            "/api/traces/v1/redhat-ods-monitoring/tempo/api/traces/0123456789abcdef0123456789abcdef",
        )
        self.assertEqual(parsed.query, "")

    def test_requires_bounded_search_window(self) -> None:
        args = search_args(start="0", end="90000")
        with self.assertRaises(query_traces.QueryError) as context:
            query_traces.query_metadata(args, "search")
        self.assertEqual(context.exception.kind, "invalid_argument")

    def test_rejects_unbounded_search(self) -> None:
        args = search_args(start=None, end=None)
        with self.assertRaises(query_traces.QueryError) as context:
            query_traces.query_metadata(args, "search")
        self.assertEqual(context.exception.kind, "invalid_argument")

    def test_rejects_invalid_trace_id(self) -> None:
        with self.assertRaises(query_traces.QueryError) as context:
            query_traces.query_metadata(trace_args(trace_id="not-a-trace"), "trace")
        self.assertEqual(context.exception.kind, "invalid_argument")

    def test_rejects_tenant_header_mismatch(self) -> None:
        endpoint = inventory()["query_endpoints"][0].copy()
        endpoint["required_headers"] = ["X-Scope-OrgID: wrong-tenant"]
        with self.assertRaises(query_traces.QueryError) as context:
            query_traces.build_request(endpoint, trace_args(), query_traces.query_metadata(trace_args(), "trace"))
        self.assertEqual(context.exception.kind, "invalid_inventory")

    def test_rejects_missing_tenant(self) -> None:
        endpoint = inventory()["query_endpoints"][0].copy()
        endpoint.pop("tenant")
        with self.assertRaises(query_traces.QueryError) as context:
            query_traces.build_request(endpoint, trace_args(), query_traces.query_metadata(trace_args(), "trace"))
        self.assertEqual(context.exception.kind, "invalid_inventory")

    def test_rejects_sensitive_required_header(self) -> None:
        endpoint = inventory()["query_endpoints"][0].copy()
        endpoint["required_headers"] = ["X-Api-Key: static-secret"]
        with self.assertRaises(query_traces.QueryError) as context:
            query_traces.build_request(endpoint, trace_args(), query_traces.query_metadata(trace_args(), "trace"))
        self.assertEqual(context.exception.kind, "invalid_inventory")

    @patch.dict(os.environ, {"TRACE_TOKEN": "test-token"})
    def test_requires_discovered_query_parameters(self) -> None:
        endpoint = inventory()["query_endpoints"][0].copy()
        endpoint["required_query_parameters"] = ["cluster"]
        with self.assertRaises(query_traces.QueryError) as context:
            query_traces.build_request(endpoint, trace_args(), query_traces.query_metadata(trace_args(), "trace"))
        self.assertEqual(context.exception.kind, "missing_query_parameter")

        args = trace_args(param=["cluster=prod"])
        url, _ = query_traces.build_request(endpoint, args, query_traces.query_metadata(args, "trace"))
        self.assertEqual(parse_qs(urlsplit(url).query), {"cluster": ["prod"]})

    @patch.dict(os.environ, {"TRACE_TOKEN": "test-token"})
    def test_queries_tempo_search_and_returns_data(self) -> None:
        response = FakeResponse({"traces": [{"traceID": "0123456789abcdef"}], "warnings": []})
        args = Namespace(timeout=20, max_response_bytes=1024, ca_file=None, bearer_token_env="TRACE_TOKEN")
        with patch("query_traces.build_opener") as mock_build_opener:
            opener = mock_build_opener.return_value
            opener.open.return_value.__enter__.return_value = response
            result = query_traces.request_tempo(
                "https://tempo.example.test/api/search?start=1&end=2&limit=1",
                {"Accept": "application/json", "Authorization": "Bearer test-token"},
                args,
                "search",
            )
        self.assertEqual(result["traces"][0]["traceID"], "0123456789abcdef")
        request = opener.open.call_args.args[0]
        self.assertEqual(request.method, "GET")
        self.assertEqual(request.get_header("Authorization"), "Bearer test-token")

    def test_rejects_raw_tempo_port(self) -> None:
        with self.assertRaises(query_traces.QueryError) as context:
            endpoint = inventory()["query_endpoints"][0].copy()
            endpoint["url"] = "https://tempo.monitoring.svc:3200"
            query_traces.build_request(
                endpoint,
                trace_args(),
                query_traces.query_metadata(trace_args(), "trace"),
            )
        self.assertEqual(context.exception.kind, "invalid_inventory")

    def test_allows_unrelated_query_endpoint_port_in_inventory(self) -> None:
        unrelated = {
            "signal": "metrics",
            "name": "metrics-query",
            "url": "https://metrics.example.test:3200",
            "access": "cluster-service",
            "scope": "monitoring",
            "auth": "none",
            "tls": "service-ca",
            "paths": ["/api/v1/query"],
            "required_query_parameters": [],
            "state": "ready",
            "source": "Service/monitoring/metrics-query",
        }
        data = inventory()
        data["query_endpoints"].append(unrelated)
        with patch("query_traces.Path.open", side_effect=lambda **_: io.StringIO(json.dumps(data))):
            loaded = query_traces.load_inventory("inventory.json")
        self.assertEqual(len(loaded["query_endpoints"]), 2)

    def test_rejects_sensitive_tag(self) -> None:
        with self.assertRaises(query_traces.QueryError) as context:
            query_traces.query_metadata(search_args(tag=["password=secret"]), "search")
        self.assertEqual(context.exception.kind, "invalid_argument")

    def test_rejects_http_trace_endpoint(self) -> None:
        endpoint = inventory()["query_endpoints"][0].copy()
        endpoint["url"] = endpoint["url"].replace("https://", "http://")
        with self.assertRaises(query_traces.QueryError) as context:
            query_traces.build_request(endpoint, trace_args(), query_traces.query_metadata(trace_args(), "trace"))
        self.assertEqual(context.exception.kind, "invalid_endpoint")

    def test_rejects_endpoint_without_discovered_tempo_base_path(self) -> None:
        endpoint = inventory()["query_endpoints"][0].copy()
        endpoint["url"] = "https://tempo.example.test:8080"
        with self.assertRaises(query_traces.QueryError) as context:
            query_traces.build_request(
                endpoint,
                trace_args(),
                query_traces.query_metadata(trace_args(), "trace"),
            )
        self.assertEqual(context.exception.kind, "invalid_endpoint")

    def test_validates_search_payload(self) -> None:
        query_traces.validate_tempo_data({"traces": []}, "search")
        with self.assertRaises(query_traces.QueryError):
            query_traces.validate_tempo_data({"traces": {}}, "search")

    def test_validates_trace_payload(self) -> None:
        query_traces.validate_tempo_data({"batches": []}, "trace")
        with self.assertRaises(query_traces.QueryError):
            query_traces.validate_tempo_data({"trace": {}}, "trace")

    def test_argparse_errors_are_structured(self) -> None:
        output = io.StringIO()
        with redirect_stdout(output), self.assertRaises(SystemExit) as context:
            query_traces.parse_args(["--inventory", "inventory.json", "--search", "--limit", "invalid"])
        self.assertEqual(context.exception.code, 2)
        result = json.loads(output.getvalue())
        self.assertEqual(result["error"]["type"], "invalid_argument")
        self.assertEqual(result["status"], "error")

    def test_validates_structured_trace_result(self) -> None:
        result = {
            "schema_version": "1",
            "status": "success",
            "endpoint": {
                "name": "tempo",
                "signal": "traces",
                "scope": "redhat-ods-monitoring",
                "auth": "caller-bearer",
                "tls": "service-ca",
                "state": "ready",
                "source": "Service/redhat-ods-monitoring/tempo",
                "url": "https://tempo.example.test:8080/api/traces/v1/redhat-ods-monitoring/tempo",
                "tenant": "redhat-ods-monitoring",
                "required_headers": ["X-Scope-OrgID"],
            },
            "query": {"mode": "trace", "trace_id": "0123456789abcdef"},
            "data": {"batches": []},
            "warnings": [],
        }
        with patch("sys.stdin", io.StringIO(json.dumps(result))), patch("builtins.print"):
            self.assertEqual(validate_result.main(), 0)

    def test_rejects_tag_delimiter_and_duration_order(self) -> None:
        with self.assertRaises(query_traces.QueryError):
            query_traces.query_metadata(search_args(tag=["http.route=/a,b"]), "search")
        with self.assertRaises(query_traces.QueryError):
            query_traces.query_metadata(
                search_args(min_duration="5s", max_duration="1s"), "search"
            )


if __name__ == "__main__":
    unittest.main()
