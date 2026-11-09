#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import sys
import unittest
from argparse import Namespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit


SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts"))
sys.path.insert(0, SCRIPT_DIR)

import query_metrics  # noqa: E402


def inventory(endpoint: dict | None = None) -> dict:
    return {
        "schema_version": "1",
        "monitoring": {"management_state": "Managed", "namespace": "redhat-ods-monitoring"},
        "query_endpoints": [
            endpoint
            or {
                "signal": "metrics",
                "name": "data-science-thanos-querier-route",
                "url": "https://metrics.example.test",
                "access": "route",
                "scope": "data-science",
                "auth": "caller-bearer",
                "tls": "route-reencrypt",
                "paths": ["/api/v1/query", "/api/v1/query_range"],
                "required_query_parameters": ["namespace"],
                "state": "ready",
                "source": "Route/redhat-ods-monitoring/data-science-thanos-querier-route",
            }
        ],
        "telemetry_ingest": [],
        "diagnostic_endpoints": [],
    }


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


class QueryMetricsTests(unittest.TestCase):
    def test_selects_ready_metrics_endpoint(self) -> None:
        selected = query_metrics.select_endpoint(inventory(), None, None)
        self.assertEqual(selected["name"], "data-science-thanos-querier-route")

    def test_rejects_ambiguous_lanes(self) -> None:
        second = inventory()["query_endpoints"][0].copy()
        second["name"] = "cluster-prometheus-datasource"
        second["scope"] = "openshift-platform"
        data = inventory()
        data["query_endpoints"].append(second)
        with self.assertRaises(query_metrics.QueryError) as context:
            query_metrics.select_endpoint(data, None, None)
        self.assertEqual(context.exception.kind, "ambiguous_endpoint")

    @patch.dict(os.environ, {"METRICS_TOKEN": "test-token"})
    def test_builds_namespace_isolated_get_request(self) -> None:
        args = Namespace(
            namespace="team-a",
            param=[],
            query="up",
            time=None,
            start=None,
            end=None,
            step=None,
            bearer_token_env="METRICS_TOKEN",
        )
        url, headers = query_metrics.build_request(
            inventory()["query_endpoints"][0], args, {"mode": "instant", "promql": "up"}
        )
        parsed = urlsplit(url)
        self.assertEqual(parsed.path, "/api/v1/query")
        self.assertEqual(parse_qs(parsed.query), {"namespace": ["team-a"], "query": ["up"]})
        self.assertEqual(headers["Authorization"], "Bearer test-token")

    def test_enforces_range_limits(self) -> None:
        args = Namespace(
            query="up",
            start="0",
            end="90000",
            step="1m",
            time=None,
            max_range="24h",
            min_step="15s",
        )
        with self.assertRaises(query_metrics.QueryError) as context:
            query_metrics.query_metadata(args, "range")
        self.assertEqual(context.exception.kind, "invalid_argument")

    def test_requires_explicit_namespace(self) -> None:
        args = Namespace(
            namespace=None,
            param=[],
            query="up",
            time=None,
            start=None,
            end=None,
            step=None,
            bearer_token_env="METRICS_TOKEN",
        )
        with self.assertRaises(query_metrics.QueryError) as context:
            query_metrics.build_request(
                inventory()["query_endpoints"][0], args, {"mode": "instant", "promql": "up"}
            )
        self.assertEqual(context.exception.kind, "missing_query_parameter")

    def test_rejects_namespace_embedded_in_endpoint_url(self) -> None:
        endpoint = inventory()["query_endpoints"][0].copy()
        endpoint["url"] = "https://metrics.example.test?namespace=team-a"
        args = Namespace(
            namespace="team-a",
            param=[],
            query="up",
            time=None,
            start=None,
            end=None,
            step=None,
            bearer_token_env="METRICS_TOKEN",
        )
        with self.assertRaises(query_metrics.QueryError) as context:
            query_metrics.build_request(endpoint, args, {"mode": "instant", "promql": "up"})
        self.assertEqual(context.exception.kind, "invalid_inventory")

    @patch.dict(os.environ, {"METRICS_TOKEN": "test-token"})
    def test_rejects_unknown_auth_mode(self) -> None:
        endpoint = inventory()["query_endpoints"][0].copy()
        endpoint["auth"] = "see-notes"
        args = Namespace(
            namespace="team-a",
            param=[],
            query="up",
            time=None,
            start=None,
            end=None,
            step=None,
            bearer_token_env="METRICS_TOKEN",
        )
        with self.assertRaises(query_metrics.QueryError) as context:
            query_metrics.build_request(endpoint, args, {"mode": "instant", "promql": "up"})
        self.assertEqual(context.exception.kind, "unsupported_auth")

    @patch.dict(os.environ, {"METRICS_TOKEN": "test-token"})
    def test_rejects_http_caller_bearer_endpoint(self) -> None:
        endpoint = inventory()["query_endpoints"][0].copy()
        endpoint.update({"url": "http://metrics.example.test", "tls": "none"})
        args = Namespace(
            namespace="team-a",
            param=[],
            query="up",
            time=None,
            start=None,
            end=None,
            step=None,
            bearer_token_env="METRICS_TOKEN",
        )
        with self.assertRaises(query_metrics.QueryError) as context:
            query_metrics.build_request(endpoint, args, {"mode": "instant", "promql": "up"})
        self.assertEqual(context.exception.kind, "invalid_endpoint")
        self.assertEqual(context.exception.exit_code, 3)

    @patch.dict(os.environ, {"METRICS_TOKEN": "test-token"})
    @patch("query_metrics.build_opener")
    def test_queries_prometheus_and_returns_data(self, mock_build_opener: object) -> None:
        response = FakeResponse(
            {
                "status": "success",
                "data": {"resultType": "vector", "result": [{"metric": {}, "value": [1, "1"]}]},
            }
        )
        mock_opener = mock_build_opener.return_value  # type: ignore[attr-defined]
        mock_opener.open.return_value.__enter__.return_value = response
        args = Namespace(timeout=20, max_response_bytes=1024, ca_file=None, bearer_token_env="METRICS_TOKEN")
        result = query_metrics.request_metrics(
            "https://metrics.example.test/api/v1/query?namespace=team-a&query=up",
            {"Accept": "application/json", "Authorization": "Bearer test-token"},
            args,
        )
        self.assertEqual(result["data"]["resultType"], "vector")
        request = mock_opener.open.call_args.args[0]
        self.assertEqual(request.method, "GET")
        self.assertEqual(request.get_header("Authorization"), "Bearer test-token")

    @patch("query_metrics.build_opener")
    @patch("query_metrics.ssl.create_default_context")
    def test_adds_supplied_ca_to_default_context(
        self, mock_create_default_context: object, mock_build_opener: object
    ) -> None:
        response = FakeResponse(
            {
                "status": "success",
                "data": {"resultType": "vector", "result": []},
            }
        )
        context = mock_create_default_context.return_value  # type: ignore[attr-defined]
        mock_opener = mock_build_opener.return_value  # type: ignore[attr-defined]
        mock_opener.open.return_value.__enter__.return_value = response
        args = Namespace(
            timeout=20,
            max_response_bytes=1024,
            ca_file="/path/to/service-ca.crt",
            bearer_token_env=None,
        )

        query_metrics.request_metrics(
            "https://metrics.example.test/api/v1/query?query=up",
            {"Accept": "application/json"},
            args,
        )

        mock_create_default_context.assert_called_once_with()  # type: ignore[attr-defined]
        context.load_verify_locations.assert_called_once_with(  # type: ignore[attr-defined]
            cafile="/path/to/service-ca.crt"
        )

    @patch.dict(os.environ, {"METRICS_TOKEN": "test-token"})
    def test_requires_namespace_option_instead_of_generic_parameter(self) -> None:
        args = Namespace(
            namespace=None,
            param=["namespace=team-a"],
            query="up",
            time=None,
            start=None,
            end=None,
            step=None,
            bearer_token_env="METRICS_TOKEN",
        )
        with self.assertRaises(query_metrics.QueryError) as context:
            query_metrics.build_request(
                inventory()["query_endpoints"][0], args, {"mode": "instant", "promql": "up"}
            )
        self.assertEqual(context.exception.kind, "missing_query_parameter")

    def test_rejects_malformed_success_data(self) -> None:
        with self.assertRaises(query_metrics.QueryError) as context:
            query_metrics.validate_prometheus_data({"resultType": "vector", "result": [{"metric": {}}]})
        self.assertEqual(context.exception.kind, "invalid_response")

    def test_rejects_user_supplied_safety_limit_bypass(self) -> None:
        args = Namespace(
            query="up",
            start="0",
            end="60",
            step="15s",
            time=None,
            max_range="25h",
            min_step="15s",
        )
        with self.assertRaises(query_metrics.QueryError):
            query_metrics.query_metadata(args, "range")

    @patch.dict(os.environ, {"METRICS_TOKEN": "test-token"})
    def test_builds_range_request(self) -> None:
        args = Namespace(
            namespace="team-a",
            param=[],
            query="up",
            time=None,
            start="100",
            end="160",
            step="15s",
            bearer_token_env="METRICS_TOKEN",
        )
        url, _ = query_metrics.build_request(
            inventory()["query_endpoints"][0],
            args,
            {"mode": "range", "promql": "up"},
        )
        parsed = urlsplit(url)
        self.assertEqual(parsed.path, "/api/v1/query_range")
        self.assertEqual(
            parse_qs(parsed.query),
            {"namespace": ["team-a"], "query": ["up"], "start": ["100"], "end": ["160"], "step": ["15s"]},
        )

    def test_rejects_raw_backend_url(self) -> None:
        with self.assertRaises(query_metrics.QueryError) as context:
            query_metrics.parse_safe_url("https://prometheus-operated.monitoring.svc:10902")
        self.assertEqual(context.exception.kind, "invalid_inventory")

    @patch("query_metrics.build_opener")
    def test_preserves_prometheus_error_warnings(self, mock_build_opener: object) -> None:
        response = FakeResponse(
            {
                "status": "error",
                "errorType": "bad_data",
                "error": "invalid expression",
                "warnings": ["partial response"],
            }
        )
        mock_opener = mock_build_opener.return_value  # type: ignore[attr-defined]
        mock_opener.open.return_value.__enter__.return_value = response
        args = Namespace(timeout=20, max_response_bytes=1024, ca_file=None)
        with self.assertRaises(query_metrics.QueryError) as context:
            query_metrics.request_metrics(
                "https://metrics.example.test/api/v1/query?query=up",
                {"Accept": "application/json"},
                args,
            )
        self.assertEqual(context.exception.kind, "bad_data")
        self.assertEqual(context.exception.warnings, ["partial response"])


if __name__ == "__main__":
    unittest.main()
