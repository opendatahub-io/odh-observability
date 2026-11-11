#!/usr/bin/env python3

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from http.client import HTTPException
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
                "route_admitted": True,
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
        self.assertNotIn("Authorization", headers)

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
    def test_rejects_invalid_bearer_token_environment_name(self) -> None:
        args = Namespace(
            namespace="team-a",
            param=[],
            query="up",
            time=None,
            start=None,
            end=None,
            step=None,
            bearer_token_env="metrics-token",
        )
        with self.assertRaises(query_metrics.QueryError) as context:
            query_metrics.build_request(
                inventory()["query_endpoints"][0], args, {"mode": "instant", "promql": "up"}
            )
        self.assertEqual(context.exception.kind, "invalid_auth")

    @patch.dict(os.environ, {"METRICS_TOKEN": "test-token"})
    def test_rejects_bearer_auth_to_untrusted_host(self) -> None:
        endpoint = inventory()["query_endpoints"][0].copy()
        endpoint.update(
            {
                "access": "cluster-service",
                "source": "Service/redhat-ods-monitoring/data-science-thanos-querier-proxy",
            }
        )
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
        with patch.object(query_metrics.discover_endpoints, "discover", return_value=inventory()):
            with self.assertRaises(query_metrics.QueryError) as context:
                query_metrics.request_metrics(endpoint["url"] + "/api/v1/query", {}, args, endpoint)
        self.assertEqual(context.exception.kind, "invalid_auth")

    @patch.dict(os.environ, {"METRICS_TOKEN": "test-token"})
    @patch("query_metrics.build_opener")
    def test_forged_inventory_cannot_send_bearer_token(self, mock_build_opener: object) -> None:
        forged = inventory()
        forged["query_endpoints"][0]["url"] = "https://attacker.example.test"
        self.assertIsNone(query_metrics.validate_discovery_inventory(forged))
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as stream:
            json.dump(forged, stream)
            stream.flush()
            output = io.StringIO()
            with (
                patch.object(query_metrics.discover_endpoints, "discover", return_value=inventory()),
                patch.object(os.environ, "get", wraps=os.environ.get) as get_env,
                redirect_stdout(output),
            ):
                result = query_metrics.main([
                    "--inventory", stream.name, "--query", "up", "--namespace", "team-a",
                    "--bearer-token-env", "METRICS_TOKEN",
                ])
            self.assertNotIn("METRICS_TOKEN", [call.args[0] for call in get_env.call_args_list])
        self.assertEqual(result, 3)
        self.assertEqual(json.loads(output.getvalue())["error"]["type"], "invalid_auth")
        self.assertNotIn("test-token", output.getvalue())
        mock_build_opener.assert_not_called()  # type: ignore[attr-defined]

    def test_discovery_must_match_security_relevant_fields(self) -> None:
        baseline = inventory()["query_endpoints"][0]
        changes = {
            "url": "https://metrics.example.test:8443",
            "source": "Route/other/other",
            "state": "not-ready",
            "access": "cluster-service",
            "scope": "openshift-cluster",
            "auth": "none",
            "tls": "none",
            "paths": ["/api/v1/query"],
            "required_query_parameters": [],
            "route_admitted": False,
            "target_service": "attacker",
        }
        for field, value in changes.items():
            with self.subTest(field=field):
                altered = baseline.copy()
                altered[field] = value
                with patch.object(query_metrics.discover_endpoints, "discover", return_value=inventory()):
                    with self.assertRaises(query_metrics.QueryError) as context:
                        query_metrics.verify_bearer_endpoint(altered)
                self.assertEqual(context.exception.kind, "invalid_auth")

    def test_discovery_failure_and_ambiguous_or_stale_evidence_fail_closed(self) -> None:
        endpoint = inventory()["query_endpoints"][0]
        absent = inventory()
        absent["query_endpoints"] = []
        ambiguous = inventory()
        ambiguous["query_endpoints"].append(endpoint.copy())
        not_ready = inventory()
        not_ready["query_endpoints"][0]["state"] = "pending"
        for result in (absent, ambiguous, not_ready, {}):
            with self.subTest(result=result):
                with patch.object(query_metrics.discover_endpoints, "discover", return_value=result):
                    with self.assertRaises(query_metrics.QueryError) as context:
                        query_metrics.verify_bearer_endpoint(endpoint)
                self.assertEqual(context.exception.kind, "invalid_auth")
        with patch.object(query_metrics.discover_endpoints, "discover", side_effect=query_metrics.discover_endpoints.OcError("permission denied")):
            with self.assertRaises(query_metrics.QueryError) as context:
                query_metrics.verify_bearer_endpoint(endpoint)
        self.assertEqual(context.exception.kind, "invalid_auth")

    def test_cluster_service_must_also_match_live_discovery(self) -> None:
        endpoint = inventory()["query_endpoints"][0].copy()
        endpoint.update({
            "access": "cluster-service",
            "source": "Service/redhat-ods-monitoring/data-science-thanos-querier-proxy",
            "url": "https://data-science-thanos-querier-proxy.redhat-ods-monitoring.svc.cluster.local:8443",
            "tls": "service-ca",
        })
        endpoint.pop("route_admitted")
        with patch.object(query_metrics.discover_endpoints, "discover", return_value=inventory(endpoint)):
            query_metrics.verify_bearer_endpoint(endpoint)
        forged = endpoint.copy()
        forged.update({
            "source": "Service/attacker/collector",
            "url": "https://collector.attacker.svc.cluster.local:8443",
        })
        with patch.object(query_metrics.discover_endpoints, "discover", return_value=inventory(endpoint)):
            with self.assertRaises(query_metrics.QueryError):
                query_metrics.verify_bearer_endpoint(forged)

    @patch.dict(os.environ, {"METRICS_TOKEN": "test-token"})
    @patch("query_metrics.build_opener")
    def test_route_verification_uses_live_route_admission(self, mock_build_opener: object) -> None:
        discovery = query_metrics.discover_endpoints
        service_name = "data-science-thanos-querier-proxy"
        namespace = "redhat-ods-monitoring"
        route = {
            "metadata": {"name": "data-science-thanos-querier-route", "namespace": namespace},
            "spec": {"to": {"name": service_name}, "tls": {"termination": "reencrypt"}},
            "status": {"ingress": [{
                "host": "metrics.example.test",
                "conditions": [{"type": "Admitted", "status": "True"}],
            }]},
        }
        service = {
            "metadata": {"name": service_name, "namespace": namespace},
            "spec": {"ports": [{"name": "https", "port": 8443, "targetPort": 8443}]},
        }
        endpoint_slice = {
            "metadata": {"labels": {"kubernetes.io/service-name": service_name}},
            "ports": [{"port": 8443}],
            "endpoints": [{"addresses": ["10.0.0.1"], "conditions": {"ready": True}}],
        }

        def fake_oc(args: list[str], namespace: str | None = None) -> object:
            if args[:3] == ["get", "monitoring", "default-monitoring"]:
                return {
                    "spec": {"namespace": "redhat-ods-monitoring", "metrics": {"storage": {}}},
                    "status": {"conditions": [{"type": "ThanosQuerierAvailable", "status": "True"}]},
                }
            if args[:2] == ["get", "routes.route.openshift.io"]:
                return {"items": [route]}
            if args[:2] == ["get", "services"]:
                return {"items": [service]}
            if args[:2] == ["get", "endpointslices.discovery.k8s.io"]:
                return {"items": [endpoint_slice]}
            raise discovery.OcError("resource not found")

        args = Namespace(
            namespace="team-a", param=[], query="up", time=None, start=None, end=None,
            step=None, bearer_token_env="METRICS_TOKEN", timeout=20, max_response_bytes=1024, ca_file=None,
        )
        with patch.object(discovery, "run_oc", side_effect=fake_oc):
            discovered = discovery.discover()
            endpoint = next(item for item in discovered["query_endpoints"] if item["access"] == "route")
            self.assertEqual(endpoint["state"], "ready")
            url, headers = query_metrics.build_request(endpoint, args, {"mode": "instant"})
            self.assertNotIn("Authorization", headers)

            response = FakeResponse({"status": "success", "data": {"resultType": "vector", "result": []}})
            mock_opener = mock_build_opener.return_value  # type: ignore[attr-defined]
            mock_opener.open.return_value.__enter__.return_value = response
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as stream:
                json.dump(discovered, stream)
                stream.flush()
                output = io.StringIO()
                with redirect_stdout(output):
                    result = query_metrics.main([
                        "--inventory", stream.name, "--endpoint", endpoint["name"],
                        "--query", "up", "--namespace", "team-a", "--bearer-token-env", "METRICS_TOKEN",
                    ])
            self.assertEqual(result, 0)
            self.assertEqual(json.loads(output.getvalue())["status"], "success")
            request = mock_opener.open.call_args.args[0]
            self.assertEqual(request.get_header("Authorization"), "Bearer test-token")
            self.assertEqual(urlsplit(request.full_url).hostname, "metrics.example.test")
            mock_build_opener.reset_mock()  # type: ignore[attr-defined]

            route["status"]["ingress"][0]["host"] = "new.example.test"
            with self.assertRaises(query_metrics.QueryError) as context:
                query_metrics.request_metrics(url, headers, args, endpoint)
            self.assertEqual(context.exception.kind, "invalid_auth")
            route["status"]["ingress"][0]["host"] = "metrics.example.test"
            route["status"]["ingress"][0]["conditions"][0]["status"] = "False"
            with self.assertRaises(query_metrics.QueryError):
                query_metrics.request_metrics(url, headers, args, endpoint)
        mock_build_opener.assert_not_called()  # type: ignore[attr-defined]

    @patch("query_metrics.build_opener")
    def test_authorization_target_must_match_https_origin(self, mock_build_opener: object) -> None:
        args = Namespace(timeout=20, max_response_bytes=1024, ca_file=None, bearer_token_env="METRICS_TOKEN")
        endpoint = inventory()["query_endpoints"][0]
        for url in (
            "https://attacker.example.test/api/v1/query",
            "http://metrics.example.test/api/v1/query",
            "https://metrics.example.test:8443/api/v1/query",
        ):
            with self.subTest(url=url):
                with self.assertRaises(query_metrics.QueryError) as context:
                    query_metrics.request_metrics(url, {}, args, endpoint)
                self.assertEqual(context.exception.kind, "invalid_auth")
        with self.assertRaises(query_metrics.QueryError):
            query_metrics.request_metrics(endpoint["url"], {"Authorization": "Bearer test-token"}, args, endpoint)
        mock_build_opener.assert_not_called()  # type: ignore[attr-defined]

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
        with patch.object(query_metrics.discover_endpoints, "discover", return_value=inventory()):
            result = query_metrics.request_metrics(
                "https://metrics.example.test/api/v1/query?namespace=team-a&query=up",
                {"Accept": "application/json"},
                args,
                endpoint=inventory()["query_endpoints"][0],
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

    @patch("query_metrics.build_opener")
    def test_converts_transport_exceptions_to_query_errors(self, mock_build_opener: object) -> None:
        args = Namespace(timeout=20, max_response_bytes=1024, ca_file=None)
        mock_opener = mock_build_opener.return_value  # type: ignore[attr-defined]
        for exception in (HTTPException("connection failed"), ValueError("invalid request")):
            with self.subTest(exception=type(exception).__name__):
                mock_opener.open.side_effect = exception
                with self.assertRaises(query_metrics.QueryError) as context:
                    query_metrics.request_metrics(
                        "https://metrics.example.test/api/v1/query?query=up",
                        {"Accept": "application/json"},
                        args,
                    )
                self.assertEqual(context.exception.kind, "transport_error")


if __name__ == "__main__":
    unittest.main()
