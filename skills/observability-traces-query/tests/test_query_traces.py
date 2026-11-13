#!/usr/bin/env python3

from __future__ import annotations

import json
import io
import os
import signal
import socket
import sys
import tempfile
import threading
import time
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit


SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts"))
sys.path.insert(0, SCRIPT_DIR)

import query_traces  # noqa: E402
import discover_endpoints  # noqa: E402
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
    @patch.dict(os.environ, {"TRACE_TOKEN": "test-token"})
    def test_discovered_tempo_gateway_builds_trace_requests(self) -> None:
        namespace = "redhat-ods-monitoring"
        gateway = "tempo-data-science-tempomonolithic-gateway"
        objects = {
            "monitoring": {
                "spec": {"namespace": namespace, "traces": {"storage": {"backend": "pv"}}},
                "status": {"conditions": [{"type": "TempoAvailable", "status": "True"}]},
            },
            "services": {"items": [{
                "metadata": {"name": gateway, "namespace": namespace},
                "spec": {"ports": [{"port": 8080}]},
            }]},
            "endpointslices.discovery.k8s.io": {"items": [{
                "metadata": {"labels": {"kubernetes.io/service-name": gateway}},
                "ports": [{"port": 8080}],
                "endpoints": [{"addresses": ["10.0.0.1"], "conditions": {"ready": True}}],
            }]},
            "tempomonolithic": {
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            },
        }

        def fake_oc(args: list[str], namespace: str | None = None) -> object:
            if args[0] == "whoami":
                return "test"
            return objects.get(args[1], {"items": []})

        with patch.object(discover_endpoints, "run_oc", side_effect=fake_oc):
            discovered = discover_endpoints.discover()
            query_traces.verify_endpoint(discovered["query_endpoints"][0])

        self.assertIsNone(query_traces.validate_discovery_inventory(discovered))
        for mode, args in (("search", search_args()), ("trace", trace_args())):
            with self.subTest(mode=mode):
                endpoint = query_traces.select_endpoint(discovered, gateway, mode)
                url, headers = query_traces.build_request(
                    endpoint, args, query_traces.query_metadata(args, mode)
                )
                self.assertTrue(url.startswith(
                    f"https://{gateway}.{namespace}.svc.cluster.local:8080/"
                    f"api/traces/v1/{namespace}/tempo/api/"
                ))
                self.assertEqual(headers["X-Scope-OrgID"], namespace)
                self.assertEqual(headers["Authorization"], "Bearer test-token")

        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as stream:
            json.dump(discovered, stream)
            stream.flush()
            with patch.object(discover_endpoints, "run_oc", side_effect=fake_oc), patch("query_traces.build_opener") as opener:
                for mode, options, payload in (
                    ("search", ["--search", "--start", "1", "--end", "2"], {"traces": []}),
                    ("trace", ["--trace-id", "0123456789abcdef"], {"batches": []}),
                ):
                    with self.subTest(mode=mode):
                        opener.return_value.open.return_value.__enter__.return_value = FakeResponse(payload)
                        output = io.StringIO()
                        with redirect_stdout(output):
                            result = query_traces.main([
                                "--inventory", stream.name, "--endpoint", gateway,
                                "--bearer-token-env", "TRACE_TOKEN", *options,
                            ])
                        self.assertEqual(result, 0)
                        self.assertEqual(json.loads(output.getvalue())["status"], "success")
                        request = opener.return_value.open.call_args.args[0]
                        self.assertEqual(request.get_header("Authorization"), "Bearer test-token")

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

    def test_malformed_conditions_preserve_no_ready_endpoint(self) -> None:
        for monitoring in (None, "invalid", {"conditions": "invalid"},
                           {"conditions": {"TempoAvailable": "invalid"}},
                           {"conditions": {"InstrumentationAvailable": [1]}}):
            with self.subTest(monitoring=monitoring):
                data = inventory()
                data["monitoring"] = monitoring
                data["query_endpoints"] = []
                with self.assertRaises(query_traces.QueryError) as context:
                    query_traces.select_endpoint(data, None, "search")
                self.assertEqual(context.exception.kind, "no_ready_endpoint")
        data["monitoring"] = {"conditions": {"TempoAvailable": {"status": "False", "reason": "NotReady"}}}
        with self.assertRaises(query_traces.QueryError) as context:
            query_traces.select_endpoint(data, None, "search")
        self.assertIn("TempoAvailable: NotReady", context.exception.message)

        for conditions in ("invalid", {"TempoAvailable": "invalid"}):
            data = inventory()
            data["query_endpoints"] = []
            data["monitoring"]["conditions"] = conditions
            output = io.StringIO()
            with patch("query_traces.Path.open", return_value=io.StringIO(json.dumps(data))), redirect_stdout(output):
                result = query_traces.main(["--inventory", "inventory.json", "--search", "--start", "1", "--end", "2"])
            self.assertEqual(result, 3)
            self.assertEqual(json.loads(output.getvalue())["error"]["type"], "no_ready_endpoint")

    @patch.dict(os.environ, {"TRACE_TOKEN": "test-token"})
    def test_forged_inventory_fails_before_reading_token_or_opening_connection(self) -> None:
        forged = inventory()
        forged["query_endpoints"][0]["url"] = (
            "https://attacker.example.test:8080/api/traces/v1/redhat-ods-monitoring/tempo"
        )
        self.assertIsNone(query_traces.validate_discovery_inventory(forged))
        output = io.StringIO()
        with (
            patch("query_traces.Path.open", return_value=io.StringIO(json.dumps(forged))),
            patch.object(discover_endpoints, "discover", return_value=inventory()),
            patch.object(os.environ, "get", wraps=os.environ.get) as get_env,
            patch("query_traces.build_opener") as opener,
            redirect_stdout(output),
        ):
            result = query_traces.main([
                "--inventory", "inventory.json", "--trace-id", "0123456789abcdef",
                "--bearer-token-env", "TRACE_TOKEN",
            ])
        self.assertEqual(result, 3)
        self.assertEqual(json.loads(output.getvalue())["error"]["type"], "invalid_auth")
        self.assertNotIn("test-token", output.getvalue())
        self.assertNotIn("TRACE_TOKEN", [call.args[0] for call in get_env.call_args_list])
        opener.assert_not_called()

    def test_live_discovery_must_match_all_security_fields(self) -> None:
        endpoint = inventory()["query_endpoints"][0]
        changes = {
            "url": "https://other.example.test:8080/api/traces/v1/redhat-ods-monitoring/tempo",
            "source": "Service/other/other", "state": "pending", "access": "route",
            "scope": "other", "auth": "none", "tls": "none", "paths": ["/api/search"],
            "required_query_parameters": ["other"], "required_headers": ["X-Scope-OrgID: other"],
            "tenant": "other", "service_port": 8443,
        }
        for field, value in changes.items():
            with self.subTest(field=field):
                changed = endpoint.copy()
                changed[field] = value
                with patch.object(discover_endpoints, "discover", return_value=inventory()):
                    with self.assertRaises(query_traces.QueryError) as context:
                        query_traces.verify_endpoint(changed)
                self.assertEqual(context.exception.kind, "invalid_auth")
        for data in ({}, {**inventory(), "query_endpoints": []},
                     {**inventory(), "query_endpoints": [endpoint, endpoint.copy()]},
                     inventory({**endpoint, "state": "pending"})):
            with self.subTest(data=data), patch.object(discover_endpoints, "discover", return_value=data):
                with self.assertRaises(query_traces.QueryError) as context:
                    query_traces.verify_endpoint(endpoint)
                self.assertEqual(context.exception.kind, "invalid_auth")
        with patch.object(discover_endpoints, "discover", side_effect=RuntimeError("secret")):
            with self.assertRaises(query_traces.QueryError) as context:
                query_traces.verify_endpoint(endpoint)
        self.assertEqual(context.exception.kind, "invalid_auth")
        self.assertNotIn("secret", context.exception.message)
        with patch.object(discover_endpoints, "discover", return_value=inventory()):
            query_traces.verify_endpoint(endpoint)
        no_auth = {**endpoint, "auth": "none"}
        with patch.object(discover_endpoints, "discover", return_value=inventory(no_auth)):
            query_traces.verify_endpoint(no_auth)
            with self.assertRaises(query_traces.QueryError):
                query_traces.verify_endpoint({**no_auth, "url": endpoint["url"] + "?forged=1"})

    def test_duplicate_named_endpoint_is_ambiguous(self) -> None:
        data = inventory()
        data["query_endpoints"].append(data["query_endpoints"][0].copy())
        with self.assertRaises(query_traces.QueryError) as context:
            query_traces.select_endpoint(data, data["query_endpoints"][0]["name"], "search")
        self.assertEqual(context.exception.kind, "ambiguous_endpoint")

    @patch.dict(os.environ, {"TRACE_TOKEN": "test-token"})
    def test_builds_search_request_with_tenant_and_filters(self) -> None:
        args = search_args(service="checkout", operation="GET /orders", status="error", tag=["http.method=GET"])
        metadata = query_traces.query_metadata(args, "search")
        url, headers = query_traces.build_request(inventory()["query_endpoints"][0], args, metadata)
        parsed = urlsplit(url)
        self.assertEqual(parsed.path, "/api/traces/v1/redhat-ods-monitoring/tempo/api/search")
        params = parse_qs(parsed.query)
        self.assertEqual(params["limit"], ["100"])
        self.assertEqual(
            params["q"],
            ['{ status = error && resource.service.name = "checkout" && name = "GET /orders" }'],
        )
        self.assertNotIn("status", params)
        self.assertEqual(params["tags"], ["http.method=GET"])
        self.assertEqual(headers["X-Scope-OrgID"], "redhat-ods-monitoring")
        self.assertEqual(headers["Authorization"], "Bearer test-token")

    @patch.dict(os.environ, {"TRACE_TOKEN": "test-token"})
    def test_builds_logfmt_tags_with_spaces_and_commas(self) -> None:
        args = search_args(
            service="checkout,blue",
            operation="GET /orders",
            tag=["http.route=/orders,bulk", "http.target=/v1/orders 42"],
        )
        metadata = query_traces.query_metadata(args, "search")
        url, _ = query_traces.build_request(inventory()["query_endpoints"][0], args, metadata)
        params = parse_qs(urlsplit(url).query)
        self.assertEqual(
            params["tags"],
            ['service.name=checkout,blue name="GET /orders" http.route=/orders,bulk http.target="/v1/orders 42"'],
        )

    def test_rejects_status_with_query(self) -> None:
        with self.assertRaises(query_traces.QueryError) as context:
            query_traces.query_metadata(search_args(status="error", query="{ .http.status_code = 500 }"), "search")
        self.assertEqual(context.exception.kind, "invalid_argument")

    def test_filters_reject_compound_traceql_without_changing_unfiltered_queries(self) -> None:
        for query in ('{ .a = 1 } || { .b = 2 }', '{ .a = 1 || .b = 2 }',
                      '{ .a = `x\\` || .b = `y` }',
                      '({ .a = 1 } && { .b = 2 })', '{ .a = 1 } | count()'):
            for option in ("service", "operation"):
                with self.subTest(query=query, option=option):
                    args = search_args(query=query, **{option: "checkout"})
                    with self.assertRaises(query_traces.QueryError) as context:
                        query_traces.build_traceql_query(args)
                    self.assertEqual(context.exception.kind, "invalid_argument")
                    self.assertIn("compound", context.exception.message)
            self.assertEqual(query_traces.build_traceql_query(search_args(query=query)), query)
        for query in ('{ .name = "a || } { b" }', '{ .name = `a || } { b` }',
                      '{ .name = "a \\\" || b" }'):
            with self.subTest(query=query):
                result = query_traces.build_traceql_query(search_args(query=query, service="checkout"))
                self.assertIn('&& resource.service.name = "checkout"', result)
        self.assertEqual(
            query_traces.build_traceql_query(search_args(query='{ .a = 1 }', operation="work")),
            '{ .a = 1 && name = "work" }',
        )

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

    @patch.dict(os.environ, {"TRACE_TOKEN": "test-token"})
    def test_rounds_search_timestamps_outward_to_epoch_seconds(self) -> None:
        args = search_args(start="1704067200.1", end="1704067200.9")
        metadata = query_traces.query_metadata(args, "search")
        url, _ = query_traces.build_request(inventory()["query_endpoints"][0], args, metadata)
        params = parse_qs(urlsplit(url).query)
        self.assertEqual(params["start"], ["1704067200"])
        self.assertEqual(params["end"], ["1704067201"])

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

    def test_deadline_interrupts_slow_headers_bodies_and_http_errors(self) -> None:
        class SlowHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                mode = self.path.rsplit("/", 1)[-1]
                try:
                    if mode == "headers":
                        for byte in b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n":
                            self.connection.sendall(bytes([byte]))
                            time.sleep(0.04)
                    else:
                        time.sleep(0.13)
                        self.send_response(500 if mode == "error" else 200)
                        self.send_header("Content-Length", "1000")
                        self.end_headers()
                        for _ in range(1000):
                            self.wfile.write(b"x")
                            self.wfile.flush()
                            time.sleep(0.04)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, format: str, *args: object) -> None:
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), SlowHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join)
        self.addCleanup(server.shutdown)
        args = Namespace(timeout=0.25, max_response_bytes=2048, ca_file=None)
        previous_handler = signal.getsignal(signal.SIGALRM)
        for mode in ("headers", "body", "error"):
            with self.subTest(mode=mode):
                start = time.monotonic()
                with self.assertRaises(query_traces.QueryError) as context:
                    query_traces.request_tempo(f"http://127.0.0.1:{server.server_port}/{mode}", {}, args, "search")
                self.assertEqual(context.exception.kind, "transport_error")
                self.assertIn("deadline exceeded", context.exception.message)
                self.assertLess(time.monotonic() - start, 1.5)
                self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0.0, 0.0))
                self.assertEqual(signal.getsignal(signal.SIGALRM), previous_handler)

    def test_connection_deadline_does_not_retry_next_address(self) -> None:
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                requests.append(self.path)
                body = b'{"traces": []}'
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join)
        self.addCleanup(server.shutdown)

        real_socket = socket.socket
        addresses = [
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("192.0.2.1", server.server_port)),
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("127.0.0.1", server.server_port)),
        ]
        args = Namespace(timeout=0.2, max_response_bytes=2048, ca_file=None)
        url = f"http://127.0.0.1:{server.server_port}/api/search"

        for stalled in (True, False):
            with self.subTest(stalled=stalled):
                attempts = []

                class FirstSocket:
                    def settimeout(self, timeout: float) -> None:
                        pass

                    def connect(self, address: tuple[str, int]) -> None:
                        attempts.append(address)
                        if stalled:
                            time.sleep(1)
                        else:
                            raise OSError("first address unavailable")

                    def close(self) -> None:
                        pass

                def socket_factory(*args: object, **kwargs: object) -> object:
                    if not attempts and len(args) == 3 and "fileno" not in kwargs:
                        return FirstSocket()
                    return real_socket(*args, **kwargs)

                with (
                    patch.object(socket, "getaddrinfo", return_value=addresses),
                    patch.object(socket, "socket", side_effect=socket_factory),
                    patch.dict(os.environ, {"no_proxy": "127.0.0.1", "NO_PROXY": "127.0.0.1"}),
                ):
                    if stalled:
                        start = time.monotonic()
                        with self.assertRaises(query_traces.QueryError) as context:
                            query_traces.request_tempo(url, {}, args, "search")
                        self.assertEqual(context.exception.kind, "transport_error")
                        self.assertIn("deadline exceeded", context.exception.message)
                        self.assertLess(time.monotonic() - start, 1.0)
                        self.assertEqual(requests, [])
                    else:
                        self.assertEqual(query_traces.request_tempo(url, {}, args, "search"), {"traces": []})
                        self.assertEqual(requests, ["/api/search"])
                self.assertEqual(attempts, [addresses[0][4]])

    def test_deadline_preserves_existing_timer_and_rejects_worker_thread(self) -> None:
        handler = signal.getsignal(signal.SIGALRM)
        signal.setitimer(signal.ITIMER_REAL, 10)
        try:
            with self.assertRaises(query_traces.QueryError):
                with query_traces.request_deadline(0.1):
                    self.fail("active timer must not be replaced")
            self.assertGreater(signal.getitimer(signal.ITIMER_REAL)[0], 0)
            self.assertEqual(signal.getsignal(signal.SIGALRM), handler)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
        errors = []

        def run() -> None:
            try:
                with query_traces.request_deadline(0.1):
                    self.fail("worker thread must not take over SIGALRM")
            except query_traces.QueryError as error:
                errors.append(error.kind)

        thread = threading.Thread(target=run)
        thread.start()
        thread.join()
        self.assertEqual(errors, ["transport_error"])

    def test_response_size_limit_remains_enforced(self) -> None:
        response = FakeResponse({"traces": [{"large": "x" * 100}]})
        with self.assertRaises(query_traces.QueryError) as context:
            query_traces.read_limited(response, 10)
        self.assertEqual(context.exception.kind, "response_too_large")
        response.headers = {}
        with self.assertRaises(query_traces.QueryError) as context:
            query_traces.read_limited(response, 10)
        self.assertEqual(context.exception.kind, "response_too_large")

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

    def test_rejects_invalid_tag_key(self) -> None:
        with self.assertRaises(query_traces.QueryError) as context:
            query_traces.query_metadata(search_args(tag=["http route=value"]), "search")
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
        query_traces.query_metadata(search_args(tag=["http.route=/a,b"]), "search")
        with self.assertRaises(query_traces.QueryError):
            query_traces.query_metadata(search_args(min_duration="1d"), "search")
        with self.assertRaises(query_traces.QueryError):
            query_traces.query_metadata(
                search_args(min_duration="5s", max_duration="1s"), "search"
            )


if __name__ == "__main__":
    unittest.main()
