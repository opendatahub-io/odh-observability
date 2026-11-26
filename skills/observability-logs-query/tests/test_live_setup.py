#!/usr/bin/env python3

from __future__ import annotations

import os
import io
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts"))
sys.path.insert(0, SCRIPT_DIR)

import validate_live_setup  # noqa: E402
import sanitize_live_result  # noqa: E402


def role(name: str) -> dict:
    return {
        "metadata": {"name": name},
        "rules": [
            {
                "apiGroups": ["loki.grafana.com"],
                "resources": ["application"],
                "resourceNames": ["logs"],
                "verbs": ["get"],
            }
        ],
    }


def namespace_role(name: str) -> dict:
    return {
        "metadata": {"name": name},
        "rules": [{"apiGroups": [""], "resources": ["namespaces"], "verbs": ["get", "list", "watch"]}],
    }


class LiveSetupContractTests(unittest.TestCase):
    def test_documented_recovery_is_scoped_and_requires_confirmation(self) -> None:
        skill = (Path(SCRIPT_DIR).parent / "SKILL.md").read_text(encoding="utf-8")
        setup = skill.split("### Live cleanup", 1)[0]
        self.assertEqual(setup.count('observability.opendatahub.io/live-test-run: "${RUN_ID}"'), 6)
        recovery = skill.split("Identify an abandoned run", 1)[1].split("```bash\n", 1)[1].split("\n```", 1)[0]
        run_id = "abcdef0123456789abcdef0123456789"
        cases = (
            ("confirmed", f"{run_id}\n{run_id}\n", False, 4),
            ("wrong confirmation", f"{run_id}\nother\n", False, 0),
            ("invalid ID", "invalid\n", False, 0),
            ("listing failed", f"{run_id}\n{run_id}\n", True, 0),
        )
        with tempfile.TemporaryDirectory() as directory:
            oc = Path(directory) / "oc"
            oc.write_text(
                '#!/bin/sh\nprintf "%s\\n" "$*" >> "$OC_CALLS"\n'
                '[ "${OC_FAIL_GET:-}" != 1 ] || [ "$1" != get ]\n',
                encoding="utf-8",
            )
            oc.chmod(0o755)
            for name, user_input, fail_get, expected_deletes in cases:
                with self.subTest(name=name):
                    calls = Path(directory) / "calls"
                    calls.write_text("", encoding="utf-8")
                    env = {**os.environ, "PATH": f"{directory}:{os.environ['PATH']}", "OC_CALLS": str(calls)}
                    env["OC_FAIL_GET"] = "1" if fail_get else "0"
                    subprocess.run(["bash", "-c", recovery], input=user_input, text=True,
                                   capture_output=True, check=True, env=env)
                    deletes = [line for line in calls.read_text(encoding="utf-8").splitlines()
                               if line.startswith("delete ")]
                    self.assertEqual(len(deletes), expected_deletes)
                    for command in deletes:
                        self.assertIn(f"observability.opendatahub.io/live-test-run={run_id}", command)
                        self.assertIn("app.kubernetes.io/part-of=observability-logs-query-live-test", command)

    def test_live_result_sanitizer_removes_raw_data_and_logql(self) -> None:
        payload = {
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
                "logql": "{secret=\"do-not-print\"}",
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-01-01T00:05:00Z",
                "range_seconds": 300,
                "limit": 5,
                "direction": "backward",
            },
            "data": {
                "resultType": "streams",
                "result": [{"stream": {"app": "test"}, "values": [["1", "secret line"]]}],
            },
            "warnings": [],
        }
        output = io.StringIO()
        with redirect_stdout(output):
            summary, status = sanitize_live_result.sanitize(payload)
        self.assertEqual(status, 0)
        self.assertIsNotNone(summary)
        rendered = json.dumps(summary)
        self.assertNotIn("secret line", rendered)
        self.assertNotIn("do-not-print", rendered)
        self.assertNotIn("data", summary)
        self.assertNotIn("logql", summary["query"])
        self.assertEqual(summary["result"]["value_count"], 1)

    def test_oc_reads_are_time_bounded(self) -> None:
        with patch(
            "validate_live_setup.subprocess.run",
            side_effect=subprocess.TimeoutExpired(["oc"], 30.0),
        ):
            with self.assertRaises(validate_live_setup.SetupError) as context:
                validate_live_setup.run_oc_json("pod", "logs-query-test", "test-ns")
        self.assertIn("timed out", str(context.exception))

    def test_accepts_least_privilege_reader_roles(self) -> None:
        validate_live_setup.validate_cluster_role(role("reader"), "reader")
        validate_live_setup.validate_namespace_role(namespace_role("namespace-reader"), "namespace-reader")

    def test_rejects_write_or_broad_loki_permissions(self) -> None:
        broad = role("reader")
        broad["rules"][0]["verbs"] = ["get", "create"]
        with self.assertRaises(validate_live_setup.SetupError):
            validate_live_setup.validate_cluster_role(broad, "reader")

        broad = role("reader")
        broad["rules"][0]["resources"] = ["application", "infrastructure"]
        with self.assertRaises(validate_live_setup.SetupError):
            validate_live_setup.validate_cluster_role(broad, "reader")

    def test_accepts_narrow_additive_gateway_policy(self) -> None:
        policy = {
            "metadata": {"name": "gateway-test"},
            "spec": {
                "podSelector": {
                    "matchLabels": {
                        "app.kubernetes.io/component": "lokistack-gateway",
                        "app.kubernetes.io/instance": "data-science-lokistack",
                        "app.kubernetes.io/name": "lokistack",
                    }
                },
                "policyTypes": ["Ingress"],
                "ingress": [
                    {
                        "from": [
                            {
                                "namespaceSelector": {
                                    "matchLabels": {"kubernetes.io/metadata.name": "test-ns"}
                                },
                                "podSelector": {
                                    "matchLabels": {
                                        "app.kubernetes.io/component": "logs-query-test",
                                        "app.kubernetes.io/name": "observability-logs-query",
                                    }
                                },
                            }
                        ],
                        "ports": [{"protocol": "TCP", "port": 8080}],
                    }
                ],
            },
        }
        validate_live_setup.validate_network_policy(policy, "gateway-test", "test-ns")

    def test_rejects_unscoped_gateway_policy(self) -> None:
        policy = {
            "metadata": {"name": "gateway-test"},
            "spec": {
                "podSelector": {
                    "matchLabels": {
                        "app.kubernetes.io/component": "lokistack-gateway",
                        "app.kubernetes.io/instance": "data-science-lokistack",
                        "app.kubernetes.io/name": "lokistack",
                    }
                },
                "policyTypes": ["Ingress"],
                "ingress": [{"ports": [{"protocol": "TCP", "port": 8080}], "from": [{}]}],
            },
        }
        with self.assertRaises(validate_live_setup.SetupError):
            validate_live_setup.validate_network_policy(policy, "gateway-test", "test-ns")


if __name__ == "__main__":
    unittest.main()
