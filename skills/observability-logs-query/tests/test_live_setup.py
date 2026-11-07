#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import sys
import unittest
from unittest.mock import patch


SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts"))
sys.path.insert(0, SCRIPT_DIR)

import validate_live_setup  # noqa: E402


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
