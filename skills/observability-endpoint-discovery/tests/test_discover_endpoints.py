#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import sys
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit


SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts"))
sys.path.insert(0, SCRIPT_DIR)

import discover_endpoints  # noqa: E402


class DiscoverEndpointsTests(unittest.TestCase):
    def test_service_endpoint_preserves_query_base_path(self) -> None:
        output = {"query_endpoints": []}
        service = {
            "metadata": {"name": "tempo-gateway", "namespace": "monitoring"},
            "spec": {"ports": [{"port": 8080}]},
        }
        slices = {
            "tempo-gateway": [
                {
                    "ports": [{"port": 8080}],
                    "endpoints": [
                        {"addresses": ["10.0.0.1"], "conditions": {"ready": True}}
                    ],
                }
            ]
        }

        discover_endpoints.add_service_endpoint(
            output,
            service=service,
            port={"port": 8080},
            slices_by_service=slices,
            signal="traces",
            name="tempo-gateway",
            scheme="https",
            scope="monitoring",
            auth="caller-bearer",
            tls="service-ca",
            paths=["/api/search"],
            condition=None,
            conditions={},
            url_path="/api/traces/v1/monitoring/tempo",
        )

        self.assertEqual(
            output["query_endpoints"][0]["url"],
            "https://tempo-gateway.monitoring.svc.cluster.local:8080/"
            "api/traces/v1/monitoring/tempo",
        )

    def test_cluster_service_name_requires_full_current_namespace_dns(self) -> None:
        self.assertEqual(
            discover_endpoints.cluster_service_name(
                urlsplit("https://gateway.monitoring.svc.cluster.local:8080/path"),
                "monitoring",
            ),
            "gateway",
        )
        for hostname in (
            "gateway.other.svc.cluster.local",
            "gateway.monitoring.svc.cluster.local.attacker.example",
            "gateway.extra.monitoring.svc.cluster.local",
            "gateway.monitoring.svc",
        ):
            with self.subTest(hostname=hostname):
                self.assertIsNone(
                    discover_endpoints.cluster_service_name(
                        urlsplit(f"https://{hostname}:8080/path"), "monitoring"
                    )
                )

    @patch.object(discover_endpoints, "run_oc")
    def test_required_operand_read_propagates_rbac_error(self, mock_run_oc: object) -> None:
        mock_run_oc.side_effect = discover_endpoints.OcError("forbidden")  # type: ignore[attr-defined]

        with self.assertRaises(discover_endpoints.OcError) as context:
            discover_endpoints.required_oc(
                ["get", "services", "-o", "json"], "monitoring", "operand Services"
            )

        self.assertIn("operand Services could not be read", str(context.exception))

    @patch.object(discover_endpoints, "discover")
    def test_discovery_error_returns_nonzero(self, mock_discover: object) -> None:
        mock_discover.side_effect = discover_endpoints.OcError("operand Services denied")  # type: ignore[attr-defined]
        with patch.object(sys, "argv", ["discover_endpoints.py"]), patch("builtins.print"):
            self.assertEqual(discover_endpoints.main(), 2)

    def test_run_oc_error_is_a_discovery_error(self) -> None:
        with patch.object(
            discover_endpoints, "subprocess"
        ) as mock_subprocess:
            mock_subprocess.run.return_value = subprocess.CompletedProcess(
                args=[], returncode=1, stdout="", stderr="forbidden"
            )
            with self.assertRaises(discover_endpoints.OcError):
                discover_endpoints.run_oc(["get", "services"])


if __name__ == "__main__":
    unittest.main()
