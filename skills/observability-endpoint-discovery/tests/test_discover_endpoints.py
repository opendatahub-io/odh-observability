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

    def test_named_target_port_matches_service_port_name(self) -> None:
        service = {
            "metadata": {"name": "gateway", "namespace": "monitoring"},
            "spec": {
                "ports": [{"name": "https", "port": 8443, "targetPort": "web"}]
            },
        }
        slices = {
            "gateway": [
                {
                    "ports": [{"name": "https", "port": 8443}],
                    "endpoints": [
                        {"addresses": ["10.0.0.1"], "conditions": {"ready": True}}
                    ],
                }
            ]
        }

        self.assertTrue(
            discover_endpoints.service_has_ready_endpoint(
                service, slices, service["spec"]["ports"][0]
            )
        )

    def test_named_target_port_matches_empty_service_port_name(self) -> None:
        service = {
            "metadata": {"name": "gateway", "namespace": "monitoring"},
            "spec": {"ports": [{"port": 8443, "targetPort": "web"}]},
        }
        slices = {
            "gateway": [
                {
                    "ports": [{"name": "", "port": 8443}],
                    "endpoints": [
                        {"addresses": ["10.0.0.1"], "conditions": {"ready": True}}
                    ],
                }
            ]
        }

        self.assertTrue(
            discover_endpoints.service_has_ready_endpoint(
                service, slices, service["spec"]["ports"][0]
            )
        )

    @patch.object(discover_endpoints, "discover")
    def test_dsci_unavailability_is_failure(self, mock_discover: object) -> None:
        mock_discover.return_value = {"unavailable": [{"component": "dsci"}]}  # type: ignore[attr-defined]
        with patch.object(sys, "argv", ["discover_endpoints.py"]), patch("builtins.print"):
            self.assertEqual(discover_endpoints.main(), 2)

    def test_datasource_query_parameters_redact_values_and_sensitive_keys(self) -> None:
        self.assertEqual(
            discover_endpoints.sanitize_query_parameters(
                {
                    "namespace": "${namespace:queryparam}",
                    "api_key": "do-not-record",
                    "safe": "also-do-not-record",
                }
            ),
            {"namespace": "<redacted>", "safe": "<redacted>"},
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
            with self.assertRaises(discover_endpoints.OcError) as context:
                discover_endpoints.run_oc(["get", "services"])

            self.assertNotIn("forbidden", str(context.exception))
            self.assertEqual(
                discover_endpoints.oc_error_category("forbidden: user=alice"),
                "permission denied",
            )

    def test_run_oc_timeout_is_a_discovery_error(self) -> None:
        with patch.object(
            discover_endpoints.subprocess,
            "run",
            side_effect=subprocess.TimeoutExpired(
                cmd=["oc"], timeout=discover_endpoints.OC_COMMAND_TIMEOUT_SECONDS
            ),
        ) as mock_run:
            with self.assertRaises(discover_endpoints.OcError) as context:
                discover_endpoints.run_oc(["get", "services"])

            self.assertEqual(str(context.exception), "request timed out")
            self.assertEqual(
                mock_run.call_args.kwargs["timeout"],
                discover_endpoints.OC_COMMAND_TIMEOUT_SECONDS,
            )

    def test_authoritative_platform_thanos_service_requires_all_identity_inputs(
        self,
    ) -> None:
        platform_prometheus = {
            "status": {
                "conditions": [{"type": "Available", "status": "True"}]
            }
        }
        labels = {
            "app.kubernetes.io/managed-by": "cluster-monitoring-operator",
            "app.kubernetes.io/part-of": "openshift-monitoring",
            "app.kubernetes.io/component": "query-layer",
            "app.kubernetes.io/instance": "thanos-querier",
            "app.kubernetes.io/name": "thanos-query",
        }
        service = {
            "metadata": {"namespace": "openshift-monitoring", "labels": labels}
        }

        self.assertTrue(
            discover_endpoints.is_authoritative_platform_thanos_service(
                service, platform_prometheus
            )
        )
        self.assertFalse(
            discover_endpoints.is_authoritative_platform_thanos_service(
                {"metadata": {"namespace": "tenant", "labels": labels}},
                platform_prometheus,
            )
        )
        for label_name, label_value in (
            ("app.kubernetes.io/managed-by", None),
            ("app.kubernetes.io/managed-by", "other-controller"),
            ("app.kubernetes.io/component", None),
            ("app.kubernetes.io/component", "other-component"),
        ):
            with self.subTest(label_name=label_name, label_value=label_value):
                altered_labels = labels.copy()
                if label_value is None:
                    altered_labels.pop(label_name)
                else:
                    altered_labels[label_name] = label_value
                self.assertFalse(
                    discover_endpoints.is_authoritative_platform_thanos_service(
                        {
                            "metadata": {
                                "namespace": "openshift-monitoring",
                                "labels": altered_labels,
                            }
                        },
                        platform_prometheus,
                    )
                )

        self.assertFalse(
            discover_endpoints.is_authoritative_platform_thanos_service(
                service,
                {"status": {"conditions": [{"type": "Available", "status": "False"}]}},
            )
        )
        self.assertFalse(
            discover_endpoints.is_authoritative_platform_thanos_service(service, None)
        )


if __name__ == "__main__":
    unittest.main()
