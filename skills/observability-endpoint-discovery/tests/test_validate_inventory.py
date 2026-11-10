#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import unittest


SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts"))
sys.path.insert(0, SCRIPT_DIR)

import validate_inventory  # noqa: E402


def endpoint(**extra: object) -> dict[str, object]:
    value: dict[str, object] = {
        "signal": "metrics",
        "name": "prometheus",
        "url": "https://prometheus.example.test",
        "access": "route",
        "scope": "cluster",
        "auth": "caller-bearer",
        "tls": "route-reencrypt",
        "paths": ["/api/v1/query"],
        "state": "ready",
        "source": "Route/monitoring/prometheus",
    }
    value.update(extra)
    return value


class ValidateInventoryTests(unittest.TestCase):
    def test_query_parameters_can_be_omitted_or_null(self) -> None:
        self.assertIsNone(
            validate_inventory.validate_endpoint("diagnostic_endpoints", 0, endpoint())
        )
        self.assertIsNone(
            validate_inventory.validate_endpoint(
                "diagnostic_endpoints", 0, endpoint(query_parameters=None)
            )
        )

    def test_query_parameters_must_contain_only_redacted_values(self) -> None:
        self.assertIsNone(
            validate_inventory.validate_endpoint(
                "diagnostic_endpoints",
                0,
                endpoint(query_parameters={"namespace": "<redacted>"}),
            )
        )
        for query_parameters in (
            [],
            {"namespace": "workload"},
            {"api_key": "<redacted>"},
        ):
            with self.subTest(query_parameters=query_parameters):
                self.assertIsNotNone(
                    validate_inventory.validate_endpoint(
                        "diagnostic_endpoints",
                        0,
                        endpoint(query_parameters=query_parameters),
                    )
                )


if __name__ == "__main__":
    unittest.main()
