#!/usr/bin/env python3
"""Validate the temporary, least-privilege live Loki query test setup."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
OC_SUBPROCESS_TIMEOUT_SECONDS = 30.0


class SetupError(RuntimeError):
    """A safe live-setup validation failure."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SetupError(message)


def run_oc_json(kind: str, name: str, namespace: str | None = None) -> dict[str, Any]:
    command = ["oc", "get", kind, name, "-o", "json"]
    if namespace:
        command.extend(["-n", namespace])
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=OC_SUBPROCESS_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as error:
        raise SetupError(
            f"oc timed out after {OC_SUBPROCESS_TIMEOUT_SECONDS:g} seconds reading {kind}/{name}"
        ) from error
    except OSError as error:
        raise SetupError(f"could not execute oc: {error}") from error
    if completed.returncode != 0:
        raise SetupError(f"oc could not read {kind}/{name}")
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise SetupError(f"oc returned invalid JSON for {kind}/{name}") from error
    if not isinstance(value, dict):
        raise SetupError(f"oc returned a non-object for {kind}/{name}")
    return value


def validate_namespace(namespace: dict[str, Any], expected_name: str) -> None:
    metadata = namespace.get("metadata") or {}
    require(metadata.get("name") == expected_name, "temporary namespace name is unexpected")
    labels = metadata.get("labels") or {}
    require(
        labels.get("kubernetes.io/metadata.name") == expected_name,
        "temporary namespace is missing its Kubernetes metadata label",
    )


def validate_service_account(service_account: dict[str, Any], expected_name: str) -> None:
    metadata = service_account.get("metadata") or {}
    require(metadata.get("name") == expected_name, "temporary service account name is unexpected")


def validate_cluster_role(role: dict[str, Any], expected_name: str) -> None:
    metadata = role.get("metadata") or {}
    require(metadata.get("name") == expected_name, "temporary reader ClusterRole name is unexpected")
    require(
        role.get("rules")
        == [
            {
                "apiGroups": ["loki.grafana.com"],
                "resources": ["application"],
                "resourceNames": ["logs"],
                "verbs": ["get"],
            }
        ],
        "temporary reader ClusterRole is broader than the Loki read permission",
    )


def validate_namespace_role(role: dict[str, Any], expected_name: str) -> None:
    metadata = role.get("metadata") or {}
    require(metadata.get("name") == expected_name, "namespace reader ClusterRole name is unexpected")
    require(
        role.get("rules")
        == [
            {
                "apiGroups": [""],
                "resources": ["namespaces"],
                "verbs": ["get", "list", "watch"],
            }
        ],
        "namespace reader ClusterRole is broader than namespace metadata read access",
    )


def validate_binding(
    binding: dict[str, Any], expected_name: str, role_name: str, namespace: str, service_account: str
) -> None:
    metadata = binding.get("metadata") or {}
    require(metadata.get("name") == expected_name, "temporary ClusterRoleBinding name is unexpected")
    require(
        binding.get("roleRef")
        == {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": role_name},
        "temporary ClusterRoleBinding role reference is unexpected",
    )
    require(
        binding.get("subjects") == [{"kind": "ServiceAccount", "name": service_account, "namespace": namespace}],
        "temporary ClusterRoleBinding subject is unexpected",
    )


def validate_network_policy(policy: dict[str, Any], expected_name: str, test_namespace: str) -> None:
    metadata = policy.get("metadata") or {}
    require(metadata.get("name") == expected_name, "temporary NetworkPolicy name is unexpected")
    spec = policy.get("spec") or {}
    require(
        spec.get("podSelector", {}).get("matchLabels")
        == {
            "app.kubernetes.io/component": "lokistack-gateway",
            "app.kubernetes.io/instance": "data-science-lokistack",
            "app.kubernetes.io/name": "lokistack",
        },
        "temporary NetworkPolicy selects an unexpected workload",
    )
    require(spec.get("policyTypes") == ["Ingress"], "temporary NetworkPolicy must only declare ingress")
    ingress = spec.get("ingress") or []
    require(len(ingress) == 1, "temporary NetworkPolicy must contain one ingress rule")
    rule = ingress[0]
    require(
        rule.get("ports") == [{"protocol": "TCP", "port": 8080}],
        "temporary NetworkPolicy must allow only TCP/8080",
    )
    require(len(rule.get("from") or []) == 1, "temporary NetworkPolicy must contain one source peer")
    peer = rule["from"][0]
    require(
        peer.get("namespaceSelector", {}).get("matchLabels")
        == {"kubernetes.io/metadata.name": test_namespace},
        "temporary NetworkPolicy namespace selector is unexpected",
    )
    require(
        peer.get("podSelector", {}).get("matchLabels")
        == {
            "app.kubernetes.io/component": "logs-query-test",
            "app.kubernetes.io/name": "observability-logs-query",
        },
        "temporary NetworkPolicy pod selector is unexpected",
    )


def validate_ca_config_map(config_map: dict[str, Any]) -> None:
    data = config_map.get("data") or {}
    require(isinstance(data.get("service-ca.crt"), str) and bool(data["service-ca.crt"]), "service CA is missing")


def validate_pod(pod: dict[str, Any], expected_name: str, service_account: str) -> None:
    metadata = pod.get("metadata") or {}
    require(metadata.get("name") == expected_name, "live query pod name is unexpected")
    labels = metadata.get("labels") or {}
    require(
        labels.get("app.kubernetes.io/component") == "logs-query-test"
        and labels.get("app.kubernetes.io/name") == "observability-logs-query",
        "live query pod labels are unexpected",
    )
    require((pod.get("spec") or {}).get("serviceAccountName") == service_account, "live query pod SA is unexpected")
    require((pod.get("status") or {}).get("phase") == "Succeeded", "live query pod did not succeed")


def validate_result(path: str) -> None:
    try:
        raw = Path(path).read_text(encoding="utf-8")
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError) as error:
        raise SetupError(f"could not read live query result: {error}") from error
    require(isinstance(payload, dict), "live query result is not an object")
    require(payload.get("status") == "success", "live query result is not successful")
    require("data" not in payload, "live query result must not contain raw Loki data")
    endpoint = payload.get("endpoint") or {}
    require(endpoint.get("auth") == "caller-bearer", "live result did not use caller-bearer auth")
    require(endpoint.get("tls") == "service-ca", "live result did not use service-ca TLS")
    require(endpoint.get("tenant") == "application", "live result did not use the application tenant")
    query = payload.get("query") or {}
    require(isinstance(query, dict), "live query summary is missing query metadata")
    require("logql" not in query, "live query summary must not contain the raw LogQL query")
    result = payload.get("result") or {}
    require(isinstance(result, dict), "live query summary is missing result metadata")
    require(result.get("result_type") in {"streams", "matrix"}, "live result type is invalid")
    for field in ("stream_count", "value_count"):
        value = result.get(field)
        require(isinstance(value, int) and not isinstance(value, bool) and value >= 0, f"live {field} is invalid")
    warning_count = payload.get("warning_count")
    require(
        isinstance(warning_count, int) and not isinstance(warning_count, bool) and warning_count >= 0,
        "live warning count is invalid",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--monitoring-namespace", default="redhat-ods-monitoring")
    parser.add_argument("--test-namespace", required=True)
    parser.add_argument("--service-account", default="logs-query-test")
    parser.add_argument("--reader-role", default="rhai-15151-logs-query-reader")
    parser.add_argument("--reader-binding", default="rhai-15151-logs-query-reader")
    parser.add_argument("--namespace-reader-role", default="rhai-15151-logs-query-namespace-reader")
    parser.add_argument("--namespace-reader-binding", default="rhai-15151-logs-query-namespace-reader")
    parser.add_argument("--network-policy", default="rhai-15151-logs-query-gateway-ingress")
    parser.add_argument("--ca-configmap", default="loki-gateway-ca")
    parser.add_argument("--pod", default="logs-query-test")
    parser.add_argument("--result", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        validate_namespace(run_oc_json("namespace", args.test_namespace), args.test_namespace)
        validate_service_account(
            run_oc_json("serviceaccount", args.service_account, args.test_namespace), args.service_account
        )
        validate_cluster_role(run_oc_json("clusterrole", args.reader_role), args.reader_role)
        validate_namespace_role(run_oc_json("clusterrole", args.namespace_reader_role), args.namespace_reader_role)
        validate_binding(
            run_oc_json("clusterrolebinding", args.reader_binding),
            args.reader_binding,
            args.reader_role,
            args.test_namespace,
            args.service_account,
        )
        validate_binding(
            run_oc_json("clusterrolebinding", args.namespace_reader_binding),
            args.namespace_reader_binding,
            args.namespace_reader_role,
            args.test_namespace,
            args.service_account,
        )
        validate_network_policy(
            run_oc_json("networkpolicy", args.network_policy, args.monitoring_namespace),
            args.network_policy,
            args.test_namespace,
        )
        validate_ca_config_map(run_oc_json("configmap", args.ca_configmap, args.test_namespace))
        validate_pod(run_oc_json("pod", args.pod, args.test_namespace), args.pod, args.service_account)
        validate_result(args.result)
    except SetupError as error:
        print(f"live logs query setup validation failed: {error}", file=sys.stderr)
        return 1
    print("live logs query setup validation passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
