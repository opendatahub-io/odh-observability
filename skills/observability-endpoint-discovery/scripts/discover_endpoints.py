#!/usr/bin/env python3
"""Discover RHOAI observability endpoints without reading Secrets.

The script intentionally uses only the OpenShift CLI and Python's standard
library. It emits the handoff shape described by the parent SKILL.md and keeps
resource readiness separate from caller reachability.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from typing import Any
from urllib.parse import urlparse


READY_STATES = {"ready", "configured", "pending", "not-ready", "unavailable"}


class OcError(RuntimeError):
    """Raised when a required oc command cannot be completed."""


def run_oc(args: list[str], namespace: str | None = None) -> Any:
    command = ["oc"]
    if namespace:
        command.extend(["-n", namespace])
    command.extend(args)
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        detail = result.stderr.strip().splitlines()
        raise OcError(detail[-1] if detail else f"oc exited with {result.returncode}")
    if "-o" in args and "json" in args:
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise OcError(f"oc returned invalid JSON: {error}") from error
    return result.stdout.strip()


def optional_oc(
    args: list[str],
    namespace: str | None,
    warnings: list[str],
    description: str,
) -> Any | None:
    try:
        return run_oc(args, namespace)
    except OcError as error:
        warnings.append(f"{description} was not discoverable: {error}")
        return None


def condition_map(resource: dict[str, Any]) -> dict[str, dict[str, Any]]:
    status = resource.get("status") or {}
    conditions = status.get("conditions") or resource.get("conditions") or []
    return {
        condition.get("type", "unknown"): {
            "status": condition.get("status"),
            "reason": condition.get("reason"),
            "message": condition.get("message"),
            "severity": condition.get("severity"),
        }
        for condition in conditions
        if condition.get("type")
    }


def condition_is_true(conditions: dict[str, dict[str, Any]], name: str) -> bool:
    return conditions.get(name, {}).get("status") == "True"


def component_state(
    conditions: dict[str, dict[str, Any]], condition: str, resource_ready: bool
) -> str:
    status = conditions.get(condition, {}).get("status")
    if status == "False":
        return "not-ready"
    if status != "True":
        return "configured"
    return "ready" if resource_ready else "not-ready"


def service_port(
    service: dict[str, Any],
    numbers: tuple[int, ...] = (),
    names: tuple[str, ...] = (),
) -> dict[str, Any] | None:
    ports = (service.get("spec") or {}).get("ports") or []
    for port in ports:
        if port.get("name") in names or port.get("port") in numbers:
            return port
    return None


def endpoint_is_ready(endpoint: dict[str, Any]) -> bool:
    conditions = endpoint.get("conditions") or {}
    # EndpointSlice treats an omitted ready condition as ready. Serving and
    # terminating are checked where present so terminating endpoints are not
    # presented as usable.
    return (
        conditions.get("ready") is not False
        and conditions.get("serving") is not False
        and conditions.get("terminating") is not True
        and bool(endpoint.get("addresses"))
    )


def service_has_ready_endpoint(
    service: dict[str, Any],
    slices_by_service: dict[str, list[dict[str, Any]]],
    port: dict[str, Any] | None,
) -> bool:
    if port is None:
        return False
    service_name = (service.get("metadata") or {}).get("name")
    slices = slices_by_service.get(service_name, [])
    target_port = port.get("targetPort")
    for endpoint_slice in slices:
        endpoint_ports = endpoint_slice.get("ports") or []
        matching_port = any(
            endpoint_port.get("name") == target_port
            if isinstance(target_port, str)
            else endpoint_port.get("port") == target_port
            for endpoint_port in endpoint_ports
        )
        if not matching_port and not isinstance(target_port, str):
            # Some generated EndpointSlices omit the port name when the
            # Service uses a numeric targetPort. In that case, compare the
            # Service port as a conservative fallback.
            matching_port = any(
                endpoint_port.get("port") == port.get("port")
                for endpoint_port in endpoint_ports
            )
        if matching_port and any(
            endpoint_is_ready(endpoint) for endpoint in endpoint_slice.get("endpoints", [])
        ):
            return True
    return False


def route_admission(route: dict[str, Any]) -> tuple[str | None, bool]:
    for ingress in (route.get("status") or {}).get("ingress") or []:
        host = ingress.get("host")
        conditions = ingress.get("conditions") or []
        admitted = any(
            condition.get("type") == "Admitted"
            and condition.get("status") == "True"
            for condition in conditions
        )
        if host and admitted:
            return host, True
    return None, False


def route_target_port(
    route: dict[str, Any], service: dict[str, Any] | None
) -> dict[str, Any] | None:
    if service is None:
        return None
    route_port = ((route.get("spec") or {}).get("port") or {}).get("targetPort")
    if isinstance(route_port, str):
        selected = service_port(service, names=(route_port,))
    elif isinstance(route_port, int):
        selected = service_port(service, numbers=(route_port,))
    else:
        selected = None
    return selected or service_port(service, numbers=(8443,), names=("https",))


def route_tls(route: dict[str, Any]) -> tuple[str, str]:
    termination = ((route.get("spec") or {}).get("tls") or {}).get("termination")
    if not termination:
        return "http", "none"
    normalized = str(termination).lower()
    return "https", f"route-{normalized}"


def parse_http_url(value: Any) -> Any | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = urlparse(value)
        hostname = parsed.hostname
        parsed.port
    except (TypeError, ValueError):
        return None
    if parsed.scheme not in {"http", "https"} or not hostname:
        return None
    if parsed.username or parsed.password:
        return None
    return parsed


def source(kind: str, namespace: str, name: str) -> str:
    return f"{kind}/{namespace}/{name}"


def endpoint_record(
    *,
    signal: str,
    name: str,
    url: str | None,
    access: str,
    scope: str,
    auth: str,
    tls: str,
    paths: list[str],
    state: str,
    source_ref: str,
    **extra: Any,
) -> dict[str, Any]:
    if state not in READY_STATES:
        raise ValueError(f"invalid endpoint state {state!r}")
    record: dict[str, Any] = {
        "signal": signal,
        "name": name,
        "url": url,
        "access": access,
        "scope": scope,
        "auth": auth,
        "tls": tls,
        "paths": paths,
        "required_query_parameters": [],
        "state": state,
        "source": source_ref,
    }
    record.update(extra)
    return record


def add_service_endpoint(
    output: dict[str, Any],
    *,
    service: dict[str, Any],
    port: dict[str, Any] | None,
    slices_by_service: dict[str, list[dict[str, Any]]],
    signal: str,
    name: str,
    scheme: str,
    scope: str,
    auth: str,
    tls: str,
    paths: list[str],
    condition: str | None,
    conditions: dict[str, dict[str, Any]],
    collection: str = "query_endpoints",
    required_query_parameters: list[str] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    service_name = (service.get("metadata") or {}).get("name", "unknown")
    service_namespace = (service.get("metadata") or {}).get("namespace", "unknown")
    ready = service_has_ready_endpoint(service, slices_by_service, port)
    if condition:
        state = component_state(conditions, condition, ready)
    else:
        state = "ready" if ready else "not-ready"
    port_number = port.get("port") if port else None
    url = (
        f"{scheme}://{service_name}.{service_namespace}.svc.cluster.local:{port_number}"
        if port_number
        else None
    )
    record = endpoint_record(
        signal=signal,
        name=name,
        url=url,
        access="cluster-service",
        scope=scope,
        auth=auth,
        tls=tls,
        paths=paths,
        state=state,
        source_ref=source("Service", service_namespace, service_name),
        service_port=port_number,
        **extra,
    )
    if required_query_parameters:
        record["required_query_parameters"] = required_query_parameters
    output[collection].append(record)
    return record


def add_route_endpoint(
    output: dict[str, Any],
    *,
    route: dict[str, Any],
    target_service: dict[str, Any] | None,
    target_port: dict[str, Any] | None,
    slices_by_service: dict[str, list[dict[str, Any]]],
    signal: str,
    name: str,
    scope: str,
    auth: str,
    paths: list[str],
    conditions: dict[str, dict[str, Any]],
    condition: str | None,
    collection: str = "query_endpoints",
    required_query_parameters: list[str] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    metadata = route.get("metadata") or {}
    namespace = metadata.get("namespace", "unknown")
    route_name = metadata.get("name", "unknown")
    host, admitted = route_admission(route)
    route_scheme, route_tls_mode = route_tls(route)
    service_ready = bool(
        target_service
        and service_has_ready_endpoint(target_service, slices_by_service, target_port)
    )
    resource_ready = admitted and service_ready
    if condition:
        state = component_state(conditions, condition, resource_ready)
    else:
        state = "ready" if resource_ready else "pending"
    url = f"{route_scheme}://{host}" if host else None
    record = endpoint_record(
        signal=signal,
        name=name,
        url=url,
        access="route",
        scope=scope,
        auth=auth,
        tls=route_tls_mode,
        paths=paths,
        state=state,
        source_ref=source("Route", namespace, route_name),
        target_service=(target_service.get("metadata") or {}).get("name")
        if target_service
        else (route.get("spec") or {}).get("to", {}).get("name"),
        route_admitted=admitted,
        **extra,
    )
    if required_query_parameters:
        record["required_query_parameters"] = required_query_parameters
    output[collection].append(record)
    return record


def find_route(routes: dict[str, dict[str, Any]], name: str) -> dict[str, Any] | None:
    return routes.get(name)


def empty_inventory() -> dict[str, Any]:
    return {
        "schema_version": "1",
        "cluster": {"server": None, "user": None},
        "monitoring": {
            "resource": "default-monitoring",
            "namespace": None,
            "management_state": None,
            "conditions": {},
            "dsci": None,
            "network_policies": [],
            "platform_metrics_namespace": None,
        },
        "query_endpoints": [],
        "telemetry_ingest": [],
        "diagnostic_endpoints": [],
        "unavailable": [],
        "warnings": [],
    }


def discover() -> dict[str, Any]:
    output = empty_inventory()

    output["cluster"]["server"] = optional_oc(
        ["whoami", "--show-server"], None, output["warnings"], "cluster server"
    )
    output["cluster"]["user"] = optional_oc(
        ["whoami"], None, output["warnings"], "cluster user"
    )

    try:
        monitoring = run_oc(["get", "monitoring", "default-monitoring", "-o", "json"])
    except OcError as error:
        output["unavailable"].append(
            {"component": "monitoring", "reason": f"Monitoring resource unavailable: {error}"}
        )
        return output

    spec = monitoring.get("spec") or {}
    status = monitoring.get("status") or {}
    namespace = spec.get("namespace")
    management_state = spec.get("managementState") or "Managed"
    output["monitoring"]["namespace"] = namespace
    output["monitoring"]["management_state"] = management_state
    output["monitoring"]["conditions"] = condition_map(monitoring)

    if not namespace:
        output["unavailable"].append(
            {"component": "monitoring", "reason": "Monitoring.spec.namespace is empty"}
        )
        return output

    dsci_list = optional_oc(
        ["get", "dsci", "-o", "json"], None, output["warnings"], "DSCI"
    )
    if dsci_list:
        candidates = [
            item
            for item in dsci_list.get("items", [])
            if ((item.get("spec") or {}).get("monitoring") or {}).get("managementState", "Managed")
            != "Removed"
        ]
        if len(candidates) == 1:
            dsci = candidates[0]
            dsci_spec = (dsci.get("spec") or {}).get("monitoring") or {}
            dsci_namespace = dsci_spec.get("namespace")
            output["monitoring"]["dsci"] = {
                "name": (dsci.get("metadata") or {}).get("name"),
                "namespace": dsci_namespace,
                "management_state": dsci_spec.get("managementState") or "Managed",
                "namespace_agrees": dsci_namespace == namespace,
            }
            if dsci_namespace != namespace:
                output["warnings"].append(
                    "DSCI monitoring namespace does not match Monitoring.spec.namespace"
                )
        elif len(candidates) > 1:
            output["unavailable"].append(
                {"component": "dsci", "reason": "Multiple active DSCI resources found"}
            )
            return output
        else:
            output["warnings"].append("No active DSCI resource found; using Monitoring directly")

    if management_state != "Managed":
        output["unavailable"].append(
            {"component": "monitoring", "reason": f"managementState is {management_state}"}
        )
        return output

    routes_obj = optional_oc(
        ["get", "routes.route.openshift.io", "-o", "json"],
        namespace,
        output["warnings"],
        "operand Routes",
    )
    services_obj = optional_oc(
        ["get", "services", "-o", "json"],
        namespace,
        output["warnings"],
        "operand Services",
    )
    slices_obj = optional_oc(
        ["get", "endpointslices.discovery.k8s.io", "-o", "json"],
        namespace,
        output["warnings"],
        "operand EndpointSlices",
    )
    network_policies = optional_oc(
        ["get", "networkpolicies", "-o", "json"],
        namespace,
        output["warnings"],
        "operand NetworkPolicies",
    )

    routes = {
        (item.get("metadata") or {}).get("name"): item
        for item in (routes_obj or {}).get("items", [])
        if (item.get("metadata") or {}).get("name")
    }
    services = {
        (item.get("metadata") or {}).get("name"): item
        for item in (services_obj or {}).get("items", [])
        if (item.get("metadata") or {}).get("name")
    }
    slices_by_service: dict[str, list[dict[str, Any]]] = {}
    for item in (slices_obj or {}).get("items", []):
        service_name = (item.get("metadata") or {}).get("labels", {}).get(
            "kubernetes.io/service-name"
        )
        if service_name:
            slices_by_service.setdefault(service_name, []).append(item)

    output["monitoring"]["network_policies"] = sorted(
        (item.get("metadata") or {}).get("name")
        for item in (network_policies or {}).get("items", [])
        if (item.get("metadata") or {}).get("name")
    )
    if output["monitoring"]["network_policies"]:
        output["warnings"].append(
            "NetworkPolicies exist; endpoint readiness does not prove caller reachability"
        )

    conditions = output["monitoring"]["conditions"]
    metrics_config = spec.get("metrics")
    traces_config = spec.get("traces")
    usage_logs_config = spec.get("usageLogs")
    metrics_storage = isinstance(metrics_config, dict) and metrics_config.get("storage") is not None
    traces_storage = isinstance(traces_config, dict) and traces_config.get("storage") is not None
    trace_storage = (
        traces_config.get("storage")
        if isinstance(traces_config, dict) and isinstance(traces_config.get("storage"), dict)
        else {}
    )
    logs_configured = spec.get("logs") is not None
    usage_logs_storage = (
        isinstance(usage_logs_config, dict) and usage_logs_config.get("storage") is not None
    )

    if metrics_storage:
        thanos_service = services.get("data-science-thanos-querier-proxy")
        thanos_port = service_port(thanos_service or {}, numbers=(8443,), names=("https",))
        add_service_endpoint(
            output,
            service=thanos_service,
            port=thanos_port,
            slices_by_service=slices_by_service,
            signal="metrics",
            name="data-science-thanos",
            scheme="https",
            scope="data-science-namespace",
            auth="caller-bearer",
            tls="service-ca",
            paths=["/api/v1/query", "/api/v1/query_range", "/api/v1/series", "/api/v1/labels"],
            required_query_parameters=["namespace"],
            condition="ThanosQuerierAvailable",
            conditions=conditions,
        ) if thanos_service else output["unavailable"].append(
            {"component": "metrics", "reason": "Thanos proxy Service not found"}
        )

        thanos_route = find_route(routes, "data-science-thanos-querier-route")
        if thanos_route:
            target = services.get(
                (thanos_route.get("spec") or {}).get("to", {}).get("name", "")
            )
            target_port = route_target_port(thanos_route, target)
            route_record = add_route_endpoint(
                output,
                route=thanos_route,
                target_service=target,
                target_port=target_port,
                slices_by_service=slices_by_service,
                signal="metrics",
                name="data-science-thanos-route",
                scope="data-science-namespace",
                auth="caller-bearer",
                paths=["/api/v1/query", "/api/v1/query_range", "/api/v1/series", "/api/v1/labels"],
                required_query_parameters=["namespace"],
                condition="ThanosQuerierAvailable",
                conditions=conditions,
            )
            if status.get("url") and route_record["state"] != "ready":
                output["warnings"].append(
                    "Monitoring.status.url is present but the current Thanos Route is not "
                    "admitted and ready; treating status.url as stale"
                )
        elif status.get("url"):
            output["warnings"].append(
                "Monitoring.status.url is present but the current Thanos Route was not found; treating it as stale"
            )

        namespace_proxy = services.get("data-science-prometheus-namespace-proxy")
        namespace_proxy_port = service_port(
            namespace_proxy or {}, numbers=(8443,), names=("https",)
        )
        if namespace_proxy:
            add_service_endpoint(
                output,
                service=namespace_proxy,
                port=namespace_proxy_port,
                slices_by_service=slices_by_service,
                signal="metrics",
                name="data-science-prometheus-namespace-proxy",
                scheme="https",
                scope="data-science-namespace",
                auth="caller-bearer",
                tls="service-ca",
                paths=["/api/v1/query", "/api/v1/query_range", "/api/v1/series", "/api/v1/labels"],
                required_query_parameters=["namespace"],
                condition="MonitoringStackAvailable",
                conditions=conditions,
                notes="GET/HEAD only; namespace must be URL query data",
            )
        compatibility_service = services.get("data-science-prometheus-namespace-proxy-service")
        compatibility_port = service_port(compatibility_service or {}, numbers=(9090,), names=("web",))
        if compatibility_service:
            add_service_endpoint(
                output,
                service=compatibility_service,
                port=compatibility_port,
                slices_by_service=slices_by_service,
                signal="metrics",
                name="data-science-prometheus-namespace-proxy-service",
                scheme="https",
                scope="data-science-namespace",
                auth="caller-bearer",
                tls="service-ca",
                paths=["/api/v1/query", "/api/v1/query_range", "/api/v1/series", "/api/v1/labels"],
                required_query_parameters=["namespace"],
                condition="MonitoringStackAvailable",
                conditions=conditions,
                notes="Compatibility Service; GET/HEAD only and namespace must be URL query data",
            )
        prometheus_route = find_route(routes, "data-science-prometheus-route")
        if prometheus_route:
            target = services.get(
                (prometheus_route.get("spec") or {}).get("to", {}).get("name", "")
            )
            target_port = route_target_port(prometheus_route, target)
            add_route_endpoint(
                output,
                route=prometheus_route,
                target_service=target,
                target_port=target_port,
                slices_by_service=slices_by_service,
                signal="metrics",
                name="data-science-prometheus-route",
                scope="data-science-namespace",
                auth="caller-bearer",
                paths=["/api/v1/query", "/api/v1/query_range", "/api/v1/series", "/api/v1/labels"],
                required_query_parameters=["namespace"],
                condition="MonitoringStackAvailable",
                conditions=conditions,
                notes="GET/HEAD only; namespace must be URL query data",
            )

    ocp_thanos_result = optional_oc(
        ["get", "services", "--all-namespaces", "-o", "json"],
        None,
        output["warnings"],
        "OpenShift Thanos Service",
    )
    ocp_thanos_candidates = (
        [
            item
            for item in ocp_thanos_result.get("items", [])
            if (item.get("metadata") or {}).get("name") == "thanos-querier"
        ]
        if isinstance(ocp_thanos_result, dict) and isinstance(ocp_thanos_result.get("items"), list)
        else [ocp_thanos_result]
        if isinstance(ocp_thanos_result, dict)
        else []
    )
    if len(ocp_thanos_candidates) == 1:
        ocp_thanos = ocp_thanos_candidates[0]
        platform_namespace = (ocp_thanos.get("metadata") or {}).get("namespace")
        output["monitoring"]["platform_metrics_namespace"] = platform_namespace
        ocp_slices_obj = optional_oc(
            [
                "get",
                "endpointslices.discovery.k8s.io",
                "-l",
                "kubernetes.io/service-name=thanos-querier",
                "-o",
                "json",
            ],
            platform_namespace,
            output["warnings"],
            "OpenShift Thanos EndpointSlices",
        )
        ocp_slices = {
            "thanos-querier": (ocp_slices_obj or {}).get("items", [])
        }
        for port_number, name, scope, parameters in (
            (9091, "cluster-prometheus", "openshift-cluster", []),
            (9092, "cluster-prometheus-tenancy", "openshift-namespace", ["namespace"]),
        ):
            port = service_port(ocp_thanos, numbers=(port_number,))
            if port:
                add_service_endpoint(
                    output,
                    service=ocp_thanos,
                    port=port,
                    slices_by_service=ocp_slices,
                    signal="metrics",
                    name=name,
                    scheme="https",
                    scope=scope,
                    auth="caller-bearer",
                    tls="service-ca",
                    paths=["/api/v1/query", "/api/v1/query_range", "/api/v1/series", "/api/v1/labels"],
                    required_query_parameters=parameters,
                    condition=None,
                    conditions=conditions,
                    notes="OpenShift platform metrics; do not combine with Data Science metrics in one PromQL query",
                )
    elif len(ocp_thanos_candidates) > 1:
        output["unavailable"].append(
            {
                "component": "openshift-metrics",
                "reason": "Multiple Thanos Services named thanos-querier were discovered",
            }
        )

    if logs_configured:
        logs_endpoint = status.get("logsEndpoint")
        if logs_endpoint and condition_is_true(conditions, "LokiStackAvailable"):
            parsed = parse_http_url(logs_endpoint)
            if not parsed:
                output["unavailable"].append(
                    {
                        "component": "logs",
                        "reason": "Monitoring.status.logsEndpoint is not a valid "
                        "credential-free HTTP URL",
                    }
                )
            else:
                gateway_service_name = (parsed.hostname or "").split(".")[0]
                gateway_service = services.get(gateway_service_name)
                gateway_port = service_port(
                    gateway_service or {},
                    numbers=(parsed.port or (443 if parsed.scheme == "https" else 80),),
                )
                ready = bool(
                    gateway_service
                    and service_has_ready_endpoint(gateway_service, slices_by_service, gateway_port)
                )
                output["query_endpoints"].append(
                    endpoint_record(
                        signal="logs",
                        name="data-science-loki",
                        url=logs_endpoint,
                        access="cluster-service",
                        scope="application",
                        auth="caller-bearer",
                        tls="service-ca" if parsed.scheme == "https" else "none",
                        paths=["/loki/api/v1/query", "/loki/api/v1/query_range", "/loki/api/v1/labels"],
                        state="ready" if ready else "configured",
                        source_ref=source("Monitoring", "cluster", "default-monitoring"),
                        tenant="application",
                        gateway_service=gateway_service_name,
                        service_port=parsed.port,
                        required_headers=["X-Scope-OrgID: application"],
                    )
                )
                if not condition_is_true(conditions, "ClusterLogForwarderAvailable"):
                    output["warnings"].append(
                        "Loki gateway is ready but ClusterLogForwarder is not ready; "
                        "historical application logs may be incomplete"
                    )
        else:
            output["unavailable"].append(
                {"component": "logs", "reason": "Loki query endpoint is not published and ready"}
            )

    if usage_logs_storage and status.get("usageLogsEndpoint"):
        usage_endpoint = status["usageLogsEndpoint"]
        parsed = parse_http_url(usage_endpoint)
        if not parsed:
            output["unavailable"].append(
                {
                    "component": "usage-logs",
                    "reason": "Monitoring.status.usageLogsEndpoint is not a valid "
                    "credential-free HTTP URL",
                }
            )
        else:
            usage_service_name = (parsed.hostname or "").split(".")[0]
            usage_service = services.get(usage_service_name)
            usage_port = service_port(
                usage_service or {},
                numbers=(parsed.port or (443 if parsed.scheme == "https" else 80),),
            )
            ready = bool(
                usage_service
                and service_has_ready_endpoint(usage_service, slices_by_service, usage_port)
            )
            usage_condition = conditions.get("UsageLogsLokiStackAvailable", {}).get("status")
            if usage_condition == "False":
                usage_state = "not-ready"
            elif usage_condition == "True" and ready:
                usage_state = "ready"
            else:
                usage_state = "configured"
            output["telemetry_ingest"].append(
                endpoint_record(
                    signal="logs",
                    name="usage-logs-otlp",
                    url=usage_endpoint,
                    access="cluster-service",
                    scope="application",
                    auth="service-account",
                    tls="service-ca" if parsed.scheme == "https" else "none",
                    paths=["/api/logs/v1/application/otlp"],
                    state=usage_state,
                    source_ref=source("Monitoring", "cluster", "default-monitoring"),
                    gateway_service=usage_service_name,
                    service_port=parsed.port,
                    notes="OTLP ingestion only; not a query endpoint",
                )
            )
    elif usage_logs_storage:
        output["unavailable"].append(
            {"component": "usage-logs", "reason": "Usage log storage is configured but no OTLP endpoint is published"}
        )

    if traces_storage:
        trace_backend = trace_storage.get("backend")
        expected_gateway = {
            "pv": "tempo-data-science-tempomonolithic-gateway",
            "s3": "tempo-data-science-tempostack-gateway",
            "gcs": "tempo-data-science-tempostack-gateway",
        }.get(trace_backend)
        if expected_gateway and expected_gateway in services:
            gateway_name = expected_gateway
            gateway = services[gateway_name]
            port = service_port(gateway, numbers=(8080,))
            add_service_endpoint(
                output,
                service=gateway,
                port=port,
                slices_by_service=slices_by_service,
                signal="traces",
                name=gateway_name,
                scheme="https",
                scope=namespace,
                auth="caller-bearer",
                tls="service-ca",
                paths=["/api/search", "/api/traces/<trace-id>"],
                condition="TempoAvailable",
                conditions=conditions,
                tenant=namespace,
                required_headers=[f"X-Scope-OrgID: {namespace}"],
            )
        else:
            output["unavailable"].append(
                {
                    "component": "traces",
                    "reason": (
                        "Tempo storage backend is missing or its expected gateway Service was not discovered"
                    ),
                }
            )

    perses_enabled = metrics_storage or traces_storage
    if perses_enabled:
        perses = services.get("data-science-perses")
        if perses:
            port = service_port(perses, numbers=(8080,))
            add_service_endpoint(
                output,
                service=perses,
                port=port,
                slices_by_service=slices_by_service,
                signal="dashboards",
                name="data-science-perses",
                scheme="http",
                scope="monitoring",
                auth="see-notes",
                tls="none",
                paths=["/api/v1/health", "/api/v1/projects"],
                condition="PersesAvailable",
                conditions=conditions,
                notes="Perses API; signal datasources should be preferred for direct queries",
            )
            for route in routes.values():
                target = (route.get("spec") or {}).get("to") or {}
                if target.get("kind", "Service") != "Service" or target.get("name") != "data-science-perses":
                    continue
                add_route_endpoint(
                    output,
                    route=route,
                    target_service=perses,
                    target_port=route_target_port(route, perses),
                    slices_by_service=slices_by_service,
                    signal="dashboards",
                    name=f"data-science-perses-route/{(route.get('metadata') or {}).get('name', 'unknown')}",
                    scope="monitoring",
                    auth="see-notes",
                    paths=["/api/v1/health", "/api/v1/projects"],
                    condition="PersesAvailable",
                    conditions=conditions,
                    notes="Perses API; signal datasources should be preferred for direct queries",
                )
        else:
            output["unavailable"].append(
                {"component": "perses", "reason": "Perses Service not found"}
            )

        datasources = optional_oc(
            ["get", "persesdatasources.perses.dev", "-o", "json"],
            namespace,
            output["warnings"],
            "Perses datasources",
        )
        for datasource in (datasources or {}).get("items", []):
            ds_name = (datasource.get("metadata") or {}).get("name", "unknown")
            proxy = (
                ((datasource.get("spec") or {}).get("config") or {})
                .get("plugin", {})
                .get("spec", {})
                .get("proxy", {})
                .get("spec", {})
            )
            url = proxy.get("url")
            if url:
                output["diagnostic_endpoints"].append(
                    endpoint_record(
                        signal="datasource",
                        name=f"perses-datasource/{ds_name}",
                        url=url,
                        access="perses-datasource",
                        scope="dashboard-configured",
                        auth="see-notes",
                        tls="see-notes",
                        paths=[],
                        state="configured",
                        source_ref=source("PersesDatasource", namespace, ds_name),
                        query_parameters=(
                            ((datasource.get("spec") or {}).get("config") or {})
                            .get("plugin", {})
                            .get("spec", {})
                            .get("queryParams")
                        ),
                    )
                )

    correlation_enabled = any(
        spec.get(feature) is not None for feature in ("metrics", "traces", "logs")
    )
    if correlation_enabled:
        korrel8r = services.get("korrel8r")
        if korrel8r:
            port = service_port(korrel8r, numbers=(8443,), names=("https",))
            add_service_endpoint(
                output,
                service=korrel8r,
                port=port,
                slices_by_service=slices_by_service,
                signal="correlation",
                name="korrel8r",
                scheme="https",
                scope="RHOAI telemetry",
                auth="caller-bearer",
                tls="service-ca",
                paths=[
                    "/api/v1alpha1/domains",
                    "/api/v1alpha1/objects",
                    "/api/v1alpha1/graphs/neighbors",
                    "/api/v1alpha1/graphs/goals",
                ],
                condition="Korrel8rAvailable",
                conditions=conditions,
                notes="Graph paths are version-dependent; probe 404/501 as unavailable. Requests must be bounded.",
            )
        else:
            output["unavailable"].append(
                {"component": "korrel8r", "reason": "Korrel8r Service not found"}
            )

    for service_name, port_numbers, signal, paths, scheme in (
        ("data-science-collector-collector-headless", (4317,), "telemetry", [], "https"),
        ("data-science-collector-collector-headless", (4318,), "telemetry", ["/v1/traces"], "https"),
        ("data-science-collector-monitoring", (8890,), "metrics", ["/metrics"], "https"),
        ("data-science-collector-prometheus", (8889,), "metrics", ["/metrics"], "https"),
    ):
        service = services.get(service_name)
        if not service:
            continue
        for port_number in port_numbers:
            port = service_port(service, numbers=(port_number,))
            if port:
                add_service_endpoint(
                    output,
                    service=service,
                    port=port,
                    slices_by_service=slices_by_service,
                    signal=signal,
                    name=f"{service_name}:{port_number}",
                    scheme=scheme,
                    scope="collector",
                    auth="service-account-or-network-policy",
                    tls="service-ca",
                    paths=paths,
                    condition="OpenTelemetryCollectorAvailable",
                    conditions=conditions,
                    collection="telemetry_ingest",
                    notes="Non-query telemetry endpoint",
                )

    cluster_proxy = services.get("data-science-prometheus-cluster-proxy")
    if cluster_proxy:
        port = service_port(cluster_proxy, numbers=(8443,), names=("https",))
        add_service_endpoint(
            output,
            service=cluster_proxy,
            port=port,
            slices_by_service=slices_by_service,
            signal="metrics",
            name="data-science-prometheus-cluster-proxy",
            scheme="https",
            scope="node-metrics-scrape-only",
            auth="caller-bearer",
            tls="service-ca",
            paths=["/metrics"],
            condition="NodeMetricsEndpointAvailable",
            conditions=conditions,
            collection="diagnostic_endpoints",
            notes="Not a PromQL query endpoint",
        )
        cluster_proxy_route = find_route(routes, "data-science-prometheus-cluster-proxy")
        if cluster_proxy_route:
            target = services.get(
                (cluster_proxy_route.get("spec") or {}).get("to", {}).get("name", "")
            )
            target_port = route_target_port(cluster_proxy_route, target)
            add_route_endpoint(
                output,
                route=cluster_proxy_route,
                target_service=target,
                target_port=target_port,
                slices_by_service=slices_by_service,
                signal="metrics",
                name="data-science-prometheus-cluster-proxy-route",
                scope="node-metrics-scrape-only",
                auth="caller-bearer",
                paths=["/metrics"],
                condition="NodeMetricsEndpointAvailable",
                conditions=conditions,
                collection="diagnostic_endpoints",
                notes="Not a PromQL query endpoint",
            )

    for service_name, service in services.items():
        labels = (service.get("metadata") or {}).get("labels") or {}
        if "alertmanager" not in service_name and labels.get("app.kubernetes.io/name") != "alertmanager":
            continue
        port = service_port(service, numbers=(9093,), names=("web", "http"))
        if port:
            add_service_endpoint(
                output,
                service=service,
                port=port,
                slices_by_service=slices_by_service,
                signal="alerting",
                name=service_name,
                scheme="http",
                scope="monitoring-alerts",
                auth="see-notes",
                tls="see-notes",
                paths=["/api/v2/alerts", "/api/v2/status"],
                condition=None,
                conditions=conditions,
                collection="diagnostic_endpoints",
                notes="Alertmanager diagnostic API; not a metrics query backend",
            )

    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pretty", action="store_true", help="pretty-print the JSON output"
    )
    args = parser.parse_args()
    try:
        result = discover()
    except (OSError, ValueError, OcError) as error:
        result = empty_inventory()
        result["unavailable"].append({"component": "discovery", "reason": str(error)})
        print(json.dumps(result, indent=2 if args.pretty else None, sort_keys=args.pretty))
        return 2
    print(json.dumps(result, indent=2 if args.pretty else None, sort_keys=args.pretty))
    return 0 if not result["unavailable"] or all(
        item.get("component") != "monitoring" for item in result["unavailable"]
    ) else 2


if __name__ == "__main__":
    sys.exit(main())
