# RHOAI Korrel8r Deployment Design

## Goal

Deploy one RHOAI-owned Korrel8r REST service from the `odh-observability` Monitoring reconciler whenever metrics, traces, or logs are configured.

## Scope

The operand is managed by the existing controller and runs in the Monitoring target namespace. It is configured for the RHOAI Kubernetes API, ThanosQuerier, Tempo gateway, and LokiStack gateway only. Store configuration is operator-owned, including tenant values and URLs; the service does not expose a caller-controlled configuration endpoint, Route, COO Troubleshooting Panel, Perses integration, MCP integration, or producer-side instrumentation.

## Reconciliation

Add a `deployKorrel8r` action to the existing template-source action chain. It is a no-op with an Info-severity `Korrel8rAvailable` condition when no signal feature is configured. When at least one signal feature is configured it adds embedded templates for the Deployment, Service, ConfigMap, ServiceAccount/RBAC, and NetworkPolicy resources. Garbage collection remains the final cleanup action.

The action has no new CRD prerequisite. Existing metrics, traces, and Loki conditions continue to report backend readiness; the Korrel8r condition reports whether the engine Deployment and Service have ready workloads and endpoints. A missing Loki/CLO backend must not prevent metrics or trace stores from being configured.

## Configuration and security

The ConfigMap is mounted outside the image's built-in `/etc/korrel8r` tree and includes `/etc/korrel8r/rules/all.yaml`. It configures only enabled RHOAI stores, sets `requestTimeout: 30s` and `sessionTimeout: 5m`, and uses `direct: true` for the application-log fallback. Tempo and Loki tenant values are pinned in rendered configuration. The Korrel8r process receives the incoming bearer token when querying Tempo/Loki so gateway SAR remains the tenant boundary; the service account is not used as a cross-tenant proxy.

The operand uses a pinned default image matching the supported COO Korrel8r image and allows a related-image environment override. Deployment resources are fixed at 50m CPU/64Mi memory requests and 200m CPU/256Mi memory limits. The Service is ClusterIP-only. RBAC and NetworkPolicy permit only the target query paths and the required token-review capability.

## Tests and verification

Unit tests cover the signal trigger, condition behavior, selected template sources, image override, and rendered resource invariants. The repository's normal unit-test, vet, format, lint, and Helm validation commands must pass. Cluster verification uses the documented port-forward REST calls after the separate endpoint/tenant validation work is available.
