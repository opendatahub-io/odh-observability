# RHOAI Korrel8r Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reconcile a bounded, RHOAI-owned Korrel8r service for configured metrics, traces, or logs.

**Architecture:** Extend the existing Monitoring controller's embedded-template action chain. The action renders a stateless Korrel8r Deployment, ClusterIP Service, custom ConfigMap, service account/RBAC, and network policy in the Monitoring namespace. It configures only RHOAI stores enabled by the Monitoring CR and reuses existing endpoint/tenant data.

**Tech Stack:** Go 1.25, controller-runtime, Kubernetes YAML templates, `text/template`, Server-Side Apply, Helm chart tests.

**Spec:** `docs/superpowers/specs/2026-09-03-korrel8r-deployment-design.md`

## Global Constraints

- Deploy only when `spec.metrics`, `spec.traces`, or `spec.logs` is configured.
- Keep store URLs and tenant values operator-owned; do not add a public configuration API.
- Use the supported pinned COO Korrel8r image with `RELATED_IMAGE_KORREL8R_IMAGE` override.
- Set requests to `50m` CPU/`64Mi` memory and limits to `200m` CPU/`256Mi` memory.
- Set Korrel8r `requestTimeout` to `30s` and `sessionTimeout` to `5m`.
- Keep GC as the final action in the reconciliation chain.
- Do not add a Route, COO Troubleshooting Panel, Perses, MCP, producer instrumentation, or a new public CRD field.

### Task 1: Add failing action and template-data tests

**Files:**
- Modify: `internal/controller/actions_test.go`
- Modify: `internal/controller/templatedata_extended_test.go`
- Modify: `internal/controller/monitoring_reconciler_test.go` if the integration assertion belongs there

- [ ] Add table-driven tests for no signal configuration, metrics only, traces only, logs only, and metrics plus logs. Assert the no-signal case adds no sources and marks `Korrel8rAvailable` Info-severity, while configured cases add the Korrel8r sources and mark the condition True.
- [ ] Add tests for the default and `RELATED_IMAGE_KORREL8R_IMAGE` override.
- [ ] Add template-data tests for monitoring namespace, Thanos URL, Tempo query URL, Loki URL, feature booleans, and sizing values.
- [ ] Run the focused tests and confirm they fail because the action, condition, and data keys do not yet exist.

### Task 2: Implement condition, trigger, and template data

**Files:**
- Modify: `internal/controller/conditions/conditions.go`
- Modify: `internal/controller/actions.go`
- Modify: `internal/controller/monitoring_reconciler.go`
- Modify: `internal/controller/templatedata.go`

- [ ] Add `ConditionKorrel8rAvailable` to the condition registry and mark it Info when no signal feature is configured.
- [ ] Add the `deployKorrel8r` action to the non-Perses action sequence after backend actions and before rendering; leave the garbage-collection call last.
- [ ] Add `Korrel8rImage`, fixed resource values, `Korrel8rServiceName`, and RHOAI store endpoint data to `buildTemplateData` using the existing namespace and Tempo endpoint conventions.
- [ ] Add `getKorrel8rImage` with a pinned supported default and environment override.
- [ ] Run the focused tests and confirm they pass.

### Task 3: Add the embedded Korrel8r operand templates

**Files:**
- Create: `internal/controller/resources/korrel8r-deployment.tmpl.yaml`
- Create: `internal/controller/resources/korrel8r-config.tmpl.yaml`
- Create: `internal/controller/resources/korrel8r-rbac.tmpl.yaml`
- Create: `internal/controller/resources/korrel8r-network-policy.tmpl.yaml`
- Modify: `internal/controller/actions.go`

- [ ] Render a Deployment with `korrel8r web`, custom ConfigMap mounted outside `/etc/korrel8r`, service account, readiness/liveness probes, ClusterIP Service, and fixed Burstable resources.
- [ ] Render stores for `k8s` and each enabled RHOAI backend: ThanosQuerier for metrics, Tempo gateway plus pinned tenant for traces, and LokiStack gateway plus pinned tenant/direct fallback for logs. Include built-in rules and tuning values.
- [ ] Render least-privilege RBAC and NetworkPolicy for the service's in-namespace and backend query paths, without cluster Loki/platform Tempo or a Route.
- [ ] Render the templates with representative data and validate YAML structure with Helm/template tooling.

### Task 4: Wire module image propagation and documentation

**Files:**
- Modify: `internal/controller/modules/monitoring/handler.go` in `opendatahub-operator` only if the module's related-image contract requires it
- Modify: corresponding monitoring handler test only if that list changes
- Modify: `README.md` or the existing monitoring documentation only to document the REST/port-forward verification path

- [ ] Verify how related-image environment values are injected into the standalone operator deployment and add `RELATED_IMAGE_KORREL8R_IMAGE` at the module boundary if needed.
- [ ] Document the REST API as the verification interface and keep the service ClusterIP-only.

### Task 5: Verify and review

**Files:**
- All files modified by Tasks 1–4

- [ ] Run `make vet` immediately after the final implementation edit.
- [ ] Run `make fmt lint` and `make test` (or the repository's exact equivalent targets) and fix all failures.
- [ ] Run Helm lint/template validation and inspect the rendered Korrel8r resources.
- [ ] Review `git diff` for unrelated changes and confirm the action ordering leaves GC last.
- [ ] Launch the read-only verification subagent because controller logic and multiple Go files changed.
