/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e_test

import (
	"testing"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega
)

const korrel8rName = "korrel8r"

func (tc *MonitoringTestCtx) runKorrel8rTests(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	t.Run("Korrel8r resources", func(t *testing.T) {
		tc = tc.WithT(t)
		tc.setupMetrics(t)
		t.Cleanup(func() {
			tc.cleanupGroup(t, "")
		})

		t.Run("reconciles a ready TLS query service", tc.ValidateKorrel8rResources)
	})
}

// ValidateKorrel8rResources catches regressions that omit the query service,
// expose it without TLS/MCP hardening, or remove its Kubernetes API egress.
func (tc *MonitoringTestCtx) ValidateKorrel8rResources(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionKorrel8rAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("Monitoring should report Korrel8r available after its service has a ready endpoint"),
	)

	tc.EnsureDeploymentReady(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{Name: korrel8rName, Namespace: tc.MonitoringNamespace}),
	)
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{Name: korrel8rName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`
			([.spec.template.spec.containers[].args[] | select(. == "--https=:8443")] | length) == 1 and
			([.spec.template.spec.containers[].args[] | select(. == "--mcp=false")] | length) == 1 and
			(.spec.template.metadata.annotations."platform.opendatahub.io/korrel8r-config-checksum" | length) == 64
		`)),
		WithCustomErrorMsg("Korrel8r should expose only its TLS REST listener with MCP disabled"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Service, types.NamespacedName{Name: korrel8rName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.spec.type == "ClusterIP"`),
			jq.Match(`.spec.ports | length == 1 and .[0].name == "https" and .[0].port == 8443 and .[0].targetPort == "https"`),
			jq.Match(`.metadata.annotations."service.beta.openshift.io/serving-cert-secret-name" == "korrel8r-tls"`),
		)),
		WithCustomErrorMsg("Korrel8r Service should expose only its service-certificate-protected HTTPS port"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ConfigMap, types.NamespacedName{Name: "korrel8r-config", Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.data."korrel8r.yaml" | contains("domain: k8s") and contains("domain: metric") and contains("/etc/korrel8r/custom/rhoai-metrics.yaml")`),
			jq.Match(`.data."rhoai-metrics.yaml" | contains("exported_namespace=\"{{.metadata.namespace}}\"") and contains("exported_pod=\"{{.metadata.name}}\"")`),
			jq.Match(`
				.data."korrel8r.yaml" | contains("/etc/korrel8r/rules/all.yaml") and
				contains("/etc/korrel8r/custom/rhai-inference-rules.yaml")
			`),
			jq.Match(`
				.data."rhai-inference-rules.yaml" |
				contains("LLMInferenceService.v1alpha2.serving.kserve.io") and
				contains("app.kubernetes.io/name") and
				contains("app.kubernetes.io/part-of") and
				contains("HTTPRoute.v1.gateway.networking.k8s.io") and
				contains("InferencePool.v1.inference.networking.k8s.io") and
				contains("kserve.io/component")
			`),
		)),
		WithCustomErrorMsg("Korrel8r ConfigMap should ship guarded RHOAI inference rules and retain built-in Pod telemetry rules"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{Name: korrel8rName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`[.spec.ingress[]?.ports[]? | select(.port == 8443 and .protocol == "TCP")] | length == 1`),
			jq.Match(`[.spec.egress[] | select(([.ports[]? | select(.port == 6443 and .protocol == "TCP")] | length > 0) and ([.to[]?.ipBlock.cidr] | length > 0))] | length == 1`),
			jq.Match(`[.spec.egress[]?.to[]?.ipBlock.cidr | select(. == "0.0.0.0/0" or . == "::/0")] | length == 0`),
		)),
		WithCustomErrorMsg("Korrel8r NetworkPolicy should permit only TCP/6443 egress to discovered API endpoints"),
	)
}
