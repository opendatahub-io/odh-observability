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
	"fmt"
	"testing"
	"time"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
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
			jq.Match(`.spec.podSelector.matchLabels."app.kubernetes.io/name" == "korrel8r"`),
			jq.Match(`[.spec.ingress[]?] | length == 0`),
			jq.Match(`[.spec.egress[] | select(([.ports[]? | select(.port == 6443 and .protocol == "TCP")] | length > 0) and ([.to[]?.ipBlock.cidr] | length > 0))] | length == 1`),
			jq.Match(`[.spec.egress[]?.to[]?.ipBlock.cidr | select(. == "0.0.0.0/0" or . == "::/0")] | length == 0`),
		)),
		WithCustomErrorMsg("Korrel8r NetworkPolicy should deny ingress and permit only discovered API endpoint egress"),
	)

	tc.validateKorrel8rIngressDenied(t)
}

func (tc *MonitoringTestCtx) validateKorrel8rIngressDenied(t *testing.T) {
	t.Helper()

	probeName := fmt.Sprintf("korrel8r-ingress-%x", time.Now().UnixNano())
	endpoint := fmt.Sprintf("https://korrel8r.%s.svc.cluster.local:8443", tc.MonitoringNamespace)
	script := fmt.Sprintf(`
connection=$(curl --noproxy '*' -skS --connect-timeout 5 --max-time 8 -o /dev/null -w '%%{num_connects}:%%{http_code}' %q)
result=$?
echo "curl exit=$result connections:http=$connection"
[ "$connection" = "0:000" ] && { [ "$result" -eq 7 ] || [ "$result" -eq 28 ]; }
`, endpoint)
	probe := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: probeName, Namespace: tc.MonitoringNamespace},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			AutomountServiceAccountToken: new(false),
			SecurityContext:              fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name:                     "curl",
				Image:                    fakeGCSClientImage,
				Command:                  []string{"/bin/sh", "-c"},
				Args:                     []string{script},
				SecurityContext:          fixtureContainerSecurityContext(),
				TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
			}},
		},
	}
	tc.createFixtureResource(t, gvk.Pod, probe)
	t.Cleanup(func() {
		if err := tc.Client().Delete(tc.Context(), probe); err != nil && !k8serr.IsNotFound(err) {
			t.Logf("clean up Korrel8r ingress probe %s: %v", probeName, err)
		}
	})
	tc.g.Eventually(func() error {
		current := &corev1.Pod{}
		if err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: probeName, Namespace: tc.MonitoringNamespace}, current); err != nil {
			return err
		}
		switch current.Status.Phase {
		case corev1.PodSucceeded:
			return nil
		case corev1.PodFailed:
			return StopErr(fmt.Errorf("pod failed: %v", current.Status.ContainerStatuses),
				"Korrel8r ingress probe %s failed", probeName)
		default:
			return fmt.Errorf("Korrel8r ingress probe %s is %s", probeName, current.Status.Phase)
		}
	}).WithTimeout(3 * time.Minute).WithPolling(2 * time.Second).Should(Succeed())
	t.Logf("same-namespace Korrel8r ingress probe denied: %s", probeName)
}
