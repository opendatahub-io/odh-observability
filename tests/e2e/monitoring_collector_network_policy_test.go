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
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega matchers
)

// TestCollectorNetworkPolicyContract is intentionally standalone so the live
// NetworkPolicy contract can be qualified without mutating the broader
// monitoring lifecycle suite.
func TestCollectorNetworkPolicyContract(t *testing.T) {
	tc, err := NewTestContext(t)
	require.NoError(t, err)
	tc.MonitoringNamespace = tc.detectMonitoringNamespace(t)

	(&MonitoringTestCtx{TestContext: tc}).ValidateCollectorNetworkPolicyContract(t)
}

// TestTargetAllocatorMTLSNetworkPolicy checks the secure discovery path enabled
// by TargetAllocator mTLS independently of the broader monitoring lifecycle.
func TestTargetAllocatorMTLSNetworkPolicy(t *testing.T) {
	tc, err := NewTestContext(t)
	require.NoError(t, err)
	tc.MonitoringNamespace = tc.detectMonitoringNamespace(t)
	(&MonitoringTestCtx{TestContext: tc}).runTargetAllocatorMTLSNetworkPolicyProbes(t)
}

func collectorNetworkPolicyLabels(namespace string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/component":  "opentelemetry-collector",
		"app.kubernetes.io/instance":   namespace + ".data-science-collector",
		"app.kubernetes.io/managed-by": "opentelemetry-operator",
		"app.kubernetes.io/part-of":    "opentelemetry",
	}
}

func targetAllocatorNetworkPolicyLabels(namespace string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/component":  "opentelemetry-targetallocator",
		"app.kubernetes.io/instance":   namespace + ".data-science-collector",
		"app.kubernetes.io/managed-by": "opentelemetry-operator",
		"app.kubernetes.io/part-of":    "opentelemetry",
	}
}

func otlpCallerNetworkPolicyLabels(component string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/component": component,
		"app.kubernetes.io/part-of":   "llminferenceservice",
	}
}

// ValidateCollectorNetworkPolicyContract verifies both the rendered policy
// ownership contract and the enforced positive/negative ingress behavior.
func (tc *MonitoringTestCtx) ValidateCollectorNetworkPolicyContract(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	policies := &networkingv1.NetworkPolicyList{}
	require.NoError(t, tc.Client().List(tc.Context(), policies, client.InNamespace(tc.MonitoringNamespace)))

	collectorLabels := collectorNetworkPolicyLabels(tc.MonitoringNamespace)
	targetAllocatorLabels := targetAllocatorNetworkPolicyLabels(tc.MonitoringNamespace)
	collectorPods := &corev1.PodList{}
	require.NoError(t, tc.Client().List(tc.Context(), collectorPods,
		client.InNamespace(tc.MonitoringNamespace), client.MatchingLabels(collectorLabels)))
	require.NotEmpty(t, collectorPods.Items, "collector pods must exist")
	targetAllocatorPods := &corev1.PodList{}
	require.NoError(t, tc.Client().List(tc.Context(), targetAllocatorPods,
		client.InNamespace(tc.MonitoringNamespace), client.MatchingLabels(targetAllocatorLabels)))
	require.NotEmpty(t, targetAllocatorPods.Items, "TargetAllocator pods must exist")
	collectorPolicies := policiesSelectingPods(t, policies.Items, collectorPods.Items)
	targetAllocatorPolicies := policiesSelectingPods(t, policies.Items, targetAllocatorPods.Items)
	require.NotEmpty(t, collectorPolicies, "at least one NetworkPolicy must select the collector pods")
	require.NotEmpty(t, targetAllocatorPolicies, "at least one NetworkPolicy must select the TargetAllocator pods")
	requireOnlyIngressPolicy(t, collectorPolicies, "data-science-collector-monitoring-ingress")
	requireOnlyIngressPolicy(t, targetAllocatorPolicies, "data-science-collector-targetallocator-ingress")

	collectorPolicy := policyByName(t, collectorPolicies, "data-science-collector-monitoring-ingress")
	require.Len(t, collectorPolicy.Spec.Ingress, 2)
	require.Len(t, collectorPolicy.Spec.Ingress[0].From, 1, "prometheus scrape rule uses a single peer")
	require.NotNil(t, collectorPolicy.Spec.Ingress[0].From[0].PodSelector)
	require.Nil(t, collectorPolicy.Spec.Ingress[0].From[0].IPBlock)
	require.Equal(t, collectorLabels, collectorPolicy.Spec.PodSelector.MatchLabels)
	require.Nil(t, collectorPolicy.Spec.Ingress[0].From[0].NamespaceSelector)
	require.Equal(t, map[string]string{
		"app.kubernetes.io/instance": "data-science-monitoringstack",
		"app.kubernetes.io/name":     "prometheus",
	}, collectorPolicy.Spec.Ingress[0].From[0].PodSelector.MatchLabels)
	require.Equal(t, []int32{8889, 8890}, networkPolicyPorts(collectorPolicy.Spec.Ingress[0]))
	require.NotContains(t, networkPolicyPorts(collectorPolicy.Spec.Ingress[0]), int32(8888))
	require.Len(t, collectorPolicy.Spec.Ingress[1].From, 2)
	require.Equal(t, map[string]string{"app.kubernetes.io/part-of": "llminferenceservice"},
		collectorPolicy.Spec.Ingress[1].From[0].PodSelector.MatchLabels)
	require.Equal(t, []metav1.LabelSelectorRequirement{{
		Key:      "app.kubernetes.io/component",
		Operator: metav1.LabelSelectorOpIn,
		Values: []string{
			"llminferenceservice-workload",
			"llminferenceservice-workload-prefill",
			"llminferenceservice-workload-worker",
			"llminferenceservice-workload-leader",
			"llminferenceservice-workload-worker-prefill",
			"llminferenceservice-workload-leader-prefill",
			"llminferenceservice-router-scheduler",
		},
	}}, collectorPolicy.Spec.Ingress[1].From[0].PodSelector.MatchExpressions)
	require.Equal(t, &metav1.LabelSelector{MatchLabels: map[string]string{
		otlpNamespaceLabel: "true",
	}}, collectorPolicy.Spec.Ingress[1].From[0].NamespaceSelector)
	require.Equal(t, &metav1.LabelSelector{MatchLabels: map[string]string{
		otlpNamespaceLabel: "true",
	}}, collectorPolicy.Spec.Ingress[1].From[1].NamespaceSelector)
	require.Len(t, collectorPolicy.Spec.Ingress[1].From[1].PodSelector.MatchExpressions, 1)
	require.Equal(t, inferenceServicePodLabelKey, collectorPolicy.Spec.Ingress[1].From[1].PodSelector.MatchExpressions[0].Key)
	require.Equal(t, metav1.LabelSelectorOpExists, collectorPolicy.Spec.Ingress[1].From[1].PodSelector.MatchExpressions[0].Operator)
	require.Equal(t, []int32{4317, 4318}, networkPolicyPorts(collectorPolicy.Spec.Ingress[1]))

	targetAllocatorPolicy := policyByName(t, targetAllocatorPolicies, "data-science-collector-targetallocator-ingress")
	require.Len(t, targetAllocatorPolicy.Spec.Ingress, 1)
	require.Len(t, targetAllocatorPolicy.Spec.Ingress[0].From, 1)
	require.NotNil(t, targetAllocatorPolicy.Spec.Ingress[0].From[0].PodSelector)
	require.Nil(t, targetAllocatorPolicy.Spec.Ingress[0].From[0].NamespaceSelector)
	require.Nil(t, targetAllocatorPolicy.Spec.Ingress[0].From[0].IPBlock)
	require.Equal(t, collectorLabels, targetAllocatorPolicy.Spec.Ingress[0].From[0].PodSelector.MatchLabels)
	require.Equal(t, []int32{8080, 8443}, networkPolicyPorts(targetAllocatorPolicy.Spec.Ingress[0]))

	tc.runCollectorNetworkPolicyProbes(t, collectorLabels)
}

func (tc *MonitoringTestCtx) runCollectorNetworkPolicyProbes(t *testing.T, collectorLabels map[string]string) {
	t.Helper()

	collectorEndpoints := []string{
		fmt.Sprintf("https://data-science-collector-prometheus.%s.svc.cluster.local:8889/metrics", tc.MonitoringNamespace),
		fmt.Sprintf("https://data-science-collector-monitoring.%s.svc.cluster.local:8890/metrics", tc.MonitoringNamespace),
	}
	prometheusLabels := map[string]string{
		"app.kubernetes.io/name":       "prometheus",
		"app.kubernetes.io/instance":   MonitoringStackName,
		"app.kubernetes.io/managed-by": "prometheus-operator",
	}
	tc.runNetworkPolicyProbe(t, "collector-prometheus-allowed", tc.MonitoringNamespace, prometheusLabels, collectorEndpoints, true)
	tc.runNetworkPolicyProbe(t, "collector-untrusted-denied", tc.MonitoringNamespace, map[string]string{
		"app.kubernetes.io/name": "network-policy-untrusted",
	}, collectorEndpoints, false)
	otlpProbeNamespace, hasCaller := tc.runOTLPWorkloadNetworkPolicyProbe(t)
	if !hasCaller {
		t.Log("no running instrumented KServe workload exporting OTLP to the collector; only selector probes will run")
	}
	if otlpProbeNamespace == "" || otlpProbeNamespace == tc.MonitoringNamespace {
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name:   fmt.Sprintf("collector-network-policy-%x", time.Now().UnixNano()),
			Labels: map[string]string{otlpNamespaceLabel: "true"},
		}}
		require.NoError(t, tc.Client().Create(tc.Context(), namespace))
		t.Cleanup(func() {
			if err := tc.Client().Delete(tc.Context(), namespace); err != nil && !errors.IsNotFound(err) {
				t.Logf("failed to clean up NetworkPolicy namespace %s: %v", namespace.Name, err)
			}
		})
		otlpProbeNamespace = namespace.Name
	}
	for _, namespace := range []string{tc.MonitoringNamespace, otlpProbeNamespace} {
		enrolled := tc.otlpNamespaceEnrolled(t, namespace)
		t.Logf("OTLP selector probes use namespace %s (enrolled=%t)", namespace, enrolled)
		tc.runOTLPSelectorNetworkPolicyProbes(t, namespace, enrolled)
	}

	targetAllocatorEndpoint := fmt.Sprintf("http://%s.%s.svc.cluster.local:80/", TargetAllocatorDeploymentName, tc.MonitoringNamespace)
	tc.runNetworkPolicyProbe(t, "targetallocator-collector-allowed", tc.MonitoringNamespace, collectorLabels, []string{targetAllocatorEndpoint}, true)
	tc.runNetworkPolicyProbe(t, "targetallocator-untrusted-denied", tc.MonitoringNamespace, map[string]string{
		"app.kubernetes.io/name": "network-policy-untrusted",
	}, []string{targetAllocatorEndpoint}, false)

	collector, err := tc.fetchResource(t, gvk.OpenTelemetryCollector, types.NamespacedName{
		Name: OpenTelemetryCollectorName, Namespace: tc.MonitoringNamespace,
	})
	require.NoError(t, err)
	mtls, _, err := unstructured.NestedBool(collector.Object, "spec", "targetAllocator", "mtls", "enabled")
	require.NoError(t, err)
	if mtls {
		tc.runTargetAllocatorMTLSNetworkPolicyProbes(t)
	}
}

func (tc *MonitoringTestCtx) runOTLPSelectorNetworkPolicyProbes(t *testing.T, namespace string, enrolled bool) {
	t.Helper()

	otlpHTTPEndpoint := fmt.Sprintf("http://data-science-collector-collector.%s.svc.cluster.local:4318/v1/traces", tc.MonitoringNamespace)
	otlpGRPCEndpoint := fmt.Sprintf("http://data-science-collector-collector.%s.svc.cluster.local:4317/", tc.MonitoringNamespace)
	endpoints := []string{otlpGRPCEndpoint, otlpHTTPEndpoint}
	for _, caller := range []struct{ name, component string }{
		{"workload", "llminferenceservice-workload"},
		{"prefill", "llminferenceservice-workload-prefill"},
		{"worker", "llminferenceservice-workload-worker"},
		{"leader", "llminferenceservice-workload-leader"},
		{"worker-prefill", "llminferenceservice-workload-worker-prefill"},
		{"leader-prefill", "llminferenceservice-workload-leader-prefill"},
		{"scheduler", "llminferenceservice-router-scheduler"},
	} {
		tc.runNetworkPolicyProbe(t, "collector-otlp-"+caller.name+"-allowed", namespace,
			otlpCallerNetworkPolicyLabels(caller.component), endpoints, enrolled)
	}
	tc.runNetworkPolicyProbe(t, "collector-otlp-inferenceservice-allowed", namespace,
		map[string]string{inferenceServicePodLabelKey: "demo"}, endpoints, enrolled)
	for _, caller := range []struct {
		name   string
		labels map[string]string
	}{
		{name: "unlabeled"},
		{name: "part-only", labels: map[string]string{"app.kubernetes.io/part-of": "llminferenceservice"}},
		{name: "role-only", labels: map[string]string{"app.kubernetes.io/component": "llminferenceservice-workload"}},
		{name: "wrong-role", labels: otlpCallerNetworkPolicyLabels("unrelated-component")},
	} {
		tc.runNetworkPolicyProbe(t, "collector-otlp-"+caller.name+"-denied", namespace, caller.labels, endpoints, false)
	}
}

func (tc *MonitoringTestCtx) runOTLPWorkloadNetworkPolicyProbe(t *testing.T) (string, bool) {
	t.Helper()

	callers := &corev1.PodList{}
	require.NoError(t, tc.Client().List(tc.Context(), callers,
		client.MatchingLabels{"app.kubernetes.io/part-of": "llminferenceservice"}))
	probeNamespace := ""
	for _, caller := range callers.Items {
		if !strings.HasPrefix(caller.Labels["app.kubernetes.io/component"], "llminferenceservice-workload") {
			continue
		}
		containerName := otlpCallerContainer(caller)
		if caller.Status.Phase != corev1.PodRunning || containerName == "" {
			continue
		}
		require.True(t, tc.otlpNamespaceEnrolled(t, caller.Namespace),
			"an administrator must enroll inference namespace %s with %s=true before upgrading", caller.Namespace, otlpNamespaceLabel)
		tc.runActualOTLPWorkloadNetworkPolicyProbe(t, caller, containerName)
		probeNamespace = caller.Namespace
	}
	return probeNamespace, probeNamespace != ""
}

func (tc *MonitoringTestCtx) otlpNamespaceEnrolled(t *testing.T, name string) bool {
	t.Helper()
	namespace := &corev1.Namespace{}
	require.NoError(t, tc.Client().Get(tc.Context(), types.NamespacedName{Name: name}, namespace))
	return namespace.Labels[otlpNamespaceLabel] == "true"
}

func otlpCallerContainer(pod corev1.Pod) string {
	for _, container := range pod.Spec.Containers {
		for _, env := range container.Env {
			if env.Name == "OTEL_EXPORTER_OTLP_ENDPOINT" && strings.Contains(env.Value, "data-science-collector") {
				return container.Name
			}
		}
	}
	return ""
}

func (tc *MonitoringTestCtx) runActualOTLPWorkloadNetworkPolicyProbe(t *testing.T, caller corev1.Pod, containerName string) {
	t.Helper()

	grpcEndpoint := fmt.Sprintf("http://data-science-collector-collector.%s.svc.cluster.local:4317/", tc.MonitoringNamespace)
	httpEndpoint := fmt.Sprintf("http://data-science-collector-collector.%s.svc.cluster.local:4318/v1/traces", tc.MonitoringNamespace)
	now := time.Now().UnixNano()
	payload := fmt.Sprintf(`{"resourceSpans":[{"scopeSpans":[{"spans":[{`+
		`"traceId":"%032x","spanId":"%016x","name":"network-policy-probe",`+
		`"startTimeUnixNano":"%d","endTimeUnixNano":"%d"}]}]}]}`,
		now, now, now, now+1_000_000)
	const curlBase = "curl --noproxy '*' -skS --connect-timeout 5 --max-time 15 -o /dev/null -w '%{http_code}'"
	var script strings.Builder
	script.WriteString("set -eu\n")
	fmt.Fprintf(&script, "grpc=$(%s --http2-prior-knowledge %q)\n", curlBase, grpcEndpoint)
	script.WriteString("test \"$grpc\" != 000\n")
	fmt.Fprintf(&script, "http=$(%s -H 'Content-Type: application/json' --data-binary %q %q)\n", curlBase, payload, httpEndpoint)
	script.WriteString("test \"$http\" = 200\n")
	args := []string{"exec", "-n", caller.Namespace, caller.Name, "-c", containerName, "--", "sh", "-c", script.String()}
	output, err := exec.CommandContext(tc.Context(), "oc", args...).CombinedOutput()
	require.NoError(t, err, "actual OTLP caller %s/%s could not export to the collector: %s", caller.Namespace, caller.Name, strings.TrimSpace(string(output)))
}

func (tc *MonitoringTestCtx) runTargetAllocatorMTLSNetworkPolicyProbes(t *testing.T) {
	t.Helper()

	endpoint := fmt.Sprintf("https://%s.%s.svc:443", TargetAllocatorDeploymentName, tc.MonitoringNamespace)
	// The Operator names the client Certificate and its Secret identically.
	// Mount the existing client Secret; do not grant the probe API Secret access.
	clientCertificate := OpenTelemetryCollectorName + "-ta-client-cert"
	tc.runNetworkPolicyProbe(t, "targetallocator-mtls-allowed", tc.MonitoringNamespace,
		collectorNetworkPolicyLabels(tc.MonitoringNamespace), nil, true, func(probe *corev1.Pod) {
			probe.Spec.Volumes = []corev1.Volume{{
				Name:         "client-certificate",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: clientCertificate}},
			}}
			probe.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{
				Name: "client-certificate", MountPath: "/tls", ReadOnly: true,
			}}
			probe.Spec.Containers[0].Args = []string{fmt.Sprintf(`set -eu
for resource in jobs scrape_configs; do
  status=$(curl --noproxy '*' -sS --cacert /tls/ca.crt --cert /tls/tls.crt --key /tls/tls.key \
    --connect-timeout 5 --max-time 15 -o /dev/null -w '%%{http_code}' %q/"$resource")
  printf 'authenticated %%s: %%s\n' "$resource" "$status"
  test "$status" = 200
done
# A reachable TLS listener must reject a client without its certificate.
set +e
connection=$(curl --noproxy '*' -sS --cacert /tls/ca.crt \
  --connect-timeout 5 --max-time 15 -o /dev/null -w '%%{num_connects}:%%{http_code}' %q/jobs)
result=$?
set -e
printf 'missing client certificate: connection=%%s curl=%%s\n' "$connection" "$result"
test "$connection" = 1:000
# Curl versions can surface an mTLS rejection as 16 (HTTP/2 framing),
# 35 (TLS handshake), 55 (send failure/broken pipe), or 56 (recv failure).
# In each case the TCP connection succeeded without an HTTP response.
test "$result" -eq 16 -o "$result" -eq 35 -o "$result" -eq 55 -o "$result" -eq 56
`, endpoint, endpoint)}
		})
	tc.runNetworkPolicyProbe(t, "targetallocator-mtls-untrusted-denied", tc.MonitoringNamespace,
		map[string]string{"app.kubernetes.io/name": "network-policy-untrusted"}, []string{endpoint + "/jobs"}, false)
}

func (tc *MonitoringTestCtx) runNetworkPolicyProbe(
	t *testing.T, name, namespace string, probeLabels map[string]string, endpoints []string, allowed bool,
	configurePod ...func(*corev1.Pod),
) {
	t.Helper()

	probeName := fmt.Sprintf("%s-%x", name, time.Now().UnixNano())
	probeImage := tc.networkProbeImage(t)
	probe := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: probeName, Namespace: namespace, Labels: probeLabels},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			AutomountServiceAccountToken: new(false),
			SecurityContext:              fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name:                     "curl",
				Image:                    probeImage,
				Command:                  []string{"/bin/sh", "-c"},
				Args:                     []string{networkPolicyProbeScript(endpoints, allowed)},
				SecurityContext:          fixtureContainerSecurityContext(),
				TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
			}},
		},
	}
	for _, configure := range configurePod {
		configure(probe)
	}
	tc.createFixtureResource(t, gvk.Pod, probe)
	t.Cleanup(func() {
		if err := tc.Client().Delete(tc.Context(), probe); err != nil && !errors.IsNotFound(err) {
			t.Logf("failed to clean up NetworkPolicy probe %s: %v", probeName, err)
		}
	})

	tc.g.Eventually(func() error {
		current := &corev1.Pod{}
		if err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: probeName, Namespace: namespace}, current); err != nil {
			return err
		}
		switch current.Status.Phase {
		case corev1.PodSucceeded:
			return nil
		case corev1.PodFailed:
			for _, status := range current.Status.ContainerStatuses {
				if status.State.Terminated != nil {
					return StopErr(fmt.Errorf("pod phase is %s, container %s exited %d: %s",
						current.Status.Phase, status.Name, status.State.Terminated.ExitCode,
						status.State.Terminated.Message), "NetworkPolicy probe %s failed", probeName)
				}
			}
			return StopErr(fmt.Errorf("pod phase is %s", current.Status.Phase), "NetworkPolicy probe %s failed", probeName)
		default:
			return fmt.Errorf("NetworkPolicy probe %s is %s", probeName, current.Status.Phase)
		}
	}).WithTimeout(5 * time.Minute).WithPolling(2 * time.Second).Should(Succeed())
}

func (tc *MonitoringTestCtx) networkProbeImage(t *testing.T) string {
	t.Helper()

	pods := &corev1.PodList{}
	require.NoError(t, tc.Client().List(tc.Context(), pods,
		client.InNamespace(tc.MonitoringNamespace),
		client.MatchingLabels{
			"app.kubernetes.io/instance": MonitoringStackName,
			"app.kubernetes.io/name":     "prometheus",
		},
	))
	for _, pod := range pods.Items {
		for _, container := range pod.Spec.Containers {
			if container.Name == "prometheus" && container.Image != "" {
				return container.Image
			}
		}
	}
	// Traces-only and external-exporter configurations have no Prometheus pod.
	// Reuse the pinned curl image already used by the storage fixtures.
	return fakeGCSClientImage
}

func TestNetworkProbeImageWithoutPrometheus(t *testing.T) {
	t.Parallel()
	tc := &MonitoringTestCtx{TestContext: &TestContext{
		client:              fake.NewClientBuilder().Build(),
		ctx:                 context.Background(),
		MonitoringNamespace: "monitoring",
	}}
	require.Equal(t, "curlimages/curl@sha256:58adaa4e8dca9c988bae2aba4ab3434a0bb2da16bbe3f92dec39ec7785166777", tc.networkProbeImage(t))
}

func networkPolicyProbeScript(endpoints []string, allowed bool) string {
	if allowed {
		var script strings.Builder
		script.WriteString("set -eu\n")
		for _, endpoint := range endpoints {
			fmt.Fprintf(&script, "status=$(curl --noproxy '*' %s-skS --connect-timeout 5 --max-time 15 -o /dev/null -w '%%{http_code}' %q)\n", otlpGRPCCurlOption(endpoint), endpoint)
			script.WriteString("test \"$status\" != 000\n")
		}
		return script.String()
	}

	var script strings.Builder
	script.WriteString("set -u\n")
	for _, endpoint := range endpoints {
		fmt.Fprintf(&script,
			"connection=$(curl --noproxy '*' %s-skS --connect-timeout 5 --max-time 15 -o /dev/null -w '%%{num_connects}:%%{http_code}' %q)\n",
			otlpGRPCCurlOption(endpoint), endpoint)
		script.WriteString("result=$?\n")
		script.WriteString("test \"$connection\" = 0:000 || exit 1\n")
		script.WriteString("test \"$result\" -eq 7 -o \"$result\" -eq 28 || exit 1\n")
	}
	return script.String()
}

func otlpGRPCCurlOption(endpoint string) string {
	if strings.Contains(endpoint, ":4317/") {
		return "--http2-prior-knowledge "
	}
	return ""
}

func TestDeniedNetworkPolicyProbeChecksEveryEndpoint(t *testing.T) {
	t.Parallel()

	const fakeCurl = `curl() {
  case "$*" in
    *reachable*) printf '1:200'; return 0 ;;
    *blocked*) printf '0:000'; return 28 ;;
    *dns-error*) printf '0:000'; return 6 ;;
  esac
  return 1
}
`
	for _, test := range []struct {
		name      string
		endpoints []string
		wantError bool
	}{
		{name: "both blocked", endpoints: []string{"http://blocked-first", "http://blocked-last"}},
		{name: "first reachable", endpoints: []string{"http://reachable", "http://blocked"}, wantError: true},
		{name: "last reachable", endpoints: []string{"http://blocked", "http://reachable"}, wantError: true},
		{name: "DNS failure is not policy enforcement", endpoints: []string{"http://dns-error", "http://blocked"}, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			// Execute only the fixed shell fixture and endpoint literals above.
			//nolint:gosec // G204: shell execution is the behavior under test; no external input is used.
			output, err := exec.CommandContext(t.Context(), "/bin/sh", "-c",
				fakeCurl+networkPolicyProbeScript(test.endpoints, false)).CombinedOutput()
			if test.wantError {
				require.Error(t, err, "probe accepted an unverified denial: %s", output)
				return
			}
			require.NoError(t, err, "probe rejected blocked endpoints: %s", output)
		})
	}
}

func policiesSelectingPods(t *testing.T, policies []networkingv1.NetworkPolicy, pods []corev1.Pod) []networkingv1.NetworkPolicy {
	t.Helper()

	selected := make([]networkingv1.NetworkPolicy, 0)
	for _, policy := range policies {
		if policy.DeletionTimestamp != nil {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(&policy.Spec.PodSelector)
		require.NoError(t, err, "NetworkPolicy %s has an invalid pod selector", policy.Name)
		for _, pod := range pods {
			if selector.Matches(labels.Set(pod.Labels)) {
				selected = append(selected, policy)
				break
			}
		}
	}
	return selected
}

func requireOnlyIngressPolicy(t *testing.T, policies []networkingv1.NetworkPolicy, expectedName string) {
	t.Helper()
	for _, policy := range policies {
		if len(policy.Spec.Ingress) == 0 {
			continue
		}
		require.Equal(t, expectedName, policy.Name, "unexpected NetworkPolicy grants ingress to collector or TargetAllocator pods")
		for index, rule := range policy.Spec.Ingress {
			require.NotEmpty(t, rule.From, "NetworkPolicy %s ingress rule %d must declare explicit peers", policy.Name, index)
			for peerIndex, peer := range rule.From {
				require.True(t, networkPolicyPeerRestricted(peer),
					"NetworkPolicy %s ingress peer %d must not be unrestricted", policy.Name, peerIndex)
			}
		}
	}
}

func networkPolicyPeerRestricted(peer networkingv1.NetworkPolicyPeer) bool {
	for _, selector := range []*metav1.LabelSelector{peer.PodSelector, peer.NamespaceSelector} {
		if selector != nil && (len(selector.MatchLabels) > 0 || len(selector.MatchExpressions) > 0) {
			return true
		}
	}
	return peer.IPBlock != nil && peer.IPBlock.CIDR != "0.0.0.0/0" && peer.IPBlock.CIDR != "::/0"
}

func TestPoliciesSelectingPodsUsesActualLabels(t *testing.T) {
	t.Parallel()

	pods := []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
		"app.kubernetes.io/component": "opentelemetry-collector",
		"operator.example/extra":      "selected",
	}}}}
	policies := []networkingv1.NetworkPolicy{{
		ObjectMeta: metav1.ObjectMeta{Name: "extra-selector"},
		Spec: networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{
			"app.kubernetes.io/component": "opentelemetry-collector",
			"operator.example/extra":      "selected",
		}}},
	}}
	require.Equal(t, []networkingv1.NetworkPolicy{policies[0]}, policiesSelectingPods(t, policies, pods))
}

func TestNetworkPolicyPeerRestricted(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		peer       networkingv1.NetworkPolicyPeer
		restricted bool
	}{
		{name: "no selectors"},
		{name: "empty pod selector", peer: networkingv1.NetworkPolicyPeer{PodSelector: &metav1.LabelSelector{}}},
		{name: "empty namespace selector", peer: networkingv1.NetworkPolicyPeer{NamespaceSelector: &metav1.LabelSelector{}}},
		{name: "both empty selectors", peer: networkingv1.NetworkPolicyPeer{PodSelector: &metav1.LabelSelector{}, NamespaceSelector: &metav1.LabelSelector{}}},
		{name: "all IPv4", peer: networkingv1.NetworkPolicyPeer{IPBlock: &networkingv1.IPBlock{CIDR: "0.0.0.0/0"}}},
		{name: "all IPv6", peer: networkingv1.NetworkPolicyPeer{IPBlock: &networkingv1.IPBlock{CIDR: "::/0"}}},
		{name: "pod labels", peer: networkingv1.NetworkPolicyPeer{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "client"}}}, restricted: true},
		{name: "namespace expression", peer: networkingv1.NetworkPolicyPeer{
			NamespaceSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key: "team", Operator: metav1.LabelSelectorOpIn, Values: []string{"ai"},
			}}},
		}, restricted: true},
		{name: "narrow CIDR", peer: networkingv1.NetworkPolicyPeer{IPBlock: &networkingv1.IPBlock{CIDR: "172.20.0.1/32"}}, restricted: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.restricted, networkPolicyPeerRestricted(test.peer))
		})
	}
}

func policyByName(t *testing.T, policies []networkingv1.NetworkPolicy, name string) *networkingv1.NetworkPolicy {
	t.Helper()
	for index := range policies {
		if policies[index].Name == name {
			return &policies[index]
		}
	}
	t.Fatalf("NetworkPolicy %s was not found", name)
	return nil
}

func networkPolicyPorts(rule networkingv1.NetworkPolicyIngressRule) []int32 {
	ports := make([]int32, 0, len(rule.Ports))
	for _, port := range rule.Ports {
		if port.Port != nil {
			ports = append(ports, port.Port.IntVal)
		}
	}
	return ports
}
