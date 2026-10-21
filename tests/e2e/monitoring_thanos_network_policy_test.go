package e2e_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega
)

// ValidateThanosQuerierIngressConnectivity checks the deployed policy against
// the CNI and the labels on the actual operand pods. Each positive probe copies
// only the policy selector labels verified on a ready client pod.
func (tc *MonitoringTestCtx) ValidateThanosQuerierIngressConnectivity(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(tc.resetMonitoringConfigToMetrics)
	tc.updateMonitoringConfig(withManagementState(common.Managed), tc.withMetricsConfig())

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
			Name:      "data-science-thanos-querier-ingress",
			Namespace: tc.MonitoringNamespace,
		}),
	)
	tc.assertReadyPodLabels(t, "querier", thanosQuerierDeploymentName+"-", map[string]string{
		"app.kubernetes.io/part-of":    "ThanosQuerier",
		"app.kubernetes.io/managed-by": "observability-operator",
	})

	endpoint := fmt.Sprintf("http://%s.%s.svc.cluster.local:10902/api/v1/query?query=up",
		thanosQuerierDeploymentName, tc.MonitoringNamespace)
	clients := []struct {
		name      string
		podPrefix string
		selector  map[string]string
		allowed   bool
	}{
		{
			name:      "prometheus",
			podPrefix: "prometheus-" + MonitoringStackName + "-",
			selector: map[string]string{
				"app.kubernetes.io/name":     "prometheus",
				"app.kubernetes.io/instance": MonitoringStackName,
			},
			allowed: true,
		},
		// Perses must not reach Thanos :10902 directly; queries go through
		// data-science-thanos-querier-proxy so data-science-metrics-view applies.
		{
			name:      "perses",
			podPrefix: persesPodName,
			selector: map[string]string{
				"instance":               PersesName,
				"app.kubernetes.io/name": PersesName,
			},
			allowed: false,
		},
		{
			name:      "korrel8r",
			podPrefix: korrel8rName + "-",
			selector: map[string]string{
				"app.kubernetes.io/name":      korrel8rName,
				"app.kubernetes.io/component": "correlation",
				"app.kubernetes.io/part-of":   "monitoring",
			},
			allowed: true,
		},
		{
			name:      "authenticated-proxy",
			podPrefix: ThanosQuerierProxyName + "-",
			selector: map[string]string{
				"app": ThanosQuerierProxyName,
			},
			allowed: true,
		},
	}

	for _, source := range clients {
		t.Run(source.name, func(t *testing.T) {
			tc := tc.WithT(t)
			tc.assertReadyPodLabels(t, source.name, source.podPrefix, source.selector)
			tc.runThanosIngressProbe(t, source.name, tc.MonitoringNamespace, source.selector, endpoint, source.allowed)
		})
	}

	t.Run("other-namespace-denied", func(t *testing.T) {
		tc := tc.WithT(t)
		probeNamespace := fmt.Sprintf("thanos-ingress-probe-%x", time.Now().UnixNano())
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: probeNamespace}}
		if err := tc.Client().Create(tc.Context(), namespace); err != nil {
			t.Fatalf("create untrusted probe namespace: %v", err)
		}
		t.Cleanup(func() {
			if err := tc.Client().Delete(tc.Context(), namespace); err != nil && !k8serr.IsNotFound(err) {
				t.Logf("clean up probe namespace %s: %v", probeNamespace, err)
			}
		})
		tc.runThanosIngressProbe(t, "untrusted", probeNamespace, nil, endpoint, false)
	})
}

func (tc *MonitoringTestCtx) assertReadyPodLabels(t *testing.T, name, podPrefix string, selector map[string]string) {
	t.Helper()
	tc.g.Eventually(func() error {
		pods := &corev1.PodList{}
		if err := tc.Client().List(tc.Context(), pods, client.InNamespace(tc.MonitoringNamespace), client.MatchingLabels(selector)); err != nil {
			return fmt.Errorf("list %s pods: %w", name, err)
		}
		for _, pod := range pods.Items {
			if !strings.HasPrefix(pod.Name, podPrefix) {
				continue
			}
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
					t.Logf("%s: ready pod %s has selector labels %v", name, pod.Name, selector)
					return nil
				}
			}
		}
		return fmt.Errorf("no ready generated %s pod with selector labels %v", name, selector)
	}).WithTimeout(3 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
}

func (tc *MonitoringTestCtx) runThanosIngressProbe(t *testing.T, name, namespace string, labels map[string]string, endpoint string, allowed bool) {
	t.Helper()
	probeName := fmt.Sprintf("thanos-ingress-%s-%x", name, time.Now().UnixNano())
	var script string
	if allowed {
		script = fmt.Sprintf(`curl --noproxy '*' -fsS --connect-timeout 5 --max-time 10 -o /dev/null %q`, endpoint)
	} else {
		// Exit 7 (connection refused) or 28 (timeout) with zero completed
		// connections proves a denial. DNS failures and stalled HTTP fail.
		script = fmt.Sprintf(`
connection=$(curl --noproxy '*' -sS --connect-timeout 5 --max-time 8 -o /dev/null -w '%%{num_connects}:%%{http_code}' %q)
result=$?
echo "curl exit=$result connections:http=$connection"
[ "$connection" = "0:000" ] && { [ "$result" -eq 7 ] || [ "$result" -eq 28 ]; }
`, endpoint)
	}
	probe := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: probeName, Namespace: namespace, Labels: labels},
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
			t.Logf("clean up ingress probe %s/%s: %v", namespace, probeName, err)
		}
	})
	tc.g.Eventually(func() error {
		current := &corev1.Pod{}
		if err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: probeName, Namespace: namespace}, current); err != nil {
			return err
		}
		if current.Status.Phase == corev1.PodSucceeded {
			return nil
		}
		if current.Status.Phase == corev1.PodFailed {
			return StopErr(fmt.Errorf("pod failed: %v", current.Status.ContainerStatuses),
				"Thanos ingress probe %s/%s failed", namespace, probeName)
		}
		return fmt.Errorf("Thanos ingress probe %s/%s is %s", namespace, probeName, current.Status.Phase)
	}).WithTimeout(3 * time.Minute).WithPolling(2 * time.Second).Should(Succeed())
	t.Logf("Thanos ingress probe %s from %s completed (allowed=%t)", name, namespace, allowed)
}
