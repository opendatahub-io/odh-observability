package e2e_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega
)

const prometheusTransitionTimeout = 12 * time.Minute

func (tc *MonitoringTestCtx) prometheusPods(t *testing.T) (*corev1.PodList, error) {
	t.Helper()
	pods := &corev1.PodList{}
	err := tc.Client().List(tc.Context(), pods,
		client.InNamespace(tc.MonitoringNamespace),
		client.MatchingLabels{
			"app.kubernetes.io/name":     "prometheus",
			"app.kubernetes.io/instance": MonitoringStackName,
		},
	)
	return pods, err
}

// The MonitoringStack CR can disappear while its StatefulSet pods are still
// terminating. Recreating metrics before those pod names are released stalls
// the new StatefulSet, especially with Prometheus's 600-second grace period.
func (tc *MonitoringTestCtx) waitForPrometheusPodsGone(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Log("Waiting for previous Prometheus pods to finish terminating")
	tc.g.Eventually(func() error {
		pods, err := tc.prometheusPods(t)
		if err != nil {
			return err
		}
		if len(pods.Items) == 0 {
			return nil
		}
		pending := make([]string, 0, len(pods.Items))
		for _, pod := range pods.Items {
			pending = append(pending, fmt.Sprintf("%s (deleting=%t, phase=%s)",
				pod.Name, pod.DeletionTimestamp != nil, pod.Status.Phase))
		}
		return fmt.Errorf("Prometheus pods remain: %s", strings.Join(pending, ", "))
	}).WithTimeout(prometheusTransitionTimeout).WithPolling(5 * time.Second).Should(Succeed())
}

// A Ready Monitoring CR does not guarantee that Prometheus has finished a
// rollout or can supply data to Thanos. Check the generated workload as well.
func (tc *MonitoringTestCtx) waitForPrometheusReady(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Log("Waiting for Prometheus StatefulSet and pods to be ready")
	tc.g.Eventually(func() error {
		statefulSet := &appsv1.StatefulSet{}
		if err := tc.Client().Get(tc.Context(), types.NamespacedName{
			Name:      "prometheus-" + MonitoringStackName,
			Namespace: tc.MonitoringNamespace,
		}, statefulSet); err != nil {
			return err
		}
		if statefulSet.Spec.Replicas == nil || *statefulSet.Spec.Replicas == 0 {
			return errors.New("Prometheus StatefulSet has no desired replicas")
		}
		want := *statefulSet.Spec.Replicas
		if statefulSet.Status.ObservedGeneration < statefulSet.Generation ||
			statefulSet.Status.ReadyReplicas != want || statefulSet.Status.UpdatedReplicas != want {
			return fmt.Errorf("Prometheus StatefulSet generation=%d observed=%d ready=%d updated=%d desired=%d",
				statefulSet.Generation, statefulSet.Status.ObservedGeneration,
				statefulSet.Status.ReadyReplicas, statefulSet.Status.UpdatedReplicas, want)
		}
		pods, err := tc.prometheusPods(t)
		if err != nil {
			return err
		}
		ready := 0
		for _, pod := range pods.Items {
			if pod.DeletionTimestamp != nil {
				return fmt.Errorf("Prometheus pod %s is still terminating", pod.Name)
			}
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
					ready++
					break
				}
			}
		}
		if int32(ready) != want {
			return fmt.Errorf("Prometheus ready pods=%d desired=%d", ready, want)
		}
		return nil
	}).WithTimeout(prometheusTransitionTimeout).WithPolling(5 * time.Second).Should(Succeed())
}
