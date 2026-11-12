package e2e_test

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega matchers
)

// ValidateModelResourceFederation checks quota, container-limit and CPU-usage
// series through the Data Science namespace proxy, including tenant isolation.
func (tc *MonitoringTestCtx) ValidateModelResourceFederation(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	tc.setupMetrics(t)

	runID := strconv.FormatInt(time.Now().UnixNano(), 16)
	namespace := "tests-cpu-federation-" + runID
	foreignNamespace := "tests-cpu-foreign-" + runID
	for _, name := range []string{namespace, foreignNamespace} {
		tc.createFixtureResource(t, gvk.Namespace, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}})
		t.Cleanup(func() {
			tc.DeleteResource(
				WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: name}),
				WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute),
			)
		})
	}

	quotaKind := schema.GroupVersionKind{Version: "v1", Kind: "ResourceQuota"}
	quotas := []corev1.ResourceQuota{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "cpu-quota-a", Namespace: namespace},
			Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
				corev1.ResourceRequestsCPU: resource.MustParse("2"),
				corev1.ResourceLimitsCPU:   resource.MustParse("4"),
			}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "cpu-quota-b", Namespace: namespace},
			Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("3"),
			}},
		},
	}
	for i := range quotas {
		quota := &quotas[i]
		tc.createFixtureResource(t, quotaKind, quota)
		tc.g.Eventually(func() error {
			var current corev1.ResourceQuota
			if err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: quota.Name, Namespace: namespace}, &current); err != nil {
				return err
			}
			if len(current.Status.Hard) != len(quota.Spec.Hard) {
				return fmt.Errorf("quota %s has not initialized", quota.Name)
			}
			return nil
		}, 2*time.Minute, 5*time.Second).Should(Succeed(), "quota accounting must initialize before creating the fixture pod")
	}

	pod := modelResourceFixturePod(namespace)
	podName := pod.Name
	tc.createFixtureResource(t, gvk.Pod, pod)
	tc.g.Eventually(func() error {
		var current corev1.Pod
		if err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: podName, Namespace: namespace}, &current); err != nil {
			return err
		}
		if !podIsReady(current) {
			return fmt.Errorf("pod %s/%s is not ready: phase=%s container statuses=%+v", namespace, podName, current.Status.Phase, current.Status.ContainerStatuses)
		}
		return nil
	}, 5*time.Minute, 5*time.Second).Should(Succeed(), "CPU usage requires a running fixture container")

	viewer := tc.metricsViewerToken(t, "cpu-federation-viewer", namespace)
	foreignViewer := tc.metricsViewerToken(t, "cpu-federation-foreign-viewer", foreignNamespace)
	rootCAs := clusterIngressCAPool(t, tc)
	routeHost := tc.fetchPrometheusRouteHost(t)
	checks := []struct {
		query         string
		want          float64
		variableValue bool
		podLabels     bool
	}{
		{query: `kube_resourcequota{resourcequota="cpu-quota-a",resource="requests.cpu",type="hard",odh_federation_source="model-resources"}`, want: 2},
		{query: `kube_resourcequota{resourcequota="cpu-quota-a",resource="requests.cpu",type="used",odh_federation_source="model-resources"}`, want: 0.1},
		{query: `kube_resourcequota{resourcequota="cpu-quota-a",resource="limits.cpu",type="hard",odh_federation_source="model-resources"}`, want: 4},
		{query: `kube_resourcequota{resourcequota="cpu-quota-a",resource="limits.cpu",type="used",odh_federation_source="model-resources"}`, want: 0.5},
		{query: `kube_resourcequota{resourcequota="cpu-quota-b",resource="cpu",type="hard",odh_federation_source="model-resources"}`, want: 3},
		{query: `kube_resourcequota{resourcequota="cpu-quota-b",resource="cpu",type="used",odh_federation_source="model-resources"}`, want: 0.1},
		{
			query: `kube_pod_container_resource_limits{pod="cpu-limit-fixture",container="fixture",resource="cpu",unit="core",odh_federation_source="model-resources"}`,
			want:  0.5, podLabels: true,
		},
		{
			query:         `node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{pod="cpu-limit-fixture",container="fixture",odh_federation_source="model-resources"}`,
			variableValue: true, podLabels: true,
		},
	}
	for _, check := range checks {
		tc.g.Eventually(func() error {
			status, response, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, routeHost, viewer, namespace, check.query)
			if err != nil {
				return err
			}
			if status != http.StatusOK || len(response.Data.Result) == 0 {
				return fmt.Errorf("query %q: HTTP %d, %d series", check.query, status, len(response.Data.Result))
			}
			for _, result := range response.Data.Result {
				if result.Metric["namespace"] != namespace || len(result.Value) != 2 {
					return fmt.Errorf("query %q returned invalid namespace or value", check.query)
				}
				if check.podLabels && (result.Metric["pod"] != podName || result.Metric["container"] != "fixture") {
					return fmt.Errorf("query %q returned invalid pod or container", check.query)
				}
				valueText, ok := result.Value[1].(string)
				if !ok {
					return fmt.Errorf("query %q returned non-string value", check.query)
				}
				value, err := strconv.ParseFloat(valueText, 64)
				if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
					return fmt.Errorf("query %q returned non-finite value %q", check.query, valueText)
				}
				if !check.variableValue && math.Abs(value-check.want) > 0.000001 {
					return fmt.Errorf("query %q value %q, want %v", check.query, valueText, check.want)
				}
			}
			return nil
		}, 10*time.Minute, 15*time.Second).Should(Succeed(), "federated resource series must reach the Data Science namespace proxy")

		status, response, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, routeHost, foreignViewer, foreignNamespace, check.query)
		if err != nil || status != http.StatusOK || len(response.Data.Result) != 0 {
			t.Fatalf("foreign namespace must not see query %q: HTTP %d, %d series, error=%v", check.query, status, len(response.Data.Result), err)
		}
	}
}

// The stock Prometheus image stores its TSDB in /prometheus, which is owned by
// the image user. OpenShift's restricted SCC assigns a different UID and an
// emptyDir is needed for the process to start (as with the tenant fixture).
func modelResourceFixturePod(namespace string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "cpu-limit-fixture", Namespace: namespace},
		Spec: corev1.PodSpec{
			SecurityContext: fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name: "fixture", Image: tenantMetricsImage,
				SecurityContext: fixtureContainerSecurityContext(),
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
					Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("256Mi")},
				},
				VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/prometheus"}},
			}},
			Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
		},
	}
}

func TestModelResourceFixturePod(t *testing.T) {
	pod := modelResourceFixturePod("test-namespace")
	if len(pod.Spec.Containers) != 1 || len(pod.Spec.Volumes) != 1 || pod.Spec.Volumes[0].EmptyDir == nil {
		t.Fatalf("fixture must have one container and writable emptyDir: %+v", pod.Spec)
	}
	container := pod.Spec.Containers[0]
	if len(container.VolumeMounts) != 1 || container.VolumeMounts[0].Name != pod.Spec.Volumes[0].Name ||
		container.VolumeMounts[0].MountPath != "/prometheus" || container.VolumeMounts[0].ReadOnly {
		t.Fatalf("fixture must mount the emptyDir at the Prometheus TSDB path: %+v", container.VolumeMounts)
	}
}
