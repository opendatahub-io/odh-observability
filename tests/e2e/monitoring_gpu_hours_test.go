package e2e_test

import (
	"crypto/x509"
	"fmt"
	"net/http"
	"testing"
	"time"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega
)

// ValidateAcceleratorGPUHoursRecording exercises the GPU-hours attribution
// recording rules without requiring GPU hardware: a synthetic DCGM exporter in
// the trusted GPU-operator namespace reports an allocated-but-idle GPU
// (utilization 0) for a real tenant pod, and the recorded billing metrics are
// checked through the namespace proxy. The Kueue subtest additionally proves
// the platform kube-state-metrics exposes pod labels through federation; on
// clusters whose KSM allowlist filters kueue.x-k8s.io/* it fails, which is the
// signal that Tier 2 degraded to passthrough. The reserved-name subtest
// proves a workload cannot seed recording-rule output names through the
// workload scrape path.
func (tc *MonitoringTestCtx) ValidateAcceleratorGPUHoursRecording(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	const operatorNamespace = "nvidia-gpu-operator"
	if err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: operatorNamespace}, &corev1.Namespace{}); err != nil {
		if k8serr.IsNotFound(err) {
			t.Skip("GPU operator namespace is not installed")
		}
		t.Fatalf("read GPU operator namespace: %v", err)
	}
	tc.setupMetrics(t)
	id := fmt.Sprintf("%x", time.Now().UnixNano())
	// Reset the collector configuration before the fixture namespaces go away.
	t.Cleanup(tc.resetMonitoringConfigToMetrics)
	tc.updateMonitoringConfig(withManagementState(common.Managed), tc.withMetricsConfig(), jq.Transform(
		`.spec.collectorReplicas = 2 | del(.spec.metrics.builtinDCGMScrapeEnabled, .spec.metrics.builtinDCGMTLS)`))
	tc.ensureOpenTelemetryCollectorReady(t)

	namespace := "tests-gpu-hours-" + id
	foreign := "tests-gpu-hours-foreign-" + id
	// The tenant namespace opts into monitoring so the forge fixture's
	// ServiceMonitor below is admitted and scraped by the collector.
	tc.createFixtureResource(t, gvk.Namespace, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:   namespace,
		Labels: map[string]string{ODHLabelMonitoring: "true"},
	}})
	tc.createFixtureResource(t, gvk.Namespace, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: foreign}})
	for _, name := range []string{namespace, foreign} {
		t.Cleanup(func() {
			tc.DeleteResource(WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: name}), WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))
		})
	}

	// A real running pod so platform kube-state-metrics reports its Kueue
	// labels for the Tier 2 join. The synthetic GPU metrics below attribute a
	// device to it.
	workload := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "model",
			Namespace: namespace,
			Labels: map[string]string{
				"kueue.x-k8s.io/cluster-queue-name": "cq-e2e-" + id,
				"kueue.x-k8s.io/local-queue-name":   "lq-e2e-" + id,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			AutomountServiceAccountToken: new(false),
			SecurityContext:              fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name:            "workload",
				Image:           "registry.access.redhat.com/ubi9/python-311:latest",
				Command:         []string{"/bin/sh", "-c"},
				Args:            []string{"sleep 3600"},
				SecurityContext: fixtureContainerSecurityContext(),
			}},
		},
	}
	tc.createFixtureResource(t, gvk.Pod, workload)
	t.Cleanup(func() {
		tc.DeleteResource(WithMinimalObject(gvk.Pod, types.NamespacedName{Name: workload.Name, Namespace: namespace}),
			WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))
	})

	// Synthetic DCGM exporter in the trusted namespace: the GPU is allocated
	// (GPU_UTIL present, value 0) but idle, with occupancy 0.5.
	gpu := "GPU-e2e-" + id
	data := fmt.Sprintf("# TYPE DCGM_FI_DEV_GPU_UTIL gauge\n"+
		"DCGM_FI_DEV_GPU_UTIL{namespace=%q,pod=\"model\",modelName=\"E2E-GPU\",UUID=%q,gpu=\"0\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} 0\n"+
		"# TYPE DCGM_FI_PROF_GR_ENGINE_ACTIVE gauge\n"+
		"DCGM_FI_PROF_GR_ENGINE_ACTIVE{namespace=%q,pod=\"model\",modelName=\"E2E-GPU\",UUID=%q,gpu=\"0\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} 0.5\n",
		namespace, gpu, namespace, gpu)
	server := fmt.Sprintf(`from http.server import BaseHTTPRequestHandler, HTTPServer
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  data = %q.encode()
  self.send_response(200)
  self.send_header('Content-Type', 'text/plain; version=0.0.4')
  self.send_header('Content-Length', str(len(data)))
  self.end_headers()
  self.wfile.write(data)
httpd = HTTPServer(('0.0.0.0', 9400), Handler)
httpd.serve_forever()
`, data)
	exporter := "nvidia-dcgm-exporter-e2e-" + id
	tc.createFixtureResource(t, gvk.Pod, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: exporter, Namespace: operatorNamespace, Labels: map[string]string{"app": "odh-dcgm-fixture"}},
		Spec: corev1.PodSpec{
			SecurityContext: fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name:            "fixture",
				Image:           "registry.access.redhat.com/ubi9/python-311:latest",
				Command:         []string{"python3", "-c", server},
				Ports:           []corev1.ContainerPort{{ContainerPort: 9400}},
				SecurityContext: fixtureContainerSecurityContext(),
			}},
		},
	})
	t.Cleanup(func() {
		tc.DeleteResource(WithMinimalObject(gvk.Pod, types.NamespacedName{Name: exporter, Namespace: operatorNamespace}),
			WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))
	})

	// A workload in the tenant namespace tries to seed a reserved
	// recording-rule name through the ordinary workload scrape path. The
	// reserved-name subtest below asserts the ingestion drop; the control
	// probe series on the same endpoint makes that assertion meaningful.
	forge := tc.createReservedNameForge(t, namespace, gpu, id)

	viewer := tc.metricsViewerToken(t, "gpu-hours-viewer-"+id, namespace)
	foreignViewer := tc.metricsViewerToken(t, "gpu-hours-foreign-viewer-"+id, foreign)
	monitorViewer := tc.metricsViewerToken(t, "gpu-hours-monitor-viewer-"+id, tc.MonitoringNamespace)
	rootCAs := clusterIngressCAPool(t, tc)
	host := tc.fetchPrometheusRouteHost(t)

	// Tier 1: zero utilization still counts as allocated, with the trusted
	// workload attribution promoted onto the recorded series.
	tc.awaitGPUHoursAllocation(t, rootCAs, host, viewer, namespace, gpu)

	// Tier 1: utilization-based GPU-hours track the occupancy fraction.
	tc.awaitGPUHoursUtilization(t, rootCAs, host, viewer, namespace, gpu)

	// Tier 3: the primary billing shape aggregates the tenant's devices.
	tc.awaitGPUHoursByNamespace(t, rootCAs, host, viewer, namespace)

	// Health: the collector's scrape of the fixture exporter is up.
	tc.awaitDCGMScrapeUp(t, rootCAs, host, monitorViewer)

	// Tier 2: the Kueue join enriches the tenant's allocation with the
	// workload's queue labels. Failure here means the platform KSM pod-label
	// allowlist filters kueue.x-k8s.io/* and Tier 2 degraded to passthrough.
	t.Run("Kueue queue attribution", func(t *testing.T) {
		tc.awaitKueueQueueAttribution(t, rootCAs, host, viewer, namespace, gpu, id)
	})

	// Reserved names: a workload-supplied series seeding a recording-rule
	// output name must be dropped at ingestion, leaving billing unchanged.
	t.Run("Reserved recording-rule names", func(t *testing.T) {
		tc.awaitReservedNameRejection(t, rootCAs, host, viewer, namespace, forge)
	})

	// Tenant isolation: a foreign viewer sees no billing data for the tenant.
	query := fmt.Sprintf(`accelerator_gpu_hours_allocated{UUID=%q}`, gpu)
	status, response, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, foreignViewer, foreign, query)
	if err != nil || status != http.StatusOK || len(response.Data.Result) != 0 {
		t.Fatalf("foreign tenant can see GPU-hours sample: status=%d count=%d error=%v", status, len(response.Data.Result), err)
	}
}

// awaitPrometheusQuery polls a PromQL query through the namespace proxy until
// assert accepts the response.
func (tc *MonitoringTestCtx) awaitPrometheusQuery(t *testing.T, rootCAs *x509.CertPool, host, token, namespace, query string,
	assert func(prometheusQueryResponse) error,
) {
	t.Helper()
	// Bind the matcher to this helper's t, not tc.g (the outer test): the
	// Kueue subtest calls through here, and a failure must be reported on
	// the subtest with its assertion message, not as a generic Goexit error
	// on the outer test.
	g := NewWithT(t)
	g.Eventually(func() error {
		status, response, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, token, namespace, query)
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("query %q: unexpected HTTP status %d", query, status)
		}
		return assert(response)
	}, 10*time.Minute, 15*time.Second).Should(Succeed())
}

// awaitGPUHoursAllocation waits for the tenant's allocated GPU: value 1 with
// the trusted workload attribution promoted onto the recorded series, and no
// cluster label collision between the static rule label and externalLabels.
func (tc *MonitoringTestCtx) awaitGPUHoursAllocation(t *testing.T, rootCAs *x509.CertPool, host, viewer, namespace, gpu string) {
	t.Helper()
	query := fmt.Sprintf(`accelerator_gpu_hours_allocated{UUID=%q}`, gpu)
	tc.awaitPrometheusQuery(t, rootCAs, host, viewer, namespace, query, func(response prometheusQueryResponse) error {
		if len(response.Data.Result) != 1 {
			return fmt.Errorf("expected one allocated GPU sample: count=%d", len(response.Data.Result))
		}
		result := response.Data.Result[0]
		if result.Metric["namespace"] != namespace || result.Metric["pod"] != "model" || result.Metric["modelName"] != "E2E-GPU" ||
			len(result.Value) != 2 || result.Value[1] != "1" {
			return fmt.Errorf("incorrect allocation attribution or value: %v %v", result.Metric, result.Value)
		}
		if cluster := result.Metric["cluster"]; cluster != "" && result.Metric["exported_cluster"] != "" {
			return fmt.Errorf("cluster label collision: cluster=%q exported_cluster=%q", cluster, result.Metric["exported_cluster"])
		}
		return nil
	})
}

// awaitGPUHoursUtilization waits for the utilization-based GPU-hours to track
// the synthetic exporter's occupancy fraction.
func (tc *MonitoringTestCtx) awaitGPUHoursUtilization(t *testing.T, rootCAs *x509.CertPool, host, viewer, namespace, gpu string) {
	t.Helper()
	query := fmt.Sprintf(`accelerator_gpu_hours_utilized{UUID=%q}`, gpu)
	tc.awaitPrometheusQuery(t, rootCAs, host, viewer, namespace, query, func(response prometheusQueryResponse) error {
		if len(response.Data.Result) != 1 || response.Data.Result[0].Value[1] != "0.5" {
			return fmt.Errorf("expected utilization 0.5: results=%v", response.Data.Result)
		}
		return nil
	})
}

// awaitGPUHoursByNamespace waits for the primary billing shape to aggregate
// the tenant's devices.
func (tc *MonitoringTestCtx) awaitGPUHoursByNamespace(t *testing.T, rootCAs *x509.CertPool, host, viewer, namespace string) {
	t.Helper()
	const query = `accelerator_gpu_hours_allocated_by_namespace`
	tc.awaitPrometheusQuery(t, rootCAs, host, viewer, namespace, query, func(response prometheusQueryResponse) error {
		if len(response.Data.Result) != 1 ||
			response.Data.Result[0].Metric["namespace"] != namespace || response.Data.Result[0].Value[1] != "1" {
			return fmt.Errorf("expected one allocated GPU for the tenant: results=%v", response.Data.Result)
		}
		return nil
	})
}

// awaitDCGMScrapeUp waits for the collector's scrape of the fixture exporter
// to be reported up. The up series carries the collector's monitoring
// namespace, so it is queried as a monitor viewer.
func (tc *MonitoringTestCtx) awaitDCGMScrapeUp(t *testing.T, rootCAs *x509.CertPool, host, monitorViewer string) {
	t.Helper()
	const query = `accelerator_dcgm_scrape_up{component="odh-dcgm-fixture"}`
	tc.awaitPrometheusQuery(t, rootCAs, host, monitorViewer, tc.MonitoringNamespace, query, func(response prometheusQueryResponse) error {
		if len(response.Data.Result) != 1 || response.Data.Result[0].Value[1] != "1" {
			return fmt.Errorf("expected the fixture exporter scrape to be up: results=%v", response.Data.Result)
		}
		return nil
	})
}

// awaitKueueQueueAttribution waits for the Kueue join to enrich the tenant's
// allocation with the workload's queue labels. Failure here means the platform
// kube-state-metrics pod-label allowlist filters kueue.x-k8s.io/* and Tier 2
// degraded to passthrough.
func (tc *MonitoringTestCtx) awaitKueueQueueAttribution(t *testing.T, rootCAs *x509.CertPool, host, viewer, namespace, gpu, id string) {
	t.Helper()
	query := fmt.Sprintf(`accelerator_gpu_hours_allocated_kueue{UUID=%q}`, gpu)
	tc.awaitPrometheusQuery(t, rootCAs, host, viewer, namespace, query, func(response prometheusQueryResponse) error {
		if len(response.Data.Result) != 1 {
			return fmt.Errorf("expected one Kueue-enriched GPU sample: count=%d", len(response.Data.Result))
		}
		result := response.Data.Result[0]
		if result.Metric["label_kueue_x_k8s_io_cluster_queue_name"] != "cq-e2e-"+id ||
			result.Metric["label_kueue_x_k8s_io_local_queue_name"] != "lq-e2e-"+id {
			return fmt.Errorf("Kueue queue labels missing from GPU-hours attribution; the platform "+
				"kube-state-metrics pod-label allowlist may filter kueue.x-k8s.io/*: %v", result.Metric)
		}
		return nil
	})
}

// createReservedNameForge creates a workload in the tenant namespace that
// exposes a reserved recording-rule name through the ordinary workload scrape
// path: accelerator_gpu_hours_allocated with value -1, mirroring the tenant's
// real GPU labels so a missing ingestion drop would poison the billing
// aggregate. A control probe series on the same endpoint lets the rejection
// test distinguish "the drop worked" from "the scrape never happened".
// Returns the forge pod's name.
func (tc *MonitoringTestCtx) createReservedNameForge(t *testing.T, namespace, gpu, id string) string {
	t.Helper()
	forge := "reserved-name-forge-" + id
	data := fmt.Sprintf("# TYPE accelerator_gpu_hours_allocated gauge\n"+
		"accelerator_gpu_hours_allocated{modelName=\"E2E-GPU\",UUID=%q,gpu=\"0\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} -1\n"+
		"# TYPE gpu_hours_reserved_name_probe gauge\n"+
		"gpu_hours_reserved_name_probe 1\n", gpu)
	server := fmt.Sprintf(`from http.server import BaseHTTPRequestHandler, HTTPServer
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  data = %q.encode()
  self.send_response(200)
  self.send_header('Content-Type', 'text/plain; version=0.0.4')
  self.send_header('Content-Length', str(len(data)))
  self.end_headers()
  self.wfile.write(data)
httpd = HTTPServer(('0.0.0.0', 9401), Handler)
httpd.serve_forever()
`, data)
	tc.createFixtureResource(t, gvk.Pod, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: forge, Namespace: namespace, Labels: map[string]string{"app": forge}},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			AutomountServiceAccountToken: new(false),
			SecurityContext:              fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name:            "forge",
				Image:           "registry.access.redhat.com/ubi9/python-311:latest",
				Command:         []string{"python3", "-c", server},
				Ports:           []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9401}},
				SecurityContext: fixtureContainerSecurityContext(),
			}},
		},
	})
	t.Cleanup(func() {
		tc.DeleteResource(WithMinimalObject(gvk.Pod, types.NamespacedName{Name: forge, Namespace: namespace}),
			WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))
	})
	// The ServiceMonitor selects Services by label, so the Service itself
	// must carry app=forge; the spec selector only picks the backing Pods.
	tc.createFixtureResource(t, gvk.Service, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: forge, Namespace: namespace, Labels: map[string]string{"app": forge}},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": forge},
			Ports: []corev1.ServicePort{{
				Name:       "metrics",
				Port:       9401,
				TargetPort: intstr.FromString("metrics"),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	})
	t.Cleanup(func() {
		tc.DeleteResource(WithMinimalObject(gvk.Service, types.NamespacedName{Name: forge, Namespace: namespace}),
			WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))
	})
	// The ServiceMonitor carries the monitoring label explicitly so
	// discovery by the target allocator does not depend on the admission
	// webhook injecting it.
	tc.createFixtureResource(t, gvk.ServiceMonitorCoreos, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "monitoring.coreos.com/v1",
		"kind":       "ServiceMonitor",
		"metadata": map[string]any{
			"name":      forge,
			"namespace": namespace,
			"labels":    map[string]any{ODHLabelMonitoring: "true"},
		},
		"spec": map[string]any{
			"selector":  map[string]any{"matchLabels": map[string]any{"app": forge}},
			"endpoints": []any{map[string]any{"port": "metrics"}},
		},
	}})
	t.Cleanup(func() {
		tc.DeleteResource(WithMinimalObject(gvk.ServiceMonitorCoreos, types.NamespacedName{Name: forge, Namespace: namespace}),
			WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))
	})
	return forge
}

// awaitReservedNameRejection proves the workload scrape path drops series
// that seed reserved recording-rule names. The forge endpoint's control probe
// must appear first: its visibility proves the scrape reached Prometheus, so
// the reserved name's absence is the drop working, not a missing scrape. The
// forged -1 must not have been summed into the billing aggregate.
func (tc *MonitoringTestCtx) awaitReservedNameRejection(t *testing.T, rootCAs *x509.CertPool, host, viewer, namespace, forgePod string) {
	t.Helper()
	tc.awaitPrometheusQuery(t, rootCAs, host, viewer, namespace, "gpu_hours_reserved_name_probe", func(response prometheusQueryResponse) error {
		if len(response.Data.Result) != 1 || response.Data.Result[0].Value[1] != "1" {
			return fmt.Errorf("expected the forge endpoint's control probe: results=%v", response.Data.Result)
		}
		return nil
	})
	// The scrape is proven, so query directly: an Eventually on absence
	// would pass vacuously before the forge endpoint is scraped at all.
	query := fmt.Sprintf(`accelerator_gpu_hours_allocated{pod=%q}`, forgePod)
	status, response, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, viewer, namespace, query)
	if err != nil || status != http.StatusOK || len(response.Data.Result) != 0 {
		t.Fatalf("forged reserved-name series was ingested: status=%d count=%d error=%v", status, len(response.Data.Result), err)
	}
	status, response, err = queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, viewer, namespace, "accelerator_gpu_hours_allocated_by_namespace")
	if err != nil || status != http.StatusOK {
		t.Fatalf("query billing aggregate: status=%d error=%v", status, err)
	}
	if len(response.Data.Result) != 1 || response.Data.Result[0].Metric["namespace"] != namespace || response.Data.Result[0].Value[1] != "1" {
		t.Fatalf("billing aggregate corrupted by the forged series: results=%v", response.Data.Result)
	}
}
