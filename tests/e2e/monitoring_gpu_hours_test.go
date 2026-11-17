package e2e_test

import (
	"crypto/x509"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	k8smeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega
)

// ValidateAcceleratorGPUHoursRecording exercises the GPU-hours attribution
// recording rules without requiring GPU hardware: a synthetic DCGM exporter in
// the trusted GPU-operator namespace reports an allocated-but-idle GPU
// (utilization 0) for a real tenant pod, and the recorded billing metrics are
// checked through the namespace proxy. The fixture also reports a free GPU
// with no workload labels; the unattributed-device subtest proves it stays
// out of every billing shape. The Kueue subtest additionally proves the
// platform kube-state-metrics exposes pod labels through federation; on
// clusters whose KSM allowlist filters kueue.x-k8s.io/* it fails, which is
// the signal that Tier 2 degraded to passthrough. The real-admission subtest
// drives a workload through actual Kueue admission and proves the queue
// labels the join matches are stamped by Kueue itself, not only by
// hand-labeled fixtures. The reserved-name subtest proves a workload cannot
// seed recording-rule output names through the workload scrape path. The
// reassignment subtest proves a device handoff through both scrape hops: the
// timestamp freshness guard collapses the device onto the successor workload
// once the original exporter is gone. The release-expiry subtest proves a
// released device stops being billed within the collector exporter's
// stale-series expiry when the exporter target stays alive and only the
// workload labels disappear.
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
	// Register each namespace's deletion before its create: if admission
	// rejects the second create, the first namespace still cleans up.
	// WithIgnoreNotFound covers namespaces that were never created.
	for _, name := range []string{namespace, foreign} {
		t.Cleanup(func() {
			tc.DeleteResource(WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: name}), WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))
		})
	}
	// The tenant namespace opts into monitoring so the forge fixture's
	// ServiceMonitor below is admitted and scraped by the collector.
	tc.createFixtureResource(t, gvk.Namespace, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:   namespace,
		Labels: map[string]string{ODHLabelMonitoring: "true"},
	}})
	tc.createFixtureResource(t, gvk.Namespace, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: foreign}})

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
	// A second device with no workload labels: DCGM exports hardware series
	// for every physical GPU, free or not, and only workloads bound to a pod
	// carry namespace/pod labels. The free device also reports occupancy, so
	// the unattributed-device subtest can assert it stays out of the
	// utilization-based billing shape as well — without the occupancy sample
	// that assertion would pass vacuously, the rule never seeing free-device
	// input at all.
	freeGPU := "GPU-free-e2e-" + id
	data := fmt.Sprintf("# TYPE DCGM_FI_DEV_GPU_UTIL gauge\n"+
		"DCGM_FI_DEV_GPU_UTIL{namespace=%q,pod=\"model\",modelName=\"E2E-GPU\",UUID=%q,gpu=\"0\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} 0\n"+
		"DCGM_FI_DEV_GPU_UTIL{modelName=\"E2E-FREE-GPU\",UUID=%q,gpu=\"1\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} 0\n"+
		"# TYPE DCGM_FI_PROF_GR_ENGINE_ACTIVE gauge\n"+
		"DCGM_FI_PROF_GR_ENGINE_ACTIVE{namespace=%q,pod=\"model\",modelName=\"E2E-GPU\",UUID=%q,gpu=\"0\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} 0.5\n"+
		"DCGM_FI_PROF_GR_ENGINE_ACTIVE{modelName=\"E2E-FREE-GPU\",UUID=%q,gpu=\"1\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} 0.5\n",
		namespace, gpu, freeGPU, namespace, gpu, freeGPU)
	exporter := "nvidia-dcgm-exporter-e2e-" + id
	tc.dcgmFixtureExporter(t, exporter, data, "", namespace)

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

	// Real Kueue admission: the queue labels the Tier 2 join matches must
	// be stamped on the pod by actual admission, not only by hand-labeled
	// fixtures. Skips when the cluster runs no Kueue with the Deployment
	// integration (a cluster prerequisite, like the GPU operator namespace).
	// Rebinding tc keeps fixture failures on the subtest: a parent-bound
	// matcher called from this goroutine would abort the whole test.
	t.Run("Kueue real admission", func(t *testing.T) {
		tc := tc.WithT(t)
		tc.awaitKueueRealAdmission(t, rootCAs, host, id)
	})

	// Reserved names: a workload-supplied series seeding a recording-rule
	// output name must be dropped at ingestion, leaving billing unchanged.
	t.Run("Reserved recording-rule names", func(t *testing.T) {
		tc.awaitReservedNameRejection(t, rootCAs, host, viewer, namespace, forge)
	})

	// Unattributed device: a free GPU — hardware series with no workload
	// labels — must stay out of every billing shape. The queries go through
	// the monitor viewer in the monitoring namespace: the free device's
	// series carries the collector's Prometheus target labels (the
	// monitoring namespace), not the tenant's, so a tenant-viewer assertion
	// would pass vacuously.
	t.Run("Unattributed device", func(t *testing.T) {
		tc.awaitGPUHoursUnattributedExcluded(t, rootCAs, host, viewer, monitorViewer, namespace, freeGPU)
	})

	// Tenant isolation: a foreign viewer sees no billing data for the tenant.
	query := fmt.Sprintf(`accelerator_gpu_hours_allocated{UUID=%q}`, gpu)
	status, response, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, foreignViewer, foreign, query)
	if err != nil || status != http.StatusOK || len(response.Data.Result) != 0 {
		t.Fatalf("foreign tenant can see GPU-hours sample: status=%d count=%d error=%v", status, len(response.Data.Result), err)
	}

	// Reassignment: the timestamp freshness guard must collapse the device
	// onto the successor workload once the original exporter is gone. The
	// rebinding keeps fixture failures on the subtest.
	t.Run("Device reassignment", func(t *testing.T) {
		tc := tc.WithT(t)
		tc.awaitGPUHoursReassignment(t, rootCAs, host, viewer, namespace, gpu, id, exporter)
	})

	// Release expiry: a released GPU — the exporter target stays alive and
	// only the workload labels disappear from its exposition — must stop
	// being billed within the collector exporter's stale-series expiry, not
	// the 5m default. Deleting the exporter (above) clears the target from
	// the scrape config; only a label flip exercises the exporter's
	// label-set cache. The rebinding keeps fixture failures on the subtest.
	t.Run("Device release expiry", func(t *testing.T) {
		tc := tc.WithT(t)
		tc.awaitGPUHoursReleaseExpiry(t, rootCAs, host, foreignViewer, monitorViewer, foreign, id)
	})
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

// dcgmFixtureExporter creates a synthetic DCGM exporter pod in the trusted
// GPU-operator namespace, matching the collector's DCGM discovery (pods named
// nvidia-dcgm-exporter* there, serving on port 9400). data is the exposition
// payload the pod serves verbatim. nodeName pins the pod to a worker; the
// freshness guard's device key includes the exporter pod's node, so fixtures
// that must compete for one device pass the same node here. ownerNamespace
// names the per-run fixture Namespace that owns the pod (see
// dcgmFixtureFlippingExporter).
func (tc *MonitoringTestCtx) dcgmFixtureExporter(t *testing.T, name, data, nodeName, ownerNamespace string) {
	t.Helper()
	tc.dcgmFixtureFlippingExporter(t, name, data, data, 0, nodeName, ownerNamespace)
}

// dcgmFixtureFlippingExporter creates a synthetic DCGM exporter pod that
// serves the attributed exposition for the first attributedWindow of its
// life and the released exposition after that, forever. Unlike deleting the
// exporter, the scrape target stays alive across the flip: only the collector
// exporter's stale-series expiry (metric_expiration) can stop the superseded
// label set from being served. A zero window serves released from the start.
// The pod lives in nvidia-gpu-operator, outside ownerNamespace, so it is
// owned by that per-run Namespace: a cluster-scoped owner is valid for a
// namespaced dependent, and namespace deletion garbage-collects the pod even
// when a crashed runner left its explicit t.Cleanup unexecuted — otherwise
// the orphan would keep serving synthetic billing data until cleaned up by
// hand. Normal teardown still uses the explicit deletion below.
func (tc *MonitoringTestCtx) dcgmFixtureFlippingExporter(t *testing.T, name, attributed, released string, attributedWindow time.Duration, nodeName, ownerNamespace string) {
	t.Helper()
	window := int(attributedWindow.Seconds())
	server := fmt.Sprintf(`import time
from http.server import BaseHTTPRequestHandler, HTTPServer
START = time.monotonic()
ATTRIBUTED = %q.encode()
RELEASED = %q.encode()
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  data = ATTRIBUTED if time.monotonic() - START < %d else RELEASED
  self.send_response(200)
  self.send_header('Content-Type', 'text/plain; version=0.0.4')
  self.send_header('Content-Length', str(len(data)))
  self.end_headers()
  self.wfile.write(data)
httpd = HTTPServer(('0.0.0.0', 9400), Handler)
httpd.serve_forever()
`, attributed, released, window)
	owner := &corev1.Namespace{}
	if err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: ownerNamespace}, owner); err != nil {
		t.Fatalf("reading the owner namespace %q: %v", ownerNamespace, err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       "nvidia-gpu-operator",
			Labels:          map[string]string{"app": "odh-dcgm-fixture"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Namespace", Name: owner.Name, UID: owner.UID}},
		},
		Spec: corev1.PodSpec{
			// The exporter only serves metrics; it needs no API credential.
			AutomountServiceAccountToken: new(false),
			SecurityContext:              fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name:            "fixture",
				Image:           "registry.access.redhat.com/ubi9/python-311:latest",
				Command:         []string{"python3", "-c", server},
				Ports:           []corev1.ContainerPort{{ContainerPort: 9400}},
				SecurityContext: fixtureContainerSecurityContext(),
			}},
		},
	}
	if nodeName != "" {
		// Pin via required node affinity rather than spec.nodeName: affinity
		// keeps the scheduler's taint and fit checks in the loop while still
		// placing the fixture on the same worker as the exporter it competes
		// with — the freshness guard's device key includes the node.
		pod.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchFields: []corev1.NodeSelectorRequirement{{
						Key:      "metadata.name",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{nodeName},
					}},
				}},
			},
		}}
	}
	tc.createFixtureResource(t, gvk.Pod, pod)
	t.Cleanup(func() {
		tc.DeleteResource(WithMinimalObject(gvk.Pod, types.NamespacedName{Name: name, Namespace: "nvidia-gpu-operator"}),
			WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))
	})
}

// awaitKueueRealAdmission proves the Kueue queue labels the Tier 2 join
// matches are produced by real Kueue admission, not only by hand-labeled
// fixture pods: a Deployment goes through the real quota chain (ResourceFlavor
// -> ClusterQueue -> LocalQueue -> admitted Workload) and the GPU attributed
// to its pod must be enriched with the queue names admission stamped on the
// pod. The Deployment integration is used because the Pod integration cannot
// work on OpenShift: unsuspending a standalone pod is a pod update, which SCC
// re-validation rejects, so an admitted pod stays gated forever. Kueue itself
// is a cluster prerequisite — the subtest skips when no Kueue instance with
// the Deployment integration is deployed.
func (tc *MonitoringTestCtx) awaitKueueRealAdmission(t *testing.T, rootCAs *x509.CertPool, host, id string) {
	t.Helper()

	// The OpenShift Kueue only manages the frameworks its instance CR
	// lists, and only namespaces labeled kueue.openshift.io/managed=true
	// (its webhook's namespaceSelector).
	instances := &unstructured.UnstructuredList{}
	instances.SetGroupVersionKind(gvk.Kueue.GroupVersion().WithKind("KueueList"))
	if err := tc.Client().List(tc.Context(), instances); err != nil {
		// Skip only on a confirmed absence — an undiscovered API or a
		// missing endpoint. Anything else (forbidden, timeout) must fail
		// the subtest, not silently skip past the admission check.
		if k8smeta.IsNoMatchError(err) || k8serr.IsNotFound(err) {
			t.Skipf("Kueue is not installed: %v", err)
		}
		t.Fatalf("listing Kueue instances: %v", err)
	}
	deploymentIntegration := false
	for _, instance := range instances.Items {
		frameworks, found, err := unstructured.NestedStringSlice(instance.Object, "spec", "config", "integrations", "frameworks")
		if err != nil {
			t.Fatalf("decoding Kueue integration frameworks: %v", err)
		}
		if found && slices.Contains(frameworks, "Deployment") {
			deploymentIntegration = true
		}
	}
	if !deploymentIntegration {
		t.Skipf("no Kueue instance enables the Deployment integration: %d instance(s)", len(instances.Items))
	}

	// A second fixture namespace keeps the tenant namespace's billing
	// assertions (by_namespace == 1) untouched, and the managed label opts
	// the namespace into Kueue.
	namespace := "tests-gpu-hours-kueue-" + id
	tc.createFixtureResource(t, gvk.Namespace, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:   namespace,
		Labels: map[string]string{"kueue.openshift.io/managed": "true"},
	}})
	t.Cleanup(func() {
		tc.DeleteResource(WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: namespace}),
			WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))
	})

	// The real quota chain: a plain flavor, a ClusterQueue with a slice of
	// CPU quota open to every namespace, and a LocalQueue in the fixture
	// namespace. The cluster-scoped objects are cleaned up explicitly; the
	// LocalQueue dies with its namespace.
	flavor := "flavor-e2e-" + id
	clusterQueue := "cq-e2e-real-" + id
	localQueue := "lq-e2e-real-" + id
	tc.createFixtureResource(t, gvk.ResourceFlavor, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kueue.x-k8s.io/v1beta2",
		"kind":       "ResourceFlavor",
		"metadata":   map[string]any{"name": flavor},
	}})
	t.Cleanup(func() {
		tc.DeleteResource(WithMinimalObject(gvk.ResourceFlavor, types.NamespacedName{Name: flavor}),
			WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))
	})
	tc.createFixtureResource(t, gvk.ClusterQueue, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kueue.x-k8s.io/v1beta2",
		"kind":       "ClusterQueue",
		"metadata":   map[string]any{"name": clusterQueue},
		"spec": map[string]any{
			"namespaceSelector": map[string]any{},
			"resourceGroups": []any{map[string]any{
				"coveredResources": []any{"cpu", "memory"},
				"flavors": []any{map[string]any{
					"name": flavor,
					"resources": []any{
						map[string]any{"name": "cpu", "nominalQuota": "1"},
						map[string]any{"name": "memory", "nominalQuota": "1Gi"},
					},
				}},
			}},
		},
	}})
	t.Cleanup(func() {
		tc.DeleteResource(WithMinimalObject(gvk.ClusterQueue, types.NamespacedName{Name: clusterQueue}),
			WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))
	})
	tc.createFixtureResource(t, gvk.LocalQueue, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kueue.x-k8s.io/v1beta2",
		"kind":       "LocalQueue",
		"metadata":   map[string]any{"name": localQueue, "namespace": namespace},
		"spec":       map[string]any{"clusterQueue": clusterQueue},
	}})

	// The workload: a Deployment labeled with the LocalQueue. Kueue admits
	// it against the ClusterQueue and stamps the queue names onto the pod —
	// the labels kube-state-metrics federates and the Tier 2 join matches.
	deploy := "kueue-workload-" + id
	tc.createFixtureResource(t, gvk.Deployment, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploy,
			Namespace: namespace,
			Labels:    map[string]string{"kueue.x-k8s.io/queue-name": localQueue},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": deploy}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": deploy}},
				Spec: corev1.PodSpec{
					// The workload only sleeps; it needs no API credential.
					AutomountServiceAccountToken: new(false),
					SecurityContext:              fixturePodSecurityContext(),
					Containers: []corev1.Container{{
						Name:            "workload",
						Image:           "registry.access.redhat.com/ubi9/python-311:latest",
						Command:         []string{"/bin/sh", "-c"},
						Args:            []string{"sleep 3600"},
						SecurityContext: fixtureContainerSecurityContext(),
						Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("128Mi"),
						}},
					}},
				},
			},
		},
	})
	t.Cleanup(func() {
		tc.DeleteResource(WithMinimalObject(gvk.Deployment, types.NamespacedName{Name: deploy, Namespace: namespace}),
			WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))
	})

	// Await real admission: the pod only runs ungated once its Workload is
	// admitted, and the queue labels only exist because admission stamped
	// them — a running pod carrying both is the provenance proof at the
	// Kubernetes layer, before the metrics layer is consulted.
	g := NewWithT(t)
	var podName string
	g.Eventually(func() error {
		pods := &corev1.PodList{}
		if err := tc.Client().List(tc.Context(), pods, client.InNamespace(namespace), client.MatchingLabels{"app": deploy}); err != nil {
			return err
		}
		for i := range pods.Items {
			pod := &pods.Items[i]
			if pod.Status.Phase != corev1.PodRunning {
				continue
			}
			if pod.Labels["kueue.x-k8s.io/cluster-queue-name"] != clusterQueue ||
				pod.Labels["kueue.x-k8s.io/local-queue-name"] != localQueue {
				return fmt.Errorf("running pod is missing the admission-stamped queue labels: %v", pod.Labels)
			}
			podName = pod.Name
			return nil
		}
		return fmt.Errorf("no running pod for the Deployment yet: %d pod(s)", len(pods.Items))
	}, 10*time.Minute, 15*time.Second).Should(Succeed())

	// Attribute a second device to the admitted pod. The exporter is created
	// only now because the pod name is generated, and the device UUID is
	// distinct from the main fixture's so the timestamp freshness guard is
	// not a factor here.
	kueueGPU := "GPU-kueue-e2e-" + id
	data := fmt.Sprintf("# TYPE DCGM_FI_DEV_GPU_UTIL gauge\n"+
		"DCGM_FI_DEV_GPU_UTIL{namespace=%q,pod=%q,modelName=\"E2E-GPU\",UUID=%q,gpu=\"0\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} 0\n",
		namespace, podName, kueueGPU)
	tc.dcgmFixtureExporter(t, "nvidia-dcgm-exporter-e2e-"+id+"-kueue", data, "", namespace)

	viewer := tc.metricsViewerToken(t, "gpu-hours-kueue-viewer-"+id, namespace)

	// Non-vacuous precondition: the raw series must be flowing before the
	// enrichment is asserted (raw scrapes are replica-independent, so this
	// does not depend on which replica the namespace proxy serves).
	tc.awaitPrometheusQuery(t, rootCAs, host, viewer, namespace,
		fmt.Sprintf(`DCGM_FI_DEV_GPU_UTIL{UUID=%q, odh_dcgm_source="verified"}`, kueueGPU),
		func(response prometheusQueryResponse) error {
			if len(response.Data.Result) != 1 {
				return fmt.Errorf("expected the Kueue workload's raw DCGM samples: results=%v", response.Data.Result)
			}
			return nil
		})

	// The metrics-layer proof: the admitted pod's queue labels reach the
	// recorded series through the KSM federation and the Tier 2 join. These
	// values exist only because real Kueue admission stamped them on the
	// pod — nothing in this fixture sets them by hand.
	query := fmt.Sprintf(`accelerator_gpu_hours_allocated_kueue{UUID=%q}`, kueueGPU)
	tc.awaitPrometheusQuery(t, rootCAs, host, viewer, namespace, query, func(response prometheusQueryResponse) error {
		if len(response.Data.Result) != 1 {
			return fmt.Errorf("expected one Kueue-enriched GPU sample: count=%d", len(response.Data.Result))
		}
		result := response.Data.Result[0]
		if result.Metric["label_kueue_x_k8s_io_cluster_queue_name"] != clusterQueue ||
			result.Metric["label_kueue_x_k8s_io_local_queue_name"] != localQueue {
			return fmt.Errorf("real-admission queue labels missing from GPU-hours attribution: %v", result.Metric)
		}
		return nil
	})
}

// createReservedNameForge creates a workload in the tenant namespace that
// exposes a reserved recording-rule name through the ordinary workload scrape
// path: accelerator_gpu_hours_allocated with value -1, mirroring the tenant's
// real GPU labels so a missing ingestion drop would poison the billing
// aggregate. A control probe series on the same endpoint lets the rejection
// test distinguish "the drop worked" from "the scrape never happened", and an
// unrelated accelerator_-prefixed series proves the drop is narrowed to the
// reserved names rather than the whole prefix. Returns the forge pod's name.
func (tc *MonitoringTestCtx) createReservedNameForge(t *testing.T, namespace, gpu, id string) string {
	t.Helper()
	forge := "reserved-name-forge-" + id
	data := fmt.Sprintf("# TYPE accelerator_gpu_hours_allocated gauge\n"+
		"accelerator_gpu_hours_allocated{modelName=\"E2E-GPU\",UUID=%q,gpu=\"0\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} -1\n"+
		"# TYPE accelerator_workload_custom gauge\n"+
		"accelerator_workload_custom{lane=\"unrelated\"} 7\n"+
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
// that seed reserved recording-rule names while still accepting unrelated
// accelerator_-prefixed workload metrics. The forge endpoint's control probe
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
	// Same endpoint, same scrape: an accelerator_-prefixed name that is not
	// reserved must survive the drop — the drop is narrowed to the exact
	// record names, not the whole prefix.
	query = fmt.Sprintf(`accelerator_workload_custom{pod=%q}`, forgePod)
	status, response, err = queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, viewer, namespace, query)
	if err != nil || status != http.StatusOK || len(response.Data.Result) != 1 || response.Data.Result[0].Value[1] != "7" {
		t.Fatalf("unrelated accelerator_-prefixed workload metric was dropped: status=%d results=%v error=%v", status, response.Data.Result, err)
	}
}

// awaitGPUHoursUnattributedExcluded proves a free device stays out of the
// billing shapes. DCGM exports hardware series for every physical GPU, free
// or not, but only workloads bound to a pod carry namespace/pod labels; the
// workload-label matchers in the Tier 1 rules must therefore exclude the
// free device from both the allocation and the utilization shapes. The free
// device's queries go through the monitor viewer in
// the monitoring namespace: an unattributed series carries the collector's
// Prometheus target labels (namespace = the monitoring namespace),
// invisible to the tenant viewer, so a tenant-viewer assertion would pass
// vacuously. The count_over_time preconditions prove the raw samples have
// flowed inside the rules' own 45s window — with 15s rule evaluations on a
// 45s presence window, a device the rules wrongly counted would already
// have records — so the empty exclusion result is the filter working, not a
// missing scrape.
func (tc *MonitoringTestCtx) awaitGPUHoursUnattributedExcluded(t *testing.T, rootCAs *x509.CertPool, host, viewer, monitorViewer, namespace, freeGPU string) {
	t.Helper()
	// The probe window matches the allocation rule's own 45s presence
	// window: a longer window could pass on samples the rule no longer sees,
	// making the empty exclusion result below prove only expiry. The
	// >= bool 1 form pushes the sample-count comparison into the query, so
	// the poll can assert on the returned 1 like every other value check
	// here (the JSON value element is an any).
	tc.awaitPrometheusQuery(t, rootCAs, host, monitorViewer, tc.MonitoringNamespace,
		fmt.Sprintf(`count_over_time(DCGM_FI_DEV_GPU_UTIL{UUID=%q, odh_dcgm_source="verified"}[45s]) >= bool 1`, freeGPU),
		func(response prometheusQueryResponse) error {
			if len(response.Data.Result) != 1 || response.Data.Result[0].Value[1] != "1" {
				return fmt.Errorf("expected a fresh raw sample from the free device: results=%v", response.Data.Result)
			}
			return nil
		})
	// The free device also reports occupancy, so the utilization rule
	// receives free-device input it must filter out — the precondition
	// keeps the utilized check below from passing vacuously.
	tc.awaitPrometheusQuery(t, rootCAs, host, monitorViewer, tc.MonitoringNamespace,
		fmt.Sprintf(`count_over_time(DCGM_FI_PROF_GR_ENGINE_ACTIVE{UUID=%q, odh_dcgm_source="verified"}[45s]) >= bool 1`, freeGPU),
		func(response prometheusQueryResponse) error {
			if len(response.Data.Result) != 1 || response.Data.Result[0].Value[1] != "1" {
				return fmt.Errorf("expected a fresh occupancy sample from the free device: results=%v", response.Data.Result)
			}
			return nil
		})
	// The raw samples are proven, so query directly: an Eventually on
	// absence would pass vacuously before the device is scraped at all.
	query := fmt.Sprintf(`accelerator_gpu_hours_allocated{UUID=%q}`, freeGPU)
	status, response, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, monitorViewer, tc.MonitoringNamespace, query)
	if err != nil || status != http.StatusOK || len(response.Data.Result) != 0 {
		t.Fatalf("free device was counted as allocated: status=%d count=%d error=%v", status, len(response.Data.Result), err)
	}
	query = fmt.Sprintf(`accelerator_gpu_hours_utilized{UUID=%q}`, freeGPU)
	status, response, err = queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, monitorViewer, tc.MonitoringNamespace, query)
	if err != nil || status != http.StatusOK || len(response.Data.Result) != 0 {
		t.Fatalf("free device was counted as utilized: status=%d count=%d error=%v", status, len(response.Data.Result), err)
	}
	// The exclusion must not take the attributed device with it: the
	// tenant's billing aggregate stays at exactly one GPU.
	status, response, err = queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, viewer, namespace, "accelerator_gpu_hours_allocated_by_namespace")
	if err != nil || status != http.StatusOK {
		t.Fatalf("query billing aggregate: status=%d error=%v", status, err)
	}
	if len(response.Data.Result) != 1 || response.Data.Result[0].Metric["namespace"] != namespace || response.Data.Result[0].Value[1] != "1" {
		t.Fatalf("billing aggregate lost the attributed device: results=%v", response.Data.Result)
	}
}

// awaitGPUHoursReassignment proves a device handoff through both scrape
// hops: a second fixture exporter attributes the same device to a successor
// workload, and once the original exporter is deleted the timestamp
// freshness guard must leave exactly the successor's assignment recorded.
func (tc *MonitoringTestCtx) awaitGPUHoursReassignment(t *testing.T, rootCAs *x509.CertPool, host, viewer, namespace, gpu, id, original string) {
	t.Helper()
	const operatorNamespace = "nvidia-gpu-operator"

	// The freshness guard's device key includes the exporter pod's node: the
	// trusted receiver sets node from the pod's node name, overriding the
	// exposition. If the two exporters landed on different workers their
	// device keys would differ, the guard would never compare them, and the
	// handoff assertion below would pass vacuously. Pin the successor to the
	// original's node so both speak for the same device.
	originalPod := &corev1.Pod{}
	if err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: original, Namespace: operatorNamespace}, originalPod); err != nil {
		t.Fatalf("reading the original exporter pod: %v", err)
	}
	if originalPod.Spec.NodeName == "" {
		t.Fatal("original exporter pod has no node assignment")
	}

	// The successor must match the collector's DCGM discovery: pods in the
	// GPU operator namespace named nvidia-dcgm-exporter*, port 9400.
	successor := "nvidia-dcgm-exporter-e2e-" + id + "-b"
	data := fmt.Sprintf("# TYPE DCGM_FI_DEV_GPU_UTIL gauge\n"+
		"DCGM_FI_DEV_GPU_UTIL{namespace=%q,pod=\"model2\",modelName=\"E2E-GPU\",UUID=%q,gpu=\"0\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} 0\n"+
		"# TYPE DCGM_FI_PROF_GR_ENGINE_ACTIVE gauge\n"+
		"DCGM_FI_PROF_GR_ENGINE_ACTIVE{namespace=%q,pod=\"model2\",modelName=\"E2E-GPU\",UUID=%q,gpu=\"0\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} 0.5\n",
		namespace, gpu, namespace, gpu)
	tc.dcgmFixtureExporter(t, successor, data, originalPod.Spec.NodeName, namespace)

	// Wait for the successor's raw samples before triggering the handoff,
	// so the post-handoff assertion cannot pass vacuously. Assert on the raw
	// series, not the record: raw scrapes are replica-independent (every
	// Prometheus replica scrapes every collector), while the recorded series
	// is not — while both exporters compete for the device, the freshness
	// guard's winner differs per replica (each replica scrapes the collectors
	// at its own offsets and evaluates on its own grid), and the namespace
	// proxy serves from a single replica. The successor's record is
	// therefore not guaranteed to be tenant-visible during contention, and
	// asserting it there would be flaky by design.
	tc.awaitPrometheusQuery(t, rootCAs, host, viewer, namespace,
		fmt.Sprintf(`DCGM_FI_DEV_GPU_UTIL{UUID=%q, pod="model2", odh_dcgm_source="verified"}`, gpu),
		func(response prometheusQueryResponse) error {
			if len(response.Data.Result) != 1 {
				return fmt.Errorf("expected the successor's raw DCGM samples: results=%v", response.Data.Result)
			}
			return nil
		})

	// The original exporter's own cleanup tolerates absence, so deleting it
	// here explicitly is safe.
	tc.DeleteResource(WithMinimalObject(gvk.Pod, types.NamespacedName{Name: original, Namespace: operatorNamespace}),
		WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))

	// After the handoff the device must be attributed to the successor only:
	// the successor is the device's only live source, so its samples are
	// strictly fresher on every replica and the guard drops the superseded
	// assignment's samples even while they are still inside the 45s presence
	// window. The rule manager's stale markers end the original record's
	// instant-query visibility, so exactly one assignment must remain.
	query := fmt.Sprintf(`accelerator_gpu_hours_allocated{UUID=%q}`, gpu)
	tc.awaitPrometheusQuery(t, rootCAs, host, viewer, namespace, query, func(response prometheusQueryResponse) error {
		if len(response.Data.Result) != 1 {
			return fmt.Errorf("expected exactly one assignment after the handoff: results=%v", response.Data.Result)
		}
		result := response.Data.Result[0]
		if result.Metric["namespace"] != namespace || result.Metric["pod"] != "model2" || result.Value[1] != "1" {
			return fmt.Errorf("expected the successor assignment after the handoff: %v %v", result.Metric, result.Value)
		}
		return nil
	})
}

// awaitGPUHoursReleaseExpiry proves the collector's prometheus exporter
// expires a released device's stale label set. A fixture exporter attributes
// a device to a workload for a fixed window, then keeps serving the same
// device with no workload labels — the target stays alive and scraped, so
// only metric_expiration can stop the superseded label set from being served
// (target removal, which the reassignment subtest covers, also clears it).
// The final poll therefore requires the released series itself to keep
// flowing while the attributed record is gone: without that, a dead fixture
// pod or a transient pre-flip gap in the attributed samples would satisfy
// the empty result without exercising the expiry at all. The attributed
// record must disappear well before the exporter's default 5m expiry would
// clear it: the assertion deadline is calibrated to fail the unfixed
// collector and pass the 90s expiry with margin on both sides.
func (tc *MonitoringTestCtx) awaitGPUHoursReleaseExpiry(t *testing.T, rootCAs *x509.CertPool, host, viewer, monitorViewer, namespace, id string) {
	t.Helper()

	// The attributed exposition is served for four minutes: long enough for
	// the attributed record to appear through both scrape hops and the
	// evaluation grid (well under two minutes), short enough to keep the
	// subtest bounded.
	const attributedWindow = 4 * time.Minute
	// From the exporter's creation: a 90s metric_expiration clears the record
	// by flip + 90s expiry + 30s scrape + 45s presence window + evaluation
	// grid ≈ creation + 7m; the 5m default clears it at flip + 5m + the same
	// tail ≈ creation + 10.5m. Eight and a half minutes splits them with
	// roughly a minute of margin on each side.
	const deadlineOffset = 8*time.Minute + 30*time.Second

	gpu := "GPU-flip-e2e-" + id
	exporter := "nvidia-dcgm-exporter-e2e-" + id + "-flip"
	attributed := fmt.Sprintf("# TYPE DCGM_FI_DEV_GPU_UTIL gauge\n"+
		"DCGM_FI_DEV_GPU_UTIL{namespace=%q,pod=\"flip\",modelName=\"E2E-GPU\",UUID=%q,gpu=\"0\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} 0\n",
		namespace, gpu)
	released := fmt.Sprintf("# TYPE DCGM_FI_DEV_GPU_UTIL gauge\n"+
		"DCGM_FI_DEV_GPU_UTIL{modelName=\"E2E-GPU\",UUID=%q,gpu=\"0\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} 0\n",
		gpu)
	created := time.Now()
	tc.dcgmFixtureFlippingExporter(t, exporter, attributed, released, attributedWindow, "", namespace)
	deadline := created.Add(deadlineOffset)

	// Non-vacuous precondition: the attributed raw series must flow before
	// the record is asserted (raw scrapes are replica-independent), so a
	// discovery or scrape failure fails here pointing at the pipeline, not
	// at the rule.
	tc.awaitPrometheusQuery(t, rootCAs, host, viewer, namespace,
		fmt.Sprintf(`count_over_time(DCGM_FI_DEV_GPU_UTIL{UUID=%q, odh_dcgm_source="verified"}[45s]) >= bool 1`, gpu),
		func(response prometheusQueryResponse) error {
			if len(response.Data.Result) != 1 || response.Data.Result[0].Value[1] != "1" {
				return fmt.Errorf("expected a fresh attributed raw sample from the flip fixture: results=%v", response.Data.Result)
			}
			return nil
		})

	// Non-vacuous precondition: the attributed record must exist before the
	// flip can make it disappear.
	query := fmt.Sprintf(`accelerator_gpu_hours_allocated{UUID=%q}`, gpu)
	tc.awaitPrometheusQuery(t, rootCAs, host, viewer, namespace, query, func(response prometheusQueryResponse) error {
		if len(response.Data.Result) != 1 {
			return fmt.Errorf("expected one allocated GPU sample before the release: count=%d", len(response.Data.Result))
		}
		result := response.Data.Result[0]
		if result.Metric["namespace"] != namespace || result.Metric["pod"] != "flip" || result.Value[1] != "1" {
			return fmt.Errorf("incorrect release-fixture attribution: %v %v", result.Metric, result.Value)
		}
		return nil
	})

	// After the flip the released device's stale label set must stop being
	// served, and with it the attributed record, before the deadline. The
	// record's absence only proves expiry while the released series keeps
	// flowing: the poll also asserts the fixture is still scraped, so a dead
	// fixture pod (whose target removal also clears the record) or a
	// transient pre-flip gap in the attributed samples cannot satisfy it.
	// The remaining time is measured to the absolute deadline so a slow
	// precondition cannot stretch the window past what the unfixed collector
	// would also satisfy.
	releasedQuery := fmt.Sprintf(`count_over_time(DCGM_FI_DEV_GPU_UTIL{UUID=%q, odh_dcgm_source="verified", odh_dcgm_workload_namespace=""}[45s]) >= bool 1`, gpu)
	g := NewWithT(t)
	g.Eventually(func() error {
		status, response, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, viewer, namespace, query)
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("query %q: unexpected HTTP status %d", query, status)
		}
		if len(response.Data.Result) != 0 {
			return fmt.Errorf("released device still billed %s after its workload labels disappeared (stale exporter label-set cache; metric_expiration not effective): results=%v",
				time.Until(deadline).Round(time.Second), response.Data.Result)
		}
		rstatus, rresponse, rerr := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, monitorViewer, tc.MonitoringNamespace, releasedQuery)
		if rerr != nil {
			return rerr
		}
		if rstatus != http.StatusOK {
			return fmt.Errorf("query %q: unexpected HTTP status %d", releasedQuery, rstatus)
		}
		if len(rresponse.Data.Result) != 1 || rresponse.Data.Result[0].Value[1] != "1" {
			return fmt.Errorf("released device's raw series is not flowing, so its record's absence does not prove expiry: results=%v", rresponse.Data.Result)
		}
		return nil
	}, time.Until(deadline), 15*time.Second).Should(Succeed())
}
