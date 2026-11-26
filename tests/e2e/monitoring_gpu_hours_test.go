package e2e_test

import (
	"bytes"
	"context"
	"crypto/x509"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	k8smeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcfg "sigs.k8s.io/controller-runtime/pkg/client/config"

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
	// A killed process or CI timeout skips t.Cleanup, so a previous run may
	// have left its fixtures behind; duplicates would fail this test's own
	// exactly-one preconditions (see sweepGPUHoursFixtureLeftovers).
	tc.sweepGPUHoursFixtureLeftovers(t)
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
	// A third device attributed to the monitoring namespace itself. The
	// unattributed-device subtest runs its exclusion queries through the
	// monitor viewer, whose every selector gets the monitoring namespace
	// enforced — the tenant device's records are invisible there, so this
	// device is the positive control proving a Tier 1 evaluation actually
	// ran on the answering replica (see awaitGPUHoursUnattributedExcluded).
	// It rides the same exporter scrape as the free device, so its record
	// also proves that evaluation consumed the free device's samples.
	monitorGPU := "GPU-monitor-e2e-" + id
	data := fmt.Sprintf("# TYPE DCGM_FI_DEV_GPU_UTIL gauge\n"+
		"DCGM_FI_DEV_GPU_UTIL{namespace=%q,pod=\"model\",modelName=\"E2E-GPU\",UUID=%q,gpu=\"0\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} 0\n"+
		"DCGM_FI_DEV_GPU_UTIL{modelName=\"E2E-FREE-GPU\",UUID=%q,gpu=\"1\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} 0\n"+
		"DCGM_FI_DEV_GPU_UTIL{namespace=%q,pod=\"monitor-control\",modelName=\"E2E-MONITOR-GPU\",UUID=%q,gpu=\"2\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} 0\n"+
		"# TYPE DCGM_FI_PROF_GR_ENGINE_ACTIVE gauge\n"+
		"DCGM_FI_PROF_GR_ENGINE_ACTIVE{namespace=%q,pod=\"model\",modelName=\"E2E-GPU\",UUID=%q,gpu=\"0\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} 0.5\n"+
		"DCGM_FI_PROF_GR_ENGINE_ACTIVE{modelName=\"E2E-FREE-GPU\",UUID=%q,gpu=\"1\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} 0.5\n"+
		"DCGM_FI_PROF_GR_ENGINE_ACTIVE{namespace=%q,pod=\"monitor-control\",modelName=\"E2E-MONITOR-GPU\",UUID=%q,gpu=\"2\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} 0.5\n",
		namespace, gpu, freeGPU, tc.MonitoringNamespace, monitorGPU,
		namespace, gpu, freeGPU, tc.MonitoringNamespace, monitorGPU)
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
		tc.awaitGPUHoursUnattributedExcluded(t, rootCAs, host, viewer, monitorViewer, namespace, freeGPU, monitorGPU)
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

// sweepGPUHoursFixtureLeftovers deletes fixture components an interrupted
// previous run of this test left behind: a killed process or CI timeout skips
// t.Cleanup, and duplicates would then fail this test's own preconditions —
// the scrape-health check demands exactly one fixture exporter and the
// by-namespace aggregation demands exactly one tenant. Every deletion below is
// name-keyed to components only this test creates (the exporter pod prefix is
// shared with the trusted-DCGM test, which runs serially in the same suite),
// so the sweep cannot touch anything outside the test fixtures. The e2e suite
// already assumes one run per cluster at a time: the tests share and reconfigure
// a single monitoring stack.
func (tc *MonitoringTestCtx) sweepGPUHoursFixtureLeftovers(t *testing.T) {
	t.Helper()

	// Fixture exporter pods outlive their run: they live in the trusted
	// GPU-operator namespace, not in the per-run fixture namespaces below.
	pods := &corev1.PodList{}
	if err := tc.Client().List(tc.Context(), pods, client.InNamespace("nvidia-gpu-operator")); err != nil {
		t.Fatalf("listing GPU-operator pods for the leftover sweep: %v", err)
	}
	for _, pod := range pods.Items {
		if !strings.HasPrefix(pod.Name, "nvidia-dcgm-exporter-e2e-") {
			continue
		}
		t.Logf("sweeping leftover fixture exporter pod %s/%s from an interrupted run", pod.Namespace, pod.Name)
		tc.DeleteResource(WithMinimalObject(gvk.Pod, types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}),
			WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))
	}

	// The per-run fixture namespaces: tenant, foreign-namespace, and Kueue.
	namespaces := &corev1.NamespaceList{}
	if err := tc.Client().List(tc.Context(), namespaces); err != nil {
		t.Fatalf("listing namespaces for the leftover sweep: %v", err)
	}
	for _, fixture := range namespaces.Items {
		if !strings.HasPrefix(fixture.Name, "tests-gpu-hours-") {
			continue
		}
		t.Logf("sweeping leftover fixture namespace %s from an interrupted run", fixture.Name)
		tc.DeleteResource(WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: fixture.Name}),
			WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))
	}

	// The real-admission subtest's cluster-scoped quota objects. Kueue may not
	// be installed at all, which is not an error here — there is nothing to
	// sweep.
	for _, kind := range []struct {
		list   schema.GroupVersionKind
		object schema.GroupVersionKind
		prefix string
	}{
		{gvk.ClusterQueue.GroupVersion().WithKind("ClusterQueueList"), gvk.ClusterQueue, "cq-e2e-real-"},
		{gvk.ResourceFlavor.GroupVersion().WithKind("ResourceFlavorList"), gvk.ResourceFlavor, "flavor-e2e-"},
	} {
		items := &unstructured.UnstructuredList{}
		items.SetGroupVersionKind(kind.list)
		if err := tc.Client().List(tc.Context(), items); err != nil {
			if k8smeta.IsNoMatchError(err) || k8serr.IsNotFound(err) {
				continue
			}
			t.Fatalf("listing %ss for the leftover sweep: %v", kind.object.Kind, err)
		}
		for _, item := range items.Items {
			if !strings.HasPrefix(item.GetName(), kind.prefix) {
				continue
			}
			t.Logf("sweeping leftover %s %s from an interrupted run", kind.object.Kind, item.GetName())
			tc.DeleteResource(WithMinimalObject(kind.object, types.NamespacedName{Name: item.GetName()}),
				WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))
		}
	}

	// Viewer ServiceAccounts all live in the monitoring namespace (see
	// metricsViewerToken), and so does the monitor viewer's RoleBinding; the
	// other RoleBindings die with their per-run namespaces above.
	accounts := &corev1.ServiceAccountList{}
	if err := tc.Client().List(tc.Context(), accounts, client.InNamespace(tc.MonitoringNamespace)); err != nil {
		t.Fatalf("listing viewer ServiceAccounts for the leftover sweep: %v", err)
	}
	for _, account := range accounts.Items {
		if !strings.HasPrefix(account.Name, "gpu-hours-") {
			continue
		}
		t.Logf("sweeping leftover viewer ServiceAccount %s/%s from an interrupted run", account.Namespace, account.Name)
		tc.DeleteResource(WithMinimalObject(gvk.ServiceAccount, types.NamespacedName{Name: account.Name, Namespace: account.Namespace}),
			WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))
	}
	bindings := &rbacv1.RoleBindingList{}
	if err := tc.Client().List(tc.Context(), bindings, client.InNamespace(tc.MonitoringNamespace)); err != nil {
		t.Fatalf("listing viewer RoleBindings for the leftover sweep: %v", err)
	}
	for _, binding := range bindings.Items {
		if !strings.HasPrefix(binding.Name, "gpu-hours-") {
			continue
		}
		t.Logf("sweeping leftover viewer RoleBinding %s/%s from an interrupted run", binding.Namespace, binding.Name)
		tc.DeleteResource(WithMinimalObject(gvk.RoleBinding, types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}),
			WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))
	}
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
		if len(response.Data.Result) != 1 {
			return fmt.Errorf("expected exactly one fixture-exporter scrape: leftover fixture pods from an "+
				"interrupted run duplicate the series: count=%d results=%v", len(response.Data.Result), response.Data.Result)
		}
		if response.Data.Result[0].Value[1] != "1" {
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
	// Passing data as both expositions makes the flip marker a no-op: the
	// pod serves data whether or not it was ever flipped, so static fixtures
	// never need the trigger.
	tc.dcgmFixtureFlippingExporter(t, name, data, data, nodeName, ownerNamespace)
}

// dcgmFixtureFlippingExporter creates a synthetic DCGM exporter pod that
// serves the attributed exposition until flipDCGMFixtureExporter marks it
// released, and the released exposition after that, forever. The flip is
// triggered by the test rather than by a timer so it cannot precede the
// caller's preconditions: a time-based window could flip while the
// preconditions are still polling, and on a collector with the default
// expiry the record's stale cache can keep a post-flip precondition
// successful, anchoring any later timing measurement too late. Unlike
// deleting the exporter, the scrape target stays alive across the flip:
// only the collector exporter's stale-series expiry (metric_expiration)
// can stop the superseded label set from being served. Either payload may
// embed an @SERVED_AT@ token: the server replaces it with the request's own
// wall-clock seconds (the pod's clock, not the runner's), so a payload value
// that keeps changing through the monitoring stack proves the fixture
// itself is still being scraped — a cached exporter re-serve would repeat
// one frozen value — and the release-expiry subtest measures the gap
// between the last attributed request's stamp and the first released
// request's stamp in that one clock, where query retries, poll grids, and
// exec-return delay cannot stretch it.
// The pod lives in nvidia-gpu-operator, outside ownerNamespace, so it is
// owned by that per-run Namespace: a cluster-scoped owner is valid for a
// namespaced dependent, and namespace deletion garbage-collects the pod even
// when a crashed runner left its explicit t.Cleanup unexecuted — otherwise
// the orphan would keep serving synthetic billing data until cleaned up by
// hand. Normal teardown still uses the explicit deletion below.
func (tc *MonitoringTestCtx) dcgmFixtureFlippingExporter(t *testing.T, name, attributed, released string, nodeName, ownerNamespace string) {
	t.Helper()
	server := fmt.Sprintf(`import os, time
from http.server import BaseHTTPRequestHandler, HTTPServer
ATTRIBUTED = %q.encode()
RELEASED = %q.encode()
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  data = (RELEASED if os.path.exists('/tmp/flip') else ATTRIBUTED).replace(b'@SERVED_AT@', str(int(time.time())).encode())
  self.send_response(200)
  self.send_header('Content-Type', 'text/plain; version=0.0.4')
  self.send_header('Content-Length', str(len(data)))
  self.end_headers()
  self.wfile.write(data)
httpd = HTTPServer(('0.0.0.0', 9400), Handler)
httpd.serve_forever()
`, attributed, released)
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

// flipDCGMFixtureExporter releases the device a flipping fixture attributes:
// it execs into the exporter pod and creates the marker file the fixture's
// Python server checks on every scrape, so the next collector scrape fetches
// the released exposition. Exec (rather than, say, a ConfigMap the pod
// watches) keeps the trigger synchronous — no propagation delay sits between
// this call and the flip for a downstream timing budget to absorb.
func (tc *MonitoringTestCtx) flipDCGMFixtureExporter(t *testing.T, name string) {
	t.Helper()
	tc = tc.WithT(t)

	cfg, err := ctrlcfg.GetConfig()
	if err != nil {
		t.Fatalf("loading kubeconfig to trigger the fixture flip: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("creating the Kubernetes client to trigger the fixture flip: %v", err)
	}
	request := clientset.CoreV1().RESTClient().Post().
		Resource("pods").Name(name).
		Namespace("nvidia-gpu-operator").SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "fixture",
			Command:   []string{"python3", "-c", "open('/tmp/flip', 'w').close()"},
			Stdout:    true,
			Stderr:    true,
		}, clientgoscheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(cfg, http.MethodPost, request.URL())
	if err != nil {
		t.Fatalf("creating the pod exec stream to trigger the fixture flip: %v", err)
	}
	ctx, cancel := context.WithTimeout(tc.Context(), 30*time.Second)
	defer cancel()
	var stdout, stderr bytes.Buffer
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	}); err != nil {
		t.Fatalf("triggering the fixture flip in pod %s: %v (stderr: %q)", name, err, stderr.String())
	}
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

// freeDeviceExcludedError accepts an exclusion response only when the same
// response carries the control device's record. The control device is
// attributed to the monitoring namespace, so its record — unlike the tenant
// device's — survives the namespace proxy's enforced matcher and is visible
// to the monitor viewer the exclusion queries run through, and it is
// produced by the very rule under test: the control record can only exist
// if a Tier 1 evaluation succeeded on the answering replica, and since both
// devices arrive in every scrape of the shared fixture exporter, that
// evaluation necessarily read the free device's samples too. The free
// device's absence from the same response is then the workload-label filter
// working — not a missing scrape, an unevaluated replica, or a failed rule
// evaluation. A counted free device would appear here as well: its record
// carries the monitoring namespace (the collector's target labels).
func freeDeviceExcludedError(response prometheusQueryResponse, freeGPU, controlGPU, shape string) error {
	control := false
	for _, result := range response.Data.Result {
		switch result.Metric["UUID"] {
		case freeGPU:
			return fmt.Errorf("free device was counted as %s: results=%v", shape, response.Data.Result)
		case controlGPU:
			control = true
		}
	}
	if !control {
		return fmt.Errorf("the monitoring-namespace control device has no %s record on this replica yet, so no "+
			"rule evaluation that read the free device's samples has been observed: results=%v",
			shape, response.Data.Result)
	}
	return nil
}

// awaitGPUHoursUnattributedExcluded proves a free device stays out of the
// billing shapes. DCGM exports hardware series for every physical GPU, free
// or not, but only workloads bound to a pod carry namespace/pod labels; the
// workload-label matchers in the Tier 1 rules must therefore exclude the
// free device from both the allocation and the utilization shapes. The free
// device's queries go through the monitor viewer in the monitoring
// namespace: an unattributed series carries the collector's Prometheus
// target labels (namespace = the monitoring namespace), invisible to the
// tenant viewer, so a tenant-viewer assertion would pass vacuously — and
// conversely, the namespace proxy's enforced matcher hides the tenant's
// records from the monitor viewer, so the tenant device's record cannot
// serve as the positive control. The control device fills that gap: it is
// attributed to the monitoring namespace itself, so its records are visible
// to the monitor viewer, and it rides every scrape of the same fixture
// exporter as the free device. Each exclusion query therefore fetches both
// devices' records in ONE response, and the response is accepted only when
// the control device's record is present and the free device's is not: the
// control record proves the answering replica ingested the shared scrape
// and ran the very rule under test over it — a successful evaluation that
// consumed the free device's samples — so the free device's absence from
// that same response is the filter working. The poll retries until a
// replica answers with the control record, which also covers the
// load-balanced two-replica case.
func (tc *MonitoringTestCtx) awaitGPUHoursUnattributedExcluded(t *testing.T, rootCAs *x509.CertPool, host, viewer, monitorViewer, namespace, freeGPU, monitorGPU string) {
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
	// The exclusion queries must carry their own evaluation evidence rather
	// than lean on the preconditions above: queries are load-balanced across
	// the Prometheus replicas, and raw-sample presence shows ingestion, not
	// that a rule evaluation ran. Each query fetches the free device's and
	// the control device's records together, and the response is accepted
	// only when the control record is present and the free device's is not
	// (see freeDeviceExcludedError). The poll retries until a replica
	// answers with the control record, covering the two-replica case.
	tc.awaitPrometheusQuery(t, rootCAs, host, monitorViewer, tc.MonitoringNamespace,
		fmt.Sprintf(`accelerator_gpu_hours_allocated{UUID=~%q}`, freeGPU+"|"+monitorGPU),
		func(response prometheusQueryResponse) error {
			return freeDeviceExcludedError(response, freeGPU, monitorGPU, "allocated")
		})
	tc.awaitPrometheusQuery(t, rootCAs, host, monitorViewer, tc.MonitoringNamespace,
		fmt.Sprintf(`accelerator_gpu_hours_utilized{UUID=~%q}`, freeGPU+"|"+monitorGPU),
		func(response prometheusQueryResponse) error {
			return freeDeviceExcludedError(response, freeGPU, monitorGPU, "utilized")
		})
	// The exclusion must not take the attributed device with it: the
	// tenant's billing aggregate stays at exactly one GPU.
	status, response, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, viewer, namespace, "accelerator_gpu_hours_allocated_by_namespace")
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

// awaitLastAttributedStamp reads the flipping fixture's last attributed
// request stamp: the fixture stamps every request with its own wall clock
// as the sample value, and the collector exporter's cache holds exactly
// the last received sample, so the latest attributed value is the epoch of
// the last attributed scrape — where the exporter's 90s/300s expiry clock
// starts and the final poll's deadline is anchored. The read starts 40s
// after the trigger, not immediately: the last attributed request's value
// only lands in the monitoring Prometheus up to one 30s scrape hop past
// the trigger, and an earlier poll could return the previous request's
// stamp from the trailing window and anchor the deadline one scrape
// early. By trigger+40s every ingestion in the window provably carries
// the last request's stamp, and the fixed 90s expiry still serves the
// label set for ~105s past the trigger, so the read window is safely
// inside the readability window. An empty read past the trigger means the
// cache already expired the attributed label set — a scrape pause ran out
// the expiry clock before the measurement could start.
func (tc *MonitoringTestCtx) awaitLastAttributedStamp(t *testing.T, rootCAs *x509.CertPool, host, token, namespace, gpu string, triggered time.Time) float64 {
	t.Helper()

	query := fmt.Sprintf(`max_over_time(DCGM_FI_DEV_GPU_UTIL{UUID=%q, odh_dcgm_source="verified"}[45s])`, gpu)
	readableFrom := triggered.Add(40 * time.Second)
	g := NewWithT(t)
	var stamp float64
	g.Eventually(func() error {
		if delay := time.Until(readableFrom); delay > 0 {
			return fmt.Errorf("waiting %s for the last attributed request's stamp to be ingested", delay.Round(time.Second))
		}
		status, response, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, token, namespace, query)
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("query %q: unexpected HTTP status %d", query, status)
		}
		if len(response.Data.Result) != 1 {
			return fmt.Errorf("expected one attributed raw series from the flip fixture — if it stays empty, the collector's exporter "+
				"already dropped the attributed label set and the last attributed scrape cannot be bounded: results=%v", response.Data.Result)
		}
		valueText, ok := response.Data.Result[0].Value[1].(string)
		if !ok {
			return fmt.Errorf("unexpected attributed stamp sample shape: %v", response.Data.Result[0].Value)
		}
		value, perr := strconv.ParseFloat(valueText, 64)
		if perr != nil {
			return fmt.Errorf("parsing the attributed device's request stamp %q: %w", valueText, perr)
		}
		// The collector's transform divides this metric by 100; restore the
		// fixture's seconds so stamp differences are wall-clock durations.
		stamp = value * 100
		return nil
	}, 100*time.Second, 5*time.Second).Should(Succeed())
	return stamp
}

// awaitReleasedSeriesFlowing detects the release: the released raw series
// appears once the flip marker takes effect on a collector scrape, so its
// first observation is the release event plus a bounded detection delay
// (one collector scrape hop, one Prometheus scrape hop, and the 15s poll
// grid). The trailing window is 2m so transient query retries cannot age
// the first released sample out of it before the poll captures it, and
// the released label set has no older samples for the wider window to
// pick up. A path that died before the marker never produces a released
// series, so this poll fails the subtest outright. Detection anchors
// nothing — the expiry deadline is measured from the last attributed
// request's stamp — so retries here only delay observations.
func (tc *MonitoringTestCtx) awaitReleasedSeriesFlowing(t *testing.T, rootCAs *x509.CertPool, host, token, gpu string) {
	t.Helper()

	query := fmt.Sprintf(`count_over_time(DCGM_FI_DEV_GPU_UTIL{UUID=%q, odh_dcgm_source="verified", odh_dcgm_workload_namespace=""}[2m]) >= bool 1`, gpu)
	g := NewWithT(t)
	g.Eventually(func() error {
		status, response, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, token, tc.MonitoringNamespace, query)
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("query %q: unexpected HTTP status %d", query, status)
		}
		if len(response.Data.Result) != 1 || response.Data.Result[0].Value[1] != "1" {
			return fmt.Errorf("the released raw series is not flowing yet, so the fixture has not flipped (or stopped being scraped): results=%v", response.Data.Result)
		}
		return nil
	}, 2*time.Minute, 15*time.Second).Should(Succeed())
}

// awaitReleasedRecordCleared polls until the released device's attributed
// record is gone from the tenant view while the released series is provably
// still being scraped, or fails once the fixture's request stamps pass the
// expiry deadline. The record's absence only proves expiry while the
// fixture is still being scraped, so the poll also requires the released
// series' request stamp to advance across its own polls — which neither a
// sample-timestamp query nor a single trailing range can prove: the
// monitoring Prometheus stamps every sample when it scrapes the collector's
// exporter, and that exporter re-serves its cached label sets with fresh
// timestamps for up to the 90s metric_expiration after the fixture stops
// being scraped, while a stopped scrape path still leaves the stamp's last
// varying values inside a trailing range for up to the range's length —
// and the record can clear within that overlap, since rule staleness
// drops it roughly 45-60s after ingestion stops. The poll therefore reads
// the released exposition's request stamp (the fixture stamps every
// request with its own clock, so it only changes through real scrapes of
// the live fixture) per collector view, and succeeds only when the same
// view's stamp is strictly greater than the one captured at an earlier
// poll with the record absent: fresh ingestion during a sustained
// absence. Capturing baselines only during the absence is what makes
// that hold — a baseline taken while the record is still billed would
// let an advance pair two requests that both predate the clearing.
// Nor can the baseline captured at the first absent poll itself be a
// stale pre-clearing value: the monitoring scrape that evidenced the
// absence sits inside the same 45s stamp window (the 15s rule-eval
// grid plus the 15s poll grid sum to less than it), and it carries
// the exporter cache's value — which, by the 10s collector scrape
// timeout, already includes every request stamped before the
// deadline, since a pre-deadline request is received within 10s
// while the absence evidence postdates the drop by at least one
// scrape. The first absent poll's baseline is therefore at least the
// last pre-deadline request's stamp, so any later advance is a
// post-deadline request — and the trip fails it.
// A frozen path — dead fixture, removed target, broken hop —
// repeats one stamp across polls and fails. The 45s stamp window keeps
// only recently ingested views, and the view key is the full label set,
// so a stale view frozen by a TargetAllocator failover cannot stand in
// for the advancing one.
// The deadline is the last attributed request's stamp plus the expiry
// budget — both in the fixture's clock, the same clock the exporter's
// expiry runs on — and it is enforced inside the poll, on the observed
// request stamps, with precedence over the success condition: an
// iteration that observes both a cleared record and post-deadline stamps
// fails rather than passes. This is a plain loop rather than a Gomega
// Eventually because the framework accepts a callback that started before
// its timeout but returned success after it (each query here has a 30s
// HTTP timeout), and the deadline is data — the request stamps — not the
// runner's wall clock, so enforcing it on the observations rather than on
// the polling schedule is what makes a late success impossible. The
// runner backstop only ends a hung poll; the verdict never depends on it.
func (tc *MonitoringTestCtx) awaitReleasedRecordCleared(t *testing.T, rootCAs *x509.CertPool, host, viewer, monitorViewer,
	namespace, query, stampQuery string, anchorStamp, deadlineStamp float64, backstop time.Time,
) {
	t.Helper()

	stamps := map[string]float64{}
	var lastErr error
	for time.Now().Before(backstop) {
		status, response, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, viewer, namespace, query)
		if err != nil {
			lastErr = err
			time.Sleep(15 * time.Second)
			continue
		}
		if status != http.StatusOK {
			lastErr = fmt.Errorf("query %q: unexpected HTTP status %d", query, status)
			time.Sleep(15 * time.Second)
			continue
		}
		present := len(response.Data.Result) != 0
		if present {
			// The absence must be sustained across the two stamp
			// observations, so a reappearing record discards them.
			stamps = map[string]float64{}
		}
		rstatus, rresponse, rerr := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, monitorViewer, tc.MonitoringNamespace, stampQuery)
		if rerr != nil {
			lastErr = rerr
			time.Sleep(15 * time.Second)
			continue
		}
		if rstatus != http.StatusOK {
			lastErr = fmt.Errorf("query %q: unexpected HTTP status %d", stampQuery, rstatus)
			time.Sleep(15 * time.Second)
			continue
		}
		maxStamp, advancing, serr := releasedStampPoll(rresponse, stamps, !present)
		if serr != nil {
			lastErr = serr
			time.Sleep(15 * time.Second)
			continue
		}
		// The deadline is enforced here, before the success conditions
		// below: an iteration whose stamps already passed it fails the
		// subtest even if the record looks cleared, so no success can
		// land past the deadline however late this iteration's queries
		// returned.
		if maxStamp > deadlineStamp {
			state := "the attributed record is still billed"
			if !present {
				state = "the attributed record disappeared only now"
			}
			t.Fatalf("the released device's requests passed the expiry deadline %.0fs past the last attributed request (stamp %.0f) — "+
				"the latest observed request stamp is %.0f and %s: the 90s metric_expiration clears the record by ~2m25s past that request, "+
				"so a clearing this late means the exporter's stale label-set cache is not being expired within the budget",
				deadlineStamp-anchorStamp, anchorStamp, maxStamp, state)
		}
		switch {
		case present:
			lastErr = fmt.Errorf("released device still billed after its workload labels disappeared (stale exporter label-set cache; metric_expiration not effective): results=%v",
				response.Data.Result)
		case advancing:
			// A sustained absence with fresh ingestion throughout: the
			// expiry cleared the record.
			return
		default:
			lastErr = fmt.Errorf("released device's request stamp has not advanced across polls while the allocation is absent, so a dead fixture or a "+
				"removed target could explain the record's absence without expiry: results=%v", rresponse.Data.Result)
		}
		time.Sleep(15 * time.Second)
	}
	t.Fatalf("the released device's record did not clear before the runner backstop at %s: %v", backstop.Format(time.RFC3339), lastErr)
}

// releasedStampPoll evaluates one observation of the released series'
// request stamps: the maximum stamp across collector views, and whether any
// view's stamp advanced past the baseline this map captured at an earlier
// poll. Stamps are returned in the fixture's seconds — the collector's
// transform divides the metric by 100, restored here — matching the unit
// of the deadline the trip check compares the maximum against.
// Baselines are captured only while the attributed record is absent
// (the caller's capture flag, and it resets the map whenever the record is
// present), so an advance proves a request served after the absence was
// first observed — never two requests that both predate the clearing, which
// a baseline captured during presence would allow.
func releasedStampPoll(response prometheusQueryResponse, stamps map[string]float64, capture bool) (float64, bool, error) {
	maxStamp := -1.0
	advancing := false
	for _, result := range response.Data.Result {
		valueText, ok := result.Value[1].(string)
		if !ok {
			return 0, false, fmt.Errorf("unexpected released stamp sample shape: %v", result.Value)
		}
		value, perr := strconv.ParseFloat(valueText, 64)
		if perr != nil {
			return 0, false, fmt.Errorf("parsing the released device's request stamp %q: %w", valueText, perr)
		}
		// The collector's transform divides this metric by 100; restore the
		// fixture's seconds so the trip comparison against the deadline —
		// anchored at the last attributed stamp, in seconds — and the
		// baseline arithmetic share one unit.
		value *= 100
		if value > maxStamp {
			maxStamp = value
		}
		view := fmt.Sprint(result.Metric)
		baseline, seen := stamps[view]
		if !seen {
			if capture {
				stamps[view] = value
			}
			continue
		}
		if value > baseline {
			advancing = true
		}
	}
	return maxStamp, advancing, nil
}

// awaitGPUHoursReleaseExpiry proves the collector's prometheus exporter
// expires a released device's stale label set. A fixture exporter attributes
// a device to a workload until the test triggers the release, then keeps
// serving the same device with no workload labels — the target stays alive
// and scraped, so only metric_expiration can stop the superseded label set
// from being served (target removal, which the reassignment subtest covers,
// also clears it). The release is triggered only after the attributed
// preconditions have passed: a time-based window could flip while the
// preconditions are still polling, and on the unfixed collector the record's
// 5m stale cache can keep a post-flip precondition successful — the
// detection below would then start (and the budget with it) too late for the
// default-expiry regression to fail inside it.
// The final poll therefore requires the released series' request stamp to
// advance across two of its own polls while the attributed record stays
// absent — the fixture stamps every request with its own clock as the
// released sample's value, and that value only changes through real
// scrapes of the live fixture, unlike a sample timestamp, which the
// monitoring Prometheus re-stamps on every scrape of the collector's
// exporter even while that exporter re-serves a cached label set.
// Comparing point-in-time stamps across polls, rather than values inside
// one trailing range, also rules out the stale varying values a stopped
// scrape path leaves inside such a range for up to its length. The
// fixture pod's UID and restart counts are asserted unchanged across the
// window as well. A collector restart would also
// clear the stale label set —
// its exporter registry starts empty and re-scraping the released fixture
// cannot refill the superseded labels — so the collector pods' identities
// and restart counts are captured before the attributed preconditions and
// asserted unchanged after the record is gone: the record's presence lags
// the collector's registry by up to the 45s presence window, so a snapshot
// taken only once the record is confirmed present cannot prove the
// registry still holds the label set. The expiry deadline is
// anchored at the last attributed request's stamp — the fixture
// stamps every request with its own wall clock as the sample value,
// and the collector exporter's cache holds exactly the last received
// sample, so right after the trigger the test reads the epoch of the
// last attributed scrape. The exporter's 90s/300s expiry clocks run
// from that same request, so the deadline and the expiry live in one
// clock with no translation left to bound: a scrape pause before the
// trigger, a late exec return, query retries, and poll-grid delays
// can shift when the test observes the race, but not when either
// side of it happens. The final poll fails as soon as it observes a
// released request stamped past the last attributed stamp plus the
// budget below — an offset the 5m default cannot reach, because its
// exporter serves the superseded label set until 5m past that
// request (the monitoring Prometheus re-stamps the cached serve
// fresh on every scrape, so no stale marker can exist on it before
// the drop), while the 90s expiry has the record observed gone by
// ~3m35s past it worst case.
func (tc *MonitoringTestCtx) awaitGPUHoursReleaseExpiry(t *testing.T, rootCAs *x509.CertPool, host, viewer, monitorViewer, namespace, id string) {
	t.Helper()

	// The deadline the final poll enforces, as an offset from the last
	// attributed request's stamp — the fixture's clock, the same clock the
	// exporter's expiry runs on. With the 90s metric_expiration the record
	// is gone by ~2m25s past that request (an in-flight attributed scrape
	// within the 10s scrape timeout + 90s expiry + 30s scrape hop + 15s
	// evaluation grid — the stale marker the monitoring Prometheus writes
	// when the exporter stops serving the label set ends the rule's range
	// immediately, matching the clearing times observed in the green
	// runs), and the final poll then needs up to ~70s more to observe the
	// absence and the released stamp advance, so ~3m35s worst case. The
	// 5m default keeps serving the superseded label set until 5m past the
	// last attributed request — 30s past this deadline — and the
	// monitoring Prometheus re-stamps the cached serve fresh on every
	// scrape, so its record cannot be observed absent before the drop. A
	// four-and-a-half-minute offset splits them: ~55s of margin past the
	// fixed path's worst-case observation, and a deadline the unfixed
	// path cannot reach because its exporter is still serving the stale
	// label set when the poll trips.
	const expiryDeadline = 4*time.Minute + 30*time.Second

	gpu := "GPU-flip-e2e-" + id
	exporter := "nvidia-dcgm-exporter-e2e-" + id + "-flip"
	// The attributed exposition stamps each request with the fixture's own
	// wall clock as the sample value: the last attributed request's stamp,
	// read right after the trigger from the exporter's cache, anchors the
	// expiry deadline below in the same clock the exporter's expiry runs
	// on. The allocation rule counts presence, not values, and the
	// freshness guard reads timestamps, so the changing value is inert to
	// the rule.
	attributed := fmt.Sprintf("# TYPE DCGM_FI_DEV_GPU_UTIL gauge\n"+
		"DCGM_FI_DEV_GPU_UTIL{namespace=%q,pod=\"flip\",modelName=\"E2E-GPU\",UUID=%q,gpu=\"0\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} @SERVED_AT@\n",
		namespace, gpu)
	// The released exposition stamps each request with the fixture's own
	// wall clock as the sample value: the value only changes when the live
	// fixture serves a new request to a real collector scrape, so its
	// continuing variation through the monitoring stack is scrape-liveness
	// evidence that a sample timestamp cannot provide (see the final poll
	// below), and it is the same clock the deadline is anchored in. The
	// collector's transform rescales this metric, but a linear rescale
	// preserves both uses.
	released := fmt.Sprintf("# TYPE DCGM_FI_DEV_GPU_UTIL gauge\n"+
		"DCGM_FI_DEV_GPU_UTIL{modelName=\"E2E-GPU\",UUID=%q,gpu=\"0\",Hostname=\"gpu-node-e2e\",node=\"gpu-node-e2e\"} @SERVED_AT@\n",
		gpu)
	tc.dcgmFixtureFlippingExporter(t, exporter, attributed, released, "", namespace)

	// The expiry verdict below is only sound if no collector restarts between
	// this snapshot and the final assertion: a restarted collector's exporter
	// registry starts empty and re-scraping the released fixture cannot
	// refill the superseded label set, so a restart clears the record without
	// metric_expiration. The snapshot precedes the preconditions because the
	// record's presence lags the registry by up to the 45s presence window
	// plus the scrape and evaluation hops — capturing the identities only
	// once the record is confirmed present cannot prove the registry still
	// held the label set at that moment. Any restart in the window fails the
	// subtest, including one the still-attributed fixture would have let the
	// collector recover from: the measurement is confounded either way.
	collectorBaseline := tc.collectorPodIdentities(t)

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

	// The fixture pod's own identity is captured only now, after the
	// preconditions: the pod is provably running and scraped at this point,
	// so its container status is posted and the restart-count baseline
	// cannot silently skip the fixture container the way a snapshot taken
	// right after Pod creation can. A restart before this snapshot cannot
	// confound the verdict — the flip marker is only created by the trigger
	// below, so until then a restarted pod simply re-serves the attributed
	// exposition the preconditions already accepted — while a restart after
	// it resets the Python server and the marker with it, making the pod
	// re-serve the attributed exposition and confounding the expiry verdict.
	fixtureBaseline := tc.fixturePodIdentity(t, exporter)

	// Trigger the release only now that the preconditions hold — the record
	// is proven present while the device is still attributed, so its later
	// disappearance can only be the expiry. The exec's return time anchors
	// nothing the verdict depends on: the deadline below is anchored at the
	// last attributed request's stamp, in the fixture's clock, so a late
	// exec return can only delay observations, never the race itself.
	// `triggered` only bounds the detection poll and the final poll's
	// runner backstop.
	tc.flipDCGMFixtureExporter(t, exporter)
	triggered := time.Now()

	// Read the last attributed request's stamp, before the fixed 90s
	// expiry can drop the attributed label set: the exporter's cache holds
	// exactly the last received sample, so this is the epoch of the last
	// attributed scrape — where both the unfixed collector's 300s expiry
	// clock starts and the deadline below is anchored. The read itself
	// waits out the ingestion hop (see the helper) and still completes
	// ~105s inside the fixed path's readability window. An empty read
	// means the cache already expired the label set (a scrape pause ran
	// out the expiry clock before the trigger), which fails the subtest
	// instead of feeding the measurement.
	lastAttributedStamp := tc.awaitLastAttributedStamp(t, rootCAs, host, viewer, namespace, gpu, triggered)

	// Detect the release: the released raw series can only appear once the
	// fixture stops serving workload labels, so its first observation is
	// the release event plus a bounded detection delay — one collector
	// scrape hop, one Prometheus scrape hop, and the 15s poll grid, under
	// ~90s in total. Two minutes bounds it with retry slack. Detection
	// only confirms the flip took effect — it anchors nothing, so
	// transient query failures that delay it cannot stretch the deadline;
	// they only shrink the final poll's share of the window before the
	// deadline stamp, and this bound still leaves it over a minute.
	tc.awaitReleasedSeriesFlowing(t, rootCAs, host, monitorViewer, gpu)

	// After the flip the released device's stale label set must stop being
	// served, and with it the attributed record, before the fixture's
	// request stamps pass the deadline anchored at the last attributed
	// request; the record's absence only counts while the fixture is
	// provably still being scraped, which the stamp-advance check inside
	// the poll enforces. The runner backstop only guards against a hung
	// pipeline — the verdict itself is enforced on the request stamps.
	stampQuery := fmt.Sprintf(
		`max_over_time(DCGM_FI_DEV_GPU_UTIL{UUID=%q, odh_dcgm_source="verified", odh_dcgm_workload_namespace=""}[45s])`, gpu)
	tc.awaitReleasedRecordCleared(t, rootCAs, host, viewer, monitorViewer, namespace, query, stampQuery,
		lastAttributedStamp, lastAttributedStamp+expiryDeadline.Seconds(), triggered.Add(6*time.Minute))

	// The empty result proved expiry only if no collector was recreated or
	// restarted while the record disappeared (see the baseline above), and
	// the fixture pod serving through it all was the same pod.
	tc.assertCollectorPodsUnchanged(t, collectorBaseline)
	tc.assertFixturePodUnchanged(t, exporter, fixtureBaseline)
}

// fixturePodIdentity snapshots a fixture exporter pod: its UID (recreation)
// and every container's restart count. A recreated or restarted fixture
// resets the Python server — and the flip marker with it, since the marker
// lives in the container's ephemeral filesystem — making the pod re-serve
// the attributed exposition, so either change across the measurement window
// confounds the expiry verdict.
func (tc *MonitoringTestCtx) fixturePodIdentity(t *testing.T, name string) collectorPodIdentity {
	t.Helper()
	pod := &corev1.Pod{}
	if err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: name, Namespace: "nvidia-gpu-operator"}, pod); err != nil {
		t.Fatalf("reading the fixture exporter pod %s: %v", name, err)
	}
	// A pod whose status has not been posted yet reports no container
	// statuses, and a baseline taken from it would silently skip the
	// fixture container in the restart check below. Both the baseline and
	// the post-measurement snapshot come through here, so either one lacking
	// the status fails the subtest instead of passing vacuously.
	if len(pod.Status.ContainerStatuses) == 0 {
		t.Fatalf("fixture exporter pod %s has no container status yet: the restart-count check cannot run", name)
	}
	restarts := make(map[string]int32, len(pod.Status.ContainerStatuses))
	for _, status := range pod.Status.ContainerStatuses {
		restarts[status.Name] = status.RestartCount
	}
	return collectorPodIdentity{uid: pod.UID, restarts: restarts}
}

// assertFixturePodUnchanged fails when the fixture pod captured in the
// baseline was recreated or had a container restart since.
func (tc *MonitoringTestCtx) assertFixturePodUnchanged(t *testing.T, name string, before collectorPodIdentity) {
	t.Helper()
	after := tc.fixturePodIdentity(t, name)
	if after.uid != before.uid {
		t.Fatalf("fixture exporter pod %s was recreated during the release-expiry measurement (UID %s -> %s): "+
			"a recreated pod re-serves the attributed exposition, so the record's absence no longer isolates metric_expiration",
			name, before.uid, after.uid)
	}
	for container, count := range before.restarts {
		afterCount, ok := after.restarts[container]
		if !ok {
			t.Fatalf("fixture exporter pod %s no longer reports container %s: the restart-count check cannot run",
				name, container)
		}
		if afterCount > count {
			t.Fatalf("fixture exporter container %s/%s restarted during the release-expiry measurement (%d -> %d): "+
				"a restarted container re-serves the attributed exposition, so the record's absence no longer isolates metric_expiration",
				name, container, count, afterCount)
		}
	}
}

// collectorPodIdentity captures what a collector restart changes: the pod's
// UID (recreation) and every container's restart count.
type collectorPodIdentity struct {
	uid      types.UID
	restarts map[string]int32
}

// collectorPodIdentities snapshots the collector StatefulSet's pods. The
// labels select exactly the collector, not the TargetAllocator, which the
// trusted DCGM scrape path does not use.
func (tc *MonitoringTestCtx) collectorPodIdentities(t *testing.T) map[string]collectorPodIdentity {
	t.Helper()
	pods := &corev1.PodList{}
	if err := tc.Client().List(tc.Context(), pods,
		client.InNamespace(tc.MonitoringNamespace),
		client.MatchingLabels(collectorNetworkPolicyLabels(tc.MonitoringNamespace))); err != nil {
		t.Fatalf("listing collector pods: %v", err)
	}
	if len(pods.Items) == 0 {
		t.Fatal("no collector pods found")
	}
	identities := make(map[string]collectorPodIdentity, len(pods.Items))
	for _, pod := range pods.Items {
		restarts := make(map[string]int32, len(pod.Status.ContainerStatuses))
		for _, status := range pod.Status.ContainerStatuses {
			restarts[status.Name] = status.RestartCount
		}
		identities[pod.Name] = collectorPodIdentity{uid: pod.UID, restarts: restarts}
	}
	return identities
}

// assertCollectorPodsUnchanged fails when any pod captured in the baseline was
// deleted, recreated, or had a container restart since: each resets the
// collector's exporter registry, and with it the stale label set whose expiry
// the release subtest measures. Pods added since the baseline are ignored —
// a new pod cannot remove a label set the baseline pods still serve.
func (tc *MonitoringTestCtx) assertCollectorPodsUnchanged(t *testing.T, baseline map[string]collectorPodIdentity) {
	t.Helper()
	current := tc.collectorPodIdentities(t)
	for name, before := range baseline {
		after, ok := current[name]
		if !ok {
			t.Fatalf("collector pod %s was deleted during the release-expiry measurement: a deleted pod's exporter registry cannot prove metric_expiration", name)
		}
		if after.uid != before.uid {
			t.Fatalf("collector pod %s was recreated during the release-expiry measurement (UID %s -> %s): "+
				"a recreated pod's empty exporter registry clears the stale label set without metric_expiration",
				name, before.uid, after.uid)
		}
		for container, count := range before.restarts {
			if after.restarts[container] > count {
				t.Fatalf("collector container %s/%s restarted during the release-expiry measurement (%d -> %d): "+
					"a restarted container's exporter registry clears the stale label set without metric_expiration",
					name, container, count, after.restarts[container])
			}
		}
	}
}
