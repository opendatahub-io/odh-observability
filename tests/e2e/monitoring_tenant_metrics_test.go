package e2e_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcfg "sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega matchers
)

const (
	// tenantMetricsNamespace hosts a workload whose metrics must only be
	// visible through the namespace-restricted proxy to authorized users.
	tenantMetricsNamespace = "tests-monitoring-tenant-metrics"
	// tenantMetricsForeignNamespace has no metrics and no authorization for
	// the tenant service account; queries against it must fail closed.
	tenantMetricsForeignNamespace = "tests-monitoring-tenant-metrics-foreign"
	tenantMetricsWorkloadName     = "tenant-metrics-exporter"
	tenantMetricsServiceAccount   = "tenant-metrics-viewer"
	// Prometheus serves valid exposition on :9090 with its stock self-scrape
	// config, so no custom metrics workload image is needed.
	tenantMetricsImage = "quay.io/prometheus/prometheus:v3.7.3@sha256:49214755b6153f90a597adcbff0252cc61069f8ab69ce8411285cd4a560e8038"
	// tenantMetricsQueryMetric is exposed by the workload's Prometheus server
	// and carries no job/instance label that the scrape chain would rewrite.
	tenantMetricsQueryMetric = "prometheus_build_info"

	prometheusRouteName = "data-science-prometheus-route"

	// otlpIdentitySpoofPodName names the persistent curl pod that pushes OTLP
	// metrics from inside the monitoring namespace.
	otlpIdentitySpoofPodName       = "otlp-identity-spoof-probe"
	otlpIdentityProbeContainerName = "otlp-spoof-probe"
	// otlpIdentitySpoofMetricPrefix prefixes the pushed metric name; a unique
	// suffix per run keeps stale series from earlier runs irrelevant.
	otlpIdentitySpoofMetricPrefix = "odh_observability_otlp_identity_probe"
	otlpIdentityProbeNamespace    = "tests-monitoring-otlp-identity"
)

// ValidateTenantWorkloadMetricsNamespaceRestriction proves the end-to-end
// tenant metrics contract: a workload in a monitored namespace is scraped via
// its ServiceMonitor, the collector-enriched k8s labels are promoted to
// namespace/pod by the collector ServiceMonitor's metricRelabelings, and the
// namespace-restricted proxy serves the resulting series only to users
// authorized for that namespace.
func (tc *MonitoringTestCtx) ValidateTenantWorkloadMetricsNamespaceRestriction(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.setupMetrics(t)

	tc.ensureTenantMetricsEnvironment(t)

	tc.EnsureDeploymentReady(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{
			Name:      tenantMetricsWorkloadName,
			Namespace: tenantMetricsNamespace,
		}),
	)

	routeHost := tc.fetchPrometheusRouteHost(t)
	tenantToken := tc.requestServiceAccountToken(t, tenantMetricsNamespace, tenantMetricsServiceAccount)
	// Route certificates are signed by the cluster ingress CA, which is not
	// necessarily part of the runner's system CA bundle. Resolve it once and
	// reuse the pool for every query below.
	rootCAs := clusterIngressCAPool(t, tc)

	// The full pipeline (ServiceMonitor discovery by the target allocator,
	// scrape, export, Prometheus ingestion) is eventually consistent; poll
	// until the tenant's series appear under the promoted namespace label.
	tc.g.Eventually(func() error {
		status, parsed, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, routeHost, tenantToken, tenantMetricsNamespace, tenantMetricsQueryMetric)
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("tenant query via %s returned status %d", routeHost, status)
		}
		if len(parsed.Data.Result) == 0 {
			return fmt.Errorf("no %s series visible in namespace %s yet", tenantMetricsQueryMetric, tenantMetricsNamespace)
		}
		for _, result := range parsed.Data.Result {
			if result.Metric["namespace"] != tenantMetricsNamespace {
				return errors.New("workload series has unexpected namespace attribution")
			}
			if result.Metric["pod"] == "" {
				return errors.New("workload series is missing promoted pod identity")
			}
		}
		return nil
	}, 10*time.Minute, 15*time.Second).Should(Succeed(),
		fmt.Sprintf("workload metrics from %s must be queryable via the namespace proxy with namespace=%q", tenantMetricsNamespace, tenantMetricsNamespace))

	// The tenant's token must be rejected for a namespace it cannot view:
	// kube-rbac-proxy performs a SubjectAccessReview per query.
	status, _, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, routeHost, tenantToken, tenantMetricsForeignNamespace, tenantMetricsQueryMetric)
	require.NoError(t, err, "query for foreign namespace must reach the proxy")
	require.Equal(t, http.StatusForbidden, status,
		"tenant service account must be forbidden to query metrics of namespace %s", tenantMetricsForeignNamespace)

	// Even a viewer authorized for the foreign namespace must not see the
	// tenant's series under it: prom-label-proxy enforces the namespace label.
	foreignViewerToken := tc.metricsViewerToken(t, "tenant-metrics-foreign-viewer", tenantMetricsForeignNamespace)
	status, parsed, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, routeHost, foreignViewerToken, tenantMetricsForeignNamespace, tenantMetricsQueryMetric)
	require.NoError(t, err, "viewer query for foreign namespace must reach the proxy")
	require.Equal(t, http.StatusOK, status)
	if seriesCount := len(parsed.Data.Result); seriesCount != 0 {
		t.Fatalf("viewer query for foreign namespace returned %d series", seriesCount)
	}
}

// ValidateOTLPMetricIdentityCannotBeSpoofed proves the OTLP trust boundary:
// a workload in an enrolled namespace sends a uniquely named metric whose
// resource attributes claim a real victim workload's namespace/pod identity,
// in dotted and underscored spellings, plus forged pod IP/UID association hints.
// The series must arrive, identify the sender, and remain invisible to an
// authorized tenant. Delivery, attribution, sanitization, and isolation are
// reported independently so one failure does not hide the others.
func (tc *MonitoringTestCtx) ValidateOTLPMetricIdentityCannotBeSpoofed(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	evidence := &otlpIdentityEvidence{}
	var sender, victim otlpPodIdentity
	defer func() {
		if t.Failed() {
			tc.logOTLPIdentityDiagnostics(t, evidence, sender, victim)
		}
	}()

	tc.setupMetrics(t)
	tc.ensureTenantMetricsEnvironment(t)
	tc.ensureOTLPIdentityProbeNamespace(t)

	otlpHost := tc.discoverCollectorOTLPHTTPHost(t)
	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	// Validate the tenant route before using an empty query as evidence of
	// isolation: the authorized tenant identity must be able to see real data.
	routeHost := tc.fetchPrometheusRouteHost(t)
	rootCAs := clusterIngressCAPool(t, tc)
	viewerToken := tc.metricsViewerToken(t, "otlp-identity-viewer", otlpIdentityProbeNamespace)
	tenantToken := tc.requestServiceAccountToken(t, tenantMetricsNamespace, tenantMetricsServiceAccount)
	victim = tc.readyTenantMetricsPodIdentity(t)
	tc.requireTenantMetricsPositiveControl(t, rootCAs, routeHost, tenantToken, victim)

	// Run an inference-labeled probe from the enrolled namespace. Keep it
	// alive for warm-up and repeated attack submissions.
	tc.createOTLPIdentityProbe(t, otlpIdentityProbeNamespace)
	probe := tc.readyOTLPIdentityProbe(t, otlpIdentityProbeNamespace)
	sender = podIdentity(probe)
	require.NotEmpty(t, sender.IP, "OTLP identity probe must have a pod IP")

	collectorPods := tc.readyCollectorReplicaPods(t)
	scrapeInterval := tc.collectorPrometheusScrapeInterval(t)
	warmupResults := tc.warmCollectorReplicas(t, collectorPods, runID, sender, scrapeInterval, rootCAs, routeHost, viewerToken)
	evidence.warmups = warmupResults

	metricName := otlpIdentitySpoofMetricPrefix + "_attack_" + runID
	resourceAttributes := map[string]string{
		"k8s.namespace.name": tenantMetricsNamespace,
		"k8s.pod.name":       victim.Name,
		"k8s.pod.uid":        string(victim.UID),
		"k8s.pod.ip":         victim.IP,
		"k8s_namespace_name": tenantMetricsNamespace,
		"k8s_pod_name":       victim.Name,
		"namespace":          tenantMetricsNamespace,
		"pod":                victim.Name,
		"exported_namespace": tenantMetricsNamespace,
		"exported_pod":       victim.Name,
	}
	observationWindow := 3 * scrapeInterval
	pollInterval := max(time.Second, min(scrapeInterval/3, 10*time.Second))
	observationWindow += pollInterval
	t.Logf("observing OTLP delivery and tenant isolation for %s (Prometheus scrape interval %s)", observationWindow, scrapeInterval)

	started := time.Now()
	deadline := started.Add(observationWindow)
	for attempt := 1; ; attempt++ {
		datapointAttributes := map[string]string{
			"namespace":          tenantMetricsNamespace,
			"pod":                victim.Name,
			"exported_namespace": tenantMetricsNamespace,
			"exported_pod":       victim.Name,
			"k8s_namespace_name": tenantMetricsNamespace,
			"k8s_pod_name":       victim.Name,
			"probe_attempt":      strconv.Itoa(attempt),
		}
		payload, payloadErr := buildOTLPMetricsPayload(metricName, time.Now(), resourceAttributes, datapointAttributes)
		if payloadErr != nil {
			evidence.lastPushFailed = true
		} else {
			push := tc.pushOTLPMetrics(t, sender.Namespace, otlpHost, payload)
			evidence.pushAttempts++
			evidence.lastPushStatus = push.status
			evidence.lastPushFailed = push.err != nil
			if push.err == nil {
				evidence.acceptedPushes++
			}
		}

		snapshot := otlpIdentitySnapshot{
			at:     time.Now(),
			sender: queryOTLPMetric(tc.Context(), rootCAs, routeHost, viewerToken, sender.Namespace, metricName),
			tenant: queryOTLPMetric(tc.Context(), rootCAs, routeHost, tenantToken, tenantMetricsNamespace, metricName),
		}
		evidence.snapshots = append(evidence.snapshots, snapshot)
		if !time.Now().Before(deadline) {
			break
		}
		time.Sleep(pollInterval)
	}

	for _, warmup := range warmupResults {
		result := warmup
		t.Run("collector replica warm-up/"+result.collectorPod, func(t *testing.T) {
			require.True(t, result.attributed,
				"warm-up metric was not attributed on collector replica %s (push status=%d, push failed=%t, query status=%d, query failed=%t, series count=%d)",
				result.collectorPod, result.lastPushStatus, result.lastPushFailed,
				result.lastQueryStatus, result.lastQueryFailed, result.lastQuerySeriesCount)
		})
	}
	t.Run("OTLP delivery", func(t *testing.T) {
		require.NoError(t, otlpDeliveryError(evidence))
	})
	t.Run("sender attribution", func(t *testing.T) {
		require.NoError(t, otlpAttributionError(evidence.snapshots, sender))
	})
	t.Run("forged identity sanitization", func(t *testing.T) {
		require.NoError(t, otlpSanitizationError(evidence.snapshots, sender))
	})
	t.Run("tenant isolation", func(t *testing.T) {
		require.NoError(t, otlpTenantIsolationError(evidence.snapshots, 3*scrapeInterval),
			"authorized tenant queries must stay empty for the injected metric")
	})
}

type otlpPodIdentity struct {
	Namespace string
	Name      string
	UID       types.UID
	IP        string
}

type otlpAnyValue struct {
	StringValue string `json:"stringValue"`
}

type otlpKeyValue struct {
	Key   string       `json:"key"`
	Value otlpAnyValue `json:"value"`
}

type otlpDataPoint struct {
	TimeUnixNano string         `json:"timeUnixNano"`
	AsDouble     float64        `json:"asDouble"`
	Attributes   []otlpKeyValue `json:"attributes"`
}

type otlpGauge struct {
	DataPoints []otlpDataPoint `json:"dataPoints"`
}

type otlpMetric struct {
	Name  string    `json:"name"`
	Gauge otlpGauge `json:"gauge"`
}

type otlpScope struct {
	Name string `json:"name"`
}

type otlpScopeMetrics struct {
	Scope   otlpScope    `json:"scope"`
	Metrics []otlpMetric `json:"metrics"`
}

type otlpResource struct {
	Attributes []otlpKeyValue `json:"attributes"`
}

type otlpResourceMetrics struct {
	Resource     otlpResource       `json:"resource"`
	ScopeMetrics []otlpScopeMetrics `json:"scopeMetrics"`
}

type otlpExportMetricsRequest struct {
	ResourceMetrics []otlpResourceMetrics `json:"resourceMetrics"`
}

type otlpQueryObservation struct {
	status int
	series []map[string]string
	err    error
}

func (observation otlpQueryObservation) successful() bool {
	return observation.err == nil && observation.status == http.StatusOK
}

type otlpIdentitySnapshot struct {
	at     time.Time
	sender otlpQueryObservation
	tenant otlpQueryObservation
}

type otlpWarmupResult struct {
	collectorPod         string
	instance             string
	attributed           bool
	lastPushStatus       int
	lastPushFailed       bool
	lastQueryStatus      int
	lastQueryFailed      bool
	lastQuerySeriesCount int
}

type otlpIdentityEvidence struct {
	pushAttempts   int
	acceptedPushes int
	lastPushStatus int
	lastPushFailed bool
	snapshots      []otlpIdentitySnapshot
	warmups        []otlpWarmupResult
}

type otlpPushObservation struct {
	status int
	err    error
}

func buildOTLPMetricsPayload(metricName string, timestamp time.Time, resourceAttributes, datapointAttributes map[string]string) ([]byte, error) {
	datapoint := otlpDataPoint{
		TimeUnixNano: strconv.FormatInt(timestamp.UnixNano(), 10),
		AsDouble:     1,
		Attributes:   otlpKeyValues(datapointAttributes),
	}
	request := otlpExportMetricsRequest{
		ResourceMetrics: []otlpResourceMetrics{{
			Resource: otlpResource{Attributes: otlpKeyValues(resourceAttributes)},
			ScopeMetrics: []otlpScopeMetrics{{
				Scope: otlpScope{Name: "odh-observability-e2e-spoof-probe"},
				Metrics: []otlpMetric{{
					Name:  metricName,
					Gauge: otlpGauge{DataPoints: []otlpDataPoint{datapoint}},
				}},
			}},
		}},
	}
	return json.Marshal(request)
}

func otlpKeyValues(attributes map[string]string) []otlpKeyValue {
	keys := make([]string, 0, len(attributes))
	for key := range attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	values := make([]otlpKeyValue, 0, len(keys))
	for _, key := range keys {
		values = append(values, otlpKeyValue{Key: key, Value: otlpAnyValue{StringValue: attributes[key]}})
	}
	return values
}

func queryOTLPMetric(ctx context.Context, rootCAs *x509.CertPool, routeHost, token, namespace, query string) otlpQueryObservation {
	status, parsed, err := queryMetricsViaNamespaceProxy(ctx, rootCAs, routeHost, token, namespace, query)
	observation := otlpQueryObservation{status: status, err: err}
	if err == nil && status == http.StatusOK && parsed.Status != "success" {
		observation.err = errors.New("Prometheus query returned a non-success API response")
	}
	for _, result := range parsed.Data.Result {
		observation.series = append(observation.series, result.Metric)
	}
	return observation
}

func podIdentity(pod corev1.Pod) otlpPodIdentity {
	return otlpPodIdentity{
		Namespace: pod.Namespace,
		Name:      pod.Name,
		UID:       pod.UID,
		IP:        pod.Status.PodIP,
	}
}

func podIsReady(pod corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (tc *MonitoringTestCtx) readyTenantMetricsPodIdentity(t *testing.T) otlpPodIdentity {
	t.Helper()
	tc = tc.WithT(t)

	var identity otlpPodIdentity
	tc.g.Eventually(func() error {
		pods := &corev1.PodList{}
		if err := tc.Client().List(tc.Context(), pods,
			client.InNamespace(tenantMetricsNamespace),
			client.MatchingLabels{"app": tenantMetricsWorkloadName},
		); err != nil {
			return fmt.Errorf("list tenant workload pods: %w", err)
		}
		for i := range pods.Items {
			if podIsReady(pods.Items[i]) {
				identity = podIdentity(pods.Items[i])
				return nil
			}
		}
		return fmt.Errorf("no ready %s pod in namespace %s", tenantMetricsWorkloadName, tenantMetricsNamespace)
	}, 5*time.Minute, 5*time.Second).Should(Succeed(), "tenant metrics workload pod must be ready")
	return identity
}

func (tc *MonitoringTestCtx) requireTenantMetricsPositiveControl(t *testing.T, rootCAs *x509.CertPool, routeHost, tenantToken string, victim otlpPodIdentity) {
	t.Helper()
	tc = tc.WithT(t)

	tc.g.Eventually(func() error {
		observation := queryOTLPMetric(tc.Context(), rootCAs, routeHost, tenantToken, tenantMetricsNamespace, tenantMetricsQueryMetric)
		if observation.err != nil {
			return fmt.Errorf("tenant positive-control query failed (HTTP status %d)", observation.status)
		}
		if observation.status != http.StatusOK {
			return fmt.Errorf("tenant positive-control query returned HTTP %d", observation.status)
		}
		for _, series := range observation.series {
			if series["namespace"] == victim.Namespace && series["pod"] == victim.Name {
				return nil
			}
		}
		return errors.New("tenant positive-control query has not returned workload metrics")
	}, 5*time.Minute, 10*time.Second).Should(Succeed(),
		"authorized tenant query must see real workload metrics before isolation is checked")
}

func (tc *MonitoringTestCtx) ensureOTLPIdentityProbeNamespace(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.DeleteResource(
		WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: otlpIdentityProbeNamespace}),
		WithIgnoreNotFound(true),
		WithWaitForDeletion(true),
	)
	t.Cleanup(func() {
		tc.DeleteResource(
			WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: otlpIdentityProbeNamespace}),
			WithIgnoreNotFound(true),
			WithWaitForDeletion(true),
		)
	})
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: otlpIdentityProbeNamespace}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.SetLabels(map[string]string{otlpNamespaceLabel: "true"})
			return nil
		}),
	)
}

func (tc *MonitoringTestCtx) createOTLPIdentityProbe(t *testing.T, namespace string) {
	t.Helper()
	tc = tc.WithT(t)

	tc.DeleteResource(
		WithMinimalObject(gvk.Pod, types.NamespacedName{Name: otlpIdentitySpoofPodName, Namespace: namespace}),
		WithIgnoreNotFound(true),
		WithWaitForDeletion(true),
	)
	t.Cleanup(func() {
		tc.DeleteResource(
			WithMinimalObject(gvk.Pod, types.NamespacedName{Name: otlpIdentitySpoofPodName, Namespace: namespace}),
			WithIgnoreNotFound(true),
			WithWaitForDeletion(true),
		)
	})
	tc.createFixtureResource(t, gvk.Pod, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      otlpIdentitySpoofPodName,
			Namespace: namespace,
			Labels:    otlpCallerNetworkPolicyLabels("llminferenceservice-workload"),
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			AutomountServiceAccountToken: new(false),
			SecurityContext:              fixturePodSecurityContext(),
			Volumes: []corev1.Volume{{
				Name: "service-ca",
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "openshift-service-ca.crt"},
					Items:                []corev1.KeyToPath{{Key: "service-ca.crt", Path: "service-ca.crt"}},
				}},
			}},
			Containers: []corev1.Container{{
				Name:            otlpIdentityProbeContainerName,
				Image:           fakeGCSClientImage,
				Command:         []string{"/bin/sh", "-c"},
				Args:            []string{"sleep 1800"},
				VolumeMounts:    []corev1.VolumeMount{{Name: "service-ca", MountPath: "/var/run/secrets/service-ca", ReadOnly: true}},
				SecurityContext: fixtureContainerSecurityContext(),
			}},
		},
	})
}

func (tc *MonitoringTestCtx) readyOTLPIdentityProbe(t *testing.T, namespace string) corev1.Pod {
	t.Helper()
	tc = tc.WithT(t)

	var probe corev1.Pod
	tc.g.Eventually(func() error {
		if err := tc.Client().Get(tc.Context(), types.NamespacedName{
			Name: otlpIdentitySpoofPodName, Namespace: namespace,
		}, &probe); err != nil {
			return err
		}
		if !podIsReady(probe) {
			return fmt.Errorf("OTLP probe pod phase=%s is not Ready", probe.Status.Phase)
		}
		for _, status := range probe.Status.ContainerStatuses {
			if status.Name == otlpIdentityProbeContainerName && status.Ready {
				return nil
			}
		}
		return fmt.Errorf("OTLP probe container %s is not Ready", otlpIdentityProbeContainerName)
	}, 5*time.Minute, 5*time.Second).Should(Succeed(), "OTLP identity probe must become ready")
	return probe
}

func (tc *MonitoringTestCtx) readyCollectorReplicaPods(t *testing.T) []corev1.Pod {
	t.Helper()
	tc = tc.WithT(t)

	collectorLabels := client.MatchingLabels{
		"app.kubernetes.io/component":  "opentelemetry-collector",
		"app.kubernetes.io/instance":   tc.MonitoringNamespace + ".data-science-collector",
		"app.kubernetes.io/managed-by": "opentelemetry-operator",
		"app.kubernetes.io/part-of":    "opentelemetry",
	}
	var readyPods []corev1.Pod
	tc.g.Eventually(func() error {
		statefulSets := &appsv1.StatefulSetList{}
		if err := tc.Client().List(tc.Context(), statefulSets,
			client.InNamespace(tc.MonitoringNamespace),
			client.MatchingLabels{
				"app.kubernetes.io/instance":   tc.MonitoringNamespace + ".data-science-collector",
				"app.kubernetes.io/managed-by": "opentelemetry-operator",
			},
		); err != nil {
			return fmt.Errorf("list collector StatefulSets: %w", err)
		}
		if len(statefulSets.Items) != 1 {
			return fmt.Errorf("found %d collector StatefulSets, want exactly one", len(statefulSets.Items))
		}
		statefulSet := statefulSets.Items[0]
		desiredReplicas := int32(1)
		if statefulSet.Spec.Replicas != nil {
			desiredReplicas = *statefulSet.Spec.Replicas
		}
		if statefulSet.Status.ObservedGeneration < statefulSet.Generation || statefulSet.Status.ReadyReplicas < desiredReplicas {
			return fmt.Errorf("collector StatefulSet ready=%d desired=%d observedGeneration=%d generation=%d",
				statefulSet.Status.ReadyReplicas, desiredReplicas,
				statefulSet.Status.ObservedGeneration, statefulSet.Generation)
		}

		pods := &corev1.PodList{}
		if err := tc.Client().List(tc.Context(), pods, client.InNamespace(tc.MonitoringNamespace), collectorLabels); err != nil {
			return fmt.Errorf("list collector pods: %w", err)
		}
		readyPods = readyPods[:0]
		var readyPodCount int32
		for i := range pods.Items {
			if podIsReady(pods.Items[i]) {
				readyPods = append(readyPods, pods.Items[i])
				readyPodCount++
			}
		}
		if readyPodCount < desiredReplicas {
			return fmt.Errorf("found %d Ready collector pods, want at least %d", len(readyPods), desiredReplicas)
		}
		sort.Slice(readyPods, func(i, j int) bool { return readyPods[i].Name < readyPods[j].Name })
		return nil
	}, 10*time.Minute, 5*time.Second).Should(Succeed(), "all collector replicas must be Ready before OTLP warm-up")
	return readyPods
}

func (tc *MonitoringTestCtx) collectorPrometheusScrapeInterval(t *testing.T) time.Duration {
	t.Helper()
	tc = tc.WithT(t)

	monitor := tc.FetchResource(WithMinimalObject(gvk.ServiceMonitor, types.NamespacedName{
		Name: "data-science-prometheus-monitor", Namespace: tc.MonitoringNamespace,
	}))
	endpoints, found, err := unstructured.NestedSlice(monitor.Object, "spec", "endpoints")
	require.NoError(t, err, "read collector ServiceMonitor endpoints")
	require.True(t, found && len(endpoints) > 0, "collector ServiceMonitor must define a scrape endpoint")
	endpoint, ok := endpoints[0].(map[string]any)
	require.True(t, ok, "collector ServiceMonitor endpoint must be an object")
	interval, found, err := unstructured.NestedString(endpoint, "interval")
	require.NoError(t, err, "read collector ServiceMonitor interval")
	if found && interval != "" {
		parsed, parseErr := time.ParseDuration(interval)
		require.NoError(t, parseErr, "parse ServiceMonitor interval %q", interval)
		t.Logf("collector Prometheus scrape interval: %s (ServiceMonitor)", parsed)
		return parsed
	}

	for _, group := range []string{"monitoring.coreos.com", "monitoring.rhobs"} {
		for _, name := range []string{MonitoringStackName, "prometheus-" + MonitoringStackName} {
			prometheus := &unstructured.Unstructured{}
			prometheus.SetGroupVersionKind(schema.GroupVersionKind{Group: group, Version: "v1", Kind: "Prometheus"})
			err := tc.Client().Get(tc.Context(), types.NamespacedName{
				Name: name, Namespace: tc.MonitoringNamespace,
			}, prometheus)
			if err != nil {
				if meta.IsNoMatchError(err) || k8serr.IsNotFound(err) {
					continue
				}
				require.NoError(t, err, "read generated Prometheus scrape interval")
			}
			interval, found, err := unstructured.NestedString(prometheus.Object, "spec", "scrapeInterval")
			require.NoError(t, err, "read generated Prometheus scrape interval")
			if found && interval != "" {
				parsed, parseErr := time.ParseDuration(interval)
				require.NoError(t, parseErr, "parse Prometheus scrape interval %q", interval)
				t.Logf("collector Prometheus scrape interval: %s (Prometheus resource)", parsed)
				return parsed
			}
			break
		}
	}

	// Prometheus Operator defaults PrometheusSpec.scrapeInterval to 30s.
	t.Log("collector Prometheus scrape interval: 30s (Prometheus Operator default)")
	return 30 * time.Second
}

func (tc *MonitoringTestCtx) warmCollectorReplicas(
	t *testing.T,
	collectors []corev1.Pod,
	runID string,
	sender otlpPodIdentity,
	scrapeInterval time.Duration,
	rootCAs *x509.CertPool,
	routeHost, viewerToken string,
) []otlpWarmupResult {
	t.Helper()
	tc = tc.WithT(t)

	results := make([]otlpWarmupResult, 0, len(collectors))
	for _, collector := range collectors {
		result := otlpWarmupResult{
			collectorPod: collector.Name,
			instance:     net.JoinHostPort(collector.Status.PodIP, "8889"),
		}
		warmupQuery := fmt.Sprintf(`{__name__=~"%s_%s_warm_[0-9]+"}`, otlpIdentitySpoofMetricPrefix, runID)
		deadline := time.Now().Add(3*scrapeInterval + 30*time.Second)
		for attempt := 1; time.Now().Before(deadline); attempt++ {
			metricName := fmt.Sprintf("%s_%s_warm_%d", otlpIdentitySpoofMetricPrefix, runID, attempt)
			payload, err := buildOTLPMetricsPayload(metricName, time.Now(), nil, nil)
			if err != nil {
				result.lastPushFailed = true
				break
			}
			push := tc.pushOTLPMetrics(t, sender.Namespace, net.JoinHostPort(collector.Status.PodIP, "4318"), payload)
			result.lastPushStatus = push.status
			result.lastPushFailed = push.err != nil

			query := queryOTLPMetric(tc.Context(), rootCAs, routeHost, viewerToken, sender.Namespace, warmupQuery)
			result.lastQueryStatus = query.status
			result.lastQueryFailed = !query.successful()
			result.lastQuerySeriesCount = len(query.series)
			for _, series := range query.series {
				if series["instance"] != result.instance {
					continue
				}
				if otlpSeriesAttributionError(series, sender) == nil {
					result.attributed = true
					break
				}
			}
			if result.attributed {
				break
			}
			time.Sleep(5 * time.Second)
		}
		results = append(results, result)
		t.Logf("collector warm-up pod=%s attributed=%t pushStatus=%d pushFailed=%t queryStatus=%d queryFailed=%t seriesCount=%d",
			result.collectorPod, result.attributed, result.lastPushStatus, result.lastPushFailed,
			result.lastQueryStatus, result.lastQueryFailed, result.lastQuerySeriesCount)
	}
	return results
}

func (tc *MonitoringTestCtx) pushOTLPMetrics(t *testing.T, namespace, address string, payload []byte) otlpPushObservation {
	t.Helper()
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return otlpPushObservation{err: fmt.Errorf("parse OTLP address %q: %w", address, err)}
	}

	// Keep the service DNS name in the HTTPS URL so curl validates the
	// service-ca certificate's SAN while --connect-to pins each warm-up probe
	// to the selected collector replica.
	tlsHost := host
	var connectTo []string
	if net.ParseIP(host) != nil {
		tlsHost = fmt.Sprintf("%s-headless.%s.svc.cluster.local", OpenTelemetryCollectorServiceName, tc.MonitoringNamespace)
		connectTo = []string{"--connect-to", net.JoinHostPort(tlsHost, port) + ":" + net.JoinHostPort(host, port)}
	}
	command := make([]string, 0, 7+len(connectTo)+9)
	command = append(command,
		"curl", "--silent", "--show-error", "--max-time", "15",
		"--cacert", "/var/run/secrets/service-ca/service-ca.crt",
	)
	command = append(command, connectTo...)
	command = append(command,
		"--request", "POST", "--header", "Content-Type: application/json",
		"--data-binary", "@-", "--write-out", "\\n%{http_code}",
		"https://"+net.JoinHostPort(tlsHost, port)+"/v1/metrics",
	)
	stdout, stderr, err := tc.execOTLPProbe(t, namespace, command, payload)
	status, body, responseErr := parseOTLPHTTPResponse(stdout)
	if err != nil {
		return otlpPushObservation{status: status, err: fmt.Errorf("exec OTLP curl: %w: %s", err, strings.TrimSpace(stderr))}
	}
	if responseErr != nil {
		return otlpPushObservation{status: status, err: responseErr}
	}
	if body != "" {
		var response struct {
			PartialSuccess *struct {
				RejectedDataPoints json.RawMessage `json:"rejectedDataPoints"`
				ErrorMessage       string          `json:"errorMessage"`
			} `json:"partialSuccess"`
		}
		if err := json.Unmarshal([]byte(body), &response); err != nil {
			return otlpPushObservation{status: status, err: fmt.Errorf("decode OTLP response: %w", err)}
		}
		if response.PartialSuccess != nil {
			rejected, err := jsonInt64(response.PartialSuccess.RejectedDataPoints)
			if err != nil {
				return otlpPushObservation{status: status, err: fmt.Errorf("decode rejected OTLP data-point count: %w", err)}
			}
			if rejected > 0 || response.PartialSuccess.ErrorMessage != "" {
				return otlpPushObservation{status: status, err: fmt.Errorf("collector partially rejected OTLP request: rejected=%d error=%q", rejected, response.PartialSuccess.ErrorMessage)}
			}
		}
	}
	return otlpPushObservation{status: status}
}

func jsonInt64(value json.RawMessage) (int64, error) {
	value = bytes.TrimSpace(value)
	if len(value) == 0 || string(value) == "null" {
		return 0, nil
	}
	if value[0] == '"' {
		var stringValue string
		if err := json.Unmarshal(value, &stringValue); err != nil {
			return 0, err
		}
		return strconv.ParseInt(stringValue, 10, 64)
	}
	return strconv.ParseInt(string(value), 10, 64)
}

func parseOTLPHTTPResponse(output string) (int, string, error) {
	separator := strings.LastIndexByte(output, '\n')
	if separator < 0 {
		return 0, "", errors.New("curl response did not include HTTP status")
	}
	status, err := strconv.Atoi(strings.TrimSpace(output[separator+1:]))
	if err != nil {
		return 0, "", fmt.Errorf("parse curl HTTP status: %w", err)
	}
	body := strings.TrimSpace(output[:separator])
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		return status, body, fmt.Errorf("collector returned HTTP %d", status)
	}
	return status, body, nil
}

func (tc *MonitoringTestCtx) execOTLPProbe(t *testing.T, namespace string, command []string, stdin []byte) (string, string, error) {
	t.Helper()
	tc = tc.WithT(t)

	cfg, err := ctrlcfg.GetConfig()
	if err != nil {
		return "", "", fmt.Errorf("load kubeconfig for pod exec: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return "", "", fmt.Errorf("create Kubernetes client for pod exec: %w", err)
	}
	request := clientset.CoreV1().RESTClient().Post().
		Resource("pods").Name(otlpIdentitySpoofPodName).
		Namespace(namespace).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: otlpIdentityProbeContainerName,
			Command:   command,
			Stdin:     stdin != nil,
			Stdout:    true,
			Stderr:    true,
		}, clientgoscheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(cfg, http.MethodPost, request.URL())
	if err != nil {
		return "", "", fmt.Errorf("create Kubernetes pod exec stream: %w", err)
	}
	ctx, cancel := context.WithTimeout(tc.Context(), 30*time.Second)
	defer cancel()

	var stdout, stderr bytes.Buffer
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  bytes.NewReader(stdin),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return stdout.String(), stderr.String(), err
}

func otlpVisibleSeries(snapshots []otlpIdentitySnapshot) []map[string]string {
	var series []map[string]string
	for _, snapshot := range snapshots {
		if snapshot.sender.successful() {
			series = append(series, snapshot.sender.series...)
		}
		if snapshot.tenant.successful() {
			series = append(series, snapshot.tenant.series...)
		}
	}
	return series
}

func senderNamespaceObserved(snapshots []otlpIdentitySnapshot) bool {
	for _, snapshot := range snapshots {
		if snapshot.sender.successful() && len(snapshot.sender.series) > 0 {
			return true
		}
	}
	return false
}

func otlpSeriesAttributionError(series map[string]string, sender otlpPodIdentity) error {
	for _, expected := range []struct{ label, value string }{
		{label: "namespace", value: sender.Namespace},
		{label: "pod", value: sender.Name},
		{label: "k8s_namespace_name", value: sender.Namespace},
		{label: "k8s_pod_name", value: sender.Name},
	} {
		if got := series[expected.label]; got != expected.value {
			return fmt.Errorf("label %q does not match sender identity", expected.label)
		}
	}
	return nil
}

func otlpSeriesSanitizationError(series map[string]string, sender otlpPodIdentity) error {
	if err := otlpSeriesAttributionError(series, sender); err != nil {
		return fmt.Errorf("identity labels were not replaced with the sender identity: %w", err)
	}
	for _, label := range []string{"exported_namespace", "exported_pod"} {
		if got := series[label]; got != "" {
			return fmt.Errorf("forbidden identity label %q remains on series", label)
		}
	}
	for _, expected := range []struct{ label, value string }{
		{label: "k8s_pod_ip", value: sender.IP},
		{label: "k8s_pod_uid", value: string(sender.UID)},
	} {
		if got := series[expected.label]; got != "" && got != expected.value {
			return fmt.Errorf("label %q does not match sender identity", expected.label)
		}
	}
	return nil
}

func otlpDeliveryError(evidence *otlpIdentityEvidence) error {
	if evidence.acceptedPushes == 0 {
		return fmt.Errorf("collector accepted no OTLP pushes after %d attempts (last HTTP status=%d, request failed=%t)",
			evidence.pushAttempts, evidence.lastPushStatus, evidence.lastPushFailed)
	}
	if !senderNamespaceObserved(evidence.snapshots) {
		return errors.New("accepted OTLP metrics were not observed under the sender namespace")
	}
	return nil
}

func otlpAttributionError(snapshots []otlpIdentitySnapshot, sender otlpPodIdentity) error {
	series := otlpVisibleSeries(snapshots)
	if len(series) == 0 {
		return errors.New("no attack series observed to verify sender attribution")
	}
	for i, labels := range series {
		if err := otlpSeriesAttributionError(labels, sender); err != nil {
			return fmt.Errorf("series %d: %w", i+1, err)
		}
	}
	return nil
}

func otlpSanitizationError(snapshots []otlpIdentitySnapshot, sender otlpPodIdentity) error {
	series := otlpVisibleSeries(snapshots)
	if len(series) == 0 {
		return errors.New("no attack series observed to verify forged-identity sanitization")
	}
	for i, labels := range series {
		if err := otlpSeriesSanitizationError(labels, sender); err != nil {
			return fmt.Errorf("series %d: %w", i+1, err)
		}
	}
	return nil
}

func otlpTenantIsolationError(snapshots []otlpIdentitySnapshot, minObservationSpan time.Duration) error {
	var emptyQueries []time.Time
	for _, snapshot := range snapshots {
		if !snapshot.tenant.successful() {
			continue
		}
		if len(snapshot.tenant.series) > 0 {
			return fmt.Errorf("tenant query observed %d injected series", len(snapshot.tenant.series))
		}
		emptyQueries = append(emptyQueries, snapshot.at)
	}
	if len(emptyQueries) < 4 {
		return fmt.Errorf("only %d successful empty tenant queries; need at least 4", len(emptyQueries))
	}
	span := emptyQueries[len(emptyQueries)-1].Sub(emptyQueries[0])
	if span < minObservationSpan {
		return fmt.Errorf("successful empty tenant queries span %s, want at least %s", span, minObservationSpan)
	}
	return nil
}

// logOTLPIdentityDiagnostics emits allowlisted summaries only; CI output may
// be retained, so keep raw labels, errors, objects, and collector logs out.
func (tc *MonitoringTestCtx) logOTLPIdentityDiagnostics(t *testing.T, evidence *otlpIdentityEvidence, sender, victim otlpPodIdentity) {
	t.Helper()
	t.Logf("OTLP identity summary: senderAvailable=%t victimAvailable=%t pushes=%d accepted=%d lastPushStatus=%d lastPushFailed=%t snapshots=%d",
		sender.Name != "", victim.Name != "", evidence.pushAttempts, evidence.acceptedPushes,
		evidence.lastPushStatus, evidence.lastPushFailed, len(evidence.snapshots))
	for _, warmup := range evidence.warmups {
		t.Logf("collector warm-up: pod=%s attributed=%t pushStatus=%d pushFailed=%t queryStatus=%d queryFailed=%t seriesCount=%d",
			warmup.collectorPod, warmup.attributed, warmup.lastPushStatus, warmup.lastPushFailed,
			warmup.lastQueryStatus, warmup.lastQueryFailed, warmup.lastQuerySeriesCount)
	}
	if len(evidence.snapshots) > 0 {
		last := evidence.snapshots[len(evidence.snapshots)-1]
		t.Logf("last sender query: status=%d failed=%t seriesCount=%d",
			last.sender.status, !last.sender.successful(), len(last.sender.series))
		t.Logf("last tenant query: status=%d failed=%t seriesCount=%d",
			last.tenant.status, !last.tenant.successful(), len(last.tenant.series))
	}

	diagnosticContext, cancel := context.WithTimeout(tc.Context(), 45*time.Second)
	defer cancel()
	diagnosticTC := *tc
	diagnosticTC.ctx = diagnosticContext
	for _, identity := range []struct {
		role     string
		identity otlpPodIdentity
	}{
		{role: "sender", identity: sender},
		{role: "victim", identity: victim},
	} {
		if identity.identity.Name == "" {
			continue
		}
		pod := &corev1.Pod{}
		err := diagnosticTC.Client().Get(diagnosticTC.Context(), types.NamespacedName{
			Name: identity.identity.Name, Namespace: identity.identity.Namespace,
		}, pod)
		if err != nil {
			t.Logf("identity pod diagnostics unavailable: role=%s", identity.role)
			continue
		}
		t.Logf("identity pod status: role=%s %s", identity.role, podDiagnosticSummary(pod))
	}

	collector, err := diagnosticTC.fetchResource(t, gvk.OpenTelemetryCollector, types.NamespacedName{
		Name: OpenTelemetryCollectorName, Namespace: tc.MonitoringNamespace,
	})
	pipelineConfigured := false
	if err == nil {
		if pipelines, found, nestedErr := unstructured.NestedMap(collector.Object, "spec", "config", "service", "pipelines"); nestedErr == nil && found {
			pipelineConfigured = pipelines["metrics/otlp"] != nil
		}
	}
	t.Logf("collector metrics/otlp pipeline configured=%t", pipelineConfigured)

	pods := &corev1.PodList{}
	if err := diagnosticTC.Client().List(diagnosticTC.Context(), pods, client.InNamespace(tc.MonitoringNamespace), client.MatchingLabels{
		"app.kubernetes.io/component":  "opentelemetry-collector",
		"app.kubernetes.io/instance":   tc.MonitoringNamespace + ".data-science-collector",
		"app.kubernetes.io/managed-by": "opentelemetry-operator",
	}); err != nil {
		t.Log("collector pod status diagnostics unavailable")
		return
	}
	t.Logf("collector replica count=%d", len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		t.Logf("collector pod status: name=%s %s", pod.Name, podDiagnosticSummary(pod))
	}
}

// podDiagnosticSummary exposes status needed for triage without pod specs,
// network identifiers, image references, or free-form status messages.
func podDiagnosticSummary(pod *corev1.Pod) string {
	containers := make([]string, 0, len(pod.Status.ContainerStatuses))
	for i, status := range pod.Status.ContainerStatuses {
		state, reason := containerDiagnosticState(status)
		exitCode := ""
		if status.State.Terminated != nil {
			exitCode = fmt.Sprintf(",exitCode=%d", status.State.Terminated.ExitCode)
		}
		containers = append(containers, fmt.Sprintf("container[%d](state=%s,reason=%s,ready=%t,restarts=%d%s)",
			i, state, reason, status.Ready, status.RestartCount, exitCode))
	}
	return fmt.Sprintf("phase=%s ready=%t containers=%v", safePodPhase(pod.Status.Phase), podIsReady(*pod), containers)
}

func safePodPhase(phase corev1.PodPhase) string {
	switch phase {
	case corev1.PodPending, corev1.PodRunning, corev1.PodSucceeded, corev1.PodFailed, corev1.PodUnknown:
		return string(phase)
	default:
		return "other"
	}
}

func containerDiagnosticState(status corev1.ContainerStatus) (string, string) {
	switch {
	case status.State.Waiting != nil:
		return "waiting", safeContainerReason(status.State.Waiting.Reason)
	case status.State.Running != nil:
		return "running", "none"
	case status.State.Terminated != nil:
		return "terminated", safeContainerReason(status.State.Terminated.Reason)
	default:
		return "unknown", "unspecified"
	}
}

func safeContainerReason(reason string) string {
	switch reason {
	case "ContainerCreating", "PodInitializing", "ErrImagePull", "ImagePullBackOff", "ImageInspectError",
		"ErrImageNeverPull", "CreateContainerConfigError", "CreateContainerError", "CrashLoopBackOff",
		"RunContainerError", "InvalidImageName", "ContainerCannotRun", "OOMKilled", "Error", "Completed", "StartError":
		return reason
	case "":
		return "unspecified"
	default:
		return "other"
	}
}

// discoverCollectorOTLPHTTPHost finds the metrics collector's
// operator-managed Service exposing the OTLP HTTP receiver (port 4318) and
// returns its in-cluster DNS host. The instance label pins the discovery to
// the data-science-collector CR so co-located collectors (e.g. the usage
// logs collector, which also listens on 4318) cannot be picked.
func (tc *MonitoringTestCtx) discoverCollectorOTLPHTTPHost(t *testing.T) string {
	t.Helper()
	tc = tc.WithT(t)

	serviceList := &corev1.ServiceList{}
	err := tc.Client().List(tc.Context(), serviceList,
		client.InNamespace(tc.MonitoringNamespace),
		client.MatchingLabels{
			"app.kubernetes.io/managed-by": "opentelemetry-operator",
			"app.kubernetes.io/instance":   tc.MonitoringNamespace + ".data-science-collector",
		},
	)
	require.NoError(t, err, "failed to list collector services in namespace %s", tc.MonitoringNamespace)

	for i := range serviceList.Items {
		if serviceList.Items[i].Name != OpenTelemetryCollectorServiceName+"-headless" ||
			serviceList.Items[i].Annotations["service.beta.openshift.io/serving-cert-secret-name"] != "data-science-collector-collector-headless-tls" {
			continue
		}
		for _, port := range serviceList.Items[i].Spec.Ports {
			if port.Port == 4318 {
				return fmt.Sprintf("%s.%s.svc:4318", serviceList.Items[i].Name, tc.MonitoringNamespace)
			}
		}
	}
	t.Fatalf("no data-science-collector service exposing the OTLP HTTP receiver port 4318 found in namespace %s", tc.MonitoringNamespace)
	return ""
}

// ensureTenantMetricsEnvironment creates the tenant workload, its
// ServiceMonitor, and the tenant viewer identity, cleaning everything up
// afterwards. Namespaces are deleted first so reruns start from scratch.
func (tc *MonitoringTestCtx) ensureTenantMetricsEnvironment(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	for _, namespace := range []string{tenantMetricsNamespace, tenantMetricsForeignNamespace} {
		tc.DeleteResource(
			WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: namespace}),
			WithIgnoreNotFound(true),
			WithWaitForDeletion(true),
		)
	}

	t.Cleanup(func() {
		tc.DeleteResource(
			WithMinimalObject(gvk.ServiceMonitorCoreos, types.NamespacedName{Name: tenantMetricsWorkloadName, Namespace: tenantMetricsNamespace}),
			WithIgnoreNotFound(true),
			WithWaitForDeletion(true),
		)
		tc.DeleteResource(
			WithMinimalObject(gvk.Deployment, types.NamespacedName{Name: tenantMetricsWorkloadName, Namespace: tenantMetricsNamespace}),
			WithIgnoreNotFound(true),
			WithWaitForDeletion(true),
		)
		tc.DeleteResource(
			WithMinimalObject(gvk.Service, types.NamespacedName{Name: tenantMetricsWorkloadName, Namespace: tenantMetricsNamespace}),
			WithIgnoreNotFound(true),
			WithWaitForDeletion(true),
		)
		for _, namespace := range []string{tenantMetricsNamespace, tenantMetricsForeignNamespace} {
			tc.DeleteResource(
				WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: namespace}),
				WithIgnoreNotFound(true),
				WithWaitForDeletion(true),
			)
		}
	})

	// The monitoring label opts the namespace in: the admission webhook
	// injects monitoring.opendatahub.io/scrape=true into monitors created
	// here. The ServiceMonitor below also carries the label explicitly so
	// discovery does not depend on the webhook.
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: tenantMetricsNamespace}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.SetLabels(map[string]string{ODHLabelMonitoring: "true"})
			return nil
		}),
	)

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: tenantMetricsForeignNamespace}),
	)

	// The Prometheus image's /prometheus TSDB directory is owned by the image
	// user, which the random UID of OpenShift's restricted SCCs cannot write.
	// Mount an emptyDir there and pin the restricted security context, like
	// every other workload fixture in this suite.
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{
			Name:      tenantMetricsWorkloadName,
			Namespace: tenantMetricsNamespace,
		}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.Object["spec"] = map[string]any{
				"replicas": int64(1),
				"selector": map[string]any{"matchLabels": map[string]any{"app": tenantMetricsWorkloadName}},
				"template": map[string]any{
					"metadata": map[string]any{"labels": map[string]any{"app": tenantMetricsWorkloadName}},
					"spec": map[string]any{
						"securityContext": map[string]any{
							"runAsNonRoot":   true,
							"seccompProfile": map[string]any{"type": "RuntimeDefault"},
						},
						"containers": []any{map[string]any{
							"name":  tenantMetricsWorkloadName,
							"image": tenantMetricsImage,
							"ports": []any{map[string]any{
								"name":          "metrics",
								"containerPort": int64(9090),
								"protocol":      "TCP",
							}},
							"resources": map[string]any{
								"requests": map[string]any{"cpu": "50m", "memory": "128Mi"},
								"limits":   map[string]any{"cpu": "200m", "memory": "512Mi"},
							},
							"volumeMounts": []any{map[string]any{
								"name":      "data",
								"mountPath": "/prometheus",
							}},
							"securityContext": map[string]any{
								"allowPrivilegeEscalation": false,
								"capabilities":             map[string]any{"drop": []any{"ALL"}},
							},
						}},
						"volumes": []any{map[string]any{
							"name":     "data",
							"emptyDir": map[string]any{},
						}},
					},
				},
			}
			return nil
		}),
	)

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Service, types.NamespacedName{
			Name:      tenantMetricsWorkloadName,
			Namespace: tenantMetricsNamespace,
		}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.SetLabels(map[string]string{"app": tenantMetricsWorkloadName})
			u.Object["spec"] = map[string]any{
				"selector": map[string]any{"app": tenantMetricsWorkloadName},
				"ports": []any{map[string]any{
					"name":       "metrics",
					"port":       int64(9090),
					"targetPort": int64(9090),
					"protocol":   "TCP",
				}},
			}
			return nil
		}),
	)

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.ServiceMonitorCoreos, types.NamespacedName{
			Name:      tenantMetricsWorkloadName,
			Namespace: tenantMetricsNamespace,
		}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.SetLabels(map[string]string{
				"app":              tenantMetricsWorkloadName,
				ODHLabelMonitoring: "true",
			})
			u.Object["spec"] = map[string]any{
				"selector":  map[string]any{"matchLabels": map[string]any{"app": tenantMetricsWorkloadName}},
				"endpoints": []any{map[string]any{"port": "metrics"}},
			}
			return nil
		}),
	)

	// The tenant viewer may only view its own namespace; the aggregated
	// data-science-metrics-view ClusterRole lets kube-rbac-proxy authorize
	// its proxy queries.
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.ServiceAccount, types.NamespacedName{
			Name:      tenantMetricsServiceAccount,
			Namespace: tenantMetricsNamespace,
		}),
	)

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.RoleBinding, types.NamespacedName{
			Name:      tenantMetricsServiceAccount,
			Namespace: tenantMetricsNamespace,
		}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.Object["roleRef"] = map[string]any{
				"apiGroup": "rbac.authorization.k8s.io",
				"kind":     "ClusterRole",
				"name":     "view",
			}
			u.Object["subjects"] = []any{map[string]any{
				"kind":      "ServiceAccount",
				"name":      tenantMetricsServiceAccount,
				"namespace": tenantMetricsNamespace,
			}}
			return nil
		}),
	)
}

func (tc *MonitoringTestCtx) fetchPrometheusRouteHost(t *testing.T) string {
	t.Helper()
	tc = tc.WithT(t)

	var routeHost string
	tc.g.Eventually(func() error {
		route := tc.FetchResource(WithMinimalObject(gvk.Route, types.NamespacedName{
			Name:      prometheusRouteName,
			Namespace: tc.MonitoringNamespace,
		}))
		host, found, err := unstructured.NestedString(route.Object, "spec", "host")
		if err != nil {
			return err
		}
		if !found || host == "" {
			return fmt.Errorf("route %s has no admitted host yet", prometheusRouteName)
		}
		routeHost = host
		return nil
	}, 5*time.Minute, 5*time.Second).Should(Succeed(), "prometheus route must be admitted")

	return routeHost
}

// requestServiceAccountToken mints a short-lived token so the test exercises
// the proxy as the tenant service account rather than as the admin runner.
func (tc *MonitoringTestCtx) requestServiceAccountToken(t *testing.T, namespace, name string) string {
	t.Helper()
	tc = tc.WithT(t)

	cfg, err := ctrlcfg.GetConfig()
	require.NoError(t, err, "failed to load kubeconfig")

	clientset, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err, "failed to create client-go clientset")

	expirationSeconds := int64(3600)
	tokenRequest, err := clientset.CoreV1().ServiceAccounts(namespace).CreateToken(
		tc.Context(),
		name,
		&authenticationv1.TokenRequest{
			Spec: authenticationv1.TokenRequestSpec{
				ExpirationSeconds: &expirationSeconds,
			},
		},
		metav1.CreateOptions{},
	)
	require.NoError(t, err, "failed to request token for service account %s/%s", namespace, name)
	require.NotEmpty(t, tokenRequest.Status.Token, "token request returned an empty token")

	return tokenRequest.Status.Token
}

// metricsViewerToken mints a TokenRequest token for a service account that is
// authorized to view the given namespace. Proxy queries must not depend on how
// the test runner authenticates to Kubernetes: on CI clusters the kubeconfig
// credential is not accepted by kube-rbac-proxy's TokenReview (constant 401),
// so authorized queries use minted service-account tokens instead — the same
// approach as the Thanos route probes.
func (tc *MonitoringTestCtx) metricsViewerToken(t *testing.T, name, namespace string) string {
	t.Helper()
	tc = tc.WithT(t)

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.ServiceAccount, types.NamespacedName{
			Name:      name,
			Namespace: tc.MonitoringNamespace,
		}),
	)
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.RoleBinding, types.NamespacedName{
			Name:      name,
			Namespace: namespace,
		}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.Object["roleRef"] = map[string]any{
				"apiGroup": "rbac.authorization.k8s.io",
				"kind":     "ClusterRole",
				"name":     "view",
			}
			u.Object["subjects"] = []any{map[string]any{
				"kind":      "ServiceAccount",
				"name":      name,
				"namespace": tc.MonitoringNamespace,
			}}
			return nil
		}),
	)
	t.Cleanup(func() {
		tc.DeleteResource(
			WithMinimalObject(gvk.RoleBinding, types.NamespacedName{Name: name, Namespace: namespace}),
			WithIgnoreNotFound(true),
		)
		tc.DeleteResource(
			WithMinimalObject(gvk.ServiceAccount, types.NamespacedName{Name: name, Namespace: tc.MonitoringNamespace}),
			WithIgnoreNotFound(true),
		)
	})

	return tc.requestServiceAccountToken(t, tc.MonitoringNamespace, name)
}

// queryMetricsViaNamespaceProxy sends a namespace-restricted instant query
// through the data-science-prometheus-route and returns the HTTP status along
// with the parsed response. rootCAs must contain the CA that signed the route
// certificate (see clusterIngressCAPool); it replaces the system pool. A
// non-200 status is returned, not errored, so callers can assert on
// authorization and filtering behavior; the response body is only JSON-parsed
// for successful queries because kube-rbac-proxy authorization failures
// (401/403) return plain-text bodies.
func queryMetricsViaNamespaceProxy(ctx context.Context, rootCAs *x509.CertPool, routeHost, token, namespace, query string) (int, prometheusQueryResponse, error) {
	var parsed prometheusQueryResponse

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+routeHost+"/api/v1/query", nil)
	if err != nil {
		return 0, parsed, err
	}
	params := req.URL.Query()
	params.Set("query", query)
	params.Set("namespace", namespace)
	req.URL.RawQuery = params.Encode()
	req.Header.Set("Authorization", "Bearer "+token)

	httpClient := &http.Client{
		// The route certificate is signed by the cluster ingress CA, which
		// private clusters do not add to the runner's system CA bundle.
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    rootCAs,
		}},
		Timeout: 30 * time.Second,
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, parsed, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return resp.StatusCode, parsed, fmt.Errorf("reading response of query %q: %w", query, err)
	}
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, parsed, nil
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return resp.StatusCode, parsed, fmt.Errorf("query %q returned status %d with %d unparsable bytes: %w",
			query, resp.StatusCode, len(body), err)
	}
	return resp.StatusCode, parsed, nil
}

// startQueriedTLSServer starts a local TLS server whose certificate chains
// to a freshly generated CA, returning the server and a pool trusting that
// CA. It exists to exercise queryMetricsViaNamespaceProxy without a cluster.
func startQueriedTLSServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *x509.CertPool) {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err, "failed to generate CA key")
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "tenant-metrics-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err, "failed to create CA certificate")
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err, "failed to parse CA certificate")

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err, "failed to generate server key")
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	require.NoError(t, err, "failed to create server certificate")

	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{
		MinVersion: tls.VersionTLS12,
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{leafDER},
			PrivateKey:  leafKey,
		}},
	}
	server.StartTLS()
	t.Cleanup(server.Close)

	trustedPool := x509.NewCertPool()
	trustedPool.AddCert(caCert)
	return server, trustedPool
}

// TestQueryMetricsViaNamespaceProxyTLS proves locally that
// queryMetricsViaNamespaceProxy honors the supplied CA pool: a server whose
// certificate chains to the pool must be queried successfully, while a pool
// without the signing CA must fail the TLS handshake instead of silently
// falling back to the system roots.
func TestQueryMetricsViaNamespaceProxyTLS(t *testing.T) {
	server, trustedPool := startQueriedTLSServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","data":{"result":[{"metric":{"namespace":"trusted-ns","pod":"trusted-pod"}}]}}`)
	})

	routeHost := strings.TrimPrefix(server.URL, "https://")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	status, parsed, err := queryMetricsViaNamespaceProxy(ctx, trustedPool, routeHost, "token", "trusted-ns", "up")
	require.NoError(t, err, "query against server trusted by the supplied pool must succeed")
	require.Equal(t, http.StatusOK, status)
	require.Len(t, parsed.Data.Result, 1)
	require.Equal(t, "trusted-ns", parsed.Data.Result[0].Metric["namespace"])

	// A non-nil pool without the signing CA must not fall back to the system
	// roots: the handshake has to fail instead.
	untrustedPool := x509.NewCertPool()
	_, _, err = queryMetricsViaNamespaceProxy(ctx, untrustedPool, routeHost, "token", "trusted-ns", "up")
	require.Error(t, err, "query against a server whose CA is absent from the pool must fail the TLS handshake")
}

// TestQueryMetricsViaNamespaceProxyMalformedBody proves locally that JSON
// decode failures produce a diagnostic error carrying the HTTP status, the
// body size and the decode error — but never the response body itself, which
// the proxy does not control and which may contain sensitive content.
func TestQueryMetricsViaNamespaceProxyMalformedBody(t *testing.T) {
	const sentinel = "SECRET-SENSITIVE-BODY-CONTENT"
	server, trustedPool := startQueriedTLSServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, "not json: "+sentinel)
	})

	routeHost := strings.TrimPrefix(server.URL, "https://")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	status, _, err := queryMetricsViaNamespaceProxy(ctx, trustedPool, routeHost, "token", "any-ns", "up")
	require.Error(t, err, "a 200 response with an unparsable body must produce a decode error")
	require.Equal(t, http.StatusOK, status)
	require.Contains(t, err.Error(), "status 200", "the error must carry the HTTP status")
	require.Contains(t, err.Error(), "unparsable bytes", "the error must carry the body size")
	require.NotContains(t, err.Error(), sentinel, "the error must not embed the response body")
}
