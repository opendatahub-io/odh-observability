package e2e_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcfg "sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

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

	// otlpIdentitySpoofPodName names the one-shot curl pod in the monitoring
	// namespace that pushes an OTLP metric with forged identity attributes.
	otlpIdentitySpoofPodName = "otlp-identity-spoof-probe"
	// otlpIdentitySpoofedPodName is the fake pod identity the probe forges.
	otlpIdentitySpoofedPodName = "totally-not-the-sender-pod"
	// otlpIdentitySpoofMetricPrefix prefixes the pushed metric name; a unique
	// suffix per run keeps stale series from earlier runs irrelevant.
	otlpIdentitySpoofMetricPrefix = "odh_observability_otlp_identity_probe"
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
				return fmt.Errorf("series %v has namespace=%q, want %q", result.Metric, result.Metric["namespace"], tenantMetricsNamespace)
			}
			if result.Metric["pod"] == "" {
				return fmt.Errorf("series %v has no promoted pod label", result.Metric)
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
	require.Empty(t, parsed.Data.Result,
		"viewer query for namespace %s must return no tenant series", tenantMetricsForeignNamespace)
}

// ValidateOTLPMetricIdentityCannotBeSpoofed proves the OTLP trust boundary:
// a workload in the monitoring namespace (the only namespace the
// NetworkPolicy lets push telemetry) sends a uniquely named metric whose
// resource attributes claim a foreign tenant namespace/pod identity, in both
// dotted and underscored spellings, plus a forged pod-IP association hint.
// The series must still arrive — attributed to the sender's real pod in the
// monitoring namespace — and the tenant's authorized query for its own
// namespace must never see the injected series. Absence of the series does
// not count as success: the delivery check runs first.
func (tc *MonitoringTestCtx) ValidateOTLPMetricIdentityCannotBeSpoofed(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.setupMetrics(t)
	tc.ensureTenantMetricsEnvironment(t)

	otlpHost := tc.discoverCollectorOTLPHTTPHost(t)

	// Unique metric name and timestamp per run so stale series from earlier
	// runs can never satisfy either assertion.
	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	metricName := otlpIdentitySpoofMetricPrefix + "_" + runID

	// Forged identity, in every spelling the promotion chain could pick up:
	// the dotted semconv attributes, the underscored labels the exporter's
	// resource_to_telemetry_conversion emits, a bogus association hint, and
	// the datapoint-level bare and exported_-prefixed identity labels.
	payload := `{"resourceMetrics":[{"resource":{"attributes":[` +
		`{"key":"k8s.namespace.name","value":{"stringValue":"` + tenantMetricsNamespace + `"}},` +
		`{"key":"k8s.pod.name","value":{"stringValue":"` + otlpIdentitySpoofedPodName + `"}},` +
		`{"key":"k8s.pod.ip","value":{"stringValue":"192.0.2.1"}},` +
		`{"key":"k8s_namespace_name","value":{"stringValue":"` + tenantMetricsNamespace + `"}},` +
		`{"key":"k8s_pod_name","value":{"stringValue":"` + otlpIdentitySpoofedPodName + `"}},` +
		`{"key":"exported_namespace","value":{"stringValue":"` + tenantMetricsNamespace + `"}},` +
		`{"key":"exported_pod","value":{"stringValue":"` + otlpIdentitySpoofedPodName + `"}}]},` +
		`"scopeMetrics":[{"scope":{"name":"odh-observability-e2e-spoof-probe"},` +
		`"metrics":[{"name":"` + metricName + `","gauge":{"dataPoints":[` +
		`{"timeUnixNano":"` + runID + `","asDouble":1,"attributes":[` +
		`{"key":"namespace","value":{"stringValue":"` + tenantMetricsNamespace + `"}},` +
		`{"key":"pod","value":{"stringValue":"` + otlpIdentitySpoofedPodName + `"}},` +
		`{"key":"exported_namespace","value":{"stringValue":"` + tenantMetricsNamespace + `"}},` +
		`{"key":"exported_pod","value":{"stringValue":"` + otlpIdentitySpoofedPodName + `"}}]}]}}]}]}]}`

	// The probe pod itself lives in the monitoring namespace: that is exactly
	// the NetworkPolicy's trust boundary for OTLP pushers, so this test
	// covers the strongest attacker the deployment model allows.
	tc.deleteFixtureResources(otlpIdentitySpoofPodName)
	t.Cleanup(func() {
		tc.deleteFixtureResources(otlpIdentitySpoofPodName)
	})
	script := fmt.Sprintf(
		`i=0; until curl -fsS -X POST -H 'Content-Type: application/json' -d '%s' 'http://%s/v1/metrics'; do i=$((i+1)); [ "$i" -lt 60 ] || exit 1; sleep 2; done`,
		payload, otlpHost,
	)
	tc.createFixtureResource(t, gvk.Pod, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      otlpIdentitySpoofPodName,
			Namespace: tc.MonitoringNamespace,
			Labels:    map[string]string{"app": otlpIdentitySpoofPodName},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:   corev1.RestartPolicyNever,
			SecurityContext: fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name:            "otlp-spoof-probe",
				Image:           fakeGCSClientImage,
				Command:         []string{"/bin/sh", "-c"},
				Args:            []string{script},
				SecurityContext: fixtureContainerSecurityContext(),
			}},
		},
	})
	// Waiting for Succeeded or Failed (rather than only Succeeded) makes a
	// failing probe exit fast instead of burning the full timeout.
	probePod := tc.EnsureResourceExists(
		WithMinimalObject(gvk.Pod, types.NamespacedName{
			Name:      otlpIdentitySpoofPodName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`.status.phase == "Succeeded" or .status.phase == "Failed"`)),
		WithEventuallyTimeout(5*time.Minute),
		WithCustomErrorMsg("OTLP spoof probe pod must finish"),
	)
	// Succeeded means the collector accepted the push (curl -f fails on
	// non-2xx): the transport-level positive control.
	probePhase, found, err := unstructured.NestedString(probePod.Object, "status", "phase")
	require.NoError(t, err, "failed to read the OTLP spoof probe pod phase")
	require.True(t, found, "OTLP spoof probe pod must report a phase")
	require.Equal(t, "Succeeded", probePhase, "OTLP spoof probe pod must push successfully")

	routeHost := tc.fetchPrometheusRouteHost(t)
	rootCAs := clusterIngressCAPool(t, tc)
	viewerToken := tc.metricsViewerToken(t, "otlp-identity-viewer", tc.MonitoringNamespace)
	tenantToken := tc.requestServiceAccountToken(t, tenantMetricsNamespace, tenantMetricsServiceAccount)

	// Delivery control: the pushed series must be visible under the sender's
	// real namespace, with the pod label derived from the actual sender pod —
	// proving connection-based association won over the forged attributes.
	// The exporter expires metrics five minutes after the last push, so the
	// polling window stays well inside that.
	tc.g.Eventually(func() error {
		status, parsed, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, routeHost, viewerToken, tc.MonitoringNamespace, metricName)
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("viewer query via %s returned status %d", routeHost, status)
		}
		if len(parsed.Data.Result) == 0 {
			return fmt.Errorf("no %s series visible in namespace %s yet", metricName, tc.MonitoringNamespace)
		}
		for _, result := range parsed.Data.Result {
			if result.Metric["namespace"] != tc.MonitoringNamespace {
				return fmt.Errorf("series %v has namespace=%q, want the sender's real namespace %q", result.Metric, result.Metric["namespace"], tc.MonitoringNamespace)
			}
			if result.Metric["pod"] != otlpIdentitySpoofPodName {
				return fmt.Errorf("series %v has pod=%q, want the sender's real pod %q", result.Metric, result.Metric["pod"], otlpIdentitySpoofPodName)
			}
			if result.Metric["exported_namespace"] != "" || result.Metric["exported_pod"] != "" {
				return fmt.Errorf("series %v carries forged exported_ identity labels", result.Metric)
			}
		}
		return nil
	}, 4*time.Minute, 10*time.Second).Should(Succeed(),
		fmt.Sprintf("pushed metric %s must be attributed to the sender pod %s in namespace %s", metricName, otlpIdentitySpoofPodName, tc.MonitoringNamespace))

	// Spoof control: the tenant is authorized for its own namespace, so an
	// injected series would be visible to it. It must see none.
	status, parsed, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, routeHost, tenantToken, tenantMetricsNamespace, metricName)
	require.NoError(t, err, "tenant query for its own namespace must reach the proxy")
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, parsed.Data.Result,
		"tenant %s must not see series injected via forged OTLP resource attributes", tenantMetricsNamespace)
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
			WithMinimalObject(gvk.CoreosServiceMonitor, types.NamespacedName{Name: tenantMetricsWorkloadName, Namespace: tenantMetricsNamespace}),
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
		WithMinimalObject(gvk.CoreosServiceMonitor, types.NamespacedName{
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
