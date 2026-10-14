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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
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

	// Even a privileged query must not see the tenant's series under a
	// foreign namespace: prom-label-proxy enforces the namespace label.
	status, parsed, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, routeHost, getAuthToken(tc.TestContext), tenantMetricsForeignNamespace, tenantMetricsQueryMetric)
	require.NoError(t, err, "privileged query for foreign namespace must reach the proxy")
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, parsed.Data.Result,
		"privileged query for namespace %s must return no tenant series", tenantMetricsForeignNamespace)
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
		return resp.StatusCode, parsed, fmt.Errorf("query %q returned status %d with unparsable body: %s",
			query, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return resp.StatusCode, parsed, nil
}

// TestQueryMetricsViaNamespaceProxyTLS proves locally that
// queryMetricsViaNamespaceProxy honors the supplied CA pool: a server whose
// certificate chains to the pool must be queried successfully, while a pool
// without the signing CA must fail the TLS handshake instead of silently
// falling back to the system roots.
func TestQueryMetricsViaNamespaceProxyTLS(t *testing.T) {
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

	responseBody := `{"status":"success","data":{"result":[{"metric":{"namespace":"trusted-ns","pod":"trusted-pod"}}]}}`
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, responseBody)
	}))
	server.TLS = &tls.Config{
		MinVersion: tls.VersionTLS12,
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{leafDER},
			PrivateKey:  leafKey,
		}},
	}
	server.StartTLS()
	t.Cleanup(server.Close)

	routeHost := strings.TrimPrefix(server.URL, "https://")

	trustedPool := x509.NewCertPool()
	trustedPool.AddCert(caCert)
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
