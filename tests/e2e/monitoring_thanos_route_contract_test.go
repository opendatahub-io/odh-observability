package e2e_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcfg "sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega
)

const thanosRouteProbeQuery = "up"

type thanosRouteProbe struct {
	HTTPStatus       int
	PrometheusStatus string
	Labels           []map[string]string
}

type prometheusQueryResponse struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Value  []any             `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// ValidateThanosQuerierRouteNamespaceIsolation exercises the route with two
// independent identities. The authorized identity must receive populated data
// for its namespace, while an authenticated identity without that namespace's
// permission must be rejected before it reaches Thanos.
func (tc *MonitoringTestCtx) ValidateThanosQuerierRouteNamespaceIsolation(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)
	tc.waitForPrometheusReady(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{
			Name:      ThanosQuerierProxyName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`.status.readyReplicas == 1`)),
		WithCustomErrorMsg("Thanos Querier proxy should be ready before probing the Route"),
	)

	route := tc.EnsureResourceExists(
		WithMinimalObject(gvk.Route, types.NamespacedName{
			Name:      ThanosQuerierRouteName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`.spec.host != null and .spec.host != "" and ([.status.ingress[]?.conditions[]? | select(.type == "Admitted" and .status == "True")] | length) > 0`)),
		WithCustomErrorMsg("Thanos Querier Route should have an admitted host before probing authorization"),
	)
	routeHost, found, err := unstructured.NestedString(route.Object, "spec", "host")
	if err != nil || !found || routeHost == "" {
		t.Fatalf("Thanos Querier Route host is missing: found=%t error=%v", found, err)
	}
	rootCAs := clusterIngressCAPool(t, tc)

	identitySuffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	authorizedName := "thanos-route-authorized-" + identitySuffix
	restrictedName := "thanos-route-restricted-" + identitySuffix
	restrictedSA := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name:      restrictedName,
		Namespace: tc.MonitoringNamespace,
	}}
	registerProbeCleanup := func(object client.Object) {
		t.Cleanup(func() {
			if err := tc.Client().Delete(tc.Context(), object); err != nil && !k8serr.IsNotFound(err) {
				t.Logf("failed to clean up %T %s: %v", object, object.GetName(), err)
			}
		})
	}
	if err := tc.Client().Create(tc.Context(), restrictedSA); err != nil {
		t.Fatalf("failed to create restricted probe ServiceAccount: %v", err)
	}
	registerProbeCleanup(restrictedSA)
	setupRestrictedProbeAccess(t, tc, restrictedName, identitySuffix, registerProbeCleanup)

	metricsRole := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      authorizedName + "-metrics",
			Namespace: tc.MonitoringNamespace,
		},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{"metrics.k8s.io"},
			Resources: []string{"pods"},
			Verbs:     []string{"get"},
		}},
	}
	if err := tc.Client().Create(tc.Context(), metricsRole); err != nil {
		t.Fatalf("failed to create metrics probe Role: %v", err)
	}
	registerProbeCleanup(metricsRole)
	metricsRoleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      authorizedName + "-metrics",
			Namespace: tc.MonitoringNamespace,
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     metricsRole.Name,
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      authorizedName,
			Namespace: tc.MonitoringNamespace,
		}},
	}
	if err := tc.Client().Create(tc.Context(), metricsRoleBinding); err != nil {
		t.Fatalf("failed to create metrics probe RoleBinding: %v", err)
	}
	registerProbeCleanup(metricsRoleBinding)

	authorizedToken := authorizedThanosProbeToken(t, tc, authorizedName, false)
	restrictedToken := serviceAccountToken(t, tc, restrictedName)

	anonymous := waitForAnonymousThanosRoute(t, rootCAs, routeHost, tc.MonitoringNamespace)
	logThanosRouteEvidence(t, "anonymous", anonymous)
	if anonymous.HTTPStatus != http.StatusUnauthorized && anonymous.HTTPStatus != http.StatusForbidden {
		t.Fatalf("unauthenticated Thanos route request must be rejected with 401 or 403, got %d", anonymous.HTTPStatus)
	}

	restricted := probeThanosRoute(t, rootCAs, routeHost, restrictedToken, tc.MonitoringNamespace, thanosRouteProbeQuery)
	logThanosRouteEvidence(t, "restricted", restricted)
	if restricted.HTTPStatus != http.StatusForbidden {
		t.Fatalf("restricted Thanos route request must be forbidden for an unauthorized namespace, got %d", restricted.HTTPStatus)
	}

	// Datasource contract: Perses injects namespace= via queryParams. Missing it
	// must fail closed (not fall through to unscoped Thanos).
	missingNamespace, err := probeThanosRouteWithError(rootCAs, routeHost, authorizedToken, "", thanosRouteProbeQuery)
	if err != nil {
		t.Fatalf("missing-namespace Thanos route probe failed: %v", err)
	}
	logThanosRouteEvidence(t, "authorized-missing-namespace", missingNamespace)
	if missingNamespace.HTTPStatus == http.StatusOK {
		t.Fatalf("authorized query without namespace= must not succeed, got %d", missingNamespace.HTTPStatus)
	}
	if missingNamespace.HTTPStatus != http.StatusBadRequest && missingNamespace.HTTPStatus != http.StatusForbidden {
		t.Fatalf("authorized query without namespace= must be 400 or 403, got %d", missingNamespace.HTTPStatus)
	}

	var authorized thanosRouteProbe
	t.Log("Waiting for the authorized Thanos route to return namespace-scoped Prometheus data")
	tc.g.Eventually(func() error {
		var probeErr error
		authorized, probeErr = probeThanosRouteWithError(rootCAs, routeHost, authorizedToken, tc.MonitoringNamespace, thanosRouteProbeQuery)
		if probeErr != nil {
			return probeErr
		}
		if authorized.HTTPStatus != http.StatusOK || authorized.PrometheusStatus != "success" {
			return fmt.Errorf("authorized query returned http_status=%d prometheus_status=%q", authorized.HTTPStatus, authorized.PrometheusStatus)
		}
		if len(authorized.Labels) == 0 {
			return errors.New("authorized query returned no metric series")
		}
		return nil
	}).WithTimeout(5*time.Minute).WithPolling(5*time.Second).Should(Succeed(), "authorized Thanos route query should return populated namespace data")
	logThanosRouteEvidence(t, "authorized", authorized)
	assertThanosRouteNamespaceSeries(t, "authorized", authorized.Labels, tc.MonitoringNamespace)

	// Keep the selector broad enough to match the known series returned above;
	// otherwise an empty response would make the namespace-boundary assertion vacuous.
	crafted := probeThanosRoute(t, rootCAs, routeHost, authorizedToken, tc.MonitoringNamespace, `up{namespace=~".*"}`)
	logThanosRouteEvidence(t, "authorized-crafted-selector", crafted)
	if crafted.HTTPStatus != http.StatusOK || crafted.PrometheusStatus != "success" {
		t.Fatalf("authorized crafted-selector query should remain a successful Prometheus request, got http_status=%d prometheus_status=%q", crafted.HTTPStatus, crafted.PrometheusStatus)
	}
	if len(crafted.Labels) == 0 {
		t.Fatalf("authorized crafted-selector query returned no metric series")
	}
	assertThanosRouteNamespaceSeries(t, "crafted selector", crafted.Labels, tc.MonitoringNamespace)

	// Perses sends the namespace in the URL and PromQL in the form body.
	assertAuthorizedThanosPost(t, rootCAs, routeHost, authorizedToken, tc.MonitoringNamespace)
	assertThanosRouteRejectsUnauthorizedNamespaces(t, rootCAs, routeHost, authorizedToken, tc.MonitoringNamespace)
}

func assertThanosRouteRejectsUnauthorizedNamespaces(t *testing.T, rootCAs *x509.CertPool, routeHost, token, namespace string) {
	t.Helper()
	// Repeated URL values are supported, but every namespace must pass the SAR.
	const secondNamespace = "kube-system"
	dualNamespace, err := probeThanosRouteWithNamespaces(
		rootCAs,
		routeHost,
		token,
		[]string{namespace, secondNamespace},
		thanosRouteProbeQuery,
	)
	if err != nil {
		t.Fatalf("dual-namespace GET probe failed: %v", err)
	}
	logThanosRouteEvidence(t, "authorized-dual-namespace-get", dualNamespace)
	for _, labels := range dualNamespace.Labels {
		if labels["namespace"] == secondNamespace {
			t.Fatalf("GET with two namespace query values must not return series from %q", secondNamespace)
		}
	}
	if dualNamespace.HTTPStatus != http.StatusForbidden {
		t.Fatalf("GET including an unauthorized namespace must be forbidden, got http_status=%d", dualNamespace.HTTPStatus)
	}

	// A form-body namespace is not authorized by kube-rbac-proxy and must be
	// rejected before prom-label-proxy can merge it into the label matcher.
	duplicateNamespace, err := probeThanosRoutePostWithError(
		rootCAs,
		routeHost,
		token,
		namespace,
		secondNamespace,
		thanosRouteProbeQuery,
	)
	if err != nil {
		t.Fatalf("duplicate namespace POST probe failed: %v", err)
	}
	logThanosRouteEvidence(t, "authorized-duplicate-namespace-post", duplicateNamespace)
	if duplicateNamespace.HTTPStatus != http.StatusForbidden {
		t.Fatalf("POST with a form namespace must be rejected with 403 by the method gate, got %d", duplicateNamespace.HTTPStatus)
	}
}

func assertAuthorizedThanosPost(t *testing.T, rootCAs *x509.CertPool, routeHost, token, namespace string) {
	t.Helper()
	probe, err := probeThanosRoutePostWithError(rootCAs, routeHost, token, namespace, "", thanosRouteProbeQuery)
	if err != nil {
		t.Fatalf("authorized POST probe failed: %v", err)
	}
	logThanosRouteEvidence(t, "authorized-post", probe)
	if probe.HTTPStatus != http.StatusOK || probe.PrometheusStatus != "success" {
		t.Fatalf("authorized POST query should succeed, got http_status=%d prometheus_status=%q", probe.HTTPStatus, probe.PrometheusStatus)
	}
	if len(probe.Labels) == 0 {
		t.Fatal("authorized POST query returned no metric series")
	}
	assertThanosRouteNamespaceSeries(t, "authorized POST", probe.Labels, namespace)
}

func assertThanosRouteNamespaceSeries(t *testing.T, persona string, series []map[string]string, namespace string) {
	t.Helper()
	for _, labels := range series {
		if labels["namespace"] != namespace {
			t.Fatalf("%s response returned a series outside %q: namespace=%q", persona, namespace, labels["namespace"])
		}
	}
}

func setupRestrictedProbeAccess(t *testing.T, tc *MonitoringTestCtx, restrictedName, identitySuffix string, registerCleanup func(client.Object)) {
	t.Helper()
	restrictedNamespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: "thanos-route-other-" + identitySuffix,
	}}
	createAndRegisterProbeObject(t, tc, restrictedNamespace, "restricted probe namespace", registerCleanup)

	restrictedViewBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restrictedName + "-view",
			Namespace: restrictedNamespace.Name,
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "view",
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      restrictedName,
			Namespace: tc.MonitoringNamespace,
		}},
	}
	createAndRegisterProbeObject(t, tc, restrictedViewBinding, "restricted probe view RoleBinding", registerCleanup)

	restrictedMetricsRole := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restrictedName + "-metrics",
			Namespace: restrictedNamespace.Name,
		},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{"metrics.k8s.io"},
			Resources: []string{"pods"},
			Verbs:     []string{"get"},
		}},
	}
	createAndRegisterProbeObject(t, tc, restrictedMetricsRole, "restricted probe metrics Role", registerCleanup)

	restrictedMetricsBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restrictedName + "-metrics",
			Namespace: restrictedNamespace.Name,
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     restrictedMetricsRole.Name,
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      restrictedName,
			Namespace: tc.MonitoringNamespace,
		}},
	}
	createAndRegisterProbeObject(t, tc, restrictedMetricsBinding, "restricted probe metrics RoleBinding", registerCleanup)
}

func createAndRegisterProbeObject(t *testing.T, tc *MonitoringTestCtx, object client.Object, description string, registerCleanup func(client.Object)) {
	t.Helper()
	if err := tc.Client().Create(tc.Context(), object); err != nil {
		t.Fatalf("failed to create %s: %v", description, err)
	}
	registerCleanup(object)
}

func waitForAnonymousThanosRoute(t *testing.T, rootCAs *x509.CertPool, routeHost, namespace string) thanosRouteProbe {
	t.Helper()
	// A ready proxy and admitted Route can precede the router's backend update.
	// Retry only 503s; any other unexpected response should fail immediately.
	unavailableProbes := 0
	deadline := time.Now().Add(2 * time.Minute)
	var probe thanosRouteProbe
	for {
		probe = probeThanosRoute(t, rootCAs, routeHost, "", namespace, thanosRouteProbeQuery)
		if probe.HTTPStatus != http.StatusServiceUnavailable {
			break
		}
		unavailableProbes++
		if !time.Now().Before(deadline) {
			break
		}
		time.Sleep(5 * time.Second)
	}
	if unavailableProbes > 0 {
		t.Logf("Thanos Route returned 503 for %d anonymous probe(s) before status %d", unavailableProbes, probe.HTTPStatus)
	}
	return probe
}

func logThanosRouteEvidence(t *testing.T, persona string, probe thanosRouteProbe) {
	t.Helper()
	t.Logf(
		"thanos route evidence persona=%s http_status=%d prometheus_status=%q series_count=%d namespaces=%v",
		persona,
		probe.HTTPStatus,
		probe.PrometheusStatus,
		len(probe.Labels),
		namespaceLabels(probe.Labels),
	)
}

func namespaceLabels(series []map[string]string) []string {
	seen := make(map[string]struct{}, len(series))
	for _, labels := range series {
		if namespace := labels["namespace"]; namespace != "" {
			seen[namespace] = struct{}{}
		}
	}

	namespaces := make([]string, 0, len(seen))
	for namespace := range seen {
		namespaces = append(namespaces, namespace)
	}
	slices.Sort(namespaces)
	return namespaces
}

func clusterIngressCAPool(t *testing.T, tc *MonitoringTestCtx) *x509.CertPool {
	t.Helper()
	return ingressCAPool(t, tc.Context(), tc.Client())
}

// authorizedThanosProbeToken creates a namespace-scoped identity for Route
// queries, independent of how the test runner authenticates to Kubernetes.
func authorizedThanosProbeToken(t *testing.T, tc *MonitoringTestCtx, name string, bindView bool) string {
	t.Helper()
	serviceAccount := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: tc.MonitoringNamespace,
	}}
	if err := tc.Client().Create(tc.Context(), serviceAccount); err != nil {
		t.Fatalf("failed to create authorized probe ServiceAccount: %v", err)
	}
	t.Cleanup(func() {
		if err := tc.Client().Delete(tc.Context(), serviceAccount); err != nil && !k8serr.IsNotFound(err) {
			t.Logf("failed to clean up authorized probe ServiceAccount %s: %v", name, err)
		}
	})

	if bindView {
		roleBinding := &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: tc.MonitoringNamespace},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "view",
			},
			Subjects: []rbacv1.Subject{{
				Kind: "ServiceAccount", Name: name, Namespace: tc.MonitoringNamespace,
			}},
		}
		if err := tc.Client().Create(tc.Context(), roleBinding); err != nil {
			t.Fatalf("failed to create authorized probe RoleBinding: %v", err)
		}
		t.Cleanup(func() {
			if err := tc.Client().Delete(tc.Context(), roleBinding); err != nil && !k8serr.IsNotFound(err) {
				t.Logf("failed to clean up authorized probe RoleBinding %s: %v", name, err)
			}
		})
	}
	return serviceAccountToken(t, tc, name)
}

func serviceAccountToken(t *testing.T, tc *MonitoringTestCtx, serviceAccountName string) string {
	t.Helper()
	config, err := ctrlcfg.GetConfig()
	if err != nil {
		t.Fatalf("failed to load Kubernetes config for probe token: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("failed to create Kubernetes client for probe token: %v", err)
	}
	tokenRequest, err := clientset.CoreV1().ServiceAccounts(tc.MonitoringNamespace).CreateToken(
		tc.Context(),
		serviceAccountName,
		&authenticationv1.TokenRequest{},
		metav1.CreateOptions{},
	)
	if err != nil {
		t.Fatalf("failed to create token for probe ServiceAccount %q: %v", serviceAccountName, err)
	}
	if tokenRequest.Status.Token == "" {
		t.Fatalf("TokenRequest for probe ServiceAccount %q returned an empty token", serviceAccountName)
	}
	return tokenRequest.Status.Token
}

func probeThanosRoute(t *testing.T, rootCAs *x509.CertPool, routeHost, token, namespace, promQL string) thanosRouteProbe {
	t.Helper()
	probe, err := probeThanosRouteWithError(rootCAs, routeHost, token, namespace, promQL)
	if err != nil {
		t.Fatalf("Thanos route probe failed: %v", err)
	}
	return probe
}

func probeThanosRouteWithError(rootCAs *x509.CertPool, routeHost, token, namespace, promQL string) (thanosRouteProbe, error) {
	return probeThanosRouteWithNamespaces(rootCAs, routeHost, token, []string{namespace}, promQL)
}

func probeThanosRouteWithNamespaces(rootCAs *x509.CertPool, routeHost, token string, namespaces []string, promQL string) (thanosRouteProbe, error) {
	endpoint := url.URL{
		Scheme: "https",
		Host:   routeHost,
		Path:   "/api/v1/query",
	}
	query := endpoint.Query()
	for _, namespace := range namespaces {
		if namespace != "" {
			query.Add("namespace", namespace)
		}
	}
	query.Set("query", promQL)
	endpoint.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return thanosRouteProbe{}, err
	}
	request.Header.Set("Accept", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	return executeThanosRouteRequest(rootCAs, request)
}

func probeThanosRoutePostWithError(rootCAs *x509.CertPool, routeHost, token, urlNamespace, formNamespace, promQL string) (thanosRouteProbe, error) {
	endpoint := url.URL{
		Scheme: "https",
		Host:   routeHost,
		Path:   "/api/v1/query",
	}
	query := endpoint.Query()
	if urlNamespace != "" {
		query.Set("namespace", urlNamespace)
	}
	endpoint.RawQuery = query.Encode()

	form := url.Values{}
	if formNamespace != "" {
		form.Set("namespace", formNamespace)
	}
	form.Set("query", promQL)
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint.String(), strings.NewReader(form.Encode()))
	if err != nil {
		return thanosRouteProbe{}, err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	return executeThanosRouteRequest(rootCAs, request)
}

func executeThanosRouteRequest(rootCAs *x509.CertPool, request *http.Request) (thanosRouteProbe, error) {
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    rootCAs,
		}},
		Timeout: 30 * time.Second,
	}
	response, err := client.Do(request)
	if err != nil {
		return thanosRouteProbe{}, err
	}
	defer response.Body.Close()

	probe := thanosRouteProbe{HTTPStatus: response.StatusCode}
	if response.StatusCode != http.StatusOK {
		return probe, nil
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return probe, err
	}
	var prometheusResponse prometheusQueryResponse
	if err := json.Unmarshal(body, &prometheusResponse); err != nil {
		return probe, fmt.Errorf("decode Prometheus response: %w", err)
	}
	probe.PrometheusStatus = prometheusResponse.Status
	probe.Labels = make([]map[string]string, 0, len(prometheusResponse.Data.Result))
	for _, result := range prometheusResponse.Data.Result {
		probe.Labels = append(probe.Labels, maps.Clone(result.Metric))
	}
	return probe, nil
}
