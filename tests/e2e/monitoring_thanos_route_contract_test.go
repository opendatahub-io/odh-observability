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

const (
	clusterIngressCANamespace = "openshift-config-managed"
	clusterIngressCAConfigMap = "default-ingress-cert"
	clusterIngressCAKey       = "ca-bundle.crt"
)

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
	t.Cleanup(tc.resetMonitoringConfigToManaged)
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	route := tc.EnsureResourceExists(
		WithMinimalObject(gvk.Route, types.NamespacedName{
			Name:      ThanosQuerierRouteName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`.spec.host != null and .spec.host != "" and (.status.ingress | length) > 0`)),
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
	authorizedSA := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name:      authorizedName,
		Namespace: tc.MonitoringNamespace,
	}}
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
	if err := tc.Client().Create(tc.Context(), authorizedSA); err != nil {
		t.Fatalf("failed to create authorized probe ServiceAccount: %v", err)
	}
	registerProbeCleanup(authorizedSA)
	if err := tc.Client().Create(tc.Context(), restrictedSA); err != nil {
		t.Fatalf("failed to create restricted probe ServiceAccount: %v", err)
	}
	registerProbeCleanup(restrictedSA)
	setupRestrictedProbeAccess(t, tc, restrictedName, identitySuffix, registerProbeCleanup)

	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      authorizedName,
			Namespace: tc.MonitoringNamespace,
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "view",
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      authorizedName,
			Namespace: tc.MonitoringNamespace,
		}},
	}
	if err := tc.Client().Create(tc.Context(), roleBinding); err != nil {
		t.Fatalf("failed to create authorized probe RoleBinding: %v", err)
	}
	registerProbeCleanup(roleBinding)

	postRole := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      authorizedName + "-post",
			Namespace: tc.MonitoringNamespace,
		},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{"metrics.k8s.io"},
			Resources: []string{"pods"},
			Verbs:     []string{"get", "create"},
		}},
	}
	if err := tc.Client().Create(tc.Context(), postRole); err != nil {
		t.Fatalf("failed to create POST probe Role: %v", err)
	}
	registerProbeCleanup(postRole)
	postRoleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      authorizedName + "-post",
			Namespace: tc.MonitoringNamespace,
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     postRole.Name,
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      authorizedName,
			Namespace: tc.MonitoringNamespace,
		}},
	}
	if err := tc.Client().Create(tc.Context(), postRoleBinding); err != nil {
		t.Fatalf("failed to create POST probe RoleBinding: %v", err)
	}
	registerProbeCleanup(postRoleBinding)

	authorizedToken := serviceAccountToken(t, tc, authorizedName)
	restrictedToken := serviceAccountToken(t, tc, restrictedName)

	anonymous := probeThanosRoute(t, rootCAs, routeHost, "", tc.MonitoringNamespace, thanosRouteProbeQuery)
	logThanosRouteEvidence(t, "anonymous", anonymous)
	if anonymous.HTTPStatus != http.StatusUnauthorized && anonymous.HTTPStatus != http.StatusForbidden {
		t.Fatalf("unauthenticated Thanos route request must be rejected with 401 or 403, got %d", anonymous.HTTPStatus)
	}

	restricted := probeThanosRoute(t, rootCAs, routeHost, restrictedToken, tc.MonitoringNamespace, thanosRouteProbeQuery)
	logThanosRouteEvidence(t, "restricted", restricted)
	if restricted.HTTPStatus != http.StatusForbidden {
		t.Fatalf("restricted Thanos route request must be forbidden for an unauthorized namespace, got %d", restricted.HTTPStatus)
	}

	var authorized thanosRouteProbe
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
	}).WithTimeout(3*time.Minute).WithPolling(5*time.Second).Should(Succeed(), "authorized Thanos route query should return populated namespace data")
	logThanosRouteEvidence(t, "authorized", authorized)
	for _, labels := range authorized.Labels {
		if labels["namespace"] != tc.MonitoringNamespace {
			t.Fatalf("authorized response returned a series outside %q: namespace=%q", tc.MonitoringNamespace, labels["namespace"])
		}
	}

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
	for _, labels := range crafted.Labels {
		if labels["namespace"] != tc.MonitoringNamespace {
			t.Fatalf("crafted selector escaped the authorized namespace boundary: namespace=%q", labels["namespace"])
		}
	}

	duplicateNamespace, err := probeThanosRoutePostWithError(
		rootCAs,
		routeHost,
		authorizedToken,
		tc.MonitoringNamespace,
		"kube-system",
		thanosRouteProbeQuery,
	)
	if err != nil {
		t.Fatalf("duplicate namespace POST probe failed: %v", err)
	}
	logThanosRouteEvidence(t, "authorized-duplicate-namespace-post", duplicateNamespace)
	if duplicateNamespace.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("POST with conflicting URL and form namespace values must be rejected, got %d", duplicateNamespace.HTTPStatus)
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
			Verbs:     []string{"get", "create"},
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

	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		rootCAs = x509.NewCertPool()
	}

	configMap := &corev1.ConfigMap{}
	if err := tc.Client().Get(tc.Context(), types.NamespacedName{
		Name:      clusterIngressCAConfigMap,
		Namespace: clusterIngressCANamespace,
	}, configMap); err != nil {
		t.Fatalf("failed to read the cluster ingress CA ConfigMap: %v", err)
	}
	caBundle := configMap.Data[clusterIngressCAKey]
	if caBundle == "" {
		t.Fatalf("cluster ingress CA ConfigMap %q is missing %q", clusterIngressCAConfigMap, clusterIngressCAKey)
	}
	if !rootCAs.AppendCertsFromPEM([]byte(caBundle)) {
		t.Fatalf("cluster ingress CA ConfigMap %q does not contain a valid PEM certificate bundle", clusterIngressCAConfigMap)
	}
	return rootCAs
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
	endpoint := url.URL{
		Scheme: "https",
		Host:   routeHost,
		Path:   "/api/v1/query",
	}
	query := endpoint.Query()
	query.Set("namespace", namespace)
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
	query.Set("namespace", urlNamespace)
	endpoint.RawQuery = query.Encode()

	form := url.Values{}
	form.Set("namespace", formNamespace)
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
