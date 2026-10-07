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
	"strconv"
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

	authorizedToken := serviceAccountToken(t, tc, authorizedName)
	restrictedToken := serviceAccountToken(t, tc, restrictedName)

	anonymous := probeThanosRoute(t, rootCAs, routeHost, "", tc.MonitoringNamespace, thanosRouteProbeQuery)
	t.Logf("thanos route evidence persona=anonymous http_status=%d prometheus_status=%q labels=%v", anonymous.HTTPStatus, anonymous.PrometheusStatus, anonymous.Labels)
	if anonymous.HTTPStatus != http.StatusUnauthorized && anonymous.HTTPStatus != http.StatusForbidden {
		t.Fatalf("unauthenticated Thanos route request must be rejected with 401 or 403, got %d", anonymous.HTTPStatus)
	}

	restricted := probeThanosRoute(t, rootCAs, routeHost, restrictedToken, tc.MonitoringNamespace, thanosRouteProbeQuery)
	t.Logf("thanos route evidence persona=restricted http_status=%d prometheus_status=%q labels=%v", restricted.HTTPStatus, restricted.PrometheusStatus, restricted.Labels)
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
	t.Logf("thanos route evidence persona=authorized http_status=%d prometheus_status=%q labels=%v", authorized.HTTPStatus, authorized.PrometheusStatus, authorized.Labels)
	for _, labels := range authorized.Labels {
		if labels["namespace"] != tc.MonitoringNamespace {
			t.Fatalf("authorized response returned a series outside %q: labels=%v", tc.MonitoringNamespace, labels)
		}
	}

	crafted := probeThanosRoute(t, rootCAs, routeHost, authorizedToken, tc.MonitoringNamespace, `up{namespace="kube-system"}`)
	t.Logf("thanos route evidence persona=authorized-crafted-selector http_status=%d prometheus_status=%q labels=%v", crafted.HTTPStatus, crafted.PrometheusStatus, crafted.Labels)
	if crafted.HTTPStatus != http.StatusOK || crafted.PrometheusStatus != "success" {
		t.Fatalf("authorized crafted-selector query should remain a successful Prometheus request, got http_status=%d prometheus_status=%q", crafted.HTTPStatus, crafted.PrometheusStatus)
	}
	for _, labels := range crafted.Labels {
		if labels["namespace"] != tc.MonitoringNamespace {
			t.Fatalf("crafted selector escaped the authorized namespace boundary: labels=%v", labels)
		}
	}
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
