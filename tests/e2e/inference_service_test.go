package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"
)

const (
	inferenceServiceName            = "facebook-opt-125m-isvc"
	inferenceServiceNamespace       = "default"
	inferenceServiceRuntime         = "csr-kserve-vllm-cpu-x86"
	inferenceServiceModel           = inferenceServiceName
	inferenceServiceTracingEndpoint = "http://data-science-collector-collector.redhat-ods-monitoring.svc.cluster.local:4317"
	inferenceServiceTraceparent     = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	inferenceServiceOATSCase        = "isvc"
)

var clusterServingRuntimeGVK = schema.GroupVersionKind{
	Group:   "serving.kserve.io",
	Version: "v1alpha1",
	Kind:    "ClusterServingRuntime",
}

func TestInferenceServiceTracing(t *testing.T) {
	tc, err := NewTestContext(t)
	require.NoError(t, err)

	tc.DefaultResourceOpts = []ResourceOpts{
		WithEventuallyTimeout(5 * time.Minute),
		WithEventuallyPollingInterval(2 * time.Second),
	}

	projectRoot, err := findProjectRoot()
	require.NoError(t, err)

	setupInferencePrerequisites(t, tc, projectRoot)
	require.NoError(t, ensureCRDExists(tc.Context(), tc, "inferenceservices.serving.kserve.io"), "KServe InferenceService CRD must be installed")
	requireStandardVLLMCPURuntime(t, tc)

	oatsBin := ensureOatsBinary(t, projectRoot)
	oatsEnv := buildOatsEnv(t, tc)
	g := gomega.NewWithT(t)

	serviceNN := types.NamespacedName{Name: inferenceServiceName, Namespace: inferenceServiceNamespace}
	proxyName := inferenceServiceName + "-p"
	proxyNN := types.NamespacedName{Name: proxyName, Namespace: inferenceServiceNamespace}
	tlsSecretName := proxyName + "-tls"
	cookieSecretName := proxyName + "-cookie"
	bindingName := proxyName + "-auth-delegator"
	serviceAccountName := proxyName
	cleanupResources := []struct {
		gvk  schema.GroupVersionKind
		name string
		ns   string
	}{
		{gvk.Route, proxyName, inferenceServiceNamespace},
		{gvk.Deployment, proxyName, inferenceServiceNamespace},
		{gvk.Service, proxyName, inferenceServiceNamespace},
		{gvk.Secret, tlsSecretName, inferenceServiceNamespace},
		{gvk.Secret, cookieSecretName, inferenceServiceNamespace},
		{gvk.ClusterRoleBinding, bindingName, ""},
		{gvk.ServiceAccount, serviceAccountName, inferenceServiceNamespace},
		{gvk.InferenceService, inferenceServiceName, inferenceServiceNamespace},
	}
	// Register cleanup before creating anything so failures do not leave the
	// stable, test-specific ISVC or OAuth proxy resources behind.
	t.Cleanup(func() {
		for _, resource := range cleanupResources {
			tc.DeleteResource(
				WithMinimalObject(resource.gvk, types.NamespacedName{Name: resource.name, Namespace: resource.ns}),
				WithIgnoreNotFound(true),
				WithWaitForDeletion(true),
			)
		}
	})

	t.Logf("Creating traced InferenceService %s/%s", serviceNN.Namespace, serviceNN.Name)
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.InferenceService, serviceNN),
		WithMutateFunc(func(service *unstructured.Unstructured) error {
			service.Object["spec"] = map[string]any{
				"predictor": map[string]any{
					"model": map[string]any{
						"modelFormat": map[string]any{"name": "vLLM"},
						"runtime":     inferenceServiceRuntime,
						"storageUri":  "hf://facebook/opt-125m",
						"env": []any{
							map[string]any{"name": "VLLM_CPU_KVCACHE_SPACE", "value": "1"},
						},
						"resources": map[string]any{
							"requests": map[string]any{"memory": "4Gi"},
							"limits":   map[string]any{"memory": "4Gi"},
						},
					},
				},
				"tracing": map[string]any{
					"exporterEndpoint": inferenceServiceTracingEndpoint,
					"sampler":          "always_on",
				},
			}
			return nil
		}),
	)

	persistedService := tc.FetchResource(WithMinimalObject(gvk.InferenceService, serviceNN))
	tracingEndpoint, tracingEndpointFound, err := unstructured.NestedString(persistedService.Object, "spec", "tracing", "exporterEndpoint")
	require.NoError(t, err, "failed to read persisted InferenceService tracing exporter endpoint")
	require.True(t, tracingEndpointFound, "KServe admission dropped spec.tracing.exporterEndpoint even though the CRD schema exposes it; the installed KServe webhook/controller must include InferenceService tracing support")
	require.Equal(t, inferenceServiceTracingEndpoint, tracingEndpoint, "KServe must preserve the configured tracing exporter endpoint")
	tracingSampler, tracingSamplerFound, err := unstructured.NestedString(persistedService.Object, "spec", "tracing", "sampler")
	require.NoError(t, err, "failed to read persisted InferenceService tracing sampler")
	require.True(t, tracingSamplerFound, "the KServe InferenceService CRD must preserve spec.tracing.sampler")
	require.Equal(t, "always_on", tracingSampler, "KServe must preserve the configured tracing sampler")

	t.Logf("Waiting for InferenceService %s/%s to become Ready", serviceNN.Namespace, serviceNN.Name)
	tc.EnsureResourceConditionMet(
		gvk.InferenceService,
		serviceNN,
		"Ready",
		"True",
		WithEventuallyTimeout(10*time.Minute),
	)

	inferenceURL := inferenceServiceURL(t, tc, serviceNN)
	routeURL := createInferenceServiceOAuthProxy(t, tc, serviceNN, proxyNN, proxyName, serviceAccountName, bindingName, tlsSecretName, cookieSecretName, inferenceURL)
	ocToken := getAuthToken(tc)
	require.NotEmpty(t, ocToken, "Kubernetes bearer token is required for the OAuth proxy request")

	requestURL, err := url.JoinPath(routeURL, "v1", "completions")
	require.NoError(t, err, "failed to construct the OpenAI-compatible completions endpoint")
	t.Logf("Sending completion requests to InferenceService OAuth Route %s", requestURL)
	g.Eventually(func() error {
		return sendInferenceServiceCompletion(tc.Context(), requestURL, ocToken, "")
	}, 5*time.Minute, 5*time.Second).Should(gomega.Succeed(), "completion request without traceparent should succeed through the OAuth proxy")
	g.Eventually(func() error {
		return sendInferenceServiceCompletion(tc.Context(), requestURL, ocToken, inferenceServiceTraceparent)
	}, 5*time.Minute, 5*time.Second).Should(gomega.Succeed(), "completion request with traceparent should succeed through the OAuth proxy")

	t.Logf("Running OATS case with dedicated tag %q", inferenceServiceOATSCase)
	g.Eventually(func() error {
		return runOatsCase(tc.Context(), t, oatsBin, inferenceServiceOATSCase, oatsEnv, projectRoot)
	}, 2*time.Minute, 10*time.Second).Should(gomega.Succeed(), "InferenceService tracing OATS checks should succeed")
}

func requireStandardVLLMCPURuntime(t *testing.T, tc *TestContext) {
	t.Helper()

	clusterRuntime := &unstructured.Unstructured{}
	clusterRuntime.SetGroupVersionKind(clusterServingRuntimeGVK)
	err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: inferenceServiceRuntime}, clusterRuntime)
	if k8serr.IsNotFound(err) {
		t.Fatalf("required standard vLLM CPU ClusterServingRuntime %q was not found; install the KServe vLLM CPU runtime before running this test", inferenceServiceRuntime)
	}
	require.NoError(t, err, "failed to check the standard vLLM CPU ClusterServingRuntime")
}

func inferenceServiceURL(t *testing.T, tc *TestContext, nn types.NamespacedName) string {
	t.Helper()

	service := tc.FetchResource(WithMinimalObject(gvk.InferenceService, nn))
	endpoint, found, err := unstructured.NestedString(service.Object, "status", "address", "url")
	require.NoError(t, err, "failed to read InferenceService status.address.url")
	require.True(t, found && endpoint != "", "InferenceService status.address.url should be set when Ready")

	parsed, err := url.Parse(endpoint)
	require.NoError(t, err, "InferenceService status.url should be a valid URL")
	require.Contains(t, []string{"http", "https"}, parsed.Scheme, "InferenceService status.url should use HTTP or HTTPS")
	require.NotEmpty(t, parsed.Host, "InferenceService status.url should include a host")
	return endpoint
}

func createInferenceServiceOAuthProxy(
	t *testing.T,
	tc *TestContext,
	serviceNN, proxyNN types.NamespacedName,
	proxyName, serviceAccountName, bindingName, tlsSecretName, cookieSecretName, inferenceURL string,
) string {
	t.Helper()

	clusterDomain, err := getClusterDomain(tc)
	require.NoError(t, err, "failed to discover cluster domain")
	ocToken := getAuthToken(tc)
	require.NotEmpty(t, ocToken, "Kubernetes bearer token is required for the OAuth proxy")

	inferenceEndpoint, err := url.Parse(inferenceURL)
	require.NoError(t, err, "InferenceService status.url should be a valid upstream URL")
	upstream := inferenceEndpoint.Scheme + "://" + inferenceEndpoint.Host
	routeHost := serviceNN.Name + "." + clusterDomain

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.ServiceAccount, types.NamespacedName{Name: serviceAccountName, Namespace: serviceNN.Namespace}),
		WithMutateFunc(func(serviceAccount *unstructured.Unstructured) error {
			serviceAccount.SetAnnotations(map[string]string{
				"serviceaccounts.openshift.io/oauth-redirecturi." + proxyName: "https://" + routeHost + "/oauth2/callback",
			})
			return nil
		}),
	)
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{Name: bindingName}),
		WithMutateFunc(func(binding *unstructured.Unstructured) error {
			binding.Object["roleRef"] = map[string]any{
				"apiGroup": "rbac.authorization.k8s.io",
				"kind":     "ClusterRole",
				"name":     "system:auth-delegator",
			}
			binding.Object["subjects"] = []any{map[string]any{
				"kind":      "ServiceAccount",
				"name":      serviceAccountName,
				"namespace": serviceNN.Namespace,
			}}
			return nil
		}),
	)

	var sessionSecret [32]byte
	_, err = rand.Read(sessionSecret[:])
	require.NoError(t, err, "failed to generate OAuth proxy session secret")
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Secret, types.NamespacedName{Name: cookieSecretName, Namespace: serviceNN.Namespace}),
		WithMutateFunc(func(secret *unstructured.Unstructured) error {
			secret.Object["type"] = "Opaque"
			secret.Object["stringData"] = map[string]any{
				"session_secret": base64.StdEncoding.EncodeToString(sessionSecret[:]),
			}
			return nil
		}),
	)
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Service, proxyNN),
		WithMutateFunc(func(service *unstructured.Unstructured) error {
			service.SetAnnotations(map[string]string{"service.beta.openshift.io/serving-cert-secret-name": tlsSecretName})
			service.Object["spec"] = map[string]any{
				"selector": map[string]any{"app": proxyName},
				"ports":    []any{map[string]any{"name": "https", "port": int64(8443), "targetPort": int64(8443)}},
			}
			return nil
		}),
	)

	proxyArgs := []any{
		"--provider=openshift", "--https-address=:8443", "--http-address=", "--upstream=" + upstream,
		"--tls-cert=/etc/proxy/tls/tls.crt", "--tls-key=/etc/proxy/tls/tls.key",
		"--cookie-secret-file=/etc/proxy/secrets/session_secret",
		"--openshift-service-account=" + serviceAccountName,
		"--openshift-delegate-urls={\"/\":{}}",
		"--openshift-ca=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
		"--openshift-ca=/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt",
		"--upstream-ca=/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt",
	}
	proxyContainer := map[string]any{
		"name": "oauth-proxy", "image": "quay.io/openshift/origin-oauth-proxy:4.22.0", "args": proxyArgs,
		"ports": []any{map[string]any{"name": "https", "containerPort": int64(8443)}},
		"volumeMounts": []any{
			map[string]any{"name": "tls", "mountPath": "/etc/proxy/tls", "readOnly": true},
			map[string]any{"name": "session-secret", "mountPath": "/etc/proxy/secrets", "readOnly": true},
		},
	}
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Deployment, proxyNN),
		WithMutateFunc(func(deployment *unstructured.Unstructured) error {
			deployment.Object["spec"] = map[string]any{
				"replicas": int64(1),
				"selector": map[string]any{"matchLabels": map[string]any{"app": proxyName}},
				"template": map[string]any{
					"metadata": map[string]any{"labels": map[string]any{"app": proxyName}},
					"spec": map[string]any{
						"serviceAccountName": serviceAccountName,
						"containers":         []any{proxyContainer},
						"volumes": []any{
							map[string]any{"name": "tls", "secret": map[string]any{"secretName": tlsSecretName}},
							map[string]any{"name": "session-secret", "secret": map[string]any{"secretName": cookieSecretName}},
						},
					},
				},
			}
			return nil
		}),
	)
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Route, types.NamespacedName{Name: proxyName, Namespace: serviceNN.Namespace}),
		WithMutateFunc(func(route *unstructured.Unstructured) error {
			route.Object["spec"] = map[string]any{
				"host": routeHost,
				"to":   map[string]any{"kind": "Service", "name": proxyName, "weight": int64(100)},
				"port": map[string]any{"targetPort": "https"},
				"tls":  map[string]any{"termination": "reencrypt", "insecureEdgeTerminationPolicy": "Redirect"},
			}
			return nil
		}),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Secret, types.NamespacedName{Name: tlsSecretName, Namespace: serviceNN.Namespace}),
		WithCondition(jq.Match(`.data["tls.crt"] != null and .data["tls.key"] != null`)),
	)
	tc.EnsureResourceConditionMet(gvk.Deployment, proxyNN, "Available", metav1.ConditionTrue)
	g := gomega.NewWithT(t)
	g.Eventually(func() error {
		route := &unstructured.Unstructured{}
		route.SetGroupVersionKind(gvk.Route)
		if err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: proxyName, Namespace: serviceNN.Namespace}, route); err != nil {
			return err
		}
		ingress, found, err := unstructured.NestedSlice(route.Object, "status", "ingress")
		if err != nil || !found || len(ingress) == 0 {
			return fmt.Errorf("Route %s has not been admitted yet", routeHost)
		}
		entry, ok := ingress[0].(map[string]any)
		if !ok {
			return fmt.Errorf("Route %s admission status is malformed", routeHost)
		}
		conditions, _, _ := unstructured.NestedSlice(entry, "conditions")
		for _, raw := range conditions {
			condition, ok := raw.(map[string]any)
			if ok && condition["type"] == "Admitted" && condition["status"] == "False" {
				return fmt.Errorf("Route %s admission failed: %v", routeHost, condition["message"])
			}
		}
		return nil
	}).Should(gomega.Succeed(), "OAuth proxy Route %s should be admitted", routeHost)

	return "https://" + routeHost
}

func sendInferenceServiceCompletion(ctx context.Context, endpoint, ocToken, traceparent string) error {
	payload := map[string]any{
		"model":       inferenceServiceModel,
		"prompt":      "San Francisco is a",
		"max_tokens":  7,
		"temperature": 0,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode completion request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create completion request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+ocToken)
	req.Header.Set("Content-Type", "application/json")
	if traceparent != "" {
		req.Header.Set("Traceparent", traceparent)
	}

	httpClient := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}},
		Timeout:   2 * time.Minute,
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send completion request to %s: %w", req.URL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		responseBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("completion request to %s failed with status %d (read response body: %w)", req.URL, resp.StatusCode, readErr)
		}
		return fmt.Errorf("completion request to %s failed with status %d: %s", req.URL, resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	return nil
}
