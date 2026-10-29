package e2e_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
)

const (
	inferenceServiceRuntime               = "csr-kserve-vllm-cpu-x86"
	inferenceServiceOATSCase              = "isvc"
	maxInferenceServiceHTTPErrorBodyBytes = 1 << 20
)

var clusterServingRuntimeGVK = schema.GroupVersionKind{
	Group:   "serving.kserve.io",
	Version: "v1alpha1",
	Kind:    "ClusterServingRuntime",
}

func inferenceServiceTracingEndpoint() string {
	return fmt.Sprintf("http://%s.%s.svc.cluster.local:4317", OpenTelemetryCollectorServiceName, inferenceMonitoringNamespace())
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

	t.Run("explicitExporterEndpoint", func(t *testing.T) {
		runInferenceServiceTracingScenario(t, tc, gomega.NewWithT(t), oatsBin, oatsEnv, projectRoot)
	})
}

func runInferenceServiceTracingScenario(
	t *testing.T,
	tc *TestContext,
	g *gomega.WithT,
	oatsBin string,
	oatsEnv []string,
	projectRoot string,
) {
	t.Helper()

	serviceName, err := randomServiceName("vllm-trace-isvc")
	require.NoError(t, err, "failed to generate per-run InferenceService name")
	workloadNamespace, err := randomServiceName("vllm-trace")
	require.NoError(t, err, "failed to generate per-run workload namespace")
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: workloadNamespace}),
	)
	t.Cleanup(func() {
		tc.DeleteResource(
			WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: workloadNamespace}),
			WithIgnoreNotFound(true),
			WithWaitForDeletion(true),
		)
	})
	ensureInferenceWorkloadNamespaceScrape(t, tc, workloadNamespace)
	serviceNN := types.NamespacedName{Name: serviceName, Namespace: workloadNamespace}
	proxyName := serviceName + "-p"
	tlsSecretName := proxyName + "-tls"
	cookieSecretName := proxyName + "-cookie"
	bindingName := proxyName + "-auth-delegator"
	serviceAccountName := proxyName
	cleanupResources := []struct {
		gvk  schema.GroupVersionKind
		name string
		ns   string
	}{
		{gvk.Route, proxyName, workloadNamespace},
		{gvk.Deployment, proxyName, workloadNamespace},
		{gvk.Service, proxyName, workloadNamespace},
		{gvk.Secret, tlsSecretName, workloadNamespace},
		{gvk.Secret, cookieSecretName, workloadNamespace},
		{gvk.ClusterRoleBinding, bindingName, ""},
		{gvk.ServiceAccount, serviceAccountName, workloadNamespace},
		{gvk.InferenceService, serviceName, workloadNamespace},
	}
	t.Cleanup(func() {
		for _, resource := range cleanupResources {
			tc.DeleteResource(
				WithMinimalObject(resource.gvk, types.NamespacedName{Name: resource.name, Namespace: resource.ns}),
				WithIgnoreNotFound(true),
				WithWaitForDeletion(true),
			)
		}
	})

	platformEndpoint := inferenceServiceTracingEndpoint()
	t.Logf("Creating traced InferenceService %s/%s", serviceNN.Namespace, serviceNN.Name)
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.InferenceService, serviceNN),
		WithMutateFunc(func(service *unstructured.Unstructured) error {
			spec := map[string]any{
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
			}
			spec["tracing"] = map[string]any{
				"exporterEndpoint": platformEndpoint,
				"sampler":          "always_on",
			}
			service.Object["spec"] = spec
			return nil
		}),
	)

	persistedService := tc.FetchResource(WithMinimalObject(gvk.InferenceService, serviceNN))
	tracingEndpoint, tracingEndpointFound, err := unstructured.NestedString(persistedService.Object, "spec", "tracing", "exporterEndpoint")
	require.NoError(t, err, "failed to read persisted InferenceService tracing exporter endpoint")
	require.True(t, tracingEndpointFound,
		"KServe admission dropped spec.tracing.exporterEndpoint even though the CRD schema exposes it; "+
			"the installed KServe webhook/controller must include InferenceService tracing support")
	require.Equal(t, platformEndpoint, tracingEndpoint, "KServe must preserve the configured tracing exporter endpoint")
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
	clusterDomain, err := getClusterDomain(tc)
	require.NoError(t, err, "failed to discover cluster domain")
	inferenceEndpoint, err := url.Parse(inferenceURL)
	require.NoError(t, err, "InferenceService status.url should be a valid upstream URL")
	routeURL := createOAuthProxy(t, tc, oauthProxyConfig{
		namespace:          serviceNN.Namespace,
		proxyName:          proxyName,
		serviceAccountName: serviceAccountName,
		bindingName:        bindingName,
		tlsSecretName:      tlsSecretName,
		cookieSecretName:   cookieSecretName,
		routeHost:          serviceNN.Name + "." + clusterDomain,
		upstream:           inferenceEndpoint.Scheme + "://" + inferenceEndpoint.Host,
	})
	ocToken := getAuthToken(tc)
	require.NotEmpty(t, ocToken, "Kubernetes bearer token is required for the OAuth proxy request")

	requestURL, err := url.JoinPath(routeURL, "v1", "completions")
	require.NoError(t, err, "failed to construct the OpenAI-compatible completions endpoint")
	t.Logf("Sending completion requests to InferenceService OAuth Route %s", requestURL)
	routeCAs := ingressCAPool(t, tc.Context(), tc.Client())
	g.Eventually(func() error {
		return sendInferenceServiceCompletion(tc.Context(), requestURL, ocToken, serviceName, "", routeCAs)
	}, 5*time.Minute, 5*time.Second).Should(gomega.Succeed(), "completion request without traceparent should succeed through the OAuth proxy")
	traceparent, err := newTestTraceparent()
	require.NoError(t, err, "failed to generate per-run trace context")
	traceID, err := traceIDFromTraceparent(traceparent)
	require.NoError(t, err, "failed to parse trace ID from traceparent")
	parentSpanID, err := parentSpanIDFromTraceparent(traceparent)
	require.NoError(t, err, "failed to parse parent span ID from traceparent")
	t.Logf("Sending traced completion with trace ID %s", traceID)
	g.Eventually(func() error {
		return sendInferenceServiceCompletion(tc.Context(), requestURL, ocToken, serviceName, traceparent, routeCAs)
	}, 5*time.Minute, 5*time.Second).Should(gomega.Succeed(), "completion request with traceparent should succeed through the OAuth proxy")

	t.Logf("Running OATS case with dedicated tag %q and trace ID %s", inferenceServiceOATSCase, traceID)
	casePath := writeInferenceServiceOatsTraceCaseFile(t, inferenceServiceOATSCase, serviceName, traceID)
	g.Eventually(func() error {
		return runOatsCaseFile(tc.Context(), t, oatsBin, casePath, oatsEnv, projectRoot)
	}, 5*time.Minute, 15*time.Second).Should(gomega.Succeed(), "InferenceService tracing OATS checks should succeed")

	// OATS prepares the platform-appropriate gcx binary before these direct
	// trace inspections run.
	g.Eventually(func() error {
		return assertInferenceServiceTraceIDInTempo(tc.Context(), t, traceID, oatsEnv)
	}, 5*time.Minute, 15*time.Second).Should(gomega.Succeed(), "injected trace ID should be retrievable from Tempo via gcx")
	g.Eventually(func() error {
		return assertVLLMSpanHasInjectedParent(tc.Context(), traceID, parentSpanID, oatsEnv)
	}, 5*time.Minute, 15*time.Second).Should(gomega.Succeed(), "vLLM llm_request span should retain the injected parent span ID")

	t.Log("Verifying traced OTLP export succeeds under collector NetworkPolicy enforcement")
	runInferenceServiceOTLPNetworkPolicyProbe(t, tc, serviceNN.Namespace, serviceNN.Name)
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

func sendInferenceServiceCompletion(ctx context.Context, endpoint, ocToken, model, traceparent string, rootCAs *x509.CertPool) error {
	payload := map[string]any{
		"model":       model,
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
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    rootCAs,
		}},
		Timeout: 2 * time.Minute,
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send completion request to %s: %w", req.URL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, maxInferenceServiceHTTPErrorBodyBytes))
		if readErr != nil {
			return fmt.Errorf("completion request to %s failed with status %d (read response body: %w)", req.URL, resp.StatusCode, readErr)
		}
		return fmt.Errorf("completion request to %s failed with status %d", req.URL, resp.StatusCode)
	}
	return nil
}
