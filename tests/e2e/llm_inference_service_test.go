package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"
	platformcommon "github.com/opendatahub-io/odh-platform-utilities/api/common"
	"github.com/stretchr/testify/require"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"
)

var (
	gvkLLMInferenceService = schema.GroupVersionKind{
		Group:   "serving.kserve.io",
		Version: "v1alpha2",
		Kind:    "LLMInferenceService",
	}

	// gvk.DSCInitialization is v1; the inference prerequisites use v2.
	gvkDSCInitializationV2 = schema.GroupVersionKind{
		Group:   "dscinitialization.opendatahub.io",
		Version: "v2",
		Kind:    "DSCInitialization",
	}

	gvkCustomResourceDefinition = schema.GroupVersionKind{
		Group:   "apiextensions.k8s.io",
		Version: "v1",
		Kind:    "CustomResourceDefinition",
	}

	// The MaaS Config CR is cluster-scoped, despite what `oc get config.maas.opendatahub.io -A`
	// suggests.
	gvkMaaSConfig = schema.GroupVersionKind{
		Group:   "maas.opendatahub.io",
		Version: "v1alpha1",
		Kind:    "Config",
	}

	gvkEnvoyFilter = schema.GroupVersionKind{
		Group:   "networking.istio.io",
		Version: "v1alpha3",
		Kind:    "EnvoyFilter",
	}
)

func findProjectRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", errors.New("go.mod file not found in parent directories")
}

func ensureOatsBinary(t *testing.T, projectRoot string) string {
	t.Helper()

	if oatsBin, err := exec.LookPath("oats"); err == nil {
		return oatsBin
	}

	localBin := filepath.Join(projectRoot, "bin")
	oatsBin := filepath.Join(localBin, "oats")
	if _, err := os.Stat(oatsBin); err == nil {
		return oatsBin
	}

	t.Logf("Installing OATS into %s...", localBin)
	if err := os.MkdirAll(localBin, 0755); err != nil {
		t.Fatalf("Failed to create bin directory %s: %v", localBin, err)
	}

	cmd := exec.CommandContext(t.Context(), "go", "install", "github.com/grafana/oats@v0.10.0")
	cmd.Dir = projectRoot
	// OATS is a tool binary, not a dependency of this module -- it is in neither go.mod nor
	// vendor/. "go install pkg@version" has to query the proxy and refuses to run under
	// -mod=vendor, which this repo's vendor/ directory and a GOFLAGS=-mod=vendor in the user's
	// go env both select.
	cmd.Env = append(os.Environ(), "GOBIN="+localBin, "GOFLAGS=-mod=mod")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to install OATS: %v\nOutput: %s", err, string(out))
	}

	return oatsBin
}

func buildOatsEnv(t *testing.T, tc *TestContext) []string {
	t.Helper()

	envMap := make(map[string]string)
	for _, envStr := range os.Environ() {
		parts := strings.SplitN(envStr, "=", 2)
		if len(parts) == 2 {
			envMap[parts[0]] = parts[1]
		}
	}

	if envMap["GCX_TELEMETRY"] == "" {
		envMap["GCX_TELEMETRY"] = "disabled"
	}

	if envMap["GRAFANA_ORG_ID"] == "" {
		envMap["GRAFANA_ORG_ID"] = "1"
	}

	if envMap["GRAFANA_SERVER"] == "" {
		route := &unstructured.Unstructured{}
		route.SetGroupVersionKind(gvk.Route)
		err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: "lgtm", Namespace: inferenceMonitoringNamespace()}, route)
		host, _, hostErr := unstructured.NestedString(route.Object, "spec", "host")
		if err == nil && hostErr == nil && host != "" {
			envMap["GRAFANA_SERVER"] = "https://" + host
		}
		if envMap["GRAFANA_SERVER"] == "" {
			t.Log("Warning: GRAFANA_SERVER is not set and could not be resolved from OpenShift route 'lgtm'")
		}
	}

	if envMap["GRAFANA_TOKEN"] == "" {
		if token := getAuthToken(tc); token != "" {
			envMap["GRAFANA_TOKEN"] = token
		}
		if envMap["GRAFANA_TOKEN"] == "" {
			t.Log("Warning: GRAFANA_TOKEN is not set and no Kubernetes bearer token is available")
		}
	}

	envList := make([]string, 0, len(envMap))
	for k, v := range envMap {
		envList = append(envList, fmt.Sprintf("%s=%s", k, v))
	}
	return envList
}

func getClusterDomain(tc *TestContext) (string, error) {
	if domain := os.Getenv("CLUSTER_DOMAIN"); domain != "" {
		return domain, nil
	}

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "Ingress",
	})
	err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: "cluster"}, u)
	if err == nil {
		domain, found, _ := unstructured.NestedString(u.Object, "spec", "domain")
		if found && domain != "" {
			return domain, nil
		}
	}

	return "", errors.New("failed to discover cluster domain")
}

func getAuthToken(tc *TestContext) string {
	if token := os.Getenv("OC_TOKEN"); token != "" {
		return token
	}
	return tc.AuthToken()
}

func sendCompletion(ctx context.Context, routeHost, ocToken string) error {
	payload := map[string]any{
		"model":       "facebook/opt-125m",
		"prompt":      "San Francisco is a",
		"max_tokens":  7,
		"temperature": 0,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	httpClient := &http.Client{
		// ROSA route certificates are trusted by the runner's system CA bundle.
		Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}},
		Timeout:   30 * time.Second,
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://"+routeHost+"/v1/completions", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+ocToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		responseBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("completions request to %s failed with status %d (read response body: %w)", req.URL, resp.StatusCode, readErr)
		}
		return fmt.Errorf("completions request to %s failed with status %d: %s", req.URL, resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	return nil
}

func runOatsCase(ctx context.Context, t *testing.T, oatsBin, topology string, oatsEnv []string, projectRoot string) error {
	t.Helper()

	args := []string{"-vv", "--gcx-download", "auto", "--gcx-version", "v1.2.0", "--tags", topology, "--gcx-context", "default"}
	cmd := exec.CommandContext(ctx, oatsBin, args...)
	cmd.Dir = projectRoot
	cmd.Env = oatsEnv

	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &outBuf

	t.Logf("Running OATS topology %s: %s", topology, formatOatsCommand(oatsEnv, append([]string{oatsBin}, args...)))
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("OATS topology %s failed: %w\nOutput: %s", topology, err, outBuf.String())
	}
	return nil
}

func formatOatsCommand(env []string, args []string) string {
	wanted := map[string]bool{
		"GCX_TELEMETRY":  true,
		"GRAFANA_SERVER": true,
		"GRAFANA_ORG_ID": true,
		"GRAFANA_TOKEN":  true,
	}
	assignments := make([]string, 0, len(wanted))
	for _, entry := range env {
		name, value, found := strings.Cut(entry, "=")
		if !found || !wanted[name] {
			continue
		}
		if name == "GRAFANA_TOKEN" {
			assignments = append(assignments, name+"="+shellQuote("<redacted>"))
			continue
		}

		assignments = append(assignments, name+"="+shellQuote(value))
	}
	command := make([]string, 0, len(assignments)+len(args))
	command = append(command, assignments...)
	for _, arg := range args {
		command = append(command, shellQuote(arg))
	}
	return strings.Join(command, " ")
}

func shellQuote(value string) string {
	if value != "" && !strings.ContainsAny(value, " \t\n'\"\\$`<>|&;*?~#()") {
		return value
	}
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func restartVLLMPod(t *testing.T, tc *TestContext, namespace, llmSvcName string) {
	t.Helper()
	g := gomega.NewWithT(t)

	podSelector := vllmPodSelector(llmSvcName)
	readyPods, err := readyPodsForSelector(tc, namespace, podSelector)
	g.Expect(err).NotTo(gomega.HaveOccurred(), "failed to find a ready LLMInferenceService workload pod")
	targetPod := readyPods[0]
	initialReadyCount := len(readyPods)
	initialPodUIDs := make(map[types.UID]struct{}, initialReadyCount)
	for _, pod := range readyPods {
		initialPodUIDs[pod.GetUID()] = struct{}{}
	}
	targetPodUID := targetPod.GetUID()

	podName := targetPod.GetName()
	t.Logf("Deleting pod %s in namespace %s for scrape recovery test...", podName, namespace)

	err = tc.Client().Delete(tc.Context(), targetPod)
	g.Expect(err).NotTo(gomega.HaveOccurred(), "failed to delete pod %s", podName)

	g.Eventually(func() bool {
		readyPods, findErr := readyPodsForSelector(tc, namespace, podSelector)
		if findErr != nil || len(readyPods) < initialReadyCount {
			return false
		}

		hasNewPod := false
		for _, pod := range readyPods {
			if pod.GetUID() == targetPodUID {
				return false
			}
			if _, existed := initialPodUIDs[pod.GetUID()]; !existed {
				hasNewPod = true
			}
		}
		return hasNewPod
	}, 3*time.Minute, 5*time.Second).Should(gomega.BeTrue(), "New LLMInferenceService workload pod should be recreated and reach Ready state")
}

func vllmPodSelector(llmSvcName string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/part-of": "llminferenceservice",
		"app.kubernetes.io/name":    llmSvcName,
	}
}

func readyPodsForSelector(tc *TestContext, namespace string, selector map[string]string) ([]*unstructured.Unstructured, error) {
	podList := &unstructured.UnstructuredList{}
	podList.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "PodList"})
	if err := tc.Client().List(tc.Context(), podList, client.InNamespace(namespace), client.MatchingLabels(selector)); err != nil {
		return nil, err
	}

	readyPods := make([]*unstructured.Unstructured, 0, len(podList.Items))
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.GetDeletionTimestamp() == nil && isReadyPod(pod) {
			readyPods = append(readyPods, pod)
		}
	}
	if len(readyPods) == 0 {
		return nil, errors.New("no ready pod matched the LLMInferenceService workload selector")
	}
	return readyPods, nil
}

func isReadyPod(pod *unstructured.Unstructured) bool {
	conditions, found, _ := unstructured.NestedSlice(pod.Object, "status", "conditions")
	if !found {
		return false
	}
	for _, condition := range conditions {
		conditionMap, ok := condition.(map[string]any)
		if ok && conditionMap["type"] == "Ready" && conditionMap["status"] == "True" {
			return true
		}
	}
	return false
}

func randomServiceName(base string) (string, error) {
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", fmt.Errorf("generate service name suffix: %w", err)
	}
	name := fmt.Sprintf("%s-%x", base, suffix)
	if len(name) > 63 {
		name = strings.TrimRight(name[:63], "-")
	}
	return name, nil
}

func discoverInferenceService(tc *TestContext, llmSvcName string) (string, int64, error) {
	services := &unstructured.UnstructuredList{}
	services.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ServiceList"})
	listCtx, cancel := context.WithTimeout(tc.Context(), 10*time.Second)
	defer cancel()
	started := time.Now()
	tc.t.Logf("[%s] listing Services in default for owner %q", tc.t.Name(), llmSvcName)
	if err := tc.Client().List(listCtx, services, client.InNamespace("default")); err != nil {
		tc.t.Logf("[%s] Service list for owner %q failed after %s: %v", tc.t.Name(), llmSvcName, time.Since(started).Round(time.Millisecond), err)
		return "", 0, fmt.Errorf("list Services for LLMInferenceService %q: %w", llmSvcName, err)
	}
	tc.t.Logf("[%s] listed %d Services in %s", tc.t.Name(), len(services.Items), time.Since(started).Round(time.Millisecond))

	var candidates []unstructured.Unstructured
	for _, svc := range services.Items {
		owned := false
		for _, ref := range svc.GetOwnerReferences() {
			if ref.Name == llmSvcName && ref.Kind == "LLMInferenceService" {
				owned = true
			}
		}
		labels := svc.GetLabels()
		if owned || labels["serving.kserve.io/inferenceservice"] == llmSvcName || labels["serving.kserve.io/llminferenceservice"] == llmSvcName {
			candidates = append(candidates, svc)
		}
	}
	if len(candidates) != 1 {
		names := make([]string, 0, len(candidates))
		for _, svc := range candidates {
			names = append(names, svc.GetName())
		}
		return "", 0, fmt.Errorf("expected exactly one generated inference Service for %q, found %d (%s)", llmSvcName, len(candidates), strings.Join(names, ", "))
	}

	ports, found, err := unstructured.NestedSlice(candidates[0].Object, "spec", "ports")
	if err != nil || !found || len(ports) == 0 {
		return "", 0, fmt.Errorf("generated inference Service %q has no ports", candidates[0].GetName())
	}
	var selected map[string]any
	for _, raw := range ports {
		port, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		name, _ := port["name"].(string)
		if strings.Contains(strings.ToLower(name), "http") || strings.Contains(strings.ToLower(name), "inference") || strings.Contains(strings.ToLower(name), "openai") {
			if selected != nil {
				return "", 0, fmt.Errorf("inference Service %q has multiple HTTP-like ports", candidates[0].GetName())
			}
			selected = port
		}
	}
	if selected == nil && len(ports) == 1 {
		selected, _ = ports[0].(map[string]any)
	}
	if selected == nil {
		return "", 0, fmt.Errorf("inference Service %q has ambiguous ports; expected one HTTP/inference port", candidates[0].GetName())
	}
	port, ok := selected["port"].(int64)
	if !ok {
		if number, numberOK := selected["port"].(float64); numberOK {
			port = int64(number)
			ok = true
		}
	}
	if !ok || port <= 0 {
		return "", 0, fmt.Errorf("inference Service %q has invalid selected port", candidates[0].GetName())
	}
	return candidates[0].GetName(), port, nil
}

//nolint:maintidx // topology setup intentionally keeps the end-to-end workflow together.
func runLLMInferenceServiceTopologyTest(t *testing.T, tc *TestContext, topology, projectRoot, oatsBin string, oatsEnv []string) {
	t.Helper()
	g := gomega.NewWithT(t)

	llmSvcName, err := randomServiceName("facebook-opt-125m-" + topology)
	require.NoError(t, err, "failed to generate a unique LLMInferenceService name")
	llmSvcNN := types.NamespacedName{Name: llmSvcName, Namespace: "default"}
	proxyName := llmSvcName + "-p"
	proxyNN := types.NamespacedName{Name: proxyName, Namespace: "default"}
	tlsSecretName := proxyName + "-tls"
	cookieSecretName := proxyName + "-cookie"
	saName := proxyName
	bindingName := proxyName + "-auth-delegator"
	noCleanup := os.Getenv("NO_CLEANUP") == "1"
	t.Logf("Starting %s topology with LLMInferenceService %s", topology, llmSvcNN)
	if topology == "multi-node" {
		checkCtx, cancel := context.WithTimeout(tc.Context(), 10*time.Second)
		err := ensureCRDExists(checkCtx, tc, "leaderworkersets.leaderworkerset.x-k8s.io")
		cancel()
		if err != nil {
			t.Fatalf("multi-node LLMInferenceService requires the LeaderWorkerSet CRD/operator: %v", err)
		}
	}

	cleanupResources := []struct {
		gvk  schema.GroupVersionKind
		name string
		ns   string
	}{
		{gvk.Route, proxyName, "default"}, {gvk.Deployment, proxyName, "default"}, {gvk.Service, proxyName, "default"},
		{gvk.Secret, tlsSecretName, "default"}, {gvk.Secret, cookieSecretName, "default"}, {gvk.ClusterRoleBinding, bindingName, ""}, {gvk.ServiceAccount, saName, "default"},
		{gvkLLMInferenceService, llmSvcName, "default"},
	}
	// Register cleanup before creating anything so failures or aborted subtests
	// do not leave topology-specific resources behind unless NO_CLEANUP=1.
	t.Cleanup(func() {
		if noCleanup {
			logManualCleanupCommands(t, topology, llmSvcName, proxyName, tlsSecretName, cookieSecretName, bindingName, saName)
			return
		}
		t.Logf("[%s] Cleaning up topology resources", topology)
		for _, resource := range cleanupResources {
			tc.DeleteResource(WithMinimalObject(resource.gvk, types.NamespacedName{Name: resource.name, Namespace: resource.ns}), WithIgnoreNotFound(true), WithWaitForDeletion(true))
		}
	})

	memory := "4Gi"
	if topology == "multi-node" {
		memory = "8Gi"
	}

	spec := map[string]any{
		"model": map[string]any{
			"uri":  "hf://facebook/opt-125m",
			"name": "facebook/opt-125m",
		},
		"tracing": map[string]any{
			"exporterEndpoint": fmt.Sprintf("http://data-science-collector.%s.svc.cluster.local:4317", inferenceMonitoringNamespace()),
			"sampler":          "always_on",
		},
		"replicas": int64(1),
		"template": map[string]any{
			"containers": []map[string]any{
				{
					"name":  "main",
					"image": "registry.redhat.io/rhaii/vllm-cpu-rhel9:3.4.1-1780356811",
					"env":   []map[string]any{{"name": "VLLM_CPU_KVCACHE_SPACE", "value": "1"}},
					"resources": map[string]any{
						"requests": map[string]any{"memory": memory},
						"limits":   map[string]any{"memory": memory},
					},
				},
			},
		},
	}

	switch topology {
	case "multi-node":
		// A worker requires explicit data or pipeline parallelism.
		// deployment provides the worker data-parallel preset.
		spec["parallelism"] = map[string]any{"data": int64(2), "dataLocal": int64(1)}
		// worker is a PodSpec in serving.kserve.io/v1alpha2; unlike prefill,
		// it does not contain a nested template. Unknown fields are pruned by
		// the API server, so keep the containers directly under worker.
		spec["worker"] = map[string]any{
			"containers": []map[string]any{
				{
					"name":  "main",
					"image": "registry.redhat.io/rhaii/vllm-cpu-rhel9:3.4.1-1780356811",
					"env":   []map[string]any{{"name": "VLLM_CPU_KVCACHE_SPACE", "value": "1"}},
					"resources": map[string]any{
						"requests": map[string]any{"memory": memory},
						"limits":   map[string]any{"memory": memory},
					},
				},
			},
		}
	case "disaggregated":
		spec["prefill"] = map[string]any{
			"template": map[string]any{
				"containers": []map[string]any{
					{
						"name":  "main",
						"image": "registry.redhat.io/rhaii/vllm-cpu-rhel9:3.4.1-1780356811",
						"env":   []map[string]any{{"name": "VLLM_CPU_KVCACHE_SPACE", "value": "1"}},
						"resources": map[string]any{
							"requests": map[string]any{"memory": memory},
							"limits":   map[string]any{"memory": memory},
						},
					},
				},
			},
		}
	}

	// 1. Deployment & Reconciliation
	t.Logf("[%s] Creating or updating LLMInferenceService", topology)
	llmSvcOpts := []ResourceOpts{
		WithMinimalObject(gvkLLMInferenceService, llmSvcNN),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.Object["spec"] = spec
			return nil
		}),
	}
	switch topology {
	case "multi-node":
		// Validate the object returned by the API server, not just the local
		// object passed to Create/Patch. This catches CRD field pruning during
		// create or update instead of failing later as an unexplained readiness
		// timeout.
		llmSvcOpts = append(llmSvcOpts,
			WithCondition(jq.Match(`.spec.worker.containers[0].image == %q`, "registry.redhat.io/rhaii/vllm-cpu-rhel9:3.4.1-1780356811")),
			WithCustomErrorMsg("multi-node LLMInferenceService should retain worker container configuration"),
		)
	case "disaggregated":
		// prefill has its own template, unlike the worker PodSpec above.
		llmSvcOpts = append(llmSvcOpts,
			WithCondition(jq.Match(`.spec.prefill.template.containers[0].image == %q`, "registry.redhat.io/rhaii/vllm-cpu-rhel9:3.4.1-1780356811")),
			WithCustomErrorMsg("disaggregated LLMInferenceService should retain prefill container configuration"),
		)
	}
	tc.EventuallyResourceCreatedOrPatched(llmSvcOpts...)

	// 2. Discover the generated inference Service and expose it through OAuth.
	// The generated Service is created before the workload becomes Ready, so this
	// allows OAuth proxy provisioning to overlap with model startup.
	t.Logf("[%s] Discovering cluster domain and generated inference Service", topology)
	clusterDomain, err := getClusterDomain(tc)
	require.NoError(t, err, "failed to discover cluster domain")
	var inferenceService string
	var inferencePort int64
	g.Eventually(func() error {
		var discoveryErr error
		inferenceService, inferencePort, discoveryErr = discoverInferenceService(tc, llmSvcName)
		return discoveryErr
	}, 2*time.Minute, 5*time.Second).Should(gomega.Succeed(), "generated inference Service should become discoverable")
	t.Logf("[%s] Using inference Service %s on port %d", topology, inferenceService, inferencePort)
	ocToken := getAuthToken(tc)
	require.NotEmpty(t, ocToken, "Kubernetes bearer token is required for the OAuth proxy request")

	routeHost := llmSvcName + "." + clusterDomain

	t.Logf("[%s] Creating OAuth proxy ServiceAccount and delegated-auth RBAC", topology)
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.ServiceAccount, types.NamespacedName{Name: saName, Namespace: "default"}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.SetAnnotations(map[string]string{"serviceaccounts.openshift.io/oauth-redirecturi." + proxyName: "https://" + routeHost + "/oauth2/callback"})
			return nil
		}),
	)
	tc.EventuallyResourceCreatedOrPatched(WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{Name: bindingName}), WithMutateFunc(func(u *unstructured.Unstructured) error {
		u.Object["roleRef"] = map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "system:auth-delegator"}
		u.Object["subjects"] = []any{map[string]any{"kind": "ServiceAccount", "name": saName, "namespace": "default"}}
		return nil
	}))
	t.Logf("[%s] Creating OAuth proxy cookie Secret and service-ca-backed Service", topology)
	var sessionSecret [32]byte
	_, err = rand.Read(sessionSecret[:])
	require.NoError(t, err, "failed to generate OAuth session secret")
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Secret, types.NamespacedName{Name: cookieSecretName, Namespace: "default"}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.Object["type"] = "Opaque"
			u.Object["stringData"] = map[string]any{"session_secret": base64.StdEncoding.EncodeToString(sessionSecret[:])}
			return nil
		}),
	)
	tc.EventuallyResourceCreatedOrPatched(WithMinimalObject(gvk.Service, proxyNN), WithMutateFunc(func(u *unstructured.Unstructured) error {
		u.SetAnnotations(map[string]string{"service.beta.openshift.io/serving-cert-secret-name": tlsSecretName})
		u.Object["spec"] = map[string]any{"selector": map[string]any{"app": proxyName}, "ports": []any{map[string]any{"name": "https", "port": int64(8443), "targetPort": int64(8443)}}}
		return nil
	}))
	t.Logf("[%s] Creating OAuth proxy Deployment with upstream %s", topology, inferenceService)
	upstream := "https://" + inferenceService + ".default.svc.cluster.local:" + strconv.FormatInt(inferencePort, 10)
	proxyArgs := []any{
		"--provider=openshift", "--https-address=:8443", "--http-address=", "--upstream=" + upstream,
		"--tls-cert=/etc/proxy/tls/tls.crt", "--tls-key=/etc/proxy/tls/tls.key",
		"--cookie-secret-file=/etc/proxy/secrets/session_secret",
		"--openshift-service-account=" + saName,
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
	tc.EventuallyResourceCreatedOrPatched(WithMinimalObject(gvk.Deployment, proxyNN), WithMutateFunc(func(u *unstructured.Unstructured) error {
		u.Object["spec"] = map[string]any{
			"replicas": int64(1), "selector": map[string]any{"matchLabels": map[string]any{"app": proxyName}},
			"template": map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": proxyName}}, "spec": map[string]any{
				"serviceAccountName": saName, "containers": []any{proxyContainer},
				"volumes": []any{
					map[string]any{"name": "tls", "secret": map[string]any{"secretName": tlsSecretName}},
					map[string]any{"name": "session-secret", "secret": map[string]any{"secretName": cookieSecretName}},
				},
			}},
		}
		return nil
	}))
	t.Logf("[%s] Creating Route %s", topology, routeHost)
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Route, types.NamespacedName{Name: proxyName, Namespace: "default"}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.Object["spec"] = map[string]any{
				"host": routeHost,
				"to":   map[string]any{"kind": "Service", "name": proxyName, "weight": int64(100)},
				"port": map[string]any{"targetPort": "https"},
				"tls":  map[string]any{"termination": "reencrypt", "insecureEdgeTerminationPolicy": "Redirect"},
			}
			return nil
		}),
	)
	t.Logf("[%s] Waiting for service-ca TLS Secret", topology)
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Secret, types.NamespacedName{Name: tlsSecretName, Namespace: "default"}),
		WithCondition(jq.Match(`.data["tls.crt"] != null and .data["tls.key"] != null`)),
	)
	t.Logf("[%s] Waiting for OAuth proxy Deployment availability", topology)
	tc.EnsureResourceConditionMet(gvk.Deployment, proxyNN, "Available", metav1.ConditionTrue)
	t.Logf("[%s] Waiting for Route admission", topology)
	g.Eventually(func() error {
		route := &unstructured.Unstructured{}
		route.SetGroupVersionKind(gvk.Route)
		if err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: proxyName, Namespace: "default"}, route); err != nil {
			return err
		}
		ingress, found, err := unstructured.NestedSlice(route.Object, "status", "ingress")
		if err != nil || !found || len(ingress) == 0 {
			return errors.New("route has not been admitted yet")
		}
		entry, ok := ingress[0].(map[string]any)
		if !ok {
			return errors.New("route admission status is malformed")
		}
		conditions, _, _ := unstructured.NestedSlice(entry, "conditions")
		for _, raw := range conditions {
			condition, ok := raw.(map[string]any)
			if ok && condition["type"] == "Admitted" && condition["status"] == "False" {
				return fmt.Errorf("route admission failed: %v", condition["message"])
			}
		}
		return nil
	}).Should(gomega.Succeed(), "Route %s should be admitted", routeHost)

	t.Logf("[%s] Waiting for LLMInferenceService Ready=True", topology)
	tc.EnsureResourceConditionMet(
		gvkLLMInferenceService,
		llmSvcNN,
		"Ready",
		metav1.ConditionTrue,
		WithEventuallyTimeout(10*time.Minute),
	)
	t.Logf("[%s] LLMInferenceService is Ready; sending authenticated completion request", topology)
	send := func() error { return sendCompletion(tc.Context(), routeHost, ocToken) }
	g.Eventually(send, 2*time.Minute, 5*time.Second).Should(gomega.Succeed(), "Should successfully send completion request through OAuth proxy")

	// 3. Verification via OATS
	t.Logf("[%s] Running OATS verification", topology)
	g.Eventually(func() error {
		return runOatsCase(tc.Context(), t, oatsBin, topology, oatsEnv, projectRoot)
	}, 2*time.Minute, 10*time.Second).Should(gomega.Succeed(), "OATS verification should succeed within 2 minutes")

	// 4. Pod Termination & Scrape Recovery
	t.Logf("[%s] Restarting vLLM pod for scrape recovery", topology)
	restartVLLMPod(t, tc, "default", llmSvcName)

	t.Logf("[%s] Sending completion request after pod restart", topology)
	g.Eventually(func() error {
		return send()
	}, 2*time.Minute, 5*time.Second).Should(gomega.Succeed(), "Second completion request should succeed after pod restart")

	g.Eventually(func() error {
		return runOatsCase(tc.Context(), t, oatsBin, topology, oatsEnv, projectRoot)
	}, 2*time.Minute, 10*time.Second).Should(gomega.Succeed(), "Scrape recovery verification should succeed after pod restart")

	// 5. Teardown & PodMonitor Cleanup
	if noCleanup {
		t.Logf("[%s] NO_CLEANUP=1; leaving LLMInferenceService and generated resources in place", topology)
		return
	}
	t.Logf("[%s] Deleting LLMInferenceService and waiting for PodMonitor cleanup", topology)
	tc.DeleteResource(WithMinimalObject(gvkLLMInferenceService, llmSvcNN), WithIgnoreNotFound(true), WithWaitForDeletion(true))

	g.Eventually(func() bool {
		pmList := &unstructured.UnstructuredList{}
		pmList.SetGroupVersionKind(gvk.CoreosPodMonitor)
		if err := tc.Client().List(tc.Context(), pmList, client.InNamespace("default")); err != nil {
			return true
		}
		for _, pm := range pmList.Items {
			if strings.Contains(pm.GetName(), llmSvcName) {
				return false
			}
		}
		return true
	}, 1*time.Minute, 2*time.Second).Should(gomega.BeTrue(), "Associated PodMonitor should be deleted within 1 reconciliation cycle of LLMInferenceService deletion")
}

func logManualCleanupCommands(t *testing.T, topology, llmSvcName, proxyName, tlsSecretName, cookieSecretName, bindingName, saName string) {
	t.Helper()
	t.Logf(
		"[%s] NO_CLEANUP=1; retained resources: LLMInferenceService/%s, Route/%s, Deployment/%s, "+
			"Service/%s, Secrets/%s and %s, ServiceAccount/%s, ClusterRoleBinding/%s",
		topology, llmSvcName, proxyName, proxyName, proxyName,
		tlsSecretName, cookieSecretName, saName, bindingName,
	)
}

func ensureOAuthProxySecret(tc *TestContext) error {
	const secretName = "oauth-proxy-secrets"
	namespace := inferenceMonitoringNamespace()
	secret := &unstructured.Unstructured{}
	secret.SetGroupVersionKind(gvk.Secret)
	err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: secretName, Namespace: namespace}, secret)
	if err == nil {
		return nil
	}
	if !k8serr.IsNotFound(err) {
		return fmt.Errorf("get OAuth proxy session secret: %w", err)
	}

	var sessionSecret [32]byte
	if _, err := rand.Read(sessionSecret[:]); err != nil {
		return fmt.Errorf("generate OAuth proxy session secret: %w", err)
	}
	secret.Object["type"] = "Opaque"
	secret.Object["data"] = map[string]any{"session_secret": base64.StdEncoding.EncodeToString(sessionSecret[:])}
	secret.SetName(secretName)
	secret.SetNamespace(namespace)
	if err := tc.Client().Create(tc.Context(), secret); err != nil {
		return fmt.Errorf("create OAuth proxy session secret: %w", err)
	}
	return nil
}

func inferencePrerequisiteDir(projectRoot string) string {
	return filepath.Join(projectRoot, "tests", "e2e", "prerequisites", "inference")
}

// warnIfStorageClassMissing reports a missing StorageClass up front. dsci.yaml pins
// usageLogs to a named class, and PVCs that reference a missing one stay Pending
// indefinitely, surfacing only as an unexplained LokiStack readiness timeout.
func warnIfStorageClassMissing(t *testing.T, tc *TestContext, name string) {
	t.Helper()

	storageClasses := &unstructured.UnstructuredList{}
	storageClasses.SetGroupVersionKind(schema.GroupVersionKind{Group: "storage.k8s.io", Version: "v1", Kind: "StorageClassList"})
	if err := tc.Client().List(tc.Context(), storageClasses); err != nil {
		t.Logf("WARNING: failed to list StorageClasses to verify %q: %v", name, err)
		return
	}
	available := make([]string, 0, len(storageClasses.Items))
	for _, storageClass := range storageClasses.Items {
		if storageClass.GetName() == name {
			return
		}
		available = append(available, storageClass.GetName())
	}
	t.Logf("WARNING: dsci.yaml pins usageLogs storageClassName=%q but the cluster has only [%s]; "+
		"LokiStack PVCs will stay Pending", name, strings.Join(available, ", "))
}

// logUsageLogsPropagation reports whether the DSCI kept spec.monitoring.usageLogs. It never
// fails the run: DSCI -> Monitoring propagation is the opendatahub-operator's job, and
// applyMonitoringManifest configures the Monitoring CR regardless.
func logUsageLogsPropagation(t *testing.T, tc *TestContext) {
	t.Helper()

	dsci, err := tc.fetchResource(t, gvkDSCInitializationV2, types.NamespacedName{Name: tc.DSCICRName})
	if err != nil {
		t.Logf("could not read DSCI %s to check usageLogs propagation: %v", tc.DSCICRName, err)
		return
	}
	_, found, err := unstructured.NestedMap(dsci.Object, "spec", "monitoring", "usageLogs", "storage")
	switch {
	case err != nil:
		t.Logf("could not read spec.monitoring.usageLogs.storage from DSCI %s: %v", tc.DSCICRName, err)
	case found:
		t.Log("DSCI kept spec.monitoring.usageLogs; the opendatahub-operator should propagate it " +
			"to the Monitoring CR")
	default:
		t.Log("WARNING: the installed DSCInitialization CRD has no spec.monitoring.usageLogs, so " +
			"the stanza in dsci.yaml was pruned; usage logs depend on monitoring.yaml being applied")
	}
}

// requireNamespacesMatchExistingDSCI fails before dsci.yaml is applied when the cluster already
// carries a DSCI pinned to different namespaces.
//
// spec.applicationsNamespace and spec.monitoring.namespace are both immutable
// (rule: self == oldSelf), so the apply would be rejected with a CEL message naming a field but
// not the flag that set it, and nothing short of deleting the CRs recovers. The namespaces are
// chosen per cluster, not per run.
func requireNamespacesMatchExistingDSCI(t *testing.T, tc *TestContext) {
	t.Helper()

	dsci, err := tc.fetchResource(t, gvkDSCInitializationV2, types.NamespacedName{Name: tc.DSCICRName})
	if err != nil {
		// No DSCI yet is the normal path: there is nothing to conflict with.
		return
	}

	for _, pinned := range []struct {
		flag  string
		want  string
		field []string
	}{
		{"-applications-namespace", inferenceApplicationsNamespace(), []string{"spec", "applicationsNamespace"}},
		{"-monitoring-namespace", inferenceMonitoringNamespace(), []string{"spec", "monitoring", "namespace"}},
	} {
		got, found, err := unstructured.NestedString(dsci.Object, pinned.field...)
		if err != nil || !found || got == pinned.want {
			continue
		}
		t.Fatalf("DSCI %s has %s=%q but this run is configured for %q via %s, and the field is "+
			"immutable; delete the DSCI, DSC, and Monitoring CRs to switch namespaces",
			tc.DSCICRName, strings.Join(pinned.field, "."), got, pinned.want, pinned.flag)
	}
}

// warnIfOperatorPredatesNamespace reports a namespace the opendatahub-operator cannot see. The
// operator builds a namespace-scoped cache when its pod starts, so a namespace created later
// fails every module deploy with "unknown namespace for the cache" -- which surfaces only as
// odh-observability never appearing, several minutes into the run.
func warnIfOperatorPredatesNamespace(t *testing.T, tc *TestContext, namespace string) {
	t.Helper()

	ns, err := tc.fetchResource(t, gvk.Namespace, types.NamespacedName{Name: namespace})
	if err != nil {
		return
	}
	pods := &unstructured.UnstructuredList{}
	pods.SetGroupVersionKind(gvk.Pod.GroupVersion().WithKind(gvk.Pod.Kind + "List"))
	if err := tc.Client().List(tc.Context(), pods, client.InNamespace(odhOperatorNamespace)); err != nil {
		return
	}

	for _, pod := range pods.Items {
		started, found, err := unstructured.NestedString(pod.Object, "status", "startTime")
		if err != nil || !found {
			continue
		}
		startTime, err := time.Parse(time.RFC3339, started)
		if err != nil || !ns.GetCreationTimestamp().After(startTime) {
			continue
		}
		t.Logf("WARNING: namespace %s was created at %s but %s/%s started at %s; the operator's "+
			"namespace cache predates it and module deploys will fail with \"unknown namespace "+
			"for the cache\". Run: oc rollout restart deploy/%s -n %s",
			namespace, ns.GetCreationTimestamp().Format(time.RFC3339), odhOperatorNamespace,
			pod.GetName(), started, odhOperatorDeployment, odhOperatorNamespace)
		return
	}
}

func ensureCRDExists(ctx context.Context, tc *TestContext, name string) error {
	crd := &unstructured.Unstructured{}
	crd.SetGroupVersionKind(schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"})
	if err := tc.Client().Get(ctx, types.NamespacedName{Name: name}, crd); err != nil {
		return err
	}
	return nil
}

// inferenceMonitoringNamespace and inferenceApplicationsNamespace resolve the namespaces the
// inference prerequisites install into. -monitoring-namespace is shared with the TestMonitoring
// suite, where an empty value means "auto-detect from the running operator"; nothing is deployed
// yet here, so the inference suite falls back to the manifest literal instead.
func inferenceMonitoringNamespace() string {
	if testOpts.monitoringNamespace != "" {
		return testOpts.monitoringNamespace
	}
	return defaultInferenceMonitoringNamespace
}

func inferenceApplicationsNamespace() string {
	if testOpts.applicationsNamespace != "" {
		return testOpts.applicationsNamespace
	}
	return defaultInferenceApplicationsNamespace
}

// renderManifest reads a manifest and rewrites the default namespaces to the configured ones.
//
// Substitution is textual because the namespace is not only a field: it is embedded in the
// exporter endpoints (http://lgtm.<ns>.svc.cluster.local:4317) that dsci.yaml and monitoring.yaml
// both carry. It runs in one direction only -- redhat-ods-* to the configured value, never the
// reverse -- which is why the manifests keep the RHOAI namespaces as their literals. Rewriting
// toward them would have to match "opendatahub", a substring of every apiVersion in the
// directory.
func renderManifest(path string) ([]byte, error) {
	manifest, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return []byte(strings.NewReplacer(
		defaultInferenceMonitoringNamespace, inferenceMonitoringNamespace(),
		defaultInferenceApplicationsNamespace, inferenceApplicationsNamespace(),
	).Replace(string(manifest))), nil
}

func applyManifest(tc *TestContext, path string) error {
	return applyManifestWithOptions(tc, path, true)
}

// applyManifestIfAbsent creates missing resources without taking ownership of existing ones.
func applyManifestIfAbsent(tc *TestContext, path string) error {
	return applyManifestWithOptions(tc, path, false)
}

func applyManifestWithOptions(tc *TestContext, path string, updateExisting bool) error {
	manifest, err := renderManifest(path)
	if err != nil {
		return err
	}

	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(manifest), 4096)
	for {
		object := map[string]any{}
		if err := decoder.Decode(&object); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if len(object) == 0 {
			continue
		}
		resource := &unstructured.Unstructured{Object: object}
		key := types.NamespacedName{Name: resource.GetName(), Namespace: resource.GetNamespace()}
		current := &unstructured.Unstructured{}
		current.SetGroupVersionKind(resource.GroupVersionKind())
		getErr := tc.Client().Get(tc.Context(), key, current)
		if k8serr.IsNotFound(getErr) {
			if err := tc.Client().Create(tc.Context(), resource); err != nil {
				return fmt.Errorf("create %s/%s: %w", resource.GroupVersionKind(), key, err)
			}
			continue
		}
		if getErr != nil {
			return getErr
		}
		if !updateExisting {
			continue
		}
		resource.SetResourceVersion(current.GetResourceVersion())
		if err := tc.Client().Update(tc.Context(), resource); err != nil {
			return fmt.Errorf("update %s/%s: %w", resource.GroupVersionKind(), key, err)
		}
	}
}

// readSingleManifest decodes a manifest holding exactly one object. applyManifestWithOptions
// streams multi-document files straight to the API server; callers that need to inspect or
// reshape the object before writing it use this instead.
func readSingleManifest(path string) (*unstructured.Unstructured, error) {
	manifest, err := renderManifest(path)
	if err != nil {
		return nil, err
	}

	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(manifest), 4096)
	object := map[string]any{}
	if err := decoder.Decode(&object); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	if len(object) == 0 {
		return nil, fmt.Errorf("%s contains no object", path)
	}

	extra := map[string]any{}
	switch err := decoder.Decode(&extra); {
	case errors.Is(err, io.EOF):
	case err != nil:
		return nil, fmt.Errorf("decode %s: %w", path, err)
	case len(extra) > 0:
		return nil, fmt.Errorf("%s must contain a single object", path)
	}

	return &unstructured.Unstructured{Object: object}, nil
}

// monitoringUpdateAttempts bounds the conflict retries in updateMonitoringSpec. The
// opendatahub-operator writes the Monitoring CR on every reconcile, so a 409 here is routine.
const monitoringUpdateAttempts = 5

// updateMonitoringSpec replaces the Monitoring CR's spec with the manifest's. The CR already
// exists -- the opendatahub-operator creates and owns it -- so the live metadata is carried over:
// a whole-object Update would otherwise drop monitoring.opendatahub.io/cleanup, leaving the CR
// deletable without deleteAllOwned running, along with the operator's ownerReferences.
func updateMonitoringSpec(tc *TestContext, manifest *unstructured.Unstructured) error {
	nn := types.NamespacedName{Name: manifest.GetName()}

	var conflictErr error
	for range monitoringUpdateAttempts {
		current := &unstructured.Unstructured{}
		current.SetGroupVersionKind(manifest.GroupVersionKind())
		if err := tc.Client().Get(tc.Context(), nn, current); err != nil {
			return fmt.Errorf("get %s: %w", nn, err)
		}

		desired := manifest.DeepCopy()
		desired.SetResourceVersion(current.GetResourceVersion())
		desired.SetFinalizers(current.GetFinalizers())
		desired.SetOwnerReferences(current.GetOwnerReferences())
		desired.SetLabels(current.GetLabels())
		desired.SetAnnotations(current.GetAnnotations())

		err := tc.Client().Update(tc.Context(), desired)
		switch {
		case err == nil:
			return nil
		case k8serr.IsConflict(err):
			conflictErr = err
		default:
			return fmt.Errorf("update %s: %w", nn, err)
		}
	}
	return fmt.Errorf("update %s: %w", nn, conflictErr)
}

// applyMonitoringManifest overwrites the Monitoring CR the opendatahub-operator created with the
// spec from monitoring.yaml. DSCI -> Monitoring propagation belongs to the opendatahub-operator,
// and a DSCInitialization CRD that predates usageLogs prunes the stanza silently; writing the
// config straight onto the Monitoring CR keeps odh-observability -- the thing actually under
// test -- exercised end to end.
func applyMonitoringManifest(t *testing.T, tc *TestContext, dir string) {
	t.Helper()

	path := filepath.Join(dir, "monitoring.yaml")
	manifest, err := readSingleManifest(path)
	if err != nil {
		t.Fatalf("failed to read the Monitoring manifest: %v", err)
	}
	if name := manifest.GetName(); name != tc.MonitoringCRName {
		t.Fatalf("%s targets Monitoring %q but the suite is configured for %q", path, name, tc.MonitoringCRName)
	}
	// spec.namespace is immutable (api/v1alpha1/monitoring_types.go), so a desync with what the
	// opendatahub-operator wrote rejects the update with a CEL message that reads nothing like
	// the real cause.
	namespace, _, err := unstructured.NestedString(manifest.Object, "spec", "namespace")
	if err != nil {
		t.Fatalf("failed to read spec.namespace from %s: %v", path, err)
	}
	if namespace != inferenceMonitoringNamespace() {
		t.Fatalf("%s pins spec.namespace=%q but the prerequisites use %q, and the field is immutable",
			path, namespace, inferenceMonitoringNamespace())
	}

	nn := types.NamespacedName{Name: tc.MonitoringCRName}
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, nn),
		WithEventuallyTimeout(5*time.Minute),
		WithCustomErrorMsg("Monitoring CR %s should be created from the DSCI", tc.MonitoringCRName),
	)

	t.Logf("Overwriting the Monitoring CR spec from %s", path)
	if err := updateMonitoringSpec(tc, manifest); err != nil {
		t.Fatalf("failed to configure the Monitoring CR from %s: %v", path, err)
	}

	// Read back. A Monitoring CRD that predates usageLogs prunes the stanza exactly as the DSCI
	// CRD did and the update still returns 200; without this the first symptom is the LokiStack
	// wait below spending its full budget blaming a LokiStack nobody asked for.
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, nn),
		WithCondition(jq.Match(`.spec.usageLogs.storage.secretName == "%s"`, lokiS3SecretName)),
		WithCustomErrorMsg("Monitoring CR %s should carry usageLogs storage after applying %s", tc.MonitoringCRName, path),
	)
}

// enableMaaSUsageLogging flips spec.usageLogging on the cluster-scoped MaaS Config CR.
//
// It writes the single field rather than replacing the spec: the maas-controller owns
// spec.limitadorScrapeInterval and the maas.opendatahub.io/default-aitenant-bootstrapped
// annotation on the same object, and there is no manifest here to declare them from.
func enableMaaSUsageLogging(tc *TestContext) error {
	nn := types.NamespacedName{Name: maasConfigCRName}

	var conflictErr error
	for range monitoringUpdateAttempts {
		config := &unstructured.Unstructured{}
		config.SetGroupVersionKind(gvkMaaSConfig)
		if err := tc.Client().Get(tc.Context(), nn, config); err != nil {
			return fmt.Errorf("get MaaS Config %s: %w", nn, err)
		}

		// Re-runs against a configured cluster are the common case.
		if enabled, _, err := unstructured.NestedBool(config.Object, "spec", "usageLogging"); err == nil && enabled {
			return nil
		}
		if err := unstructured.SetNestedField(config.Object, true, "spec", "usageLogging"); err != nil {
			return fmt.Errorf("set spec.usageLogging on MaaS Config %s: %w", nn, err)
		}

		err := tc.Client().Update(tc.Context(), config)
		switch {
		case err == nil:
			return nil
		case k8serr.IsConflict(err):
			conflictErr = err
		default:
			return fmt.Errorf("update MaaS Config %s: %w", nn, err)
		}
	}
	return fmt.Errorf("update MaaS Config %s: %w", nn, conflictErr)
}

// enableMaaSUsageLoggingPrerequisite turns on per-request usage logging in MaaS.
//
// The maas-controller only deploys the gateway EnvoyFilter that emits usage logs -- the input
// this repo's collector ingests -- when spec.usageLogging is true, and the field defaults to
// false. Without this there is nothing for the collector to receive.
//
// It must run after the DSC is Ready: the maas-controller is deployed by the DSC's aigateway
// component, and it is the controller, not any manifest here, that bootstraps Config/default.
func enableMaaSUsageLoggingPrerequisite(t *testing.T, tc *TestContext) {
	t.Helper()

	nn := types.NamespacedName{Name: maasConfigCRName}
	tc.EnsureResourceExists(
		WithMinimalObject(gvkMaaSConfig, nn),
		WithEventuallyTimeout(5*time.Minute),
		WithCustomErrorMsg("MaaS Config %s should be bootstrapped by the maas-controller", maasConfigCRName),
	)

	t.Logf("Enabling spec.usageLogging on MaaS Config %s", maasConfigCRName)
	if err := enableMaaSUsageLogging(tc); err != nil {
		t.Fatalf("failed to enable MaaS usage logging: %v", err)
	}

	// Read back. A maas CRD predating the field prunes the write and still returns 200; without
	// this the only symptom is a gateway EnvoyFilter that never appears.
	tc.EnsureResourceExists(
		WithMinimalObject(gvkMaaSConfig, nn),
		WithCondition(jq.Match(`.spec.usageLogging == true`)),
		WithCustomErrorMsg("MaaS Config %s should have spec.usageLogging=true", maasConfigCRName),
	)
}

// maasResourceAttributeKeysExpr collects every OTel access-log resource-attribute key declared in
// a MaaS EnvoyFilter. It walks all configPatches rather than indexing: only the NETWORK_FILTER
// patch carries access_log, and its position in the list is not part of any contract. The "?" on
// access_log is what lets the HTTP_FILTER patches fall through -- their patch.value.typed_config
// has no access_log, so the iterator yields nothing instead of erroring.
const maasResourceAttributeKeysExpr = `[.spec.configPatches[]?.patch.value.typed_config` +
	`.access_log[]?.typed_config.resource_attributes.values[]?.key]`

// testMaaSUsageLogEnvoyFilter pins the one thing this repo needs from the EnvoyFilter MaaS
// deploys when usage logging is on: a service.namespace resource attribute.
//
// The collector inserts kubernetes_namespace_name from it
// (internal/controller/resources/usage-logs-opentelemetry-collector.tmpl.yaml:45-54), and that is
// the subject LokiStack per-tenant RBAC filters on. If MaaS stops emitting the attribute the
// tenancy quietly loses its subject rather than failing.
//
// Only the key's presence is asserted -- the value is MaaS's to choose.
func testMaaSUsageLogEnvoyFilter(t *testing.T, tc *TestContext) {
	t.Helper()

	nn := types.NamespacedName{
		Name:      maasUsageLogEnvoyFilter,
		Namespace: maasUsageLogEnvoyFilterNamespace,
	}

	// Split from the attribute check below so "never created" and "created but missing the
	// attribute" are distinguishable failures. The maas-controller creates the filter in reaction
	// to the Config flip, which nothing waited for.
	tc.EnsureResourceExists(
		WithMinimalObject(gvkEnvoyFilter, nn),
		WithEventuallyTimeout(5*time.Minute),
		WithCustomErrorMsg(
			"the maas-controller should create EnvoyFilter %s after spec.usageLogging is enabled on MaaS Config %s",
			maasUsageLogEnvoyFilter, maasConfigCRName),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvkEnvoyFilter, nn),
		WithCondition(jq.Match(maasResourceAttributeKeysExpr+` | any(. == "%s")`, maasTenantResourceAttribute)),
		WithCustomErrorMsg(
			"EnvoyFilter %s must declare %s as an OTel access-log resource attribute; the collector maps it to "+
				"kubernetes_namespace_name, which LokiStack per-tenant RBAC depends on",
			maasUsageLogEnvoyFilter, maasTenantResourceAttribute),
	)
}

func waitForCondition(tc *TestContext, g schema.GroupVersionKind, nn types.NamespacedName, conditionType string) error {
	resource := &unstructured.Unstructured{}
	resource.SetGroupVersionKind(g)
	if err := tc.Client().Get(tc.Context(), nn, resource); err != nil {
		return err
	}
	conditions, found, err := unstructured.NestedSlice(resource.Object, "status", "conditions")
	if err != nil || !found {
		return fmt.Errorf("%s/%s has no status conditions", g.Kind, nn)
	}
	for _, raw := range conditions {
		condition, ok := raw.(map[string]any)
		if ok && condition["type"] == conditionType && condition["status"] == "True" {
			return nil
		}
	}
	return fmt.Errorf("%s/%s condition %s is not True", g.Kind, nn, conditionType)
}

func installInferenceOperators(t *testing.T, tc *TestContext) {
	t.Helper()

	t.Log("Installing Cluster Observability, cert-manager, Tempo, OpenTelemetry, LeaderWorkerSet, " +
		"Loki, and Red Hat Connectivity Link operators")
	tc.EnsureOperatorInstalled(observabilityOpNamespace, observabilityOpName, observabilityOpChannel)
	tc.EnsureOperatorInstalled(certManagerOpNamespace, certManagerOpName, certManagerOpChannel)
	tc.EnsureOperatorInstalled(tempoOpNamespace, tempoOpName, tempoOpChannel)
	tc.EnsureOperatorInstalled(opentelemetryOpNamespace, opentelemetryOpName, opentelemetryOpChannel)
	tc.EnsureOperatorInstalledInOwnNamespace(leaderWorkerSetOpNamespace, leaderWorkerSetOpName, leaderWorkerSetOpChannel)
	tc.EnsureOperatorInstalledFromCatalog(connectivityLinkOpNamespace, connectivityLinkOpName, connectivityLinkOpChannel, connectivityLinkOpSource)
	// dsci.yaml enables usageLogs, so the operator renders a LokiStack.
	tc.ensureLokiOperatorInstalled(t)
}

// requireObservabilityAgreesOnNamespaces gates the run on odh-observability being deployed and
// pointed at the same namespaces the prerequisites used. It can only run after dsci.yaml: the
// ODH operator deploys odh-observability in response to the DSCI monitoring stanza, and the
// applications namespace is the operator's to create.
func requireObservabilityAgreesOnNamespaces(t *testing.T, tc *TestContext) {
	t.Helper()

	for _, namespace := range []string{inferenceApplicationsNamespace(), inferenceMonitoringNamespace()} {
		warnIfOperatorPredatesNamespace(t, tc, namespace)
	}

	// Nothing else reconciles the Monitoring CR, so without this the waits that follow spend
	// their full budget on a CR that was never going to be touched.
	operatorMonitoringNamespace := tc.ensureOperatorDeploymentReady(t)
	// validateMonitoringNamespace (internal/controller/monitoring_reconciler.go) refuses to
	// reconcile on a mismatch, so a desync here stalls silently rather than erroring visibly.
	if operatorMonitoringNamespace != inferenceMonitoringNamespace() {
		t.Fatalf("odh-observability is configured with MONITORING_NAMESPACE=%q but dsci.yaml pins "+
			"spec.monitoring.namespace=%q; the operator will refuse to reconcile the Monitoring CR",
			operatorMonitoringNamespace, inferenceMonitoringNamespace())
	}
}

func setupInferencePrerequisites(t *testing.T, tc *TestContext, projectRoot string) {
	t.Helper()
	g := gomega.NewWithT(t)

	// The storage fixtures and the Loki S3 Secret must land where the operator creates the
	// LokiStack, which dsci.yaml pins to spec.monitoring.namespace.
	tc.MonitoringNamespace = inferenceMonitoringNamespace()
	t.Logf("Inference prerequisites use applicationsNamespace=%s, monitoring.namespace=%s",
		inferenceApplicationsNamespace(), inferenceMonitoringNamespace())
	requireNamespacesMatchExistingDSCI(t, tc)

	waitFor := func(description string, gvk schema.GroupVersionKind, nn types.NamespacedName, condition string, timeout ...time.Duration) {
		wait := 5 * time.Minute
		if len(timeout) > 0 {
			wait = timeout[0]
		}
		t.Logf("Waiting for %s (timeout %s)", description, wait)
		g.Eventually(func() error {
			return waitForCondition(tc, gvk, nn, condition)
		}, wait, 2*time.Second).Should(gomega.Succeed(), description)
		t.Logf("Completed wait: %s", description)
	}
	dir := inferencePrerequisiteDir(projectRoot)
	applyPrerequisite := func(name string) {
		path := filepath.Join(dir, name)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("inference prerequisite manifest %s is unavailable: %v", path, err)
		}
		apply := applyManifest
		if name == "kuadrant.yaml" {
			// Kuadrant may be preconfigured; this test only needs it to exist.
			apply = applyManifestIfAbsent
		}
		if err := apply(tc, path); err != nil {
			t.Fatalf("failed to apply inference prerequisite %s: %v", name, err)
		}
	}

	if testOpts.installOperators {
		installInferenceOperators(t, tc)
	}

	t.Log("Setting up DSCI, DSC, and UIPlugin prerequisites")
	for _, crd := range []string{
		// Gate on the CRD's Established condition rather than listing LokiStack objects:
		// the client's RESTMapper can keep reporting NoMatch briefly after install.
		"lokistacks.loki.grafana.com",
		"dscinitializations.dscinitialization.opendatahub.io",
		"datascienceclusters.datasciencecluster.opendatahub.io",
		"uiplugins.observability.openshift.io",
	} {
		waitFor(
			"CRD "+crd+" should be established",
			gvkCustomResourceDefinition,
			types.NamespacedName{Name: crd},
			"Established",
		)
	}

	// The operator renders the LokiStack as soon as it sees usageLogs storage and never
	// checks the Secret, so the S3 backend and its credentials must exist before dsci.yaml
	// is applied. The namespace does not exist yet; the ODH operator adopts it later.
	t.Logf("Setting up the S3 backend for usage logs in %s", inferenceMonitoringNamespace())
	tc.ensureNamespaceExists(inferenceMonitoringNamespace())
	tc.startSeaweedFS(t, lokiS3Bucket)
	tc.createLokiS3Secret(t, lokiS3SecretName, inferenceMonitoringNamespace())

	for _, name := range []string{"dsci.yaml", "dsc.yaml", "coo-uiplugins.yaml"} {
		applyPrerequisite(name)
	}
	// Diagnostic only: a DSCInitialization CRD that predates usageLogs prunes the stanza
	// silently, and applyMonitoringManifest below covers for it. Logging which path configured
	// usage logs is the difference between reading a run's log and guessing.
	logUsageLogsPropagation(t, tc)

	warnIfStorageClassMissing(t, tc, lokiStorageClassName)
	requireObservabilityAgreesOnNamespaces(t, tc)

	if testOpts.applyMonitoringManifest {
		applyMonitoringManifest(t, tc, dir)
	}

	waitFor(
		"LokiStack "+LokiStackName+" should be Ready",
		gvk.LokiStack,
		types.NamespacedName{Name: LokiStackName, Namespace: inferenceMonitoringNamespace()},
		"Ready",
		20*time.Minute,
	)
	// Assert on the Monitoring CR rather than on the DSCI: the DSCI copies monitoring
	// conditions across and then sets phase Ready unconditionally, so it reports Ready over a
	// broken monitoring stack.
	//
	// Ready on the Monitoring CR is no better: AggregateReady leaves it True and flips Degraded
	// instead when a configured feature fails, because the platform treats Ready as a runlevel
	// gate rather than a health summary. Degraded is the health signal.
	//
	// Degraded is only an alarm for things the spec actually asked for -- an unrequested
	// feature is marked Severity: Info and skipped by the aggregation. That is exact when you
	// write the Monitoring spec yourself, but this test does not: the spec propagates
	// DSCI -> opendatahub-operator -> Monitoring. The guard above only proves the stanza
	// survived the DSCI CRD, so confirm the ask landed here before trusting Degraded.
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(gomega.And(
			jq.Match(`.spec.namespace == %q`, inferenceMonitoringNamespace()),
			jq.Match(`.spec.usageLogs.storage.secretName == %q`, lokiS3SecretName),
			// Every action function runs before AggregateReady in the same pass, so
			// ProvisioningSucceeded=True means the feature conditions Degraded reads are from
			// this reconcile. The early-return paths skip the aggregation and leave them stale.
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`,
				platformcommon.ConditionTypeProvisioningSucceeded, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`,
				platformcommon.ConditionTypeDegraded, metav1.ConditionFalse),
			// Implied by the above, but asserted directly because it is the artifact the envoy
			// filter consumes. An absent field compares as null, so default it first.
			jq.Match(`(.status.usageLogsEndpoint // "") != ""`),
		)),
		WithEventuallyTimeout(10*time.Minute),
		WithCustomErrorMsg("Monitoring %s should expose a usage logs endpoint and not be Degraded", tc.MonitoringCRName),
	)
	waitFor(
		"DSC default-dsc should be Ready",
		schema.GroupVersionKind{
			Group: "datasciencecluster.opendatahub.io", Version: "v2", Kind: "DataScienceCluster",
		},
		types.NamespacedName{Name: "default-dsc"},
		"Ready",
	)

	enableMaaSUsageLoggingPrerequisite(t, tc)

	for _, crd := range []string{
		"kuadrants.kuadrant.io",
		"authpolicies.kuadrant.io",
	} {
		waitFor(
			"CRD "+crd+" should be established",
			gvkCustomResourceDefinition,
			types.NamespacedName{Name: crd},
			"Established",
		)
	}
	tc.ensureNamespaceExists(kuadrantResourceNamespace)
	applyPrerequisite("kuadrant.yaml")
	waitFor(
		"Kuadrant kuadrant should be Ready",
		schema.GroupVersionKind{Group: "kuadrant.io", Version: "v1beta1", Kind: "Kuadrant"},
		types.NamespacedName{Name: "kuadrant", Namespace: kuadrantResourceNamespace},
		"Ready",
	)

	t.Log("Setting up LGTM and remaining inference prerequisites")
	waitFor(
		"CRD leaderworkersetoperators.operator.openshift.io should be established",
		gvkCustomResourceDefinition,
		types.NamespacedName{Name: "leaderworkersetoperators.operator.openshift.io"},
		"Established",
	)
	applyPrerequisite("lwsoperator.yaml")
	waitFor(
		"CRD leaderworkersets.leaderworkerset.x-k8s.io should be established",
		gvkCustomResourceDefinition,
		types.NamespacedName{Name: "leaderworkersets.leaderworkerset.x-k8s.io"},
		"Established",
	)
	waitFor(
		"CRD llminferenceservices.serving.kserve.io should be established",
		gvkCustomResourceDefinition,
		types.NamespacedName{Name: "llminferenceservices.serving.kserve.io"},
		"Established",
	)
	if err := ensureOAuthProxySecret(tc); err != nil {
		t.Fatalf("failed to ensure OAuth proxy cookie Secret: %v", err)
	}
	applyPrerequisite("lgtm.yaml")
	waitFor(
		"opendatahub-operator should be Available",
		schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
		types.NamespacedName{Name: odhOperatorDeployment, Namespace: odhOperatorNamespace},
		"Available",
	)
	waitFor(
		"LGTM should be Available",
		schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
		types.NamespacedName{Name: "lgtm", Namespace: inferenceMonitoringNamespace()},
		"Available",
	)
	for _, endpoint := range []struct{ service, namespace string }{
		{odhOperatorWebhookService, odhOperatorNamespace},
		{"kserve-webhook-server-service", inferenceApplicationsNamespace()},
		{"llmisvc-webhook-server-service", inferenceApplicationsNamespace()},
		{"lgtm", inferenceMonitoringNamespace()},
	} {
		g.Eventually(func() bool {
			endpoints := &unstructured.Unstructured{}
			endpoints.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Endpoints"})
			if err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: endpoint.service, Namespace: endpoint.namespace}, endpoints); err != nil {
				return false
			}
			subsets, found, _ := unstructured.NestedSlice(endpoints.Object, "subsets")
			if !found {
				return false
			}
			for _, rawSubset := range subsets {
				subset, ok := rawSubset.(map[string]any)
				if !ok {
					continue
				}
				addresses, _, _ := unstructured.NestedSlice(subset, "addresses")
				if len(addresses) > 0 {
					return true
				}
			}
			return false
		}, 5*time.Minute, 2*time.Second).Should(gomega.BeTrue(), "endpoints for %s/%s should be ready", endpoint.namespace, endpoint.service)
	}
}

func TestLLMInferenceService(t *testing.T) {
	tc, err := NewTestContext(t)
	require.NoError(t, err)

	tc.DefaultResourceOpts = []ResourceOpts{
		WithEventuallyTimeout(5 * time.Minute),
		WithEventuallyPollingInterval(2 * time.Second),
	}

	projectRoot, err := findProjectRoot()
	require.NoError(t, err)

	setupInferencePrerequisites(t, tc, projectRoot)

	// Runs before the OATS setup below: it needs neither the binary nor an LLMInferenceService.
	t.Run("maas-usage-log-envoyfilter", func(t *testing.T) {
		testMaaSUsageLogEnvoyFilter(t, tc)
	})

	oatsBin := ensureOatsBinary(t, projectRoot)
	oatsEnv := buildOatsEnv(t, tc)

	topologies := []string{"single-node", "multi-node", "disaggregated"}

	for _, topology := range topologies {
		t.Run(topology, func(t *testing.T) {
			runLLMInferenceServiceTopologyTest(t, tc, topology, projectRoot, oatsBin, oatsEnv)
		})
	}
}
