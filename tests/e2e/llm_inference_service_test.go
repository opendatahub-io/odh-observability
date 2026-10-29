package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
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

	if envMap["GRAFANA_TLS_CA_FILE"] == "" {
		envMap["GRAFANA_TLS_CA_FILE"] = writeClusterIngressCAFile(t, tc.Context(), tc.Client())
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

func newTestTraceparent() (string, error) {
	var traceID [16]byte
	var parentSpanID [8]byte
	var zeroTraceID [16]byte
	var zeroParentSpanID [8]byte

	for traceID == zeroTraceID {
		if _, err := rand.Read(traceID[:]); err != nil {
			return "", fmt.Errorf("generate test trace ID: %w", err)
		}
	}
	for parentSpanID == zeroParentSpanID {
		if _, err := rand.Read(parentSpanID[:]); err != nil {
			return "", fmt.Errorf("generate test parent span ID: %w", err)
		}
	}

	return fmt.Sprintf("00-%x-%x-01", traceID, parentSpanID), nil
}

func traceIDFromTraceparent(traceparent string) (string, error) {
	parts := strings.Split(traceparent, "-")
	if len(parts) != 4 || parts[0] != "00" {
		return "", fmt.Errorf("invalid traceparent %q", traceparent)
	}
	if len(parts[1]) != 32 {
		return "", fmt.Errorf("invalid trace ID in traceparent %q", traceparent)
	}
	return parts[1], nil
}

// traceIDForTraceQL returns the trace ID forms gcx/Tempo accept in TraceQL (hex and UUID).
func traceIDForTraceQL(hexID string) (string, string) {
	hex := strings.ToLower(hexID)
	if len(hex) != 32 {
		return hex, hex
	}
	uuid := fmt.Sprintf("%s-%s-%s-%s-%s", hex[0:8], hex[8:12], hex[12:16], hex[16:20], hex[20:32])
	return hex, uuid
}

func writeOatsTraceCaseFile(t *testing.T, tag, podNamePrefix, traceID, parentSpanID string) string {
	t.Helper()

	dir := filepath.Join(t.TempDir(), tag)
	require.NoError(t, os.MkdirAll(dir, 0o750))
	path := filepath.Join(dir, "oats-case.yaml")
	traceHex, _ := traceIDForTraceQL(traceID)
	// Require the current run's trace ID as well as the pod prefix so stale spans cannot pass.
	content := fmt.Sprintf(`name: %s vLLM tracing (run-specific)
tags: ["e2e", "vllm", "%s"]

fixture:
  remote:
    endpoint: default

expected:
  traces:
    - traceql: '{ trace:id = "%s" && name = "llm_request" && resource.k8s.pod.name =~ "%s.*" && span:parentID = "%s" }'
      count: ">= 1"
`, tag, tag, traceHex, podNamePrefix, parentSpanID)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

const oatsGCXVersion = "1.2.0"

var gcxVersionPattern = regexp.MustCompile(`(?:^|[^0-9A-Za-z])v?([0-9]+\.[0-9]+\.[0-9]+)(?:[^0-9A-Za-z]|$)`)

func gcxHasVersion(ctx context.Context, path, expected string) bool {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, path, "--version").Output()
	if err != nil {
		return false
	}
	match := gcxVersionPattern.FindStringSubmatch(string(out))
	return len(match) > 1 && match[1] == expected
}

func gcxTooling(ctx context.Context) (string, []string, error) {
	if configured := os.Getenv("OATS_GCX"); configured != "" {
		path, err := exec.LookPath(configured)
		if err != nil {
			return "", nil, fmt.Errorf("resolve OATS_GCX %q: %w", configured, err)
		}
		return path, []string{"--gcx", path, "--gcx-download", "never"}, nil
	}
	// Match OATS's --gcx-version resolution order: an exact version on PATH
	// wins over the cache, otherwise OATS uses/downloads the pinned cache copy.
	if path, err := exec.LookPath("gcx"); err == nil && gcxHasVersion(ctx, path, oatsGCXVersion) {
		return path, []string{"--gcx-version", oatsGCXVersion, "--gcx-download", "auto"}, nil
	}
	cacheDir := os.Getenv("OATS_CACHE_DIR")
	if cacheDir == "" && runtime.GOOS != "windows" {
		if stateHome := os.Getenv("XDG_STATE_HOME"); stateHome != "" {
			cacheDir = filepath.Join(stateHome, "oats")
		}
	}
	if cacheDir == "" {
		userCacheDir, err := os.UserCacheDir()
		if err != nil {
			return "", nil, fmt.Errorf("resolve OATS cache directory: %w", err)
		}
		cacheDir = filepath.Join(userCacheDir, "oats")
	}
	binaryName := "gcx"
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	path := filepath.Join(cacheDir, "tools", "gcx", oatsGCXVersion, runtime.GOOS+"_"+runtime.GOARCH, binaryName)
	// Do not pass the not-yet-downloaded path through --gcx: OATS treats an
	// explicit missing binary as an error instead of applying its auto-download
	// fallback. The shared cache directory and pinned version make the binary
	// OATS downloads the same one used by the direct checks below.
	args := []string{"--cache-dir", cacheDir, "--gcx-download", "auto", "--gcx-version", oatsGCXVersion}
	return path, args, nil
}

func gcxBinaryPath(ctx context.Context) (string, error) {
	path, _, err := gcxTooling(ctx)
	return path, err
}

func assertTraceIDInTempo(ctx context.Context, t *testing.T, traceID string, oatsEnv []string) error {
	t.Helper()

	traceHex, traceUUID := traceIDForTraceQL(traceID)
	gcxBin, err := gcxBinaryPath(ctx)
	if err != nil {
		return err
	}
	var lastErr error
	for _, id := range []string{traceHex, traceUUID} {
		cmd := exec.CommandContext(ctx, gcxBin, "traces", "get", id)
		cmd.Env = oatsEnv
		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Logf("gcx traces get %s succeeded (%d bytes)", id, len(out))
			return nil
		}
		lastErr = fmt.Errorf("gcx traces get %s: %w\n%s", id, err, strings.TrimSpace(string(out)))
	}
	return lastErr
}

func sendCompletion(ctx context.Context, routeHost, ocToken string, rootCAs *x509.CertPool, traceparent ...string) error {
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
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    rootCAs,
		}},
		Timeout: 30 * time.Second,
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://"+routeHost+"/v1/completions", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+ocToken)
	req.Header.Set("Content-Type", "application/json")
	if len(traceparent) > 0 && traceparent[0] != "" {
		req.Header.Set("Traceparent", traceparent[0])
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, maxInferenceServiceHTTPErrorBodyBytes))
		if readErr != nil {
			return fmt.Errorf("completions request to %s failed with status %d (read response body: %w)", req.URL, resp.StatusCode, readErr)
		}
		return fmt.Errorf("completions request to %s failed with status %d", req.URL, resp.StatusCode)
	}
	return nil
}

// ensureInferenceWorkloadNamespaceScrape opts the workload namespace into tenant metrics
// scraping. The operator excludes built-in namespaces like default from automatic enrollment.
func ensureInferenceWorkloadNamespaceScrape(t *testing.T, tc *TestContext, namespace string) {
	t.Helper()
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(gvk.Namespace)
	require.NoError(t, tc.Client().Get(tc.Context(), types.NamespacedName{Name: namespace}, existing))
	originalLabels := existing.GetLabels()
	originalMonitoring, hadMonitoring := originalLabels[ODHLabelMonitoring]
	originalOTLP, hadOTLP := originalLabels[otlpNamespaceLabel]
	t.Cleanup(func() {
		tc.EventuallyResourceCreatedOrPatched(
			WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: namespace}),
			WithMutateFunc(func(u *unstructured.Unstructured) error {
				labels := u.GetLabels()
				if labels == nil {
					labels = map[string]string{}
				}
				if hadMonitoring {
					labels[ODHLabelMonitoring] = originalMonitoring
				} else {
					delete(labels, ODHLabelMonitoring)
				}
				if hadOTLP {
					labels[otlpNamespaceLabel] = originalOTLP
				} else {
					delete(labels, otlpNamespaceLabel)
				}
				u.SetLabels(labels)
				return nil
			}),
		)
	})
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: namespace}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			labels := u.GetLabels()
			if labels == nil {
				labels = map[string]string{}
			}
			labels[ODHLabelMonitoring] = "true"
			labels[otlpNamespaceLabel] = "true"
			u.SetLabels(labels)
			return nil
		}),
	)
}

func runOatsCase(ctx context.Context, t *testing.T, oatsBin, topology string, oatsEnv []string, projectRoot string) error {
	t.Helper()
	_, gcxArgs, err := gcxTooling(ctx)
	if err != nil {
		return err
	}
	args := append([]string{"-vv"}, gcxArgs...)
	args = append(args, "--tags", topology, "--gcx-context", "default")
	return runOats(ctx, t, oatsBin, oatsEnv, projectRoot, args)
}

func runOatsCaseFile(ctx context.Context, t *testing.T, oatsBin, caseFile string, oatsEnv []string, projectRoot string) error {
	t.Helper()

	caseDir := filepath.Dir(caseFile)
	configDir := filepath.Dir(caseDir)
	relCase, err := filepath.Rel(configDir, caseFile)
	if err != nil {
		return fmt.Errorf("resolve OATS case path: %w", err)
	}
	configPath := filepath.Join(configDir, "oats-config.yaml")
	config := fmt.Sprintf("meta:\n  version: 3\ncases:\n  - \"%s\"\n", filepath.ToSlash(relCase))
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		return fmt.Errorf("write OATS config: %w", err)
	}
	_, gcxArgs, err := gcxTooling(ctx)
	if err != nil {
		return err
	}
	args := make([]string, 0, 3+len(gcxArgs)+2)
	args = append(args, "--config", configPath, "-vv")
	args = append(args, gcxArgs...)
	args = append(args, "--gcx-context", "default")
	return runOats(ctx, t, oatsBin, oatsEnv, configDir, args)
}

func runOats(ctx context.Context, t *testing.T, oatsBin string, oatsEnv []string, projectRoot string, args []string) error {
	t.Helper()

	cmd := exec.CommandContext(ctx, oatsBin, args...)
	cmd.Dir = projectRoot
	cmd.Env = oatsEnv

	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &outBuf

	t.Logf("Running OATS: %s", formatOatsCommand(oatsEnv, append([]string{oatsBin}, args...)))
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("OATS failed: %w\nOutput: %s", err, outBuf.String())
	}
	return nil
}

func formatOatsCommand(env []string, args []string) string {
	wanted := map[string]bool{
		"GCX_TELEMETRY":       true,
		"GRAFANA_ORG_ID":      true,
		"GRAFANA_SERVER":      true,
		"GRAFANA_TLS_CA_FILE": true,
		"GRAFANA_TOKEN":       true,
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

func discoverInferenceService(tc *TestContext, namespace, llmSvcName string) (string, int64, error) {
	services := &unstructured.UnstructuredList{}
	services.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ServiceList"})
	listCtx, cancel := context.WithTimeout(tc.Context(), 10*time.Second)
	defer cancel()
	started := time.Now()
	tc.t.Logf("[%s] listing Services in %s for owner %q", tc.t.Name(), namespace, llmSvcName)
	if err := tc.Client().List(listCtx, services, client.InNamespace(namespace)); err != nil {
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
	noCleanup := os.Getenv("NO_CLEANUP") == "1"
	workloadNS := llmInferenceWorkloadNamespace()
	proxyNS, err := randomServiceName("llmisvc-proxy")
	require.NoError(t, err, "failed to generate a unique OAuth proxy namespace")
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: proxyNS}),
		WithMutateFunc(func(namespace *unstructured.Unstructured) error {
			labels := namespace.GetLabels()
			if labels == nil {
				labels = map[string]string{}
			}
			labels["opendatahub.io/generated-namespace"] = "true"
			namespace.SetLabels(labels)
			return nil
		}),
	)
	t.Cleanup(func() {
		if noCleanup {
			t.Logf("[%s] NO_CLEANUP=1; leaving OAuth proxy namespace %s in place", topology, proxyNS)
			return
		}
		tc.DeleteResource(
			WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: proxyNS}),
			WithIgnoreNotFound(true),
			WithWaitForDeletion(true),
		)
	})
	llmSvcNN := types.NamespacedName{Name: llmSvcName, Namespace: workloadNS}
	proxyName := llmSvcName + "-p"
	tlsSecretName := proxyName + "-tls"
	cookieSecretName := proxyName + "-cookie"
	saName := proxyName
	bindingName := proxyName + "-auth-delegator"
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
		{gvk.Route, proxyName, proxyNS}, {gvk.Deployment, proxyName, proxyNS}, {gvk.Service, proxyName, proxyNS},
		{gvk.Secret, tlsSecretName, proxyNS}, {gvk.Secret, cookieSecretName, proxyNS}, {gvk.ClusterRoleBinding, bindingName, ""}, {gvk.ServiceAccount, saName, proxyNS},
		{gvkLLMInferenceService, llmSvcName, workloadNS},
	}
	// Register cleanup before creating anything so failures or aborted subtests
	// do not leave topology-specific resources behind unless NO_CLEANUP=1.
	t.Cleanup(func() {
		if noCleanup {
			logManualCleanupCommands(t, topology, workloadNS, proxyNS, llmSvcName, bindingName)
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
			"exporterEndpoint": fmt.Sprintf("http://%s.%s.svc.cluster.local:4317", OpenTelemetryCollectorServiceName, inferenceMonitoringNamespace()),
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
		inferenceService, inferencePort, discoveryErr = discoverInferenceService(tc, workloadNS, llmSvcName)
		return discoveryErr
	}, 2*time.Minute, 5*time.Second).Should(gomega.Succeed(), "generated inference Service should become discoverable")
	t.Logf("[%s] Using inference Service %s on port %d", topology, inferenceService, inferencePort)
	ocToken := getAuthToken(tc)
	require.NotEmpty(t, ocToken, "Kubernetes bearer token is required for the OAuth proxy request")

	routeHost := llmSvcName + "." + clusterDomain
	t.Logf("[%s] Creating OAuth proxy Deployment with upstream %s", topology, inferenceService)
	upstream := fmt.Sprintf("https://%s.%s.svc.cluster.local:%d", inferenceService, workloadNS, inferencePort)
	createOAuthProxy(t, tc, oauthProxyConfig{
		namespace:          proxyNS,
		proxyName:          proxyName,
		serviceAccountName: saName,
		bindingName:        bindingName,
		tlsSecretName:      tlsSecretName,
		cookieSecretName:   cookieSecretName,
		routeHost:          routeHost,
		upstream:           upstream,
	})

	t.Logf("[%s] Waiting for LLMInferenceService Ready=True", topology)
	tc.EnsureResourceConditionMet(
		gvkLLMInferenceService,
		llmSvcNN,
		"Ready",
		metav1.ConditionTrue,
		WithEventuallyTimeout(10*time.Minute),
	)
	t.Logf("[%s] LLMInferenceService is Ready; sending authenticated completion request", topology)
	routeCAs := ingressCAPool(t, tc.Context(), tc.Client())
	send := func() error { return sendCompletion(tc.Context(), routeHost, ocToken, routeCAs) }
	g.Eventually(send, 2*time.Minute, 5*time.Second).Should(gomega.Succeed(), "Should successfully send completion request through OAuth proxy")
	var tracedRequestTraceID string
	var tracedRequestParentSpanID string
	if topology == "single-node" {
		traceparent, err := newTestTraceparent()
		require.NoError(t, err, "failed to generate randomized test trace context")
		tracedRequestTraceID, err = traceIDFromTraceparent(traceparent)
		require.NoError(t, err, "failed to parse trace ID from traceparent")
		tracedRequestParentSpanID, err = parentSpanIDFromTraceparent(traceparent)
		require.NoError(t, err, "failed to parse parent span ID from traceparent")
		t.Logf("[%s] Sending completion request with trace ID %s", topology, tracedRequestTraceID)
		g.Eventually(func() error {
			return sendCompletion(tc.Context(), routeHost, ocToken, routeCAs, traceparent)
		}, 2*time.Minute, 5*time.Second).Should(gomega.Succeed(), "Should successfully send completion request with traceparent through OAuth proxy")
	}

	// 3. Verification via OATS
	t.Logf("[%s] Running OATS verification", topology)
	g.Eventually(func() error {
		if tracedRequestTraceID != "" {
			casePath := writeOatsTraceCaseFile(t, topology, llmSvcName, tracedRequestTraceID, tracedRequestParentSpanID)
			return runOatsCaseFile(tc.Context(), t, oatsBin, casePath, oatsEnv, projectRoot)
		}
		return runOatsCase(tc.Context(), t, oatsBin, topology, oatsEnv, projectRoot)
	}, 5*time.Minute, 15*time.Second).Should(gomega.Succeed(), "OATS verification should succeed within 2 minutes")
	if tracedRequestTraceID != "" {
		// OATS prepares the platform-appropriate gcx binary before this direct
		// trace lookup runs.
		g.Eventually(func() error {
			return assertTraceIDInTempo(tc.Context(), t, tracedRequestTraceID, oatsEnv)
		}, 5*time.Minute, 15*time.Second).Should(gomega.Succeed(), "injected trace ID should be retrievable from Tempo via gcx")
		g.Eventually(func() error {
			return assertVLLMSpanHasInjectedParent(tc.Context(), tracedRequestTraceID, tracedRequestParentSpanID, oatsEnv)
		}, 5*time.Minute, 15*time.Second).Should(gomega.Succeed(), "vLLM llm_request span should retain the injected parent span ID")
	}

	// 4. Pod Termination & Scrape Recovery
	if topology == "single-node" {
		t.Logf("[%s] Trace validation complete; skipping metrics-only scrape recovery", topology)
	} else {
		t.Logf("[%s] Restarting vLLM pod for scrape recovery", topology)
		restartVLLMPod(t, tc, workloadNS, llmSvcName)

		t.Logf("[%s] Sending completion request after pod restart", topology)
		g.Eventually(func() error {
			return send()
		}, 2*time.Minute, 5*time.Second).Should(gomega.Succeed(), "Second completion request should succeed after pod restart")

		g.Eventually(func() error {
			return runOatsCase(tc.Context(), t, oatsBin, topology, oatsEnv, projectRoot)
		}, 2*time.Minute, 10*time.Second).Should(gomega.Succeed(), "Scrape recovery verification should succeed after pod restart")
	}

	// 5. Teardown & PodMonitor Cleanup
	if noCleanup {
		t.Logf("[%s] NO_CLEANUP=1; leaving LLMInferenceService and generated resources in place", topology)
		return
	}
	t.Logf("[%s] Deleting LLMInferenceService and waiting for PodMonitor cleanup", topology)
	tc.DeleteResource(WithMinimalObject(gvkLLMInferenceService, llmSvcNN), WithIgnoreNotFound(true), WithWaitForDeletion(true))

	g.Eventually(func() bool {
		pmList := &unstructured.UnstructuredList{}
		pmList.SetGroupVersionKind(gvk.PodMonitorCoreos)
		if err := tc.Client().List(tc.Context(), pmList, client.InNamespace(workloadNS)); err != nil {
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

func logManualCleanupCommands(t *testing.T, topology, workloadNS, proxyNS, llmSvcName, bindingName string) {
	t.Helper()
	t.Logf(
		"[%s] NO_CLEANUP=1; retained LLMInferenceService/%s in namespace %s, OAuth proxy resources in namespace %s, and ClusterRoleBinding/%s",
		topology, llmSvcName, workloadNS, proxyNS, bindingName,
	)
	t.Logf(
		"[%s] Manual cleanup: oc delete llminferenceservice %s -n %s; oc delete namespace %s; oc delete clusterrolebinding %s",
		topology, llmSvcName, workloadNS, proxyNS, bindingName,
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
func warnIfOperatorPredatesNamespace(t *testing.T, tc *TestContext, platformOp platformOperator, namespace string) {
	t.Helper()

	ns, err := tc.fetchResource(t, gvk.Namespace, types.NamespacedName{Name: namespace})
	if err != nil {
		return
	}
	pods := &unstructured.UnstructuredList{}
	pods.SetGroupVersionKind(gvk.Pod.GroupVersion().WithKind(gvk.Pod.Kind + "List"))
	if err := tc.Client().List(tc.Context(), pods, client.InNamespace(platformOp.Namespace)); err != nil {
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
			namespace, ns.GetCreationTimestamp().Format(time.RFC3339), platformOp.Namespace,
			pod.GetName(), started, platformOp.Deployment, platformOp.Namespace)
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

// llmInferenceWorkloadNamespace is where LLMInferenceService e2e deploys workloads so
// PodMonitors are scraped as tenant metrics (built-in namespaces like default are excluded).
func llmInferenceWorkloadNamespace() string {
	return inferenceApplicationsNamespace()
}

func inferenceApplicationsNamespace() string {
	if testOpts.applicationsNamespace != "" {
		return testOpts.applicationsNamespace
	}
	return defaultInferenceApplicationsNamespace
}

// inferenceStorageClassName is the StorageClass the LokiStack PVCs bind to.
// setupInferencePrerequisites sets it from the cluster's default class before any manifest is
// applied; it is a package variable rather than a flag because the value is discovered, not
// configured.
var inferenceStorageClassName string

// inferenceStorageClass resolves the class renderManifest substitutes into the manifests. Empty
// means "use the manifest literal", which keeps rendering a no-op for the offline tests.
func inferenceStorageClass() string {
	if inferenceStorageClassName != "" {
		return inferenceStorageClassName
	}
	return lokiStorageClassName
}

// renderManifest reads a manifest and rewrites the default namespaces and StorageClass to the
// ones this run resolved.
//
// Substitution is textual because the namespace is not only a field: it is embedded in the
// exporter endpoints (http://lgtm.<ns>.svc.cluster.local:4317) that dsci.yaml and monitoring.yaml
// both carry. It runs in one direction only -- redhat-ods-* to the configured value, never the
// reverse -- which is why the manifests keep the RHOAI namespaces as their literals. Rewriting
// toward them would have to match "opendatahub", a substring of every apiVersion in the
// directory.
//
// The StorageClass rides the same mechanism for a different reason: dsci.yaml and monitoring.yaml
// must declare it identically (TestMonitoringManifestMatchesDSCI), and every apply path funnels
// through here, so one replacement keeps them in step.
func renderManifest(path string) ([]byte, error) {
	manifest, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return []byte(strings.NewReplacer(
		defaultInferenceMonitoringNamespace, inferenceMonitoringNamespace(),
		defaultInferenceApplicationsNamespace, inferenceApplicationsNamespace(),
		lokiStorageClassName, inferenceStorageClass(),
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

// setMaaSUsageLogging writes spec.usageLogging on the cluster-scoped MaaS Config CR.
//
// It writes the single field rather than replacing the spec: the maas-controller owns
// spec.limitadorScrapeInterval and the maas.opendatahub.io/default-aitenant-bootstrapped
// annotation on the same object, and there is no manifest here to declare them from. That is also
// why this is the one cluster-scoped change the suite rolls back -- nothing in the repo declares
// it, so leaving it on would silently enable MaaS logging for whatever runs next.
//
// Passing remove deletes the field instead of writing false, for restoring a Config that never
// carried it.
func setMaaSUsageLogging(tc *TestContext, enabled, remove bool) error {
	nn := types.NamespacedName{Name: maasConfigCRName}

	var conflictErr error
	for range monitoringUpdateAttempts {
		config := &unstructured.Unstructured{}
		config.SetGroupVersionKind(gvkMaaSConfig)
		if err := tc.Client().Get(tc.Context(), nn, config); err != nil {
			return fmt.Errorf("get MaaS Config %s: %w", nn, err)
		}

		// Already in the wanted state: the common case both for re-runs against a configured
		// cluster and for a restore that has nothing to undo.
		current, found, err := unstructured.NestedBool(config.Object, "spec", "usageLogging")
		if err == nil && found == !remove && current == enabled {
			return nil
		}
		if remove {
			unstructured.RemoveNestedField(config.Object, "spec", "usageLogging")
		} else if err := unstructured.SetNestedField(config.Object, enabled, "spec", "usageLogging"); err != nil {
			return fmt.Errorf("set spec.usageLogging on MaaS Config %s: %w", nn, err)
		}

		err = tc.Client().Update(tc.Context(), config)
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

// registerUsageLogsFixtureCleanup removes the SeaweedFS fixtures and the Loki S3 Secret once the
// whole suite has finished. Call it before startSeaweedFS, so an abort partway through setup
// still tears down whatever got as far as existing.
//
// TestMonitoring registers the same teardown per group, because each of its groups owns a
// separate fixture with its own bucket (monitoring_test.go:1345, monitoring_usage_logs_test.go:28).
// Here the fixture is suite-scoped -- one bucket, needed from setup through the last topology --
// so the teardown is too. The pre-clean in startSeaweedFS covers only the case where neither
// runs, such as a killed binary. The Secret is not part of the fixture, so it is deleted here
// alongside it.
//
// The LokiStack is deliberately left behind: it is the operand of a manifest-declared spec, and
// the next run recreates this backend at the same Service DNS name.
func registerUsageLogsFixtureCleanup(t *testing.T, tc *TestContext) {
	t.Helper()

	namespace := tc.MonitoringNamespace
	t.Cleanup(func() {
		if !testOpts.cleanupTestState {
			// "not deleting" rather than "retained": this runs even when setup aborted before
			// startSeaweedFS, in which case some of these were never created.
			t.Logf("-cleanup-test-state=false; not deleting pods %s and %s, Service %s, or Secret %s in %s",
				seaweedFSPodName, seaweedFSBucketPod, seaweedFSServiceName, lokiS3SecretName, namespace)
			return
		}
		t.Logf("Removing the usage-logs S3 fixtures from %s", namespace)
		tc.cleanupSeaweedFS()
		tc.DeleteResource(
			WithMinimalObject(gvk.Secret, types.NamespacedName{Name: lokiS3SecretName, Namespace: namespace}),
			WithIgnoreNotFound(true),
			WithWaitForDeletion(true),
		)
	})
}

// registerMaaSUsageLoggingRestore captures spec.usageLogging before the suite enables it and puts
// it back afterwards.
//
// It is called from enableMaaSUsageLoggingPrerequisite, between the wait that proves
// Config/default exists and the write that changes it. Both halves of that position matter.
// Capturing earlier races the maas-controller's bootstrap of the CR: the Get would fail on a
// fresh cluster, no cleanup would be registered, and the suite would then turn usage logging on
// with nothing to turn it off -- the cross-run leak this function exists to prevent. Capturing
// later would read back the suite's own value and restore nothing.
//
// t.Cleanup is LIFO and registerUsageLogsFixtureCleanup is registered much earlier, so MaaS
// logging is still turned off before the S3 backend it writes through is removed.
func registerMaaSUsageLoggingRestore(t *testing.T, tc *TestContext) {
	t.Helper()

	nn := types.NamespacedName{Name: maasConfigCRName}
	config := &unstructured.Unstructured{}
	config.SetGroupVersionKind(gvkMaaSConfig)
	if err := tc.Client().Get(tc.Context(), nn, config); err != nil {
		// The caller just waited for this CR, so this is not the bootstrap race. Continuing would
		// mean enabling cluster-wide identity logging with no recorded value to restore.
		t.Fatalf("failed to read MaaS Config %s to record spec.usageLogging: %v", maasConfigCRName, err)
	}
	original, hadValue, err := unstructured.NestedBool(config.Object, "spec", "usageLogging")
	if err != nil {
		t.Fatalf("failed to read spec.usageLogging from MaaS Config %s: %v", maasConfigCRName, err)
	}

	t.Cleanup(func() {
		if !testOpts.cleanupTestState {
			// Report the recorded prior value and the action being skipped, never the current
			// one: this handler does no read-back, and it also runs when the suite died before
			// the write, so any claim about what the field holds now can be false.
			if hadValue {
				t.Logf("-cleanup-test-state=false; not restoring spec.usageLogging=%t on MaaS Config %s",
					original, maasConfigCRName)
			} else {
				t.Logf("-cleanup-test-state=false; not removing spec.usageLogging from MaaS Config %s, "+
					"which did not carry the field before the run", maasConfigCRName)
			}
			return
		}
		if hadValue && original {
			return
		}
		t.Logf("Restoring spec.usageLogging on MaaS Config %s", maasConfigCRName)
		if err := setMaaSUsageLogging(tc, original, !hadValue); err != nil {
			t.Errorf("failed to restore spec.usageLogging on MaaS Config %s: %v", maasConfigCRName, err)
		}
	})
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

	// Only now is the CR known to exist, so this is the earliest point its prior value can be
	// recorded -- and it is still before the write below.
	registerMaaSUsageLoggingRestore(t, tc)

	t.Logf("Enabling spec.usageLogging on MaaS Config %s", maasConfigCRName)
	if err := setMaaSUsageLogging(tc, true, false); err != nil {
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

	t.Log("Installing Cluster Observability, Tempo, OpenTelemetry, LeaderWorkerSet, Loki, and Red Hat Connectivity Link operators")
	tc.EnsureOperatorInstalled(observabilityOpNamespace, observabilityOpName, observabilityOpChannel)
	// cert-manager is preinstalled by the CI pipeline from a different catalog
	// source than redhat-operators, so EnsureOperatorInstalled cannot match it.
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
func requireObservabilityAgreesOnNamespaces(t *testing.T, tc *TestContext, platformOp platformOperator) {
	t.Helper()

	for _, namespace := range []string{inferenceApplicationsNamespace(), inferenceMonitoringNamespace()} {
		warnIfOperatorPredatesNamespace(t, tc, platformOp, namespace)
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
	// Resolve the StorageClass before anything is applied: renderManifest substitutes it into
	// dsci.yaml and monitoring.yaml, and a cluster with no default class has to fail here rather
	// than 20 minutes later on a LokiStack whose PVCs were never going to bind.
	inferenceStorageClassName = tc.ensureDefaultStorageClass(t)
	t.Logf("Inference prerequisites use applicationsNamespace=%s, monitoring.namespace=%s, storageClassName=%s",
		inferenceApplicationsNamespace(), inferenceMonitoringNamespace(), inferenceStorageClassName)
	requireNamespacesMatchExistingDSCI(t, tc)

	platformOp, err := detectPlatformOperator(tc.Context(), tc.Client())
	require.NoError(t, err, "platform operator must be installed before inference prerequisites")
	t.Logf("Using platform operator deployment %s/%s (webhook Service %s/%s)",
		platformOp.Namespace, platformOp.Deployment, platformOp.Namespace, platformOp.WebhookService)

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
	registerUsageLogsFixtureCleanup(t, tc)
	tc.startSeaweedFS(t, lokiS3Bucket)
	tc.createLokiS3Secret(t, lokiS3SecretName, inferenceMonitoringNamespace())

	for _, name := range []string{"dsci.yaml", "dsc.yaml", "coo-uiplugins.yaml"} {
		applyPrerequisite(name)
	}
	// Diagnostic only: a DSCInitialization CRD that predates usageLogs prunes the stanza
	// silently, and applyMonitoringManifest below covers for it. Logging which path configured
	// usage logs is the difference between reading a run's log and guessing.
	logUsageLogsPropagation(t, tc)

	requireObservabilityAgreesOnNamespaces(t, tc, platformOp)
	ensureInferenceWorkloadNamespaceScrape(t, tc, llmInferenceWorkloadNamespace())

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
		fmt.Sprintf("platform operator %s/%s should be Available", platformOp.Namespace, platformOp.Deployment),
		schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
		types.NamespacedName{Name: platformOp.Deployment, Namespace: platformOp.Namespace},
		"Available",
	)
	waitFor(
		"LGTM should be Available",
		schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
		types.NamespacedName{Name: "lgtm", Namespace: inferenceMonitoringNamespace()},
		"Available",
	)
	for _, endpoint := range []struct{ service, namespace string }{
		{platformOp.WebhookService, platformOp.Namespace},
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
