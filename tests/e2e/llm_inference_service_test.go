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
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
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

func ensureOatsBinaries(t *testing.T, projectRoot string) (string, string) {
	t.Helper()

	localBin := filepath.Join(projectRoot, "bin")
	oatsBin := filepath.Join(localBin, "oats")
	gcxBin := filepath.Join(localBin, "gcx")

	needOats := false
	needGcx := false

	if _, err := os.Stat(oatsBin); os.IsNotExist(err) {
		needOats = true
	}
	if _, err := os.Stat(gcxBin); os.IsNotExist(err) {
		needGcx = true
	}

	if needOats || needGcx {
		t.Logf("Installing tool dependencies into %s...", localBin)
		if err := os.MkdirAll(localBin, 0755); err != nil {
			t.Fatalf("Failed to create bin directory %s: %v", localBin, err)
		}

		args := []string{"install"}
		if needOats {
			args = append(args, "github.com/grafana/oats")
		}
		if needGcx {
			args = append(args, "github.com/grafana/gcx/cmd/gcx")
		}

		cmd := exec.CommandContext(t.Context(), "go", args...)
		cmd.Dir = projectRoot
		cmd.Env = append(os.Environ(), "GOBIN="+localBin)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("Failed to install oats/gcx tools: %v\nOutput: %s", err, string(out))
		}
	}

	return oatsBin, gcxBin
}

func buildOatsEnv(t *testing.T) []string {
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
		cmd := exec.CommandContext(t.Context(), "oc", "get", "route", "lgtm", "-n", "redhat-ods-monitoring", "-o", "jsonpath={.spec.host}")
		out, err := cmd.Output()
		if err == nil {
			host := strings.TrimSpace(string(out))
			if host != "" {
				envMap["GRAFANA_SERVER"] = "https://" + host
			}
		}
		if envMap["GRAFANA_SERVER"] == "" {
			t.Log("Warning: GRAFANA_SERVER is not set and could not be resolved from OpenShift route 'lgtm'")
		}
	}

	if envMap["GRAFANA_TOKEN"] == "" {
		cmd := exec.CommandContext(t.Context(), "oc", "whoami", "-t")
		out, err := cmd.Output()
		if err == nil {
			token := strings.TrimSpace(string(out))
			if token != "" {
				envMap["GRAFANA_TOKEN"] = token
			}
		}
		if envMap["GRAFANA_TOKEN"] == "" {
			t.Log("Warning: GRAFANA_TOKEN is not set and could not be resolved via 'oc whoami -t'")
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

	cmd := exec.CommandContext(tc.Context(), "oc", "get", "ingresses.config.openshift.io", "cluster", "-o", "jsonpath={.spec.domain}")
	out, err := cmd.Output()
	if err == nil {
		domain := strings.TrimSpace(string(out))
		if domain != "" {
			return domain, nil
		}
	}

	return "", errors.New("failed to discover cluster domain")
}

func getOCToken(ctx context.Context) (string, error) {
	if token := os.Getenv("OC_TOKEN"); token != "" {
		return token, nil
	}

	cmd := exec.CommandContext(ctx, "oc", "whoami", "-t")
	out, err := cmd.Output()
	if err == nil {
		token := strings.TrimSpace(string(out))
		if token != "" {
			return token, nil
		}
	}

	return "", errors.New("failed to obtain OpenShift authentication token")
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
		// The test route uses a cluster-generated certificate that is not trusted locally.
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec // required for the test route's cluster certificate
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

func runOatsCase(ctx context.Context, t *testing.T, oatsBin, topology, gcxBin string, oatsEnv []string, projectRoot string) error {
	t.Helper()

	cmd := exec.CommandContext(ctx, oatsBin, "-vv", "--gcx", gcxBin, "--tags", topology, "--gcx-context", "default")
	cmd.Dir = projectRoot
	cmd.Env = oatsEnv

	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &outBuf

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("OATS topology %s failed: %w\nOutput: %s", topology, err, outBuf.String())
	}
	return nil
}

func restartVLLMPod(t *testing.T, tc *TestContext, namespace, llmSvcName string) {
	t.Helper()
	g := gomega.NewWithT(t)

	podList := &unstructured.UnstructuredList{}
	podList.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "PodList"})

	err := tc.Client().List(tc.Context(), podList, client.InNamespace(namespace))
	g.Expect(err).NotTo(gomega.HaveOccurred(), "failed to list pods in namespace %s", namespace)

	var targetPod *unstructured.Unstructured
	for i := range podList.Items {
		p := &podList.Items[i]
		if strings.Contains(p.GetName(), llmSvcName) || strings.Contains(p.GetName(), "vllm") {
			targetPod = p
			break
		}
	}

	if targetPod == nil && len(podList.Items) > 0 {
		targetPod = &podList.Items[0]
	}

	g.Expect(targetPod).NotTo(gomega.BeNil(), "expected to find a vLLM pod to terminate")

	podName := targetPod.GetName()
	t.Logf("Deleting pod %s in namespace %s for scrape recovery test...", podName, namespace)

	err = tc.Client().Delete(tc.Context(), targetPod)
	g.Expect(err).NotTo(gomega.HaveOccurred(), "failed to delete pod %s", podName)

	g.Eventually(func() bool {
		newPodList := &unstructured.UnstructuredList{}
		newPodList.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "PodList"})
		if err := tc.Client().List(tc.Context(), newPodList, client.InNamespace(namespace)); err != nil {
			return false
		}
		for i := range newPodList.Items {
			p := &newPodList.Items[i]
			if p.GetName() != podName && (strings.Contains(p.GetName(), llmSvcName) || strings.Contains(p.GetName(), "vllm")) {
				conditions, found, _ := unstructured.NestedSlice(p.Object, "status", "conditions")
				if !found {
					continue
				}
				for _, c := range conditions {
					cMap, ok := c.(map[string]any)
					if ok && cMap["type"] == "Ready" && cMap["status"] == "True" {
						return true
					}
				}
			}
		}
		return false
	}, 3*time.Minute, 5*time.Second).Should(gomega.BeTrue(), "New vLLM pod should be recreated and reach Ready state")
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
func runLLMInferenceServiceTopologyTest(t *testing.T, tc *TestContext, topology, projectRoot, oatsBin, gcxBin string, oatsEnv []string) {
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
	t.Logf("Starting %s topology with LLMInferenceService %s", topology, llmSvcNN)
	if topology == "multi-node" {
		checkCtx, cancel := context.WithTimeout(tc.Context(), 10*time.Second)
		err := runOC(checkCtx, "get", "crd", "leaderworkersets.leaderworkerset.x-k8s.io")
		cancel()
		if err != nil {
			t.Fatalf("multi-node LLMInferenceService requires the LeaderWorkerSet CRD/operator: %v", err)
		}
	}

	// Register cleanup before creating anything so failures or aborted subtests
	// do not leave topology-specific resources behind.
	t.Cleanup(func() {
		t.Logf("[%s] Cleaning up topology resources", topology)
		for _, resource := range []struct {
			gvk  schema.GroupVersionKind
			name string
			ns   string
		}{
			{gvk.Route, proxyName, "default"}, {gvk.Deployment, proxyName, "default"}, {gvk.Service, proxyName, "default"},
			{gvk.Secret, tlsSecretName, "default"}, {gvk.Secret, cookieSecretName, "default"}, {gvk.ClusterRoleBinding, bindingName, ""}, {gvk.ServiceAccount, saName, "default"},
			{gvkLLMInferenceService, llmSvcName, "default"},
		} {
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
			"exporterEndpoint": "http://data-science-collector.redhat-ods-monitoring.svc.cluster.local:4317",
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
	ocToken, err := getOCToken(tc.Context())
	require.NoError(t, err, "failed to obtain OpenShift authentication token")

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
		return runOatsCase(tc.Context(), t, oatsBin, topology, gcxBin, oatsEnv, projectRoot)
	}, 2*time.Minute, 10*time.Second).Should(gomega.Succeed(), "OATS verification should succeed within 2 minutes")

	// 4. Pod Termination & Scrape Recovery
	t.Logf("[%s] Restarting vLLM pod for scrape recovery", topology)
	restartVLLMPod(t, tc, "default", llmSvcName)

	t.Logf("[%s] Sending completion request after pod restart", topology)
	g.Eventually(func() error {
		return send()
	}, 2*time.Minute, 5*time.Second).Should(gomega.Succeed(), "Second completion request should succeed after pod restart")

	g.Eventually(func() error {
		return runOatsCase(tc.Context(), t, oatsBin, topology, gcxBin, oatsEnv, projectRoot)
	}, 2*time.Minute, 10*time.Second).Should(gomega.Succeed(), "Scrape recovery verification should succeed after pod restart")

	// 5. Teardown & PodMonitor Cleanup
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

func runOC(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, "oc", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("oc %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

func ensureOAuthProxySecret(ctx context.Context) error {
	const (
		secretName = "oauth-proxy-secrets"
		namespace  = "redhat-ods-monitoring"
	)
	if err := runOC(ctx, "get", "secret", secretName, "-n", namespace); err == nil {
		return nil
	}

	var sessionSecret [32]byte
	if _, err := rand.Read(sessionSecret[:]); err != nil {
		return fmt.Errorf("generate OAuth proxy session secret: %w", err)
	}
	encodedSecret := base64.StdEncoding.EncodeToString(sessionSecret[:])
	if err := runOC(ctx, "create", "secret", "generic", secretName, "-n", namespace, "--from-literal=session_secret="+encodedSecret); err != nil {
		return fmt.Errorf("create OAuth proxy session secret: %w", err)
	}
	return nil
}

func inferencePrerequisiteDir(projectRoot string) string {
	return filepath.Join(projectRoot, "tests", "e2e", "prerequisites", "inference")
}

func setupInferencePrerequisites(t *testing.T, projectRoot string) {
	t.Helper()
	g := gomega.NewWithT(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	waitFor := func(description string, args ...string) {
		t.Logf("Waiting for %s", description)
		g.Eventually(func() error {
			return runOC(ctx, args...)
		}, 5*time.Minute, 2*time.Second).Should(gomega.Succeed(), description)
		t.Logf("Completed wait: %s", description)
	}
	dir := inferencePrerequisiteDir(projectRoot)
	applyManifest := func(name string) {
		path := filepath.Join(dir, name)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("inference prerequisite manifest %s is unavailable: %v", path, err)
		}
		if err := runOC(ctx, "apply", "-f", path); err != nil {
			t.Fatalf("failed to apply inference prerequisite %s: %v", name, err)
		}
	}

	t.Log("Setting up DSCI, DSC, and UIPlugin prerequisites")
	for _, crd := range []string{
		"dscinitializations.dscinitialization.opendatahub.io",
		"datascienceclusters.datasciencecluster.opendatahub.io",
		"uiplugins.observability.openshift.io",
	} {
		waitFor("CRD "+crd+" should be established", "wait", "--for=condition=Established", "crd/"+crd, "--timeout=300s")
	}
	for _, name := range []string{"dsci.yaml", "dsc.yaml", "coo-uiplugins.yaml"} {
		applyManifest(name)
	}
	waitFor("DSCI default-dsci should be Ready", "wait", "--for=condition=Ready", "dsci/default-dsci", "--timeout=300s")
	waitFor("DSC default-dsc should be Ready", "wait", "--for=condition=Ready", "dsc/default-dsc", "--timeout=300s")

	t.Log("Setting up LGTM and remaining inference prerequisites")
	applyManifest("lwsoperator.yaml")
	waitFor(
		"CRD leaderworkersets.leaderworkerset.x-k8s.io should be established",
		"wait", "--for=condition=Established", "crd/leaderworkersets.leaderworkerset.x-k8s.io", "--timeout=300s",
	)
	waitFor("CRD llminferenceservices.serving.kserve.io should be established", "wait", "--for=condition=Established", "crd/llminferenceservices.serving.kserve.io", "--timeout=300s")
	if err := ensureOAuthProxySecret(ctx); err != nil {
		t.Fatalf("failed to ensure OAuth proxy cookie Secret: %v", err)
	}
	for _, name := range []string{"lgtm.yaml", "networkpolicy.yaml"} {
		applyManifest(name)
	}
	waitFor("RHOAI operator should be Available", "wait", "--for=condition=Available", "deployment/rhods-operator", "-n", "redhat-ods-operator", "--timeout=300s")
	waitFor("LGTM should be Available", "wait", "--for=condition=Available", "deployment/lgtm", "-n", "redhat-ods-monitoring", "--timeout=300s")
	waitFor("LGTM Route should have a host", "wait", "--for=jsonpath={.spec.host}", "route/lgtm", "-n", "redhat-ods-monitoring", "--timeout=300s")
	for _, endpoint := range []struct{ service, namespace string }{
		{"rhods-operator-service", "redhat-ods-operator"},
		{"kserve-webhook-server-service", "redhat-ods-applications"},
		{"llmisvc-webhook-server-service", "redhat-ods-applications"},
		{"lgtm", "redhat-ods-monitoring"},
	} {
		g.Eventually(func() bool {
			// endpoint values come from a fixed test list.
			//nolint:gosec // endpoint values come from a fixed test list.
			cmd := exec.CommandContext(
				ctx, "oc", "get", "endpoints", endpoint.service, "-n", endpoint.namespace,
				"-o", "jsonpath={.subsets[*].addresses[*].ip}",
			)
			output, err := cmd.Output()
			return err == nil && strings.TrimSpace(string(output)) != ""
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

	setupInferencePrerequisites(t, projectRoot)
	oatsBin, gcxBin := ensureOatsBinaries(t, projectRoot)
	oatsEnv := buildOatsEnv(t)

	topologies := []string{"single-node", "multi-node", "disaggregated"}

	for _, topology := range topologies {
		t.Run(topology, func(t *testing.T) {
			runLLMInferenceServiceTopologyTest(t, tc, topology, projectRoot, oatsBin, gcxBin, oatsEnv)
		})
	}
}
