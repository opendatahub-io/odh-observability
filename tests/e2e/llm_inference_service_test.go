package e2e_test

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
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
	gvkMaaSModelRef = schema.GroupVersionKind{
		Group:   "maas.opendatahub.io",
		Version: "v1alpha1",
		Kind:    "MaaSModelRef",
	}
	gvkMaaSSubscription = schema.GroupVersionKind{
		Group:   "maas.opendatahub.io",
		Version: "v1alpha1",
		Kind:    "MaaSSubscription",
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
	return "", fmt.Errorf("go.mod file not found in parent directories")
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

		cmd := exec.Command("go", args...)
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
		cmd := exec.Command("oc", "get", "route", "lgtm", "-n", "redhat-ods-monitoring", "-o", "jsonpath={.spec.host}")
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
		cmd := exec.Command("oc", "whoami", "-t")
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

	var envList []string
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

	cmd := exec.Command("oc", "get", "ingresses.config.openshift.io", "cluster", "-o", "jsonpath={.spec.domain}")
	out, err := cmd.Output()
	if err == nil {
		domain := strings.TrimSpace(string(out))
		if domain != "" {
			return domain, nil
		}
	}

	return "", fmt.Errorf("failed to discover cluster domain")
}

func getOCToken() (string, error) {
	if token := os.Getenv("OC_TOKEN"); token != "" {
		return token, nil
	}

	cmd := exec.Command("oc", "whoami", "-t")
	out, err := cmd.Output()
	if err == nil {
		token := strings.TrimSpace(string(out))
		if token != "" {
			return token, nil
		}
	}

	return "", fmt.Errorf("failed to obtain OpenShift authentication token")
}

func getMaaSAPIKey(clusterDomain, ocToken, subscriptionName string) (string, error) {
	maasURL := fmt.Sprintf("https://maas.%s/maas-api/v1/api-keys", clusterDomain)
	payload := map[string]any{
		"name":         "validation-key",
		"description":  "Key for validation",
		"expiresIn":    "1h",
		"subscription": subscriptionName,
	}
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Timeout: 30 * time.Second,
	}

	req, err := http.NewRequest("POST", maasURL, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+ocToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("API key request to %s failed with status %d: %s", maasURL, resp.StatusCode, string(body))
	}

	var result struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}
	if result.Key == "" {
		return "", fmt.Errorf("empty API key returned from %s: %s", maasURL, string(body))
	}

	return result.Key, nil
}

func sendMaaSCompletion(clusterDomain, apiKey string) error {
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Timeout: 30 * time.Second,
	}

	modelsURL := fmt.Sprintf("https://maas.%s/maas-api/v1/models", clusterDomain)
	req, err := http.NewRequest("GET", modelsURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("models request to %s failed with status %d: %s", modelsURL, resp.StatusCode, string(body))
	}

	var modelsResp struct {
		Data []struct {
			ID  string `json:"id"`
			URL string `json:"url"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &modelsResp); err != nil {
		return err
	}
	if len(modelsResp.Data) == 0 || modelsResp.Data[0].URL == "" {
		return fmt.Errorf("no model URL found in response from %s: %s", modelsURL, string(body))
	}

	modelURL := modelsResp.Data[0].URL
	modelID := modelsResp.Data[0].ID
	if modelID == "" {
		modelID = "publishers/default/models/facebook/opt-125m"
	}

	completionURL := strings.TrimRight(modelURL, "/") + "/v1/completions"

	payload := map[string]any{
		"model":       modelID,
		"prompt":      "San Francisco is a",
		"max_tokens":  7,
		"temperature": 0,
	}
	compBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	compReq, err := http.NewRequest("POST", completionURL, bytes.NewBuffer(compBytes))
	if err != nil {
		return err
	}
	compReq.Header.Set("Authorization", "Bearer "+apiKey)
	compReq.Header.Set("Content-Type", "application/json")

	compResp, err := client.Do(compReq)
	if err != nil {
		return err
	}
	defer compResp.Body.Close()

	compBody, err := io.ReadAll(compResp.Body)
	if err != nil {
		return err
	}

	if compResp.StatusCode != http.StatusOK {
		return fmt.Errorf("completions request to %s failed with status %d: %s", completionURL, compResp.StatusCode, string(compBody))
	}

	return nil
}

func runOatsCase(t *testing.T, oatsBin, caseAbsPath, gcxBin string, oatsEnv []string, projectRoot string) error {
	t.Helper()

	cmd := exec.Command(oatsBin, caseAbsPath, "--gcx", gcxBin, "--gcx-context", "default")
	cmd.Dir = projectRoot
	cmd.Env = oatsEnv

	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &outBuf

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("OATS case %s failed: %v\nOutput: %s", caseAbsPath, err, outBuf.String())
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

func runLLMInferenceServiceTopologyTest(t *testing.T, tc *TestContext, topology, projectRoot, oatsBin, gcxBin string, oatsEnv []string) {
	t.Helper()
	g := gomega.NewWithT(t)

	llmSvcName := "facebook-opt-125m-" + topology
	llmSvcNN := types.NamespacedName{Name: llmSvcName, Namespace: "default"}
	modelRefNN := types.NamespacedName{Name: llmSvcName, Namespace: "default"}
	subName := llmSvcName + "-subscription"
	subNN := types.NamespacedName{Name: subName, Namespace: "models-as-a-service"}

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
		"router": map[string]any{
			"scheduler": map[string]any{},
			"route":     map[string]any{},
			"gateway": map[string]any{
				"refs": []map[string]any{
					{
						"name":      "maas-default-gateway",
						"namespace": "openshift-ingress",
					},
				},
			},
		},
		"template": map[string]any{
			"containers": []map[string]any{
				{
					"name":  "main",
					"image": "registry.redhat.io/rhaii/vllm-cpu-rhel9:3.4.1-1780356811",
				},
			},
		},
	}

	if topology == "multi-node" {
		spec["worker"] = map[string]any{
			"template": map[string]any{
				"containers": []map[string]any{
					{
						"name":  "main",
						"image": "registry.redhat.io/rhaii/vllm-cpu-rhel9:3.4.1-1780356811",
					},
				},
			},
		}
	} else if topology == "disaggregated" {
		spec["prefill"] = map[string]any{
			"template": map[string]any{
				"containers": []map[string]any{
					{
						"name":  "main",
						"image": "registry.redhat.io/rhaii/vllm-cpu-rhel9:3.4.1-1780356811",
					},
				},
			},
		}
	}

	tc.ensureNamespaceExists("models-as-a-service")

	// 1. Deployment & Reconciliation
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvkLLMInferenceService, llmSvcNN),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.Object["spec"] = spec
			return nil
		}),
	)

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvkMaaSModelRef, modelRefNN),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.SetAnnotations(map[string]string{
				"openshift.io/display-name": "Facebook OPT 125M (Simulated)",
				"openshift.io/description":  "A simulated OPT-125M model for free-tier testing",
			})
			u.Object["spec"] = map[string]any{
				"modelRef": map[string]any{
					"kind": "LLMInferenceService",
					"name": llmSvcName,
				},
			}
			return nil
		}),
	)

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvkMaaSSubscription, subNN),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.Object["spec"] = map[string]any{
				"owner": map[string]any{
					"groups": []map[string]any{
						{"name": "system:authenticated"},
					},
					"users": []any{},
				},
				"modelRefs": []map[string]any{
					{
						"name":      llmSvcName,
						"namespace": "default",
						"tokenRateLimits": []map[string]any{
							{
								"limit":  int64(100),
								"window": "1m",
							},
						},
					},
				},
			}
			return nil
		}),
		WithCondition(jq.Match(`.status.phase == "Active"`)),
		WithCustomErrorMsg("MaaSSubscription %s should reach Active phase", subName),
	)

	tc.EnsureResourceConditionMet(
		gvkLLMInferenceService,
		llmSvcNN,
		"Ready",
		metav1.ConditionTrue,
	)

	// 2. Discovery & MaaS API Completions Execution
	clusterDomain, err := getClusterDomain(tc)
	require.NoError(t, err, "failed to discover cluster domain")

	ocToken, err := getOCToken()
	require.NoError(t, err, "failed to obtain OpenShift authentication token")

	var apiKey string
	g.Eventually(func() error {
		var err error
		apiKey, err = getMaaSAPIKey(clusterDomain, ocToken, subName)
		return err
	}, 2*time.Minute, 5*time.Second).Should(gomega.Succeed(), "Should successfully acquire MaaS API key")

	g.Eventually(func() error {
		return sendMaaSCompletion(clusterDomain, apiKey)
	}, 2*time.Minute, 5*time.Second).Should(gomega.Succeed(), "Should successfully send completion request via MaaS API")

	// 3. AC1 Metric Verification via OATS
	scrapeVerificationCase := filepath.Join(projectRoot, "tests/e2e/oats/vllm/e2e-scrape-verification/oats-case.yaml")
	g.Eventually(func() error {
		return runOatsCase(t, oatsBin, scrapeVerificationCase, gcxBin, oatsEnv, projectRoot)
	}, 2*time.Minute, 10*time.Second).Should(gomega.Succeed(), "AC1: OATS scrape verification should succeed within 2 minutes")

	// 4. Pod Termination & Scrape Recovery
	restartVLLMPod(t, tc, "default", llmSvcName)

	g.Eventually(func() error {
		return sendMaaSCompletion(clusterDomain, apiKey)
	}, 2*time.Minute, 5*time.Second).Should(gomega.Succeed(), "Second completion request should succeed after pod restart")

	g.Eventually(func() error {
		return runOatsCase(t, oatsBin, scrapeVerificationCase, gcxBin, oatsEnv, projectRoot)
	}, 2*time.Minute, 10*time.Second).Should(gomega.Succeed(), "Scrape recovery verification should succeed after pod restart")

	// 5. AC2 Metric Cardinality Validation via OATS
	cardinalityCase := filepath.Join(projectRoot, "tests/e2e/oats/vllm/cardinality/oats-case.yaml")
	g.Eventually(func() error {
		return runOatsCase(t, oatsBin, cardinalityCase, gcxBin, oatsEnv, projectRoot)
	}, 2*time.Minute, 10*time.Second).Should(gomega.Succeed(), "AC2: OATS metric cardinality validation should succeed")

	// 6. AC3 Teardown & PodMonitor Cleanup
	tc.DeleteResource(WithMinimalObject(gvkLLMInferenceService, llmSvcNN))
	tc.DeleteResource(WithMinimalObject(gvkMaaSModelRef, modelRefNN))
	tc.DeleteResource(WithMinimalObject(gvkMaaSSubscription, subNN))

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
	}, 1*time.Minute, 2*time.Second).Should(gomega.BeTrue(), "AC3: Associated PodMonitor should be deleted within 1 reconciliation cycle of LLMInferenceService deletion")
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

	oatsBin, gcxBin := ensureOatsBinaries(t, projectRoot)
	oatsEnv := buildOatsEnv(t)

	topologies := []string{"single-node", "multi-node", "disaggregated"}

	for _, topology := range topologies {
		t.Run(topology, func(t *testing.T) {
			runLLMInferenceServiceTopologyTest(t, tc, topology, projectRoot, oatsBin, gcxBin, oatsEnv)
		})
	}
}
