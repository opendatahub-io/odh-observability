package e2e_test

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	inferenceServicePodLabelKey = "serving.kserve.io/inferenceservice"
	vLLMRequestSpanName         = "llm_request"
)

type parsedTraceSpan struct {
	Name         string
	SpanID       string
	ParentSpanID string
}

// writeInferenceServiceOatsTraceCaseFile builds an ISVC-specific OATS case
// scoped to the randomized workload and injected trace ID.
func writeInferenceServiceOatsTraceCaseFile(t *testing.T, tag, podNamePrefix, traceID string) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, tag, "oats-case.yaml")
	requireDir(t, filepath.Dir(path))

	traceHex, _ := traceIDForTraceQL(traceID)
	content := fmt.Sprintf(`name: %s vLLM tracing (run-specific)
tags: ["e2e", "vllm", "%s"]

fixture:
  remote:
    endpoint: default

expected:
  traces:
    - traceql: '{ name = "%s" && resource.k8s.pod.name =~ "%s.*" && span:parentID = "0000000000000000" }'
      count: ">= 1"
    - traceql: '{ trace:id = "%s" && name = "%s" && resource.k8s.pod.name =~ "%s.*" }'
      count: ">= 1"
    - traceql: '{ trace:id = "%s" && name = "%s" && span:parentID != "0000000000000000" }'
      count: ">= 1"
`, tag, tag, vLLMRequestSpanName, podNamePrefix, traceHex, vLLMRequestSpanName, podNamePrefix, traceHex, vLLMRequestSpanName)
	requireWriteFile(t, path, content)
	return path
}

func requireDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("create directory %s: %v", dir, err)
	}
}

func requireWriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func fetchTraceJSON(ctx context.Context, traceID string, oatsEnv []string) ([]byte, error) {
	traceHex, traceUUID := traceIDForTraceQL(traceID)
	gcxBin, err := gcxBinaryPath(ctx)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, id := range []string{traceHex, traceUUID} {
		cmd := exec.CommandContext(ctx, gcxBin, "traces", "get", id, "-o", "json")
		cmd.Env = oatsEnv
		out, err := cmd.CombinedOutput()
		if err == nil {
			return out, nil
		}
		lastErr = fmt.Errorf("gcx traces get %s: %w\n%s", id, err, strings.TrimSpace(string(out)))
	}
	return nil, lastErr
}

func parseTraceSpansFromGCXJSON(raw []byte) ([]parsedTraceSpan, error) {
	var root any
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil, fmt.Errorf("decode trace JSON: %w", err)
	}
	spans := make([]parsedTraceSpan, 0)
	collectTraceSpans(root, &spans)
	if len(spans) == 0 {
		return nil, fmt.Errorf("no spans found in gcx trace JSON (%d bytes)", len(raw))
	}
	return spans, nil
}

func collectTraceSpans(node any, spans *[]parsedTraceSpan) {
	switch typed := node.(type) {
	case map[string]any:
		if span := spanFromMap(typed); span != nil {
			*spans = append(*spans, *span)
		}
		for _, value := range typed {
			collectTraceSpans(value, spans)
		}
	case []any:
		for _, value := range typed {
			collectTraceSpans(value, spans)
		}
	}
}

func spanFromMap(fields map[string]any) *parsedTraceSpan {
	name := firstString(fields, "name", "spanName")
	spanID := normalizeSpanID(firstString(fields, "spanID", "spanId", "span_id", "id"))
	if name == "" || spanID == "" {
		return nil
	}
	parent := normalizeSpanID(firstString(fields, "parentSpanID", "parentSpanId", "parent_span_id", "parentID", "parentId"))
	return &parsedTraceSpan{Name: name, SpanID: spanID, ParentSpanID: parent}
}

func firstString(fields map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := fields[key].(string); ok && value != "" {
			return value
		}
	}
	return ""
}

func normalizeSpanID(id string) string {
	id = strings.TrimSpace(id)
	if decoded, err := base64.StdEncoding.DecodeString(id); err == nil && len(decoded) == 8 {
		id = hex.EncodeToString(decoded)
	} else if decoded, err := base64.RawStdEncoding.DecodeString(id); err == nil && len(decoded) == 8 {
		id = hex.EncodeToString(decoded)
	}
	id = strings.TrimPrefix(strings.ToLower(id), "0x")
	if id == "" || id == "0000000000000000" {
		return ""
	}
	return id
}

func parentSpanIDFromTraceparent(traceparent string) (string, error) {
	parts := strings.Split(traceparent, "-")
	if len(parts) != 4 || parts[0] != "00" || len(parts[2]) != 16 {
		return "", fmt.Errorf("invalid parent span ID in traceparent %q", traceparent)
	}
	return strings.ToLower(parts[2]), nil
}

func assertInferenceServiceTraceIDInTempo(ctx context.Context, t *testing.T, traceID string, oatsEnv []string) error {
	t.Helper()
	raw, err := fetchTraceJSON(ctx, traceID, oatsEnv)
	if err != nil {
		return err
	}
	spans, err := parseTraceSpansFromGCXJSON(raw)
	if err != nil {
		return err
	}
	t.Logf("gcx returned trace %s with %d span(s)", traceID, len(spans))
	return nil
}

func assertVLLMSpanHasInjectedParent(ctx context.Context, traceID, parentSpanID string, oatsEnv []string) error {
	raw, err := fetchTraceJSON(ctx, traceID, oatsEnv)
	if err != nil {
		return err
	}
	spans, err := parseTraceSpansFromGCXJSON(raw)
	if err != nil {
		return err
	}
	wantParent := normalizeSpanID(parentSpanID)
	for _, span := range spans {
		if span.Name == vLLMRequestSpanName && span.ParentSpanID == wantParent {
			return nil
		}
	}
	return fmt.Errorf("trace %s has no %q span with injected parent ID %s", traceID, vLLMRequestSpanName, wantParent)
}

func findInferenceServicePredictorPod(ctx context.Context, kube client.Client, namespace, serviceName string) (*corev1.Pod, string, error) {
	pods := &corev1.PodList{}
	if err := kube.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabels{
		inferenceServicePodLabelKey: serviceName,
	}); err != nil {
		return nil, "", fmt.Errorf("list InferenceService pods: %w", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		for _, container := range pod.Spec.Containers {
			for _, env := range container.Env {
				if env.Name == "OTEL_EXPORTER_OTLP_ENDPOINT" && env.Value != "" {
					return pod, container.Name, nil
				}
			}
		}
	}
	return nil, "", fmt.Errorf("no running InferenceService pod with OTEL_EXPORTER_OTLP_ENDPOINT for %s/%s", namespace, serviceName)
}

func runInferenceServiceOTLPNetworkPolicyProbe(t *testing.T, tc *TestContext, namespace, serviceName string) {
	t.Helper()

	pod, containerName, err := findInferenceServicePredictorPod(tc.Context(), tc.Client(), namespace, serviceName)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if !(&MonitoringTestCtx{TestContext: tc}).otlpNamespaceEnrolled(t, namespace) {
		t.Fatalf("namespace %s must be enrolled with %s=true before OTLP NetworkPolicy validation", namespace, otlpNamespaceLabel)
	}
	mtc := &MonitoringTestCtx{TestContext: tc}
	mtc.MonitoringNamespace = inferenceMonitoringNamespace()
	mtc.runActualOTLPWorkloadNetworkPolicyProbe(t, *pod, containerName)
}

func TestWriteOatsTraceCaseFileIncludesTraceID(t *testing.T) {
	t.Parallel()

	path := writeInferenceServiceOatsTraceCaseFile(t, "isvc", "vllm-trace-isvc-abc", "0123456789abcdef0123456789abcdef")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	content := string(raw)
	require.Contains(t, content, `resource.k8s.pod.name =~ "vllm-trace-isvc-abc.*" && span:parentID = "0000000000000000"`)
	require.Contains(t, content, `trace:id = "0123456789abcdef0123456789abcdef"`)
}

func TestWriteLLMOatsTraceCaseFileIncludesTraceID(t *testing.T) {
	t.Parallel()

	path := writeOatsTraceCaseFile(
		t,
		"single-node",
		"facebook-opt-125m-single-node-abc",
		"fedcba9876543210fedcba9876543210",
		"0123456789abcdef",
	)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	content := string(raw)
	require.Contains(t, content, `trace:id = "fedcba9876543210fedcba9876543210"`)
	require.Contains(t, content, `resource.k8s.pod.name =~ "facebook-opt-125m-single-node-abc.*"`)
	require.Contains(t, content, `span:parentID = "0123456789abcdef"`)
}

func TestGCXToolingUsesPinnedCacheBinary(t *testing.T) {
	t.Setenv("OATS_GCX", "")
	t.Setenv("PATH", "")
	cacheDir := t.TempDir()
	t.Setenv("OATS_CACHE_DIR", cacheDir)

	path, args, err := gcxTooling(t.Context())
	require.NoError(t, err)
	require.Contains(t, path, filepath.Join("tools", "gcx", oatsGCXVersion, runtime.GOOS+"_"+runtime.GOARCH))
	require.Contains(t, args, "--cache-dir")
	require.Contains(t, args, cacheDir)
	require.Contains(t, args, oatsGCXVersion)
	require.NotContains(t, args, path, "OATS must be allowed to download the initially missing cached binary")
}

func TestGCXToolingUsesMatchingPATHBinary(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test helper uses a POSIX shell script")
	}
	t.Setenv("OATS_GCX", "")
	binDir := t.TempDir()
	path := filepath.Join(binDir, "gcx")
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\necho 'gcx v"+oatsGCXVersion+"'\n"), 0o600))
	require.NoError(t, os.Chmod(path, 0o700))
	t.Setenv("PATH", binDir)

	gotPath, args, err := gcxTooling(t.Context())
	require.NoError(t, err)
	require.Equal(t, path, gotPath)
	require.Equal(t, []string{"--gcx-version", oatsGCXVersion, "--gcx-download", "auto"}, args)
}

func TestAssertVLLMSpanHasInjectedParentAcceptsOTLPBase64IDs(t *testing.T) {
	t.Parallel()

	const parentHex = "57190950d0c80215"
	parentBytes, err := hex.DecodeString(parentHex)
	require.NoError(t, err)
	raw := fmt.Appendf(nil, `{"spans":[{"name":"llm_request","spanId":"hrGjYEDrYZ4=","parentSpanId":%q}]}`,
		base64.StdEncoding.EncodeToString(parentBytes))
	spans, err := parseTraceSpansFromGCXJSON(raw)
	require.NoError(t, err)
	require.Len(t, spans, 1)
	require.Equal(t, parentHex, spans[0].ParentSpanID)
}
