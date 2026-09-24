package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestDCGMMetricsAreRetainedWithOriginalNames(t *testing.T) {
	templateBytes, err := resourcesFS.ReadFile(OpenTelemetryCollectorTemplate)
	if err != nil {
		t.Fatalf("failed to read template: %v", err)
	}

	templateContent := string(templateBytes)

	dcgmJob := extractSection(templateContent, "job_name: 'dcgm-exporter-accelerator-metrics'", "{{- end }}")
	if dcgmJob == "" {
		t.Fatal("dcgm-exporter-accelerator-metrics job must exist in template")
	}

	relabelSection := extractSection(dcgmJob, "relabel_configs:", "metric_relabel_configs:")
	if relabelSection == "" {
		t.Fatal("relabel_configs section must be extractable from dcgm job")
	}
	metricRelabelSection := extractSection(dcgmJob, "metric_relabel_configs:", "scrape_interval:")
	if metricRelabelSection == "" {
		t.Fatal("metric_relabel_configs section must be extractable from dcgm job")
	}

	dcgmMetrics := []string{
		"DCGM_FI_DEV_GPU_TEMP",
		"DCGM_FI_DEV_GPU_UTIL",
		"DCGM_FI_PROF_GR_ENGINE_ACTIVE",
		"DCGM_FI_DEV_MEM_COPY_UTIL",
		"DCGM_FI_DEV_FB_USED",
		"DCGM_FI_DEV_FB_FREE",
		"DCGM_FI_DEV_POWER_USAGE",
		"DCGM_FI_DEV_SM_CLOCK",
		"DCGM_FI_DEV_MEM_CLOCK",
	}

	for _, metric := range dcgmMetrics {
		if strings.Contains(relabelSection, metric) {
			t.Errorf("metric %s must not be referenced in relabel_configs", metric)
		}
		if !strings.Contains(metricRelabelSection, metric) {
			t.Errorf("metric %s must be retained by metric_relabel_configs", metric)
		}
	}

	if strings.Contains(relabelSection, "__name__") {
		t.Error("relabel_configs must not reference __name__ (unavailable at target-discovery stage)")
	}
	if strings.Contains(metricRelabelSection, "target_label: __name__") {
		t.Error("DCGM metric names must not be renamed in metric_relabel_configs")
	}

	keepRuleStart := strings.LastIndex(metricRelabelSection, "- source_labels: [__name__]")
	keepRule := ""
	if keepRuleStart != -1 {
		keepRule = metricRelabelSection[keepRuleStart:]
	}
	expectedKeepRegex := "(" + strings.Join(dcgmMetrics, "|") + ")"
	if !strings.Contains(keepRule, "action: keep") || !strings.Contains(keepRule, "regex: '"+expectedKeepRegex+"'") {
		t.Error("DCGM metrics must be included in the final keep list")
	}
}

func TestOpenTelemetryCollectorTemplateRendersValidGPUConfig(t *testing.T) {
	collector := renderCollectorTemplate(t)

	config, found, err := unstructured.NestedMap(collector.Object, "spec", "config")
	if err != nil || !found {
		t.Fatalf("collector config must be present in rendered resource: found=%t, error=%v", found, err)
	}
	processors, found, err := unstructured.NestedMap(config, "processors")
	if err != nil || !found {
		t.Fatalf("collector processors must be present: found=%t, error=%v", found, err)
	}
	transform, found, err := unstructured.NestedMap(processors, "transform/gpu_metrics")
	if err != nil || !found {
		t.Fatalf("GPU metric transform processor must be present: found=%t, error=%v", found, err)
	}
	metricStatements, ok := transform["metric_statements"].([]any)
	if !ok || len(metricStatements) != 1 {
		t.Fatalf("GPU metric transform must contain one metric statement block, got %v", transform["metric_statements"])
	}
	statementBlock, ok := metricStatements[0].(map[string]any)
	if !ok {
		t.Fatalf("GPU metric statement block has unexpected type: %T", metricStatements[0])
	}
	statements, ok := statementBlock["statements"].([]any)
	if !ok {
		t.Fatalf("GPU metric transform statements have unexpected type: %T", statementBlock["statements"])
	}
	expectedStatements := []string{
		`set(datapoint.value_double, datapoint.value_double / 100) where metric.name == "DCGM_FI_DEV_GPU_UTIL"`,
		`set(datapoint.value_double, datapoint.value_double / 100) where metric.name == "DCGM_FI_DEV_MEM_COPY_UTIL"`,
	}
	if len(statements) != len(expectedStatements) {
		t.Fatalf("expected %d GPU scaling statements, got %d", len(expectedStatements), len(statements))
	}
	for index, expected := range expectedStatements {
		if statements[index] != expected {
			t.Errorf("GPU metric statement %d: got %v, want %s", index, statements[index], expected)
		}
	}

	pipelines, found, err := unstructured.NestedMap(config, "service", "pipelines", "metrics")
	if err != nil || !found {
		t.Fatalf("metrics pipeline must be present: found=%t, error=%v", found, err)
	}
	if !containsString(pipelines["processors"], "transform/gpu_metrics") {
		t.Error("metrics pipeline must invoke the GPU metric transform processor")
	}
}

func TestDCGMMetricRelabelingPreservesDRAAttributionLabels(t *testing.T) {
	collector := renderCollectorTemplate(t)
	config, found, err := unstructured.NestedMap(collector.Object, "spec", "config")
	if err != nil || !found {
		t.Fatalf("collector config must be present in rendered resource: found=%t, error=%v", found, err)
	}
	scrapeConfigs, found, err := unstructured.NestedSlice(config, "receivers", "prometheus", "config", "scrape_configs")
	if err != nil || !found || len(scrapeConfigs) < 2 {
		t.Fatalf("expected DCGM scrape config in rendered resource: found=%t, error=%v", found, err)
	}
	var dcgmJob map[string]any
	for _, rawScrapeConfig := range scrapeConfigs {
		scrapeConfig, ok := rawScrapeConfig.(map[string]any)
		if ok && scrapeConfig["job_name"] == "dcgm-exporter-accelerator-metrics" {
			dcgmJob = scrapeConfig
			break
		}
	}
	if dcgmJob == nil {
		t.Fatal("DCGM scrape config must be present")
	}
	metricRelabelConfigs, found, err := unstructured.NestedSlice(dcgmJob, "metric_relabel_configs")
	if err != nil || !found {
		t.Fatalf("DCGM metric relabel configs must be present: found=%t, error=%v", found, err)
	}
	for _, rawRule := range metricRelabelConfigs {
		rule, ok := rawRule.(map[string]any)
		if !ok {
			t.Fatal("DCGM metric relabel rule has unexpected type")
		}
		if action, _ := rule["action"].(string); action == "labeldrop" || action == "labelkeep" {
			t.Errorf("metric relabel rule must not remove DRA attribution labels: %v", rule)
		}
		for _, label := range []string{"pod", "namespace", "dra_claim_name"} {
			if strings.Contains(fmt.Sprint(rule), label) {
				t.Errorf("metric relabel rule must not modify DRA attribution label %q: %v", label, rule)
			}
		}
	}
	keepRule, ok := metricRelabelConfigs[len(metricRelabelConfigs)-1].(map[string]any)
	if !ok {
		t.Fatal("final DCGM metric relabel rule has unexpected type")
	}
	if keepRule["action"] != "keep" {
		t.Fatalf("final DCGM metric relabel rule must be a keep rule: %v", keepRule)
	}
	sourceLabels, ok := keepRule["source_labels"].([]any)
	if !ok || len(sourceLabels) != 1 || sourceLabels[0] != "__name__" {
		t.Fatalf("DCGM keep rule must match only __name__ so DRA labels are preserved: %v", keepRule)
	}
}

func renderCollectorTemplate(t *testing.T) unstructured.Unstructured {
	t.Helper()
	resources, err := rendertemplate.Render(context.Background(), nil, []rendertemplate.TemplateSource{{
		FS:   resourcesFS,
		Path: OpenTelemetryCollectorTemplate,
	}}, map[string]any{
		"Namespace":              "redhat-ods-monitoring",
		"Metrics":                true,
		"MetricsStorage":         true,
		"AcceleratorMetrics":     true,
		"Traces":                 false,
		"CollectorReplicas":      1,
		"CollectorCPULimit":      "1",
		"CollectorMemoryLimit":   "1Gi",
		"CollectorCPURequest":    "100m",
		"CollectorMemoryRequest": "256Mi",
		"MetricsExporterNames":   []string{},
		"MetricsExporters":       map[string]string{},
		"TracesExporterNames":    []string{},
		"TracesExporters":        map[string]string{},
	})
	if err != nil {
		t.Fatalf("collector template must render as valid YAML: %v", err)
	}
	if len(resources) != 1 {
		t.Fatalf("expected one rendered collector resource, got %d", len(resources))
	}
	return resources[0]
}

func containsString(value any, want string) bool {
	values, ok := value.([]any)
	if !ok {
		return false
	}
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func extractSection(content, startMarker, endMarker string) string {
	startIdx := strings.Index(content, startMarker)
	if startIdx == -1 {
		return ""
	}
	endIdx := strings.Index(content[startIdx:], endMarker)
	if endIdx == -1 {
		return content[startIdx:]
	}
	return content[startIdx : startIdx+endIdx]
}
