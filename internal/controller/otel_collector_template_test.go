package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestDCGMRenameRulesInMetricRelabelConfigs(t *testing.T) {
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

	dcgmRenameMetrics := []string{
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

	for _, metric := range dcgmRenameMetrics {
		if strings.Contains(relabelSection, metric) {
			t.Errorf("rename rule for %s must not be in relabel_configs (__name__ unavailable at target-discovery stage)", metric)
		}
		if !strings.Contains(metricRelabelSection, metric) {
			t.Errorf("rename rule for %s must be in metric_relabel_configs (post-scrape stage)", metric)
		}
	}

	if strings.Contains(relabelSection, "__name__") {
		t.Error("relabel_configs must not reference __name__ (unavailable at target-discovery stage)")
	}

	engineRule := strings.Join([]string{
		"- action: replace",
		"                  regex: 'DCGM_FI_PROF_GR_ENGINE_ACTIVE'",
		"                  replacement: 'nvidia_gpu_engine_active_ratio'",
	}, "\n")
	if !strings.Contains(metricRelabelSection, engineRule) {
		t.Error("DCGM_FI_PROF_GR_ENGINE_ACTIVE must be renamed to nvidia_gpu_engine_active_ratio")
	}
	keepRuleStart := strings.LastIndex(metricRelabelSection, "- source_labels: [__name__]")
	keepRule := ""
	if keepRuleStart != -1 {
		keepRule = metricRelabelSection[keepRuleStart:]
	}
	if !strings.Contains(keepRule, "action: keep") || !strings.Contains(keepRule, "nvidia_gpu_engine_active_ratio") {
		t.Error("nvidia_gpu_engine_active_ratio must be included in the final keep list")
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
	metricStatements := stringValue(transform["metric_statements"])
	for _, metric := range []string{
		"nvidia_gpu_utilization_ratio",
		"nvidia_gpu_memory_utilization_ratio",
	} {
		if !strings.Contains(metricStatements, metric) {
			t.Errorf("GPU metric transform must scale %s", metric)
		}
	}
	if !strings.Contains(metricStatements, "datapoint.double_value / 100") {
		t.Error("GPU metric transform must scale percentage values to ratios")
	}
	if strings.Contains(metricStatements, "metric.name == \"nvidia_gpu_engine_active_ratio\"") {
		t.Error("nvidia_gpu_engine_active_ratio is already a ratio and must not be rescaled")
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
}

func renderCollectorTemplate(t *testing.T) unstructured.Unstructured {
	t.Helper()
	resources, err := rendertemplate.Render(context.Background(), nil, []rendertemplate.TemplateSource{{
		FS:   resourcesFS,
		Path: OpenTelemetryCollectorTemplate,
	}}, map[string]any{
		"Namespace":              "redhat-ods-monitoring",
		"Metrics":                true,
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

func stringValue(value any) string {
	return fmt.Sprint(value)
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
