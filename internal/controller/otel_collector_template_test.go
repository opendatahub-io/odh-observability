package controller

import (
	"strings"
	"testing"
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
