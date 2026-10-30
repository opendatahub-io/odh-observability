package e2e_test

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestCollectorMetricsPipelineMatchers(t *testing.T) {
	const exporter = "otlphttp/metrics-external"
	tests := []struct {
		name                 string
		pipelines            map[string]any
		include, exact       bool
		scrapeGone, otlpGone bool
	}{
		{
			name: "both pipelines",
			pipelines: map[string]any{
				"metrics/scrape": map[string]any{"exporters": []any{exporter}},
				"metrics/otlp":   map[string]any{"exporters": []any{exporter}},
			},
			include: true, exact: true,
		},
		{
			name: "extra exporter",
			pipelines: map[string]any{
				"metrics/scrape": map[string]any{"exporters": []any{exporter, "debug"}},
				"metrics/otlp":   map[string]any{"exporters": []any{exporter}},
			},
			include: true,
		},
		{
			name: "wrong otlp exporter",
			pipelines: map[string]any{
				"metrics/scrape": map[string]any{"exporters": []any{exporter}},
				"metrics/otlp":   map[string]any{"exporters": []any{"debug"}},
			},
		},
		{
			name: "scrape missing",
			pipelines: map[string]any{
				"metrics/otlp": map[string]any{"exporters": []any{exporter}},
			},
			scrapeGone: true,
		},
		{
			name: "otlp missing",
			pipelines: map[string]any{
				"metrics/scrape": map[string]any{"exporters": []any{exporter}},
			},
			otlpGone: true,
		},
		{
			name: "internal metrics only",
			pipelines: map[string]any{
				"metrics/internal": map[string]any{"exporters": []any{"prometheus/monitoring"}},
			},
			scrapeGone: true, otlpGone: true,
		},
		{
			name: "obsolete metrics pipeline",
			pipelines: map[string]any{
				"metrics": map[string]any{"exporters": []any{exporter}},
			},
			scrapeGone: true, otlpGone: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector := &unstructured.Unstructured{Object: map[string]any{
				"spec": map[string]any{"config": map[string]any{"service": map[string]any{"pipelines": tt.pipelines}}},
			}}
			checks := []struct {
				name  string
				match func(any) (bool, error)
				want  bool
			}{
				{"include", collectorMetricsPipelinesInclude(exporter).Match, tt.include},
				{"exact", collectorMetricsPipelinesOnly(exporter).Match, tt.exact},
				{"scrape absent", collectorPipelineAbsent("metrics/scrape").Match, tt.scrapeGone},
				{"otlp absent", collectorPipelineAbsent("metrics/otlp").Match, tt.otlpGone},
			}
			for _, check := range checks {
				got, err := check.match(collector)
				if err != nil || got != check.want {
					t.Errorf("%s: got %t, err %v; want %t", check.name, got, err, check.want)
				}
			}
		})
	}
}
