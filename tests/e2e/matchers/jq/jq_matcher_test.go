package jq

import "testing"

func TestBuiltinDCGMScrapeAbsenceMatcherHandlesEmptyConfig(t *testing.T) {
	matcher := Match(`(.spec.config.receivers.prometheus.config.scrape_configs // [] | any(.[]; .job_name == "dcgm-exporter-accelerator-metrics")) | not`)

	receiver := func(config any) map[string]any {
		return map[string]any{
			"spec": map[string]any{
				"config": map[string]any{
					"receivers": map[string]any{
						"prometheus": config,
					},
				},
			},
		}
	}

	tests := []struct {
		name   string
		config any
		want   bool
	}{
		{name: "empty config", config: map[string]any{"config": map[string]any{}}, want: true},
		{name: "omitted config", config: map[string]any{}, want: true},
		{name: "null config", config: map[string]any{"config": nil}, want: true},
		{
			name: "built-in DCGM job configured",
			config: map[string]any{"config": map[string]any{
				"scrape_configs": []any{map[string]any{"job_name": "dcgm-exporter-accelerator-metrics"}},
			}},
			want: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			matched, err := matcher.Match(receiver(test.config))
			if err != nil {
				t.Fatalf("match Prometheus config: %v", err)
			}
			if matched != test.want {
				t.Errorf("match result: want %t, got %t", test.want, matched)
			}
		})
	}
}

func TestVolumeMountSelectionMatcherReturnsBoolean(t *testing.T) {
	t.Parallel()

	matcher := Match(`[.spec.volumeMounts[] | select(.name == "otlp-tls-certs" and .mountPath == "/etc/otel-collector/otlp-tls" and .readOnly == true)] | length > 0`)
	tests := []struct {
		name        string
		volumeMount map[string]any
		want        bool
	}{
		{
			name: "matching mount",
			volumeMount: map[string]any{
				"name":      "otlp-tls-certs",
				"mountPath": "/etc/otel-collector/otlp-tls",
				"readOnly":  true,
			},
			want: true,
		},
		{
			name: "wrong mount path",
			volumeMount: map[string]any{
				"name":      "otlp-tls-certs",
				"mountPath": "/etc/otel-collector/other",
				"readOnly":  true,
			},
			want: false,
		},
		{
			name: "writable mount",
			volumeMount: map[string]any{
				"name":      "otlp-tls-certs",
				"mountPath": "/etc/otel-collector/otlp-tls",
				"readOnly":  false,
			},
			want: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			resource := map[string]any{
				"spec": map[string]any{
					"volumeMounts": []any{test.volumeMount},
				},
			}
			matched, err := matcher.Match(resource)
			if err != nil {
				t.Fatalf("match OTLP TLS volume mount: %v", err)
			}
			if matched != test.want {
				t.Errorf("match result: want %t, got %t", test.want, matched)
			}
		})
	}
}
