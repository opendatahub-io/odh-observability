package controller

import (
	"context"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor"
	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestUsageLogsCollectorTransform(t *testing.T) {
	processorConfig := usageLogsTransformConfig(t)
	factory := transformprocessor.NewFactory()
	config := factory.CreateDefaultConfig()
	transformConfig, ok := config.(*transformprocessor.Config)
	if !ok {
		t.Fatalf("expected transform processor config, got %T", config)
	}
	if err := transformConfig.Unmarshal(confmap.NewFromStringMap(processorConfig)); err != nil {
		t.Fatalf("failed to decode transform processor config: %v", err)
	}

	ctx := context.Background()
	var processed plog.Logs
	next, err := consumer.NewLogs(func(_ context.Context, logs plog.Logs) error {
		processed = logs
		return nil
	})
	if err != nil {
		t.Fatalf("failed to create next consumer: %v", err)
	}

	transform, err := factory.CreateLogs(ctx, processor.Settings{
		ID:                component.NewID(factory.Type()),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}, config, next)
	if err != nil {
		t.Fatalf("failed to create transform processor: %v", err)
	}
	if err := transform.Start(ctx, nil); err != nil {
		t.Fatalf("failed to start transform processor: %v", err)
	}
	t.Cleanup(func() {
		if err := transform.Shutdown(ctx); err != nil {
			t.Errorf("failed to shut down transform processor: %v", err)
		}
	})

	tests := []struct {
		name         string
		upstreamHost string
		wantIP       string
		wantIPSet    bool
	}{
		{name: "IPv4", upstreamHost: "10.1.2.3:8080", wantIP: "10.1.2.3", wantIPSet: true},
		{name: "IPv6", upstreamHost: "[2001:db8::1]:8080", wantIP: "2001:db8::1", wantIPSet: true},
		{name: "missing upstream host", wantIPSet: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processed = plog.Logs{}
			logs := plog.NewLogs()
			resource := logs.ResourceLogs().AppendEmpty().Resource()
			if tt.upstreamHost != "" {
				resource.Attributes().PutStr("upstream_host", tt.upstreamHost)
			}
			logs.ResourceLogs().At(0).ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

			if err := transform.ConsumeLogs(ctx, logs); err != nil {
				t.Fatalf("transform failed: %v", err)
			}
			if processed.ResourceLogs().Len() != 1 {
				t.Fatalf("expected one processed resource log, got %d", processed.ResourceLogs().Len())
			}

			value, ok := processed.ResourceLogs().At(0).Resource().Attributes().Get("k8s.pod.ip")
			if ok != tt.wantIPSet {
				t.Errorf("k8s.pod.ip present = %t, want %t", ok, tt.wantIPSet)
			}
			if !tt.wantIPSet {
				return
			}
			if !ok {
				t.Fatal("expected k8s.pod.ip attribute")
			}
			if got := value.Str(); got != tt.wantIP {
				t.Errorf("k8s.pod.ip = %q, want %q", got, tt.wantIP)
			}
		})
	}
}

func usageLogsTransformConfig(t *testing.T) map[string]any {
	t.Helper()

	resources, err := rendertemplate.Render(context.Background(), nil, []rendertemplate.TemplateSource{{
		FS:   resourcesFS,
		Path: UsageLogsOpenTelemetryCollectorTemplate,
	}}, map[string]any{
		"Namespace":              "test-ns",
		"GatewayNamespace":       "test-ns",
		"UsageLogsCollectorName": "usage-logs",
		"CollectorReplicas":      1,
		"CollectorCPULimit":      "1",
		"CollectorMemoryLimit":   "256Mi",
		"CollectorCPURequest":    "100m",
		"CollectorMemoryRequest": "256Mi",
		"UsageLogs":              true,
		"UsageLogsEndpoint":      "https://loki.example.com",
	})
	if err != nil {
		t.Fatalf("failed to render usage logs collector template: %v", err)
	}
	if len(resources) != 1 {
		t.Fatalf("expected one rendered resource, got %d", len(resources))
	}

	config, found, err := unstructured.NestedMap(resources[0].Object, "spec", "config", "processors", "transform")
	if err != nil {
		t.Fatalf("failed to extract transform processor config: %v", err)
	}
	if !found {
		t.Fatal("transform processor config not found")
	}
	return config
}
