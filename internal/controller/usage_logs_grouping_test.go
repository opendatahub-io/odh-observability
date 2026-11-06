package controller

import (
	"context"
	"maps"
	"slices"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/groupbyattrsprocessor"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processortest"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestUsageLogsGroupingPreservesOptionalIdentityAttributes(t *testing.T) {
	collector := renderUsageLogsCollectorTemplate(t)
	keys, found, err := unstructured.NestedStringSlice(collector.Object, "spec", "config", "processors", "groupbyattrs/maas", "keys")
	require.NoError(t, err)
	require.True(t, found, "rendered collector must configure MaaS grouping keys")

	// Execute the upstream processor with the keys from our rendered template.
	// A fake Kubernetes client cannot prove that records with missing IDs survive.
	factory := groupbyattrsprocessor.NewFactory()
	config := &groupbyattrsprocessor.Config{GroupByKeys: keys}
	sink := &consumertest.LogsSink{}
	processor, err := factory.CreateLogs(t.Context(), processortest.NewNopSettings(factory.Type()), config, sink)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, processor.Shutdown(context.Background()))
	})
	require.NoError(t, processor.Start(t.Context(), componenttest.NewNopHost()))

	cases := []struct {
		name         string
		identity     map[string]string
		withoutGroup bool
	}{
		{name: "both IDs", identity: map[string]string{"organization_id": "org-a", "user_id": "user-a"}},
		{name: "organization only", identity: map[string]string{"organization_id": "org-a"}},
		{name: "user only", identity: map[string]string{"user_id": "user-a"}},
		{name: "neither ID"},
		{name: "different organization", identity: map[string]string{"organization_id": "org-b", "user_id": "user-a"}},
		{name: "different user", identity: map[string]string{"organization_id": "org-a", "user_id": "user-b"}},
		{name: "no grouping attributes", withoutGroup: true},
	}
	baseResource := map[string]any{"service.name": "models-as-a-service", "cluster_name": "spoke-a"}
	maasAttributes := map[string]any{"model": "test-model", "subscription": "test-subscription", "response_type": "success"}
	expectedResources := make(map[string]map[string]any, len(cases))

	// Mix all scenarios under one input Resource so grouping cannot accidentally
	// inherit an ID from a neighboring record or discard an unattributed record.
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	require.NoError(t, resourceLogs.Resource().Attributes().FromRaw(baseResource))
	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	scopeLogs.Scope().SetName("usage-logs-regression")
	for _, tc := range cases {
		record := scopeLogs.LogRecords().AppendEmpty()
		record.Body().SetStr(tc.name)
		expectedResource := maps.Clone(baseResource)
		if !tc.withoutGroup {
			require.NoError(t, record.Attributes().FromRaw(maasAttributes))
			maps.Copy(expectedResource, maasAttributes)
		}
		record.Attributes().PutInt("tokens_total", 7)
		for key, value := range tc.identity {
			record.Attributes().PutStr(key, value)
			expectedResource[key] = value
		}
		expectedResources[tc.name] = expectedResource
	}

	require.NoError(t, processor.ConsumeLogs(t.Context(), logs))
	require.Equal(t, len(cases), sink.LogRecordCount(), "grouping must preserve every record, including records without IDs")
	exported := sink.AllLogs()
	require.Len(t, exported, 1)
	resources := exported[0].ResourceLogs()
	seen := make(map[string]bool, len(cases))
	for i := range resources.Len() {
		resource := resources.At(i)
		for j := range resource.ScopeLogs().Len() {
			scope := resource.ScopeLogs().At(j)
			require.Equal(t, "usage-logs-regression", scope.Scope().Name())
			for k := range scope.LogRecords().Len() {
				record := scope.LogRecords().At(k)
				name := record.Body().Str()
				require.Contains(t, expectedResources, name, "unexpected exported record")
				require.False(t, seen[name], "record %q was exported more than once", name)
				seen[name] = true
				t.Run(name, func(t *testing.T) {
					require.Equal(t, expectedResources[name], resource.Resource().Attributes().AsRaw(),
						"present IDs must become resource attributes; missing IDs must remain absent")
					require.Equal(t, map[string]any{"tokens_total": int64(7)}, record.Attributes().AsRaw(),
						"grouping must preserve unrelated attributes and move grouped attributes off the record")
				})
			}
		}
	}
	require.Len(t, seen, len(cases), "every input scenario must be exported")
}

func assertUsageLogsGroupingConfig(t *testing.T, collector *unstructured.Unstructured) {
	t.Helper()

	keys, found, err := unstructured.NestedStringSlice(collector.Object, "spec", "config", "processors", "groupbyattrs/maas", "keys")
	require.NoError(t, err)
	require.True(t, found, "rendered collector must configure MaaS grouping keys")
	require.Subset(t, keys, []string{"model", "subscription", "response_type", "organization_id", "user_id"})

	pipelines, found, err := unstructured.NestedMap(collector.Object, "spec", "config", "service", "pipelines")
	require.NoError(t, err)
	require.True(t, found)
	require.NotEmpty(t, pipelines)
	for name := range pipelines {
		processors, found, err := unstructured.NestedStringSlice(pipelines, name, "processors")
		require.NoError(t, err)
		require.True(t, found, "pipeline %s must configure processors", name)
		grouping := slices.Index(processors, "groupbyattrs/maas")
		batch := slices.Index(processors, "batch")
		require.NotEqual(t, -1, grouping, "pipeline %s must group MaaS attributes", name)
		require.Greater(t, batch, grouping, "pipeline %s must group MaaS attributes before batching", name)
	}
}

func renderUsageLogsCollectorTemplate(t *testing.T) unstructured.Unstructured {
	t.Helper()

	resources := renderObservabilityTemplate(t, UsageLogsOpenTelemetryCollectorTemplate, map[string]any{
		"Namespace":                 "redhat-ods-monitoring",
		"UsageLogsCollectorName":    "data-science-usage-logs",
		"UsageLogs":                 true,
		"UsageLogsEndpoint":         "https://loki.example.svc.cluster.local:8080/otlp",
		"UsageLogsExporters":        map[string]string{},
		"UsageLogsExporterNames":    []string{},
		"UsageLogsCredentialSecret": "",
		"UsageLogsAttributes":       map[string]string{},
		"UsageLogsAttributeNames":   []string{},
		"CollectorReplicas":         1,
		"CollectorCPULimit":         "500m",
		"CollectorMemoryLimit":      "1Gi",
		"CollectorCPURequest":       "100m",
		"CollectorMemoryRequest":    "256Mi",
	})
	require.Len(t, resources, 1)
	return resources[0]
}
