package controller

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestCollectorServiceMonitorsPrometheusEndpointConfig guards the trusted
// namespace/pod attribution for user workload metrics. The prometheus exporter
// endpoint must promote the collector's k8sattributes-enriched
// k8s_namespace_name/k8s_pod_name labels to namespace/pod via
// metricRelabelings, and must not set honorLabels (which would trust
// workload-supplied labels and drop the exported_namespace/exported_pod
// labels Korrel8r relies on).
func TestCollectorServiceMonitorsPrometheusEndpointConfig(t *testing.T) {
	resources := renderObservabilityTemplate(t, CollectorServiceMonitorsTemplate, map[string]any{
		"Namespace": "redhat-ods-monitoring",
		"Metrics":   true,
	})

	prometheusSM := findRenderedResource(t, resources, "ServiceMonitor", "data-science-prometheus-monitor")
	endpoint := singleEndpoint(t, prometheusSM)

	if _, found, _ := unstructured.NestedBool(endpoint, "honorLabels"); found {
		t.Error("prometheus endpoint must not set honorLabels: it trusts workload-supplied labels and drops the exported_* labels used by Korrel8r")
	}

	relabels, found, err := unstructured.NestedSlice(endpoint, "metricRelabelings")
	if err != nil || !found {
		t.Fatalf("prometheus endpoint must configure metricRelabelings: found=%t, error=%v", found, err)
	}

	want := []struct {
		sourceLabels []string
		targetLabel  string
	}{
		{sourceLabels: []string{"k8s_namespace_name"}, targetLabel: "namespace"},
		{sourceLabels: []string{"k8s_pod_name"}, targetLabel: "pod"},
	}
	if len(relabels) != len(want) {
		t.Fatalf("expected %d metricRelabelings, got %d", len(want), len(relabels))
	}
	for i, raw := range relabels {
		relabel, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("metricRelabelings[%d] has unexpected type: %T", i, raw)
		}
		if !containsString(relabel["sourceLabels"], want[i].sourceLabels[0]) {
			t.Errorf("metricRelabelings[%d].sourceLabels = %v, want [%s]", i, relabel["sourceLabels"], want[i].sourceLabels[0])
		}
		if relabel["targetLabel"] != want[i].targetLabel {
			t.Errorf("metricRelabelings[%d].targetLabel = %v, want %q", i, relabel["targetLabel"], want[i].targetLabel)
		}
		if relabel["regex"] != "(.+)" {
			t.Errorf("metricRelabelings[%d].regex = %v, want %q: the default (.*) matches an empty value and would blank the promoted label", i, relabel["regex"], "(.+)")
		}
		if action, present := relabel["action"]; present && action != "replace" {
			t.Errorf("metricRelabelings[%d].action = %v, want unset or %q", i, action, "replace")
		}
	}

	// The collector's own telemetry endpoint must stay untouched.
	monitorSM := findRenderedResource(t, resources, "ServiceMonitor", "data-science-collector-monitor")
	monitorEndpoint := singleEndpoint(t, monitorSM)
	if _, found, _ := unstructured.NestedBool(monitorEndpoint, "honorLabels"); found {
		t.Error("monitoring endpoint must not set honorLabels")
	}
	if _, found, _ := unstructured.NestedSlice(monitorEndpoint, "metricRelabelings"); found {
		t.Error("monitoring endpoint must not configure metricRelabelings")
	}
}

// TestCollectorServiceMonitorsOmitPrometheusMonitorWithoutMetrics ensures the
// user-workload ServiceMonitor is gated on metrics.storage.
func TestCollectorServiceMonitorsOmitPrometheusMonitorWithoutMetrics(t *testing.T) {
	resources := renderObservabilityTemplate(t, CollectorServiceMonitorsTemplate, map[string]any{
		"Namespace": "redhat-ods-monitoring",
		"Metrics":   false,
	})

	for _, resource := range resources {
		if resource.GetName() == "data-science-prometheus-monitor" {
			t.Error("data-science-prometheus-monitor must not render without metrics.storage")
		}
	}
}

func singleEndpoint(t *testing.T, sm unstructured.Unstructured) map[string]any {
	t.Helper()
	endpoints, found, err := unstructured.NestedSlice(sm.Object, "spec", "endpoints")
	if err != nil || !found || len(endpoints) != 1 {
		t.Fatalf("ServiceMonitor %q must have exactly one endpoint: found=%t, error=%v, endpoints=%v",
			sm.GetName(), found, err, endpoints)
	}
	endpoint, ok := endpoints[0].(map[string]any)
	if !ok {
		t.Fatalf("ServiceMonitor %q endpoint has unexpected type: %T", sm.GetName(), endpoints[0])
	}
	return endpoint
}
