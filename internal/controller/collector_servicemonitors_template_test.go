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
		"Namespace":      "redhat-ods-monitoring",
		"Metrics":        true,
		"MetricsStorage": true,
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
	if len(relabels) != len(want)+1 {
		t.Fatalf("expected %d metricRelabelings, got %d", len(want)+1, len(relabels))
	}
	dropRule, ok := relabels[0].(map[string]any)
	if !ok || dropRule["action"] != "drop" || dropRule["regex"] != "node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate|kube_resourcequota" {
		t.Fatalf("first metricRelabeling must exclude federated resource metrics: %v", relabels[0])
	}
	for i, raw := range relabels[1:] {
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

	resourceSM := findRenderedResource(t, resources, "ServiceMonitor", "data-science-prometheus-resources-monitor")
	resourceEndpoint := singleEndpoint(t, resourceSM)
	resourceRelabels, found, err := unstructured.NestedSlice(resourceEndpoint, "metricRelabelings")
	if err != nil || !found || len(resourceRelabels) != 1 {
		t.Fatalf("resource endpoint must keep exactly the federated resource metrics: found=%t error=%v relabels=%v", found, err, resourceRelabels)
	}
	keepRule, ok := resourceRelabels[0].(map[string]any)
	if !ok || keepRule["action"] != "keep" || keepRule["regex"] != "model-resources" || !containsString(keepRule["sourceLabels"], "odh_federation_source") {
		t.Errorf("resource endpoint must keep only federated resource metrics: %v", resourceRelabels[0])
	}
	if serverName, found, err := unstructured.NestedString(resourceEndpoint, "tlsConfig", "serverName"); err != nil || !found || serverName != "data-science-collector-prometheus.redhat-ods-monitoring.svc" {
		t.Errorf("resource endpoint must verify the mounted collector serving certificate: found=%t value=%q error=%v", found, serverName, err)
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

func TestOwnedServiceMonitorsCarryScrapeLabel(t *testing.T) {
	collectorResources := renderObservabilityTemplate(t, CollectorServiceMonitorsTemplate, map[string]any{
		"Namespace":      "redhat-ods-monitoring",
		"Metrics":        true,
		"MetricsStorage": true,
	})
	for _, name := range []string{"data-science-collector-monitor", "data-science-prometheus-monitor", "data-science-prometheus-resources-monitor"} {
		serviceMonitor := findRenderedResource(t, collectorResources, "ServiceMonitor", name)
		assertScrapeLabel(t, serviceMonitor)
	}

	prometheusResources := renderObservabilityTemplate(t, PrometheusSelfServiceMonitorTemplate, map[string]any{
		"Namespace": "redhat-ods-monitoring",
	})
	assertScrapeLabel(t, findRenderedResource(t, prometheusResources, "ServiceMonitor", "prometheus-self-fixed"))
}

func TestResourcePrometheusServiceSelectsOnlyOrdinalZero(t *testing.T) {
	resources := renderObservabilityTemplate(t, CollectorResourcePrometheusServiceTemplate, map[string]any{
		"Namespace": "redhat-ods-monitoring",
	})
	service := findRenderedResource(t, resources, "Service", "data-science-prometheus-resources")
	selector, found, err := unstructured.NestedStringMap(service.Object, "spec", "selector")
	if err != nil || !found {
		t.Fatalf("resource Prometheus service must have a selector: found=%t error=%v", found, err)
	}
	if selector["statefulset.kubernetes.io/pod-name"] != "data-science-collector-collector-0" {
		t.Fatalf("resource Prometheus service must select collector ordinal 0, got %v", selector)
	}
}

func assertScrapeLabel(t *testing.T, serviceMonitor unstructured.Unstructured) {
	t.Helper()
	value, found, err := unstructured.NestedString(serviceMonitor.Object, "metadata", "labels", "monitoring.opendatahub.io/scrape")
	if err != nil || !found || value != "true" {
		t.Fatalf("ServiceMonitor %q must carry monitoring.opendatahub.io/scrape=true: found=%t value=%q error=%v",
			serviceMonitor.GetName(), found, value, err)
	}
}

// TestCollectorServiceMonitorsOmitPrometheusMonitorWithoutMetrics ensures the
// user-workload ServiceMonitor is gated on metrics.storage.
func TestCollectorServiceMonitorsOmitPrometheusMonitorWithoutMetrics(t *testing.T) {
	resources := renderObservabilityTemplate(t, CollectorServiceMonitorsTemplate, map[string]any{
		"Namespace":      "redhat-ods-monitoring",
		"Metrics":        false,
		"MetricsStorage": false,
	})

	for _, resource := range resources {
		if resource.GetName() == "data-science-prometheus-monitor" {
			t.Error("data-science-prometheus-monitor must not render without metrics.storage")
		}
		if resource.GetName() == "data-science-prometheus-resources-monitor" {
			t.Error("data-science-prometheus-resources-monitor must not render without metrics.storage")
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
