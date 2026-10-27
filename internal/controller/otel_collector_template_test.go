package controller

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
	promconfig "github.com/prometheus/common/config"
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
	if strings.Contains(metricRelabelSection, "__tmp_scale_needed") {
		t.Error("metric_relabel_configs must not add __tmp_scale_needed (rejected by OTel prometheus exporter)")
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

func TestPrometheusScrapeConfigContainsOnlyDCGMJob(t *testing.T) {
	jobs := renderedPrometheusScrapeJobs(t, renderCollectorTemplate(t))
	if _, found := jobs["data-science-collector-prometheus"]; found {
		t.Error("unused generic pod scrape job must not be rendered")
	}
	if len(jobs) != 1 || jobs["dcgm-exporter-accelerator-metrics"] == nil {
		t.Fatalf("expected only the DCGM scrape job, got %v", jobs)
	}
}

func TestPrometheusReceiverConfigIsEmptyWithoutAcceleratorMetrics(t *testing.T) {
	collector := renderCollectorTemplateWithAcceleratorMetrics(t, false)
	prometheusConfig, found, err := unstructured.NestedMap(collector.Object, "spec", "config", "receivers", "prometheus", "config")
	if err != nil || !found {
		t.Fatalf("Prometheus receiver config must render as a map: found=%t, error=%v", found, err)
	}
	if len(prometheusConfig) != 0 {
		t.Fatalf("Prometheus receiver config must be empty without manual scrape jobs, got %v", prometheusConfig)
	}
}

func TestTargetAllocatorSecurity(t *testing.T) {
	collector := renderCollectorTemplate(t)
	targetAllocator, found, err := unstructured.NestedMap(collector.Object, "spec", "targetAllocator")
	if err != nil || !found {
		t.Fatalf("target allocator config must be present in rendered resource: found=%t, error=%v", found, err)
	}
	mtls, found, err := unstructured.NestedMap(targetAllocator, "mtls")
	if err != nil || !found || mtls["enabled"] != true || mtls["useCertManager"] != true {
		t.Fatalf("target allocator mTLS must be enabled with cert-manager: found=%t, config=%v, error=%v", found, mtls, err)
	}
	if targetAllocator["allowInsecureAuthSecrets"] != false {
		t.Errorf("target allocator must disable auth secrets over plaintext HTTP, got %v", targetAllocator["allowInsecureAuthSecrets"])
	}

	prometheusCR, found, err := unstructured.NestedMap(targetAllocator, "prometheusCR")
	if err != nil || !found || prometheusCR["denyFSAccessThroughSMs"] != true {
		t.Fatalf("target allocator must deny ServiceMonitor filesystem access: found=%t, config=%v, error=%v", found, prometheusCR, err)
	}
	if targetAllocator["serviceAccount"] != targetAllocatorServiceAccountName {
		t.Errorf("target allocator must use dedicated service account %q, got %v", targetAllocatorServiceAccountName, targetAllocator["serviceAccount"])
	}
	secretNamespaces, ok := prometheusCR["secretNamespaces"].([]any)
	if !ok || len(secretNamespaces) != 2 || secretNamespaces[0] != "redhat-ods-monitoring" || secretNamespaces[1] != "team-a" {
		t.Errorf("target allocator must receive the rendered Secret namespace allowlist, got %v", prometheusCR["secretNamespaces"])
	}
}

func TestOpenTelemetryCollectorDisablesOperatorNetworkPolicy(t *testing.T) {
	t.Parallel()

	collector := renderCollectorTemplate(t)
	enabled, found, err := unstructured.NestedBool(collector.Object, "spec", "networkPolicy", "enabled")
	if err != nil || !found {
		t.Fatalf("collector NetworkPolicy setting must be rendered: found=%t error=%v", found, err)
	}
	if enabled {
		t.Fatal("OpenTelemetry Operator NetworkPolicy must be disabled so the module-owned policies are authoritative")
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

	for name, pipeline := range metricsPipelines(t, collector) {
		if !containsString(pipeline["processors"], "transform/gpu_metrics") {
			t.Errorf("metrics pipeline %q must invoke the GPU metric transform processor", name)
		}
	}
}

// TestCollectorStripsWorkloadSuppliedK8sLabelAttributes guards the trust
// boundary of the namespace/pod label promotion configured by the collector
// ServiceMonitor's metricRelabelings: workloads control their exposition
// labels, so datapoint attributes named k8s_namespace_name/k8s_pod_name must
// never survive to export. The transform processor makes the k8sattributes
// resource attributes the sole source of those labels.
func TestCollectorStripsWorkloadSuppliedK8sLabelAttributes(t *testing.T) {
	collector := renderCollectorTemplate(t)

	config, found, err := unstructured.NestedMap(collector.Object, "spec", "config")
	if err != nil || !found {
		t.Fatalf("collector config must be present in rendered resource: found=%t, error=%v", found, err)
	}
	processors, found, err := unstructured.NestedMap(config, "processors")
	if err != nil || !found {
		t.Fatalf("collector processors must be present: found=%t, error=%v", found, err)
	}
	transform, found, err := unstructured.NestedMap(processors, "transform/sanitize_k8s_labels")
	if err != nil || !found {
		t.Fatalf("k8s label sanitize transform processor must be present: found=%t, error=%v", found, err)
	}
	metricStatements, ok := transform["metric_statements"].([]any)
	if !ok || len(metricStatements) != 1 {
		t.Fatalf("k8s label sanitize transform must contain one metric statement block (datapoint), got %v", transform["metric_statements"])
	}
	statementBlock, ok := metricStatements[0].(map[string]any)
	if !ok {
		t.Fatalf("k8s label sanitize statement block has unexpected type: %T", metricStatements[0])
	}
	if statementBlock["context"] != "datapoint" {
		t.Fatalf("k8s label sanitize statements must target the datapoint context, got %v", statementBlock["context"])
	}
	statements, ok := statementBlock["statements"].([]any)
	if !ok {
		t.Fatalf("k8s label sanitize statements have unexpected type: %T", statementBlock["statements"])
	}
	expectedStatements := []string{
		`delete_key(attributes, "k8s_namespace_name")`,
		`delete_key(attributes, "k8s_pod_name")`,
	}
	if len(statements) != len(expectedStatements) {
		t.Fatalf("expected %d k8s label sanitize statements, got %d", len(expectedStatements), len(statements))
	}
	for index, expected := range expectedStatements {
		if statements[index] != expected {
			t.Errorf("k8s label sanitize statement %d: got %v, want %s", index, statements[index], expected)
		}
	}

	for name, pipeline := range metricsPipelines(t, collector) {
		pipelineProcessors, ok := pipeline["processors"].([]any)
		if !ok {
			t.Fatalf("metrics pipeline %q processors have unexpected type: %T", name, pipeline["processors"])
		}
		sanitizeIndex, batchIndex := -1, -1
		for index, procName := range pipelineProcessors {
			switch procName {
			case "transform/sanitize_k8s_labels":
				sanitizeIndex = index
			case "batch":
				batchIndex = index
			}
		}
		if sanitizeIndex == -1 {
			t.Errorf("metrics pipeline %q must invoke the k8s label sanitize transform processor", name)
		}
		if batchIndex == -1 || sanitizeIndex > batchIndex {
			t.Errorf("k8s label sanitize transform must run in metrics pipeline %q before batch", name)
		}
	}
}

// TestSanitizeTransformExcludesDCGMExporterK8sLabels guards the DCGM
// scope-out of the namespace/pod promotion: the dcgm-exporter scrape job's
// k8s.namespace.name/k8s.pod.name resource attributes identify the exporter
// pod in nvidia-gpu-operator, not the workload consuming the GPU, so they
// must not be promoted to namespace/pod by the collector ServiceMonitor.
// The resource-context statements delete them for that job, making the
// promotion a no-op for accelerator series, while node/component are
// preserved (Korrel8r correlates DCGM series by node). The processor runs
// in the scrape pipeline only: an OTLP pusher controls service.name and
// must not be able to use the DCGM job name to opt out of pod attribution.
func TestSanitizeTransformExcludesDCGMExporterK8sLabels(t *testing.T) {
	collector := renderCollectorTemplate(t)

	config, found, err := unstructured.NestedMap(collector.Object, "spec", "config")
	if err != nil || !found {
		t.Fatalf("collector config must be present in rendered resource: found=%t, error=%v", found, err)
	}
	processors, found, err := unstructured.NestedMap(config, "processors")
	if err != nil || !found {
		t.Fatalf("collector processors must be present in rendered resource: found=%t, error=%v", found, err)
	}
	transform, found, err := unstructured.NestedMap(processors, "transform/exclude_dcgm_k8s_labels")
	if err != nil || !found {
		t.Fatalf("DCGM k8s label exclusion transform processor must be present: found=%t, error=%v", found, err)
	}
	metricStatements, ok := transform["metric_statements"].([]any)
	if !ok || len(metricStatements) != 1 {
		t.Fatalf("DCGM k8s label exclusion transform must contain one metric statement block (resource), got %v", transform["metric_statements"])
	}
	resourceBlock, ok := metricStatements[0].(map[string]any)
	if !ok {
		t.Fatalf("DCGM k8s label exclusion statement block has unexpected type: %T", metricStatements[0])
	}
	if resourceBlock["context"] != "resource" {
		t.Fatalf("DCGM k8s label exclusion statements must target the resource context, got %v", resourceBlock["context"])
	}
	statements, ok := resourceBlock["statements"].([]any)
	if !ok {
		t.Fatalf("resource-context statements have unexpected type: %T", resourceBlock["statements"])
	}

	// The DCGM job name scoping the statements must match the job target
	// label assigned by the dcgm scrape job's relabeling, so renaming one
	// without the other fails here.
	dcgmJobName := dcgmScrapeJobName(t, collector)
	expectedStatements := []string{
		fmt.Sprintf(`delete_key(attributes, "k8s.namespace.name") where attributes["service.name"] == %q`, dcgmJobName),
		fmt.Sprintf(`delete_key(attributes, "k8s.pod.name") where attributes["service.name"] == %q`, dcgmJobName),
	}
	if len(statements) != len(expectedStatements) {
		t.Fatalf("expected %d resource-context statements, got %d", len(expectedStatements), len(statements))
	}
	for index, expected := range expectedStatements {
		if statements[index] != expected {
			t.Errorf("resource-context statement %d: got %v, want %s", index, statements[index], expected)
		}
	}
	for _, statement := range statements {
		if strings.Contains(fmt.Sprint(statement), "k8s.node.name") {
			t.Errorf("resource-context statements must not delete k8s.node.name (Korrel8r correlates DCGM series by node): %v", statement)
		}
	}

	pipelines := metricsPipelines(t, collector)
	if !containsString(pipelines["metrics/scrape"]["processors"], "transform/exclude_dcgm_k8s_labels") {
		t.Error("metrics/scrape pipeline must invoke the DCGM k8s label exclusion processor")
	}
	if containsString(pipelines["metrics/otlp"]["processors"], "transform/exclude_dcgm_k8s_labels") {
		t.Error("metrics/otlp pipeline must not invoke the DCGM k8s label exclusion processor: an OTLP pusher controls service.name")
	}
}

// metricsPipelines returns every rendered metrics pipeline keyed by its
// configuration name, failing the test unless both the scrape and OTLP
// pipelines exist.
func metricsPipelines(t *testing.T, collector unstructured.Unstructured) map[string]map[string]any {
	t.Helper()

	config, found, err := unstructured.NestedMap(collector.Object, "spec", "config")
	if err != nil || !found {
		t.Fatalf("collector config must be present in rendered resource: found=%t, error=%v", found, err)
	}
	pipelines, found, err := unstructured.NestedMap(config, "service", "pipelines")
	if err != nil || !found {
		t.Fatalf("service pipelines must be present in rendered resource: found=%t, error=%v", found, err)
	}

	result := make(map[string]map[string]any, 2)
	for _, name := range []string{"metrics/scrape", "metrics/otlp"} {
		pipeline, ok := pipelines[name].(map[string]any)
		if !ok {
			t.Fatalf("metrics pipeline %q must be present in rendered resource", name)
		}
		result[name] = pipeline
	}
	return result
}

// renderedProcessors returns the processors section of the rendered
// collector, failing the test unless it is present.
func renderedProcessors(t *testing.T, collector unstructured.Unstructured) map[string]any {
	t.Helper()

	config, found, err := unstructured.NestedMap(collector.Object, "spec", "config")
	if err != nil || !found {
		t.Fatalf("collector config must be present in rendered resource: found=%t, error=%v", found, err)
	}
	processors, found, err := unstructured.NestedMap(config, "processors")
	if err != nil || !found {
		t.Fatalf("collector processors must be present in rendered resource: found=%t, error=%v", found, err)
	}
	return processors
}

// TestOTLPPipelineSanitizesClientSuppliedIdentity guards the OTLP metrics
// trust boundary. Pushers control the resource attributes they send, and the
// collector ServiceMonitor promotes k8s.namespace.name/k8s.pod.name (exported
// as k8s_namespace_name/k8s_pod_name) to namespace/pod, so the OTLP
// pipeline's sanitizer must strip every spelling of client-supplied identity
// attributes before enrichment runs, and its k8sattributes/otlp processor
// must associate pods from the network connection only: the default
// association would consult client-supplied k8s.pod.ip first.
func TestOTLPPipelineSanitizesClientSuppliedIdentity(t *testing.T) {
	collector := renderCollectorTemplate(t)
	processors := renderedProcessors(t, collector)

	k8sAttributes, found, err := unstructured.NestedMap(processors, "k8sattributes/otlp")
	if err != nil || !found {
		t.Fatalf("k8sattributes/otlp processor must be present: found=%t, error=%v", found, err)
	}
	associations, found, err := unstructured.NestedSlice(k8sAttributes, "pod_association")
	if err != nil || !found || len(associations) != 1 {
		t.Fatalf("k8sattributes/otlp must configure exactly one pod association rule: found=%t, rules=%v, error=%v", found, associations, err)
	}
	rule, ok := associations[0].(map[string]any)
	if !ok {
		t.Fatalf("k8sattributes/otlp pod association rule has unexpected type: %T", associations[0])
	}
	sources, found, err := unstructured.NestedSlice(rule, "sources")
	if err != nil || !found || len(sources) != 1 {
		t.Fatalf("k8sattributes/otlp association rule must have exactly one source: found=%t, sources=%v, error=%v", found, sources, err)
	}
	source, ok := sources[0].(map[string]any)
	if !ok {
		t.Fatalf("k8sattributes/otlp association source has unexpected type: %T", sources[0])
	}
	if source["from"] != "connection" {
		t.Errorf("k8sattributes/otlp must associate pods from the network connection only, got source %v", source)
	}

	transform, found, err := unstructured.NestedMap(processors, "transform/sanitize_otlp_identity")
	if err != nil || !found {
		t.Fatalf("OTLP identity sanitize transform processor must be present: found=%t, error=%v", found, err)
	}
	metricStatements, ok := transform["metric_statements"].([]any)
	if !ok || len(metricStatements) != 2 {
		t.Fatalf("OTLP identity sanitize transform must contain two metric statement blocks (resource and datapoint), got %v", transform["metric_statements"])
	}
	blocks := statementBlocksByContext(t, metricStatements)
	resourceBlock, datapointBlock := blocks["resource"], blocks["datapoint"]
	if resourceBlock == nil {
		t.Fatal("OTLP identity sanitize transform must contain a resource statement block")
	}
	if datapointBlock == nil {
		t.Fatal("OTLP identity sanitize transform must contain a datapoint statement block")
	}
	requireStatements(t, "OTLP identity sanitize resource", resourceBlock, []string{
		`delete_key(attributes, "k8s.namespace.name")`,
		`delete_key(attributes, "k8s.pod.name")`,
		`delete_key(attributes, "k8s.pod.uid")`,
		`delete_key(attributes, "k8s.pod.ip")`,
		`delete_key(attributes, "k8s.pod.start_time")`,
		`delete_key(attributes, "k8s.node.name")`,
		`delete_key(attributes, "k8s.deployment.name")`,
		`delete_key(attributes, "k8s.container.name")`,
		`delete_key(attributes, "container.id")`,
		`delete_key(attributes, "k8s_namespace_name")`,
		`delete_key(attributes, "k8s_pod_name")`,
		`delete_key(attributes, "namespace")`,
		`delete_key(attributes, "pod")`,
		`delete_key(attributes, "exported_namespace")`,
		`delete_key(attributes, "exported_pod")`,
	})
	requireStatements(t, "OTLP identity sanitize datapoint", datapointBlock, []string{
		`delete_key(attributes, "namespace")`,
		`delete_key(attributes, "pod")`,
		`delete_key(attributes, "exported_namespace")`,
		`delete_key(attributes, "exported_pod")`,
	})
}

// TestOTLPMetricsPipelineWiring guards the pipeline split itself: OTLP
// metrics must enter through metrics/otlp so they cannot bypass the identity
// sanitizer, scrape metrics must not pass through it, the sanitizer must run
// before enrichment, and both pipelines must share exporters.
func TestOTLPMetricsPipelineWiring(t *testing.T) {
	collector := renderCollectorTemplate(t)

	pipelines := metricsPipelines(t, collector)
	scrapePipeline, otlpPipeline := pipelines["metrics/scrape"], pipelines["metrics/otlp"]

	otlpReceivers, ok := otlpPipeline["receivers"].([]any)
	if !ok {
		t.Fatalf("metrics/otlp pipeline receivers have unexpected type: %T", otlpPipeline["receivers"])
	}
	if !containsString(otlpReceivers, "otlp") || len(otlpReceivers) != 1 {
		t.Errorf("metrics/otlp pipeline must receive OTLP only, got %v", otlpReceivers)
	}
	if !containsString(scrapePipeline["receivers"], "prometheus") {
		t.Errorf("metrics/scrape pipeline must receive the prometheus receiver, got %v", scrapePipeline["receivers"])
	}
	if containsString(scrapePipeline["receivers"], "otlp") {
		t.Error("metrics/scrape pipeline must not receive OTLP: pushed metrics must not bypass the identity sanitizer")
	}

	otlpProcessors, ok := otlpPipeline["processors"].([]any)
	if !ok {
		t.Fatalf("metrics/otlp pipeline processors have unexpected type: %T", otlpPipeline["processors"])
	}
	sanitizeIndex, enrichIndex := -1, -1
	for index, name := range otlpProcessors {
		switch name {
		case "transform/sanitize_otlp_identity":
			sanitizeIndex = index
		case "k8sattributes/otlp":
			enrichIndex = index
		}
	}
	if sanitizeIndex == -1 {
		t.Fatal("metrics/otlp pipeline must invoke the OTLP identity sanitize transform processor")
	}
	if enrichIndex == -1 {
		t.Fatal("metrics/otlp pipeline must invoke the k8sattributes/otlp processor")
	}
	if sanitizeIndex > enrichIndex {
		t.Error("OTLP identity sanitize transform must run before k8sattributes/otlp enrichment")
	}

	if containsString(scrapePipeline["processors"], "transform/sanitize_otlp_identity") {
		t.Error("metrics/scrape pipeline must not invoke the OTLP identity sanitizer: its k8s attributes come from trusted discovery metadata")
	}
	if containsString(scrapePipeline["processors"], "k8sattributes/otlp") {
		t.Error("metrics/scrape pipeline must keep the shared k8sattributes processor, not the OTLP-specific one")
	}
	if !containsString(scrapePipeline["processors"], "k8sattributes") {
		t.Error("metrics/scrape pipeline must invoke the shared k8sattributes processor")
	}

	if !reflect.DeepEqual(scrapePipeline["exporters"], otlpPipeline["exporters"]) {
		t.Errorf("metrics pipelines must share exporters, scrape=%v otlp=%v", scrapePipeline["exporters"], otlpPipeline["exporters"])
	}
}

// statementBlocksByContext indexes a transform's metric_statements blocks by
// their context name.
func statementBlocksByContext(t *testing.T, metricStatements []any) map[string]map[string]any {
	t.Helper()

	blocks := make(map[string]map[string]any, len(metricStatements))
	for _, rawBlock := range metricStatements {
		block, ok := rawBlock.(map[string]any)
		if !ok {
			t.Fatalf("transform statement block has unexpected type: %T", rawBlock)
		}
		context, _ := block["context"].(string)
		blocks[context] = block
	}
	return blocks
}

// requireStatements asserts that a transform statement block carries exactly
// the expected OTTL statements, in order.
func requireStatements(t *testing.T, description string, block map[string]any, expected []string) {
	t.Helper()

	statements, ok := block["statements"].([]any)
	if !ok {
		t.Fatalf("%s statements have unexpected type: %T", description, block["statements"])
	}
	if len(statements) != len(expected) {
		t.Fatalf("expected %d %s statements, got %d", len(expected), description, len(statements))
	}
	for index, want := range expected {
		if statements[index] != want {
			t.Errorf("%s statement %d: got %v, want %s", description, index, statements[index], want)
		}
	}
}

// dcgmScrapeJobName returns the job label value the dcgm scrape job's
// relabeling assigns; the prometheus receiver exposes it as the service.name
// resource attribute.
func dcgmScrapeJobName(t *testing.T, collector unstructured.Unstructured) string {
	t.Helper()
	config, found, err := unstructured.NestedMap(collector.Object, "spec", "config")
	if err != nil || !found {
		t.Fatalf("collector config must be present in rendered resource: found=%t, error=%v", found, err)
	}
	scrapeConfigs, found, err := unstructured.NestedSlice(config, "receivers", "prometheus", "config", "scrape_configs")
	if err != nil || !found {
		t.Fatalf("expected scrape configs in rendered resource: found=%t, error=%v", found, err)
	}
	for _, rawScrapeConfig := range scrapeConfigs {
		scrapeConfig, ok := rawScrapeConfig.(map[string]any)
		if !ok || scrapeConfig["job_name"] != "dcgm-exporter-accelerator-metrics" {
			continue
		}
		relabelConfigs, found, err := unstructured.NestedSlice(scrapeConfig, "relabel_configs")
		if err != nil || !found {
			t.Fatalf("DCGM relabel configs must be present: found=%t, error=%v", found, err)
		}
		for _, rawRule := range relabelConfigs {
			rule, ok := rawRule.(map[string]any)
			if !ok || rule["target_label"] != "job" {
				continue
			}
			jobName, _ := rule["replacement"].(string)
			if jobName == "" {
				t.Fatalf("DCGM job relabel rule must set a replacement: %v", rule)
			}
			return jobName
		}
		t.Fatal("DCGM scrape config must assign a job target label")
	}
	t.Fatal("DCGM scrape config must be present")
	return ""
}

func TestDCGMMetricRelabelingPreservesDRAAttributionLabels(t *testing.T) {
	collector := renderCollectorTemplate(t)
	config, found, err := unstructured.NestedMap(collector.Object, "spec", "config")
	if err != nil || !found {
		t.Fatalf("collector config must be present in rendered resource: found=%t, error=%v", found, err)
	}
	scrapeConfigs, found, err := unstructured.NestedSlice(config, "receivers", "prometheus", "config", "scrape_configs")
	if err != nil || !found || len(scrapeConfigs) == 0 {
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
	return renderCollectorTemplateWithAcceleratorMetrics(t, true)
}

func renderCollectorTemplateWithAcceleratorMetrics(t *testing.T, acceleratorMetrics bool) unstructured.Unstructured {
	t.Helper()
	resources, err := rendertemplate.Render(context.Background(), nil, []rendertemplate.TemplateSource{{
		FS:   resourcesFS,
		Path: OpenTelemetryCollectorTemplate,
	}}, map[string]any{
		"Namespace":                       "redhat-ods-monitoring",
		"Metrics":                         true,
		"MetricsStorage":                  true,
		"AcceleratorMetrics":              acceleratorMetrics,
		"Traces":                          false,
		"CollectorReplicas":               1,
		"CollectorCPULimit":               "1",
		"CollectorMemoryLimit":            "1Gi",
		"CollectorCPURequest":             "100m",
		"CollectorMemoryRequest":          "256Mi",
		"MetricsExporterNames":            []string{},
		"MetricsExporters":                map[string]string{},
		"TargetAllocatorServiceAccount":   targetAllocatorServiceAccountName,
		"TargetAllocatorSecretNamespaces": []string{"redhat-ods-monitoring", "team-a"},
		"TracesExporterNames":             []string{},
		"TracesExporters":                 map[string]string{},
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

func TestDCGMScrapeJobUsesHTTPAndVerifiesTLS(t *testing.T) {
	jobs := renderedPrometheusScrapeJobs(t, renderCollectorTemplate(t))
	job := jobs["dcgm-exporter-accelerator-metrics"]
	if job == nil {
		t.Fatal("DCGM scrape job must be present")
	}
	if job["scheme"] != "http" {
		t.Errorf("DCGM scrape job must explicitly use HTTP, got %v", job["scheme"])
	}
	if job["bearer_token_file"] != "/var/run/secrets/kubernetes.io/serviceaccount/token" {
		t.Errorf("DCGM scrape job must retain service-account bearer token authentication, got %v", job["bearer_token_file"])
	}
	if tlsConfig, found, err := unstructured.NestedMap(job, "tls_config"); err != nil {
		t.Errorf("failed to read TLS config: %v", err)
	} else if found && tlsConfig["insecure_skip_verify"] == true {
		t.Error("DCGM scrape job must not skip TLS certificate verification")
	}
}

func TestDCGMScrapeJobRetainsDiscoveryAndRelabeling(t *testing.T) {
	jobs := renderedPrometheusScrapeJobs(t, renderCollectorTemplate(t))
	job := jobs["dcgm-exporter-accelerator-metrics"]
	if job == nil {
		t.Fatal("DCGM scrape job must be present")
	}
	sdConfigs, found, err := unstructured.NestedSlice(job, "kubernetes_sd_configs")
	if err != nil || !found || len(sdConfigs) != 1 {
		t.Fatalf("DCGM job must retain one Kubernetes discovery config: found=%t configs=%v error=%v", found, sdConfigs, err)
	}
	sdConfig, ok := sdConfigs[0].(map[string]any)
	if !ok || sdConfig["role"] != "pod" {
		t.Fatalf("DCGM job must discover Pod targets, got %v", sdConfigs[0])
	}
	namespaces, found, err := unstructured.NestedStringSlice(sdConfig, "namespaces", "names")
	if err != nil || !found || len(namespaces) != 1 || namespaces[0] != "nvidia-gpu-operator" {
		t.Fatalf("DCGM discovery must remain limited to nvidia-gpu-operator, got %v (found=%t error=%v)", namespaces, found, err)
	}

	relabelRules := renderedPrometheusRelabelRules(t, job)
	expectedRules := map[string]string{
		"__address__": "$1:9400",
		"node":        "__meta_kubernetes_pod_node_name",
		"component":   "__meta_kubernetes_pod_label_app",
		"job":         "rhoai-accelerator-metrics",
	}
	for targetLabel, expected := range expectedRules {
		if !hasPrometheusRelabelRule(relabelRules, targetLabel, expected) {
			t.Errorf("DCGM scrape job must retain relabeling for %q=%q", targetLabel, expected)
		}
	}
}

func TestPrometheusHTTPClientTLSValidation(t *testing.T) {
	const serverName = "metrics.example.test"
	trustedCertPEM, trustedCert := newSelfSignedTestServerCertificate(t, serverName)
	untrustedCertPEM, _ := newSelfSignedTestServerCertificate(t, "untrusted.example.test")

	tlsServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	tlsServer.TLS = &tls.Config{Certificates: []tls.Certificate{trustedCert}, MinVersion: tls.VersionTLS12}
	tlsServer.StartTLS()
	t.Cleanup(tlsServer.Close)

	redirectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, tlsServer.URL+"/metrics", http.StatusFound)
	}))
	t.Cleanup(redirectServer.Close)

	directHTTPServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(directHTTPServer.Close)

	trustedClient := newPrometheusTestHTTPClient(t, "scrape_tls_trusted", promconfig.TLSConfig{
		CA:         string(trustedCertPEM),
		ServerName: serverName,
	})
	requirePrometheusHTTPStatus(t, trustedClient, directHTTPServer.URL, http.StatusOK)
	requirePrometheusHTTPStatus(t, trustedClient, redirectServer.URL, http.StatusOK)

	untrustedClient := newPrometheusTestHTTPClient(t, "scrape_tls_untrusted_ca", promconfig.TLSConfig{
		CA:         string(untrustedCertPEM),
		ServerName: serverName,
	})
	requirePrometheusHTTPError(t, untrustedClient, redirectServer.URL)

	wrongHostnameClient := newPrometheusTestHTTPClient(t, "scrape_tls_hostname_mismatch", promconfig.TLSConfig{
		CA:         string(trustedCertPEM),
		ServerName: "wrong.example.test",
	})
	requirePrometheusHTTPError(t, wrongHostnameClient, redirectServer.URL)

	legacyBypassClient := newPrometheusTestHTTPClient(t, "scrape_tls_legacy_bypass", promconfig.TLSConfig{
		InsecureSkipVerify: true,
		ServerName:         "wrong.example.test",
	})
	requirePrometheusHTTPStatus(t, legacyBypassClient, redirectServer.URL, http.StatusOK)
}

func renderedPrometheusScrapeJobs(t *testing.T, collector unstructured.Unstructured) map[string]map[string]any {
	t.Helper()
	config, found, err := unstructured.NestedMap(collector.Object, "spec", "config")
	if err != nil || !found {
		t.Fatalf("collector config must be present: found=%t error=%v", found, err)
	}
	scrapeConfigs, found, err := unstructured.NestedSlice(config, "receivers", "prometheus", "config", "scrape_configs")
	if err != nil || !found {
		t.Fatalf("Prometheus scrape configs must be present: found=%t error=%v", found, err)
	}
	jobs := make(map[string]map[string]any, len(scrapeConfigs))
	for _, rawConfig := range scrapeConfigs {
		job, ok := rawConfig.(map[string]any)
		if !ok {
			t.Fatalf("Prometheus scrape config has unexpected type: %T", rawConfig)
		}
		if name, ok := job["job_name"].(string); ok {
			jobs[name] = job
		}
	}
	return jobs
}

func renderedPrometheusRelabelRules(t *testing.T, job map[string]any) []map[string]any {
	t.Helper()
	rawRules, found, err := unstructured.NestedSlice(job, "relabel_configs")
	if err != nil || !found {
		t.Fatalf("Prometheus relabel configs must be present: found=%t error=%v", found, err)
	}
	rules := make([]map[string]any, 0, len(rawRules))
	for _, rawRule := range rawRules {
		rule, ok := rawRule.(map[string]any)
		if !ok {
			t.Fatalf("Prometheus relabel rule has unexpected type: %T", rawRule)
		}
		rules = append(rules, rule)
	}
	return rules
}

func hasPrometheusRelabelRule(rules []map[string]any, targetLabel, expected string) bool {
	for _, rule := range rules {
		if rule["target_label"] != targetLabel {
			continue
		}
		if targetLabel == "__address__" || targetLabel == "job" {
			return rule["replacement"] == expected
		}
		return containsString(rule["source_labels"], expected)
	}
	return false
}

func newPrometheusTestHTTPClient(t *testing.T, name string, tlsConfig promconfig.TLSConfig) *http.Client {
	t.Helper()
	client, err := promconfig.NewClientFromConfig(promconfig.HTTPClientConfig{
		FollowRedirects: true,
		TLSConfig:       tlsConfig,
	}, name)
	if err != nil {
		t.Fatalf("failed to create Prometheus HTTP client: %v", err)
	}
	return client
}

func requirePrometheusHTTPStatus(t *testing.T, client *http.Client, target string, wantStatus int) {
	t.Helper()
	response, err := doPrometheusTestRequest(t, client, target)
	if err != nil {
		t.Fatalf("GET %s failed: %v", target, err)
	}
	defer response.Body.Close()
	if response.StatusCode != wantStatus {
		t.Errorf("GET %s returned status %d, want %d", target, response.StatusCode, wantStatus)
	}
}

func requirePrometheusHTTPError(t *testing.T, client *http.Client, target string) {
	t.Helper()
	response, err := doPrometheusTestRequest(t, client, target)
	if response != nil {
		response.Body.Close()
	}
	if err == nil {
		t.Fatalf("GET %s succeeded; expected TLS validation failure", target)
	}
}

func doPrometheusTestRequest(t *testing.T, client *http.Client, target string) (*http.Response, error) {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, target, nil)
	if err != nil {
		t.Fatalf("failed to create GET request for %s: %v", target, err)
	}
	return client.Do(request)
}

func newSelfSignedTestServerCertificate(t *testing.T, serverName string) ([]byte, tls.Certificate) {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate test TLS key: %v", err)
	}
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("failed to generate test TLS certificate serial: %v", err)
	}
	now := time.Now()
	template := x509.Certificate{
		SerialNumber:          serialNumber,
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{serverName},
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("failed to create test TLS certificate: %v", err)
	}
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER})
	privateKeyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatalf("failed to marshal test TLS key: %v", err)
	}
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKeyDER})
	certificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	if err != nil {
		t.Fatalf("failed to load test TLS key pair: %v", err)
	}
	return certificatePEM, certificate
}
