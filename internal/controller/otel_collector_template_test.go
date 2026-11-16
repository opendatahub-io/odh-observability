package controller

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
)

func TestDCGMMetricsAreRetainedWithOriginalNames(t *testing.T) {
	templateBytes, err := resourcesFS.ReadFile(OpenTelemetryCollectorTemplate)
	if err != nil {
		t.Fatalf("failed to read template: %v", err)
	}

	templateContent := string(templateBytes)

	dcgmJob := extractSection(templateContent, "job_name: 'trusted-dcgm-exporter'", "\n      {{- else }}")
	if dcgmJob == "" {
		t.Fatal("dcgm-exporter-accelerator-metrics job must exist in template")
	}

	relabelSection := extractSection(dcgmJob, "relabel_configs:", "metric_relabel_configs:")
	if relabelSection == "" {
		t.Fatal("relabel_configs section must be extractable from dcgm job")
	}
	metricRelabelIndex := strings.Index(dcgmJob, "metric_relabel_configs:")
	if metricRelabelIndex < 0 {
		t.Fatal("metric_relabel_configs section must be extractable from dcgm job")
	}
	metricRelabelSection := dcgmJob[metricRelabelIndex:]

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

func TestPrometheusScrapeConfigContainsDCGMAndModelResourceJobs(t *testing.T) {
	collector := renderCollectorTemplate(t)
	jobs := renderedPrometheusScrapeJobs(t, collector)
	if _, found := jobs["data-science-collector-prometheus"]; found {
		t.Error("unused generic pod scrape job must not be rendered")
	}
	if len(jobs) != 1 || jobs["dcgm-exporter-accelerator-metrics"] == nil {
		t.Fatalf("expected one DCGM scrape job in the TargetAllocator receiver; job count=%d, jobs=%v", len(jobs), jobs)
	}
	resourceJobs := renderedPrometheusScrapeJobsForReceiver(t, collector, "prometheus/cluster_resources")
	if len(resourceJobs) != 1 || resourceJobs["cluster-prometheus-model-resources"] == nil {
		t.Fatalf("expected one model-resource scrape job in the static receiver; job count=%d, jobs=%v", len(resourceJobs), resourceJobs)
	}
}

func TestModelResourceFederationScrapeIsNarrowAndAuthenticated(t *testing.T) {
	collector := renderCollectorTemplate(t)
	job := renderedPrometheusScrapeJobsForReceiver(t, collector, "prometheus/cluster_resources")["cluster-prometheus-model-resources"]
	if job == nil {
		t.Fatal("model-resource federation scrape job must be present")
	}
	pipelines := metricsPipelines(t, collector)
	if containsString(pipelines["metrics/scrape"]["receivers"], "prometheus/cluster_resources") {
		t.Fatal("metrics/scrape pipeline must not include the model-resource federation receiver")
	}
	if !containsString(pipelines["metrics/federation"]["receivers"], "prometheus/cluster_resources") {
		t.Fatal("metrics/federation pipeline must include the model-resource federation receiver")
	}
	metricRelabelConfigs, found, err := unstructured.NestedSlice(job, "metric_relabel_configs")
	if err != nil || !found || len(metricRelabelConfigs) != 2 {
		t.Fatalf("federation job must have two metric relabel rules: found=%t configs=%v error=%v", found, metricRelabelConfigs, err)
	}
	markerRule, ok := metricRelabelConfigs[0].(map[string]any)
	if !ok || markerRule["action"] != "replace" || markerRule["target_label"] != "odh_federation_source" || markerRule["replacement"] != "model-resources" {
		t.Errorf("federation job must mark its source: %v", metricRelabelConfigs[0])
	}
	metricRelabelRule, ok := metricRelabelConfigs[1].(map[string]any)
	if !ok || metricRelabelRule["action"] != "labeldrop" || metricRelabelRule["regex"] != "prometheus(_replica)?" {
		t.Errorf("federation job must drop Prometheus replica identity labels: %v", metricRelabelConfigs[1])
	}
	assertAuthenticatedModelResourceFederationJob(t, job)

	params, found, err := unstructured.NestedStringSlice(job, "params", "match[]")
	if err != nil || !found || len(params) != 3 {
		t.Fatalf("federation job must have exactly three metric matchers: found=%t params=%v error=%v", found, params, err)
	}
	for _, want := range []string{
		`{__name__="node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate"}`,
		`{__name__="kube_resourcequota", resource=~"cpu|requests[.]cpu|limits[.]cpu", type=~"hard|used"}`,
		`{__name__="kube_pod_container_resource_limits", resource="cpu", unit="core"}`,
	} {
		if !slices.Contains(params, want) {
			t.Errorf("federation job is missing matcher %q: %v", want, params)
		}
	}
}

func assertAuthenticatedModelResourceFederationJob(t *testing.T, job map[string]any) {
	t.Helper()

	for key, want := range map[string]any{
		"scheme":            "https",
		"metrics_path":      "/federate",
		"bearer_token_file": "/var/run/secrets/kubernetes.io/serviceaccount/token",
		"honor_labels":      true,
		"scrape_interval":   "30s",
		"scrape_timeout":    "10s",
	} {
		if got := job[key]; got != want {
			t.Errorf("federation job %q: got %v, want %v", key, got, want)
		}
	}

	tlsConfig, found, err := unstructured.NestedMap(job, "tls_config")
	if err != nil || !found {
		t.Fatalf("federation job must configure TLS: found=%t error=%v", found, err)
	}
	if tlsConfig["ca_file"] != "/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt" || tlsConfig["server_name"] != "prometheus-k8s.openshift-monitoring.svc" {
		t.Errorf("federation job must use the in-cluster service CA and Prometheus service name: %v", tlsConfig)
	}

	staticConfigs, found, err := unstructured.NestedSlice(job, "static_configs")
	if err != nil || !found || len(staticConfigs) != 1 {
		t.Fatalf("federation job must have one static target group: found=%t configs=%v error=%v", found, staticConfigs, err)
	}
	staticConfig, ok := staticConfigs[0].(map[string]any)
	if !ok {
		t.Fatalf("federation static target group has unexpected type %T", staticConfigs[0])
	}
	targets, ok := staticConfig["targets"].([]any)
	if !ok || len(targets) != 1 || targets[0] != "prometheus-k8s.openshift-monitoring.svc:9091" {
		t.Errorf("federation job must target OpenShift Prometheus web: targets=%v", staticConfig["targets"])
	}
}

func TestModelResourceFederationIsOmittedWithoutMetricsStorage(t *testing.T) {
	collector := renderCollectorTemplateWithOptionsAndStorage(t, true, true, false)
	config, found, err := unstructured.NestedMap(collector.Object, "spec", "config")
	if err != nil || !found {
		t.Fatalf("collector config must be present: found=%t error=%v", found, err)
	}
	if _, found, err := unstructured.NestedMap(config, "receivers", "prometheus/cluster_resources"); err != nil {
		t.Fatalf("reading model-resource receiver: %v", err)
	} else if found {
		t.Fatal("model-resource federation receiver must not render without metrics storage")
	}
	if _, found, err := unstructured.NestedMap(config, "exporters", "prometheus/federation"); err != nil || found {
		t.Fatalf("federation exporter must not render without metrics storage: found=%t error=%v", found, err)
	}
	if containsString(metricsPipelines(t, collector)["metrics/scrape"]["receivers"], "prometheus/cluster_resources") {
		t.Fatal("metrics/scrape pipeline must not include model-resource federation without metrics storage")
	}
}

func TestCollectorMemoryLimiterFitsDefaultContainerLimit(t *testing.T) {
	collector := renderCollectorTemplate(t)
	limiter, found, err := unstructured.NestedMap(collector.Object, "spec", "config", "processors", "memory_limiter")
	if err != nil || !found {
		t.Fatalf("memory limiter must be configured: found=%t error=%v", found, err)
	}
	hard, hardOK := limiter["limit_mib"].(int64)
	spike, spikeOK := limiter["spike_limit_mib"].(int64)
	containerLimit := resource.MustParse(defaultCollectorMemoryLimit)
	if !hardOK || !spikeOK || spike <= 0 || spike >= hard || hard*(1<<20) >= containerLimit.Value() {
		t.Fatalf("memory limits must be positive, with spike < hard < container limit %s: %v", defaultCollectorMemoryLimit, limiter)
	}
}

func TestPrometheusReceiverUsesTargetAllocatorWithoutStaticScrapes(t *testing.T) {
	collector := renderCollectorTemplateWithAcceleratorMetrics(t, false)
	prometheusReceiver, found, err := unstructured.NestedMap(collector.Object, "spec", "config", "receivers", "prometheus")
	if err != nil || !found {
		t.Fatalf("Prometheus receiver must remain present: found=%t, error=%v", found, err)
	}
	if _, found := prometheusReceiver["config"]; found {
		t.Fatal("Prometheus receiver must omit config when no static scrape jobs are configured")
	}
	if _, found, err := unstructured.NestedMap(prometheusReceiver, "target_allocator"); err != nil || !found {
		t.Fatalf("Prometheus receiver must seed TargetAllocator config: found=%t, error=%v", found, err)
	}
}

func TestBuiltinDCGMScrapeCanBeDisabledWithoutDisablingMonitorCollection(t *testing.T) {
	collector := renderCollectorTemplateWithDCGMScrapeEnabled(t, false)

	config, found, err := unstructured.NestedMap(collector.Object, "spec", "config")
	if err != nil || !found {
		t.Fatalf("collector config must be present: found=%t, error=%v", found, err)
	}
	receivers, found, err := unstructured.NestedMap(config, "receivers")
	if err != nil || !found {
		t.Fatalf("collector receivers must be present: found=%t, error=%v", found, err)
	}
	prometheusReceiver, ok := receivers["prometheus"].(map[string]any)
	if !ok {
		t.Fatalf("Prometheus receiver must remain configured, got %T", receivers["prometheus"])
	}
	if _, found := prometheusReceiver["config"]; found {
		t.Fatal("built-in DCGM opt-out must omit Prometheus config so TargetAllocator can provide monitor scrapes")
	}
	if _, found, err := unstructured.NestedMap(prometheusReceiver, "target_allocator"); err != nil || !found {
		t.Fatalf("built-in DCGM opt-out must retain TargetAllocator receiver config: found=%t, error=%v", found, err)
	}

	if !containsString(metricsPipelines(t, collector)["metrics/scrape"]["receivers"], "prometheus") {
		t.Fatal("metrics/scrape pipeline must keep Prometheus receiver for TargetAllocator-discovered monitors")
	}
	if !containsString(metricsPipelines(t, collector)["metrics/scrape"]["processors"], "transform/gpu_metrics") {
		t.Fatal("metrics/scrape pipeline must retain accelerator metric processing")
	}

	targetAllocator, found, err := unstructured.NestedMap(collector.Object, "spec", "targetAllocator")
	if err != nil || !found || targetAllocator["enabled"] != true {
		t.Fatalf("TargetAllocator must remain enabled for monitor discovery: found=%t enabled=%t error=%v", found, targetAllocator["enabled"] == true, err)
	}
	prometheusCR, found, err := unstructured.NestedMap(targetAllocator, "prometheusCR")
	if err != nil || !found {
		t.Fatalf("TargetAllocator Prometheus CR discovery must remain configured: found=%t error=%v", found, err)
	}
	for _, selector := range []string{"podMonitorSelector", "serviceMonitorSelector"} {
		matchLabels, found, err := unstructured.NestedStringMap(prometheusCR, selector, "matchLabels")
		if err != nil || !found || matchLabels["monitoring.opendatahub.io/scrape"] != "true" {
			t.Errorf("TargetAllocator %s must remain selected: found=%t labels=%v error=%v", selector, found, matchLabels, err)
		}
	}
}

func TestBuiltinDCGMScrapeRequiresAcceleratorAndScrapeFlags(t *testing.T) {
	tests := []struct {
		name               string
		acceleratorMetrics bool
		scrapeEnabled      bool
		wantJob            bool
	}{
		{name: "both disabled", acceleratorMetrics: false, scrapeEnabled: false},
		{name: "only scrape enabled", acceleratorMetrics: false, scrapeEnabled: true},
		{name: "only accelerator metrics enabled", acceleratorMetrics: true, scrapeEnabled: false},
		{name: "both enabled", acceleratorMetrics: true, scrapeEnabled: true, wantJob: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			collector := renderCollectorTemplateWithOptions(t, test.acceleratorMetrics, test.scrapeEnabled)
			config, found, err := unstructured.NestedMap(collector.Object, "spec", "config")
			if err != nil || !found {
				t.Fatalf("collector config must be present: found=%t, error=%v", found, err)
			}
			scrapeConfigs, found, err := unstructured.NestedSlice(config, "receivers", "prometheus", "config", "scrape_configs")
			if err != nil {
				t.Fatalf("read Prometheus scrape configs: %v", err)
			}
			if test.wantJob {
				if !found || len(scrapeConfigs) != 1 {
					t.Fatalf("both flags enabled must render exactly one DCGM job: found=%t count=%d", found, len(scrapeConfigs))
				}
				job, ok := scrapeConfigs[0].(map[string]any)
				if !ok {
					t.Fatalf("built-in DCGM scrape config has unexpected type %T", scrapeConfigs[0])
				}
				if job["job_name"] != "dcgm-exporter-accelerator-metrics" {
					t.Fatalf("unexpected built-in DCGM job name %q", job["job_name"])
				}
				return
			}
			if found && len(scrapeConfigs) != 0 {
				t.Fatalf("DCGM job must be absent unless both flags are enabled; scrape config count=%d", len(scrapeConfigs))
			}
		})
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
		if name == "metrics/federation" {
			if containsString(pipeline["processors"], "transform/sanitize_k8s_labels") {
				t.Error("metrics/federation pipeline must not invoke workload label sanitization")
			}
			continue
		}
		if !containsString(pipeline["processors"], "transform/gpu_metrics") {
			t.Errorf("metrics pipeline %q must invoke the GPU metric transform processor", name)
		}
	}
}

func TestWorkloadPipelinesSanitizeSourceMarkers(t *testing.T) {
	collector := renderCollectorTemplate(t)
	processors := renderedProcessors(t, collector)
	transform, found, err := unstructured.NestedMap(processors, "transform/sanitize_provenance_markers")
	if err != nil || !found {
		t.Fatalf("source marker sanitizer must be present: found=%t error=%v", found, err)
	}
	metricStatements, ok := transform["metric_statements"].([]any)
	if !ok || len(metricStatements) != 2 {
		t.Fatalf("source marker sanitizer must contain resource and datapoint blocks: %v", transform["metric_statements"])
	}
	blocks := statementBlocksByContext(t, metricStatements)
	for _, context := range []string{"resource", "datapoint"} {
		requireStatements(t, "source marker sanitizer "+context, blocks[context], []string{
			`delete_matching_keys(attributes, "^odh[^a-zA-Z0-9]+federation[^a-zA-Z0-9]+source$$")`,
			`delete_matching_keys(attributes, "^odh[^a-zA-Z0-9]+dcgm[^a-zA-Z0-9]+.*$$")`,
		})
	}

	// Collector config expansion turns $$ into $. Verify the regex covers
	// spellings the Prometheus exporter normalizes to the same marker while
	// leaving unrelated labels intact.
	for _, kind := range []string{"federation", "dcgm"} {
		pattern := regexp.MustCompile(`^odh[^a-zA-Z0-9]+` + kind + `[^a-zA-Z0-9]+source$`)
		for _, key := range []string{"odh_" + kind + "_source", "odh." + kind + ".source", "odh-" + kind + "/source", "odh__" + kind + "__source", "odh._-" + kind + "_./source"} {
			if !pattern.MatchString(key) {
				t.Errorf("marker sanitizer must remove normalized spelling %q", key)
			}
		}
		for _, key := range []string{"odh_" + kind + "_source_extra", "odh_" + kind + "_source2", "prefix_odh_" + kind + "_source"} {
			if pattern.MatchString(key) {
				t.Errorf("marker sanitizer must preserve unrelated key %q", key)
			}
		}
	}
	for _, key := range []string{"odh_dcgm_workload_namespace", "odh.dcgm.workload.pod", "odh-dcgm/source", "odh__dcgm__workload__namespace", "odh._dcgm-.workload__pod"} {
		if !regexp.MustCompile(`^odh[^a-zA-Z0-9]+dcgm[^a-zA-Z0-9]+.*$`).MatchString(key) {
			t.Errorf("DCGM sanitizer must remove internal attribution label %q", key)
		}
	}

	pipelines := metricsPipelines(t, collector)
	for _, name := range []string{"metrics/scrape", "metrics/otlp"} {
		if !containsString(pipelines[name]["processors"], "transform/sanitize_provenance_markers") {
			t.Errorf("%s pipeline must sanitize source markers", name)
		}
	}
	if containsString(pipelines["metrics/federation"]["processors"], "transform/sanitize_provenance_markers") {
		t.Error("metrics/federation pipeline must preserve its verified federation marker")
	}
}

func TestWorkloadSourcesCannotMarkDCGMAsVerified(t *testing.T) {
	collector := renderCollectorTemplate(t)
	processors := renderedProcessors(t, collector)
	pipelines := metricsPipelines(t, collector)
	for _, name := range []string{"metrics/scrape", "metrics/otlp"} {
		pipelineProcessors, ok := pipelines[name]["processors"].([]any)
		if !ok {
			t.Fatalf("%s processors have unexpected type: %T", name, pipelines[name]["processors"])
		}
		for _, rawName := range pipelineProcessors {
			processorName, ok := rawName.(string)
			if !ok {
				t.Fatalf("%s processor name has unexpected type: %T", name, rawName)
			}
			if strings.Contains(fmt.Sprint(processors[processorName]), `set(attributes["odh_dcgm_source"]`) {
				t.Errorf("%s must not set a verified DCGM marker based on spoofable metadata: %s", name, processorName)
			}
		}
	}
}

// The Prometheus exporter may replace runs of punctuation with one underscore.
// Exercise the rendered sanitizer patterns against the spellings that can
// normalize to reserved markers, not just against the canonical spelling.
func TestProvenanceSanitizerCoversNormalizedMarkers(t *testing.T) {
	collector := renderCollectorTemplate(t)
	processor := renderedProcessors(t, collector)["transform/sanitize_provenance_markers"]
	processorConfig, ok := processor.(map[string]any)
	if !ok {
		t.Fatalf("invalid provenance sanitizer: %v", processor)
	}
	blocks, ok := processorConfig["metric_statements"].([]any)
	if !ok || len(blocks) != 2 {
		t.Fatalf("invalid provenance sanitizer: %v", processor)
	}
	for context, block := range statementBlocksByContext(t, blocks) {
		statements, ok := block["statements"].([]any)
		if !ok {
			t.Fatalf("invalid %s sanitizer statements: %v", context, block)
		}
		for _, raw := range statements {
			statement, ok := raw.(string)
			if !ok {
				t.Fatalf("invalid %s sanitizer statement: %v", context, raw)
			}
			start := strings.IndexByte(statement, '"')
			end := strings.LastIndexByte(statement, '"')
			if start < 0 || end <= start {
				t.Fatalf("invalid %s statement: %s", context, statement)
			}
			pattern := regexp.MustCompile(strings.ReplaceAll(statement[start+1:end], "$$", "$"))
			kind := "dcgm"
			suffixes := []string{"source", "workload_namespace", "workload_pod"}
			if strings.Contains(statement, "federation") {
				kind = "federation"
				suffixes = []string{"source"}
			}
			for _, separator := range []string{"_", "__", ".", "-", "._-", "🔥"} {
				for _, suffix := range suffixes {
					key := "odh" + separator + kind + separator + strings.ReplaceAll(suffix, "_", separator)
					if !pattern.MatchString(key) {
						t.Errorf("%s sanitizer must remove %q before normalization", context, key)
					}
				}
			}
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
		`delete_matching_keys(attributes, "^k8s[^a-zA-Z0-9]+namespace[^a-zA-Z0-9]+name$$")`,
		`delete_matching_keys(attributes, "^k8s[^a-zA-Z0-9]+pod[^a-zA-Z0-9]+name$$")`,
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
		if name == "metrics/federation" {
			if containsString(pipeline["processors"], "transform/sanitize_k8s_labels") {
				t.Error("metrics/federation pipeline must not invoke workload label sanitization")
			}
			continue
		}
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
// scope-out of namespace/pod promotion. Monitor relabelings can spoof the
// service.name derived from job, so the exclusion must also require the
// collector-enriched target namespace to be nvidia-gpu-operator. The ordered
// resource-context statements remove exporter pod identity without allowing
// tenant monitors to opt out of namespace/pod attribution. Node/component
// remain for Korrel8r. The processor stays in the scrape pipeline; OTLP
// pushers use a separate identity sanitizer.
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

	// Both the DCGM job identity and trusted target namespace must scope these
	// statements. Their order is significant: the first guard needs the namespace
	// attribute that the second statement removes.
	dcgmJobName := dcgmScrapeJobName(t, collector)
	dcgmTargetNamespace := "nvidia-gpu-operator"
	expectedStatements := []string{
		fmt.Sprintf(`delete_key(attributes, "k8s.pod.name") where attributes["service.name"] == %q and attributes["k8s.namespace.name"] == %q`, dcgmJobName, dcgmTargetNamespace),
		fmt.Sprintf(`delete_key(attributes, "k8s.namespace.name") where attributes["service.name"] == %q and attributes["k8s.namespace.name"] == %q`, dcgmJobName, dcgmTargetNamespace),
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

// metricsPipelines returns every rendered data metrics pipeline keyed by its
// configuration name, failing the test unless both scrape and OTLP pipelines
// exist. The collector's internal telemetry pipeline is excluded.
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

	result := make(map[string]map[string]any, len(pipelines))
	for name, rawPipeline := range pipelines {
		if name == "metrics/internal" {
			continue
		}
		pipeline, ok := rawPipeline.(map[string]any)
		if !ok {
			t.Fatalf("metrics pipeline %q has unexpected type: %T", name, rawPipeline)
		}
		result[name] = pipeline
	}
	for _, name := range []string{"metrics/scrape", "metrics/otlp"} {
		if _, ok := result[name]; !ok {
			t.Fatalf("metrics pipeline %q must be present in rendered resource", name)
		}
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
		`delete_matching_keys(attributes, "^k8s[^a-zA-Z0-9]+namespace[^a-zA-Z0-9]+name$$")`,
		`delete_matching_keys(attributes, "^k8s[^a-zA-Z0-9]+pod[^a-zA-Z0-9]+name$$")`,
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
	scrapePipeline, federationPipeline, otlpPipeline := pipelines["metrics/scrape"], pipelines["metrics/federation"], pipelines["metrics/otlp"]

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
	federationReceivers, ok := federationPipeline["receivers"].([]any)
	if !ok {
		t.Fatalf("metrics/federation pipeline receivers have unexpected type: %T", federationPipeline["receivers"])
	}
	if !containsString(federationReceivers, "prometheus/cluster_resources") || len(federationReceivers) != 1 {
		t.Errorf("metrics/federation pipeline must receive only the federation receiver, got %v", federationReceivers)
	}
	if containsString(federationPipeline["processors"], "transform/sanitize_provenance_markers") || containsString(federationPipeline["processors"], "transform/sanitize_k8s_labels") {
		t.Errorf("metrics/federation pipeline must not apply workload sanitizers, got %v", federationPipeline["processors"])
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
		t.Errorf("scrape and OTLP pipelines must share exporters, scrape=%v otlp=%v", scrapePipeline["exporters"], otlpPipeline["exporters"])
	}
}

func TestFederationExcludesCustomExporters(t *testing.T) {
	for _, storage := range []bool{true, false} {
		t.Run(fmt.Sprintf("storage=%t", storage), func(t *testing.T) {
			collector := renderCollectorTemplateWithExporters(t, true, true, storage, 3, []string{"otlp/first", "otlp/second"})
			pipelines := metricsPipelines(t, collector)
			wantWorkload := []any{"otlp/first", "otlp/second"}
			if storage {
				wantWorkload = append([]any{"prometheus"}, wantWorkload...)
			}
			for _, name := range []string{"metrics/scrape", "metrics/otlp"} {
				if got := pipelines[name]["exporters"]; !reflect.DeepEqual(got, wantWorkload) {
					t.Errorf("%s exporters: got %v, want %v", name, got, wantWorkload)
				}
			}
			federation, found := pipelines["metrics/federation"]
			if !storage {
				if found {
					t.Error("federation pipeline must be absent without metrics storage")
				}
				return
			}
			if !found || !reflect.DeepEqual(federation["exporters"], []any{"prometheus/federation"}) {
				t.Errorf("federation must export only to its isolated storage endpoint: %v", federation)
			}
			exporter, found, err := unstructured.NestedMap(collector.Object, "spec", "config", "exporters", "prometheus/federation")
			if err != nil || !found || exporter["endpoint"] != "0.0.0.0:8891" {
				t.Fatalf("federation exporter must use the dedicated endpoint: found=%t exporter=%v error=%v", found, exporter, err)
			}
			tls, found, err := unstructured.NestedMap(exporter, "tls")
			if err != nil || !found || tls["cert_file"] != "/etc/otel-collector/tls/tls.crt" || tls["key_file"] != "/etc/otel-collector/tls/tls.key" {
				t.Errorf("federation exporter must use the collector serving certificate: found=%t tls=%v error=%v", found, tls, err)
			}
		})
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
	scrapeConfigs, found, err := unstructured.NestedSlice(config, "receivers", "prometheus/trusted_dcgm", "config", "scrape_configs")
	if err != nil || !found {
		t.Fatalf("expected scrape configs in rendered resource: found=%t, error=%v", found, err)
	}
	for _, rawScrapeConfig := range scrapeConfigs {
		scrapeConfig, ok := rawScrapeConfig.(map[string]any)
		if !ok || scrapeConfig["job_name"] != "trusted-dcgm-exporter" {
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
	scrapeConfigs, found, err := unstructured.NestedSlice(config, "receivers", "prometheus/trusted_dcgm", "config", "scrape_configs")
	if err != nil || !found || len(scrapeConfigs) == 0 {
		t.Fatalf("expected DCGM scrape config in rendered resource: found=%t, error=%v", found, err)
	}
	var dcgmJob map[string]any
	for _, rawScrapeConfig := range scrapeConfigs {
		scrapeConfig, ok := rawScrapeConfig.(map[string]any)
		if ok && scrapeConfig["job_name"] == "trusted-dcgm-exporter" {
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
		if rule["action"] == "labeldrop" && rule["regex"] != `odh[^a-zA-Z0-9]+dcgm[^a-zA-Z0-9]+.*` {
			t.Errorf("only internal provenance labels may be dropped: %v", rule)
		}
		for _, label := range []string{"pod", "namespace", "dra_claim_name"} {
			if rule["target_label"] == label {
				t.Errorf("metric relabel rule must not modify DRA attribution label %q: %v", label, rule)
			}
		}
	}
	keepRule, ok := metricRelabelConfigs[0].(map[string]any)
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
	return renderCollectorTemplateWithOptions(t, acceleratorMetrics, acceleratorMetrics)
}

func renderCollectorTemplateWithDCGMScrapeEnabled(t *testing.T, enabled bool) unstructured.Unstructured {
	t.Helper()
	return renderCollectorTemplateWithOptions(t, true, enabled)
}

func renderCollectorTemplateWithOptions(t *testing.T, acceleratorMetrics, builtinDCGMScrapeEnabled bool) unstructured.Unstructured {
	t.Helper()
	return renderCollectorTemplateWithOptionsAndStorage(t, acceleratorMetrics, builtinDCGMScrapeEnabled, true)
}

func renderCollectorTemplateWithOptionsAndStorage(t *testing.T, acceleratorMetrics, builtinDCGMScrapeEnabled, metricsStorage bool) unstructured.Unstructured {
	t.Helper()
	return renderCollectorTemplateWithExporters(t, acceleratorMetrics, builtinDCGMScrapeEnabled, metricsStorage, 1, nil)
}

func renderCollectorTemplateWithExporters(
	t *testing.T, acceleratorMetrics, builtinDCGMScrapeEnabled, metricsStorage bool, replicas int, exporterNames []string,
) unstructured.Unstructured {
	t.Helper()
	return renderCollectorTemplateWithTLS(t, acceleratorMetrics, builtinDCGMScrapeEnabled, metricsStorage, replicas, exporterNames, nil)
}

func renderCollectorTemplateWithTLS(
	t *testing.T, acceleratorMetrics, builtinDCGMScrapeEnabled, metricsStorage bool, replicas int, exporterNames []string, dcgmTLS *v1alpha1.DCGMTLS,
) unstructured.Unstructured {
	t.Helper()
	exporters := make(map[string]string, len(exporterNames))
	for _, name := range exporterNames {
		exporters[name] = "endpoint: example.com:4317"
	}
	resources, err := rendertemplate.Render(context.Background(), nil, []rendertemplate.TemplateSource{{
		FS:   resourcesFS,
		Path: OpenTelemetryCollectorTemplate,
	}}, map[string]any{
		"Namespace":                       "redhat-ods-monitoring",
		"Metrics":                         true,
		"MetricsStorage":                  metricsStorage,
		"AcceleratorMetrics":              acceleratorMetrics,
		"BuiltinDCGMScrapeEnabled":        builtinDCGMScrapeEnabled,
		"BuiltinDCGMTLS":                  dcgmTLS,
		"Traces":                          false,
		"CollectorReplicas":               replicas,
		"CollectorCPULimit":               "1",
		"CollectorMemoryLimit":            "1Gi",
		"CollectorCPURequest":             "100m",
		"CollectorMemoryRequest":          "256Mi",
		"MetricsExporterNames":            exporterNames,
		"MetricsExporters":                exporters,
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

func TestDCGMScrapeJobUsesHTTPWithoutCredentialsAndKeepsDefaultRedirects(t *testing.T) {
	jobs := renderedPrometheusScrapeJobs(t, renderCollectorTemplate(t))
	job := jobs["dcgm-exporter-accelerator-metrics"]
	if job == nil {
		t.Fatal("DCGM scrape job must be present")
	}
	if job["scheme"] != "http" {
		t.Errorf("DCGM scrape job must explicitly use HTTP, got %v", job["scheme"])
	}
	for _, credentialField := range []string{"bearer_token_file", "bearer_token", "authorization", "basic_auth"} {
		if _, found := job[credentialField]; found {
			t.Errorf("DCGM scrape job must not configure credential field %s", credentialField)
		}
	}
	if _, found := job["follow_redirects"]; found {
		t.Errorf("DCGM scrape job must preserve Prometheus default redirect behavior, got %v", job["follow_redirects"])
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
		"job":         "rhoai-accelerator-discovery",
	}
	for targetLabel, expected := range expectedRules {
		if !hasPrometheusRelabelRule(relabelRules, targetLabel, expected) {
			t.Errorf("DCGM scrape job must retain relabeling for %q=%q", targetLabel, expected)
		}
	}
}

func renderedPrometheusScrapeJobs(t *testing.T, collector unstructured.Unstructured) map[string]map[string]any {
	t.Helper()
	return renderedPrometheusScrapeJobsForReceiver(t, collector, "prometheus")
}

func renderedPrometheusScrapeJobsForReceiver(t *testing.T, collector unstructured.Unstructured, receiverName string) map[string]map[string]any {
	t.Helper()
	config, found, err := unstructured.NestedMap(collector.Object, "spec", "config")
	if err != nil || !found {
		t.Fatalf("collector config must be present: found=%t error=%v", found, err)
	}
	scrapeConfigs, found, err := unstructured.NestedSlice(config, "receivers", receiverName, "config", "scrape_configs")
	if err != nil || !found {
		t.Fatalf("Prometheus scrape configs for receiver %q must be present: found=%t error=%v", receiverName, found, err)
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
