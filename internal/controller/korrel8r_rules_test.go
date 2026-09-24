/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"bytes"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"text/template"

	"gopkg.in/yaml.v3"
)

type korrel8rRulesFile struct {
	Rules []korrel8rRule `yaml:"rules"`
}

type korrel8rRule struct {
	Name  string `yaml:"name"`
	Start struct {
		Domain  string   `yaml:"domain"`
		Classes []string `yaml:"classes"`
	} `yaml:"start"`
	Goal struct {
		Domain  string   `yaml:"domain"`
		Classes []string `yaml:"classes"`
	} `yaml:"goal"`
	Result struct {
		Query string `yaml:"query"`
	} `yaml:"result"`
}

// TestKorrel8rRHOAIInferenceRulesRenderRepresentativeObjects catches a missing
// ConfigMap key, invalid rule file, invalid template, or a rule that no longer
// produces the resource queries on which the built-in Pod telemetry rules rely.
func TestKorrel8rRHOAIInferenceRulesRenderRepresentativeObjects(t *testing.T) {
	rules := loadRHOAIKorrel8rRules(t)

	tests := []struct {
		name     string
		ruleName string
		fixture  string
		want     []string
	}{
		{
			name:     "LLMInferenceService reaches its workload pods",
			ruleName: "LLMInferenceServiceToServingPods",
			fixture:  "testdata/korrel8r/llminferenceservice.json",
			want: []string{
				`k8s:Pod:{"namespace":"inference","labels":{"app.kubernetes.io/name":"llama","app.kubernetes.io/part-of":"llminferenceservice"}}`,
			},
		},
		{
			name:     "LLMInferenceService without a name emits no pod query",
			ruleName: "LLMInferenceServiceToServingPods",
			fixture:  "testdata/korrel8r/llminferenceservice-without-name.json",
		},
		{
			name:     "HTTPRoute reaches its service backends",
			ruleName: "HTTPRouteToBackendService",
			fixture:  "testdata/korrel8r/httproute.json",
			want: []string{
				`k8s:Service:{"namespace":"inference","name":"llama-epp-service"}`,
				`k8s:Service:{"namespace":"shared","name":"llama-vllm-service"}`,
			},
		},
		{
			name:     "HTTPRoute without a backend name emits no service query",
			ruleName: "HTTPRouteToBackendService",
			fixture:  "testdata/korrel8r/httproute-without-backend-name.json",
		},
		{
			name:     "HTTPRoute without backend references emits no service query",
			ruleName: "HTTPRouteToBackendService",
			fixture:  "testdata/korrel8r/httproute-without-backendrefs.json",
		},
		{
			name:     "HTTPRoute reaches its InferencePool backends",
			ruleName: "HTTPRouteToInferencePool",
			fixture:  "testdata/korrel8r/httproute.json",
			want: []string{
				`k8s:InferencePool.v1.inference.networking.k8s.io:{"namespace":"inference","name":"llama-pool"}`,
			},
		},
		{
			name:     "InferencePool reaches selected serving pods",
			ruleName: "InferencePoolToServingPods",
			fixture:  "testdata/korrel8r/inferencepool.json",
			want: []string{
				`k8s:Pod:{"namespace":"inference","labels":{"app.kubernetes.io/name":"llama","app.kubernetes.io/part-of":"llminferenceservice","kserve.io/component":"workload"}}`,
			},
		},
		{
			name:     "InferencePool without a required selector emits no pod query",
			ruleName: "InferencePoolToServingPods",
			fixture:  "testdata/korrel8r/inferencepool-without-selector-label.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := findKorrel8rRule(t, rules, tt.ruleName)
			got := renderKorrel8rQuery(t, rule.Result.Query, loadKorrel8rFixture(t, tt.fixture))
			if got != strings.Join(tt.want, "\n") {
				t.Fatalf("unexpected rendered query:\nwant:\n%s\n\ngot:\n%s", strings.Join(tt.want, "\n"), got)
			}
		})
	}
}

func TestKorrel8rRHOAIMetricsRuleUsesCollectorExportedPodLabels(t *testing.T) {
	configTemplate, err := resourcesFS.ReadFile(Korrel8rConfigTemplate)
	if err != nil {
		t.Fatalf("reading Korrel8r ConfigMap template: %v", err)
	}

	var out bytes.Buffer
	if err := template.Must(template.New("korrel8r-config").Option("missingkey=error").Parse(string(configTemplate))).Execute(&out, map[string]any{
		"Namespace":              "monitoring",
		"Korrel8rServiceName":    Korrel8rServiceName,
		"Metrics":                true,
		"MetricsStorage":         true,
		"TempoStorage":           false,
		"Logs":                   false,
		"ThanosQuerierEndpoint":  "https://thanos.example.test",
		"Korrel8rRequestTimeout": "30s",
		"Korrel8rSessionTimeout": "5m",
	}); err != nil {
		t.Fatalf("rendering Korrel8r ConfigMap template: %v", err)
	}

	var configMap struct {
		Data map[string]string `yaml:"data"`
	}
	if err := yaml.Unmarshal(out.Bytes(), &configMap); err != nil {
		t.Fatalf("parsing rendered Korrel8r ConfigMap: %v", err)
	}
	var rulesFile korrel8rRulesFile
	if err := yaml.Unmarshal([]byte(configMap.Data["rhoai-metrics.yaml"]), &rulesFile); err != nil {
		t.Fatalf("parsing RHOAI metrics rules: %v", err)
	}

	rule := findKorrel8rRule(t, rulesFile.Rules, "PodToRHOAIMetric")
	got := renderKorrel8rQuery(t, rule.Result.Query, map[string]any{
		"metadata": map[string]any{"namespace": "inference", "name": "llama-pod"},
	})
	const want = `metric:metric:{exported_namespace="inference",exported_pod="llama-pod"}`
	if got != want {
		t.Fatalf("RHOAI metrics rule must query the collector's exported workload labels:\nwant: %s\n got: %s", want, got)
	}
}

func TestKorrel8rConfigChecksumChangesWithEffectiveConfiguration(t *testing.T) {
	t.Parallel()

	data := map[string]any{
		"Metrics":                true,
		"MetricsStorage":         true,
		"Traces":                 true,
		"TempoStorage":           true,
		"Logs":                   false,
		"ThanosQuerierEndpoint":  "http://thanos.example.test:10902",
		"TempoQueryEndpoint":     "https://tempo.example.test:8080",
		"LokiQueryEndpoint":      "https://loki.example.test:8080",
		"Korrel8rRequestTimeout": "30s",
		"Korrel8rSessionTimeout": "5m",
	}
	if err := addKorrel8rConfigChecksum(data); err != nil {
		t.Fatalf("adding Korrel8r config checksum: %v", err)
	}
	first, ok := data["Korrel8rConfigChecksum"].(string)
	if !ok || first == "" {
		t.Fatalf("expected a non-empty Korrel8r config checksum, got %#v", data["Korrel8rConfigChecksum"])
	}

	data["ThanosQuerierEndpoint"] = "http://other-thanos.example.test:10902"
	if err := addKorrel8rConfigChecksum(data); err != nil {
		t.Fatalf("updating Korrel8r config checksum: %v", err)
	}
	if second := data["Korrel8rConfigChecksum"]; second == first {
		t.Fatalf("checksum did not change after its metric store changed: %q", second)
	}
}

func TestKorrel8rRepresentativeAuditFixtures(t *testing.T) {
	eppDeployment := loadKorrel8rFixture(t, "testdata/korrel8r/epp-deployment.json")
	vllmPod := loadKorrel8rFixture(t, "testdata/korrel8r/vllm-pod.json")
	dcgmMetric := loadKorrel8rFixture(t, "testdata/korrel8r/dcgm-metric.json")
	node := loadKorrel8rFixture(t, "testdata/korrel8r/node.json")
	lokiRecord := loadKorrel8rFixture(t, "testdata/korrel8r/loki-record.json")
	tempoSpan := loadKorrel8rFixture(t, "testdata/korrel8r/tempo-span.json")
	eppMetric := loadKorrel8rFixture(t, "testdata/korrel8r/epp-metric-without-workload-labels.json")

	for _, object := range []map[string]any{eppDeployment, vllmPod} {
		if got := korrel8rFixtureString(t, object, "metadata", "labels", "app.kubernetes.io/name"); got != "llama" {
			t.Fatalf("expected RHOAI workload name label llama, got %q", got)
		}
		if got := korrel8rFixtureString(t, object, "metadata", "labels", "app.kubernetes.io/part-of"); got != "llminferenceservice" {
			t.Fatalf("expected RHOAI workload part-of label, got %q", got)
		}
	}
	if got := korrel8rFixtureString(t, dcgmMetric, "labels", "node"); got != "gpu-worker" {
		t.Fatalf("expected DCGM node label gpu-worker, got %q", got)
	}
	if got := korrel8rFixtureString(t, node, "metadata", "name"); got != korrel8rFixtureString(t, vllmPod, "spec", "nodeName") {
		t.Fatalf("GPU Pod node and Node fixture must match, got %q and %q", korrel8rFixtureString(t, vllmPod, "spec", "nodeName"), got)
	}
	if got := korrel8rFixtureString(t, vllmPod, "spec", "containers", "0", "resources", "limits", "nvidia.com/gpu"); got != "1" {
		t.Fatalf("expected vLLM fixture to request one GPU, got %q", got)
	}
	if got := korrel8rFixtureString(t, tempoSpan, "traceID"); got == "" {
		t.Fatal("Tempo fixture must contain its traceID")
	}
	if got := korrel8rFixtureString(t, tempoSpan, "attributes", "k8s.namespace.name"); got != "inference" {
		t.Fatalf("expected Tempo namespace attribute inference, got %q", got)
	}
	if got := korrel8rFixtureString(t, tempoSpan, "attributes", "k8s.pod.name"); got != "llama-vllm" {
		t.Fatalf("expected Tempo Pod attribute llama-vllm, got %q", got)
	}
	if strings.Contains(korrel8rFixtureString(t, lokiRecord, "line"), "trace_id") {
		t.Fatal("Loki fixture must model the current unstructured application-log path without trace_id")
	}
	for _, label := range []string{"exported_namespace", "exported_pod"} {
		if _, found := korrel8rFixtureMap(t, eppMetric, "labels")[label]; found {
			t.Fatalf("EPP metric fixture unexpectedly contains %q", label)
		}
	}
}

func TestKorrel8rSkipsEPPMetricRuleWithoutVerifiedLabels(t *testing.T) {
	rules := loadRHOAIKorrel8rRules(t)
	for _, rule := range rules {
		if rule.Name == "EPPPodToRHOAIMetric" {
			t.Fatal("EPP metric rule must not ship without verified workload labels")
		}
	}

	rule := findKorrel8rRule(t, rules, "LLMInferenceServiceToServingPods")
	got := renderKorrel8rQuery(t, rule.Result.Query, loadKorrel8rFixture(t, "testdata/korrel8r/llminferenceservice.json"))
	const want = `k8s:Pod:{"namespace":"inference","labels":{"app.kubernetes.io/name":"llama","app.kubernetes.io/part-of":"llminferenceservice"}}`
	if got != want {
		t.Fatalf("neighboring LLMInferenceService rule must remain valid:\nwant: %s\n got: %s", want, got)
	}
}

func loadRHOAIKorrel8rRules(t *testing.T) []korrel8rRule {
	t.Helper()

	configTemplate, err := resourcesFS.ReadFile(Korrel8rConfigTemplate)
	if err != nil {
		t.Fatalf("reading Korrel8r ConfigMap template: %v", err)
	}

	rendered, err := template.Must(template.New("korrel8r-config").Parse(string(configTemplate))).Clone()
	if err != nil {
		t.Fatalf("parsing Korrel8r ConfigMap template: %v", err)
	}
	var out bytes.Buffer
	if err := rendered.Execute(&out, map[string]any{
		"Namespace":              "monitoring",
		"Korrel8rServiceName":    Korrel8rServiceName,
		"Korrel8rMetricsStore":   true,
		"Korrel8rTracesStore":    true,
		"Korrel8rLokiStore":      true,
		"Metrics":                true,
		"MetricsStorage":         true,
		"TempoStorage":           true,
		"Logs":                   true,
		"ThanosQuerierEndpoint":  "https://thanos.example.test",
		"TempoQueryEndpoint":     "https://tempo.example.test",
		"LokiQueryEndpoint":      "https://loki.example.test",
		"Korrel8rRequestTimeout": "30s",
		"Korrel8rSessionTimeout": "5m",
	}); err != nil {
		t.Fatalf("rendering Korrel8r ConfigMap template: %v", err)
	}

	var configMap struct {
		Data map[string]string `yaml:"data"`
	}
	if err := yaml.Unmarshal(out.Bytes(), &configMap); err != nil {
		t.Fatalf("parsing rendered Korrel8r ConfigMap: %v", err)
	}
	rawRules, ok := configMap.Data["rhai-inference-rules.yaml"]
	if !ok {
		t.Fatal("rendered Korrel8r ConfigMap does not ship RHOAI inference rules")
	}
	if !strings.Contains(configMap.Data["korrel8r.yaml"], "/etc/korrel8r/custom/rhai-inference-rules.yaml") {
		t.Fatal("rendered Korrel8r configuration does not include the shipped RHOAI inference rules")
	}

	var rulesFile korrel8rRulesFile
	if err := yaml.Unmarshal([]byte(rawRules), &rulesFile); err != nil {
		t.Fatalf("parsing shipped RHOAI Korrel8r rules: %v", err)
	}
	if len(rulesFile.Rules) == 0 {
		t.Fatal("shipped RHOAI Korrel8r rules are empty")
	}
	return rulesFile.Rules
}

func findKorrel8rRule(t *testing.T, rules []korrel8rRule, name string) korrel8rRule {
	t.Helper()
	for _, rule := range rules {
		if rule.Name == name {
			return rule
		}
	}
	t.Fatalf("Korrel8r rule %q was not shipped", name)
	return korrel8rRule{}
}

func loadKorrel8rFixture(t *testing.T, path string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading fixture %q: %v", path, err)
	}
	var fixture map[string]any
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("parsing fixture %q: %v", path, err)
	}
	return fixture
}

func renderKorrel8rQuery(t *testing.T, rawTemplate string, data map[string]any) string {
	t.Helper()
	ruleTemplate, err := template.New("rule").Option("missingkey=error").Parse(rawTemplate)
	if err != nil {
		t.Fatalf("parsing Korrel8r rule template: %v", err)
	}
	var out bytes.Buffer
	if err := ruleTemplate.Execute(&out, data); err != nil {
		t.Fatalf("rendering Korrel8r rule template: %v", err)
	}
	return strings.TrimSpace(out.String())
}

func korrel8rFixtureString(t *testing.T, fixture map[string]any, path ...string) string {
	t.Helper()
	value := korrel8rFixtureValue(t, fixture, path...)
	stringValue, ok := value.(string)
	if !ok {
		t.Fatalf("fixture value at %s must be a string, got %T", strings.Join(path, "."), value)
	}
	return stringValue
}

func korrel8rFixtureMap(t *testing.T, fixture map[string]any, path ...string) map[string]any {
	t.Helper()
	value := korrel8rFixtureValue(t, fixture, path...)
	mapValue, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("fixture value at %s must be an object, got %T", strings.Join(path, "."), value)
	}
	return mapValue
}

func korrel8rFixtureValue(t *testing.T, fixture map[string]any, path ...string) any {
	t.Helper()
	var value any = fixture
	for _, key := range path {
		if index, ok := parseKorrel8rFixtureIndex(key); ok {
			values, isSlice := value.([]any)
			if !isSlice || index >= len(values) {
				t.Fatalf("fixture value at %s must contain index %d", strings.Join(path, "."), index)
			}
			value = values[index]
			continue
		}
		values, isMap := value.(map[string]any)
		if !isMap {
			t.Fatalf("fixture value at %s must be an object before key %q, got %T", strings.Join(path, "."), key, value)
		}
		var found bool
		value, found = values[key]
		if !found {
			t.Fatalf("fixture is missing required value at %s", strings.Join(path, "."))
		}
	}
	return value
}

func parseKorrel8rFixtureIndex(value string) (int, bool) {
	index, err := strconv.Atoi(value)
	if err != nil || index < 0 {
		return 0, false
	}
	return index, true
}

func TestKorrel8rRHOAIInferenceRulesUseOnlyApprovedTransitions(t *testing.T) {
	rules := loadRHOAIKorrel8rRules(t)
	wantRules := map[string]korrel8rRuleTransition{
		"LLMInferenceServiceToServingPods": {
			startClass: "LLMInferenceService.v1alpha2.serving.kserve.io",
			goalClass:  "Pod",
		},
		"HTTPRouteToBackendService": {
			startClass: "HTTPRoute.v1.gateway.networking.k8s.io",
			goalClass:  "Service",
		},
		"HTTPRouteToInferencePool": {
			startClass: "HTTPRoute.v1.gateway.networking.k8s.io",
			goalClass:  "InferencePool.v1.inference.networking.k8s.io",
		},
		"InferencePoolToServingPods": {
			startClass: "InferencePool.v1.inference.networking.k8s.io",
			goalClass:  "Pod",
		},
	}
	for _, rule := range rules {
		want, ok := wantRules[rule.Name]
		if !ok {
			t.Fatalf("unexpected RHOAI-specific Korrel8r rule %q; use the built-in rules where they already cover the relationship", rule.Name)
		}
		delete(wantRules, rule.Name)
		assertKorrel8rRuleTransition(t, rule, want)
		assertKorrel8rRuleDoesNotUsePlatformBackend(t, rule)
	}
	for name := range wantRules {
		t.Fatalf("expected RHOAI-specific Korrel8r rule %q was not shipped", name)
	}
}

type korrel8rRuleTransition struct {
	startClass string
	goalClass  string
}

func assertKorrel8rRuleTransition(t *testing.T, rule korrel8rRule, want korrel8rRuleTransition) {
	t.Helper()
	if rule.Start.Domain != "k8s" || len(rule.Start.Classes) != 1 || rule.Start.Classes[0] != want.startClass ||
		rule.Goal.Domain != "k8s" || len(rule.Goal.Classes) != 1 || rule.Goal.Classes[0] != want.goalClass {
		t.Fatalf("rule %q must be %s -> %s in the k8s domain, got %#v", rule.Name, want.startClass, want.goalClass, rule)
	}
}

func assertKorrel8rRuleDoesNotUsePlatformBackend(t *testing.T, rule korrel8rRule) {
	t.Helper()
	if strings.Contains(rule.Result.Query, "cluster-prometheus") || strings.Contains(rule.Result.Query, "cluster-loki") || strings.Contains(rule.Result.Query, "platform-tempo") {
		t.Fatalf("rule %q points to a platform observability backend", rule.Name)
	}
}
