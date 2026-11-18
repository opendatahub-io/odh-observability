package controller

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

func TestAcceleratorAttributionRulesPromQL(t *testing.T) {
	promtool, err := exec.LookPath("promtool")
	if err != nil {
		t.Skip("promtool is not installed")
	}
	resources := renderObservabilityTemplate(t, AcceleratorAttributionRulesTemplate, map[string]any{
		"AcceleratorMetrics":       true,
		"BuiltinDCGMScrapeEnabled": true,
		"ClusterName":              "",
		"Namespace":                testMonitoringNamespace,
	})
	if len(resources) != 1 {
		t.Fatalf("expected one recording rule resource, got %d", len(resources))
	}
	groups, found, err := unstructured.NestedSlice(resources[0].Object, "spec", "groups")
	if err != nil || !found {
		t.Fatalf("recording rule groups: found=%t error=%v", found, err)
	}
	rules, err := yaml.Marshal(map[string]any{"groups": groups})
	if err != nil {
		t.Fatal(err)
	}
	testData, err := os.ReadFile("testdata/accelerator-attribution-rules.test.yaml")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for name, data := range map[string][]byte{"rules.yaml": rules, "test.yaml": testData} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.CommandContext(t.Context(), promtool, "test", "rules", filepath.Join(dir, "test.yaml"))
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("promtool test rules: %v\n%s", err, output)
	}
}
