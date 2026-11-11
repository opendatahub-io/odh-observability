package controller

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Run the actual rendered federation selectors against representative series,
// including separate quotas and labels which must not match.
func TestModelResourceFederationSelectorsPromQL(t *testing.T) {
	promtool, err := exec.LookPath("promtool")
	if err != nil {
		t.Skip("promtool is not installed")
	}
	collector := renderCollectorTemplate(t)
	job := renderedPrometheusScrapeJobsForReceiver(t, collector, "prometheus/cluster_resources")["cluster-prometheus-model-resources"]
	matchers, found, err := unstructured.NestedStringSlice(job, "params", "match[]")
	if err != nil || !found || len(matchers) != 3 {
		t.Fatalf("expected three federation matchers: found=%t matchers=%v error=%v", found, matchers, err)
	}
	testData, err := os.ReadFile("testdata/model-resource-federation.test.yaml")
	if err != nil {
		t.Fatal(err)
	}
	replacer := strings.NewReplacer(
		"@@CPU_USAGE_MATCHER@@", matchers[0],
		"@@QUOTA_MATCHER@@", matchers[1],
		"@@CONTAINER_LIMIT_MATCHER@@", matchers[2],
	)
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(replacer.Replace(string(testData))), 0o600); err != nil { // #nosec G703 -- path is under t.TempDir.
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), promtool, "test", "rules", path)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("promtool test federation selectors: %v\n%s", err, output)
	}
}
