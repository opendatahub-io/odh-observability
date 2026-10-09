package e2e_test

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestIsUnsupportedPersesTLSArg(t *testing.T) {
	tests := []struct {
		arg  string
		want bool
	}{
		{arg: "--web.tls.cert=/tmp/tls.crt", want: true},
		{arg: "--web.enable-tls", want: true},
		{arg: "--web.tls-min-version=1.2", want: false},
		{arg: "--web.tls-cipher-suites=TLS_AES_128_GCM_SHA256", want: false},
		{arg: "--config=/etc/perses/config.yaml", want: false},
		{arg: "--log.level=info", want: false},
	}
	for _, tt := range tests {
		if got := isUnsupportedPersesTLSArg(tt.arg); got != tt.want {
			t.Errorf("isUnsupportedPersesTLSArg(%q) = %v, want %v", tt.arg, got, tt.want)
		}
	}
}

func TestLooksLikeHTMLResponse(t *testing.T) {
	if !looksLikeHTMLResponse("<html><body>login</body></html>") {
		t.Fatal("expected HTML detection")
	}
	if looksLikeHTMLResponse(`{"status":"ok"}`) {
		t.Fatal("expected JSON to not be classified as HTML")
	}
}

func TestCOOCSVMatchesPrefix(t *testing.T) {
	tests := []struct {
		csv    string
		prefix string
		want   bool
	}{
		{csv: "cluster-observability-operator.v1.5.0", prefix: "cluster-observability-operator.v1.5", want: true},
		{csv: "cluster-observability-operator.v1.50.0", prefix: "cluster-observability-operator.v1.5", want: false},
		{csv: "cluster-observability-operator.v1.4.0", prefix: "cluster-observability-operator.v1.5", want: false},
	}
	for _, tt := range tests {
		if got := cooCSVMatchesPrefix(tt.csv, tt.prefix); got != tt.want {
			t.Errorf("cooCSVMatchesPrefix(%q, %q) = %v, want %v", tt.csv, tt.prefix, got, tt.want)
		}
	}
}

func TestSanitizeContainerArg(t *testing.T) {
	if got := sanitizeContainerArg("--token=abc123"); got != "--token=<redacted>" {
		t.Fatalf("sanitizeContainerArg token: got %q", got)
	}
	if got := sanitizeContainerArg("--config=/etc/perses/config.yaml"); got != "--config=<value>" {
		t.Fatalf("sanitizeContainerArg config: got %q", got)
	}
}

func TestSanitizeContainerArgsSeparateValue(t *testing.T) {
	got := sanitizeContainerArgs([]string{"--token", "super-secret", "--log.level", "info"})
	want := []string{"--token", "<redacted>", "--log.level", "info"}
	if len(got) != len(want) {
		t.Fatalf("sanitizeContainerArgs len: got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sanitizeContainerArgs[%d]: got %q want %q (full %v)", i, got[i], want[i], got)
		}
	}
}

func TestValidateKubernetesProxyPath(t *testing.T) {
	if err := validateKubernetesProxyPath("/api/v1/health"); err != nil {
		t.Fatalf("expected valid path: %v", err)
	}
	if err := validateKubernetesProxyPath("/../api/v1/health"); err == nil {
		t.Fatal("expected path traversal to be rejected")
	}
}

func TestContainsPersesStartupFailure(t *testing.T) {
	if !containsPersesStartupFailure("error: unknown flag: --web.tls") {
		t.Fatal("expected unknown flag detection")
	}
	if containsPersesStartupFailure("perses started successfully") {
		t.Fatal("expected healthy log to pass")
	}
}

func TestValidatePersesHealthResponse(t *testing.T) {
	valid := `{"buildTime":"2024-01-01T00:00:00Z","version":"0.44.0","commit":"abc","database":true}`
	if err := validatePersesHealthResponse(valid); err != nil {
		t.Fatalf("expected valid response: %v", err)
	}
	if err := validatePersesHealthResponse(""); err == nil {
		t.Fatal("expected empty body to fail")
	}
	if err := validatePersesHealthResponse(`{"buildTime":"x","commit":"y","database":false}`); err == nil {
		t.Fatal("expected missing version to fail")
	}
	if err := validatePersesHealthResponse(`{"database":true}`); err != nil {
		t.Fatalf("expected database=true to pass: %v", err)
	}
	if err := validatePersesHealthResponse("ok"); err == nil {
		t.Fatal("expected plain text to fail")
	}
}

func TestPersesContainerFromPodSpec(t *testing.T) {
	containers := []any{
		map[string]any{"name": "oauth-proxy", "image": "registry/oauth-proxy:latest"},
		map[string]any{"name": "perses", "image": "quay.io/perses/perses:v0.44.0", "args": []any{"--config=/etc/perses/config.yaml"}},
	}
	name, image, args, found := persesContainerFromPodSpec(containers)
	if !found || name != "perses" || !strings.Contains(image, "perses") || len(args) != 1 {
		t.Fatalf("unexpected result: found=%v name=%q image=%q args=%v", found, name, image, args)
	}
}

func TestValidatePrometheusInstantQueryResponse(t *testing.T) {
	valid := `{"status":"success","data":{"resultType":"vector","result":[]}}`
	if err := validatePrometheusInstantQueryResponse(valid); err != nil {
		t.Fatalf("expected valid query response: %v", err)
	}
	if err := validatePrometheusInstantQueryResponse(`{"status":"error","error":"bad"}`); err == nil {
		t.Fatal("expected error status to fail")
	}
}

func TestValidatePersesProjectsResponse(t *testing.T) {
	if err := validatePersesProjectsResponse(`[]`); err != nil {
		t.Fatalf("expected empty project list: %v", err)
	}
	if err := validatePersesProjectsResponse(`{"kind":"ProjectList","items":[]}`); err != nil {
		t.Fatalf("expected project list object: %v", err)
	}
	if err := validatePersesProjectsResponse("not-json"); err == nil {
		t.Fatal("expected invalid JSON to fail")
	}
}

func TestKubernetesServiceProxyURLWithQuery(t *testing.T) {
	got, err := kubernetesServiceProxyURL(
		"https://api.example.com:6443",
		"redhat-ods-monitoring",
		"data-science-prometheus-namespace-proxy",
		8443,
		"/api/v1/query?query=up",
		true,
	)
	if err != nil {
		t.Fatalf("kubernetesServiceProxyURL: %v", err)
	}
	if !strings.Contains(got, "query=up") {
		t.Fatalf("expected query string in proxy URL, got %q", got)
	}
}

func TestValidatePrometheusBuildInfoResponse(t *testing.T) {
	if err := validatePrometheusBuildInfoResponse(`{"status":"success","data":{"version":"2.45.0"}}`); err != nil {
		t.Fatalf("expected valid response: %v", err)
	}
	if err := validatePrometheusBuildInfoResponse(`{"status":"success","data":{}}`); err == nil {
		t.Fatal("expected empty version to fail")
	}
	if err := validatePrometheusBuildInfoResponse(`this payload mentions version but is not JSON`); err == nil {
		t.Fatal("expected non-JSON to fail")
	}
}

func TestRunningOpenShiftVersion(t *testing.T) {
	cv := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{
			"desired": map[string]any{"version": "4.16.99"},
			"history": []any{
				map[string]any{"state": "Completed", "version": "4.16.10"},
			},
		},
	}}
	version, err := runningOpenShiftVersion(cv)
	if err != nil || version != "4.16.10" {
		t.Fatalf("runningOpenShiftVersion: got %q err %v", version, err)
	}
}

func TestValidateForbiddenCompatibilityGateOverrides(t *testing.T) {
	const env = "E2E_TEST_EXPECTED_COO_VERSION_PREFIX"
	t.Setenv(env, "cluster-observability-operator.v9.9")
	err := validateForbiddenCompatibilityGateOverrides()
	if err == nil {
		t.Fatal("expected override env to be rejected")
	}
	if !strings.Contains(err.Error(), env) {
		t.Fatalf("expected error to mention %s, got %v", env, err)
	}
	t.Setenv(env, "")
	if err := validateForbiddenCompatibilityGateOverrides(); err != nil {
		t.Fatalf("expected clean env: %v", err)
	}
}

func TestTestContextConfigValidateRejectsCompatibilityGateOverrides(t *testing.T) {
	t.Setenv("E2E_TEST_SKIP_DASHBOARD_PROBE", "true")
	cfg := TestContextConfig{apiMode: APIModeDSC}
	if err := cfg.validate(); err == nil {
		t.Fatal("expected validate to reject dashboard probe skip env")
	}
	t.Setenv("E2E_TEST_SKIP_DASHBOARD_PROBE", "")
}

func TestReleaseGateCOOCSVVersions(t *testing.T) {
	for _, csv := range []string{
		"cluster-observability-operator.v1.5.0",
		"cluster-observability-operator.v1.5.3",
	} {
		if !releaseGateCOOCSVMatches(csv) {
			t.Fatalf("expected %q to match COO 1.5 release gate", csv)
		}
	}
	for _, csv := range []string{
		"cluster-observability-operator.v1.6.0",
		"cluster-observability-operator.v1.50.0",
		"cluster-observability-operator.v1.4.9",
	} {
		if releaseGateCOOCSVMatches(csv) {
			t.Fatalf("expected %q to be rejected by COO 1.5 release gate", csv)
		}
	}
}

func TestReleaseGateRHOAICSVVersions(t *testing.T) {
	for _, csv := range []string{
		"rhods-operator.3.6.0",
		"rhods-operator.3.6.1",
	} {
		if !releaseGateRHOAICSVMatches(csv) {
			t.Fatalf("expected %q to match RHOAI 3.6 release gate", csv)
		}
	}
	for _, csv := range []string{
		"rhods-operator.3.7.0",
		"rhods-operator.3.60.0",
		"rhods-operator.3.5.9",
	} {
		if releaseGateRHOAICSVMatches(csv) {
			t.Fatalf("expected %q to be rejected by RHOAI 3.6 release gate", csv)
		}
	}
}

func TestReleaseGatePersesImage(t *testing.T) {
	if err := persesImageMatchesReleaseGate(releaseGateContract.PersesImage); err != nil {
		t.Fatalf("expected exact reference to match: %v", err)
	}
	if err := persesImageMatchesReleaseGate("quay.io/perses/perses:latest"); err == nil {
		t.Fatal("expected unrelated image to fail")
	}
	digest := strings.Split(releaseGateContract.PersesImage, "@sha256:")[1]
	if digest == "" {
		t.Fatal("expected digest in release gate Perses image")
	}
	spoof := "evil.example/fake/perses-rhel9@sha256:" + digest
	if err := persesImageMatchesReleaseGate(spoof); err == nil {
		t.Fatal("expected different repository with same digest to fail")
	}
	if err := persesImageMatchesReleaseGate("not-an-image-ref"); err == nil {
		t.Fatal("expected malformed image reference to fail")
	}
}

func TestSanitizeLogExcerpt(t *testing.T) {
	got := sanitizeLogExcerpt("Authorization: Bearer super-secret-token api_key=abc123 password=sekret https://user:pass@example.com/path?token=xyz")
	leaks := []string{"super-secret", "abc123", "sekret", "user:pass", "xyz"}
	for _, secret := range leaks {
		if strings.Contains(got, secret) {
			t.Fatalf("sanitizeLogExcerpt leaked %q in %q", secret, got)
		}
	}
	if !strings.Contains(got, "<redacted>") {
		t.Fatalf("expected redaction markers in %q", got)
	}
}

func TestCOOCompatibilityGateIncludesDashboardProbe(t *testing.T) {
	if !cooCompatibilityGateIncludesDashboardProbe() {
		t.Fatal("release gate must include dashboard proxy API validation")
	}
	if len(cooCompatibilityGateSubtests) < 4 {
		t.Fatalf("expected full gate subtest list, got %v", cooCompatibilityGateSubtests)
	}
}

func TestReleaseGateContractShape(t *testing.T) {
	if releaseGateContract.COOCSVPrefix == "" || releaseGateContract.RHOAICSVPrefix == "" || releaseGateContract.PersesImage == "" {
		t.Fatalf("release gate contract must be fully specified: %+v", releaseGateContract)
	}
	if !strings.Contains(releaseGateContract.PersesImage, "perses-rhel9@sha256:") {
		t.Fatalf("unexpected Perses image reference: %q", releaseGateContract.PersesImage)
	}
}

func TestValidateClusterVersionReady(t *testing.T) {
	ready := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{
			"conditions": []any{
				map[string]any{"type": "Available", "status": "True"},
				map[string]any{"type": "Progressing", "status": "False"},
				map[string]any{"type": "Degraded", "status": "False"},
			},
		},
	}}
	if err := validateClusterVersionReady(ready); err != nil {
		t.Fatalf("expected ready cluster: %v", err)
	}
	progressing := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{
			"conditions": []any{
				map[string]any{"type": "Available", "status": "True"},
				map[string]any{"type": "Progressing", "status": "True"},
			},
		},
	}}
	if err := validateClusterVersionReady(progressing); err == nil {
		t.Fatal("expected progressing cluster to fail")
	}
}
