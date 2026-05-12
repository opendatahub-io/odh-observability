/*
Copyright 2025.

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
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
)

// --- checkMonitoringPreconditions ---

func registerOperatorCondition(s *kruntime.Scheme) {
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "operators.coreos.com", Version: "v2", Kind: "OperatorCondition",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "operators.coreos.com", Version: "v2", Kind: "OperatorConditionList",
	}, &unstructured.UnstructuredList{})
}

func newOperatorCondition(name string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "operators.coreos.com", Version: "v2", Kind: "OperatorCondition",
	})
	obj.SetName(name)
	return obj
}

func TestCheckPreconditions_NoFeaturesConfigured(t *testing.T) {
	s := newTestScheme(t)
	registerOperatorCondition(s)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	err := checkMonitoringPreconditions(context.Background(), cli, m)
	if err != nil {
		t.Fatalf("expected no error when nothing is configured, got: %v", err)
	}
}

func TestCheckPreconditions_MetricsRequiresOTelAndCOO(t *testing.T) {
	s := newTestScheme(t)
	registerOperatorCondition(s)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	err := checkMonitoringPreconditions(context.Background(), cli, m)
	if err == nil {
		t.Fatal("expected error when operators are missing")
	}

	errStr := err.Error()
	if !strings.Contains(errStr, "OpenTelemetryCollector") {
		t.Errorf("expected OpenTelemetry error, got: %s", errStr)
	}
	if !strings.Contains(errStr, "ClusterObservability") {
		t.Errorf("expected COO error, got: %s", errStr)
	}
}

func TestCheckPreconditions_TracesRequiresOTelAndTempo(t *testing.T) {
	s := newTestScheme(t)
	registerOperatorCondition(s)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Traces = &v1alpha1.Traces{
		Storage: v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV},
	}

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	err := checkMonitoringPreconditions(context.Background(), cli, m)
	if err == nil {
		t.Fatal("expected error when operators are missing")
	}

	errStr := err.Error()
	if !strings.Contains(errStr, "OpenTelemetryCollector") {
		t.Errorf("expected OpenTelemetry error, got: %s", errStr)
	}
	if !strings.Contains(errStr, "Tempo") {
		t.Errorf("expected Tempo error, got: %s", errStr)
	}
}

func TestCheckPreconditions_AllOperatorsPresent(t *testing.T) {
	s := newTestScheme(t)
	registerOperatorCondition(s)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}
	m.Spec.Traces = &v1alpha1.Traces{
		Storage: v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV},
	}

	otel := newOperatorCondition("opentelemetry-operator.v0.100.0")
	coo := newOperatorCondition("cluster-observability-operator.v1.0.0")
	tempo := newOperatorCondition("tempo-operator.v2.0.0")

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(otel, coo, tempo).Build()
	err := checkMonitoringPreconditions(context.Background(), cli, m)
	if err != nil {
		t.Fatalf("expected no error when all operators present, got: %v", err)
	}
}

// --- operatorExists ---

func TestOperatorExists_ExactMatch(t *testing.T) {
	s := newTestScheme(t)
	registerOperatorCondition(s)

	op := newOperatorCondition("opentelemetry-operator")
	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(op).Build()

	info, err := operatorExists(context.Background(), cli, "opentelemetry-operator")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info == nil {
		t.Error("expected non-nil for exact match")
	}
}

func TestOperatorExists_DotSeparatedMatch(t *testing.T) {
	s := newTestScheme(t)
	registerOperatorCondition(s)

	op := newOperatorCondition("opentelemetry-operator.v0.100.0")
	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(op).Build()

	info, err := operatorExists(context.Background(), cli, "opentelemetry-operator")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info == nil {
		t.Error("expected non-nil for dot-separated match")
	}
}

func TestOperatorExists_NoMatch(t *testing.T) {
	s := newTestScheme(t)
	registerOperatorCondition(s)

	op := newOperatorCondition("some-other-operator.v1.0.0")
	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(op).Build()

	info, err := operatorExists(context.Background(), cli, "opentelemetry-operator")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info != nil {
		t.Error("expected nil when operator is not found")
	}
}

func TestOperatorExists_PrefixCollisionPrevented(t *testing.T) {
	s := newTestScheme(t)
	registerOperatorCondition(s)

	op := newOperatorCondition("opentelemetry-operator-extra")
	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(op).Build()

	info, err := operatorExists(context.Background(), cli, "opentelemetry-operator")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info != nil {
		t.Error("should not match 'opentelemetry-operator-extra' for prefix 'opentelemetry-operator'")
	}
}

// --- addStorageData ---

func TestAddStorageData_WithStorage(t *testing.T) {
	metrics := &v1alpha1.Metrics{
		Storage: &v1alpha1.MetricsStorage{
			Size:      resource.MustParse("20Gi"),
			Retention: "180d",
		},
	}
	data := make(map[string]any)
	addStorageData(metrics, data)

	if data["StorageSize"] != "20Gi" {
		t.Errorf("StorageSize: want 20Gi, got %v", data["StorageSize"])
	}
	if data["StorageRetention"] != "180d" {
		t.Errorf("StorageRetention: want 180d, got %v", data["StorageRetention"])
	}
}

func TestAddStorageData_WithoutStorage(t *testing.T) {
	metrics := &v1alpha1.Metrics{}
	data := make(map[string]any)
	addStorageData(metrics, data)

	if data["StorageSize"] != defaultStorageSize {
		t.Errorf("StorageSize: want %q, got %v", defaultStorageSize, data["StorageSize"])
	}
	if data["StorageRetention"] != defaultRetention {
		t.Errorf("StorageRetention: want %q, got %v", defaultRetention, data["StorageRetention"])
	}
}

func TestAddStorageData_EmptyRetention(t *testing.T) {
	metrics := &v1alpha1.Metrics{
		Storage: &v1alpha1.MetricsStorage{
			Size: resource.MustParse("10Gi"),
		},
	}
	data := make(map[string]any)
	addStorageData(metrics, data)

	if data["StorageRetention"] != defaultRetention {
		t.Errorf("StorageRetention: want default %q, got %v", defaultRetention, data["StorageRetention"])
	}
}

// --- addReplicasData ---

func TestAddReplicasData_ExplicitWithStorage(t *testing.T) {
	metrics := &v1alpha1.Metrics{
		Storage:  &v1alpha1.MetricsStorage{Size: resource.MustParse("5Gi")},
		Replicas: 3,
	}
	data := make(map[string]any)
	addReplicasData(metrics, false, data)

	if data["Replicas"] != "3" {
		t.Errorf("Replicas: want '3', got %v", data["Replicas"])
	}
}

func TestAddReplicasData_DefaultMultiNode(t *testing.T) {
	metrics := &v1alpha1.Metrics{
		Storage: &v1alpha1.MetricsStorage{Size: resource.MustParse("5Gi")},
	}
	data := make(map[string]any)
	addReplicasData(metrics, false, data)

	if data["Replicas"] != "2" {
		t.Errorf("Replicas: want '2' for multi-node, got %v", data["Replicas"])
	}
}

func TestAddReplicasData_DefaultSNO(t *testing.T) {
	metrics := &v1alpha1.Metrics{
		Storage: &v1alpha1.MetricsStorage{Size: resource.MustParse("5Gi")},
	}
	data := make(map[string]any)
	addReplicasData(metrics, true, data)

	if data["Replicas"] != "1" {
		t.Errorf("Replicas: want '1' for SNO, got %v", data["Replicas"])
	}
}

func TestAddReplicasData_NoStorage(t *testing.T) {
	metrics := &v1alpha1.Metrics{}
	data := make(map[string]any)
	addReplicasData(metrics, false, data)

	if data["Replicas"] != "1" {
		t.Errorf("Replicas: want '1' fallback without storage, got %v", data["Replicas"])
	}
}

// --- addTracesTemplateData ---

func TestAddTracesTemplateData_PVBackend(t *testing.T) {
	traces := &v1alpha1.Traces{
		Storage: v1alpha1.TracesStorage{
			Backend: v1alpha1.StorageBackendPV,
			Size:    "10Gi",
		},
		SampleRatio: "0.5",
	}
	data := make(map[string]any)
	err := addTracesTemplateData(data, traces, "test-ns")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if data["Backend"] != v1alpha1.StorageBackendPV {
		t.Errorf("Backend: want %q, got %v", v1alpha1.StorageBackendPV, data["Backend"])
	}
	if data["SampleRatio"] != "0.5" {
		t.Errorf("SampleRatio: want '0.5', got %v", data["SampleRatio"])
	}

	endpoint, ok := data["TempoEndpoint"].(string)
	if !ok || !strings.Contains(endpoint, "tempomonolithic") {
		t.Errorf("TempoEndpoint should reference tempomonolithic for PV backend, got: %v", data["TempoEndpoint"])
	}
	if data["Size"] != "10Gi" {
		t.Errorf("Size: want '10Gi', got %v", data["Size"])
	}
}

func TestAddTracesTemplateData_S3Backend(t *testing.T) {
	traces := &v1alpha1.Traces{
		Storage: v1alpha1.TracesStorage{
			Backend: v1alpha1.StorageBackendS3,
			Secret:  "my-s3-secret",
		},
	}
	data := make(map[string]any)
	err := addTracesTemplateData(data, traces, "test-ns")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	endpoint, ok := data["TempoEndpoint"].(string)
	if !ok || !strings.Contains(endpoint, "tempostack") {
		t.Errorf("TempoEndpoint should reference tempostack for S3 backend, got: %v", data["TempoEndpoint"])
	}
	if data["Secret"] != "my-s3-secret" {
		t.Errorf("Secret: want 'my-s3-secret', got %v", data["Secret"])
	}
}

func TestAddTracesTemplateData_GCSBackend(t *testing.T) {
	traces := &v1alpha1.Traces{
		Storage: v1alpha1.TracesStorage{
			Backend: v1alpha1.StorageBackendGCS,
			Secret:  "my-gcs-secret",
		},
	}
	data := make(map[string]any)
	err := addTracesTemplateData(data, traces, "my-ns")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	endpoint, ok := data["TempoEndpoint"].(string)
	if !ok || !strings.Contains(endpoint, "tempostack") {
		t.Errorf("TempoEndpoint should reference tempostack for GCS, got: %v", data["TempoEndpoint"])
	}
	if data["Secret"] != "my-gcs-secret" {
		t.Errorf("Secret: want 'my-gcs-secret', got %v", data["Secret"])
	}
}

func TestAddTracesTemplateData_DefaultSampleRatio(t *testing.T) {
	traces := &v1alpha1.Traces{
		Storage: v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV},
	}
	data := make(map[string]any)
	err := addTracesTemplateData(data, traces, "ns")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if data["SampleRatio"] != defaultTracesSampleRatio {
		t.Errorf("SampleRatio: want default %q, got %v", defaultTracesSampleRatio, data["SampleRatio"])
	}
}

func TestAddTracesTemplateData_DefaultRetention(t *testing.T) {
	traces := &v1alpha1.Traces{
		Storage: v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV},
	}
	data := make(map[string]any)
	err := addTracesTemplateData(data, traces, "ns")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if data["TracesRetention"] != defaultTracesRetention {
		t.Errorf("TracesRetention: want default %q, got %v", defaultTracesRetention, data["TracesRetention"])
	}
}

func TestAddTracesTemplateData_CustomRetention(t *testing.T) {
	traces := &v1alpha1.Traces{
		Storage: v1alpha1.TracesStorage{
			Backend:   v1alpha1.StorageBackendPV,
			Retention: metav1.Duration{Duration: 48 * time.Hour},
		},
	}
	data := make(map[string]any)
	err := addTracesTemplateData(data, traces, "ns")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if data["TracesRetention"] != "48h0m0s" {
		t.Errorf("TracesRetention: want '48h0m0s', got %v", data["TracesRetention"])
	}
}

func TestAddTracesTemplateData_TLSEnabled(t *testing.T) {
	traces := &v1alpha1.Traces{
		Storage: v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV},
		TLS: &v1alpha1.TracesTLS{
			Enabled:           true,
			CertificateSecret: "tempo-cert",
			CAConfigMap:       "tempo-ca",
		},
	}
	data := make(map[string]any)
	err := addTracesTemplateData(data, traces, "ns")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if data["TempoTLSEnabled"] != true {
		t.Error("TempoTLSEnabled: want true")
	}
	if data["TempoCertificateSecret"] != "tempo-cert" {
		t.Errorf("TempoCertificateSecret: want 'tempo-cert', got %v", data["TempoCertificateSecret"])
	}
	if data["TempoCAConfigMap"] != "tempo-ca" {
		t.Errorf("TempoCAConfigMap: want 'tempo-ca', got %v", data["TempoCAConfigMap"])
	}
}

func TestAddTracesTemplateData_TLSDisabled(t *testing.T) {
	traces := &v1alpha1.Traces{
		Storage: v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV},
	}
	data := make(map[string]any)
	err := addTracesTemplateData(data, traces, "ns")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if data["TempoTLSEnabled"] != false {
		t.Error("TempoTLSEnabled: want false")
	}
	if data["TempoCertificateSecret"] != "" {
		t.Errorf("TempoCertificateSecret: want empty, got %v", data["TempoCertificateSecret"])
	}
}

func TestAddTracesTemplateData_WithExporters(t *testing.T) {
	traces := &v1alpha1.Traces{
		Storage: v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV},
		Exporters: map[string]kruntime.RawExtension{
			"otlp/custom": {Raw: []byte(`endpoint: https://collector.example.com:4317`)},
		},
	}
	data := make(map[string]any)
	err := addTracesTemplateData(data, traces, "ns")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	exporters, ok := data["TracesExporters"].(map[string]string)
	if !ok {
		t.Fatalf("TracesExporters: expected map[string]string, got %T", data["TracesExporters"])
	}
	if _, found := exporters["otlp/custom"]; !found {
		t.Error("expected 'otlp/custom' in TracesExporters")
	}

	names, ok := data["TracesExporterNames"].([]string)
	if !ok {
		t.Fatalf("TracesExporterNames: expected []string, got %T", data["TracesExporterNames"])
	}
	if len(names) != 1 || names[0] != "otlp/custom" {
		t.Errorf("TracesExporterNames: want [otlp/custom], got %v", names)
	}
}

// --- addResourceData ---

func TestAddResourceData(t *testing.T) {
	data := make(map[string]any)
	addResourceData(data)

	expectedKeys := []string{
		"CPULimit", "MemoryLimit", "CPURequest", "MemoryRequest",
		"CollectorCPULimit", "CollectorMemoryLimit", "CollectorCPURequest", "CollectorMemoryRequest",
		"TempoCPULimit", "TempoMemoryLimit", "TempoCPURequest", "TempoMemoryRequest",
	}
	for _, key := range expectedKeys {
		if _, ok := data[key]; !ok {
			t.Errorf("addResourceData missing key %q", key)
		}
	}

	if data["CPULimit"] != defaultCPULimit {
		t.Errorf("CPULimit: want %q, got %v", defaultCPULimit, data["CPULimit"])
	}
}

// --- addImageURLs ---

func TestAddImageURLs_Defaults(t *testing.T) {
	os.Unsetenv("RELATED_IMAGE_ODH_KUBE_RBAC_PROXY_IMAGE")
	os.Unsetenv("RELATED_IMAGE_OSE_PROM_LABEL_PROXY_IMAGE")
	os.Unsetenv("RELATED_IMAGE_CLI_IMAGE")

	data := make(map[string]any)
	addImageURLs(data)

	if data["KubeRBACProxyImage"] == "" {
		t.Error("KubeRBACProxyImage should have a default")
	}
	if data["PromLabelProxyImage"] == "" {
		t.Error("PromLabelProxyImage should have a default")
	}
	if data["CLIImage"] == "" {
		t.Error("CLIImage should have a default")
	}
}

func TestAddImageURLs_OverriddenByEnv(t *testing.T) {
	t.Setenv("RELATED_IMAGE_ODH_KUBE_RBAC_PROXY_IMAGE", "custom-proxy:latest")
	t.Setenv("RELATED_IMAGE_OSE_PROM_LABEL_PROXY_IMAGE", "custom-prom-proxy:latest")
	t.Setenv("RELATED_IMAGE_CLI_IMAGE", "custom-cli:latest")

	data := make(map[string]any)
	addImageURLs(data)

	if data["KubeRBACProxyImage"] != "custom-proxy:latest" {
		t.Errorf("KubeRBACProxyImage: want custom-proxy:latest, got %v", data["KubeRBACProxyImage"])
	}
	if data["PromLabelProxyImage"] != "custom-prom-proxy:latest" {
		t.Errorf("PromLabelProxyImage: want custom-prom-proxy:latest, got %v", data["PromLabelProxyImage"])
	}
	if data["CLIImage"] != "custom-cli:latest" {
		t.Errorf("CLIImage: want custom-cli:latest, got %v", data["CLIImage"])
	}
}

// --- getEnvOrDefault / getPersesImage ---

func TestGetEnvOrDefault(t *testing.T) {
	t.Setenv("TEST_VAR_FOR_HELPER", "from-env")
	if got := getEnvOrDefault("TEST_VAR_FOR_HELPER", "fallback"); got != "from-env" {
		t.Errorf("want 'from-env', got %q", got)
	}

	os.Unsetenv("UNSET_VAR_FOR_HELPER")
	if got := getEnvOrDefault("UNSET_VAR_FOR_HELPER", "fallback"); got != "fallback" {
		t.Errorf("want 'fallback', got %q", got)
	}
}

func TestGetPersesImage_Default(t *testing.T) {
	os.Unsetenv("RELATED_IMAGE_PERSES_IMAGE")
	img := getPersesImage()
	if img == "" {
		t.Error("expected non-empty default Perses image")
	}
}

func TestGetPersesImage_Override(t *testing.T) {
	t.Setenv("RELATED_IMAGE_PERSES_IMAGE", "custom-perses:1.0")
	if got := getPersesImage(); got != "custom-perses:1.0" {
		t.Errorf("want 'custom-perses:1.0', got %q", got)
	}
}

// --- buildTemplateData ---

func TestBuildTemplateData_BasicNoFeatures(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	data, err := buildTemplateData(context.Background(), cli, m, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if data["Namespace"] != "test-ns" {
		t.Errorf("Namespace: want 'test-ns', got %v", data["Namespace"])
	}
	if data["Metrics"] != false {
		t.Errorf("Metrics: want false, got %v", data["Metrics"])
	}
	if data["Traces"] != false {
		t.Errorf("Traces: want false, got %v", data["Traces"])
	}
	if data["PersesAPIVersion"] != "v1alpha2" {
		t.Errorf("PersesAPIVersion: want 'v1alpha2', got %v", data["PersesAPIVersion"])
	}
}

func TestBuildTemplateData_WithMetrics(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{
		Storage: &v1alpha1.MetricsStorage{
			Size:      resource.MustParse("10Gi"),
			Retention: "30d",
		},
	}

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	data, err := buildTemplateData(context.Background(), cli, m, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if data["Metrics"] != true {
		t.Error("Metrics: want true")
	}
	if data["StorageSize"] != "10Gi" {
		t.Errorf("StorageSize: want '10Gi', got %v", data["StorageSize"])
	}
}

func TestBuildTemplateData_CollectorReplicasExplicit(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}
	m.Spec.CollectorReplicas = 5

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	data, err := buildTemplateData(context.Background(), cli, m, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if data["CollectorReplicas"] != int32(5) {
		t.Errorf("CollectorReplicas: want 5, got %v", data["CollectorReplicas"])
	}
}

func TestBuildTemplateData_CollectorReplicasDefaultMultiNode(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	data, err := buildTemplateData(context.Background(), cli, m, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	replicas, ok := data["CollectorReplicas"].(int32)
	if !ok {
		t.Fatalf("CollectorReplicas: expected int32, got %T", data["CollectorReplicas"])
	}
	if replicas < 1 {
		t.Errorf("CollectorReplicas: want >= 1, got %d", replicas)
	}
}

func TestBuildTemplateData_PersesAPIVersionPassThrough(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	data, err := buildTemplateData(context.Background(), cli, m, "v1alpha1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if data["PersesAPIVersion"] != "v1alpha1" {
		t.Errorf("PersesAPIVersion: want 'v1alpha1', got %v", data["PersesAPIVersion"])
	}
}

// --- ExporterSchema.Validate ---

func TestExporterSchemaValidate_ValidOTLPHTTP(t *testing.T) {
	config := map[string]any{
		"endpoint":    "https://collector.example.com:4318",
		"compression": "gzip",
	}
	err := validateExporterSchema("otlphttp/custom", config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExporterSchemaValidate_InvalidCompression(t *testing.T) {
	config := map[string]any{
		"endpoint":    "https://collector.example.com:4318",
		"compression": "lz4",
	}
	err := validateExporterSchema("otlphttp/custom", config)
	if err == nil {
		t.Fatal("expected error for invalid compression value")
	}
}

func TestExporterSchemaValidate_DebugVerbosity(t *testing.T) {
	config := map[string]any{
		"verbosity": "detailed",
	}
	err := validateExporterSchema("debug", config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExporterSchemaValidate_DebugInvalidVerbosity(t *testing.T) {
	config := map[string]any{
		"verbosity": "ultra",
	}
	err := validateExporterSchema("debug", config)
	if err == nil {
		t.Fatal("expected error for invalid debug verbosity")
	}
}

func TestExporterSchemaValidate_PrometheusRemoteWrite(t *testing.T) {
	config := map[string]any{
		"endpoint": "https://prometheus.example.com:9090/api/v1/write",
	}
	err := validateExporterSchema("prometheusremotewrite/custom", config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExporterSchemaValidate_UnknownTypePassesThrough(t *testing.T) {
	config := map[string]any{
		"any_field": "any_value",
	}
	err := validateExporterSchema("completely_custom", config)
	if err != nil {
		t.Fatalf("unknown exporter types should pass validation, got: %v", err)
	}
}
