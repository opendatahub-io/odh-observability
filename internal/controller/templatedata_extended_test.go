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
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

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

func registerClusterExtension(s *kruntime.Scheme) {
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "olm.operatorframework.io", Version: "v1", Kind: "ClusterExtension",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "olm.operatorframework.io", Version: "v1", Kind: "ClusterExtensionList",
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

func newInstalledClusterExtension(name, packageName string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "olm.operatorframework.io", Version: "v1", Kind: "ClusterExtension",
	})
	obj.SetName(name)
	obj.Object["spec"] = map[string]any{
		"source": map[string]any{
			"sourceType": "Catalog",
			"catalog": map[string]any{
				"packageName": packageName,
			},
		},
	}
	obj.Object["status"] = map[string]any{
		"conditions": []any{
			map[string]any{
				"type":   "Installed",
				"status": "True",
				"reason": "Succeeded",
			},
		},
		"install": map[string]any{
			"bundle": map[string]any{
				"version": "1.0.0",
			},
		},
	}
	return obj
}

func newPendingClusterExtension(name, packageName string) *unstructured.Unstructured {
	obj := newInstalledClusterExtension(name, packageName)
	obj.Object["status"] = map[string]any{
		"conditions": []any{
			map[string]any{
				"type":   "Installed",
				"status": "False",
				"reason": "Progressing",
			},
		},
	}
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
	if !strings.Contains(errStr, "Red Hat build of OpenTelemetry") {
		t.Errorf("expected OpenTelemetry error, got: %s", errStr)
	}
	if !strings.Contains(errStr, "Cluster Observability Operator") {
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
	if !strings.Contains(errStr, "Red Hat build of OpenTelemetry") {
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

func TestCheckPreconditions_OLMv1OperatorsPresent(t *testing.T) {
	s := newTestScheme(t)
	registerOperatorCondition(s)
	registerClusterExtension(s)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}
	m.Spec.Traces = &v1alpha1.Traces{
		Storage: v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV},
	}

	objects := []kruntime.Object{
		newInstalledClusterExtension("otel", opentelemetryOperator),
		newInstalledClusterExtension("coo", clusterObservabilityOperator),
		newInstalledClusterExtension("tempo", tempoOperator),
	}
	cli := fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(objects...).Build()

	if err := checkMonitoringPreconditions(context.Background(), cli, m); err != nil {
		t.Fatalf("expected no error for installed OLMv1 operators, got: %v", err)
	}
}

func TestCheckPreconditions_OLMv1PendingOperatorIsMissing(t *testing.T) {
	s := newTestScheme(t)
	registerOperatorCondition(s)
	registerClusterExtension(s)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}

	objects := []kruntime.Object{
		newInstalledClusterExtension("otel", opentelemetryOperator),
		newPendingClusterExtension("coo", clusterObservabilityOperator),
	}
	cli := fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(objects...).Build()

	err := checkMonitoringPreconditions(context.Background(), cli, m)
	if err == nil || !strings.Contains(err.Error(), "Cluster Observability Operator") {
		t.Fatalf("expected missing ClusterObservability operator error, got: %v", err)
	}
}

func TestCheckPreconditions_OLMAPIsUnavailableReturnsError(t *testing.T) {
	s := newTestScheme(t)
	registerOperatorCondition(s)
	registerClusterExtension(s)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}

	cli := fake.NewClientBuilder().WithScheme(s).WithInterceptorFuncs(interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			gvk := list.GetObjectKind().GroupVersionKind()
			switch gvk.Group {
			case "operators.coreos.com":
				return &meta.NoKindMatchError{
					GroupKind:        schema.GroupKind{Group: gvk.Group, Kind: "OperatorCondition"},
					SearchedVersions: []string{gvk.Version},
				}
			case "olm.operatorframework.io":
				return &meta.NoKindMatchError{
					GroupKind:        schema.GroupKind{Group: gvk.Group, Kind: "ClusterExtension"},
					SearchedVersions: []string{gvk.Version},
				}
			default:
				return c.List(ctx, list, opts...)
			}
		},
	}).Build()

	err := checkMonitoringPreconditions(context.Background(), cli, m)
	if err == nil || !meta.IsNoMatchError(err) {
		t.Fatalf("expected OLM API discovery error, got: %v", err)
	}
	var missingErr *missingOperatorsError
	if errors.As(err, &missingErr) {
		t.Fatalf("API discovery failure was reported as a missing operator: %v", err)
	}
}

func TestCheckPreconditions_UsageLogsRequiresOTelAndLoki(t *testing.T) {
	s := newTestScheme(t)
	registerOperatorCondition(s)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{Storage: &v1alpha1.LokiStorageConfig{}}

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	err := checkMonitoringPreconditions(context.Background(), cli, m)
	if err == nil {
		t.Fatal("expected error when operators are missing")
	}

	errStr := err.Error()
	for _, expected := range []string{"Red Hat build of OpenTelemetry", "Loki Operator", "OperatorHub"} {
		if !strings.Contains(errStr, expected) {
			t.Errorf("expected %q in error, got: %s", expected, errStr)
		}
	}
	if strings.Contains(errStr, "OpenShift Logging") {
		t.Errorf("usage logs should not require OpenShift Logging, got: %s", errStr)
	}
}

func TestCheckPreconditions_LogsRequiresLokiAndOpenShiftLogging(t *testing.T) {
	s := newTestScheme(t)
	registerOperatorCondition(s)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Logs = &v1alpha1.Logs{Storage: &v1alpha1.LokiStorageConfig{}}

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	err := checkMonitoringPreconditions(context.Background(), cli, m)
	if err == nil {
		t.Fatal("expected error when operators are missing")
	}

	errStr := err.Error()
	for _, expected := range []string{"Loki Operator", "Red Hat OpenShift Logging Operator", "OperatorHub"} {
		if !strings.Contains(errStr, expected) {
			t.Errorf("expected %q in error, got: %s", expected, errStr)
		}
	}
	if strings.Contains(errStr, "OpenTelemetry") {
		t.Errorf("cluster log forwarding should not require OpenTelemetry, got: %s", errStr)
	}
}

func TestCheckPreconditions_AllLoggingOperatorsPresent(t *testing.T) {
	s := newTestScheme(t)
	registerOperatorCondition(s)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{Storage: &v1alpha1.LokiStorageConfig{}}
	m.Spec.Logs = &v1alpha1.Logs{Storage: &v1alpha1.LokiStorageConfig{}}

	otel := newOperatorCondition("opentelemetry-operator.v0.158.0-1")
	loki := newOperatorCondition("loki-operator.v6.6.1")
	logging := newOperatorCondition("cluster-logging.v6.6.1")

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(otel, loki, logging).Build()
	if err := checkMonitoringPreconditions(context.Background(), cli, m); err != nil {
		t.Fatalf("expected no error when all logging operators are present, got: %v", err)
	}
}

func TestCheckPreconditions_AllFeaturesAndOperatorsPresent(t *testing.T) {
	s := newTestScheme(t)
	registerOperatorCondition(s)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}
	m.Spec.Traces = &v1alpha1.Traces{
		Storage: v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV},
	}
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{Storage: &v1alpha1.LokiStorageConfig{}}
	m.Spec.Logs = &v1alpha1.Logs{Storage: &v1alpha1.LokiStorageConfig{}}

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(
		newOperatorCondition("opentelemetry-operator.v0.158.0-1"),
		newOperatorCondition("cluster-observability-operator.v1.5.2"),
		newOperatorCondition("tempo-operator.v0.22.0-2"),
		newOperatorCondition("loki-operator.v6.6.1"),
		newOperatorCondition("cluster-logging.v6.6.1"),
	).Build()

	if err := checkMonitoringPreconditions(context.Background(), cli, m); err != nil {
		t.Fatalf("expected no error when every required operator is present, got: %v", err)
	}
}

func TestCheckPreconditions_AllFeaturesReportEachMissingOperatorOnce(t *testing.T) {
	s := newTestScheme(t)
	registerOperatorCondition(s)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}
	m.Spec.Traces = &v1alpha1.Traces{
		Storage: v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV},
	}
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{Storage: &v1alpha1.LokiStorageConfig{}}
	m.Spec.Logs = &v1alpha1.Logs{Storage: &v1alpha1.LokiStorageConfig{}}

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	err := checkMonitoringPreconditions(context.Background(), cli, m)
	if err == nil {
		t.Fatal("expected error when every required operator is missing")
	}

	errStr := err.Error()
	for _, operatorName := range []string{
		"Red Hat build of OpenTelemetry",
		"Cluster Observability Operator",
		"Tempo Operator",
		"Loki Operator",
		"Red Hat OpenShift Logging Operator",
	} {
		if count := strings.Count(errStr, operatorName); count != 1 {
			t.Errorf("expected %q to be reported once, got %d occurrences in: %s", operatorName, count, errStr)
		}
	}
}

func TestCheckPreconditions_PartialOperatorInstallations(t *testing.T) {
	tests := []struct {
		name              string
		configure         func(*v1alpha1.Monitoring)
		installedOperator string
		expectedMissing   string
		unexpectedMissing string
	}{
		{
			name:              "metrics with OpenTelemetry installed",
			configure:         func(m *v1alpha1.Monitoring) { m.Spec.Metrics = &v1alpha1.Metrics{} },
			installedOperator: "opentelemetry-operator.v0.158.0-1",
			expectedMissing:   "Cluster Observability Operator",
			unexpectedMissing: "Red Hat build of OpenTelemetry",
		},
		{
			name:              "metrics with COO installed",
			configure:         func(m *v1alpha1.Monitoring) { m.Spec.Metrics = &v1alpha1.Metrics{} },
			installedOperator: "cluster-observability-operator.v1.5.2",
			expectedMissing:   "Red Hat build of OpenTelemetry",
			unexpectedMissing: "Cluster Observability Operator",
		},
		{
			name: "traces with OpenTelemetry installed",
			configure: func(m *v1alpha1.Monitoring) {
				m.Spec.Traces = &v1alpha1.Traces{Storage: v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV}}
			},
			installedOperator: "opentelemetry-operator.v0.158.0-1",
			expectedMissing:   "Tempo Operator",
			unexpectedMissing: "Red Hat build of OpenTelemetry",
		},
		{
			name: "traces with Tempo installed",
			configure: func(m *v1alpha1.Monitoring) {
				m.Spec.Traces = &v1alpha1.Traces{Storage: v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV}}
			},
			installedOperator: "tempo-operator.v0.22.0-2",
			expectedMissing:   "Red Hat build of OpenTelemetry",
			unexpectedMissing: "Tempo Operator",
		},
		{
			name: "usage logs with OpenTelemetry installed",
			configure: func(m *v1alpha1.Monitoring) {
				m.Spec.UsageLogs = &v1alpha1.UsageLogs{Storage: &v1alpha1.LokiStorageConfig{}}
			},
			installedOperator: "opentelemetry-operator.v0.158.0-1",
			expectedMissing:   "Loki Operator",
			unexpectedMissing: "Red Hat build of OpenTelemetry",
		},
		{
			name: "usage logs with Loki installed",
			configure: func(m *v1alpha1.Monitoring) {
				m.Spec.UsageLogs = &v1alpha1.UsageLogs{Storage: &v1alpha1.LokiStorageConfig{}}
			},
			installedOperator: "loki-operator.v6.6.1",
			expectedMissing:   "Red Hat build of OpenTelemetry",
			unexpectedMissing: "Loki Operator",
		},
		{
			name: "cluster logs with Loki installed",
			configure: func(m *v1alpha1.Monitoring) {
				m.Spec.Logs = &v1alpha1.Logs{Storage: &v1alpha1.LokiStorageConfig{}}
			},
			installedOperator: "loki-operator.v6.6.1",
			expectedMissing:   "Red Hat OpenShift Logging Operator",
			unexpectedMissing: "Loki Operator",
		},
		{
			name: "cluster logs with OpenShift Logging installed",
			configure: func(m *v1alpha1.Monitoring) {
				m.Spec.Logs = &v1alpha1.Logs{Storage: &v1alpha1.LokiStorageConfig{}}
			},
			installedOperator: "cluster-logging.v6.6.1",
			expectedMissing:   "Loki Operator",
			unexpectedMissing: "Red Hat OpenShift Logging Operator",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestScheme(t)
			registerOperatorCondition(s)

			m := newMonitoring(v1alpha1.MonitoringInstanceName)
			tt.configure(m)

			cli := fake.NewClientBuilder().WithScheme(s).WithObjects(
				newOperatorCondition(tt.installedOperator),
			).Build()
			err := checkMonitoringPreconditions(context.Background(), cli, m)
			if err == nil {
				t.Fatal("expected an error for the remaining missing operator")
			}

			errStr := err.Error()
			if !strings.Contains(errStr, tt.expectedMissing) {
				t.Errorf("expected missing %q, got: %s", tt.expectedMissing, errStr)
			}
			if strings.Contains(errStr, tt.unexpectedMissing) {
				t.Errorf("installed operator %q was reported missing: %s", tt.unexpectedMissing, errStr)
			}
		})
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

	data := make(map[string]any)
	addImageURLs(data)

	if data["KubeRBACProxyImage"] == "" {
		t.Error("KubeRBACProxyImage should have a default")
	}
	if data["PromLabelProxyImage"] == "" {
		t.Error("PromLabelProxyImage should have a default")
	}
}

func TestAddImageURLs_OverriddenByEnv(t *testing.T) {
	t.Setenv("RELATED_IMAGE_ODH_KUBE_RBAC_PROXY_IMAGE", "custom-proxy:latest")
	t.Setenv("RELATED_IMAGE_OSE_PROM_LABEL_PROXY_IMAGE", "custom-prom-proxy:latest")

	data := make(map[string]any)
	addImageURLs(data)

	if data["KubeRBACProxyImage"] != "custom-proxy:latest" {
		t.Errorf("KubeRBACProxyImage: want custom-proxy:latest, got %v", data["KubeRBACProxyImage"])
	}
	if data["PromLabelProxyImage"] != "custom-prom-proxy:latest" {
		t.Errorf("PromLabelProxyImage: want custom-prom-proxy:latest, got %v", data["PromLabelProxyImage"])
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

func TestGetKorrel8rImage_Default(t *testing.T) {
	t.Setenv("RELATED_IMAGE_KORREL8R_IMAGE", "")
	if got := getKorrel8rImage(); got != "registry.redhat.io/cluster-observability-operator/korrel8r-rhel9@sha256:90cc70741585b3a555888cc119c1ad630e988513dd2065158b26f6fa33dc8a22" {
		t.Errorf("unexpected default Korrel8r image: %q", got)
	}
}

func TestGetKorrel8rImage_Override(t *testing.T) {
	t.Setenv("RELATED_IMAGE_KORREL8R_IMAGE", "custom-korrel8r:1.0")
	if got := getKorrel8rImage(); got != "custom-korrel8r:1.0" {
		t.Errorf("want custom-korrel8r:1.0, got %q", got)
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

func TestBuildTemplateData_UsesEffectiveMonitoringNamespace(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Namespace = ""
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{
		Storage: &v1alpha1.LokiStorageConfig{},
	}

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	data, err := buildTemplateData(context.Background(), cli, m, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if data["Namespace"] != defaultMonitoringNamespace {
		t.Errorf("Namespace: want %q, got %v", defaultMonitoringNamespace, data["Namespace"])
	}
	if data["UsageLogsEndpoint"] != "https://data-science-lokistack-gateway-http.opendatahub.svc.cluster.local:8080/api/logs/v1/application/otlp" {
		t.Errorf("UsageLogsEndpoint: unexpected effective namespace endpoint %v", data["UsageLogsEndpoint"])
	}
}

func TestBuildTemplateData_Korrel8rConfiguration(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}
	m.Spec.Traces = &v1alpha1.Traces{
		Storage: v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV},
	}
	m.Spec.Logs = &v1alpha1.Logs{}

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(kubernetesAPIServerEndpointSlice()).Build()
	data, err := buildTemplateData(context.Background(), cli, m, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if data["Logs"] != true {
		t.Error("Logs: want true")
	}
	if data["Korrel8rImage"] != "registry.redhat.io/cluster-observability-operator/korrel8r-rhel9@sha256:90cc70741585b3a555888cc119c1ad630e988513dd2065158b26f6fa33dc8a22" {
		t.Errorf("unexpected Korrel8r image: %v", data["Korrel8rImage"])
	}
	if data["Korrel8rRequestTimeout"] != "30s" || data["Korrel8rSessionTimeout"] != "5m" {
		t.Errorf("unexpected Korrel8r timeouts: request=%v session=%v", data["Korrel8rRequestTimeout"], data["Korrel8rSessionTimeout"])
	}
	if data["Korrel8rMemoryLimit"] != "512Mi" {
		t.Errorf("Korrel8r memory limit: want 512Mi, got %v", data["Korrel8rMemoryLimit"])
	}
	if data["ThanosQuerierEndpoint"] != "http://thanos-querier-data-science-thanos-querier.test-ns.svc.cluster.local:10902" {
		t.Errorf("unexpected Thanos endpoint: %v", data["ThanosQuerierEndpoint"])
	}
	if data["LokiTenant"] != "application" {
		t.Errorf("unexpected Loki tenant: %v", data["LokiTenant"])
	}
}

func TestBuildTemplateData_Korrel8rAPIServerEndpointDiscoveryFailureBlocksRendering(t *testing.T) {
	t.Parallel()

	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}

	cli := fake.NewClientBuilder().WithScheme(s).
		WithInterceptorFuncs(kubernetesAPIServerEndpointSliceListForbidden()).Build()
	_, err := buildTemplateData(context.Background(), cli, m, "")
	if err == nil {
		t.Fatal("expected API endpoint discovery failure to block rendering")
	}
}

func kubernetesAPIServerEndpointSliceListForbidden() interceptor.Funcs {
	return interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*discoveryv1.EndpointSliceList); ok {
				return errors.New("forbidden")
			}
			return c.List(ctx, list, opts...)
		},
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

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(kubernetesAPIServerEndpointSlice()).Build()
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

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(kubernetesAPIServerEndpointSlice()).Build()
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
