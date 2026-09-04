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
	"testing"

	platformcommon "github.com/opendatahub-io/odh-platform-utilities/api/common"
	libconditions "github.com/opendatahub-io/odh-platform-utilities/pkg/controller/conditions"
	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
)

func newActionsTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := newTestScheme(t)
	return s
}

func registerCRDs(s *runtime.Scheme, gvks ...schema.GroupVersionKind) {
	for _, g := range gvks {
		s.AddKnownTypeWithName(g, &unstructured.Unstructured{})
		listGVK := schema.GroupVersionKind{
			Group:   g.Group,
			Version: g.Version,
			Kind:    g.Kind + "List",
		}
		s.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	}
}

// missingCRDs returns an interceptor that makes List return NoKindMatchError
// for the given GVKs, simulating CRDs not installed on the cluster.
func missingCRDs(gvks ...schema.GroupVersionKind) interceptor.Funcs {
	missing := make(map[string]schema.GroupKind, len(gvks))
	for _, g := range gvks {
		missing[g.Kind+"List"] = schema.GroupKind{Group: g.Group, Kind: g.Kind}
	}
	return interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			kind := list.GetObjectKind().GroupVersionKind().Kind
			if gk, ok := missing[kind]; ok {
				return &meta.NoKindMatchError{GroupKind: gk, SearchedVersions: []string{"v1"}}
			}
			return c.List(ctx, list, opts...)
		},
	}
}

func findCondition(m *v1alpha1.Monitoring, condType string) *platformcommon.Condition {
	return libconditions.FindStatusCondition(m, condType)
}

// --- deployMonitoringStackWithQuerierAndRestrictions ---

func TestDeployMonitoringStack_NoMetrics(t *testing.T) {
	s := newActionsTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = nil

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployMonitoringStackWithQuerierAndRestrictions(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 0 {
		t.Errorf("expected no sources when metrics is nil, got %d", len(sources))
	}

	msC := findCondition(m, conditions.ConditionMonitoringStackAvailable)
	if msC == nil || msC.Status != metav1.ConditionFalse || msC.Severity != platformcommon.ConditionSeverityInfo {
		t.Errorf("MonitoringStackAvailable: expected False+Info, got %v", msC)
	}

	tqC := findCondition(m, conditions.ConditionThanosQuerierAvailable)
	if tqC == nil || tqC.Status != metav1.ConditionFalse || tqC.Severity != platformcommon.ConditionSeverityInfo {
		t.Errorf("ThanosQuerierAvailable: expected False+Info, got %v", tqC)
	}
}

func TestDeployMonitoringStack_CRDsPresent(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.MonitoringStack, gvk.ThanosQuerier)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	err := deployMonitoringStackWithQuerierAndRestrictions(context.Background(), cli, m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) == 0 {
		t.Error("expected sources when CRDs are present")
	}

	msC := findCondition(m, conditions.ConditionMonitoringStackAvailable)
	if msC == nil || msC.Status != metav1.ConditionTrue {
		t.Error("MonitoringStackAvailable should be True")
	}
	tqC := findCondition(m, conditions.ConditionThanosQuerierAvailable)
	if tqC == nil || tqC.Status != metav1.ConditionTrue {
		t.Error("ThanosQuerierAvailable should be True")
	}
}

// --- deployTracingStack ---

func TestDeployTracingStack_NoTraces(t *testing.T) {
	s := newActionsTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Traces = nil

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployTracingStack(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 0 {
		t.Errorf("expected no sources, got %d", len(sources))
	}

	tempoC := findCondition(m, conditions.ConditionTempoAvailable)
	if tempoC == nil || tempoC.Status != metav1.ConditionFalse || tempoC.Severity != platformcommon.ConditionSeverityInfo {
		t.Errorf("TempoAvailable: expected False+Info, got %v", tempoC)
	}
}

func TestDeployTracingStack_PVBackend_CRDsPresent(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.TempoMonolithic, gvk.Instrumentation)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Traces = &v1alpha1.Traces{
		Storage: v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV},
	}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployTracingStack(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 2 {
		t.Errorf("expected 2 sources (TempoMonolithic + Instrumentation), got %d", len(sources))
	}

	tempoC := findCondition(m, conditions.ConditionTempoAvailable)
	if tempoC == nil || tempoC.Status != metav1.ConditionTrue {
		t.Error("TempoAvailable should be True")
	}
}

func TestDeployTracingStack_S3Backend_CRDsPresent(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.TempoStack, gvk.Instrumentation)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Traces = &v1alpha1.Traces{
		Storage: v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendS3, Secret: "my-secret"},
	}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployTracingStack(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 2 {
		t.Errorf("expected 2 sources (TempoStack + Instrumentation), got %d", len(sources))
	}
}

// --- deployOpenTelemetryCollector ---

func TestDeployOpenTelemetryCollector_NeitherMetricsNorTraces(t *testing.T) {
	s := newActionsTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployOpenTelemetryCollector(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	otcC := findCondition(m, conditions.ConditionOpenTelemetryCollectorAvailable)
	if otcC == nil || otcC.Status != metav1.ConditionFalse || otcC.Severity != platformcommon.ConditionSeverityInfo {
		t.Errorf("OTelCollectorAvailable: expected False+Info, got %v", otcC)
	}
}

func TestDeployOpenTelemetryCollector_MetricsOnly_CRDPresent(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.OpenTelemetryCollector)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployOpenTelemetryCollector(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 6 sources with the always-on OTel templates included in the metrics path
	if len(sources) != 6 {
		t.Errorf("expected 6 sources for metrics+OTel, got %d", len(sources))
	}

	otcC := findCondition(m, conditions.ConditionOpenTelemetryCollectorAvailable)
	if otcC == nil || otcC.Status != metav1.ConditionTrue {
		t.Error("OTelCollectorAvailable should be True")
	}
}

func TestDeployOpenTelemetryCollector_TracesOnly_CRDPresent(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.OpenTelemetryCollector)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Traces = &v1alpha1.Traces{
		Storage: v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV},
	}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployOpenTelemetryCollector(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 5 base sources (incl. monitor service + monitoring NetworkPolicy) + 2 traces RBAC (MLflow + Tempo)
	if len(sources) != 7 {
		t.Errorf("expected 7 sources for traces-only+OTel, got %d", len(sources))
	}
}

func TestDeployOpenTelemetryCollector_MetricsAndTraces_CRDPresent(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.OpenTelemetryCollector)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}
	m.Spec.Traces = &v1alpha1.Traces{
		Storage: v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV},
	}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployOpenTelemetryCollector(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 5 base sources + 1 prometheus service + 2 traces RBAC (MLflow + Tempo)
	if len(sources) != 8 {
		t.Errorf("expected 8 sources for metrics+traces+OTel, got %d", len(sources))
	}
}

// --- deployAlerting ---

func TestDeployAlerting_NotConfigured(t *testing.T) {
	s := newActionsTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Alerting = nil

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployAlerting(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	alertC := findCondition(m, conditions.ConditionAlertingAvailable)
	if alertC == nil || alertC.Severity != platformcommon.ConditionSeverityInfo {
		t.Error("AlertingAvailable should be Info severity when not configured")
	}
}

func TestDeployAlerting_CRDPresent(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.PrometheusRule)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Alerting = &v1alpha1.Alerting{}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployAlerting(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 1 {
		t.Errorf("expected 1 source for alerting, got %d", len(sources))
	}

	alertC := findCondition(m, conditions.ConditionAlertingAvailable)
	if alertC == nil || alertC.Status != metav1.ConditionTrue {
		t.Error("AlertingAvailable should be True")
	}
}

// --- deployNodeMetricsEndpoint ---

func TestDeployNodeMetricsEndpoint_NoMetrics(t *testing.T) {
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployNodeMetricsEndpoint(context.Background(), nil, m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 0 {
		t.Errorf("expected no sources, got %d", len(sources))
	}

	nodeC := findCondition(m, conditions.ConditionNodeMetricsEndpointAvailable)
	if nodeC == nil || nodeC.Severity != platformcommon.ConditionSeverityInfo {
		t.Error("NodeMetricsEndpointAvailable should be Info severity")
	}
}

func TestDeployNodeMetricsEndpoint_MetricsConfigured(t *testing.T) {
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}
	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployNodeMetricsEndpoint(context.Background(), nil, m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 1 {
		t.Errorf("expected 1 source, got %d", len(sources))
	}

	nodeC := findCondition(m, conditions.ConditionNodeMetricsEndpointAvailable)
	if nodeC == nil || nodeC.Status != metav1.ConditionTrue {
		t.Error("NodeMetricsEndpointAvailable should be True")
	}
}

// --- deployPerses ---

func TestDeployPerses_NoMetricsOrTraces(t *testing.T) {
	s := newActionsTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployPerses(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources, "", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	persesC := findCondition(m, conditions.ConditionPersesAvailable)
	if persesC == nil || persesC.Severity != platformcommon.ConditionSeverityInfo {
		t.Error("PersesAvailable should be Info severity when not configured")
	}
}

func TestDeployPerses_CRDNotFound(t *testing.T) {
	s := newActionsTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployPerses(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources, "", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	persesC := findCondition(m, conditions.ConditionPersesAvailable)
	if persesC == nil || persesC.Status != metav1.ConditionFalse {
		t.Error("PersesAvailable should be False when CRD not found")
	}
}

func TestDeployPerses_CRDPresent(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.PersesV1Alpha2)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployPerses(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources, "v1alpha2", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 2 {
		t.Errorf("expected 2 sources (Perses + NetworkPolicy), got %d", len(sources))
	}

	persesC := findCondition(m, conditions.ConditionPersesAvailable)
	if persesC == nil || persesC.Status != metav1.ConditionTrue {
		t.Error("PersesAvailable should be True")
	}
}

// --- deployWebhookInfrastructure ---

func TestDeployWebhookInfrastructure_TLSSecretMissing(t *testing.T) {
	s := newActionsTestScheme(t)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)

	t.Setenv("OPERATOR_NAME", "odh-observability")
	t.Setenv("POD_NAMESPACE", "test-operator-ns")

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployWebhookInfrastructure(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 0 {
		t.Errorf("expected 0 sources (chart deploys webhook resources), got %d", len(sources))
	}

	wc := findCondition(m, conditions.ConditionWebhookAvailable)
	if wc == nil || wc.Status != metav1.ConditionFalse || wc.Reason != "TLSSecretPending" {
		t.Errorf("WebhookAvailable: expected False/TLSSecretPending, got %v", wc)
	}
}

func TestDeployWebhookInfrastructure_TLSSecretReady(t *testing.T) {
	s := newActionsTestScheme(t)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)

	operatorName := "odh-observability"
	operatorNS := "test-operator-ns"
	t.Setenv("OPERATOR_NAME", operatorName)
	t.Setenv("POD_NAMESPACE", operatorNS)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      operatorName + "-webhook-cert",
			Namespace: operatorNS,
		},
		Data: map[string][]byte{
			"tls.crt": []byte("cert-data"),
			"tls.key": []byte("key-data"),
		},
	}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(secret).Build()
	err := deployWebhookInfrastructure(context.Background(), cli, m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 0 {
		t.Errorf("expected 0 sources (chart deploys webhook resources), got %d", len(sources))
	}

	wc := findCondition(m, conditions.ConditionWebhookAvailable)
	if wc == nil || wc.Status != metav1.ConditionTrue {
		t.Error("WebhookAvailable should be True")
	}
}

// --- deployMonitoringAdmissionPolicies ---

func TestDeployMonitoringAdmissionPolicies(t *testing.T) {
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployMonitoringAdmissionPolicies(context.Background(), nil, m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 1 {
		t.Errorf("expected 1 source for admission policies, got %d", len(sources))
	}
}

// --- deployPersesPrometheusIntegration ---

func TestDeployPersesPrometheusIntegration_NoMetrics(t *testing.T) {
	s := newActionsTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployPersesPrometheusIntegration(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources, "v1alpha2", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	c := findCondition(m, conditions.ConditionPersesPrometheusDataSourceAvailable)
	if c == nil || c.Severity != platformcommon.ConditionSeverityInfo {
		t.Error("expected Info severity when metrics not configured")
	}
}

func TestDeployPersesPrometheusIntegration_CRDPresent(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.PersesDatasourceV1Alpha2)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployPersesPrometheusIntegration(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources, "v1alpha2", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 3 {
		t.Errorf("expected 3 sources (prometheus + cluster prometheus + tenancy datasource), got %d", len(sources))
	}

	c := findCondition(m, conditions.ConditionPersesPrometheusDataSourceAvailable)
	if c == nil || c.Status != metav1.ConditionTrue {
		t.Error("PersesPrometheusDataSourceAvailable should be True")
	}
}

// --- deployClusterLogForwarder ---

func TestDeployClusterLogForwarder_NoLogs(t *testing.T) {
	s := newActionsTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Logs = nil

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployClusterLogForwarder(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 0 {
		t.Errorf("expected no sources when logs is nil, got %d", len(sources))
	}

	c := findCondition(m, conditions.ConditionClusterLogForwarderAvailable)
	if c == nil || c.Status != metav1.ConditionFalse || c.Severity != platformcommon.ConditionSeverityInfo {
		t.Errorf("ClusterLogForwarderAvailable: expected False+Info, got %v", c)
	}
}

func TestDeployClusterLogForwarder_CRDPresent(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.ClusterLogForwarder, gvk.LokiStack)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Logs = &v1alpha1.Logs{}

	readyLoki := &unstructured.Unstructured{}
	readyLoki.SetGroupVersionKind(gvk.LokiStack)
	readyLoki.SetName("data-science-lokistack")
	readyLoki.SetNamespace(m.Spec.Namespace)
	_ = unstructured.SetNestedSlice(readyLoki.Object, []any{
		map[string]any{
			"type":   "Ready",
			"status": "True",
		},
	}, "status", "conditions")

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(readyLoki).WithStatusSubresource(readyLoki).Build()
	err := deployClusterLogForwarder(context.Background(), cli, m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 2 {
		t.Errorf("expected 2 sources (CLF + RBAC), got %d", len(sources))
	}
}

func TestDeployClusterLogForwarder_CRDMissing(t *testing.T) {
	s := newActionsTestScheme(t)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Logs = &v1alpha1.Logs{}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	cli := fake.NewClientBuilder().WithScheme(s).
		WithInterceptorFuncs(missingCRDs(gvk.ClusterLogForwarder)).Build()
	err := deployClusterLogForwarder(context.Background(), cli, m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 0 {
		t.Errorf("expected no sources when CLF CRD is missing, got %d", len(sources))
	}

	c := findCondition(m, conditions.ConditionClusterLogForwarderAvailable)
	if c == nil {
		t.Fatal("expected ClusterLogForwarderAvailable condition to be set")
	}
	if c.Status != metav1.ConditionFalse {
		t.Errorf("expected status False, got %s", c.Status)
	}
	if c.Reason != "ClusterLogForwarderCRDNotFound" {
		t.Errorf("expected reason ClusterLogForwarderCRDNotFound, got %s", c.Reason)
	}
}

func TestDeployClusterLogForwarder_LokiStackNotReady(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.ClusterLogForwarder, gvk.LokiStack)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Logs = &v1alpha1.Logs{}

	notReadyLoki := &unstructured.Unstructured{}
	notReadyLoki.SetGroupVersionKind(gvk.LokiStack)
	notReadyLoki.SetName("data-science-lokistack")
	notReadyLoki.SetNamespace(m.Spec.Namespace)
	_ = unstructured.SetNestedSlice(notReadyLoki.Object, []any{
		map[string]any{
			"type":   "Ready",
			"status": "False",
		},
	}, "status", "conditions")

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(notReadyLoki).WithStatusSubresource(notReadyLoki).Build()
	err := deployClusterLogForwarder(context.Background(), cli, m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 0 {
		t.Errorf("expected no sources when LokiStack is not ready, got %d", len(sources))
	}

	c := findCondition(m, conditions.ConditionClusterLogForwarderAvailable)
	if c == nil {
		t.Fatal("expected ClusterLogForwarderAvailable condition to be set")
	}
	if c.Status != metav1.ConditionFalse {
		t.Errorf("expected status False, got %s", c.Status)
	}
	if c.Reason != "LokiStackNotReady" {
		t.Errorf("expected reason LokiStackNotReady, got %s", c.Reason)
	}
}

func TestDeployClusterLogForwarder_CLFExistsButNotReady(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.ClusterLogForwarder, gvk.LokiStack)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Logs = &v1alpha1.Logs{}

	// LokiStack is ready.
	readyLoki := &unstructured.Unstructured{}
	readyLoki.SetGroupVersionKind(gvk.LokiStack)
	readyLoki.SetName("data-science-lokistack")
	readyLoki.SetNamespace(m.Spec.Namespace)
	_ = unstructured.SetNestedSlice(readyLoki.Object, []any{
		map[string]any{
			"type":   "Ready",
			"status": "True",
		},
	}, "status", "conditions")

	notReadyCLF := &unstructured.Unstructured{}
	notReadyCLF.SetGroupVersionKind(gvk.ClusterLogForwarder)
	notReadyCLF.SetName("data-science-cluster-log-forwarder")
	notReadyCLF.SetNamespace(m.Spec.Namespace)
	_ = unstructured.SetNestedSlice(notReadyCLF.Object, []any{
		map[string]any{
			"type":   "Ready",
			"status": "False",
		},
	}, "status", "conditions")

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	cli := fake.NewClientBuilder().WithScheme(s).
		WithObjects(readyLoki, notReadyCLF).
		WithStatusSubresource(readyLoki, notReadyCLF).
		Build()
	err := deployClusterLogForwarder(context.Background(), cli, m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 2 {
		t.Errorf("expected 2 sources (CLF + RBAC) even when CLF is not ready, got %d", len(sources))
	}

	c := findCondition(m, conditions.ConditionClusterLogForwarderAvailable)
	if c == nil {
		t.Fatal("expected ClusterLogForwarderAvailable condition to be set")
	}
	if c.Status != metav1.ConditionFalse {
		t.Errorf("expected status False, got %s", c.Status)
	}
	if c.Reason != "ClusterLogForwarderNotReady" {
		t.Errorf("expected reason ClusterLogForwarderNotReady, got %s", c.Reason)
	}
}

func TestDeployClusterLogForwarder_ExplicitInferenceNamespaces(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.ClusterLogForwarder, gvk.LokiStack)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Logs = &v1alpha1.Logs{
		Storage:             &v1alpha1.LokiStorageConfig{Type: "s3", SecretName: "loki-s3", CredentialMode: "static"},
		InferenceNamespaces: []string{"ns-a", "ns-b"},
	}

	readyLoki := &unstructured.Unstructured{}
	readyLoki.SetGroupVersionKind(gvk.LokiStack)
	readyLoki.SetName("data-science-lokistack")
	readyLoki.SetNamespace(m.Spec.Namespace)
	_ = unstructured.SetNestedSlice(readyLoki.Object, []any{
		map[string]any{
			"type":   "Ready",
			"status": "True",
		},
	}, "status", "conditions")

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(readyLoki).WithStatusSubresource(readyLoki).Build()
	err := deployClusterLogForwarder(context.Background(), cli, m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 2 {
		t.Errorf("expected 2 sources, got %d", len(sources))
	}

	data, err := buildTemplateData(context.Background(), cli, m, "")
	if err != nil {
		t.Fatalf("buildTemplateData error: %v", err)
	}

	ns, ok := data["InferenceNamespaces"].([]string)
	if !ok {
		t.Fatal("InferenceNamespaces not found or wrong type in template data")
	}
	if len(ns) != 2 || ns[0] != "ns-a" || ns[1] != "ns-b" {
		t.Errorf("expected [ns-a ns-b], got %v", ns)
	}
}

func TestDeployClusterLogForwarder_MaliciousNamespacesFiltered(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.ClusterLogForwarder, gvk.LokiStack)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Logs = &v1alpha1.Logs{
		Storage: &v1alpha1.LokiStorageConfig{Type: "s3", SecretName: "loki-s3", CredentialMode: "static"},
		InferenceNamespaces: []string{
			"valid-ns",
			"injection\n---\napiVersion: v1",
			"UPPERCASE",
			"also-valid",
			"-starts-bad",
		},
	}

	readyLoki := &unstructured.Unstructured{}
	readyLoki.SetGroupVersionKind(gvk.LokiStack)
	readyLoki.SetName("data-science-lokistack")
	readyLoki.SetNamespace(m.Spec.Namespace)
	_ = unstructured.SetNestedSlice(readyLoki.Object, []any{
		map[string]any{
			"type":   "Ready",
			"status": "True",
		},
	}, "status", "conditions")

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(readyLoki).WithStatusSubresource(readyLoki).Build()
	data, err := buildTemplateData(context.Background(), cli, m, "")
	if err != nil {
		t.Fatalf("buildTemplateData error: %v", err)
	}

	ns, ok := data["InferenceNamespaces"].([]string)
	if !ok {
		t.Fatal("InferenceNamespaces not found or wrong type in template data")
	}
	if len(ns) != 2 {
		t.Errorf("expected 2 valid namespaces, got %d: %v", len(ns), ns)
	}
	for _, n := range ns {
		if n != "valid-ns" && n != "also-valid" {
			t.Errorf("unexpected namespace in result: %q", n)
		}
	}
}

func TestDeployClusterLogForwarder_AllNamespacesInvalid(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.ClusterLogForwarder, gvk.LokiStack)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Logs = &v1alpha1.Logs{
		Storage: &v1alpha1.LokiStorageConfig{Type: "s3", SecretName: "loki-s3", CredentialMode: "static"},
		InferenceNamespaces: []string{
			"UPPERCASE",
			"-starts-bad",
			"has\nnewline",
		},
	}

	readyLoki := &unstructured.Unstructured{}
	readyLoki.SetGroupVersionKind(gvk.LokiStack)
	readyLoki.SetName("data-science-lokistack")
	readyLoki.SetNamespace(m.Spec.Namespace)
	_ = unstructured.SetNestedSlice(readyLoki.Object, []any{
		map[string]any{
			"type":   "Ready",
			"status": "True",
		},
	}, "status", "conditions")

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(readyLoki).WithStatusSubresource(readyLoki).Build()
	data, err := buildTemplateData(context.Background(), cli, m, "")
	if err != nil {
		t.Fatalf("buildTemplateData error: %v", err)
	}

	ns, ok := data["InferenceNamespaces"].([]string)
	if !ok {
		t.Fatal("InferenceNamespaces should be a []string (possibly nil)")
	}
	if len(ns) != 0 {
		t.Errorf("expected 0 valid namespaces when all are invalid, got %d: %v", len(ns), ns)
	}
}

func TestDeployLokiStack_LogsOnlyNoUsageLogs(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.LokiStack)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.UsageLogs = nil
	m.Spec.Logs = &v1alpha1.Logs{
		Storage: &v1alpha1.LokiStorageConfig{
			Type:             "s3",
			SecretName:       "logs-only-secret",
			CredentialMode:   "static",
			StorageClassName: "gp3-csi",
		},
		InferenceNamespaces: []string{"inference-ns"},
	}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	err := deployLokiStack(context.Background(), cli, m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 1 {
		t.Fatalf("expected 1 source (LokiStackTemplate), got %d", len(sources))
	}

	data, err := buildTemplateData(context.Background(), cli, m, "")
	if err != nil {
		t.Fatalf("buildTemplateData error: %v", err)
	}

	if data["LokiStorageSecretName"] != "logs-only-secret" {
		t.Errorf("expected LokiStorageSecretName=logs-only-secret, got %v", data["LokiStorageSecretName"])
	}
	if data["LokiStorageType"] != "s3" {
		t.Errorf("expected LokiStorageType=s3, got %v", data["LokiStorageType"])
	}
	if data["LokiStorageCredentialMode"] != "static" {
		t.Errorf("expected LokiStorageCredentialMode=static, got %v", data["LokiStorageCredentialMode"])
	}
	if data["LokiStorageClassName"] != "gp3-csi" {
		t.Errorf("expected LokiStorageClassName=gp3-csi, got %v", data["LokiStorageClassName"])
	}
	if data["UsageLogs"] != false {
		t.Errorf("expected UsageLogs=false when usageLogs is nil, got %v", data["UsageLogs"])
	}
}
