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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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

	// 3 base sources + 1 prometheus service (since metrics is configured)
	if len(sources) != 4 {
		t.Errorf("expected 4 sources for metrics+OTel, got %d", len(sources))
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

	// 3 base sources only (no prometheus service since metrics nil)
	if len(sources) != 3 {
		t.Errorf("expected 3 sources for traces-only+OTel, got %d", len(sources))
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

	if len(sources) != 2 {
		t.Errorf("expected 2 sources (prometheus + cluster prometheus datasource), got %d", len(sources))
	}

	c := findCondition(m, conditions.ConditionPersesPrometheusDataSourceAvailable)
	if c == nil || c.Status != metav1.ConditionTrue {
		t.Error("PersesPrometheusDataSourceAvailable should be True")
	}
}
