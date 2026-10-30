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
	"maps"
	"slices"
	"strings"
	"testing"

	platformcommon "github.com/opendatahub-io/odh-platform-utilities/api/common"
	libconditions "github.com/opendatahub-io/odh-platform-utilities/pkg/controller/conditions"
	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
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

func kubernetesAPIServerEndpointSlice() *discoveryv1.EndpointSlice {
	ready := true
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      kubernetesServiceName,
			Namespace: metav1.NamespaceDefault,
			Labels:    map[string]string{discoveryv1.LabelServiceName: kubernetesServiceName},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses:  []string{"172.20.0.1"},
			Conditions: discoveryv1.EndpointConditions{Ready: &ready},
		}},
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

// mustFindCondition returns the named status condition, failing the test when
// it is absent. The explicit nil guard keeps staticcheck's SA5011 happy: it
// cannot see that t.Fatal terminates the test, so a bare nil check followed
// by field access looks like a possible nil pointer dereference.
func mustFindCondition(t *testing.T, m *v1alpha1.Monitoring, condType string) *platformcommon.Condition {
	t.Helper()

	c := findCondition(m, condType)
	if c == nil {
		t.Fatalf("expected %s condition to be set", condType)
	}
	return c
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

func TestDeployMonitoringStack_ExportersOnly_SkipsStack(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.MonitoringStack, gvk.ThanosQuerier)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{
		Exporters: map[string]runtime.RawExtension{
			"otlphttp/external": {Raw: []byte(`{"endpoint":"http://example:4318"}`)},
		},
	}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployMonitoringStackWithQuerierAndRestrictions(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sources) != 0 {
		t.Errorf("exporters-only metrics must not deploy MonitoringStack, got %d sources", len(sources))
	}
	msC := findCondition(m, conditions.ConditionMonitoringStackAvailable)
	if msC == nil || msC.Status != metav1.ConditionFalse {
		t.Errorf("MonitoringStackAvailable: expected False for exporters-only, got %v", msC)
	}
}

func TestDeployMonitoringStack_CRDsPresent(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.MonitoringStack, gvk.ThanosQuerier)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{
		Storage: &v1alpha1.MetricsStorage{},
	}

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

func TestDeployTracingStack_ExportersOnly_SkipsTempo(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.TempoMonolithic, gvk.Instrumentation)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Traces = &v1alpha1.Traces{
		Exporters: map[string]runtime.RawExtension{
			"debug": {Raw: []byte(`{"verbosity":"detailed"}`)},
		},
	}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployTracingStack(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sources) != 1 {
		t.Errorf("exporters-only traces must deploy Instrumentation only, got %d sources", len(sources))
	} else if sources[0].Path != InstrumentationTemplate {
		t.Errorf("expected Instrumentation template, got %q", sources[0].Path)
	}
	tempoC := findCondition(m, conditions.ConditionTempoAvailable)
	if tempoC == nil || tempoC.Status != metav1.ConditionFalse {
		t.Errorf("TempoAvailable: expected False for exporters-only, got %v", tempoC)
	}
	instrC := findCondition(m, conditions.ConditionInstrumentationAvailable)
	if instrC == nil || instrC.Status != metav1.ConditionTrue {
		t.Errorf("InstrumentationAvailable: expected True for exporters-only, got %v", instrC)
	}
}

func TestDeployTracingStack_PVBackend_CRDsPresent(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.TempoMonolithic, gvk.Instrumentation)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Traces = &v1alpha1.Traces{
		Storage: &v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV},
	}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployTracingStack(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 3 {
		t.Errorf("expected 3 sources (TempoMonolithic + NetworkPolicy + Instrumentation), got %d", len(sources))
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
		Storage: &v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendS3, Secret: "my-secret"},
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
	m.Spec.Metrics = &v1alpha1.Metrics{
		Storage: &v1alpha1.MetricsStorage{},
	}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployOpenTelemetryCollector(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 7 sources with the always-on OTel templates included in the metrics path
	if len(sources) != 7 {
		t.Errorf("expected 7 sources for metrics+OTel, got %d", len(sources))
	}

	otcC := findCondition(m, conditions.ConditionOpenTelemetryCollectorAvailable)
	if otcC == nil || otcC.Status != metav1.ConditionTrue {
		t.Error("OTelCollectorAvailable should be True")
	}
}

func TestDeployOpenTelemetryCollector_MetricsOnly_WithoutCertManagerCRD(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.OpenTelemetryCollector)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	err := deployOpenTelemetryCollector(context.Background(), cli, m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sources) != 5 {
		t.Fatalf("expected collector sources without cert-manager, got %d", len(sources))
	}

	condition := findCondition(m, conditions.ConditionOpenTelemetryCollectorAvailable)
	if condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("expected collector condition to be true without cert-manager, got %v", condition)
	}
}

func TestDeployOpenTelemetryCollector_TracesOnly_CRDPresent(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.OpenTelemetryCollector)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Traces = &v1alpha1.Traces{
		Storage: &v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV},
	}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployOpenTelemetryCollector(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 5 base sources (no ServiceMonitors without metrics.storage) + 2 traces RBAC (MLflow + Tempo)
	if len(sources) != 7 {
		t.Errorf("expected 7 sources for traces-only+OTel, got %d", len(sources))
	}
}

func TestDeployOpenTelemetryCollector_ExportersOnlyMetrics_NoServiceMonitors(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.OpenTelemetryCollector)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{
		Exporters: map[string]runtime.RawExtension{
			"otlphttp/metrics-external": {Raw: []byte(`{"endpoint":"http://otel-external.example.svc:4318"}`)},
		},
	}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployOpenTelemetryCollector(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 5 base sources only — no ServiceMonitors / prometheus Service without metrics.storage
	if len(sources) != 5 {
		t.Errorf("expected 5 sources for exporters-only metrics, got %d", len(sources))
	}
	for _, source := range sources {
		if source.Path == CollectorServiceMonitorsTemplate || source.Path == CollectorPrometheusServiceTemplate {
			t.Errorf("exporters-only metrics must not include %s", source.Path)
		}
	}

	otcC := findCondition(m, conditions.ConditionOpenTelemetryCollectorAvailable)
	if otcC == nil || otcC.Status != metav1.ConditionTrue {
		t.Error("OTelCollectorAvailable should be True")
	}
}

func TestDeployOpenTelemetryCollector_MetricsAndTraces_CRDPresent(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.OpenTelemetryCollector)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{
		Storage: &v1alpha1.MetricsStorage{},
	}
	m.Spec.Traces = &v1alpha1.Traces{
		Storage: &v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV},
	}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployOpenTelemetryCollector(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 5 base + ServiceMonitors + prometheus service + 2 traces RBAC (MLflow + Tempo)
	if len(sources) != 9 {
		t.Errorf("expected 9 sources for metrics+traces+OTel, got %d", len(sources))
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

	if len(sources) != 0 {
		t.Errorf("expected 0 sources for alerting, got %d", len(sources))
	}

	alertC := findCondition(m, conditions.ConditionAlertingAvailable)
	if alertC == nil || alertC.Status != metav1.ConditionTrue {
		t.Error("AlertingAvailable should be True")
	}
}

func TestDeployOperatorMonitoring_CRDPresent(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.PrometheusRule)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployOperatorMonitoring(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 1 {
		t.Errorf("expected 1 source for operator monitoring, got %d", len(sources))
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
	m.Spec.Metrics = &v1alpha1.Metrics{
		Storage: &v1alpha1.MetricsStorage{},
	}
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
	m.Spec.Metrics = &v1alpha1.Metrics{
		Storage: &v1alpha1.MetricsStorage{},
	}

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
	m.Spec.Metrics = &v1alpha1.Metrics{
		Storage: &v1alpha1.MetricsStorage{},
	}

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

func TestDeployPerses_ExportersOnly_SkipsPerses(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.PersesV1Alpha2)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{
		Exporters: map[string]runtime.RawExtension{
			"otlphttp/external": {Raw: []byte(`{"endpoint":"http://example:4318"}`)},
		},
	}
	m.Spec.Traces = &v1alpha1.Traces{
		Exporters: map[string]runtime.RawExtension{
			"otlphttp/traces": {Raw: []byte(`{"endpoint":"http://example:4318"}`)},
		},
	}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployPerses(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources, "v1alpha2", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sources) != 0 {
		t.Errorf("exporters-only metrics/traces must not deploy Perses, got %d sources", len(sources))
	}
	persesC := findCondition(m, conditions.ConditionPersesAvailable)
	if persesC == nil || persesC.Status != metav1.ConditionFalse || persesC.Severity != platformcommon.ConditionSeverityInfo {
		t.Errorf("PersesAvailable: expected False+Info for exporters-only, got %v", persesC)
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
	m.Spec.Metrics = &v1alpha1.Metrics{
		Storage: &v1alpha1.MetricsStorage{},
	}

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

func TestDeployPersesTempoIntegrationUsesConfiguredMonitoringNamespace(t *testing.T) {
	t.Setenv("MONITORING_NAMESPACE", "custom-monitoring")
	s := newActionsTestScheme(t)
	_, datasourceGVK, dashboardGVK := persesGVKs(persesV1Alpha2)
	registerCRDs(s, datasourceGVK, dashboardGVK)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Namespace = ""

	datasource := &unstructured.Unstructured{}
	datasource.SetGroupVersionKind(datasourceGVK)
	datasource.SetName(PersesTempoDatasourceName)
	datasource.SetNamespace("custom-monitoring")
	dashboard := &unstructured.Unstructured{}
	dashboard.SetGroupVersionKind(dashboardGVK)
	dashboard.SetName(PersesTempoDashboardName)
	dashboard.SetNamespace("custom-monitoring")

	cli := fake.NewClientBuilder().WithScheme(s).
		WithObjects(datasource, dashboard).
		Build()
	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource
	if err := deployPersesTempoIntegration(context.Background(), cli, m, cm, &sources, persesV1Alpha2, true); err != nil {
		t.Fatalf("deployPersesTempoIntegration: %v", err)
	}

	if err := cli.Get(context.Background(), client.ObjectKey{
		Namespace: "custom-monitoring",
		Name:      PersesTempoDatasourceName,
	}, datasource); err == nil {
		t.Fatal("expected Perses datasource to be deleted from the configured namespace")
	}
	if err := cli.Get(context.Background(), client.ObjectKey{
		Namespace: "custom-monitoring",
		Name:      PersesTempoDashboardName,
	}, dashboard); err == nil {
		t.Fatal("expected Perses dashboard to be deleted from the configured namespace")
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

	if len(sources) != 3 {
		t.Errorf("expected 3 sources (CLF + RBAC + NetworkPolicy), got %d", len(sources))
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
		return
	}
	c = mustFindCondition(t, m, conditions.ConditionClusterLogForwarderAvailable)
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

	if len(sources) != 3 {
		t.Errorf("expected 3 sources (CLF + RBAC + NetworkPolicy) when LokiStack is not ready, got %d", len(sources))
	}

	c := findCondition(m, conditions.ConditionClusterLogForwarderAvailable)
	if c == nil {
		t.Fatal("expected ClusterLogForwarderAvailable condition to be set")
		return
	}
	c = mustFindCondition(t, m, conditions.ConditionClusterLogForwarderAvailable)
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

	if len(sources) != 3 {
		t.Errorf("expected 3 sources (CLF + RBAC + NetworkPolicy) even when CLF is not ready, got %d", len(sources))
	}

	c := findCondition(m, conditions.ConditionClusterLogForwarderAvailable)
	if c == nil {
		t.Fatal("expected ClusterLogForwarderAvailable condition to be set")
		return
	}
	c = mustFindCondition(t, m, conditions.ConditionClusterLogForwarderAvailable)
	if c.Status != metav1.ConditionFalse {
		t.Errorf("expected status False, got %s", c.Status)
	}
	if c.Reason != "ClusterLogForwarderNotReady" {
		t.Errorf("expected reason ClusterLogForwarderNotReady, got %s", c.Reason)
	}
}

func TestDeployClusterLogForwarder_UsesClusterLoggingNamespace(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.ClusterLogForwarder, gvk.LokiStack)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Logs = &v1alpha1.Logs{}

	readyLoki := &unstructured.Unstructured{}
	readyLoki.SetGroupVersionKind(gvk.LokiStack)
	readyLoki.SetName("data-science-lokistack")
	readyLoki.SetNamespace(m.Spec.Namespace)
	_ = unstructured.SetNestedSlice(readyLoki.Object, []any{
		map[string]any{"type": "Ready", "status": "True"},
	}, "status", "conditions")

	readyCLF := &unstructured.Unstructured{}
	readyCLF.SetGroupVersionKind(gvk.ClusterLogForwarder)
	readyCLF.SetName("data-science-cluster-log-forwarder")
	readyCLF.SetNamespace("openshift-logging")
	_ = unstructured.SetNestedSlice(readyCLF.Object, []any{
		map[string]any{"type": "Ready", "status": "True"},
	}, "status", "conditions")

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource
	cli := fake.NewClientBuilder().WithScheme(s).
		WithObjects(readyLoki, readyCLF, kubernetesAPIServerEndpointSlice()).
		WithStatusSubresource(readyLoki, readyCLF).
		Build()

	if err := deployClusterLogForwarder(context.Background(), cli, m, cm, &sources); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	condition := findCondition(m, conditions.ConditionClusterLogForwarderAvailable)
	if condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("ClusterLogForwarderAvailable: want True, got %v", condition)
	}

	data, err := buildTemplateData(context.Background(), cli, m, "")
	if err != nil {
		t.Fatalf("buildTemplateData error: %v", err)
	}
	objects, err := rendertemplate.Render(context.Background(), s, sources, data)
	if err != nil {
		t.Fatalf("rendering ClusterLogForwarder templates: %v", err)
	}
	for i := range objects {
		if objects[i].GetKind() != "ClusterLogForwarder" && objects[i].GetKind() != "ServiceAccount" && objects[i].GetKind() != "NetworkPolicy" {
			continue
		}
		if objects[i].GetNamespace() != "openshift-logging" {
			t.Fatalf("%s %q: want namespace openshift-logging, got %q", objects[i].GetKind(), objects[i].GetName(), objects[i].GetNamespace())
		}
		if objects[i].GetKind() != "ClusterLogForwarder" {
			if objects[i].GetKind() == "NetworkPolicy" && objects[i].GetName() != ClusterLogForwarderNetworkPolicyName {
				t.Fatalf("unexpected ClusterLogForwarder NetworkPolicy name %q", objects[i].GetName())
			}
			continue
		}
		outputs, found, err := unstructured.NestedSlice(objects[i].Object, "spec", "outputs")
		if err != nil || !found || len(outputs) != 1 {
			t.Fatalf("reading ClusterLogForwarder output: found=%t count=%d err=%v", found, len(outputs), err)
		}
		output, ok := outputs[0].(map[string]any)
		if !ok {
			t.Fatalf("ClusterLogForwarder output has unexpected type %T", outputs[0])
		}
		tls, ok := output["tls"].(map[string]any)
		if !ok {
			t.Fatalf("ClusterLogForwarder output has no TLS configuration: %#v", output)
		}
		ca, ok := tls["ca"].(map[string]any)
		if !ok || ca["configMapName"] != "openshift-service-ca.crt" {
			t.Fatalf("ClusterLogForwarder must use the service CA in openshift-logging, got %#v", ca)
		}
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

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(readyLoki, kubernetesAPIServerEndpointSlice()).WithStatusSubresource(readyLoki).Build()
	err := deployClusterLogForwarder(context.Background(), cli, m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 3 {
		t.Errorf("expected 3 sources, got %d", len(sources))
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

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(readyLoki, kubernetesAPIServerEndpointSlice()).WithStatusSubresource(readyLoki).Build()
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

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(readyLoki, kubernetesAPIServerEndpointSlice()).WithStatusSubresource(readyLoki).Build()
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

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(kubernetesAPIServerEndpointSlice()).Build()
	if usageLogsErr, logsErr := deployBothLokiStacks(context.Background(), cli, m, cm, &sources); usageLogsErr != nil || logsErr != nil {
		t.Fatalf("unexpected errors: usage logs %v, logs %v", usageLogsErr, logsErr)
	}

	// Log forwarding brings its whole pipeline: its stack, the network
	// policies guarding it, and the forwarder that feeds it. The forwarder's
	// templates stay in the desired set while the stack is starting — that is
	// the rule that keeps the sweep from taking them down during a transient
	// Loki outage — so they are here even though no stack exists yet.
	assertTemplatePaths(t, sources, []string{
		LokiStackTemplate,
		LokiStackNetworkPolicyTemplate,
		LokiStackGatewayNetworkPolicyTemplate,
		ClusterLogForwarderTemplate,
		ClusterLogForwarderRBACTemplate,
		ClusterLogForwarderNetworkPolicyTemplate,
	})

	c := mustFindCondition(t, m, conditions.ConditionUsageLogsLokiStackAvailable)
	if c.Status != metav1.ConditionFalse || c.Reason != "LokiNotRequired" {
		t.Errorf("UsageLogsLokiStackAvailable: expected False/LokiNotRequired, got %s/%s", c.Status, c.Reason)
	}

	// The folded collector reports its own not-configured verdict: the
	// pipeline is one action now, but each part still writes its own
	// condition.
	if c := mustFindCondition(t, m, conditions.ConditionUsageLogsCollectorAvailable); c.Status != metav1.ConditionFalse ||
		c.Reason != "UsageLogsNotConfigured" {
		t.Errorf("UsageLogsCollectorAvailable: expected False/UsageLogsNotConfigured, got %s/%s", c.Status, c.Reason)
	}

	// The forwarder waits for the stack it ships into, which does not exist
	// yet in this cluster.
	if c := mustFindCondition(t, m, conditions.ConditionClusterLogForwarderAvailable); c.Status != metav1.ConditionFalse ||
		c.Reason != "LokiStackNotReady" {
		t.Errorf("ClusterLogForwarderAvailable: expected False/LokiStackNotReady, got %s/%s", c.Status, c.Reason)
	}
}

func TestDeployLokiStack_UsageLogsOnlyNoLogs(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.LokiStack)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Logs = nil
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{
		Storage: &v1alpha1.LokiStorageConfig{
			Type:             "s3",
			SecretName:       "usage-logs-only-secret",
			CredentialMode:   "static",
			StorageClassName: "gp3-csi",
		},
	}

	// The endpoint of a ready stack, published by an earlier round: the
	// collector gates its rendering on it, so setting it is what makes this
	// test see the whole folded pipeline rather than the stack alone.
	m.Status.UsageLogsEndpoint = usageLogsEndpoint

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(kubernetesAPIServerEndpointSlice()).Build()
	if usageLogsErr, logsErr := deployBothLokiStacks(context.Background(), cli, m, cm, &sources); usageLogsErr != nil || logsErr != nil {
		t.Fatalf("unexpected errors: usage logs %v, logs %v", usageLogsErr, logsErr)
	}

	// Usage logs brings its whole pipeline: the stack, the policy guarding
	// that stack's gateway, and the collector that dials it. The gateway
	// policy belongs to the stack rather than to the collector, so it is in
	// place for the whole startup window — before any collector exists.
	assertTemplatePaths(t, sources, []string{
		UsageLogsLokiStackTemplate, UsageLogsLokiStackNPTemplate,
		UsageLogsOpenTelemetryCollectorTemplate,
		UsageLogsOpenTelemetryCollectorRBACTemplate,
		UsageLogsOpenTelemetryCollectorNPTemplate,
	})

	// One action, two verdicts: the stack reports from its readiness, the
	// collector from the endpoint that readiness published.
	if c := mustFindCondition(t, m, conditions.ConditionUsageLogsCollectorAvailable); c.Status != metav1.ConditionTrue {
		t.Errorf("UsageLogsCollectorAvailable: expected True with the endpoint published, got %s/%s", c.Status, c.Reason)
	}

	// Log forwarding is not configured, so its stack must stay out of the way
	// and its forwarder must say so.
	c := mustFindCondition(t, m, conditions.ConditionLokiStackAvailable)
	if c.Status != metav1.ConditionFalse || c.Reason != "LokiNotRequired" {
		t.Errorf("LokiStackAvailable: expected False/LokiNotRequired, got %s/%s", c.Status, c.Reason)
	}
	if c := mustFindCondition(t, m, conditions.ConditionClusterLogForwarderAvailable); c.Status != metav1.ConditionFalse ||
		c.Reason != "LogsNotConfigured" {
		t.Errorf("ClusterLogForwarderAvailable: expected False/LogsNotConfigured, got %s/%s", c.Status, c.Reason)
	}
}

// TestDeployLokiStack_BothConfigured covers the two stacks being independent:
// each contributes its own templates and is reported on by its own condition.
// The storage keys that feed those templates are covered by
// TestAddLokiStorageData and TestLokiStackTemplatesRenderIndependentStorage.
func TestDeployLokiStack_BothConfigured(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.LokiStack)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{
		Storage: &v1alpha1.LokiStorageConfig{
			Type:             "s3",
			SecretName:       "usage-logs-secret",
			CredentialMode:   "static",
			StorageClassName: "usage-logs-sc",
		},
	}
	m.Spec.Logs = &v1alpha1.Logs{
		Storage: &v1alpha1.LokiStorageConfig{
			Type:           "s3",
			SecretName:     "logs-secret",
			CredentialMode: "token",
		},
	}

	// The endpoints of two ready stacks, published by earlier rounds: with
	// them set, both folded pipelines render whole.
	m.Status.UsageLogsEndpoint = usageLogsEndpoint

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	// Both storage Secrets have to exist and resolve to different buckets:
	// with both stacks configured the isolation check fails closed, so an
	// unresolvable Secret withholds them before any of this is reached.
	cli := fake.NewClientBuilder().WithScheme(s).
		WithObjects(kubernetesAPIServerEndpointSlice(),
			lokiStorageSecret("usage-logs-secret", m.Spec.Namespace, "https://s3.example.com", "usage-logs-bucket"),
			lokiStorageSecret("logs-secret", m.Spec.Namespace, "https://s3.example.com", "logs-bucket"),
			readyLokiStack(usageLogsLokiStackResourceName, m.Spec.Namespace),
			readyLokiStack(lokiStackResourceName, m.Spec.Namespace)).
		Build()

	if withheld, err := validateStorageIsolation(context.Background(), cli, m, cm); err != nil || withheld != nil {
		t.Fatalf("two distinct buckets must pass the gate withholding nothing: withheld %v, err %v", withheld, err)
	}
	if usageLogsErr, logsErr := deployBothLokiStacks(context.Background(), cli, m, cm, &sources); usageLogsErr != nil || logsErr != nil {
		t.Fatalf("two ready stacks is the healthy case: usage logs %v, logs %v", usageLogsErr, logsErr)
	}

	assertTemplatePaths(t, sources, []string{
		UsageLogsLokiStackTemplate,
		UsageLogsLokiStackNPTemplate,
		UsageLogsOpenTelemetryCollectorTemplate,
		UsageLogsOpenTelemetryCollectorRBACTemplate,
		UsageLogsOpenTelemetryCollectorNPTemplate,
		LokiStackTemplate,
		LokiStackNetworkPolicyTemplate,
		LokiStackGatewayNetworkPolicyTemplate,
		ClusterLogForwarderTemplate,
		ClusterLogForwarderRBACTemplate,
		ClusterLogForwarderNetworkPolicyTemplate,
	})

	for _, conditionType := range []string{
		conditions.ConditionUsageLogsLokiStackAvailable,
		conditions.ConditionUsageLogsCollectorAvailable,
		conditions.ConditionLokiStackAvailable,
	} {
		if c := mustFindCondition(t, m, conditionType); c.Status != metav1.ConditionTrue {
			t.Errorf("%s: expected True once the stack is ready, got %s/%s", conditionType, c.Status, c.Reason)
		}
	}

	// The forwarder's stack is ready, but the forwarder itself has not been
	// applied yet in this cluster, so it reports its own startup state.
	if c := mustFindCondition(t, m, conditions.ConditionClusterLogForwarderAvailable); c.Status != metav1.ConditionFalse ||
		c.Reason != "ClusterLogForwarderNotReady" {
		t.Errorf("ClusterLogForwarderAvailable: expected False/ClusterLogForwarderNotReady, got %s/%s", c.Status, c.Reason)
	}
}

func TestDeployLokiStack_NeitherConfigured(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.LokiStack)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.UsageLogs = nil
	m.Spec.Logs = nil

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	if usageLogsErr, logsErr := deployBothLokiStacks(context.Background(), cli, m, cm, &sources); usageLogsErr != nil || logsErr != nil {
		t.Fatalf("unexpected errors: usage logs %v, logs %v", usageLogsErr, logsErr)
	}

	if len(sources) != 0 {
		t.Fatalf("expected no sources when neither feature is configured, got %d", len(sources))
	}

	// Both pipelines report NotConfigured down to their dependents: the fold
	// must not leave the collector's or the forwarder's condition unwritten.
	for conditionType, wantReason := range map[string]string{
		conditions.ConditionUsageLogsLokiStackAvailable:  "LokiNotRequired",
		conditions.ConditionUsageLogsCollectorAvailable:  "UsageLogsNotConfigured",
		conditions.ConditionLokiStackAvailable:           "LokiNotRequired",
		conditions.ConditionClusterLogForwarderAvailable: "LogsNotConfigured",
	} {
		c := mustFindCondition(t, m, conditionType)
		if c.Status != metav1.ConditionFalse || c.Severity != platformcommon.ConditionSeverityInfo {
			t.Errorf("%s: expected False+Info when not configured, got %s/%s", conditionType, c.Status, c.Severity)
		}
		if c.Reason != wantReason {
			t.Errorf("%s: expected reason %s, got %s", conditionType, wantReason, c.Reason)
		}
	}
}

func TestDeployLokiStack_CRDMissing(t *testing.T) {
	s := newActionsTestScheme(t)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{Storage: &v1alpha1.LokiStorageConfig{Type: "s3", SecretName: "usage-logs-secret"}}
	m.Spec.Logs = &v1alpha1.Logs{Storage: &v1alpha1.LokiStorageConfig{Type: "s3", SecretName: "logs-secret"}}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	// Each action asks for itself, so the absence has to come from the client:
	// the fake one answers an unstructured List for an unregistered kind with
	// an empty list rather than a NoKindMatchError, which hasCRD would read as
	// "installed, none created yet".
	cli := fake.NewClientBuilder().WithScheme(s).
		WithInterceptorFuncs(missingCRDs(gvk.LokiStack)).Build()
	if usageLogsErr, logsErr := deployBothLokiStacks(context.Background(), cli, m, cm, &sources); usageLogsErr != nil || logsErr != nil {
		t.Fatalf("a missing operator is a reported condition, not an error: usage logs %v, logs %v", usageLogsErr, logsErr)
	}

	if len(sources) != 0 {
		t.Fatalf("expected no sources when the LokiStack CRD is missing, got %d", len(sources))
	}

	// Both features are configured, so both must report the missing operator.
	for _, conditionType := range []string{
		conditions.ConditionUsageLogsLokiStackAvailable,
		conditions.ConditionLokiStackAvailable,
	} {
		c := mustFindCondition(t, m, conditionType)
		if c.Status != metav1.ConditionFalse || c.Reason != conditions.MissingOperatorReason {
			t.Errorf("%s: expected False/%s, got %s/%s", conditionType, conditions.MissingOperatorReason, c.Status, c.Reason)
		}
	}

	// The folded dependents still write their own verdicts. The collector
	// waits for an endpoint no stack can publish while the operator is
	// missing; the forwarder names the missing CRD for itself.
	if c := mustFindCondition(t, m, conditions.ConditionUsageLogsCollectorAvailable); c.Status != metav1.ConditionFalse ||
		c.Reason != "LokiStackNotReady" {
		t.Errorf("UsageLogsCollectorAvailable: expected False/LokiStackNotReady, got %s/%s", c.Status, c.Reason)
	}
	if c := mustFindCondition(t, m, conditions.ConditionClusterLogForwarderAvailable); c.Status != metav1.ConditionFalse ||
		c.Reason != "LokiStackCRDNotFound" {
		t.Errorf("ClusterLogForwarderAvailable: expected False/LokiStackCRDNotFound, got %s/%s", c.Status, c.Reason)
	}
}

// deployBothLokiStacks runs the two stack actions and returns both errors.
//
// The reconciler does not do this: there, the first action to fail aborts the
// round before the second runs, the same as for every other action. Running
// both unconditionally is a test affordance for cases about one action's own
// behaviour, where both outcomes need asserting in one pass. How the
// reconciler composes the gate with these two — and the fact that it stops at
// the first error — is covered at reconcile level, where that composition
// actually lives.
func deployBothLokiStacks(
	ctx context.Context,
	c client.Client,
	m *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
) (error, error) {
	usageLogsErr := deployUsageLogsLokiStack(ctx, c, m, cm, sources)
	logsErr := deployRHOAILogsLokiStack(ctx, c, m, cm, sources)
	return usageLogsErr, logsErr
}

// assertWithheldComponents pins the garbage collection shield the storage gate
// hands back. It is the gate's only output besides the conditions, and the
// set has to be exact in both directions: a missing component is a resource
// the collector deletes out from under a configuration the operator refused to
// render, and an extra one is a deletion the user asked for that never happens.
func assertWithheldComponents(t *testing.T, got map[string]struct{}, want ...string) {
	t.Helper()

	wantSet := make(map[string]struct{}, len(want))
	for _, component := range want {
		wantSet[component] = struct{}{}
	}
	if !maps.Equal(got, wantSet) {
		t.Errorf("withheld components: want %v, got %v", wantSet, got)
	}
}

// lokiStorageSecret builds a LokiStack S3 storage Secret pointing at a bucket
// on an endpoint.
func lokiStorageSecret(name, namespace, endpoint, bucketnames string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Data: map[string][]byte{
			"endpoint":    []byte(endpoint),
			"bucketnames": []byte(bucketnames),
		},
	}
}

// monitoringWithBothLokiStorage configures both features against the named
// storage Secrets.
func monitoringWithBothLokiStorage(usageLogsSecret, logsSecret string) *v1alpha1.Monitoring {
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{
		Storage: &v1alpha1.LokiStorageConfig{Type: "s3", SecretName: usageLogsSecret},
	}
	m.Spec.Logs = &v1alpha1.Logs{
		Storage: &v1alpha1.LokiStorageConfig{Type: "s3", SecretName: logsSecret},
	}
	return m
}

// TestCanonicalStorageLocation pins the canonicalization rules directly, as
// an endpoint and bucket in and a key or an error out.
//
// They are the subtlest code in the gate and decide every verdict it reaches:
// a rule that normalizes too little misses a conflict and lets two writers
// share a bucket, and one that normalizes too much invents a conflict and
// withholds a configuration that was fine. TestCheckStorageIsolation reaches
// them through two Secrets and a fake client, which states the rule under test
// only implicitly and reports a failure as "expected no conflict" several
// layers from the comparison that produced it.
//
// Only equality between outputs is meaningful — the strings are map keys, not
// an API — so the expectations here are written to make the rules legible
// rather than because any caller depends on the exact spelling.
func TestCanonicalStorageLocation(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		bucket   string
		want     string
		// wantErr asserts the pair is rejected rather than keyed: an address
		// that cannot name a location must not become one that compares as
		// isolated from the locations that do.
		wantErr bool
	}{
		{
			name:     "path-style endpoint",
			endpoint: "https://s3.example.com",
			bucket:   "logs",
			want:     "//s3.example.com/logs",
		},
		{
			// http and https are transports to one gateway, not separate
			// backends: every S3-compatible store serves the same buckets over
			// both. Keeping the scheme in the key would let two claimants reach
			// one bucket under two spellings and pass the comparison.
			name:     "the scheme is not part of the location",
			endpoint: "http://s3.example.com",
			bucket:   "logs",
			want:     "//s3.example.com/logs",
		},
		{
			// Hosts are case-insensitive (RFC 4343).
			name:     "the host is lowercased",
			endpoint: "HTTPS://S3.Example.COM",
			bucket:   "logs",
			want:     "//s3.example.com/logs",
		},
		{
			name:     "an explicitly written default https port is dropped",
			endpoint: "https://s3.example.com:443",
			bucket:   "logs",
			want:     "//s3.example.com/logs",
		},
		{
			name:     "an explicitly written default http port is dropped",
			endpoint: "http://s3.example.com:80",
			bucket:   "logs",
			want:     "//s3.example.com/logs",
		},
		{
			// The port rule cannot key off the scheme when the scheme is not in
			// the key: were :80 kept here and dropped under http, the two
			// spellings of one gateway would still land on different locations
			// and the scheme would go on separating them by proxy.
			name:     "a default web port is dropped whichever scheme carried it",
			endpoint: "https://s3.example.com:80",
			bucket:   "logs",
			want:     "//s3.example.com/logs",
		},
		{
			name:     "a non-default port is kept",
			endpoint: "https://s3.example.com:9000",
			bucket:   "logs",
			want:     "//s3.example.com:9000/logs",
		},
		{
			// The fold and the port have to agree on one spelling of the host:
			// the bucket label comes off the bare hostname, and the port goes
			// back on after it. This row pins that the two spellings of one
			// ported endpoint still meet in a single key, whichever order those
			// two steps run in.
			name:     "a virtual-host bucket on a non-default port is still folded out",
			endpoint: "https://logs.s3.example.com:9000",
			bucket:   "logs",
			want:     "//s3.example.com:9000/logs",
		},
		{
			name:     "a trailing slash is not a path prefix",
			endpoint: "https://s3.example.com/",
			bucket:   "logs",
			want:     "//s3.example.com/logs",
		},
		{
			name:     "repeated slashes collapse",
			endpoint: "https://s3.example.com//",
			bucket:   "logs",
			want:     "//s3.example.com/logs",
		},
		{
			name:     "dot segments resolve",
			endpoint: "https://gateway.internal/s3/../s3/",
			bucket:   "logs",
			want:     "//gateway.internal/s3/logs",
		},
		{
			// Self-hosted gateways route on the prefix and are free to treat
			// its case as significant, so it is left alone.
			name:     "path prefix case is preserved",
			endpoint: "https://gateway.internal/S3",
			bucket:   "logs",
			want:     "//gateway.internal/S3/logs",
		},
		{
			// Virtual-host-style addressing folds the bucket into the
			// hostname; stripping it is what makes that spelling comparable
			// to the path-style one above.
			name:     "a virtual-host bucket is folded out of the hostname",
			endpoint: "https://logs.s3.example.com",
			bucket:   "logs",
			want:     "//s3.example.com/logs",
		},
		{
			name:     "the virtual-host fold ignores bucket case",
			endpoint: "https://LOGS.s3.example.com",
			bucket:   "logs",
			want:     "//s3.example.com/logs",
		},
		{
			// The fold and the key read the same lowercased bucket, so an
			// uppercase bucketnames entry has to survive both. One lowercasing
			// site serves them; this is the row that notices if it stops.
			name:     "the virtual-host fold ignores bucket case written the other way",
			endpoint: "https://logs.s3.example.com",
			bucket:   "LOGS",
			want:     "//s3.example.com/logs",
		},
		{
			// The fold reads the hostname only; a gateway path prefix is not
			// part of what it strips, so it survives the virtual-host spelling
			// and keeps that form comparable to the path-style one under the
			// same prefix.
			name:     "the virtual-host fold leaves a gateway path prefix alone",
			endpoint: "https://logs.s3.example.com/custom/prefix",
			bucket:   "logs",
			want:     "//s3.example.com/custom/prefix/logs",
		},
		{
			// The fold matches on a whole leading label, so a host that merely
			// starts with the bucket name keeps its hostname intact. Stripping
			// here would collapse two distinct endpoints into one location and
			// report a conflict that does not exist.
			name:     "a hostname that only starts with the bucket name is untouched",
			endpoint: "https://logs-archive.s3.example.com",
			bucket:   "logs",
			want:     "//logs-archive.s3.example.com/logs",
		},
		{
			// An S3 bucket name is a DNS label, so a store that distinguishes
			// these two is out of spec. Folding the case costs a conflict that
			// could only be invented by an out-of-spec backend and buys one
			// that would otherwise be silent.
			name:     "bucket case is folded",
			endpoint: "https://s3.example.com",
			bucket:   "LOGS",
			want:     "//s3.example.com/logs",
		},
		{
			// url.Parse keeps the DNS root dot, and the fully qualified
			// spelling is the ordinary one for an in-cluster Service.
			name:     "the DNS root dot is stripped",
			endpoint: "https://minio.odh.svc.cluster.local.",
			bucket:   "logs",
			want:     "//minio.odh.svc.cluster.local/logs",
		},
		{
			// url.Parse strips the brackets from an IPv6 literal and
			// JoinHostPort only restores them when a port survives, so the two
			// spellings below differ in more than the port. Nothing reparses a
			// key, so a bracketless literal is only ever compared, and the two
			// forms of one address still agree with themselves.
			name:     "an IPv6 literal keeps its brackets only alongside a port",
			endpoint: "http://[::1]:9000",
			bucket:   "logs",
			want:     "//[::1]:9000/logs",
		},
		{
			name:     "an IPv6 literal on a default port loses its brackets",
			endpoint: "http://[::1]:80",
			bucket:   "logs",
			want:     "//::1/logs",
		},
		{
			name:     "surrounding whitespace is ignored",
			endpoint: "  https://s3.example.com  ",
			bucket:   "  logs  ",
			want:     "//s3.example.com/logs",
		},
		{
			// An endpoint url.Parse rejects cannot be normalized, and a key
			// built from the raw string cannot be checked against anything.
			// Rejecting it is the fail-closed answer: two identically
			// misspelled endpoints used to conflict through the fallback key;
			// now the Secret is reported as invalid instead, which names the
			// thing the user actually has to fix.
			name:     "an unparseable endpoint is rejected",
			endpoint: "https://exa\x7fmple.com",
			bucket:   "logs",
			wantErr:  true,
		},
		{
			// The regression this row guards: an empty endpoint once parsed as
			// an empty authority and keyed to "///logs", which no valid
			// endpoint produces, so it compared as isolated from every real
			// one beside it.
			name:     "an empty endpoint is rejected",
			endpoint: "",
			bucket:   "logs",
			wantErr:  true,
		},
		{
			name:     "a whitespace-only endpoint is rejected",
			endpoint: "   ",
			bucket:   "logs",
			wantErr:  true,
		},
		{
			// Parses, and to a path — but a location without a host is the
			// same "///" key an empty endpoint produced, reached by another
			// spelling.
			name:     "a hostless endpoint is rejected",
			endpoint: "/just-a-path",
			bucket:   "logs",
			wantErr:  true,
		},
		{
			// The spelling the Loki operator takes for an in-cluster gateway.
			// With no scheme in the key it needs no special handling: it is
			// already the same location as either spelling that names it, which
			// is what the first two rows above assert from the other side.
			name:     "a scheme-less endpoint is the same location as either spelling of it",
			endpoint: "s3.example.com",
			bucket:   "logs",
			want:     "//s3.example.com/logs",
		},
		{
			// A bare host with a standard port, which is only equal to the
			// scheme-ful spellings because the port rule ignores the scheme.
			name:     "a scheme-less endpoint on a default web port drops it",
			endpoint: "s3.example.com:443",
			bucket:   "logs",
			want:     "//s3.example.com/logs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := canonicalStorageLocation(tt.endpoint, tt.bucket)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("canonicalStorageLocation(%q, %q): expected an error, got key %q",
						tt.endpoint, tt.bucket, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("canonicalStorageLocation(%q, %q): unexpected error: %v",
					tt.endpoint, tt.bucket, err)
			}
			if got != tt.want {
				t.Errorf("canonicalStorageLocation(%q, %q):\n want %q\n  got %q",
					tt.endpoint, tt.bucket, tt.want, got)
			}
		})
	}
}

// TestCanonicalStorageLocationDoesNotEchoCredentials pins the security property
// of the two error returns above. The endpoint is Secret data and may carry
// credentials in its userinfo, and these errors surface in status conditions
// and operator logs that far more principals can read than the Secret itself —
// echoing them there would hand out the credentials to anyone who can `kubectl
// get monitoring`. The wrapper in lokiStorageLocations is what keeps the
// message diagnosable instead: it names the Secret, never its contents.
func TestCanonicalStorageLocationDoesNotEchoCredentials(t *testing.T) {
	t.Parallel()

	const (
		accessKey = "AKIAIOSFODNN7EXAMPLE"
		password  = "wJalrXUtnFEMI7K7MDENGsecret"
	)
	credentialed := "https://" + accessKey + ":" + password + "@"
	for name, endpoint := range map[string]string{
		// url.Parse rejects the control character. Its own error message
		// quotes the raw URL it failed on, which is why that cause is not
		// wrapped into ours.
		"unparseable": credentialed + "exa\x7fmple.com:9000",
		// Parses, but to a path: the hostless branch.
		"hostless": credentialed + "/just-a-path",
	} {
		_, err := canonicalStorageLocation(endpoint, "logs")
		if err == nil {
			t.Fatalf("%s endpoint: expected an error", name)
		}
		for _, credential := range []string{accessKey, password} {
			if strings.Contains(err.Error(), credential) {
				t.Errorf("%s endpoint: error must not echo the endpoint's credentials, got %q", name, err)
			}
		}
	}
}

// TestLokiStorageLocationsDoesNotEchoCredentials is the same property one
// layer up, at the boundary the condition message actually renders: the
// message must name the Secret so its owner can fix it, while the credentials
// embedded in that Secret's endpoint stay in the Secret.
func TestLokiStorageLocationsDoesNotEchoCredentials(t *testing.T) {
	t.Parallel()

	const (
		ns         = "test-ns"
		accessKey  = "AKIAIOSFODNN7EXAMPLE"
		password   = "wJalrXUtnFEMI7K7MDENGsecret"
		secretName = "usage-logs-secret"
	)
	s := newActionsTestScheme(t)
	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(
		lokiStorageSecret(secretName, ns,
			"https://"+accessKey+":"+password+"@exa\x7fmple.com:9000", "logs"),
	).Build()

	_, err := lokiStorageLocations(context.Background(), cli, ns, secretName)
	if err == nil {
		t.Fatal("expected the unparseable endpoint to fail the resolution")
	}
	if !strings.Contains(err.Error(), secretName) {
		t.Errorf("error must name the Secret so its owner can fix it, got %q", err)
	}
	for _, credential := range []string{accessKey, password} {
		if strings.Contains(err.Error(), credential) {
			t.Errorf("error must not echo the endpoint's credentials, got %q", err)
		}
	}
}

// storageIsolationTest is one checkStorageIsolation case: the Secrets that
// exist, the spec that claims them, and the verdict expected of the two.
type storageIsolationTest struct {
	name         string
	secrets      []client.Object
	monitoring   *v1alpha1.Monitoring
	wantConflict bool
	wantBuckets  string
	// wantCheckReason names the reason a *storageCheckError must
	// carry. Set it for the cases where the question could not be
	// answered, as opposed to answered with a conflict.
	wantCheckReason string
}

// TestCheckStorageIsolation covers what the comparison does with the claims it
// is given: which overlaps are conflicts, which are not, and which questions it
// refuses to answer. How an endpoint is spelled is the companion test's.
func TestCheckStorageIsolation(t *testing.T) {
	const ns = "test-ns"

	runStorageIsolationTests(t, ns, []storageIsolationTest{
		{
			// Distinct Secret names are not evidence of separation.
			name: "same bucket through different secrets conflicts",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://s3.example.com", "shared-bucket"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "shared-bucket"),
			},
			monitoring:   monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantConflict: true,
			wantBuckets:  "//s3.example.com/shared-bucket",
		},
		{
			name: "the same secret for both stacks conflicts",
			secrets: []client.Object{
				lokiStorageSecret("one-secret", ns, "https://s3.example.com", "shared-bucket"),
			},
			monitoring:   monitoringWithBothLokiStorage("one-secret", "one-secret"),
			wantConflict: true,
			wantBuckets:  "//s3.example.com/shared-bucket",
		},
		{
			// bucketnames is a list, so an overlap anywhere in it conflicts.
			name: "overlap inside a bucket list conflicts",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://s3.example.com", "usage-bucket, shared-bucket"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "shared-bucket,logs-bucket"),
			},
			monitoring:   monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantConflict: true,
			wantBuckets:  "//s3.example.com/shared-bucket",
		},
		{
			name: "separate buckets are accepted",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://s3.example.com", "usage-logs-bucket"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "logs-bucket"),
			},
			monitoring: monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
		},
		{
			// The same bucket name on two backends is two different buckets.
			name: "same bucket name on different endpoints is accepted",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://s3.example.com", "logs"),
				lokiStorageSecret("logs-secret", ns, "https://s3.other.example.com", "logs"),
			},
			monitoring: monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
		},
		{
			// Fails closed. A Secret that does not exist yet resolves to no
			// locations, and "no locations" compares as isolated against
			// anything — so treating it as "cannot be shown to conflict"
			// would let both stacks be created now and collide later, once
			// the Secret lands pointing at the other stack's bucket.
			name: "one missing secret withholds both stacks",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://s3.example.com", "shared-bucket"),
			},
			monitoring:      monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantCheckReason: storageSecretMissingReason,
		},
		{
			// The GitOps ordering case, and the one the fail-open bug was
			// actually reachable through: the CR is applied before either
			// Secret exists, so neither resolves and nothing can be compared.
			name:            "both secrets missing withholds both stacks",
			monitoring:      monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantCheckReason: storageSecretMissingReason,
		},
		{
			// Present but useless: same zero-locations result as an absent
			// Secret, so it has to fail closed the same way.
			name: "secret without a bucketnames key withholds both stacks",
			secrets: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: "usage-logs-secret", Namespace: ns},
					Data:       map[string][]byte{"endpoint": []byte("https://s3.example.com")},
				},
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "logs-bucket"),
			},
			monitoring:      monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantCheckReason: storageSecretInvalidReason,
		},
		{
			// Every entry trims away to nothing, so the list parses to zero
			// buckets rather than to a bucket named "" or " ".
			name: "secret with a whitespace-only bucketnames withholds both stacks",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://s3.example.com", "  ,  "),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "logs-bucket"),
			},
			monitoring:      monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantCheckReason: storageSecretInvalidReason,
		},
		{
			// The regression this row guards: an endpoint that is empty — here
			// because the Secret never carried the key — once keyed to
			// "///bucket", which no valid endpoint produces, so it compared as
			// isolated from the real endpoint beside it and both stacks passed
			// the gate on one bucket. It fails closed now, under the reason
			// that names the Secret as the thing to fix.
			name: "secret without an endpoint key withholds both stacks",
			secrets: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: "usage-logs-secret", Namespace: ns},
					Data:       map[string][]byte{"bucketnames": []byte("shared-bucket")},
				},
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "shared-bucket"),
			},
			monitoring:      monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantCheckReason: storageSecretInvalidReason,
		},
		{
			name: "secret with a whitespace-only endpoint withholds both stacks",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "   ", "shared-bucket"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "shared-bucket"),
			},
			monitoring:      monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantCheckReason: storageSecretInvalidReason,
		},
		{
			// An endpoint url.Parse rejects is the same question unanswered: it
			// can be shown to be neither conflicting nor isolated, so it
			// withholds rather than vouches. The reason says the Secret, not
			// the cluster, is what to fix.
			name: "secret with an unparseable endpoint withholds both stacks",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://exa\x7fmple.com", "shared-bucket"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "shared-bucket"),
			},
			monitoring:      monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantCheckReason: storageSecretInvalidReason,
		},
		{
			// Failing closed is scoped to the comparison. With one stack
			// there is no second writer, so a missing Secret is the Loki
			// operator's to report and this check stays out of the way.
			name: "a single stack with a missing secret is not withheld",
			monitoring: func() *v1alpha1.Monitoring {
				m := monitoringWithBothLokiStorage("usage-logs-secret", "usage-logs-secret")
				m.Spec.Logs = nil
				return m
			}(),
		},
		{
			// With one stack there is no second writer to collide with, so the
			// check must stay out of the way however the storage is configured.
			name: "only usage logs configured is never a conflict",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://s3.example.com", "shared-bucket"),
			},
			monitoring: func() *v1alpha1.Monitoring {
				m := monitoringWithBothLokiStorage("usage-logs-secret", "usage-logs-secret")
				m.Spec.Logs = nil
				return m
			}(),
		},
		{
			name: "only log forwarding configured is never a conflict",
			secrets: []client.Object{
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "shared-bucket"),
			},
			monitoring: func() *v1alpha1.Monitoring {
				m := monitoringWithBothLokiStorage("logs-secret", "logs-secret")
				m.Spec.UsageLogs = nil
				return m
			}(),
		},
	})
}

// TestCheckStorageIsolation_EndpointSpellings covers the same comparison over
// endpoints that are written differently and mean the same thing, or written
// alike and mean different things.
//
// TestCanonicalStorageLocation pins these rules directly, as string in and
// string out. They are driven through the Secret here as well because that is
// the path the gate actually takes: these rows are what says a rule survives
// two Secrets, a bucket list and a fake client, and they are written as pairs
// of real spellings rather than as keys.
func TestCheckStorageIsolation_EndpointSpellings(t *testing.T) {
	const ns = "test-ns"

	runStorageIsolationTests(t, ns, []storageIsolationTest{
		{
			// Equivalent spellings of one endpoint must not look like two
			// backends: a trailing slash, hostname case, an explicitly written
			// default port and a doubled slash all name the same place.
			name: "equivalent endpoint spellings still conflict",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://S3.Example.com:443//", "shared-bucket"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "shared-bucket"),
			},
			monitoring:   monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantConflict: true,
			wantBuckets:  "//s3.example.com/shared-bucket",
		},
		{
			// Virtual-host-style addressing folds the bucket into the hostname;
			// it still reaches the bucket the path-style spelling reaches.
			name: "virtual-host and path-style addressing of one bucket conflict",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://shared-bucket.s3.example.com", "shared-bucket"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "shared-bucket"),
			},
			monitoring:   monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantConflict: true,
			wantBuckets:  "//s3.example.com/shared-bucket",
		},
		{
			// The same fold with a port in play. The bucket label comes off the
			// bare hostname and the port goes back on after it; this row pins
			// that the ported spellings of one endpoint still meet in a single
			// key, so the fold cannot come to depend on how the ported host is
			// written.
			name: "virtual-host and path-style addressing on a non-default port conflict",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://shared-bucket.s3.example.com:9000", "shared-bucket"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com:9000", "shared-bucket"),
			},
			monitoring:   monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantConflict: true,
			wantBuckets:  "//s3.example.com:9000/shared-bucket",
		},
		{
			// A non-default port is a different backend, not a spelling variant.
			name: "same bucket on a non-default port is accepted",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://s3.example.com:9000", "logs"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "logs"),
			},
			monitoring: monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
		},
		{
			// The case this gate was reopened for. http and https are transports
			// to one gateway, not separate backends — MinIO, Ceph RGW, ODF and
			// S3 all serve the same buckets over both — so two claimants written
			// this way are two writers on one bucket.
			name: "same bucket over http and https conflicts",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "http://s3.example.com", "shared-bucket"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "shared-bucket"),
			},
			monitoring:   monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantConflict: true,
			wantBuckets:  "//s3.example.com/shared-bucket",
		},
		{
			// The same thing on a port the scheme does not imply, where no
			// default-port rule can be doing the work: only ignoring the scheme
			// brings these together.
			name: "same bucket over http and https on one non-default port conflicts",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "http://minio.odh.svc:9000", "shared-bucket"),
				lokiStorageSecret("logs-secret", ns, "https://minio.odh.svc:9000", "shared-bucket"),
			},
			monitoring:   monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantConflict: true,
			wantBuckets:  "//minio.odh.svc:9000/shared-bucket",
		},
		{
			// Gateway path prefixes are case-sensitive, so they separate buckets.
			name: "same bucket under different gateway path prefixes is accepted",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://gateway.internal/s3", "logs"),
				lokiStorageSecret("logs-secret", ns, "https://gateway.internal/S3", "logs"),
			},
			monitoring: monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
		},
		{
			// Writing the scheme is optional, so one claimant can name the
			// endpoint bare and the other name it in full. Both reach the same
			// gateway, and with no scheme in the key they say so without the
			// gate having to decide which scheme the bare one meant.
			name: "a scheme-less endpoint conflicts with the https spelling of it",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "s3.example.com", "shared-bucket"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "shared-bucket"),
			},
			monitoring:   monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantConflict: true,
			wantBuckets:  "//s3.example.com/shared-bucket",
		},
		{
			// The other half of it: http is as likely as https for a bare
			// in-cluster endpoint, and neither spelling is privileged.
			name: "a scheme-less endpoint conflicts with the http spelling of it",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "s3.example.com", "shared-bucket"),
				lokiStorageSecret("logs-secret", ns, "http://s3.example.com", "shared-bucket"),
			},
			monitoring:   monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantConflict: true,
			wantBuckets:  "//s3.example.com/shared-bucket",
		},
		{
			name: "two scheme-less spellings of one endpoint conflict",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "s3.example.com", "shared-bucket"),
				lokiStorageSecret("logs-secret", ns, "S3.Example.com", "shared-bucket"),
			},
			monitoring:   monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantConflict: true,
			wantBuckets:  "//s3.example.com/shared-bucket",
		},
		{
			// A bare host with a standard port is only equal to the scheme-ful
			// spelling because the port rule stopped keying off the scheme.
			name: "a scheme-less endpoint on a default web port conflicts with the https spelling",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "s3.example.com:443", "shared-bucket"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "shared-bucket"),
			},
			monitoring:   monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantConflict: true,
			wantBuckets:  "//s3.example.com/shared-bucket",
		},
		{
			// The row that bounds all of the above. Folding spellings together
			// must not make claimants overlap that never did: a collision needs
			// the bucket to match too, and a bucket is named the same way
			// whichever scheme reaches it.
			name: "a scheme-less endpoint on a different bucket is accepted",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "s3.example.com", "usage-logs-bucket"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "logs-bucket"),
			},
			monitoring: monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
		},
		{
			name: "a padded scheme-less endpoint conflicts with the spelling of it",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "  s3.example.com  ", "shared-bucket"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "shared-bucket"),
			},
			monitoring:   monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantConflict: true,
			wantBuckets:  "//s3.example.com/shared-bucket",
		},
		{
			// The spelling this is actually about: an in-cluster gateway
			// addressed by Service name and port, with no scheme in sight.
			name: "a scheme-less in-cluster endpoint conflicts across the port",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "minio.odh.svc:9000", "shared-bucket"),
				lokiStorageSecret("logs-secret", ns, "http://minio.odh.svc:9000", "shared-bucket"),
			},
			monitoring:   monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantConflict: true,
			wantBuckets:  "//minio.odh.svc:9000/shared-bucket",
		},
		{
			// The fully qualified spelling of an in-cluster Service carries a
			// DNS root dot that url.Parse keeps and the two forms are one host.
			name: "an FQDN with a root dot conflicts with the spelling without one",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "http://minio.odh.svc.cluster.local.:9000", "shared-bucket"),
				lokiStorageSecret("logs-secret", ns, "http://minio.odh.svc.cluster.local:9000", "shared-bucket"),
			},
			monitoring:   monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantConflict: true,
			wantBuckets:  "//minio.odh.svc.cluster.local:9000/shared-bucket",
		},
		{
			// An S3 bucket name is a DNS label, so these name one bucket on any
			// backend that accepts both at all.
			name: "the same bucket spelled in different cases conflicts",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://s3.example.com", "SHARED-BUCKET"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "shared-bucket"),
			},
			monitoring:   monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantConflict: true,
			wantBuckets:  "//s3.example.com/shared-bucket",
		},
	})
}

// TestCheckStorageIsolation_BucketLists covers bucketnames being a list rather
// than a name: entries that resolve to nothing, and entries that resolve to
// each other.
//
// A conflict is two claimants on one location, and the comparison reads that
// off how many claimants a location collected. A claimant that names one
// bucket twice puts two entries under one key by itself, so without a dedupe
// the count stops meaning "writers" and a stack is reported as conflicting
// with itself — a withhold of both LokiStacks, under a message that can only
// name one party, for a repeated word in a Secret.
//
// The repetition does not have to be visible to whoever wrote the list.
// Canonicalization folds bucket case and trims whitespace, so entries that
// read as two buckets in the YAML can arrive as one.
func TestCheckStorageIsolation_BucketLists(t *testing.T) {
	const ns = "test-ns"

	runStorageIsolationTests(t, ns, []storageIsolationTest{
		{
			// Empty entries in a bucket list are skipped rather than claimed as
			// a location with no bucket, which every other list would overlap.
			name: "empty entries in a bucket list are ignored",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://s3.example.com", "logs-a, , shared-bucket"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", ",,shared-bucket,"),
			},
			monitoring:   monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantConflict: true,
			wantBuckets:  "//s3.example.com/shared-bucket",
		},
		{
			// The plain form: a list repeating a bucket is one claim on it.
			name: "a bucket repeated in one list is not a conflict with itself",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://s3.example.com", "bucket-a,bucket-a"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "bucket-b"),
			},
			monitoring: monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
		},
		{
			// A repetition only the canonicalizer can see: these are two
			// distinct strings in the Secret and one bucket after folding.
			name: "a bucket repeated in one list in another case is not a conflict with itself",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://s3.example.com", "Bucket-A,bucket-a"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "bucket-b"),
			},
			monitoring: monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
		},
		{
			// The same, folded by the trim that makes a list readable.
			name: "a bucket repeated in one list around whitespace is not a conflict with itself",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://s3.example.com", "bucket-a, bucket-a "),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "bucket-b"),
			},
			monitoring: monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
		},
		{
			// The row that bounds the three above. Ignoring a claimant's own
			// repetition must not cost the bucket its claim: the second
			// claimant reaching the repeated bucket is still two writers on
			// it, which is the conflict the gate exists for.
			name: "a bucket repeated in one list still conflicts with another claimant on it",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://s3.example.com", "shared-bucket,shared-bucket"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "shared-bucket"),
			},
			monitoring:   monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantConflict: true,
			wantBuckets:  "//s3.example.com/shared-bucket",
		},
	})
}

// TestCheckStorageIsolation_ThreeClaimants covers the comparison being
// all-against-all rather than neighbour-against-neighbour.
//
// checkStorageIsolation keeps a claimedBy map so a third claimant is detected
// against both of the first two, and nothing else reaches that: storageClaims
// yields at most two claims today, so the tables above cannot produce a third
// however their Secrets are written. This calls the comparison directly with
// claims a future component would supply, which is also what says the map is
// keyed on the location rather than on the claimant.
//
// The claimants that do not overlap have to stay out of the error as well as
// out of the verdict. validateStorageIsolation withholds every claim it was
// given once any of them conflict — that is deliberate, since the gate cannot
// tell which writer to prefer — but the message is what the user reads to find
// the two buckets to separate, and naming an innocent third sends them to the
// wrong Secret.
func TestCheckStorageIsolation_ThreeClaimants(t *testing.T) {
	const ns = "test-ns"

	s := newActionsTestScheme(t)
	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(
		lokiStorageSecret("first-secret", ns, "https://s3.example.com", "shared-bucket"),
		lokiStorageSecret("second-secret", ns, "https://s3.example.com", "its-own-bucket"),
		// Spelled differently from the first on purpose: the third claimant
		// has to be compared against the first through canonicalization, not
		// by matching the Secret or the literal endpoint.
		lokiStorageSecret("third-secret", ns, "shared-bucket.s3.example.com:443", "shared-bucket"),
	).Build()

	claims := []storageClaim{
		{description: "first", secretName: "first-secret"},
		{description: "second", secretName: "second-secret"},
		{description: "third", secretName: "third-secret"},
	}

	err := checkStorageIsolation(context.Background(), cli, ns, claims)

	var conflict *storageConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("expected a *storageConflictError, got %T (%v)", err, err)
	}

	// The first and third overlap across the second, which is the ordering the
	// claimedBy map exists for: a comparison that only looked at the preceding
	// claimant would find nothing here.
	if want := []string{"first", "third"}; !slices.Equal(conflict.claimants, want) {
		t.Errorf("claimants: want %v, got %v", want, conflict.claimants)
	}
	if want := []string{"//s3.example.com/shared-bucket"}; !slices.Equal(conflict.shared, want) {
		t.Errorf("shared locations: want %v, got %v", want, conflict.shared)
	}
	if strings.Contains(err.Error(), "second") {
		t.Errorf("the isolated claimant should not be named in the conflict, got %v", err)
	}
}

// runStorageIsolationTests drives the cases of both tables above. The three
// outcomes it distinguishes are the whole point of the check: isolated,
// conflicting, and unanswerable.
func runStorageIsolationTests(t *testing.T, ns string, tests []storageIsolationTest) {
	t.Helper()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newActionsTestScheme(t)
			cli := fake.NewClientBuilder().WithScheme(s).WithObjects(tt.secrets...).Build()

			err := checkStorageIsolation(context.Background(), cli, ns, storageClaims(tt.monitoring))

			// Unresolvable storage is a third outcome, distinct from both
			// "isolated" and "conflicting": nothing was compared, so the
			// stacks are withheld under a reason that says what to fix
			// rather than one that reads as an operator malfunction.
			if tt.wantCheckReason != "" {
				var checkErr *storageCheckError
				if !errors.As(err, &checkErr) {
					t.Fatalf("expected a *storageCheckError, got %T (%v)", err, err)
				}
				if got := checkErr.conditionReason(); got != tt.wantCheckReason {
					t.Errorf("reason: want %s, got %s", tt.wantCheckReason, got)
				}
				if checkErr.conditionStatus() != metav1.ConditionUnknown {
					t.Errorf("status: an unanswered question is Unknown, got %s", checkErr.conditionStatus())
				}
				return
			}

			if !tt.wantConflict {
				if err != nil {
					t.Fatalf("expected no conflict, got %v", err)
				}
				return
			}

			// A rejected configuration, not a check that could not run: the
			// two are reported under different reasons and only one of them
			// is the user's to fix.
			var conflict *storageConflictError
			if !errors.As(err, &conflict) {
				t.Fatalf("expected a *storageConflictError, got %T (%v)", err, err)
			}
			if !slices.Contains(conflict.shared, tt.wantBuckets) {
				t.Errorf("conflict should include the shared location %q, got %v", tt.wantBuckets, conflict.shared)
			}
		})
	}
}

func TestStorageConflictDoesNotExposeSecretLocation(t *testing.T) {
	const namespace = "test-ns"
	s := newActionsTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Namespace = namespace
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{Storage: &v1alpha1.LokiStorageConfig{SecretName: "usage-secret"}}
	m.Spec.Logs = &v1alpha1.Logs{Storage: &v1alpha1.LokiStorageConfig{SecretName: "logs-secret"}}
	endpoint := "https://user:password@s3.example.com/private-token"
	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(
		lokiStorageSecret("usage-secret", namespace, endpoint, "shared-bucket"),
		lokiStorageSecret("logs-secret", namespace, endpoint, "shared-bucket"),
	).Build()
	_, err := validateStorageIsolation(context.Background(), cli, m, conditions.NewConditionsManager(m, m.Generation))
	if err == nil {
		t.Fatal("expected storage conflict")
	}
	for _, message := range []string{err.Error(),
		findCondition(m, conditions.ConditionLokiStackAvailable).Message,
		findCondition(m, conditions.ConditionUsageLogsLokiStackAvailable).Message} {
		for _, secret := range []string{"password", "private-token", "s3.example.com", "shared-bucket"} {
			if strings.Contains(message, secret) {
				t.Errorf("conflict message exposes secret location: %q", message)
			}
		}
	}
}

// TestValidateStorageIsolation_SharedBucketRejected covers the conflict being
// refused, reported on both claimants, and handed back as a shield over both
// of their components.
//
// That the stack actions do not run past this is the reconciler's part, in
// TestReconcile_StorageConflictWithholdsBothStacks; keeping the already
// running stacks alive is garbage collection's, in
// TestStorageConflictShieldsLokiFromGarbageCollection.
func TestValidateStorageIsolation_SharedBucketRejected(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.LokiStack)

	m := monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret")

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(
		lokiStorageSecret("usage-logs-secret", m.Spec.Namespace, "https://s3.example.com", "shared-bucket"),
		lokiStorageSecret("logs-secret", m.Spec.Namespace, "https://s3.example.com", "shared-bucket"),
	).Build()

	cm := conditions.NewConditionsManager(m, m.Generation)

	withheld, err := validateStorageIsolation(context.Background(), cli, m, cm)
	if err == nil {
		t.Fatal("expected validateStorageIsolation to reject the shared bucket")
	}

	// The typed error survives the caller boundary, so the reconciler can tell
	// a rejected configuration from a check that could not run.
	var conflict *storageConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("expected a *storageConflictError, got %T (%v)", err, err)
	}

	assertWithheldComponents(t, withheld, componentLokiUsageLogs, componentLokiLogs)

	for _, conditionType := range []string{
		conditions.ConditionUsageLogsLokiStackAvailable,
		conditions.ConditionLokiStackAvailable,
	} {
		// False, unlike the unreadable-Secret case below: the buckets were
		// compared and found to overlap. That is a determinate answer and the
		// user's to fix, so it degrades rather than reporting Unknown.
		c := mustFindCondition(t, m, conditionType)
		if c.Status != metav1.ConditionFalse || c.Reason != storageConflictReason {
			t.Errorf("%s: expected False/%s, got %s/%s", conditionType, storageConflictReason, c.Status, c.Reason)
		}
	}
}

// TestValidateStorageIsolation_StorageSecretUnreadableWithholdsBothStacks covers
// the isolation check failing closed. An unreadable Secret leaves the overlap
// question unanswered, and answering "no conflict" would let two stacks share a
// bucket and overwrite each other's index and chunks.
func TestValidateStorageIsolation_StorageSecretUnreadableWithholdsBothStacks(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.LokiStack)

	m := monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret")

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(
		lokiStorageSecret("usage-logs-secret", m.Spec.Namespace, "https://s3.example.com", "shared-bucket"),
		lokiStorageSecret("logs-secret", m.Spec.Namespace, "https://s3.example.com", "shared-bucket"),
	).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, isSecret := obj.(*corev1.Secret); isSecret {
				return k8serr.NewForbidden(
					schema.GroupResource{Resource: "secrets"}, key.Name, errors.New("no permission"))
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}).Build()

	cm := conditions.NewConditionsManager(m, m.Generation)

	withheld, err := validateStorageIsolation(context.Background(), cli, m, cm)
	if err == nil {
		t.Fatal("expected validateStorageIsolation to fail closed when the storage secret cannot be read")
	}

	var checkErr *storageCheckError
	if !errors.As(err, &checkErr) {
		t.Fatalf("expected a *storageCheckError, got %T (%v)", err, err)
	}
	// The API error stays reachable through the wrapper, so callers can still
	// tell a permission problem from any other reason the check did not run.
	if !k8serr.IsForbidden(err) {
		t.Errorf("expected the Forbidden cause to remain unwrappable, got %v", err)
	}

	// Both stacks are withheld, so both are shielded — an unanswerable question
	// withholds exactly as much as a rejected answer does.
	assertWithheldComponents(t, withheld, componentLokiUsageLogs, componentLokiLogs)

	for _, conditionType := range []string{
		conditions.ConditionUsageLogsLokiStackAvailable,
		conditions.ConditionLokiStackAvailable,
	} {
		// Unknown, not False: the Secret could not be read, so nothing was
		// established about either stack. False would clear the published
		// endpoint and take the collector down with it, which is a heavy
		// response to a permission blip. The stacks are withheld either way.
		c := mustFindCondition(t, m, conditionType)
		if c.Status != metav1.ConditionUnknown || c.Reason != storageCheckFailedReason {
			t.Errorf("%s: expected Unknown/%s, got %s/%s", conditionType, storageCheckFailedReason, c.Status, c.Reason)
		}
	}
}

// TestValidateStorageIsolation_MissingStorageSecretsWithholdBothStacks is the CWE-636
// regression: the Monitoring CR is applied before its storage Secrets, which
// is the ordinary GitOps case because apply order is not guaranteed.
//
// Neither Secret resolves, so neither stack has a comparable location. Reading
// that as "cannot be shown to conflict" created both stacks immediately; the
// Secrets then landed pointing at one bucket, the Loki operator — which does
// watch Secrets — started both, and they overwrote each other's index and
// chunks. By the time the next reconcile noticed, withholding the render could
// not undo it, because the GC shield deliberately keeps the live stacks.
// Nothing may be created until both Secrets resolve.
func TestValidateStorageIsolation_MissingStorageSecretsWithholdBothStacks(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.LokiStack)

	m := monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret")

	// No Secrets, and both LokiStacks already ready: readiness is not the
	// reason anything is withheld here, and leaving them out would let the
	// test pass for the wrong reason.
	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(
		kubernetesAPIServerEndpointSlice(),
		readyLokiStack(usageLogsLokiStackResourceName, m.Spec.Namespace),
		readyLokiStack(lokiStackResourceName, m.Spec.Namespace),
	).Build()

	cm := conditions.NewConditionsManager(m, m.Generation)

	withheld, err := validateStorageIsolation(context.Background(), cli, m, cm)
	if err == nil {
		t.Fatal("expected validateStorageIsolation to fail closed while the storage secrets do not exist")
	}

	var checkErr *storageCheckError
	if !errors.As(err, &checkErr) {
		t.Fatalf("expected a *storageCheckError, got %T (%v)", err, err)
	}
	if !k8serr.IsNotFound(err) {
		t.Errorf("the NotFound cause must stay unwrappable, got %v", err)
	}

	// The decisive assertion is that the error reaches the reconciler at all:
	// a stack that is never rendered is never created, which is the only point
	// at which this is preventable. That the reconciler respects it is
	// TestReconcile_StorageConflictWithholdsBothStacks.
	assertWithheldComponents(t, withheld, componentLokiUsageLogs, componentLokiLogs)

	for _, conditionType := range []string{
		conditions.ConditionUsageLogsLokiStackAvailable,
		conditions.ConditionLokiStackAvailable,
	} {
		// Unknown with a reason that names the actual problem. False would
		// clear the published endpoint and take the collector down over a
		// Secret that is simply still on its way.
		c := mustFindCondition(t, m, conditionType)
		if c.Status != metav1.ConditionUnknown || c.Reason != storageSecretMissingReason {
			t.Errorf("%s: expected Unknown/%s, got %s/%s",
				conditionType, storageSecretMissingReason, c.Status, c.Reason)
		}
	}
}

// TestValidateStorageIsolation_EmptyEndpointWithholdsBothStacks is the
// endpoint half of the fail-closed rule the test above covers for missing
// Secrets: a Secret that exists but carries no endpoint.
//
// The empty endpoint once canonicalized to "///bucket" — a key no valid
// endpoint produces — so it compared as isolated from the real endpoint beside
// it and both stacks were created on one bucket. The reason the conditions
// carry is the point of the assertions: StorageSecretInvalid, not
// StorageCheckFailed, because the user fixes this by editing the Secret, not
// by fixing cluster access.
func TestValidateStorageIsolation_EmptyEndpointWithholdsBothStacks(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.LokiStack)

	m := monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret")

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "usage-logs-secret", Namespace: m.Spec.Namespace},
			Data:       map[string][]byte{"bucketnames": []byte("shared-bucket")},
		},
		lokiStorageSecret("logs-secret", m.Spec.Namespace, "https://s3.example.com", "shared-bucket"),
	).Build()

	cm := conditions.NewConditionsManager(m, m.Generation)

	withheld, err := validateStorageIsolation(context.Background(), cli, m, cm)
	if err == nil {
		t.Fatal("expected validateStorageIsolation to fail closed on a Secret with no endpoint")
	}

	var checkErr *storageCheckError
	if !errors.As(err, &checkErr) {
		t.Fatalf("expected a *storageCheckError, got %T (%v)", err, err)
	}
	if got := checkErr.conditionReason(); got != storageSecretInvalidReason {
		t.Errorf("reason: want %s, got %s", storageSecretInvalidReason, got)
	}

	assertWithheldComponents(t, withheld, componentLokiUsageLogs, componentLokiLogs)

	for _, conditionType := range []string{
		conditions.ConditionUsageLogsLokiStackAvailable,
		conditions.ConditionLokiStackAvailable,
	} {
		// Unknown, not False: nothing was established about either stack, and
		// False would clear the published endpoint over a Secret whose
		// contents the user has to complete.
		c := mustFindCondition(t, m, conditionType)
		if c.Status != metav1.ConditionUnknown || c.Reason != storageSecretInvalidReason {
			t.Errorf("%s: expected Unknown/%s, got %s/%s",
				conditionType, storageSecretInvalidReason, c.Status, c.Reason)
		}
	}
}

// TestDeployLokiStack_UnobservableStateReportsUnknown covers the API calls in
// the stack actions that can fail without telling us anything about the stacks.
//
// The caller holds these errors instead of returning them, so the reconcile
// runs on to publish endpoints and aggregate Ready from whatever the conditions
// say. Returning without marking would leave the previous round's True standing
// and report a healthy stack on the strength of a check that never ran. Unknown
// rather than False, because the condition decides whether the published
// endpoint is kept, and False would clear it — taking the usage logs collector
// and its policies down with it over one failed call.
//
// The templates are rendered anyway, and that is the point. A readiness check
// is an observation of a desired state the storage gate already accepted, so
// withholding on it would keep the stack from ever being created — the failure
// would make itself permanent. It is also what makes this error safe to hold
// without a garbage collection shield: the resources stay in the desired set,
// so the sweep is a no-op on them.
//
// The other error a stack action can return is a failed CRD discovery, which
// is covered below. That one renders nothing, which is safe only because the
// reconciler aborts on it before anything is applied or swept.
func TestDeployLokiStack_UnobservableStateReportsUnknown(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.LokiStack)

	m := monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret")

	// Not a missing stack: isLokiStackReady maps NotFound to (false, nil), so
	// an error here is only ever "could not look".
	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(
		lokiStorageSecret("usage-logs-secret", m.Spec.Namespace, "https://s3.example.com", "usage-logs-bucket"),
		lokiStorageSecret("logs-secret", m.Spec.Namespace, "https://s3.example.com", "logs-bucket"),
	).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if obj.GetObjectKind().GroupVersionKind().Kind == gvk.LokiStack.Kind {
				return k8serr.NewForbidden(
					schema.GroupResource{Resource: "lokistacks"}, key.Name, errors.New("no permission"))
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}).Build()

	cm := conditions.NewConditionsManager(m, m.Generation)

	// Both stacks were up as of the previous round. The manager preserves
	// conditions across reconciles, so without this the assertions below pass
	// on absence rather than on invalidation.
	cm.MarkTrue(conditions.ConditionUsageLogsLokiStackAvailable)
	cm.MarkTrue(conditions.ConditionLokiStackAvailable)

	var sources []rendertemplate.TemplateSource

	usageLogsErr, logsErr := deployBothLokiStacks(context.Background(), cli, m, cm, &sources)
	for name, err := range map[string]error{
		"usage logs":     usageLogsErr,
		"log forwarding": logsErr,
	} {
		if err == nil {
			t.Fatalf("expected the %s stack action to return the failure", name)
		}
		if !strings.Contains(err.Error(), "LokiStack readiness") {
			t.Errorf("%s error should mention the readiness check, got %v", name, err)
		}
	}

	// The decisive assertion. Withholding a template because its readiness Get
	// failed is what made a transient failure permanent: a stack that is never
	// applied is a stack that never becomes ready.
	assertTemplatePaths(t, sources, []string{
		UsageLogsLokiStackTemplate, UsageLogsLokiStackNPTemplate,
		LokiStackTemplate, LokiStackNetworkPolicyTemplate, LokiStackGatewayNetworkPolicyTemplate,
	})

	// Both stacks are configured and both hit the same failure, so both
	// conditions must be invalidated. A condition that keeps its stale True
	// lets publishLokiEndpoints republish an endpoint nothing verified this
	// round.
	for _, condType := range []string{
		conditions.ConditionUsageLogsLokiStackAvailable,
		conditions.ConditionLokiStackAvailable,
	} {
		c := mustFindCondition(t, m, condType)
		if c.Status != metav1.ConditionUnknown || c.Reason != lokiReadinessCheckFailedReason {
			t.Errorf("%s: expected Unknown/%s, got %s/%s",
				condType, lokiReadinessCheckFailedReason, c.Status, c.Reason)
		}
	}
}

// TestDeployLokiStack_UnconfiguredStackStaysOutOfTheAggregation bounds what an
// unobservable stack costs.
//
// A stack whose readiness cannot be observed reports Unknown, but a stack the
// user never configured has nothing to observe and must keep its Info-severity
// not-configured condition. Info severity is what keeps a feature nobody asked
// for out of the Degraded and Unknown aggregation; losing it would report the
// whole operand as degraded over a stack that was never requested.
func TestDeployLokiStack_UnconfiguredStackStaysOutOfTheAggregation(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.LokiStack)

	// Usage logs only, and its readiness Get times out. Log forwarding is off.
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{
		Storage: &v1alpha1.LokiStorageConfig{Type: "s3", SecretName: "usage-logs-secret"},
	}

	cli := fake.NewClientBuilder().WithScheme(s).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if obj.GetObjectKind().GroupVersionKind().Kind == gvk.LokiStack.Kind {
				return k8serr.NewTimeoutError("request timed out", 1)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}).Build()

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	usageLogsErr, logsErr := deployBothLokiStacks(context.Background(), cli, m, cm, &sources)
	if usageLogsErr == nil {
		t.Fatal("expected the timed-out readiness check to be returned")
	}
	if logsErr != nil {
		t.Errorf("an unconfigured stack has nothing to fail at, got %v", logsErr)
	}

	c := mustFindCondition(t, m, conditions.ConditionLokiStackAvailable)
	if c.Status != metav1.ConditionFalse || c.Reason != "LokiNotRequired" {
		t.Errorf("LokiStackAvailable: expected the not-configured condition, got %s/%s", c.Status, c.Reason)
	}
	if c.Severity != platformcommon.ConditionSeverityInfo {
		t.Errorf("LokiStackAvailable: expected Info severity, got %q", c.Severity)
	}

	cm.MarkTrue(conditions.ConditionMonitoringDependenciesReady)
	cm.AggregateReady()
	if got := mustFindCondition(t, m, string(platformcommon.ConditionTypeDegraded)); got.Status != metav1.ConditionFalse {
		t.Errorf("Degraded: an unobservable stack is not a degradation, got %s", got.Status)
	}
}

// TestDeployLokiStack_ReadinessNotFoundMarksFalse guards the other side of the
// line TestDeployLokiStack_UnobservableStateReportsUnknown draws.
//
// isLokiStackReady maps NotFound to (false, nil), so a stack that does not
// exist yet is a determinate answer: it is not ready. That must stay False and
// must keep rendering, because the stack only comes into existence by being
// applied — withholding it the way a failed check does would mean a stack that
// is absent can never be created. Only a non-nil error is "could not observe".
func TestDeployLokiStack_ReadinessNotFoundMarksFalse(t *testing.T) {
	for _, test := range []struct {
		name          string
		configure     func(*v1alpha1.Monitoring)
		condition     string
		wantTemplates []string
	}{
		{
			name: "usage logs stack not yet created",
			configure: func(m *v1alpha1.Monitoring) {
				m.Spec.UsageLogs = &v1alpha1.UsageLogs{
					Storage: &v1alpha1.LokiStorageConfig{Type: "s3", SecretName: "usage-logs-secret"},
				}
			},
			condition:     conditions.ConditionUsageLogsLokiStackAvailable,
			wantTemplates: []string{UsageLogsLokiStackTemplate, UsageLogsLokiStackNPTemplate},
		},
		{
			name: "log forwarding stack not yet created",
			configure: func(m *v1alpha1.Monitoring) {
				m.Spec.Logs = &v1alpha1.Logs{
					Storage: &v1alpha1.LokiStorageConfig{Type: "s3", SecretName: "logs-secret"},
				}
			},
			condition: conditions.ConditionLokiStackAvailable,
			// The forwarder's templates render alongside the stack's: they
			// stay in the desired set while Loki starts, so the sweep cannot
			// read a starting stack as a deletion of the forwarder.
			wantTemplates: []string{
				LokiStackTemplate,
				LokiStackNetworkPolicyTemplate,
				LokiStackGatewayNetworkPolicyTemplate,
				ClusterLogForwarderTemplate,
				ClusterLogForwarderRBACTemplate,
				ClusterLogForwarderNetworkPolicyTemplate,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newActionsTestScheme(t)
			registerCRDs(s, gvk.LokiStack)

			m := newMonitoring(v1alpha1.MonitoringInstanceName)
			test.configure(m)

			// No LokiStack objects: every Get is a genuine NotFound.
			cli := fake.NewClientBuilder().WithScheme(s).
				WithObjects(kubernetesAPIServerEndpointSlice()).Build()

			cm := conditions.NewConditionsManager(m, m.Generation)
			var sources []rendertemplate.TemplateSource

			usageLogsErr, logsErr := deployBothLokiStacks(context.Background(), cli, m, cm, &sources)
			if usageLogsErr != nil || logsErr != nil {
				t.Fatalf("an absent stack is not an error, it is the state before the first apply: %v, %v",
					usageLogsErr, logsErr)
			}

			c := mustFindCondition(t, m, test.condition)
			if c.Status != metav1.ConditionFalse || c.Reason != "LokiStackNotReady" {
				t.Errorf("%s: expected False/LokiStackNotReady for an absent stack, got %s/%s",
					test.condition, c.Status, c.Reason)
			}
			// The decisive part: the templates still render. A stack reported
			// Unknown renders nothing, and one that renders nothing is never
			// created.
			assertTemplatePaths(t, sources, test.wantTemplates)
		})
	}
}

// TestDeployLokiStack_StaleTrueIsOverwrittenOnFailedCheck is the regression
// this round exists for: a previous reconcile marked the stack ready, then the
// readiness check fails. The stale True must not survive into AggregateReady.
func TestDeployLokiStack_StaleTrueIsOverwrittenOnFailedCheck(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.LokiStack)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Logs = &v1alpha1.Logs{
		Storage: &v1alpha1.LokiStorageConfig{Type: "s3", SecretName: "logs-secret"},
	}

	cm := conditions.NewConditionsManager(m, m.Generation)
	// The stack was up as of the last round.
	cm.MarkTrue(conditions.ConditionLokiStackAvailable)

	cli := fake.NewClientBuilder().WithScheme(s).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if obj.GetObjectKind().GroupVersionKind().Kind == gvk.LokiStack.Kind {
				return k8serr.NewTimeoutError("request timed out", 1)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}).Build()

	var sources []rendertemplate.TemplateSource
	if _, logsErr := deployBothLokiStacks(context.Background(), cli, m, cm, &sources); logsErr == nil {
		t.Fatal("expected the log forwarding stack to return the timeout")
	}

	c := mustFindCondition(t, m, conditions.ConditionLokiStackAvailable)
	if c.Status != metav1.ConditionUnknown {
		t.Fatalf("LokiStackAvailable: a timed-out check must replace the stale True, got %s/%s", c.Status, c.Reason)
	}

	// And Ready must follow it down rather than reporting a working stack.
	cm.MarkTrue(conditions.ConditionMonitoringDependenciesReady)
	cm.AggregateReady()
	ready := mustFindCondition(t, m, string(platformcommon.ConditionTypeReady))
	if ready.Status != metav1.ConditionUnknown {
		t.Errorf("Ready: want Unknown while the stack's state is unobserved, got %s", ready.Status)
	}
}

// TestDeployLokiStack_OneStackFailingDoesNotPoisonTheOther is the regression
// this split exists for.
//
// The two stacks shared one action and one condition. A failed readiness Get on
// the usage logs stack left ConditionLokiStackAvailable Unknown as well — one
// because it could not be observed, the other because it had not been — and
// publishLokiEndpoints, reading those conditions, withheld status.logsEndpoint
// from a stack that was up the whole time. One apiserver blip on usage logs
// took log forwarding's address off the status.
//
// This drives the two actions directly, which is not what the reconciler does:
// there, the usage logs failure aborts the round before the log forwarding
// action runs, the same as for any other action. What is pinned here is that
// the actions are independent — each reports from its own observation and
// neither reads the other's state — and that publishLokiEndpoints turns that
// independence into the right endpoints. The storage gate has already passed,
// which is what licenses the independence: the buckets are provably disjoint.
func TestDeployLokiStack_OneStackFailingDoesNotPoisonTheOther(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.LokiStack)

	m := monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret")

	// The log forwarding stack is up; only the usage logs Get fails. Separate
	// buckets, so the gate below passes and the failure is purely an
	// observation failure.
	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(
		kubernetesAPIServerEndpointSlice(),
		lokiStorageSecret("usage-logs-secret", m.Spec.Namespace, "https://s3.example.com", "usage-logs-bucket"),
		lokiStorageSecret("logs-secret", m.Spec.Namespace, "https://s3.example.com", "logs-bucket"),
		readyLokiStack(lokiStackResourceName, m.Spec.Namespace),
	).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if obj.GetObjectKind().GroupVersionKind().Kind == gvk.LokiStack.Kind &&
				key.Name == usageLogsLokiStackResourceName {
				return k8serr.NewTimeoutError("request timed out", 1)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}).Build()

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	// Both stacks were up as of the previous round, and their endpoints were
	// published from that. The conditions and the endpoints below are what the
	// stacks report now, not leftovers.
	cm.MarkTrue(conditions.ConditionUsageLogsLokiStackAvailable)
	cm.MarkTrue(conditions.ConditionLokiStackAvailable)
	const staleUsageLogsEndpoint = "https://usage-logs.example.com/otlp"
	m.Status.UsageLogsEndpoint = staleUsageLogsEndpoint
	m.Status.LogsEndpoint = "https://logs.example.com"

	// Both outcomes are asserted, not just the first error: the defect was the
	// healthy stack inheriting a verdict it never earned, so a nil from it is
	// the thing worth pinning.
	err, logsErr := deployBothLokiStacks(context.Background(), cli, m, cm, &sources)
	if err == nil {
		t.Fatal("expected the timed-out usage logs readiness check to be returned")
	}
	if !strings.Contains(err.Error(), "checking usage logs LokiStack readiness") {
		t.Errorf("the usage logs error should name its own failure, got %v", err)
	}
	if logsErr != nil {
		t.Errorf("the log forwarding stack was healthy and must not inherit its sibling's failure: %v", logsErr)
	}

	// Both stacks render: the failed one because withholding its template
	// would keep it from ever being created, the healthy one because nothing
	// about it failed. The usage logs collector does not render — its action
	// aborted at the readiness check before reaching it — while the log
	// forwarding pipeline renders whole, forwarder included.
	assertTemplatePaths(t, sources, []string{
		UsageLogsLokiStackTemplate, UsageLogsLokiStackNPTemplate,
		LokiStackTemplate, LokiStackNetworkPolicyTemplate, LokiStackGatewayNetworkPolicyTemplate,
		ClusterLogForwarderTemplate, ClusterLogForwarderRBACTemplate, ClusterLogForwarderNetworkPolicyTemplate,
	})

	if c := mustFindCondition(t, m, conditions.ConditionUsageLogsLokiStackAvailable); c.Status != metav1.ConditionUnknown ||
		c.Reason != lokiReadinessCheckFailedReason {
		t.Errorf("UsageLogsLokiStackAvailable: want Unknown/%s, got %s/%s",
			lokiReadinessCheckFailedReason, c.Status, c.Reason)
	}
	// The decisive assertion. Before the split both stacks shared this
	// condition, so the usage logs failure wrote Unknown over a stack it had
	// not looked at.
	if c := mustFindCondition(t, m, conditions.ConditionLokiStackAvailable); c.Status != metav1.ConditionTrue {
		t.Errorf("LokiStackAvailable: a stack that was observed ready must report True, got %s/%s", c.Status, c.Reason)
	}

	// The user-visible payoff, and the only place it is covered: the healthy
	// stack's endpoint is published from its own observation, while the
	// unobservable one holds its last known value rather than being cleared —
	// clearing it would drop the usage logs collector from the desired set and
	// have garbage collection delete it.
	data, err := buildTemplateData(context.Background(), cli, m, "")
	if err != nil {
		t.Fatalf("buildTemplateData error: %v", err)
	}
	if requeue := publishLokiEndpoints(m, data); !requeue {
		t.Error("an unobservable stack must be polled again, expected a requeue")
	}
	wantLogsEndpoint, _ := data["LokiQueryEndpoint"].(string)
	if wantLogsEndpoint == "" {
		t.Fatal("LokiQueryEndpoint should be a non-empty string")
	}
	if got := m.Status.LogsEndpoint; got != wantLogsEndpoint {
		t.Errorf("LogsEndpoint: a ready stack's address must be published, want %q, got %q", wantLogsEndpoint, got)
	}
	if got := m.Status.UsageLogsEndpoint; got != staleUsageLogsEndpoint {
		t.Errorf("UsageLogsEndpoint: an unobservable stack holds its endpoint, want %q, got %q",
			staleUsageLogsEndpoint, got)
	}
}

// readyLokiStack builds a LokiStack CR reporting Ready=True, the state the
// per-stack readiness check looks for.
func readyLokiStack(name, namespace string) *unstructured.Unstructured {
	lokiStack := &unstructured.Unstructured{}
	lokiStack.SetGroupVersionKind(gvk.LokiStack)
	lokiStack.SetName(name)
	lokiStack.SetNamespace(namespace)
	_ = unstructured.SetNestedSlice(lokiStack.Object, []any{
		map[string]any{
			"type":   "Ready",
			"status": "True",
		},
	}, "status", "conditions")
	return lokiStack
}

// --- deployUsageLogsCollector ---

// usageLogsEndpoint is the usage logs LokiStack OTLP endpoint the reconciler
// publishes to status once that stack is ready; the collector deploys only
// after it is set.
const usageLogsEndpoint = "https://data-science-usage-logs-lokistack-gateway-http.test-ns.svc.cluster.local:8080/api/logs/v1/application/otlp"

func TestDeployUsageLogsCollector_NoUsageLogs(t *testing.T) {
	s := newActionsTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.UsageLogs = nil

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployUsageLogsCollector(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 0 {
		t.Errorf("expected no sources when usageLogs is nil, got %d", len(sources))
	}

	c := findCondition(m, conditions.ConditionUsageLogsCollectorAvailable)
	if c == nil || c.Status != metav1.ConditionFalse || c.Severity != platformcommon.ConditionSeverityInfo {
		t.Errorf("UsageLogsCollectorAvailable: expected False+Info, got %v", c)
	}
}

func TestDeployUsageLogsCollector_NoStorageNoExporters(t *testing.T) {
	s := newActionsTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployUsageLogsCollector(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 0 {
		t.Errorf("expected no sources when usageLogs asks for neither a store nor forwarding, got %d", len(sources))
	}

	c := findCondition(m, conditions.ConditionUsageLogsCollectorAvailable)
	if c == nil || c.Status != metav1.ConditionFalse || c.Severity != platformcommon.ConditionSeverityInfo {
		t.Errorf("UsageLogsCollectorAvailable: expected False+Info, got %v", c)
	}
}

func TestDeployUsageLogsCollector_LokiStackNotReady(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.OpenTelemetryCollector)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{
		Storage: &v1alpha1.LokiStorageConfig{
			Type:           "s3",
			SecretName:     "usage-logs-secret",
			CredentialMode: "static",
		},
	}
	// The endpoint is published to status only once LokiStack reports ready.
	m.Status.UsageLogsEndpoint = ""

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployUsageLogsCollector(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 0 {
		t.Errorf("expected no sources while the LokiStack endpoint is unset, got %d", len(sources))
	}

	c := findCondition(m, conditions.ConditionUsageLogsCollectorAvailable)
	if c == nil {
		t.Fatal("expected UsageLogsCollectorAvailable condition to be set")
		return
	}
	if c.Status != metav1.ConditionFalse {
		t.Errorf("expected status False, got %s", c.Status)
	}
	if c.Reason != "LokiStackNotReady" {
		t.Errorf("expected reason LokiStackNotReady, got %s", c.Reason)
	}
}

func TestDeployUsageLogsCollector_CRDMissing(t *testing.T) {
	s := newActionsTestScheme(t)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{
		Storage: &v1alpha1.LokiStorageConfig{
			Type:           "s3",
			SecretName:     "usage-logs-secret",
			CredentialMode: "static",
		},
	}
	m.Status.UsageLogsEndpoint = usageLogsEndpoint

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	cli := fake.NewClientBuilder().WithScheme(s).
		WithInterceptorFuncs(missingCRDs(gvk.OpenTelemetryCollector)).Build()
	err := deployUsageLogsCollector(context.Background(), cli, m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 0 {
		t.Errorf("expected no sources when the OpenTelemetryCollector CRD is missing, got %d", len(sources))
	}

	c := findCondition(m, conditions.ConditionUsageLogsCollectorAvailable)
	if c == nil {
		t.Fatal("expected UsageLogsCollectorAvailable condition to be set")
		return
	}
	if c.Status != metav1.ConditionFalse {
		t.Errorf("expected status False, got %s", c.Status)
	}
	if c.Reason != "OpenTelemetryCollectorCRDNotFound" {
		t.Errorf("expected reason OpenTelemetryCollectorCRDNotFound, got %s", c.Reason)
	}
}

// TestUsageLogsNetworkPoliciesAgreeAcrossActions renders the usage logs
// pipeline end to end and checks that the two NetworkPolicies name the pods
// that the collector and LokiStack templates actually create.
//
// The two policies are contributed by different halves of the usage logs
// pipeline action — the collector policy by the folded deployUsageLogsCollector
// half, the gateway policy by the LokiStack half, which owns it because the
// gateway has to be guarded before any collector exists to dial it. Splitting
// this test along that seam would lose the only check that the gateway
// policy's "from" peer and the collector policy's podSelector still refer to
// the same workload.
func TestUsageLogsNetworkPoliciesAgreeAcrossActions(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.OpenTelemetryCollector, gvk.LokiStack)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{
		Storage: &v1alpha1.LokiStorageConfig{
			Type:           "s3",
			SecretName:     "usage-logs-secret",
			CredentialMode: "static",
		},
	}
	m.Status.UsageLogsEndpoint = usageLogsEndpoint

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(kubernetesAPIServerEndpointSlice()).Build()

	// Spec.Logs is unset, so only the usage logs pipeline renders here — the
	// stack pair and, folded behind them, the collector trio.
	if usageLogsErr, _ := deployBothLokiStacks(context.Background(), cli, m, cm, &sources); usageLogsErr != nil {
		t.Fatalf("usage logs LokiStack error: %v", usageLogsErr)
	}

	c := findCondition(m, conditions.ConditionUsageLogsCollectorAvailable)
	if c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("UsageLogsCollectorAvailable: expected True, got %v", c)
	}
	// By path, not by count: the gateway policy leaving this action and the
	// collector policy arriving in its place would keep the count at five.
	// The gateway policy must arrive ahead of the collector's own templates —
	// it has to outlive the collector's readiness gate, which is the whole
	// reason it belongs to the LokiStack half of the action.
	assertTemplatePaths(t, sources, []string{
		UsageLogsLokiStackTemplate,
		UsageLogsLokiStackNPTemplate,
		UsageLogsOpenTelemetryCollectorTemplate,
		UsageLogsOpenTelemetryCollectorRBACTemplate,
		UsageLogsOpenTelemetryCollectorNPTemplate,
	})

	data, err := buildTemplateData(context.Background(), cli, m, "")
	if err != nil {
		t.Fatalf("buildTemplateData error: %v", err)
	}
	objects, err := rendertemplate.Render(context.Background(), s, sources, data)
	if err != nil {
		t.Fatalf("rendering usage logs templates: %v", err)
	}

	rendered := map[string]*unstructured.Unstructured{}
	for i := range objects {
		rendered[objects[i].GetKind()+"/"+objects[i].GetName()] = objects[i].DeepCopy()
	}
	for _, key := range []string{
		"OpenTelemetryCollector/data-science-usage-logs",
		"ClusterRole/data-science-usage-logs-processor",
		"ClusterRoleBinding/data-science-usage-logs-processor",
		"NetworkPolicy/data-science-usage-logs-collector-allow",
		"NetworkPolicy/data-science-usage-logs-lokistack-allow",
		"LokiStack/data-science-usage-logs-lokistack",
	} {
		if rendered[key] == nil {
			t.Errorf("missing rendered resource %s (got %v)", key, slices.Sorted(maps.Keys(rendered)))
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	// Both policies select their peers by name, so they must agree with the names
	// the collector and LokiStack templates actually render.
	collectorName, ok := data["UsageLogsCollectorName"].(string)
	if !ok {
		t.Fatal("UsageLogsCollectorName should be a string")
	}
	lokiStackName, ok := data["UsageLogsLokiStackName"].(string)
	if !ok {
		t.Fatal("UsageLogsLokiStackName should be a string")
	}

	assertUsageLogsCollectorConfig(t, rendered["OpenTelemetryCollector/data-science-usage-logs"])
	assertUsageLogsNetworkPolicies(t, usageLogsNetworkPolicyPeers{
		collectorPolicy:  rendered["NetworkPolicy/data-science-usage-logs-collector-allow"],
		lokiStackPolicy:  rendered["NetworkPolicy/data-science-usage-logs-lokistack-allow"],
		collectorPodName: collectorName + "-collector",
		lokiStackName:    lokiStackName,
		namespace:        m.Spec.Namespace,
	})
}

type usageLogsNetworkPolicyPeers struct {
	collectorPolicy  *unstructured.Unstructured
	lokiStackPolicy  *unstructured.Unstructured
	collectorPodName string
	lokiStackName    string
	namespace        string
}

func assertUsageLogsCollectorConfig(t *testing.T, collector *unstructured.Unstructured) {
	t.Helper()

	// Ingress is governed by the NetworkPolicies this operator renders, so the
	// operator-managed one must stay off.
	enabled, found, err := unstructured.NestedBool(collector.Object, "spec", "networkPolicy", "enabled")
	if err != nil || !found {
		t.Fatalf("reading spec.networkPolicy.enabled: found=%t err=%v", found, err)
	}
	if enabled {
		t.Error("the operator-managed collector NetworkPolicy must be disabled")
	}

	attributes, found, err := unstructured.NestedSlice(collector.Object, "spec", "config", "processors", "resource", "attributes")
	if err != nil || !found {
		t.Fatalf("reading resource processor attributes: found=%t err=%v", found, err)
	}
	byKey := map[string]map[string]any{}
	for _, attribute := range attributes {
		entry, ok := attribute.(map[string]any)
		if !ok {
			t.Fatalf("resource attribute has unexpected type %T", attribute)
		}
		key, _ := entry["key"].(string)
		byKey[key] = entry
	}

	// log_type is pinned to the Loki application tenant. It must not be sourced
	// from log_name, which MaaS consumes with its envoy access-logger value.
	logType := byKey["log_type"]
	if logType["action"] != "upsert" || logType["value"] != "application" {
		t.Errorf("log_type must be upserted to the application tenant, got %#v", logType)
	}
	if _, ok := logType["from_attribute"]; ok {
		t.Errorf("log_type must not be derived from another attribute, got %#v", logType)
	}

	// The AI tenant namespace arrives on service.namespace and drives LokiStack
	// RBAC. insert (not upsert) so a value already set by the sender wins.
	namespaceName := byKey["kubernetes_namespace_name"]
	if namespaceName["action"] != "insert" || namespaceName["from_attribute"] != "service.namespace" {
		t.Errorf("kubernetes_namespace_name must be inserted from service.namespace, got %#v", namespaceName)
	}

	cleanup, found, err := unstructured.NestedSlice(collector.Object, "spec", "config", "processors", "transform/cleanup", "log_statements")
	if err != nil || !found {
		t.Fatalf("reading transform/cleanup log_statements: found=%t err=%v", found, err)
	}
	for _, block := range cleanup {
		entry, ok := block.(map[string]any)
		if !ok {
			t.Fatalf("cleanup log_statement has unexpected type %T", block)
		}
		statements, _, err := unstructured.NestedStringSlice(entry, "statements")
		if err != nil {
			t.Fatalf("reading cleanup statements: %v", err)
		}
		for _, statement := range statements {
			if strings.Contains(statement, "log_name") {
				t.Errorf("cleanup must preserve log_name for MaaS consumers, got %q", statement)
			}
		}
	}
}

func assertUsageLogsNetworkPolicies(t *testing.T, peers usageLogsNetworkPolicyPeers) {
	t.Helper()

	collector := &networkingv1.NetworkPolicy{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(peers.collectorPolicy.Object, collector); err != nil {
		t.Fatalf("converting collector NetworkPolicy: %v", err)
	}
	if got := collector.Spec.PodSelector.MatchLabels["app.kubernetes.io/name"]; got != peers.collectorPodName {
		t.Errorf("collector NetworkPolicy must select the collector pods %q, got %q", peers.collectorPodName, got)
	}
	if !slices.Equal(collector.Spec.PolicyTypes, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}) {
		t.Errorf("collector NetworkPolicy must be ingress-only, got %v", collector.Spec.PolicyTypes)
	}

	// OTLP ingest is restricted to the ingress policy group (the MaaS gateway):
	// write access to Loki cannot be delegated, so reachability is the control.
	var otlpRules, openRules int
	for _, rule := range collector.Spec.Ingress {
		if len(rule.From) == 0 {
			openRules++
			if !networkPolicyAllowsIngressPort(rule, 8888) {
				t.Errorf("the unrestricted ingress rule must only expose the metrics port, got %#v", rule.Ports)
			}
			continue
		}
		otlpRules++
		for _, port := range []int32{4317, 4318} {
			if !networkPolicyAllowsIngressPort(rule, port) {
				t.Errorf("collector NetworkPolicy must admit OTLP on %d, got %#v", port, rule.Ports)
			}
		}
		for _, peer := range rule.From {
			if peer.NamespaceSelector == nil ||
				peer.NamespaceSelector.MatchLabels["network.openshift.io/policy-group"] != "ingress" {
				t.Errorf("OTLP ingest must come from the ingress policy group, got %#v", peer)
			}
		}
	}
	if otlpRules != 1 || openRules != 1 {
		t.Errorf("expected exactly one restricted OTLP rule and one metrics rule, got %d and %d", otlpRules, openRules)
	}

	lokiStack := &networkingv1.NetworkPolicy{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(peers.lokiStackPolicy.Object, lokiStack); err != nil {
		t.Fatalf("converting LokiStack NetworkPolicy: %v", err)
	}
	if lokiStack.GetNamespace() != peers.namespace {
		t.Errorf("LokiStack NetworkPolicy must live beside the gateway in %q, got namespace %q",
			peers.namespace, lokiStack.GetNamespace())
	}
	if got := lokiStack.Spec.PodSelector.MatchLabels; got["app.kubernetes.io/component"] != "lokistack-gateway" ||
		got["app.kubernetes.io/instance"] != peers.lokiStackName {
		t.Errorf("LokiStack NetworkPolicy must select the %q gateway pods, got %v", peers.lokiStackName, got)
	}
	if len(lokiStack.Spec.Ingress) != 1 {
		t.Fatalf("expected a single ingress rule on the LokiStack NetworkPolicy, got %d", len(lokiStack.Spec.Ingress))
	}
	rule := lokiStack.Spec.Ingress[0]
	if !networkPolicyAllowsIngressPort(rule, 8080) {
		t.Errorf("LokiStack NetworkPolicy must admit the gateway port 8080, got %#v", rule.Ports)
	}
	if len(rule.From) != 1 || rule.From[0].PodSelector == nil ||
		rule.From[0].PodSelector.MatchLabels["app.kubernetes.io/name"] != peers.collectorPodName {
		t.Errorf("only the usage logs collector may reach the gateway, got %#v", rule.From)
	}
}

func networkPolicyAllowsIngressPort(rule networkingv1.NetworkPolicyIngressRule, port int32) bool {
	return networkPolicyAllowsPort(networkingv1.NetworkPolicyEgressRule{Ports: rule.Ports}, port)
}

func TestDeployKorrel8r_GatedBySignalConfiguration(t *testing.T) {
	tests := []struct {
		name         string
		configure    func(*v1alpha1.Monitoring)
		wantSources  int
		wantStatus   metav1.ConditionStatus
		wantSeverity platformcommon.ConditionSeverity
		wantReason   string
	}{
		{
			name:         "no signals",
			wantStatus:   metav1.ConditionFalse,
			wantSeverity: platformcommon.ConditionSeverityInfo,
			wantReason:   "Korrel8rNotConfigured",
		},
		{
			name: "metrics",
			configure: func(m *v1alpha1.Monitoring) {
				m.Spec.Metrics = &v1alpha1.Metrics{
					Storage: &v1alpha1.MetricsStorage{},
				}
			},
			wantSources: 4,
			wantStatus:  metav1.ConditionFalse,
			wantReason:  "Korrel8rNotReady",
		},
		{
			name: "traces",
			configure: func(m *v1alpha1.Monitoring) {
				m.Spec.Traces = &v1alpha1.Traces{
					Storage: &v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV},
				}
			},
			wantSources: 4,
			wantStatus:  metav1.ConditionFalse,
			wantReason:  "Korrel8rNotReady",
		},
		{
			name: "logs",
			configure: func(m *v1alpha1.Monitoring) {
				m.Spec.Logs = &v1alpha1.Logs{}
			},
			wantSources: 4,
			wantStatus:  metav1.ConditionFalse,
			wantReason:  "Korrel8rNotReady",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newMonitoring(v1alpha1.MonitoringInstanceName)
			if tt.configure != nil {
				tt.configure(m)
			}

			cm := conditions.NewConditionsManager(m, m.Generation)
			var sources []rendertemplate.TemplateSource
			cli := fake.NewClientBuilder().WithScheme(newActionsTestScheme(t)).Build()
			if err := deployKorrel8r(context.Background(), cli, m, cm, &sources); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(sources) != tt.wantSources {
				t.Errorf("sources: want %d, got %d", tt.wantSources, len(sources))
			}
			condition := findCondition(m, conditions.ConditionKorrel8rAvailable)
			if condition == nil {
				t.Fatal("expected Korrel8rAvailable condition")
				return
			}
			if condition.Status != tt.wantStatus {
				t.Errorf("status: want %s, got %s", tt.wantStatus, condition.Status)
			}
			if tt.wantSeverity != "" && condition.Severity != tt.wantSeverity {
				t.Errorf("severity: want %s, got %s", tt.wantSeverity, condition.Severity)
			}
			if tt.wantReason != "" && condition.Reason != tt.wantReason {
				t.Errorf("reason: want %s, got %s", tt.wantReason, condition.Reason)
			}
		})
	}
}

func TestDeployKorrel8r_MarksAvailableWhenServiceReady(t *testing.T) {
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{
		Storage: &v1alpha1.MetricsStorage{},
	}

	replicas := int32(1)
	cli := fake.NewClientBuilder().WithScheme(newActionsTestScheme(t)).WithObjects(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: Korrel8rServiceName, Namespace: m.Spec.Namespace},
			Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
			Status:     appsv1.DeploymentStatus{ReadyReplicas: replicas},
		},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: Korrel8rServiceName, Namespace: m.Spec.Namespace},
		},
		&discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      Korrel8rServiceName + "-abcde",
				Namespace: m.Spec.Namespace,
				Labels:    map[string]string{discoveryv1.LabelServiceName: Korrel8rServiceName},
			},
			Endpoints: []discoveryv1.Endpoint{{
				Addresses: []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{
					Ready: func() *bool { value := true; return &value }(),
				},
			}},
		},
	).Build()

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource
	if err := deployKorrel8r(context.Background(), cli, m, cm, &sources); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	condition := findCondition(m, conditions.ConditionKorrel8rAvailable)
	if condition == nil {
		t.Fatal("expected Korrel8rAvailable condition")
		return
	}
	if condition.Status != metav1.ConditionTrue {
		t.Fatalf("status: want True, got %s", condition.Status)
	}
}

func TestDeployKorrel8r_RendersOwnedResourcesAndConfiguredStores(t *testing.T) {
	s := newActionsTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{
		Storage: &v1alpha1.MetricsStorage{},
	}
	m.Spec.Traces = &v1alpha1.Traces{
		Storage: &v1alpha1.TracesStorage{Backend: v1alpha1.StorageBackendPV},
	}
	m.Spec.Logs = &v1alpha1.Logs{}

	// The backend Services are created asynchronously by their operators. Their
	// absence on the initial render must not omit configured stores permanently.
	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(kubernetesAPIServerEndpointSlice()).Build()
	data, err := buildTemplateData(context.Background(), cli, m, "")
	if err != nil {
		t.Fatalf("buildTemplateData error: %v", err)
	}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource
	if err := deployKorrel8r(context.Background(), cli, m, cm, &sources); err != nil {
		t.Fatalf("deployKorrel8r error: %v", err)
	}

	objects, err := rendertemplate.Render(context.Background(), s, sources, data)
	if err != nil {
		t.Fatalf("rendering Korrel8r templates: %v", err)
	}
	if len(objects) != 8 {
		t.Fatalf("expected eight rendered resources, got %d", len(objects))
	}

	resources := findKorrel8rRenderedResources(t, objects)
	assertKorrel8rConfiguration(t, resources.config, resources.metricsRule)
	assertKorrel8rTLSEndpoint(t, resources.deployment, resources.service, resources.networkPolicy)
}

type korrel8rRenderedResources struct {
	config        string
	metricsRule   string
	deployment    *unstructured.Unstructured
	service       *unstructured.Unstructured
	networkPolicy *unstructured.Unstructured
}

func findKorrel8rRenderedResources(t *testing.T, objects []unstructured.Unstructured) korrel8rRenderedResources {
	t.Helper()
	resources := korrel8rRenderedResources{}
	for i := range objects {
		switch {
		case objects[i].GetKind() == "ConfigMap" && objects[i].GetName() == "korrel8r-config":
			config, _, err := unstructured.NestedString(objects[i].Object, "data", "korrel8r.yaml")
			if err != nil {
				t.Fatalf("reading Korrel8r config: %v", err)
			}
			resources.config = config
			metricsRule, _, err := unstructured.NestedString(objects[i].Object, "data", "rhoai-metrics.yaml")
			if err != nil {
				t.Fatalf("reading Korrel8r metrics rule: %v", err)
			}
			resources.metricsRule = metricsRule
		case objects[i].GetKind() == "Deployment" && objects[i].GetName() == Korrel8rServiceName:
			resources.deployment = objects[i].DeepCopy()
		case objects[i].GetKind() == "Service" && objects[i].GetName() == Korrel8rServiceName:
			resources.service = objects[i].DeepCopy()
		case objects[i].GetKind() == "NetworkPolicy" && objects[i].GetName() == Korrel8rServiceName:
			resources.networkPolicy = objects[i].DeepCopy()
		}
	}
	return resources
}

func assertKorrel8rConfiguration(t *testing.T, config, metricsRule string) {
	t.Helper()
	if config == "" {
		t.Fatal("rendered Korrel8r ConfigMap is missing korrel8r.yaml")
	}
	var parsed map[string]any
	if err := yaml.Unmarshal([]byte(config), &parsed); err != nil {
		t.Fatalf("Korrel8r config is not valid YAML: %v", err)
	}
	tuning, ok := parsed["tuning"].(map[string]any)
	if !ok {
		t.Fatalf("Korrel8r config has no tuning section: %#v", parsed["tuning"])
	}
	if tuning["totalLimit"] != 3000 || tuning["totalQueryLimit"] != 300 || tuning["requestTimeout"] != "30s" || tuning["sessionTimeout"] != "5m" {
		t.Errorf("unexpected tuning section: %#v", tuning)
	}
	for _, expected := range []string{
		"domain: k8s",
		"domain: metric",
		"domain: trace",
		"domain: log",
		"direct: true",
		"/etc/korrel8r/rules/all.yaml",
		"/etc/korrel8r/custom/rhoai-metrics.yaml",
		"/etc/korrel8r/custom/rhai-inference-rules.yaml",
		"totalLimit: 3000",
		"totalQueryLimit: 300",
		"requestTimeout: 30s",
		"sessionTimeout: 5m",
	} {
		if !strings.Contains(config, expected) {
			t.Errorf("Korrel8r config missing %q:\n%s", expected, config)
		}
	}
	if !strings.Contains(metricsRule, `metric:metric:{exported_namespace="{{.metadata.namespace}}",exported_pod="{{.metadata.name}}"}`) {
		t.Errorf("Korrel8r metrics rule must query the collector-exported workload labels, got:\n%s", metricsRule)
	}
}

func assertKorrel8rTLSEndpoint(t *testing.T, deployment, service, networkPolicy *unstructured.Unstructured) {
	t.Helper()
	if deployment == nil || service == nil || networkPolicy == nil {
		t.Fatal("rendered Korrel8r resources are incomplete")
		return
	}
	containers, found, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	if err != nil || !found || len(containers) != 1 {
		t.Fatalf("reading Korrel8r container: found=%t count=%d err=%v", found, len(containers), err)
	}
	container, ok := containers[0].(map[string]any)
	if !ok {
		t.Fatalf("Korrel8r container has unexpected type %T", containers[0])
	}
	args, found, err := unstructured.NestedStringSlice(container, "args")
	if err != nil || !found {
		t.Fatalf("reading Korrel8r container args: found=%t err=%v", found, err)
	}
	if !slices.Contains(args, "--https=:8443") || !slices.Contains(args, "--mcp=false") {
		t.Errorf("Korrel8r must use HTTPS with MCP disabled, got args %v", args)
	}
	annotations := service.GetAnnotations()
	if annotations["service.beta.openshift.io/serving-cert-secret-name"] != "korrel8r-tls" {
		t.Errorf("Korrel8r Service must request a serving certificate, got annotations %v", annotations)
	}
	checksum, found, err := unstructured.NestedString(deployment.Object, "spec", "template", "metadata", "annotations", "platform.opendatahub.io/korrel8r-config-checksum")
	if err != nil || !found || checksum == "" {
		t.Errorf("Korrel8r Deployment must roll out when its generated ConfigMap changes")
	}
	policy := &networkingv1.NetworkPolicy{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(networkPolicy.Object, policy); err != nil {
		t.Fatalf("converting Korrel8r NetworkPolicy: %v", err)
	}
	policyAnnotations := networkPolicy.GetAnnotations()
	if policyAnnotations["platform.opendatahub.io/api-server-access"] != "endpoint-slice-addresses" {
		t.Errorf("Korrel8r NetworkPolicy must declare endpoint-slice API-server access, got annotations %v", policyAnnotations)
	}
	if policyAnnotations["platform.opendatahub.io/api-server-node-source"] != "explicit-exception" {
		t.Errorf("Korrel8r NetworkPolicy must declare the node-sourced API-server exception, got annotations %v", policyAnnotations)
	}
	for _, rule := range policy.Spec.Egress {
		for _, peer := range rule.To {
			if peer.IPBlock != nil && (peer.IPBlock.CIDR == "0.0.0.0/0" || peer.IPBlock.CIDR == "::/0") {
				t.Errorf("Korrel8r NetworkPolicy must not allow a universal API-server CIDR: %#v", peer.IPBlock)
			}
		}
	}
	if !networkPolicyAllowsEgress(policy, "172.20.0.1/32", 6443) {
		t.Errorf("Korrel8r NetworkPolicy must allow the Kubernetes API endpoint on 6443: %#v", policy.Spec.Egress)
	}
}

func networkPolicyAllowsEgress(policy *networkingv1.NetworkPolicy, cidr string, port int32) bool {
	for _, rule := range policy.Spec.Egress {
		if !networkPolicyAllowsPort(rule, port) {
			continue
		}
		for _, peer := range rule.To {
			if peer.IPBlock != nil && peer.IPBlock.CIDR == cidr {
				return true
			}
		}
	}
	return false
}

func networkPolicyAllowsPort(rule networkingv1.NetworkPolicyEgressRule, port int32) bool {
	for _, policyPort := range rule.Ports {
		protocol := corev1.ProtocolTCP
		if policyPort.Protocol != nil {
			protocol = *policyPort.Protocol
		}
		if protocol == corev1.ProtocolTCP && policyPort.Port != nil && policyPort.Port.IntVal == port {
			return true
		}
	}
	return false
}

func TestNetworkPolicyAllowsPortRequiresTCP(t *testing.T) {
	udp := corev1.ProtocolUDP
	tcp := corev1.ProtocolTCP
	for _, test := range []struct {
		name     string
		protocol *corev1.Protocol
		want     bool
	}{
		{name: "nil protocol defaults to TCP", want: true},
		{name: "explicit TCP", protocol: &tcp, want: true},
		{name: "UDP is not API HTTPS", protocol: &udp, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			rule := networkingv1.NetworkPolicyEgressRule{Ports: []networkingv1.NetworkPolicyPort{{
				Protocol: test.protocol,
				Port:     &intstr.IntOrString{Type: intstr.Int, IntVal: 6443},
			}}}
			if got := networkPolicyAllowsPort(rule, 6443); got != test.want {
				t.Errorf("networkPolicyAllowsPort: want %t, got %t", test.want, got)
			}
		})
	}
}

func TestHasReadyConditionAcceptsNamespacedConditionType(t *testing.T) {
	t.Parallel()

	obj := &unstructured.Unstructured{Object: map[string]any{}}
	if err := unstructured.SetNestedSlice(obj.Object, []any{
		map[string]any{
			"type":   "observability.openshift.io/Ready",
			"status": "True",
		},
	}, "status", "conditions"); err != nil {
		t.Fatalf("setting conditions: %v", err)
	}

	ready, err := hasReadyCondition(obj)
	if err != nil {
		t.Fatalf("hasReadyCondition returned error: %v", err)
	}
	if !ready {
		t.Fatal("hasReadyCondition returned false for a namespaced Ready condition")
	}
}

// TestDeployUsageLogsCollector_RemoteExporters covers the dual-write: custom
// exporters get their own pipeline off the shared receiver, cluster identity is
// stamped after the cleanup stage that deletes cluster_name, and the local write
// keeps the record shape its dashboards and tenancy proxy expect.
func usageLogsCredentialSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "hub-usage-logs-auth",
			Namespace: "test-ns",
		},
		Data: map[string][]byte{"token": []byte("not-a-real-token")},
	}
}

// A named credential the cluster does not have yet withholds the collector instead of
// letting it fail to mount, and names the Secret it is waiting for. On a fresh spoke
// this is the window between the chart naming the Secret and ACM delivering it.
func TestDeployUsageLogsCollector_CredentialSecretMissing(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.OpenTelemetryCollector)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{
		Exporters: map[string]runtime.RawExtension{
			"otlphttp/hub": {Raw: []byte(`{"endpoint":"https://hub.example.com/api/logs/v1/infrastructure/otlp"}`)},
		},
		CredentialSecret: "hub-usage-logs-auth",
	}

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployUsageLogsCollector(context.Background(),
		fake.NewClientBuilder().WithScheme(s).Build(), m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 0 {
		t.Errorf("expected no sources while the credential Secret is absent, got %d", len(sources))
	}

	c := findCondition(m, conditions.ConditionUsageLogsCollectorAvailable)
	if c == nil {
		t.Fatal("expected UsageLogsCollectorAvailable condition to be set")
		return
	}
	if c.Status != metav1.ConditionFalse {
		t.Errorf("expected status False, got %s", c.Status)
	}
	if c.Reason != conditions.CredentialNotFoundReason {
		t.Errorf("expected reason %s, got %s", conditions.CredentialNotFoundReason, c.Reason)
	}
	if !strings.Contains(c.Message, "hub-usage-logs-auth") {
		t.Errorf("message must name the Secret being waited for, got %q", c.Message)
	}
}

func TestDeployUsageLogsCollector_RemoteExporters(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.OpenTelemetryCollector)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{
		Storage: &v1alpha1.LokiStorageConfig{
			Type:           "s3",
			SecretName:     "usage-logs-secret",
			CredentialMode: "static",
		},
		Size: "1x.demo",
		Exporters: map[string]runtime.RawExtension{
			"otlphttp/hub": {Raw: []byte(`{"endpoint":"https://hub.example.com/api/logs/v1/infrastructure/otlp"}`)},
		},
		Attributes: map[string]string{
			"cluster_name": "jeyoungspoke1",
			"cluster_id":   "eea5f0b3-5982-4eb1-b256-8e2443f7b3a0",
		},
		CredentialSecret: "hub-usage-logs-auth",
	}
	m.Status.UsageLogsEndpoint = usageLogsEndpoint

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource
	cli := fake.NewClientBuilder().WithScheme(s).
		WithObjects(kubernetesAPIServerEndpointSlice(), usageLogsCredentialSecret()).Build()
	if err := deployUsageLogsCollector(context.Background(), cli, m, cm, &sources); err != nil {
		t.Fatalf("deployUsageLogsCollector error: %v", err)
	}

	data, err := buildTemplateData(context.Background(), cli, m, "")
	if err != nil {
		t.Fatalf("buildTemplateData error: %v", err)
	}
	if got := data["LokiStackSize"]; got != "1x.demo" {
		t.Errorf("LokiStackSize: want 1x.demo, got %v", got)
	}
	objects, err := rendertemplate.Render(context.Background(), s, sources, data)
	if err != nil {
		t.Fatalf("rendering usage logs templates: %v", err)
	}

	var collector *unstructured.Unstructured
	for i := range objects {
		if objects[i].GetKind() == "OpenTelemetryCollector" {
			collector = objects[i].DeepCopy()
		}
	}
	if collector == nil {
		t.Fatal("no OpenTelemetryCollector rendered")
	}

	exporters, _, _ := unstructured.NestedMap(collector.Object, "spec", "config", "exporters")
	if _, ok := exporters["otlphttp/hub"]; !ok {
		t.Errorf("custom exporter missing; got %v", slices.Sorted(maps.Keys(exporters)))
	}
	if _, ok := exporters["otlphttp/loki"]; !ok {
		t.Error("local exporter must survive alongside the remote one")
	}

	pipelines, _, _ := unstructured.NestedMap(collector.Object, "spec", "config", "service", "pipelines")
	remote, ok := pipelines["logs/remote"].(map[string]any)
	if !ok {
		t.Fatalf("logs/remote pipeline missing; got %v", slices.Sorted(maps.Keys(pipelines)))
	}
	if recv, _, _ := unstructured.NestedStringSlice(remote, "receivers"); !slices.Equal(recv, []string{"otlp"}) {
		t.Errorf("logs/remote must share the otlp receiver, got %v", recv)
	}
	if exp, _, _ := unstructured.NestedStringSlice(remote, "exporters"); !slices.Equal(exp, []string{"otlphttp/hub"}) {
		t.Errorf("logs/remote exporters: want [otlphttp/hub], got %v", exp)
	}

	// Identity must come after transform/cleanup, which deletes cluster_name.
	procs, _, _ := unstructured.NestedStringSlice(remote, "processors")
	cleanup := slices.Index(procs, "transform/cleanup")
	identity := slices.Index(procs, "resource/usage_logs_remote")
	if cleanup < 0 || identity < 0 || identity < cleanup {
		t.Errorf("resource/usage_logs_remote must run after transform/cleanup, got %v", procs)
	}

	// And it must not touch the local pipeline.
	local, _ := pipelines["logs"].(map[string]any)
	localProcs, _, _ := unstructured.NestedStringSlice(local, "processors")
	if slices.Contains(localProcs, "resource/usage_logs_remote") {
		t.Errorf("cluster identity leaked into the local write: %v", localProcs)
	}
	if exp, _, _ := unstructured.NestedStringSlice(local, "exporters"); !slices.Equal(exp, []string{"otlphttp/loki"}) {
		t.Errorf("local pipeline exporters changed: %v", exp)
	}

	// The credential is mounted, not inlined: a bearertokenauth instance reading a
	// file, so a rotated Secret reaches the running collector.
	exts, _, _ := unstructured.NestedMap(collector.Object, "spec", "config", "extensions")
	if _, ok := exts["bearertokenauth/remote"]; !ok {
		t.Errorf("bearertokenauth/remote extension missing; got %v", slices.Sorted(maps.Keys(exts)))
	}
	if fn, _, _ := unstructured.NestedString(exts, "bearertokenauth/remote", "filename"); fn != "/var/run/secrets/usage-logs-remote/token" {
		t.Errorf("unexpected credential path %q", fn)
	}
	if enabled, _, _ := unstructured.NestedStringSlice(collector.Object, "spec", "config", "service", "extensions"); !slices.Contains(enabled, "bearertokenauth/remote") {
		t.Errorf("bearertokenauth/remote not enabled in service.extensions: %v", enabled)
	}
	vols, _, _ := unstructured.NestedSlice(collector.Object, "spec", "volumes")
	if len(vols) != 1 {
		t.Fatalf("expected one volume for the credential, got %v", vols)
	}
	vol, ok := vols[0].(map[string]any)
	if !ok {
		t.Fatalf("expected the volume to be an object, got %T", vols[0])
	}
	if name, _, _ := unstructured.NestedString(vol, "secret", "secretName"); name != "hub-usage-logs-auth" {
		t.Errorf("volume does not reference the credential Secret: %v", vols[0])
	}
	// The authenticator is attached by the controller, not written by the user:
	// exporter configs are validated against an allowlist that excludes auth.
	if a, _, _ := unstructured.NestedString(exporters, "otlphttp/hub", "auth", "authenticator"); a != "bearertokenauth/remote" {
		t.Errorf("controller did not attach the authenticator to the exporter, got %q", a)
	}

	// The local write must keep using the collector's own ServiceAccount token.
	if fn, _, _ := unstructured.NestedString(exts, "bearertokenauth", "filename"); fn != "/var/run/secrets/kubernetes.io/serviceaccount/token" {
		t.Errorf("local write credential changed: %q", fn)
	}
}

// TestDeployUsageLogsCollector_CredentialWithoutExporters covers a credential set
// with nothing to authenticate. The mount must not be rendered: there is no remote
// write to carry it, and a Secret that does not exist yet would leave the collector
// in ContainerCreating on FailedMount, taking the cluster-local write down with it.
// Forwarding only, with no cluster-local store at all. The collector must deploy
// without waiting for a LokiStack endpoint nothing will ever publish, render only the
// remote pipeline, and say which mode it is in.
func TestDeployUsageLogsCollector_ForwardingOnly(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.OpenTelemetryCollector)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{
		Exporters: map[string]runtime.RawExtension{
			"otlphttp/hub": {Raw: []byte(`{"endpoint":"https://hub.example.com/api/logs/v1/infrastructure/otlp"}`)},
		},
		Attributes:       map[string]string{"cluster_name": "jeyoungspoke1"},
		CredentialSecret: "hub-usage-logs-auth",
	}
	// Deliberately empty: no LokiStack is deployed here, so nothing ever sets it.
	m.Status.UsageLogsEndpoint = ""

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource
	cli := fake.NewClientBuilder().WithScheme(s).
		WithObjects(kubernetesAPIServerEndpointSlice(), usageLogsCredentialSecret()).Build()
	if err := deployUsageLogsCollector(context.Background(), cli, m, cm, &sources); err != nil {
		t.Fatalf("deployUsageLogsCollector error: %v", err)
	}

	c := findCondition(m, conditions.ConditionUsageLogsCollectorAvailable)
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != "ForwardingOnly" {
		t.Fatalf("UsageLogsCollectorAvailable: want True/ForwardingOnly, got %v", c)
	}

	var pipelineSources []rendertemplate.TemplateSource
	if err := deployUsageLogsLokiStack(context.Background(), cli, m,
		conditions.NewConditionsManager(m, m.Generation), &pipelineSources); err != nil {
		t.Fatalf("deployUsageLogsLokiStack error: %v", err)
	}
	if len(pipelineSources) != len(sources) {
		t.Errorf("forwarding alone must render only collector resources, got %d sources, want %d", len(pipelineSources), len(sources))
	}

	data, err := buildTemplateData(context.Background(), cli, m, "")
	if err != nil {
		t.Fatalf("buildTemplateData error: %v", err)
	}
	if got := data["UsageLogs"]; got != false {
		t.Errorf("UsageLogs: want false without storage, got %v", got)
	}
	if got := data["UsageLogsEndpoint"]; got != "" {
		t.Errorf("UsageLogsEndpoint: want empty without storage, got %v", got)
	}

	objects, err := rendertemplate.Render(context.Background(), s, sources, data)
	if err != nil {
		t.Fatalf("rendering usage logs templates: %v", err)
	}

	var collector *unstructured.Unstructured
	for i := range objects {
		if objects[i].GetKind() == "OpenTelemetryCollector" {
			collector = objects[i].DeepCopy()
		}
	}
	if collector == nil {
		t.Fatal("no OpenTelemetryCollector rendered")
	}

	exporters, _, _ := unstructured.NestedMap(collector.Object, "spec", "config", "exporters")
	if _, ok := exporters["otlphttp/loki"]; ok {
		t.Error("the local exporter must not render without storage")
	}
	if _, ok := exporters["otlphttp/hub"]; !ok {
		t.Errorf("remote exporter missing; got %v", slices.Sorted(maps.Keys(exporters)))
	}

	pipelines, _, _ := unstructured.NestedMap(collector.Object, "spec", "config", "service", "pipelines")
	if _, ok := pipelines["logs"]; ok {
		t.Error("the local pipeline must not render without storage: an empty exporter list is invalid collector config")
	}
	remote, ok := pipelines["logs/remote"].(map[string]any)
	if !ok {
		t.Fatalf("logs/remote pipeline missing; got %v", slices.Sorted(maps.Keys(pipelines)))
	}
	if exp, _, _ := unstructured.NestedStringSlice(remote, "exporters"); !slices.Equal(exp, []string{"otlphttp/hub"}) {
		t.Errorf("logs/remote exporters: want [otlphttp/hub], got %v", exp)
	}
}

func TestDeployUsageLogsCollector_CredentialWithoutExporters(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.OpenTelemetryCollector)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{
		Storage: &v1alpha1.LokiStorageConfig{
			Type:           "s3",
			SecretName:     "usage-logs-secret",
			CredentialMode: "static",
		},
		CredentialSecret: "hub-usage-logs-auth",
	}
	m.Status.UsageLogsEndpoint = usageLogsEndpoint

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource
	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(kubernetesAPIServerEndpointSlice()).Build()
	if err := deployUsageLogsCollector(context.Background(), cli, m, cm, &sources); err != nil {
		t.Fatalf("deployUsageLogsCollector error: %v", err)
	}

	data, err := buildTemplateData(context.Background(), cli, m, "")
	if err != nil {
		t.Fatalf("buildTemplateData error: %v", err)
	}
	objects, err := rendertemplate.Render(context.Background(), s, sources, data)
	if err != nil {
		t.Fatalf("rendering usage logs templates: %v", err)
	}

	var collector *unstructured.Unstructured
	for i := range objects {
		if objects[i].GetKind() == "OpenTelemetryCollector" {
			collector = objects[i].DeepCopy()
		}
	}
	if collector == nil {
		t.Fatal("no OpenTelemetryCollector rendered")
	}

	if vols, found, _ := unstructured.NestedSlice(collector.Object, "spec", "volumes"); found && len(vols) > 0 {
		t.Errorf("credential mounted with no exporter to authenticate: %v", vols)
	}
	if mounts, found, _ := unstructured.NestedSlice(collector.Object, "spec", "volumeMounts"); found && len(mounts) > 0 {
		t.Errorf("credential mount rendered with no exporter to authenticate: %v", mounts)
	}

	exts, _, _ := unstructured.NestedMap(collector.Object, "spec", "config", "extensions")
	if _, present := exts["bearertokenauth/remote"]; present {
		t.Error("bearertokenauth/remote declared with no exporter to authenticate")
	}
	if enabled, _, _ := unstructured.NestedStringSlice(collector.Object, "spec", "config", "service", "extensions"); slices.Contains(enabled, "bearertokenauth/remote") {
		t.Errorf("bearertokenauth/remote enabled with no exporter to authenticate: %v", enabled)
	}

	// The local write is untouched: still one pipeline, still the SA token.
	pipelines, _, _ := unstructured.NestedMap(collector.Object, "spec", "config", "service", "pipelines")
	if _, present := pipelines["logs/remote"]; present {
		t.Error("logs/remote pipeline rendered with no exporters")
	}
	if fn, _, _ := unstructured.NestedString(exts, "bearertokenauth", "filename"); fn != "/var/run/secrets/kubernetes.io/serviceaccount/token" {
		t.Errorf("local write credential changed: %q", fn)
	}
}
