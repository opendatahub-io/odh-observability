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

	c := mustFindCondition(t, m, conditions.ConditionClusterLogForwarderAvailable)
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

	c := mustFindCondition(t, m, conditions.ConditionClusterLogForwarderAvailable)
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

	c := mustFindCondition(t, m, conditions.ConditionClusterLogForwarderAvailable)
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
	err := deployLokiStack(context.Background(), cli, m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Log forwarding brings its own stack and the network policies guarding it,
	// and nothing belonging to usage logs.
	assertTemplatePaths(t, sources, []string{
		LokiStackTemplate,
		LokiStackNetworkPolicyTemplate,
		LokiStackGatewayNetworkPolicyTemplate,
	})

	c := mustFindCondition(t, m, conditions.ConditionUsageLogsLokiStackAvailable)
	if c.Status != metav1.ConditionFalse || c.Reason != "LokiNotRequired" {
		t.Errorf("UsageLogsLokiStackAvailable: expected False/LokiNotRequired, got %s/%s", c.Status, c.Reason)
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

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(kubernetesAPIServerEndpointSlice()).Build()
	err := deployLokiStack(context.Background(), cli, m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Usage logs brings its own stack and the policy guarding that stack's
	// gateway. The policy belongs to the stack rather than to the collector
	// that dials it, so it is in place for the whole startup window.
	assertTemplatePaths(t, sources, []string{UsageLogsLokiStackTemplate, UsageLogsLokiStackNPTemplate})

	// Log forwarding is not configured, so its stack must stay out of the way.
	c := mustFindCondition(t, m, conditions.ConditionLokiStackAvailable)
	if c.Status != metav1.ConditionFalse || c.Reason != "LokiNotRequired" {
		t.Errorf("LokiStackAvailable: expected False/LokiNotRequired, got %s/%s", c.Status, c.Reason)
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
	err := deployLokiStack(context.Background(), cli, m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertTemplatePaths(t, sources, []string{
		UsageLogsLokiStackTemplate,
		UsageLogsLokiStackNPTemplate,
		LokiStackTemplate,
		LokiStackNetworkPolicyTemplate,
		LokiStackGatewayNetworkPolicyTemplate,
	})

	for _, conditionType := range []string{
		conditions.ConditionUsageLogsLokiStackAvailable,
		conditions.ConditionLokiStackAvailable,
	} {
		if c := mustFindCondition(t, m, conditionType); c.Status != metav1.ConditionTrue {
			t.Errorf("%s: expected True once the stack is ready, got %s/%s", conditionType, c.Status, c.Reason)
		}
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
	err := deployLokiStack(context.Background(), cli, m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sources) != 0 {
		t.Fatalf("expected no sources when neither feature is configured, got %d", len(sources))
	}

	for _, conditionType := range []string{
		conditions.ConditionUsageLogsLokiStackAvailable,
		conditions.ConditionLokiStackAvailable,
	} {
		c := mustFindCondition(t, m, conditionType)
		if c.Status != metav1.ConditionFalse || c.Severity != platformcommon.ConditionSeverityInfo {
			t.Errorf("%s: expected False+Info when not configured, got %s/%s", conditionType, c.Status, c.Severity)
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

	cli := fake.NewClientBuilder().WithScheme(s).WithInterceptorFuncs(missingCRDs(gvk.LokiStack)).Build()
	err := deployLokiStack(context.Background(), cli, m, cm, &sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
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

func TestCheckLokiStorageIsolation(t *testing.T) {
	const ns = "test-ns"

	tests := []struct {
		name         string
		secrets      []client.Object
		monitoring   *v1alpha1.Monitoring
		wantConflict bool
		wantBuckets  string
		// wantCheckReason names the reason a *lokiStorageCheckError must
		// carry. Set it for the cases where the question could not be
		// answered, as opposed to answered with a conflict.
		wantCheckReason string
	}{
		{
			// Distinct Secret names are not evidence of separation.
			name: "same bucket through different secrets conflicts",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "https://s3.example.com", "shared-bucket"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "shared-bucket"),
			},
			monitoring:   monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantConflict: true,
			wantBuckets:  "https://s3.example.com/shared-bucket",
		},
		{
			name: "the same secret for both stacks conflicts",
			secrets: []client.Object{
				lokiStorageSecret("one-secret", ns, "https://s3.example.com", "shared-bucket"),
			},
			monitoring:   monitoringWithBothLokiStorage("one-secret", "one-secret"),
			wantConflict: true,
			wantBuckets:  "https://s3.example.com/shared-bucket",
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
			wantBuckets:  "https://s3.example.com/shared-bucket",
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
			wantCheckReason: lokiStorageSecretMissingReason,
		},
		{
			// The GitOps ordering case, and the one the fail-open bug was
			// actually reachable through: the CR is applied before either
			// Secret exists, so neither resolves and nothing can be compared.
			name:            "both secrets missing withholds both stacks",
			monitoring:      monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
			wantCheckReason: lokiStorageSecretMissingReason,
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
			wantCheckReason: lokiStorageSecretInvalidReason,
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
			wantCheckReason: lokiStorageSecretInvalidReason,
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
			wantBuckets:  "https://s3.example.com/shared-bucket",
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
			wantBuckets:  "https://s3.example.com/shared-bucket",
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
			// http and https are configured as separate backends; normalizing
			// them together would invent a conflict that does not exist.
			name: "same bucket over http and https is accepted",
			secrets: []client.Object{
				lokiStorageSecret("usage-logs-secret", ns, "http://s3.example.com", "logs"),
				lokiStorageSecret("logs-secret", ns, "https://s3.example.com", "logs"),
			},
			monitoring: monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret"),
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newActionsTestScheme(t)
			cli := fake.NewClientBuilder().WithScheme(s).WithObjects(tt.secrets...).Build()

			err := checkLokiStorageIsolation(context.Background(), cli, tt.monitoring)

			// Unresolvable storage is a third outcome, distinct from both
			// "isolated" and "conflicting": nothing was compared, so the
			// stacks are withheld under a reason that says what to fix
			// rather than one that reads as an operator malfunction.
			if tt.wantCheckReason != "" {
				var checkErr *lokiStorageCheckError
				if !errors.As(err, &checkErr) {
					t.Fatalf("expected a *lokiStorageCheckError, got %T (%v)", err, err)
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
			var conflict *lokiStorageConflictError
			if !errors.As(err, &conflict) {
				t.Fatalf("expected a *lokiStorageConflictError, got %T (%v)", err, err)
			}
			if !strings.Contains(err.Error(), tt.wantBuckets) {
				t.Errorf("error should name the shared location %q, got %v", tt.wantBuckets, err)
			}
		})
	}
}

// TestDeployLokiStack_SharedBucketRejected covers the conflict stopping the
// action before anything is applied. Keeping a running stack alive is the
// caller's job, covered by TestStorageConflictShieldsLokiFromGarbageCollection.
func TestDeployLokiStack_SharedBucketRejected(t *testing.T) {
	s := newActionsTestScheme(t)
	registerCRDs(s, gvk.LokiStack)

	m := monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret")

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(
		lokiStorageSecret("usage-logs-secret", m.Spec.Namespace, "https://s3.example.com", "shared-bucket"),
		lokiStorageSecret("logs-secret", m.Spec.Namespace, "https://s3.example.com", "shared-bucket"),
	).Build()

	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployLokiStack(context.Background(), cli, m, cm, &sources)
	if err == nil {
		t.Fatal("expected deployLokiStack to reject the shared bucket")
	}

	// The typed error survives the caller boundary, so the reconciler can tell
	// a rejected configuration from a check that could not run.
	var conflict *lokiStorageConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("expected a *lokiStorageConflictError, got %T (%v)", err, err)
	}

	if len(sources) != 0 {
		t.Fatalf("expected nothing to be applied while the storage conflicts, got %d sources", len(sources))
	}

	for _, conditionType := range []string{
		conditions.ConditionUsageLogsLokiStackAvailable,
		conditions.ConditionLokiStackAvailable,
	} {
		// False, unlike the unreadable-Secret case below: the buckets were
		// compared and found to overlap. That is a determinate answer and the
		// user's to fix, so it degrades rather than reporting Unknown.
		c := mustFindCondition(t, m, conditionType)
		if c.Status != metav1.ConditionFalse || c.Reason != lokiStorageConflictReason {
			t.Errorf("%s: expected False/%s, got %s/%s", conditionType, lokiStorageConflictReason, c.Status, c.Reason)
		}
	}
}

// TestDeployLokiStack_StorageSecretUnreadableWithholdsBothStacks covers the
// isolation check failing closed. An unreadable Secret leaves the overlap
// question unanswered, and answering "no conflict" would let two stacks share a
// bucket and overwrite each other's index and chunks.
func TestDeployLokiStack_StorageSecretUnreadableWithholdsBothStacks(t *testing.T) {
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
	var sources []rendertemplate.TemplateSource

	err := deployLokiStack(context.Background(), cli, m, cm, &sources)
	if err == nil {
		t.Fatal("expected deployLokiStack to fail closed when the storage secret cannot be read")
	}

	var checkErr *lokiStorageCheckError
	if !errors.As(err, &checkErr) {
		t.Fatalf("expected a *lokiStorageCheckError, got %T (%v)", err, err)
	}
	// The API error stays reachable through the wrapper, so callers can still
	// tell a permission problem from any other reason the check did not run.
	if !k8serr.IsForbidden(err) {
		t.Errorf("expected the Forbidden cause to remain unwrappable, got %v", err)
	}

	if len(sources) != 0 {
		t.Fatalf("expected neither stack to be applied while isolation is unverifiable, got %d sources", len(sources))
	}

	for _, conditionType := range []string{
		conditions.ConditionUsageLogsLokiStackAvailable,
		conditions.ConditionLokiStackAvailable,
	} {
		// Unknown, not False: the Secret could not be read, so nothing was
		// established about either stack. False would clear the published
		// endpoint and take the collector down with it, which is a heavy
		// response to a permission blip. The stacks are withheld either way.
		c := mustFindCondition(t, m, conditionType)
		if c.Status != metav1.ConditionUnknown || c.Reason != lokiStorageCheckFailedReason {
			t.Errorf("%s: expected Unknown/%s, got %s/%s", conditionType, lokiStorageCheckFailedReason, c.Status, c.Reason)
		}
	}
}

// TestDeployLokiStack_MissingStorageSecretsWithholdBothStacks is the CWE-636
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
func TestDeployLokiStack_MissingStorageSecretsWithholdBothStacks(t *testing.T) {
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
	var sources []rendertemplate.TemplateSource

	err := deployLokiStack(context.Background(), cli, m, cm, &sources)
	if err == nil {
		t.Fatal("expected deployLokiStack to fail closed while the storage secrets do not exist")
	}

	var checkErr *lokiStorageCheckError
	if !errors.As(err, &checkErr) {
		t.Fatalf("expected a *lokiStorageCheckError, got %T (%v)", err, err)
	}
	if !k8serr.IsNotFound(err) {
		t.Errorf("the NotFound cause must stay unwrappable, got %v", err)
	}

	// The decisive assertion. A stack that is never rendered is never
	// created, which is the only point at which this is preventable.
	if len(sources) != 0 {
		t.Fatalf("expected neither stack to be rendered before its storage resolves, got %d sources", len(sources))
	}

	for _, conditionType := range []string{
		conditions.ConditionUsageLogsLokiStackAvailable,
		conditions.ConditionLokiStackAvailable,
	} {
		// Unknown with a reason that names the actual problem. False would
		// clear the published endpoint and take the collector down over a
		// Secret that is simply still on its way.
		c := mustFindCondition(t, m, conditionType)
		if c.Status != metav1.ConditionUnknown || c.Reason != lokiStorageSecretMissingReason {
			t.Errorf("%s: expected Unknown/%s, got %s/%s",
				conditionType, lokiStorageSecretMissingReason, c.Status, c.Reason)
		}
	}
}

// TestDeployLokiStack_UnobservableStateReportsUnknown covers the API calls in
// deployLokiStack that can fail without telling us anything about the stacks.
//
// The caller holds this action's error instead of returning it, so the
// reconcile runs on to publish endpoints and aggregate Ready from whatever
// these conditions say. Returning without marking would leave the previous
// round's True standing and report a healthy stack on the strength of a check
// that never ran. Unknown rather than False, because the condition decides
// whether the published endpoint is kept, and False would clear it — taking
// the usage logs collector and its policies down with it over one failed call.
func TestDeployLokiStack_UnobservableStateReportsUnknown(t *testing.T) {
	denied := func(resource string) error {
		return k8serr.NewForbidden(schema.GroupResource{Resource: resource}, "", errors.New("no permission"))
	}

	for _, test := range []struct {
		name        string
		intercept   interceptor.Funcs
		wantReason  string
		wantErrText string
	}{
		{
			name: "CRD discovery fails",
			intercept: interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if list.GetObjectKind().GroupVersionKind().Kind == gvk.LokiStack.Kind+"List" {
						return denied("customresourcedefinitions")
					}
					return c.List(ctx, list, opts...)
				},
			},
			wantReason:  lokiCRDCheckFailedReason,
			wantErrText: "checking LokiStack CRD",
		},
		{
			// Not a missing stack: isLokiStackReady maps NotFound to
			// (false, nil), so an error here is only ever "could not look".
			name: "readiness Get fails",
			intercept: interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if obj.GetObjectKind().GroupVersionKind().Kind == gvk.LokiStack.Kind {
						return denied("lokistacks")
					}
					return c.Get(ctx, key, obj, opts...)
				},
			},
			wantReason:  lokiReadinessCheckFailedReason,
			wantErrText: "LokiStack readiness",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newActionsTestScheme(t)
			registerCRDs(s, gvk.LokiStack)

			m := monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret")
			cli := fake.NewClientBuilder().WithScheme(s).WithObjects(
				lokiStorageSecret("usage-logs-secret", m.Spec.Namespace, "https://s3.example.com", "usage-logs-bucket"),
				lokiStorageSecret("logs-secret", m.Spec.Namespace, "https://s3.example.com", "logs-bucket"),
			).WithInterceptorFuncs(test.intercept).Build()

			cm := conditions.NewConditionsManager(m, m.Generation)

			// Both stacks were up as of the previous round. The manager
			// preserves conditions across reconciles, so without this the
			// assertions below pass on absence rather than on invalidation.
			cm.MarkTrue(conditions.ConditionUsageLogsLokiStackAvailable)
			cm.MarkTrue(conditions.ConditionLokiStackAvailable)

			var sources []rendertemplate.TemplateSource

			err := deployLokiStack(context.Background(), cli, m, cm, &sources)
			if err == nil {
				t.Fatal("expected deployLokiStack to return the failure")
			}
			if !strings.Contains(err.Error(), test.wantErrText) {
				t.Errorf("error should mention %q, got %v", test.wantErrText, err)
			}
			if len(sources) != 0 {
				t.Errorf("expected nothing rendered from a failed check, got %d sources", len(sources))
			}

			// Both stacks are configured, so both conditions must be
			// invalidated — including the log forwarding one, which the
			// readiness path never reaches because the usage logs check
			// fails first and returns. An unchecked condition that keeps
			// its stale True lets publishLokiEndpoints republish an endpoint
			// nothing verified this round.
			for _, condType := range []string{
				conditions.ConditionUsageLogsLokiStackAvailable,
				conditions.ConditionLokiStackAvailable,
			} {
				c := mustFindCondition(t, m, condType)
				if c.Status != metav1.ConditionUnknown || c.Reason != test.wantReason {
					t.Errorf("%s: expected Unknown/%s, got %s/%s",
						condType, test.wantReason, c.Status, c.Reason)
				}
			}
		})
	}
}

// TestDeployLokiStack_SiblingInvalidationIsScoped bounds the invalidation that
// TestDeployLokiStack_UnobservableStateReportsUnknown requires.
//
// Invalidating an unchecked sibling is right because its condition would
// otherwise answer for a check that did not run. That reasoning stops at two
// edges, and crossing either does fresh damage: a sibling the user never
// configured has nothing to invalidate, and a sibling already checked this
// round has a current answer that must not be thrown away.
func TestDeployLokiStack_SiblingInvalidationIsScoped(t *testing.T) {
	failLokiStackGet := interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if obj.GetObjectKind().GroupVersionKind().Kind == gvk.LokiStack.Kind {
				return k8serr.NewTimeoutError("request timed out", 1)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}

	t.Run("an unconfigured sibling keeps its not-configured condition", func(t *testing.T) {
		s := newActionsTestScheme(t)
		registerCRDs(s, gvk.LokiStack)

		// Usage logs only. Log forwarding is off, so deployLokiStack marks it
		// not-configured at the top and the readiness failure below must
		// leave that alone.
		m := newMonitoring(v1alpha1.MonitoringInstanceName)
		m.Spec.UsageLogs = &v1alpha1.UsageLogs{
			Storage: &v1alpha1.LokiStorageConfig{Type: "s3", SecretName: "usage-logs-secret"},
		}

		cli := fake.NewClientBuilder().WithScheme(s).WithInterceptorFuncs(failLokiStackGet).Build()
		cm := conditions.NewConditionsManager(m, m.Generation)
		var sources []rendertemplate.TemplateSource

		if err := deployLokiStack(context.Background(), cli, m, cm, &sources); err == nil {
			t.Fatal("expected the timed-out readiness check to be returned")
		}

		// Info severity is what keeps a feature the user turned off out of
		// the Degraded and Unknown aggregation. Overwriting it with Unknown
		// would report the whole operand as not-ready over a stack nobody
		// asked for.
		c := mustFindCondition(t, m, conditions.ConditionLokiStackAvailable)
		if c.Status != metav1.ConditionFalse || c.Reason != "LokiNotRequired" {
			t.Errorf("LokiStackAvailable: expected the not-configured condition to survive, got %s/%s",
				c.Status, c.Reason)
		}
		if c.Severity != platformcommon.ConditionSeverityInfo {
			t.Errorf("LokiStackAvailable: expected Info severity to survive, got %q", c.Severity)
		}

		cm.MarkTrue(conditions.ConditionMonitoringDependenciesReady)
		cm.AggregateReady()
		if got := mustFindCondition(t, m, string(platformcommon.ConditionTypeDegraded)); got.Status != metav1.ConditionFalse {
			t.Errorf("Degraded: an unobservable stack is not a degradation, got %s", got.Status)
		}
	})

	t.Run("a sibling checked this round keeps its fresh answer", func(t *testing.T) {
		s := newActionsTestScheme(t)
		registerCRDs(s, gvk.LokiStack)

		m := monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret")

		// The usage logs stack is checked first and is up; only the log
		// forwarding Get fails. The usage logs answer is current, so it must
		// stay True rather than being swept up in the invalidation.
		cli := fake.NewClientBuilder().WithScheme(s).WithObjects(
			lokiStorageSecret("usage-logs-secret", m.Spec.Namespace, "https://s3.example.com", "usage-logs-bucket"),
			lokiStorageSecret("logs-secret", m.Spec.Namespace, "https://s3.example.com", "logs-bucket"),
			readyLokiStack(usageLogsLokiStackResourceName, m.Spec.Namespace),
		).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if obj.GetObjectKind().GroupVersionKind().Kind == gvk.LokiStack.Kind &&
					key.Name == lokiStackResourceName {
					return k8serr.NewTimeoutError("request timed out", 1)
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).Build()

		cm := conditions.NewConditionsManager(m, m.Generation)
		var sources []rendertemplate.TemplateSource

		if err := deployLokiStack(context.Background(), cli, m, cm, &sources); err == nil {
			t.Fatal("expected the timed-out readiness check to be returned")
		}

		if c := mustFindCondition(t, m, conditions.ConditionUsageLogsLokiStackAvailable); c.Status != metav1.ConditionTrue {
			t.Errorf("UsageLogsLokiStackAvailable: a stack verified this round must stay True, got %s/%s",
				c.Status, c.Reason)
		}
		if c := mustFindCondition(t, m, conditions.ConditionLokiStackAvailable); c.Status != metav1.ConditionUnknown {
			t.Errorf("LokiStackAvailable: expected Unknown, got %s/%s", c.Status, c.Reason)
		}
	})
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
			wantTemplates: []string{
				LokiStackTemplate,
				LokiStackNetworkPolicyTemplate,
				LokiStackGatewayNetworkPolicyTemplate,
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

			if err := deployLokiStack(context.Background(), cli, m, cm, &sources); err != nil {
				t.Fatalf("an absent stack is not an error, it is the state before the first apply: %v", err)
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
	if err := deployLokiStack(context.Background(), cli, m, cm, &sources); err == nil {
		t.Fatal("expected deployLokiStack to return the timeout")
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

// readyLokiStack builds a LokiStack CR reporting Ready=True, the state the
// readiness check in deployLokiStack looks for.
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

func TestDeployUsageLogsCollector_NoStorage(t *testing.T) {
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
		t.Errorf("expected no sources when usageLogs has no storage, got %d", len(sources))
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
// The two policies are contributed by different actions — the collector policy
// by deployUsageLogsCollector, the gateway policy by deployLokiStack, which
// owns it because the gateway has to be guarded before any collector exists to
// dial it. Splitting this test along that seam would lose the only check that
// the gateway policy's "from" peer and the collector policy's podSelector
// still refer to the same workload.
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
	if err := deployUsageLogsCollector(context.Background(), cli, m, cm, &sources); err != nil {
		t.Fatalf("deployUsageLogsCollector error: %v", err)
	}

	c := findCondition(m, conditions.ConditionUsageLogsCollectorAvailable)
	if c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("UsageLogsCollectorAvailable: expected True, got %v", c)
	}
	// By path, not by count: the gateway policy leaving this action and the
	// collector policy arriving in its place would keep the count at three.
	// The gateway policy must not be here — it has to outlive the collector's
	// readiness gate, which is the whole reason it moved to deployLokiStack.
	assertTemplatePaths(t, sources, []string{
		UsageLogsOpenTelemetryCollectorTemplate,
		UsageLogsOpenTelemetryCollectorRBACTemplate,
		UsageLogsOpenTelemetryCollectorNPTemplate,
	})

	if err := deployLokiStack(context.Background(), cli, m, cm, &sources); err != nil {
		t.Fatalf("deployLokiStack error: %v", err)
	}

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
