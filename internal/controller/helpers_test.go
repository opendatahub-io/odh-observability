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
	"encoding/base64"
	"testing"

	odhLabels "github.com/opendatahub-io/odh-platform-utilities/pkg/metadata/labels"
	routev1 "github.com/openshift/api/route/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
)

// --- hasCRD ---

func TestHasCRD_Present(t *testing.T) {
	s := newTestScheme(t)
	registerCRDs(s, gvk.MonitoringStack)

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	found, err := hasCRD(context.Background(), cli, gvk.MonitoringStack)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Error("expected hasCRD to return true when CRD is registered")
	}
}

func TestHasCRD_MultipleCRDsRegistered(t *testing.T) {
	s := newTestScheme(t)
	registerCRDs(s, gvk.MonitoringStack, gvk.ThanosQuerier)

	cli := fake.NewClientBuilder().WithScheme(s).Build()

	found, err := hasCRD(context.Background(), cli, gvk.MonitoringStack)
	if err != nil || !found {
		t.Error("MonitoringStack should be found")
	}

	found, err = hasCRD(context.Background(), cli, gvk.ThanosQuerier)
	if err != nil || !found {
		t.Error("ThanosQuerier should be found")
	}
}

// --- syncPrometheusWebTLSCA ---

func TestSyncPrometheusWebTLSCA_NoMetrics(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = nil

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	err := syncPrometheusWebTLSCA(context.Background(), cli, m)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSyncPrometheusWebTLSCA_ConfigMapMissing(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	err := syncPrometheusWebTLSCA(context.Background(), cli, m)
	if err != nil {
		t.Fatalf("expected no error when ConfigMap missing, got: %v", err)
	}
}

func TestSyncPrometheusWebTLSCA_ConfigMapPresent(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}

	cm := &unstructured.Unstructured{}
	cm.SetAPIVersion("v1")
	cm.SetKind("ConfigMap")
	cm.SetNamespace(m.Spec.Namespace)
	cm.SetName("prometheus-web-tls-ca")
	_ = unstructured.SetNestedStringMap(cm.Object, map[string]string{
		"service-ca.crt": "-----BEGIN CERTIFICATE-----\ntest-cert\n-----END CERTIFICATE-----",
	}, "data")

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(cm).Build()
	err := syncPrometheusWebTLSCA(context.Background(), cli, m)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	secret := &unstructured.Unstructured{}
	secret.SetAPIVersion("v1")
	secret.SetKind("Secret")
	err = cli.Get(context.Background(), objKey(m.Spec.Namespace, "prometheus-web-tls-ca"), secret)
	if err != nil {
		t.Fatalf("Secret not created: %v", err)
	}

	labels := secret.GetLabels()
	if _, found := labels[odhLabels.PlatformPartOf]; found {
		t.Errorf("Secret should not have %q label (causes GC to delete it every reconcile), got %v", odhLabels.PlatformPartOf, labels)
	}

	data, _, _ := unstructured.NestedStringMap(secret.Object, "data")
	encoded, ok := data["service-ca.crt"]
	if !ok {
		t.Fatal("Secret missing service-ca.crt key")
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("base64 decode failed: %v", err)
	}
	if string(decoded) != "-----BEGIN CERTIFICATE-----\ntest-cert\n-----END CERTIFICATE-----" {
		t.Errorf("decoded cert mismatch: %q", string(decoded))
	}
}

func TestSyncPrometheusWebTLSCA_EmptyData(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}

	cm := &unstructured.Unstructured{}
	cm.SetAPIVersion("v1")
	cm.SetKind("ConfigMap")
	cm.SetNamespace(m.Spec.Namespace)
	cm.SetName("prometheus-web-tls-ca")
	// No data field at all

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(cm).Build()
	err := syncPrometheusWebTLSCA(context.Background(), cli, m)
	if err != nil {
		t.Fatalf("expected no error when ConfigMap has no data: %v", err)
	}
}

// --- syncStatusURL ---

func TestSyncStatusURL_RoutePresent_MultipleIngress(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}

	route := &routev1.Route{
		ObjectMeta: metav1.ObjectMeta{
			Name:      thanosQuerierRouteName,
			Namespace: m.Spec.Namespace,
		},
		Status: routev1.RouteStatus{
			Ingress: []routev1.RouteIngress{
				{Host: "primary.example.com"},
				{Host: "secondary.example.com"},
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(m, route).WithStatusSubresource(route).Build()
	if err := syncStatusURL(context.Background(), cli, m); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should use the first ingress host.
	if m.Status.URL != "https://primary.example.com" {
		t.Errorf("Status.URL: want %q, got %q", "https://primary.example.com", m.Status.URL)
	}
}

func TestSyncStatusURL_EmptyHost(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}
	m.Status.URL = "https://stale.example.com"

	route := &routev1.Route{
		ObjectMeta: metav1.ObjectMeta{
			Name:      thanosQuerierRouteName,
			Namespace: m.Spec.Namespace,
		},
		Status: routev1.RouteStatus{
			Ingress: []routev1.RouteIngress{{Host: ""}},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(m, route).WithStatusSubresource(route).Build()
	if err := syncStatusURL(context.Background(), cli, m); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// When the route exists but the host is empty (ingress not ready), the
	// function preserves the existing URL. It will be updated on the next
	// reconcile when the host becomes available.
	if m.Status.URL != "https://stale.example.com" {
		t.Errorf("Status.URL: want preserved stale value, got %q", m.Status.URL)
	}
}

func TestSyncStatusURL_NoIngress(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{}
	m.Status.URL = "https://stale.example.com"

	route := &routev1.Route{
		ObjectMeta: metav1.ObjectMeta{
			Name:      thanosQuerierRouteName,
			Namespace: m.Spec.Namespace,
		},
		Status: routev1.RouteStatus{Ingress: nil},
	}

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(m, route).WithStatusSubresource(route).Build()
	if err := syncStatusURL(context.Background(), cli, m); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// When the route exists but has no ingress entries yet, the function
	// preserves the existing URL.
	if m.Status.URL != "https://stale.example.com" {
		t.Errorf("Status.URL: want preserved stale value, got %q", m.Status.URL)
	}
}

// --- isLocalServiceEndpoint ---

func TestIsLocalServiceEndpoint(t *testing.T) {
	tests := []struct {
		endpoint string
		want     bool
	}{
		{"http://localhost:9090", true},
		{"http://127.0.0.1:9090", true},
		{"http://127.0.0.2:9090", true},
		{"http://::1:9090", true},
		{"http://prometheus.monitoring.svc.cluster.local:9090", true},
		{"http://myservice.myns.svc:8080", true},
		{"http://single-hostname:9090", true},
		{"http://external.example.com:4317", false},
		{"https://collector.prod.acme.io:4317", false},
		{"", false},
		{"not-a-url", false},
	}

	for _, tc := range tests {
		t.Run(tc.endpoint, func(t *testing.T) {
			got := isLocalServiceEndpoint(tc.endpoint)
			if got != tc.want {
				t.Errorf("isLocalServiceEndpoint(%q): want %v, got %v", tc.endpoint, tc.want, got)
			}
		})
	}
}

// --- determineTLSEnabled ---

func TestDetermineTLSEnabled(t *testing.T) {
	tests := []struct {
		name   string
		traces *v1alpha1.Traces
		want   bool
	}{
		{
			name:   "nil TLS",
			traces: &v1alpha1.Traces{Storage: v1alpha1.TracesStorage{Backend: "pv"}},
			want:   false,
		},
		{
			name:   "TLS disabled",
			traces: &v1alpha1.Traces{Storage: v1alpha1.TracesStorage{Backend: "pv"}, TLS: &v1alpha1.TracesTLS{Enabled: false}},
			want:   false,
		},
		{
			name:   "TLS enabled",
			traces: &v1alpha1.Traces{Storage: v1alpha1.TracesStorage{Backend: "pv"}, TLS: &v1alpha1.TracesTLS{Enabled: true}},
			want:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := determineTLSEnabled(tc.traces)
			if got != tc.want {
				t.Errorf("determineTLSEnabled: want %v, got %v", tc.want, got)
			}
		})
	}
}

// --- getResourceValueOrDefault / getStringValueOrDefault ---

func TestGetResourceValueOrDefault(t *testing.T) {
	tests := []struct {
		value, dflt, want string
	}{
		{"10Gi", "5Gi", "10Gi"},
		{"0", "5Gi", "5Gi"},
		{"", "5Gi", "5Gi"},
	}
	for _, tc := range tests {
		if got := getResourceValueOrDefault(tc.value, tc.dflt); got != tc.want {
			t.Errorf("getResourceValueOrDefault(%q, %q): want %q, got %q", tc.value, tc.dflt, tc.want, got)
		}
	}
}

func TestGetStringValueOrDefault(t *testing.T) {
	tests := []struct {
		value, dflt, want string
	}{
		{"90d", "30d", "90d"},
		{"", "30d", "30d"},
	}
	for _, tc := range tests {
		if got := getStringValueOrDefault(tc.value, tc.dflt); got != tc.want {
			t.Errorf("getStringValueOrDefault(%q, %q): want %q, got %q", tc.value, tc.dflt, tc.want, got)
		}
	}
}

// --- getExporterType ---

func TestGetExporterType(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"otlp/custom", "otlp"},
		{"otlphttp/ext", "otlphttp"},
		{"debug", "debug"},
		{"prometheusremotewrite", "prometheusremotewrite"},
	}

	for _, tc := range tests {
		if got := getExporterType(tc.name); got != tc.want {
			t.Errorf("getExporterType(%q): want %q, got %q", tc.name, tc.want, got)
		}
	}
}

// --- isReservedName ---

func TestIsReservedName(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"prometheus", true},
		{"otlp/tempo", true},
		{"otlp/custom", false},
		{"debug", false},
	}

	for _, tc := range tests {
		if got := isReservedName(tc.name); got != tc.want {
			t.Errorf("isReservedName(%q): want %v, got %v", tc.name, tc.want, got)
		}
	}
}

// --- persesGVKs ---

func TestPersesGVKs(t *testing.T) {
	p, ds, db := persesGVKs("v1alpha2")
	if p != gvk.PersesV1Alpha2 {
		t.Errorf("v1alpha2 Perses GVK mismatch: %v", p)
	}
	if ds != gvk.PersesDatasourceV1Alpha2 {
		t.Errorf("v1alpha2 Datasource GVK mismatch: %v", ds)
	}
	if db != gvk.PersesDashboardV1Alpha2 {
		t.Errorf("v1alpha2 Dashboard GVK mismatch: %v", db)
	}

	p, ds, db = persesGVKs("v1alpha1")
	if p != gvk.PersesV1Alpha1 {
		t.Errorf("v1alpha1 Perses GVK mismatch: %v", p)
	}
	if ds != gvk.PersesDatasourceV1Alpha1 {
		t.Errorf("v1alpha1 Datasource GVK mismatch: %v", ds)
	}
	if db != gvk.PersesDashboardV1Alpha1 {
		t.Errorf("v1alpha1 Dashboard GVK mismatch: %v", db)
	}
}

// --- hasCRDWithVersion ---

func TestHasCRDWithVersion_Found(t *testing.T) {
	s := newTestScheme(t)
	gk := schema.GroupKind{Group: "perses.dev", Kind: "Perses"}

	s.AddKnownTypeWithName(schema.GroupVersionKind{Group: "perses.dev", Version: "v1alpha2", Kind: "Perses"}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{Group: "perses.dev", Version: "v1alpha2", Kind: "PersesList"}, &unstructured.UnstructuredList{})

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	found, err := hasCRDWithVersion(context.Background(), cli, gk, "v1alpha2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Error("expected found for v1alpha2")
	}
}

func TestHasCRDWithVersion_DifferentVersion(t *testing.T) {
	s := newTestScheme(t)
	gk := schema.GroupKind{Group: "perses.dev", Kind: "Perses"}

	// Register v1alpha1 but query for v1alpha2.
	s.AddKnownTypeWithName(schema.GroupVersionKind{Group: "perses.dev", Version: "v1alpha1", Kind: "Perses"}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{Group: "perses.dev", Version: "v1alpha1", Kind: "PersesList"}, &unstructured.UnstructuredList{})

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	found, err := hasCRDWithVersion(context.Background(), cli, gk, "v1alpha1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Error("expected v1alpha1 to be found")
	}
}

func objKey(ns, name string) types.NamespacedName {
	return types.NamespacedName{Namespace: ns, Name: name}
}
