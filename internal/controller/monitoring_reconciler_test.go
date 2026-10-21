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
	"reflect"
	"strings"
	"testing"

	platformcommon "github.com/opendatahub-io/odh-platform-utilities/api/common"
	"github.com/opendatahub-io/odh-platform-utilities/pkg/deploy"
	configv1 "github.com/openshift/api/config/v1"
	routev1 "github.com/openshift/api/route/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	controllergvk "github.com/opendatahub-io/odh-observability/internal/controller/gvk"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(v1alpha1.AddToScheme(s))
	utilruntime.Must(configv1.Install(s))
	utilruntime.Must(routev1.Install(s))
	utilruntime.Must(extv1.AddToScheme(s))
	utilruntime.Must(appsv1.AddToScheme(s))
	utilruntime.Must(batchv1.AddToScheme(s))
	utilruntime.Must(networkingv1.AddToScheme(s))
	utilruntime.Must(rbacv1.AddToScheme(s))
	return s
}

func newTestReconciler(t *testing.T, s *runtime.Scheme, c client.Client) *MonitoringReconciler {
	t.Helper()
	cs := fakeclientset.NewSimpleClientset()
	return &MonitoringReconciler{
		Client:          c,
		Scheme:          s,
		Deployer:        deploy.NewDeployer(deploy.WithFieldOwner("monitoring")),
		DynamicClient:   fakedynamic.NewSimpleDynamicClient(s),
		DiscoveryClient: cs.Discovery(),
	}
}

func newMonitoring(name string) *v1alpha1.Monitoring {
	return &v1alpha1.Monitoring{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Generation: 1,
		},
		Spec: v1alpha1.MonitoringSpec{
			ManagementSpec: platformcommon.ManagementSpec{
				ManagementState: platformcommon.Managed,
			},
			Namespace: "test-ns",
		},
	}
}

func registerOperatorConditionTypes(s *runtime.Scheme) {
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "operators.coreos.com", Version: "v2", Kind: "OperatorCondition",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "operators.coreos.com", Version: "v2", Kind: "OperatorConditionList",
	}, &unstructured.UnstructuredList{})
}

func platformConfigMap(ns, version string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      platformConfigName,
			Namespace: ns,
		},
		Data: map[string]string{
			platformVersionKey: version,
		},
	}
}

func platformConfigMapGetForbidden() interceptor.Funcs {
	return interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if key.Name == platformConfigName {
				return k8serr.NewForbidden(schema.GroupResource{Resource: "configmaps"}, key.Name, errors.New("not allowed"))
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}
}

func operatorConditionListForbidden() interceptor.Funcs {
	return interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			gvk := list.GetObjectKind().GroupVersionKind()
			if gvk.Group == "operators.coreos.com" &&
				(gvk.Kind == "OperatorCondition" || gvk.Kind == "OperatorConditionList") {
				return k8serr.NewForbidden(schema.GroupResource{Resource: "operatorconditions"}, "", errors.New("not allowed"))
			}
			return c.List(ctx, list, opts...)
		},
	}
}

func TestReadPlatformVersion(t *testing.T) {
	const ns = "apps-ns"

	t.Run("missing configmap is standalone", func(t *testing.T) {
		s := newTestScheme(t)
		t.Setenv("POD_NAMESPACE", ns)
		r := newTestReconciler(t, s, fake.NewClientBuilder().WithScheme(s).Build())

		got, err := r.readPlatformVersion(context.Background())
		if err != nil {
			t.Fatalf("NotFound should be standalone, got error: %v", err)
		}
		if got != "" {
			t.Errorf("want empty version, got %q", got)
		}
	})

	t.Run("unexpected get error is returned", func(t *testing.T) {
		s := newTestScheme(t)
		t.Setenv("POD_NAMESPACE", ns)
		cli := fake.NewClientBuilder().WithScheme(s).WithInterceptorFuncs(platformConfigMapGetForbidden()).Build()
		r := newTestReconciler(t, s, cli)

		_, err := r.readPlatformVersion(context.Background())
		if err == nil {
			t.Fatal("expected error for non-NotFound Get failure")
		}
	})

	t.Run("returns platformVersion from configmap", func(t *testing.T) {
		s := newTestScheme(t)
		t.Setenv("POD_NAMESPACE", ns)
		r := newTestReconciler(t, s, fake.NewClientBuilder().WithScheme(s).
			WithObjects(platformConfigMap(ns, "2.20.0")).Build())

		got, err := r.readPlatformVersion(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "2.20.0" {
			t.Errorf("want 2.20.0, got %q", got)
		}
	})
}

func TestValidateMonitoringNamespace(t *testing.T) {
	m := newMonitoring(v1alpha1.MonitoringInstanceName)

	t.Run("unset configuration is allowed", func(t *testing.T) {
		t.Setenv("MONITORING_NAMESPACE", "")
		if err := validateMonitoringNamespace(m); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("matching configuration is allowed", func(t *testing.T) {
		t.Setenv("MONITORING_NAMESPACE", m.Spec.Namespace)
		if err := validateMonitoringNamespace(m); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("empty namespace uses the effective default", func(t *testing.T) {
		emptyNamespace := m.DeepCopy()
		emptyNamespace.Spec.Namespace = ""
		t.Setenv("MONITORING_NAMESPACE", defaultMonitoringNamespace)
		if err := validateMonitoringNamespace(emptyNamespace); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("mismatched configuration is rejected", func(t *testing.T) {
		t.Setenv("MONITORING_NAMESPACE", "redhat-ods-monitoring")
		if err := validateMonitoringNamespace(m); err == nil {
			t.Fatal("expected namespace mismatch error")
		}
	})
}

// TestReconcile_Removed: Monitoring with Removed state should short-circuit, set
// Ready=False and ProvisioningSucceeded=False, and not return an error.
func TestReconcile_Removed(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.ManagementState = platformcommon.Removed

	r := newTestReconciler(t, s, fake.NewClientBuilder().WithScheme(s).WithObjects(m).WithStatusSubresource(m).Build())

	_, err := r.reconcile(context.Background(), m)
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var ready, provisioning, dependenciesReady string
	for _, c := range m.Status.Conditions {
		switch c.Type {
		case string(platformcommon.ConditionTypeReady):
			ready = string(c.Status)
		case string(platformcommon.ConditionTypeProvisioningSucceeded):
			provisioning = string(c.Status)
		case "MonitoringDependenciesReady":
			dependenciesReady = string(c.Status)
		}
	}
	if ready != string(metav1.ConditionFalse) {
		t.Errorf("Ready: want False, got %q", ready)
	}
	if provisioning != string(metav1.ConditionFalse) {
		t.Errorf("ProvisioningSucceeded: want False, got %q", provisioning)
	}
	if dependenciesReady != string(metav1.ConditionFalse) {
		t.Errorf("MonitoringDependenciesReady: want False, got %q", dependenciesReady)
	}
}

// TestReconcile_PreconditionsFailed: required operators are not installed.
// Should report the missing dependencies with installation guidance, set
// Ready=False, and return no reconciliation error.
func TestReconcile_PreconditionsFailed(t *testing.T) {
	s := newTestScheme(t)

	// Register OperatorCondition GVK in the fake client tracker.
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "operators.coreos.com", Version: "v2", Kind: "OperatorCondition",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "operators.coreos.com", Version: "v2", Kind: "OperatorConditionList",
	}, &unstructured.UnstructuredList{})

	// Monitoring CR requesting metrics (so preconditions are checked).
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{
		Storage: &v1alpha1.MetricsStorage{},
	}

	r := newTestReconciler(t, s, fake.NewClientBuilder().WithScheme(s).WithObjects(m).WithStatusSubresource(m).Build())

	_, err := r.reconcile(context.Background(), m)
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var ready string
	var dependenciesReady *platformcommon.Condition
	for _, c := range m.Status.Conditions {
		switch c.Type {
		case string(platformcommon.ConditionTypeReady):
			ready = string(c.Status)
		case "MonitoringDependenciesReady":
			condition := c
			dependenciesReady = &condition
		}
	}
	if ready != string(metav1.ConditionFalse) {
		t.Errorf("Ready: want False, got %q", ready)
	}
	if dependenciesReady == nil {
		t.Fatal("MonitoringDependenciesReady condition was not reported")
		return
	}
	if dependenciesReady.Status != metav1.ConditionFalse {
		t.Errorf("MonitoringDependenciesReady: want False, got %q", dependenciesReady.Status)
	}
	if dependenciesReady.Reason != "MissingOperator" {
		t.Errorf("MonitoringDependenciesReady reason: want MissingOperator, got %q", dependenciesReady.Reason)
	}
	for _, expected := range []string{"Red Hat build of OpenTelemetry", "Cluster Observability Operator", "OperatorHub"} {
		if !strings.Contains(dependenciesReady.Message, expected) {
			t.Errorf("MonitoringDependenciesReady message %q does not contain %q", dependenciesReady.Message, expected)
		}
	}
}

func TestReconcile_PreconditionLookupFailureReturnsError(t *testing.T) {
	s := newTestScheme(t)
	registerOperatorConditionTypes(s)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{
		Storage: &v1alpha1.MetricsStorage{},
	}

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(m).WithStatusSubresource(m).
		WithInterceptorFuncs(operatorConditionListForbidden()).Build()
	r := newTestReconciler(t, s, cli)

	_, err := r.reconcile(context.Background(), m)
	if err == nil {
		t.Fatal("expected lookup error to be returned for controller-runtime retry")
	}
	if !k8serr.IsForbidden(err) {
		t.Fatalf("expected forbidden lookup error, got: %v", err)
	}
	for _, condition := range m.Status.Conditions {
		if condition.Reason == "MissingOperator" {
			t.Errorf("lookup failure was incorrectly reported as a missing operator: %#v", condition)
		}
	}
}

func TestReconcile_LoggingPreconditionsFailed(t *testing.T) {
	s := newTestScheme(t)
	registerOperatorConditionTypes(s)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Logs = &v1alpha1.Logs{Storage: &v1alpha1.LokiStorageConfig{}}

	r := newTestReconciler(t, s, fake.NewClientBuilder().WithScheme(s).WithObjects(m).WithStatusSubresource(m).Build())

	_, err := r.reconcile(context.Background(), m)
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var dependenciesReady *platformcommon.Condition
	for _, c := range m.Status.Conditions {
		if c.Type == "MonitoringDependenciesReady" {
			condition := c
			dependenciesReady = &condition
			break
		}
	}
	if dependenciesReady == nil {
		t.Fatal("MonitoringDependenciesReady condition was not reported")
		return
	}
	if dependenciesReady.Status != metav1.ConditionFalse || dependenciesReady.Reason != "MissingOperator" {
		t.Errorf("MonitoringDependenciesReady: want False/MissingOperator, got %s/%s",
			dependenciesReady.Status, dependenciesReady.Reason)
	}
	for _, expected := range []string{"Loki Operator", "Red Hat OpenShift Logging Operator", "OperatorHub"} {
		if !strings.Contains(dependenciesReady.Message, expected) {
			t.Errorf("MonitoringDependenciesReady message %q does not contain %q", dependenciesReady.Message, expected)
		}
	}
}

// TestReconcile_NothingConfigured: all operators present, nothing configured.
// Should be Ready=True, Degraded=False.
func TestReconcile_NothingConfigured(t *testing.T) {
	s := newTestScheme(t)

	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "operators.coreos.com", Version: "v2", Kind: "OperatorCondition",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "operators.coreos.com", Version: "v2", Kind: "OperatorConditionList",
	}, &unstructured.UnstructuredList{})

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	// No Metrics/Traces configured: no operator precondition checks triggered.

	r := newTestReconciler(t, s, fake.NewClientBuilder().WithScheme(s).WithObjects(m).WithStatusSubresource(m).Build())

	_, err := r.reconcile(context.Background(), m)
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var ready, degraded, dependenciesReady, legacyAvailable string
	for _, c := range m.Status.Conditions {
		switch c.Type {
		case string(platformcommon.ConditionTypeReady):
			ready = string(c.Status)
		case string(platformcommon.ConditionTypeDegraded):
			degraded = string(c.Status)
		case "MonitoringDependenciesReady":
			dependenciesReady = string(c.Status)
		case "MonitoringAvailable":
			legacyAvailable = string(c.Status)
		}
	}
	if ready != string(metav1.ConditionTrue) {
		t.Errorf("Ready: want True, got %q", ready)
	}
	if degraded != string(metav1.ConditionFalse) {
		t.Errorf("Degraded: want False, got %q", degraded)
	}
	if dependenciesReady != string(metav1.ConditionTrue) {
		t.Errorf("MonitoringDependenciesReady: want True, got %q", dependenciesReady)
	}
	if legacyAvailable != dependenciesReady {
		t.Errorf("legacy MonitoringAvailable status %q does not match MonitoringDependenciesReady %q", legacyAvailable, dependenciesReady)
	}
}

func TestReconcile_CleansOperatorNetworkPoliciesInEffectiveNamespace(t *testing.T) {
	t.Setenv("MONITORING_NAMESPACE", defaultMonitoringNamespace)

	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Namespace = ""

	stalePolicy := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      collectorOperatorNetworkPolicyName,
			Namespace: defaultMonitoringNamespace,
			Labels: map[string]string{
				"app.kubernetes.io/component":  openTelemetryCollectorComponent,
				"app.kubernetes.io/instance":   defaultMonitoringNamespace + ".data-science-collector",
				"app.kubernetes.io/managed-by": opentelemetryOperator,
				"app.kubernetes.io/part-of":    openTelemetryPartOf,
			},
		},
	}
	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(m, stalePolicy).WithStatusSubresource(m).Build()
	r := newTestReconciler(t, s, cli)

	if _, err := r.reconcile(context.Background(), m); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	if err := r.Get(context.Background(), client.ObjectKeyFromObject(stalePolicy), &networkingv1.NetworkPolicy{}); !k8serr.IsNotFound(err) {
		t.Fatalf("expected generated policy in effective namespace to be deleted, got %v", err)
	}
}

// TestSyncStatusURL_RoutePresent: when metrics are configured and the route exists
// with a host, status.url is populated.
func TestSyncStatusURL_RoutePresent(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{
		Storage: &v1alpha1.MetricsStorage{},
	}

	route := &routev1.Route{
		ObjectMeta: metav1.ObjectMeta{
			Name:      thanosQuerierRouteName,
			Namespace: m.Spec.Namespace,
		},
		Status: routev1.RouteStatus{
			Ingress: []routev1.RouteIngress{
				{Host: "thanos.example.com"},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(m, route).WithStatusSubresource(route).Build()
	if err := syncStatusURL(context.Background(), c, m); err != nil {
		t.Fatalf("syncStatusURL returned error: %v", err)
	}

	want := "https://thanos.example.com"
	if m.Status.URL != want {
		t.Errorf("Status.URL: want %q, got %q", want, m.Status.URL)
	}
}

// TestSyncStatusURL_RouteMissing: when the route doesn't exist yet, URL is empty.
func TestSyncStatusURL_RouteMissing(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{
		Storage: &v1alpha1.MetricsStorage{},
	}
	m.Status.URL = "https://stale.example.com"

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(m).Build()
	if err := syncStatusURL(context.Background(), c, m); err != nil {
		t.Fatalf("syncStatusURL returned error: %v", err)
	}

	if m.Status.URL != "" {
		t.Errorf("Status.URL: want empty, got %q", m.Status.URL)
	}
}

// TestSyncStatusURL_NoMetrics: when metrics are not configured, URL is cleared.
func TestSyncStatusURL_NoMetrics(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Status.URL = "https://stale.example.com"

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(m).Build()
	if err := syncStatusURL(context.Background(), c, m); err != nil {
		t.Fatalf("syncStatusURL: %v", err)
	}

	if m.Status.URL != "" {
		t.Errorf("Status.URL: want empty, got %q", m.Status.URL)
	}
}

// TestReconcile_ReleasesPopulated: reconcile should populate releases with operator identity.
func TestReconcile_ReleasesPopulated(t *testing.T) {
	s := newTestScheme(t)

	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "operators.coreos.com", Version: "v2", Kind: "OperatorCondition",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "operators.coreos.com", Version: "v2", Kind: "OperatorConditionList",
	}, &unstructured.UnstructuredList{})

	m := newMonitoring(v1alpha1.MonitoringInstanceName)

	r := newTestReconciler(t, s, fake.NewClientBuilder().WithScheme(s).WithObjects(m).WithStatusSubresource(m).Build())

	_, err := r.reconcile(context.Background(), m)
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	releases := m.GetReleaseStatus().Releases
	if len(releases) == 0 {
		t.Fatal("releases not populated")
	}
	if releases[0].Name != v1alpha1.MonitoringServiceName {
		t.Errorf("releases[0].Name: want %q, got %q", v1alpha1.MonitoringServiceName, releases[0].Name)
	}
	if releases[0].RepoURL == "" {
		t.Error("releases[0].RepoURL: want non-empty")
	}
	if got := m.GetReleaseStatus().GetPlatformRelease(); got != "" {
		t.Errorf("platform release: want empty in standalone (no ConfigMap), got %q", got)
	}
}

// TestReconcile_PlatformVersionStampedIntoStatus: when the platform ConfigMap
// is present, reconcile copies platformVersion into status.releases[name=platform].
func TestReconcile_PlatformVersionStampedIntoStatus(t *testing.T) {
	s := newTestScheme(t)

	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "operators.coreos.com", Version: "v2", Kind: "OperatorCondition",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "operators.coreos.com", Version: "v2", Kind: "OperatorConditionList",
	}, &unstructured.UnstructuredList{})

	const (
		ns      = "apps-ns"
		version = "2.20.0"
	)
	t.Setenv("POD_NAMESPACE", ns)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)

	r := newTestReconciler(t, s, fake.NewClientBuilder().WithScheme(s).
		WithObjects(m, platformConfigMap(ns, version)).
		WithStatusSubresource(m).Build())

	_, err := r.reconcile(context.Background(), m)
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	if got := m.GetReleaseStatus().GetPlatformRelease(); got != version {
		t.Errorf("platform release: want %q, got %q", version, got)
	}
	if m.GetReleaseStatus().GetRelease(v1alpha1.MonitoringServiceName) == nil {
		t.Fatal("missing monitoring release entry")
	}
}

// TestReconcile_PlatformVersionNotStampedOnFailedReconcile: a failed reconcile
// must not write status.releases[name=platform], and must not overwrite a
// previously stamped handshake value with the ConfigMap version.
func TestReconcile_PlatformVersionNotStampedOnFailedReconcile(t *testing.T) {
	const (
		ns      = "apps-ns"
		version = "2.20.0"
	)

	t.Run("does not stamp on preconditions failure", func(t *testing.T) {
		s := newTestScheme(t)
		registerOperatorConditionTypes(s)
		t.Setenv("POD_NAMESPACE", ns)

		m := newMonitoring(v1alpha1.MonitoringInstanceName)
		m.Spec.Metrics = &v1alpha1.Metrics{
			Storage: &v1alpha1.MetricsStorage{},
		}

		r := newTestReconciler(t, s, fake.NewClientBuilder().WithScheme(s).
			WithObjects(m, platformConfigMap(ns, version)).
			WithStatusSubresource(m).Build())

		_, err := r.reconcile(context.Background(), m)
		if err != nil {
			t.Fatalf("reconcile returned error: %v", err)
		}
		if got := m.GetReleaseStatus().GetPlatformRelease(); got != "" {
			t.Errorf("platform release: want empty on failed reconcile, got %q", got)
		}
	})

	t.Run("does not overwrite previous stamp on preconditions failure", func(t *testing.T) {
		s := newTestScheme(t)
		registerOperatorConditionTypes(s)
		t.Setenv("POD_NAMESPACE", ns)

		m := newMonitoring(v1alpha1.MonitoringInstanceName)
		m.Spec.Metrics = &v1alpha1.Metrics{
			Storage: &v1alpha1.MetricsStorage{},
		}
		m.GetReleaseStatus().SetPlatformRelease("2.19.0")

		r := newTestReconciler(t, s, fake.NewClientBuilder().WithScheme(s).
			WithObjects(m, platformConfigMap(ns, version)).
			WithStatusSubresource(m).Build())

		_, err := r.reconcile(context.Background(), m)
		if err != nil {
			t.Fatalf("reconcile returned error: %v", err)
		}
		if got := m.GetReleaseStatus().GetPlatformRelease(); got != "2.19.0" {
			t.Errorf("platform release: want preserved %q, got %q", "2.19.0", got)
		}
	})

	t.Run("does not stamp when configmap get fails", func(t *testing.T) {
		s := newTestScheme(t)
		registerOperatorConditionTypes(s)
		t.Setenv("POD_NAMESPACE", ns)

		m := newMonitoring(v1alpha1.MonitoringInstanceName)
		cli := fake.NewClientBuilder().WithScheme(s).WithObjects(m).WithStatusSubresource(m).
			WithInterceptorFuncs(platformConfigMapGetForbidden()).Build()
		r := newTestReconciler(t, s, cli)

		_, err := r.reconcile(context.Background(), m)
		if err == nil {
			t.Fatal("expected reconcile error when platform ConfigMap Get fails")
		}
		if got := m.GetReleaseStatus().GetPlatformRelease(); got != "" {
			t.Errorf("platform release: want empty, got %q", got)
		}
	})
}

// TestPlatformConfigWatch_EnqueuesMonitoring: unlabeled odh-monitoring-config
// updates in POD_NAMESPACE must match the predicate and map to the Monitoring
// singleton. Same-named ConfigMaps in other namespaces must not.
func TestPlatformConfigWatch_EnqueuesMonitoring(t *testing.T) {
	const ns = "apps-ns"
	t.Setenv("POD_NAMESPACE", ns)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      platformConfigName,
			Namespace: ns,
		},
		Data: map[string]string{platformVersionKey: "2.20.0"},
	}

	pred := predicate.NewPredicateFuncs(isPlatformConfigMap)
	if !pred.Create(event.CreateEvent{Object: cm}) {
		t.Fatal("create of unlabeled odh-monitoring-config should pass the platform predicate")
	}
	if !pred.Update(event.UpdateEvent{ObjectOld: cm, ObjectNew: cm}) {
		t.Fatal("update of unlabeled odh-monitoring-config should pass the platform predicate")
	}

	other := cm.DeepCopy()
	other.Name = "unrelated"
	if pred.Update(event.UpdateEvent{ObjectOld: other, ObjectNew: other}) {
		t.Fatal("update of an unrelated ConfigMap should not pass the platform predicate")
	}

	otherNS := cm.DeepCopy()
	otherNS.Namespace = "other-ns"
	if pred.Create(event.CreateEvent{Object: otherNS}) {
		t.Fatal("create of odh-monitoring-config in another namespace should not pass the platform predicate")
	}
	if pred.Update(event.UpdateEvent{ObjectOld: otherNS, ObjectNew: otherNS}) {
		t.Fatal("update of odh-monitoring-config in another namespace should not pass the platform predicate")
	}

	reqs := singletonRequests(context.Background(), cm)
	if len(reqs) != 1 {
		t.Fatalf("want 1 request, got %d", len(reqs))
	}
	if reqs[0].Name != v1alpha1.MonitoringInstanceName {
		t.Errorf("mapped name: want %q, got %q", v1alpha1.MonitoringInstanceName, reqs[0].Name)
	}
}

func TestNamespaceWatchPredicate(t *testing.T) {
	pred := namespaceWatchPredicate()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-a"}}

	if !pred.Create(event.CreateEvent{Object: ns}) {
		t.Fatal("namespace creates should enqueue Monitoring")
	}
	if !pred.Delete(event.DeleteEvent{Object: ns}) {
		t.Fatal("namespace deletes should enqueue Monitoring")
	}
	if pred.Update(event.UpdateEvent{ObjectOld: ns, ObjectNew: ns}) {
		t.Fatal("namespace updates should not enqueue Monitoring")
	}
	if pred.Generic(event.GenericEvent{Object: ns}) {
		t.Fatal("generic namespace events should not enqueue Monitoring")
	}

	t.Run("unchanged namespace update is ignored", func(t *testing.T) {
		if pred.Update(event.UpdateEvent{ObjectOld: ns, ObjectNew: ns.DeepCopy()}) {
			t.Fatal("unchanged namespace updates should not enqueue Monitoring")
		}
	})

	t.Run("system classification change enqueues", func(t *testing.T) {
		newNamespace := ns.DeepCopy()
		newNamespace.Labels = map[string]string{"openshift.io/cluster-monitoring": "true"}
		if !pred.Update(event.UpdateEvent{ObjectOld: ns, ObjectNew: newNamespace}) {
			t.Fatal("system classification changes should enqueue Monitoring")
		}
	})

	t.Run("deletion status change enqueues", func(t *testing.T) {
		newNamespace := ns.DeepCopy()
		deletionTimestamp := metav1.Now()
		newNamespace.DeletionTimestamp = &deletionTimestamp
		newNamespace.Finalizers = []string{"kubernetes"}
		if !pred.Update(event.UpdateEvent{ObjectOld: ns, ObjectNew: newNamespace}) {
			t.Fatal("deletion status changes should enqueue Monitoring")
		}
	})

	t.Run("non-namespace update is ignored", func(t *testing.T) {
		configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "not-a-namespace"}}
		if pred.Update(event.UpdateEvent{ObjectOld: configMap, ObjectNew: configMap.DeepCopy()}) {
			t.Fatal("non-namespace updates should not enqueue Monitoring")
		}
	})
}

func TestGarbageCollectionOwnershipAndNamespaceScope(t *testing.T) {
	monitoring := newMonitoring(v1alpha1.MonitoringInstanceName)
	monitoring.UID = types.UID("monitoring-owner")

	staleTargetRole := ownedGarbageCollectionTestObject(controllergvk.Role, "team-a", targetAllocatorSecretRBACName, monitoring.UID)
	staleTargetBinding := ownedGarbageCollectionTestObject(controllergvk.RoleBinding, "team-a", targetAllocatorSecretRBACName, monitoring.UID)
	otherMonitoringRole := ownedGarbageCollectionTestObject(controllergvk.Role, "team-a", targetAllocatorSecretRBACName, types.UID("other-monitoring"))
	staleMonitoringConfigMap := ownedGarbageCollectionTestObject(schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}, monitoring.Spec.Namespace, "stale-config", monitoring.UID)
	clusterRole := ownedGarbageCollectionTestObject(controllergvk.ClusterRole, "", "stale-cluster-role", monitoring.UID)

	desiredTargetRole := resourceKey{
		gvk:       staleTargetRole.GroupVersionKind(),
		namespace: staleTargetRole.GetNamespace(),
		name:      staleTargetRole.GetName(),
	}
	desired := map[resourceKey]struct{}{desiredTargetRole: {}}

	tests := []struct {
		name         string
		obj          unstructured.Unstructured
		wantMainGC   bool
		wantTargetGC bool
		desired      map[resourceKey]struct{}
	}{
		{
			name:         "cross-namespace target Role uses dedicated collector",
			obj:          staleTargetRole,
			wantMainGC:   false,
			wantTargetGC: true,
		},
		{
			name:         "cross-namespace target RoleBinding uses dedicated collector",
			obj:          staleTargetBinding,
			wantMainGC:   false,
			wantTargetGC: true,
		},
		{
			name:         "another Monitoring owner is preserved",
			obj:          otherMonitoringRole,
			wantMainGC:   false,
			wantTargetGC: false,
		},
		{
			name:         "desired target Role is preserved",
			obj:          staleTargetRole,
			wantMainGC:   false,
			wantTargetGC: false,
			desired:      desired,
		},
		{
			name:         "monitoring namespace resource uses main collector",
			obj:          staleMonitoringConfigMap,
			wantMainGC:   true,
			wantTargetGC: false,
		},
		{
			name: "cluster logging namespace resource uses main collector",
			obj: ownedGarbageCollectionTestObject(
				schema.GroupVersionKind{Group: "networking.k8s.io", Version: "v1", Kind: "NetworkPolicy"},
				clusterLoggingNamespace,
				ClusterLogForwarderNetworkPolicyName,
				monitoring.UID,
			),
			wantMainGC:   true,
			wantTargetGC: false,
		},
		{
			name:         "cluster-scoped resource uses main collector",
			obj:          clusterRole,
			wantMainGC:   true,
			wantTargetGC: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldCollectOwnedResource(
				monitoring, test.obj, test.desired, monitoring.Spec.Namespace, nil); got != test.wantMainGC {
				t.Errorf("main GC eligibility: want %t, got %t", test.wantMainGC, got)
			}
			if got := shouldCollectTargetAllocatorRBAC(monitoring, test.obj, test.desired); got != test.wantTargetGC {
				t.Errorf("TargetAllocator GC eligibility: want %t, got %t", test.wantTargetGC, got)
			}
		})
	}

	withoutUID := monitoring.DeepCopy()
	withoutUID.UID = ""
	if shouldCollectTargetAllocatorRBAC(withoutUID, staleTargetRole, nil) {
		t.Error("GC must fail closed when the Monitoring UID is unavailable")
	}
}

func TestIsOpenTelemetryOperatorNetworkPolicy(t *testing.T) {
	t.Parallel()

	policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
		"app.kubernetes.io/component":  "opentelemetry-targetallocator",
		"app.kubernetes.io/instance":   "opendatahub.data-science-collector",
		"app.kubernetes.io/managed-by": "opentelemetry-operator",
		"app.kubernetes.io/part-of":    "opentelemetry",
	}}}
	if !isOpenTelemetryOperatorNetworkPolicy(policy, "opendatahub", "opentelemetry-targetallocator") {
		t.Fatal("expected the generated TargetAllocator policy to be identified")
	}

	policy.Labels["app.kubernetes.io/managed-by"] = "odh-observability"
	if isOpenTelemetryOperatorNetworkPolicy(policy, "opendatahub", "opentelemetry-targetallocator") {
		t.Fatal("module-owned policy must not be identified as an Operator-generated policy")
	}
}

func TestCleanupOpenTelemetryOperatorNetworkPolicies(t *testing.T) {
	t.Parallel()

	scheme := newTestScheme(t)
	stale := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-science-collector-targetallocator-networkpolicy",
			Namespace: "opendatahub",
			Labels: map[string]string{
				"app.kubernetes.io/component":  "opentelemetry-targetallocator",
				"app.kubernetes.io/instance":   "opendatahub.data-science-collector",
				"app.kubernetes.io/managed-by": "opentelemetry-operator",
				"app.kubernetes.io/part-of":    "opentelemetry",
			},
		},
	}
	reconciler := &MonitoringReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(stale).Build()}
	if err := reconciler.cleanupOpenTelemetryOperatorNetworkPolicies(context.Background(), stale.Namespace); err != nil {
		t.Fatalf("cleanup returned an error: %v", err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(stale), &networkingv1.NetworkPolicy{}); !k8serr.IsNotFound(err) {
		t.Fatalf("expected generated policy to be deleted, got %v", err)
	}
}

func TestOpenTelemetryOperatorNetworkPolicyWatch(t *testing.T) {
	t.Setenv("MONITORING_NAMESPACE", "monitoring-test")
	pred := predicate.NewPredicateFuncs(isOpenTelemetryOperatorNetworkPolicyEvent)
	for _, test := range []struct {
		name      string
		policy    string
		component string
		mutate    func(*networkingv1.NetworkPolicy)
		want      bool
	}{
		{name: "collector recreated", policy: "data-science-collector-collector-networkpolicy", component: "opentelemetry-collector", want: true},
		{name: "allocator recreated", policy: "data-science-collector-targetallocator-networkpolicy", component: "opentelemetry-targetallocator", want: true},
		{name: "other policy name", policy: "user-policy", component: "opentelemetry-collector"},
		{name: "wrong component", policy: "data-science-collector-collector-networkpolicy", component: "opentelemetry-targetallocator"},
		{name: "other namespace", policy: "data-science-collector-collector-networkpolicy", component: "opentelemetry-collector",
			mutate: func(p *networkingv1.NetworkPolicy) { p.Namespace = "other" }},
		{name: "other collector", policy: "data-science-collector-collector-networkpolicy", component: "opentelemetry-collector",
			mutate: func(p *networkingv1.NetworkPolicy) { p.Labels["app.kubernetes.io/instance"] = "monitoring-test.other" }},
		{name: "unlabeled policy", policy: "data-science-collector-collector-networkpolicy", component: "opentelemetry-collector",
			mutate: func(p *networkingv1.NetworkPolicy) { p.Labels = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{
				Name: test.policy, Namespace: "monitoring-test",
				Labels: map[string]string{
					"app.kubernetes.io/component":  test.component,
					"app.kubernetes.io/instance":   "monitoring-test.data-science-collector",
					"app.kubernetes.io/managed-by": "opentelemetry-operator",
					"app.kubernetes.io/part-of":    "opentelemetry",
				},
			}}
			if test.mutate != nil {
				test.mutate(policy)
			}
			if got := pred.Create(event.CreateEvent{Object: policy}); got != test.want {
				t.Fatalf("create event: got %t, want %t", got, test.want)
			}
			if got := pred.Update(event.UpdateEvent{ObjectOld: policy.DeepCopy(), ObjectNew: policy}); got != test.want {
				t.Fatalf("update event: got %t, want %t", got, test.want)
			}
		})
	}
	if pred.Create(event.CreateEvent{Object: &corev1.ConfigMap{}}) {
		t.Fatal("a non-NetworkPolicy event must not trigger legacy policy cleanup")
	}
}

// TestStorageSecretRequests covers the watch that makes failing closed on an
// unresolved storage Secret survivable.
//
// The storage Secrets belong to the user, carry no part-of label, and so are
// invisible to the managed-resource predicate the other Secret watch uses.
// Without this mapping a Secret applied after the Monitoring CR is not noticed
// until the periodic requeue, and both LokiStacks stay withheld for that whole
// window. The mapping runs on every Secret event in the cluster, so what it
// declines to enqueue matters as much as what it enqueues.
func TestStorageSecretRequests(t *testing.T) {
	const ns = "test-ns"

	configured := func() *v1alpha1.Monitoring {
		return monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret")
	}

	for _, test := range []struct {
		name        string
		monitoring  *v1alpha1.Monitoring
		secret      *corev1.Secret
		wantEnqueue bool
	}{
		{
			name:        "the usage logs storage secret enqueues",
			monitoring:  configured(),
			secret:      &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "usage-logs-secret", Namespace: ns}},
			wantEnqueue: true,
		},
		{
			// Both names have to be matched: covering only the first would
			// leave log forwarding waiting on the requeue it was the point
			// of this watch to avoid.
			name:        "the log forwarding storage secret enqueues",
			monitoring:  configured(),
			secret:      &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "logs-secret", Namespace: ns}},
			wantEnqueue: true,
		},
		{
			name:        "an unrelated secret in the monitoring namespace does not",
			monitoring:  configured(),
			secret:      &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "some-other-secret", Namespace: ns}},
			wantEnqueue: false,
		},
		{
			// Secret names are not unique across the cluster, and this sees
			// every namespace. Matching on name alone would have any tenant's
			// "logs-secret" drive reconciliation of the platform's Monitoring.
			name:        "the same name in another namespace does not",
			monitoring:  configured(),
			secret:      &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "logs-secret", Namespace: "other-ns"}},
			wantEnqueue: false,
		},
		{
			// Teardown: the CR is gone, so there is nothing to enqueue and
			// nothing worth logging about it either.
			name:        "no Monitoring CR does not",
			monitoring:  nil,
			secret:      &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "logs-secret", Namespace: ns}},
			wantEnqueue: false,
		},
		{
			// Storage is required within UsageLogs and Logs, but both of
			// those are optional, so the nil checks are reachable.
			name: "a secret named by an unconfigured feature does not",
			monitoring: func() *v1alpha1.Monitoring {
				m := configured()
				m.Spec.Logs = nil
				return m
			}(),
			secret:      &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "logs-secret", Namespace: ns}},
			wantEnqueue: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newActionsTestScheme(t)

			builder := fake.NewClientBuilder().WithScheme(s)
			if test.monitoring != nil {
				builder = builder.WithObjects(test.monitoring)
			}
			r := newTestReconciler(t, s, builder.Build())

			got := r.storageSecretRequests(context.Background(), test.secret)

			if !test.wantEnqueue {
				if len(got) != 0 {
					t.Fatalf("expected no requests, got %v", got)
				}
				return
			}
			want := []reconcile.Request{
				{NamespacedName: types.NamespacedName{Name: v1alpha1.MonitoringInstanceName}},
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("requests: want %v, got %v", want, got)
			}
		})
	}
}

// TestStorageConflictShieldsLokiFromGarbageCollection covers the resolution of
// a rejected storage configuration: validateStorageIsolation stops before either
// stack action runs, so nothing is rendered, and every claimant's component is
// shielded so the collector does not read that absence as "the user removed
// log storage". Collection itself keeps running, which is what separates this
// from disabling GC for the whole reconcile.
//
// It drives shouldCollectOwnedResource against hand-built withheld sets so it
// can cover combinations the reconciler cannot produce in one round, including
// the asymmetric one the two component labels exist for.
// TestReconcile_StorageConflictShieldsBothStacks covers the wiring that
// produces those sets.
func TestStorageConflictShieldsLokiFromGarbageCollection(t *testing.T) {
	ctx := context.Background()
	s := newActionsTestScheme(t)
	registerCRDs(s, controllergvk.LokiStack)

	monitoring := monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret")
	monitoring.UID = types.UID("monitoring-owner")

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(
		kubernetesAPIServerEndpointSlice(),
		lokiStorageSecret("usage-logs-secret", monitoring.Spec.Namespace, "https://s3.example.com", "shared-bucket"),
		lokiStorageSecret("logs-secret", monitoring.Spec.Namespace, "https://s3.example.com", "shared-bucket"),
	).Build()

	cm := conditions.NewConditionsManager(monitoring, monitoring.Generation)
	withheld, lokiErr := validateStorageIsolation(ctx, cli, monitoring, cm)
	if lokiErr == nil {
		t.Fatal("expected the shared bucket to be rejected")
	}

	// The gate owns the shield, so the set it hands back is the contract: both
	// claimants refused, both shielded.
	wantWithheld := map[string]struct{}{componentLokiUsageLogs: {}, componentLokiLogs: {}}
	if !reflect.DeepEqual(withheld, wantWithheld) {
		t.Fatalf("withheld components: want %v, got %v", wantWithheld, withheld)
	}

	// Nothing was rendered, so the desired set is empty and only the shield can
	// save the live resources.
	desired := map[resourceKey]struct{}{}

	ns := monitoring.Spec.Namespace
	labelled := func(gvk schema.GroupVersionKind, name, component string) unstructured.Unstructured {
		obj := ownedGarbageCollectionTestObject(gvk, ns, name, monitoring.UID)
		obj.SetLabels(map[string]string{componentLabel: component})
		return obj
	}

	// Every resource the two stack actions render, by the label its template
	// carries.
	usageLogsResources := []unstructured.Unstructured{
		labelled(controllergvk.LokiStack, usageLogsLokiStackResourceName, componentLokiUsageLogs),
		labelled(controllergvk.NetworkPolicy, usageLogsLokiStackResourceName+"-allow", componentLokiUsageLogs),
	}
	logsResources := []unstructured.Unstructured{
		labelled(controllergvk.LokiStack, lokiStackResourceName, componentLokiLogs),
		labelled(controllergvk.NetworkPolicy, "data-science-lokistack-ingress", componentLokiLogs),
		labelled(controllergvk.NetworkPolicy, "data-science-lokistack-gateway-ingress", componentLokiLogs),
	}
	for _, live := range append(append([]unstructured.Unstructured{}, usageLogsResources...), logsResources...) {
		if shouldCollectOwnedResource(monitoring, live, desired, ns, withheld) {
			t.Errorf("%s %s would be deleted while its storage configuration is rejected",
				live.GetKind(), live.GetName())
		}
	}

	// The reason the two stacks carry different labels. Shielding usage logs
	// must not shield log forwarding: if the user removed spec.logs in the same
	// round, those resources are a deletion the operator owes them, and one
	// claimant's storage problem is no reason to withhold it.
	usageLogsOnly := map[string]struct{}{componentLokiUsageLogs: {}}
	for _, live := range logsResources {
		if live.GroupVersionKind() == controllergvk.LokiStack {
			// Covered by the GVK fallback below, which is deliberately coarser.
			continue
		}
		if !shouldCollectOwnedResource(monitoring, live, desired, ns, usageLogsOnly) {
			t.Errorf("%s %s: log forwarding resources must be collected while only usage logs is withheld",
				live.GetKind(), live.GetName())
		}
	}
	for _, live := range usageLogsResources {
		if shouldCollectOwnedResource(monitoring, live, desired, ns, usageLogsOnly) {
			t.Errorf("%s %s would be deleted while usage logs storage is withheld",
				live.GetKind(), live.GetName())
		}
	}

	// Upgrade case: a stack created before componentLabel existed carries no
	// label, so only the GVK fallback stands between it and deletion. It cannot
	// tell which stack the unlabelled object belongs to, so either component
	// being withheld has to be enough.
	unlabelled := ownedGarbageCollectionTestObject(controllergvk.LokiStack, ns, lokiStackResourceName, monitoring.UID)
	for name, set := range map[string]map[string]struct{}{
		"both withheld":            withheld,
		"usage logs only withheld": usageLogsOnly,
		"log forwarding only withheld": {
			componentLokiLogs: {},
		},
	} {
		if shouldCollectOwnedResource(monitoring, unlabelled, desired, ns, set) {
			t.Errorf("%s: an unlabelled LokiStack from an older release must still be shielded", name)
		}
	}

	// Unrelated resources are still collected; the conflict degrades Loki only.
	staleConfigMap := ownedGarbageCollectionTestObject(
		schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}, ns, "stale-config", monitoring.UID)
	if !shouldCollectOwnedResource(monitoring, staleConfigMap, desired, ns, withheld) {
		t.Error("unrelated stale resources must still be collected, the conflict must not disable garbage collection")
	}

	// Without a conflict the shield lifts entirely, so removing log storage from
	// the spec still prunes. A shield that never lifts would strand them forever.
	//
	// The LokiStack is checked alongside the policies because it is the one
	// resource with a second, GVK-based shield. That fallback has to be gated on
	// a loki component actually being withheld: ungated, it would make a
	// LokiStack uncollectable, and a stack the user has removed from the spec
	// would outlive its configuration indefinitely.
	for _, live := range append(append([]unstructured.Unstructured{unlabelled}, usageLogsResources...), logsResources...) {
		if !shouldCollectOwnedResource(monitoring, live, desired, ns, nil) {
			t.Errorf("%s %s: loki resources absent from the desired set must be collected when nothing is withheld",
				live.GetKind(), live.GetName())
		}
	}
}

// lokiReconcileFixture builds the smallest cluster in which reconcile reaches
// the Loki section: both stacks configured, the three operators their
// preconditions demand installed, and the LokiStack kind registered so CRD
// discovery answers yes. Callers add the storage Secrets, which is the only
// thing the Loki tests below actually vary.
func lokiReconcileFixture(t *testing.T) (*runtime.Scheme, *v1alpha1.Monitoring, []client.Object) {
	t.Helper()

	s := newTestScheme(t)
	registerOperatorConditionTypes(s)
	registerCRDs(s, controllergvk.LokiStack)

	m := monitoringWithBothLokiStorage("usage-logs-secret", "logs-secret")
	m.UID = types.UID("monitoring-owner")

	operators := []string{
		"opentelemetry-operator.v0.158.0-1",
		"loki-operator.v6.4.0",
		"cluster-logging.v6.4.0",
	}
	objects := make([]client.Object, 0, len(operators)+2)
	objects = append(objects, m, kubernetesAPIServerEndpointSlice())
	for _, operator := range operators {
		objects = append(objects, newOperatorCondition(operator))
	}
	return s, m, objects
}

// liveLokiStacks returns the two stacks as they would already exist in the
// cluster, labelled the way their templates label them.
func liveLokiStacks(namespace string) []client.Object {
	stack := func(name, component string) client.Object {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(controllergvk.LokiStack)
		obj.SetNamespace(namespace)
		obj.SetName(name)
		obj.SetLabels(map[string]string{componentLabel: component})
		return obj
	}
	return []client.Object{
		stack(usageLogsLokiStackResourceName, componentLokiUsageLogs),
		stack(lokiStackResourceName, componentLokiLogs),
	}
}

// TestReconcile_StorageConflictWithholdsBothStacks pins the sequencing the
// reconciler owns: the storage gate runs before the two stack actions, and
// when it refuses a configuration neither action runs.
//
// This has to be driven through reconcile. The gate and the actions each hold
// up on their own — their own tests cover that — but the order they run in and
// the decision to skip past them live in reconcile and nowhere else, and a
// helper that re-stated that order would be testing the helper.
func TestReconcile_StorageConflictWithholdsBothStacks(t *testing.T) {
	ctx := context.Background()
	s, m, objects := lokiReconcileFixture(t)

	// One bucket between the two of them.
	objects = append(objects,
		lokiStorageSecret("usage-logs-secret", m.Spec.Namespace, "https://s3.example.com", "shared-bucket"),
		lokiStorageSecret("logs-secret", m.Spec.Namespace, "https://s3.example.com", "shared-bucket"),
	)
	objects = append(objects, liveLokiStacks(m.Spec.Namespace)...)

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(objects...).WithStatusSubresource(m).Build()
	r := newTestReconciler(t, s, cli)

	// The error is not asserted on, and the sweep is not reached. Deploy uses
	// server-side apply, which the fake client cannot do across a mix of typed
	// and unstructured objects, so every reconcile here returns "applying
	// resources" before the held storage error or collectGarbage. The shield
	// itself is covered against shouldCollectOwnedResource directly, above.
	if _, err := r.reconcile(ctx, m); err == nil {
		t.Fatal("expected a rejected storage configuration to fail the reconcile")
	}

	// What this does pin, and the reason it has to run through reconcile:
	// neither stack action ran. Both would have written their own verdict over
	// the gate's — LokiStackNotReady, from observing stacks the operator never
	// agreed to create — so a surviving StorageConflict is the proof that the
	// gate ran first and that the withheld set was honoured.
	for _, conditionType := range []string{
		conditions.ConditionUsageLogsLokiStackAvailable,
		conditions.ConditionLokiStackAvailable,
	} {
		c := mustFindCondition(t, m, conditionType)
		if c.Status != metav1.ConditionFalse || c.Reason != storageConflictReason {
			t.Errorf("%s: expected False/%s, got %s/%s", conditionType, storageConflictReason, c.Status, c.Reason)
		}
	}
}

// TestReconcile_LokiCRDDiscoveryFailureAbortsBeforeGarbageCollection pins the
// property that lets deployLokiStack ask about the CRD at all.
//
// That branch returns with none of its templates appended, and a template
// missing from the desired set is indistinguishable, to the collector, from
// one the user deleted. It is safe only because the error aborts the round the
// way every other action's does: no apply, no sweep, backoff, retry. Hold it
// to the end instead and the sweep deletes the live stack.
//
// Only usage logs are configured. deployClusterLogForwarder asks the same
// question earlier in the action loop and returns a byte-identical message, so
// with Spec.Logs set the error assertion below would pass on that abort
// without the Loki action ever running. Spec.Logs=nil sends the forwarder down
// its not-configured path, and the condition assertion — which only
// deployLokiStack writes — is what proves the check under test is the one that
// fired.
func TestReconcile_LokiCRDDiscoveryFailureAbortsBeforeGarbageCollection(t *testing.T) {
	ctx := context.Background()
	s, m, objects := lokiReconcileFixture(t)
	m.Spec.Logs = nil

	objects = append(objects,
		lokiStorageSecret("usage-logs-secret", m.Spec.Namespace, "https://s3.example.com", "usage-logs-bucket"),
	)

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(objects...).WithStatusSubresource(m).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if list.GetObjectKind().GroupVersionKind().Kind == controllergvk.LokiStack.Kind+"List" {
					return k8serr.NewForbidden(
						schema.GroupResource{Resource: "lokistacks"}, "", errors.New("no permission"))
				}
				return c.List(ctx, list, opts...)
			},
		}).Build()
	r := newTestReconciler(t, s, cli)

	// The stack was up as of the previous round, so the condition below is an
	// invalidation rather than a first write.
	m.Status.Conditions = nil
	conditions.NewConditionsManager(m, m.Generation).
		MarkTrue(conditions.ConditionUsageLogsLokiStackAvailable)

	_, err := r.reconcile(ctx, m)
	if err == nil {
		t.Fatal("expected CRD discovery failure to abort the reconcile")
	}
	if !strings.Contains(err.Error(), "checking LokiStack CRD") {
		t.Errorf("error should name the CRD check, got %v", err)
	}

	// Status is patched on the error path, so the stale True must still be
	// replaced before the abort — otherwise Ready keeps reporting a healthy
	// stack on the strength of a check that never ran.
	c := mustFindCondition(t, m, conditions.ConditionUsageLogsLokiStackAvailable)
	if c.Status != metav1.ConditionUnknown || c.Reason != lokiCRDCheckFailedReason {
		t.Errorf("UsageLogsLokiStackAvailable: expected Unknown/%s, got %s/%s",
			lokiCRDCheckFailedReason, c.Status, c.Reason)
	}

	// The abort is a real abort: usage logs comes first in lokiComponents, so
	// returning its error means the log forwarding action never ran and left
	// no condition behind. Everything downstream — Deploy, the sweep,
	// publishLokiEndpoints — is skipped on the same return.
	if c := findCondition(m, conditions.ConditionLokiStackAvailable); c != nil {
		t.Errorf("LokiStackAvailable: the round should have aborted first, got %s/%s", c.Status, c.Reason)
	}
}

// TestReconcile_LokiReadinessFailureAbortsBeforeGarbageCollection covers the
// stack actions' other error return, which this round deliberately changed.
//
// It used to be held to the end of the reconcile. It is not any more: a deploy
// action is a deploy action, and the first one to fail aborts the round the
// same way the ten ahead of it do. The cost is a lost round when a readiness
// Get blips; the gain is that no deploy action needs a garbage collection
// shield, because an abort reaches neither Deploy nor collectGarbage.
//
// The condition still has to be written. Reconcile patches status on the error
// path, so the stale True from the previous round must be replaced before the
// abort — this is the whole difference between these two error returns and the
// ten other hasCRD sites, which abort silently.
func TestReconcile_LokiReadinessFailureAbortsBeforeGarbageCollection(t *testing.T) {
	ctx := context.Background()
	s, m, objects := lokiReconcileFixture(t)

	// Separate buckets, so the gate passes and the only failure is the Get.
	objects = append(objects,
		lokiStorageSecret("usage-logs-secret", m.Spec.Namespace, "https://s3.example.com", "usage-logs-bucket"),
		lokiStorageSecret("logs-secret", m.Spec.Namespace, "https://s3.example.com", "logs-bucket"),
	)
	objects = append(objects, liveLokiStacks(m.Spec.Namespace)...)

	// Switched off once the reconcile is done, so the survival assertions
	// below read the cluster rather than the fault being injected into it.
	timingOut := true
	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(objects...).WithStatusSubresource(m).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption) error {
				if timingOut && obj.GetObjectKind().GroupVersionKind().Kind == controllergvk.LokiStack.Kind &&
					key.Name == usageLogsLokiStackResourceName {
					return k8serr.NewTimeoutError("request timed out", 1)
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).Build()
	r := newTestReconciler(t, s, cli)

	// Up as of the previous round, so the condition below is an invalidation.
	m.Status.Conditions = nil
	conditions.NewConditionsManager(m, m.Generation).
		MarkTrue(conditions.ConditionUsageLogsLokiStackAvailable)

	_, err := r.reconcile(ctx, m)
	timingOut = false
	if err == nil {
		t.Fatal("expected the timed-out readiness check to abort the reconcile")
	}
	if !strings.Contains(err.Error(), "checking usage logs LokiStack readiness") {
		t.Errorf("error should name the readiness check, got %v", err)
	}

	c := mustFindCondition(t, m, conditions.ConditionUsageLogsLokiStackAvailable)
	if c.Status != metav1.ConditionUnknown || c.Reason != lokiReadinessCheckFailedReason {
		t.Errorf("UsageLogsLokiStackAvailable: expected Unknown/%s, got %s/%s",
			lokiReadinessCheckFailedReason, c.Status, c.Reason)
	}

	// Nothing was swept. The log forwarding stack is still configured and its
	// action never ran, so its resources are absent from the desired set —
	// which is exactly the state a sweep would read as a deletion.
	for _, name := range []string{usageLogsLokiStackResourceName, lokiStackResourceName} {
		live := &unstructured.Unstructured{}
		live.SetGroupVersionKind(controllergvk.LokiStack)
		if err := cli.Get(ctx, types.NamespacedName{Namespace: m.Spec.Namespace, Name: name}, live); err != nil {
			t.Errorf("LokiStack %s must survive an aborted round: %v", name, err)
		}
	}
}

func ownedGarbageCollectionTestObject(gvk schema.GroupVersionKind, namespace, name string, ownerUID types.UID) unstructured.Unstructured {
	controller := true
	obj := unstructured.Unstructured{Object: map[string]any{}}
	obj.SetGroupVersionKind(gvk)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	obj.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: v1alpha1.GroupVersion.String(),
		Kind:       "Monitoring",
		Name:       v1alpha1.MonitoringInstanceName,
		UID:        ownerUID,
		Controller: &controller,
	}})
	return obj
}

// TestPublishLokiEndpoints is the status contract for the two LokiStacks: an
// endpoint is published only while its own stack reports ready, cleared when
// the stack is confirmed not ready so nothing dials an address the stack behind
// it has outlived, and held when readiness could not be observed at all.
//
// That last case is the one with teeth. Clearing UsageLogsEndpoint makes
// deployUsageLogsCollector return early, so garbage collection removes the
// collector and its policies — a response far too destructive for one failed
// API call. Unknown therefore keeps the previous value.
//
// The table deliberately crosses each pipeline's configuration with the other
// pipeline's readiness. The two stacks are independent and each reads its own
// condition, so the failure worth catching is one of them answering for the
// other — which a table that only ever moves both together cannot see.
func TestPublishLokiEndpoints(t *testing.T) {
	t.Parallel()

	const (
		usageLogsEndpoint = "http://data-science-usage-logs-lokistack-gateway-http.test-ns.svc:8080/otlp"
		logsEndpoint      = "https://data-science-lokistack-gateway-http.test-ns.svc:8080"

		// Values from an earlier round where both stacks were up. Every case
		// that does not publish has to overwrite these, not skip them.
		staleUsageLogs = "http://stale-usage-logs"
		staleLogs      = "http://stale-logs"
	)
	data := map[string]any{
		"UsageLogsEndpoint": usageLogsEndpoint,
		"LokiQueryEndpoint": logsEndpoint,
	}

	for _, test := range []struct {
		name                string
		usageLogsConfigured bool
		logsConfigured      bool
		usageLogsStatus     metav1.ConditionStatus
		logsStatus          metav1.ConditionStatus
		// firstReconcile starts from empty endpoints rather than the stale
		// pair, so "hold what you had" can be told apart from "hold a value".
		firstReconcile bool
		wantUsageLogs  string
		wantLogs       string
		wantRequeue    bool
	}{
		{
			name: "neither configured clears both",
			// Readiness is true for both: a condition left over from a
			// configuration the user has since removed must not publish.
			usageLogsStatus: metav1.ConditionTrue,
			logsStatus:      metav1.ConditionTrue,
		},
		{
			name:                "both configured and ready",
			usageLogsConfigured: true, logsConfigured: true,
			usageLogsStatus: metav1.ConditionTrue, logsStatus: metav1.ConditionTrue,
			wantUsageLogs: usageLogsEndpoint, wantLogs: logsEndpoint,
		},
		{
			name:                "both configured, only usage logs ready",
			usageLogsConfigured: true, logsConfigured: true,
			usageLogsStatus: metav1.ConditionTrue, logsStatus: metav1.ConditionFalse,
			wantUsageLogs: usageLogsEndpoint,
			wantRequeue:   true,
		},
		{
			name:                "both configured, only logs ready",
			usageLogsConfigured: true, logsConfigured: true,
			usageLogsStatus: metav1.ConditionFalse, logsStatus: metav1.ConditionTrue,
			wantLogs:    logsEndpoint,
			wantRequeue: true,
		},
		{
			name:                "both configured, neither ready",
			usageLogsConfigured: true, logsConfigured: true,
			usageLogsStatus: metav1.ConditionFalse, logsStatus: metav1.ConditionFalse,
			wantRequeue: true,
		},
		{
			name:            "only logs configured, its stack ready",
			logsConfigured:  true,
			usageLogsStatus: metav1.ConditionTrue, logsStatus: metav1.ConditionTrue,
			wantLogs: logsEndpoint,
		},
		{
			name:                "only usage logs configured, its stack ready",
			usageLogsConfigured: true,
			usageLogsStatus:     metav1.ConditionTrue, logsStatus: metav1.ConditionTrue,
			wantUsageLogs: usageLogsEndpoint,
		},
		{
			// The readiness check failed. Nothing is known about either stack,
			// so both keep the endpoint they were last published with.
			name:                "unobservable readiness holds published endpoints",
			usageLogsConfigured: true, logsConfigured: true,
			usageLogsStatus: metav1.ConditionUnknown, logsStatus: metav1.ConditionUnknown,
			wantUsageLogs: staleUsageLogs, wantLogs: staleLogs,
			wantRequeue: true,
		},
		{
			// Holding on Unknown must not invent an endpoint: a stack that has
			// never been ready has nothing to hold.
			name:                "unobservable readiness on first reconcile stays empty",
			usageLogsConfigured: true, logsConfigured: true,
			usageLogsStatus: metav1.ConditionUnknown, logsStatus: metav1.ConditionUnknown,
			firstReconcile: true,
			wantRequeue:    true,
		},
		{
			// One stack unobservable, the other confirmed down. Hold and clear
			// have to be decided per stack, not once for both.
			name:                "unobservable usage logs holds while confirmed-down logs clears",
			usageLogsConfigured: true, logsConfigured: true,
			usageLogsStatus: metav1.ConditionUnknown, logsStatus: metav1.ConditionFalse,
			wantUsageLogs: staleUsageLogs,
			wantRequeue:   true,
		},
		{
			// Unknown does not override the user turning the feature off.
			// Whatever the stack is doing, nothing should be dialling it.
			name:            "unobservable but unconfigured still clears",
			logsConfigured:  true,
			usageLogsStatus: metav1.ConditionUnknown, logsStatus: metav1.ConditionTrue,
			wantLogs: logsEndpoint,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			m := newMonitoring(v1alpha1.MonitoringInstanceName)
			if test.usageLogsConfigured {
				m.Spec.UsageLogs = &v1alpha1.UsageLogs{Storage: &v1alpha1.LokiStorageConfig{
					Type: v1alpha1.StorageBackendS3, SecretName: "usage-logs-secret",
				}}
			}
			if test.logsConfigured {
				m.Spec.Logs = &v1alpha1.Logs{Storage: &v1alpha1.LokiStorageConfig{
					Type: v1alpha1.StorageBackendS3, SecretName: "logs-secret",
				}}
			}

			if !test.firstReconcile {
				m.Status.UsageLogsEndpoint = staleUsageLogs
				m.Status.LogsEndpoint = staleLogs
			}

			cm := conditions.NewConditionsManager(m, m.Generation)
			mark := func(condType string, status metav1.ConditionStatus) {
				switch status {
				case metav1.ConditionTrue:
					cm.MarkTrue(condType)
				case metav1.ConditionFalse:
					cm.MarkFalse(condType, "LokiStackNotReady", "LokiStack is not ready yet")
				case metav1.ConditionUnknown:
					cm.MarkUnknown(condType, "ReadinessCheckFailed", "Cannot determine whether the LokiStack is ready")
				}
			}
			mark(conditions.ConditionUsageLogsLokiStackAvailable, test.usageLogsStatus)
			mark(conditions.ConditionLokiStackAvailable, test.logsStatus)

			gotRequeue := publishLokiEndpoints(m, data)

			if got := m.Status.UsageLogsEndpoint; got != test.wantUsageLogs {
				t.Errorf("UsageLogsEndpoint: want %q, got %q", test.wantUsageLogs, got)
			}
			if got := m.Status.LogsEndpoint; got != test.wantLogs {
				t.Errorf("LogsEndpoint: want %q, got %q", test.wantLogs, got)
			}
			if gotRequeue != test.wantRequeue {
				t.Errorf("requeue: want %t, got %t", test.wantRequeue, gotRequeue)
			}
		})
	}
}

func TestKorrel8rEndpointSliceWatch_EnqueuesForKorrel8rAndKubernetesAPI(t *testing.T) {
	t.Setenv("MONITORING_NAMESPACE", "test-ns")

	pred := predicate.NewPredicateFuncs(isKorrel8rEndpointSlice)
	for _, test := range []struct {
		name  string
		slice *discoveryv1.EndpointSlice
		want  bool
	}{
		{
			name: "Kubernetes API endpoint in default namespace",
			slice: &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
				Namespace: metav1.NamespaceDefault,
				Labels:    map[string]string{discoveryv1.LabelServiceName: kubernetesServiceName},
			}},
			want: true,
		},
		{
			name: "Korrel8r endpoint",
			slice: &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
				Namespace: "test-ns",
				Labels:    map[string]string{discoveryv1.LabelServiceName: Korrel8rServiceName},
			}},
			want: true,
		},
		{
			name: "Korrel8r endpoint outside monitoring namespace",
			slice: &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
				Namespace: "other-ns",
				Labels:    map[string]string{discoveryv1.LabelServiceName: Korrel8rServiceName},
			}},
			want: false,
		},
		{
			name: "Kubernetes endpoint outside default namespace",
			slice: &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
				Namespace: "other-ns",
				Labels:    map[string]string{discoveryv1.LabelServiceName: kubernetesServiceName},
			}},
			want: false,
		},
		{
			name: "unrelated endpoint",
			slice: &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
				Namespace: metav1.NamespaceDefault,
				Labels:    map[string]string{discoveryv1.LabelServiceName: "unrelated"},
			}},
			want: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := pred.Create(event.CreateEvent{Object: test.slice}); got != test.want {
				t.Errorf("Create: want %t, got %t", test.want, got)
			}
			if got := pred.Update(event.UpdateEvent{ObjectOld: test.slice, ObjectNew: test.slice}); got != test.want {
				t.Errorf("Update: want %t, got %t", test.want, got)
			}
		})
	}
}

// TestReconcile_ObservedGenerationSet: ObservedGeneration must match the CR generation.
func TestReconcile_ObservedGenerationSet(t *testing.T) {
	s := newTestScheme(t)

	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "operators.coreos.com", Version: "v2", Kind: "OperatorCondition",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "operators.coreos.com", Version: "v2", Kind: "OperatorConditionList",
	}, &unstructured.UnstructuredList{})

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Generation = 7

	r := newTestReconciler(t, s, fake.NewClientBuilder().WithScheme(s).WithObjects(m).WithStatusSubresource(m).Build())

	_, err := r.reconcile(context.Background(), m)
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	if got := m.Status.ObservedGeneration; got != 7 {
		t.Errorf("ObservedGeneration: want 7, got %d", got)
	}
}
