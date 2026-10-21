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
	"strings"
	"testing"

	platformcommon "github.com/opendatahub-io/odh-platform-utilities/api/common"
	"github.com/opendatahub-io/odh-platform-utilities/pkg/deploy"
	configv1 "github.com/openshift/api/config/v1"
	routev1 "github.com/openshift/api/route/v1"
	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
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

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
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

func TestPreparePrometheusWebTLSCA(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.UID = "monitoring-test-uid"
	m.Spec.Metrics = &v1alpha1.Metrics{Storage: &v1alpha1.MetricsStorage{}}

	ca := &unstructured.Unstructured{}
	ca.SetGroupVersionKind(controllergvk.ConfigMap)
	ca.SetName(prometheusWebTLSCAConfigMapName)
	ca.SetNamespace(m.Spec.Namespace)
	stack := &unstructured.Unstructured{}
	stack.SetGroupVersionKind(controllergvk.MonitoringStack)
	stack.SetName("data-science-monitoringstack")
	stack.SetNamespace(m.Spec.Namespace)
	collector := &unstructured.Unstructured{}
	collector.SetGroupVersionKind(controllergvk.OpenTelemetryCollector)
	collector.SetName("data-science-collector")
	collector.SetNamespace(m.Spec.Namespace)
	desired := []unstructured.Unstructured{*ca, *stack, *collector}

	cli := fake.NewClientBuilder().WithScheme(s).Build()
	r := newTestReconciler(t, s, cli)
	ready, err := r.preparePrometheusWebTLSCA(context.Background(), m, desired)
	if err != nil || ready {
		t.Fatalf("expected CA to be pending before injection, got ready=%v error=%v", ready, err)
	}

	var created corev1.ConfigMap
	if err := cli.Get(context.Background(), client.ObjectKey{Namespace: m.Spec.Namespace, Name: prometheusWebTLSCAConfigMapName}, &created); err != nil {
		t.Fatalf("CA ConfigMap was not applied before other resources: %v", err)
	}
	if got := withoutMonitoringStack(desired); len(got) != 2 || got[0].GetKind() != "ConfigMap" || got[1].GetKind() != "OpenTelemetryCollector" {
		t.Fatalf("pending CA must omit only MonitoringStack, got %v", got)
	}

	created.Data = map[string]string{serviceCACertificateKey: "test-ca"}
	if err := cli.Update(context.Background(), &created); err != nil {
		t.Fatalf("injecting CA into ConfigMap: %v", err)
	}
	ready, err = r.preparePrometheusWebTLSCA(context.Background(), m, desired)
	if err != nil || !ready {
		t.Fatalf("expected CA Secret ready after injection, got ready=%v error=%v", ready, err)
	}
	var secret corev1.Secret
	if err := cli.Get(context.Background(), client.ObjectKey{Namespace: m.Spec.Namespace, Name: prometheusWebTLSCAConfigMapName}, &secret); err != nil {
		t.Fatalf("CA Secret was not synchronized: %v", err)
	}
	if string(secret.Data[serviceCACertificateKey]) != "test-ca" {
		t.Fatalf("CA Secret was not synchronized: data=%v", secret.Data)
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

func selfSubjectRulesReviewInterceptor(reviewErr error) interceptor.Funcs {
	return interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*authorizationv1.SelfSubjectRulesReview); ok {
				// An empty rule set models a clean namespace with no collectable resources.
				return reviewErr
			}
			return c.Create(ctx, obj, opts...)
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

	r := newTestReconciler(t, s, fake.NewClientBuilder().WithScheme(s).WithObjects(m).WithStatusSubresource(m).
		WithInterceptorFuncs(selfSubjectRulesReviewInterceptor(nil)).Build())

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

func TestReconcile_Removed_CleanupError(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.ManagementState = platformcommon.Removed
	cleanupErr := errors.New("rule review failed")

	r := newTestReconciler(t, s, fake.NewClientBuilder().WithScheme(s).WithObjects(m).WithStatusSubresource(m).
		WithInterceptorFuncs(selfSubjectRulesReviewInterceptor(cleanupErr)).Build())

	_, err := r.reconcile(context.Background(), m)
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("expected cleanup error to be returned for retry, got %v", err)
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
	if m.Status.Phase != platformcommon.PhaseNotReady {
		t.Fatalf("failed reconcile should set NotReady phase, got %s", m.Status.Phase)
	}
	for _, conditionType := range []string{string(platformcommon.ConditionTypeReady), string(platformcommon.ConditionTypeProvisioningSucceeded)} {
		found := false
		for _, condition := range m.Status.Conditions {
			if condition.Type == conditionType {
				found = condition.Status == metav1.ConditionFalse
			}
		}
		if !found {
			t.Errorf("%s must be False after reconciliation error", conditionType)
		}
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

	r := newTestReconciler(t, s, fake.NewClientBuilder().WithScheme(s).WithObjects(m).WithStatusSubresource(m).
		WithInterceptorFuncs(selfSubjectRulesReviewInterceptor(nil)).Build())

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
	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(m, stalePolicy).WithStatusSubresource(m).
		WithInterceptorFuncs(selfSubjectRulesReviewInterceptor(nil)).Build()
	r := newTestReconciler(t, s, cli)

	if _, err := r.reconcile(context.Background(), m); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	if err := r.Get(context.Background(), client.ObjectKeyFromObject(stalePolicy), &networkingv1.NetworkPolicy{}); !k8serr.IsNotFound(err) {
		t.Fatalf("expected generated policy in effective namespace to be deleted, got %v", err)
	}
}

func TestReconcile_GarbageCollectionError(t *testing.T) {
	s := newTestScheme(t)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	cleanupErr := errors.New("rule review failed")
	r := newTestReconciler(t, s, fake.NewClientBuilder().WithScheme(s).WithObjects(m).WithStatusSubresource(m).
		WithInterceptorFuncs(selfSubjectRulesReviewInterceptor(cleanupErr)).Build())

	_, err := r.reconcile(context.Background(), m)
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("expected garbage collection error to be returned for retry, got %v", err)
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

	r := newTestReconciler(t, s, fake.NewClientBuilder().WithScheme(s).WithObjects(m).WithStatusSubresource(m).
		WithInterceptorFuncs(selfSubjectRulesReviewInterceptor(nil)).Build())

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
		WithStatusSubresource(m).
		WithInterceptorFuncs(selfSubjectRulesReviewInterceptor(nil)).Build())

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
	clusterRole := ownedGarbageCollectionTestObject(controllergvk.ClusterRole, "", "data-science-metrics-view", monitoring.UID)

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
			if got := shouldCollectOwnedResource(monitoring, test.obj, test.desired, monitoring.Spec.Namespace); got != test.wantMainGC {
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

	r := newTestReconciler(t, s, fake.NewClientBuilder().WithScheme(s).WithObjects(m).WithStatusSubresource(m).
		WithInterceptorFuncs(selfSubjectRulesReviewInterceptor(nil)).Build())

	_, err := r.reconcile(context.Background(), m)
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	if got := m.Status.ObservedGeneration; got != 7 {
		t.Errorf("ObservedGeneration: want 7, got %d", got)
	}
}
