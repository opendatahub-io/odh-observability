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
	"testing"

	platformcommon "github.com/opendatahub-io/odh-platform-utilities/api/common"
	"github.com/opendatahub-io/odh-platform-utilities/pkg/deploy"
	configv1 "github.com/openshift/api/config/v1"
	routev1 "github.com/openshift/api/route/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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

	var ready, provisioning string
	for _, c := range m.Status.Conditions {
		switch c.Type {
		case string(platformcommon.ConditionTypeReady):
			ready = string(c.Status)
		case string(platformcommon.ConditionTypeProvisioningSucceeded):
			provisioning = string(c.Status)
		}
	}
	if ready != string(metav1.ConditionFalse) {
		t.Errorf("Ready: want False, got %q", ready)
	}
	if provisioning != string(metav1.ConditionFalse) {
		t.Errorf("ProvisioningSucceeded: want False, got %q", provisioning)
	}
}

// TestReconcile_PreconditionsFailed: no operators installed, nothing configured.
// Should set MonitoringAvailable=False, Ready=False, no error returned.
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
	m.Spec.Metrics = &v1alpha1.Metrics{}

	r := newTestReconciler(t, s, fake.NewClientBuilder().WithScheme(s).WithObjects(m).WithStatusSubresource(m).Build())

	_, err := r.reconcile(context.Background(), m)
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var ready, monAvail string
	for _, c := range m.Status.Conditions {
		switch c.Type {
		case string(platformcommon.ConditionTypeReady):
			ready = string(c.Status)
		case "MonitoringAvailable":
			monAvail = string(c.Status)
		}
	}
	if ready != string(metav1.ConditionFalse) {
		t.Errorf("Ready: want False, got %q", ready)
	}
	if monAvail != string(metav1.ConditionFalse) {
		t.Errorf("MonitoringAvailable: want False, got %q", monAvail)
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

	var ready, degraded string
	for _, c := range m.Status.Conditions {
		switch c.Type {
		case string(platformcommon.ConditionTypeReady):
			ready = string(c.Status)
		case string(platformcommon.ConditionTypeDegraded):
			degraded = string(c.Status)
		}
	}
	if ready != string(metav1.ConditionTrue) {
		t.Errorf("Ready: want True, got %q", ready)
	}
	if degraded != string(metav1.ConditionFalse) {
		t.Errorf("Degraded: want False, got %q", degraded)
	}
}

// TestSyncStatusURL_RoutePresent: when metrics are configured and the route exists
// with a host, status.url is populated.
func TestSyncStatusURL_RoutePresent(t *testing.T) {
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
	m.Spec.Metrics = &v1alpha1.Metrics{}
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
		m.Spec.Metrics = &v1alpha1.Metrics{}

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
		m.Spec.Metrics = &v1alpha1.Metrics{}
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
