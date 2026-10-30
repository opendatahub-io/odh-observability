package controller

import (
	"context"
	"testing"

	"github.com/opendatahub-io/operator-rbac-toolkit/pkg/crossns"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
)

func TestTargetAllocatorRBACScoperCreatesToolkitManagedBindings(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	monitoring := &v1alpha1.Monitoring{
		ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.MonitoringInstanceName},
		Spec: v1alpha1.MonitoringSpec{
			Namespace: "redhat-ods-monitoring",
			Metrics:   &v1alpha1.Metrics{},
		},
	}
	namespaces := []corev1.Namespace{
		{ObjectMeta: metav1.ObjectMeta{Name: "redhat-ods-monitoring"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "inference"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "kube-system"}},
	}
	legacyBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: targetAllocatorSecretRBACName, Namespace: "inference"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: targetAllocatorSecretRBACName},
	}
	staleClusterBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: "stale-managed-clusterrolebinding",
			Labels: map[string]string{
				rbacScoperManagedLabelKey:              rbacScoperManagedLabelValue,
				TargetAllocatorRBACScoperOwnerLabelKey: TargetAllocatorRBACScoperOwnerLabelValue,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(monitoring, legacyBinding, staleClusterBinding).
		WithLists(&corev1.NamespaceList{Items: namespaces}).
		Build()
	scoper := &TargetAllocatorRBACScoper{
		Client: fakeClient,
		Reconciler: crossns.New(fakeClient, crossns.OwnerLabel{
			Key:   "app.kubernetes.io/name",
			Value: "odh-observability-targetallocator-rbac",
		}),
	}

	_, err := scoper.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: v1alpha1.MonitoringInstanceName},
	})
	if err != nil {
		t.Fatalf("TargetAllocator RBAC scoper reconcile failed: %v", err)
	}

	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Namespace: "inference",
		Name:      targetAllocatorSecretRBACName,
	}, &rbacv1.RoleBinding{}); err != nil && !k8serr.IsNotFound(err) {
		t.Fatalf("legacy TargetAllocator RoleBinding was not removed: %v", err)
	}

	role := &rbacv1.Role{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Namespace: "inference",
		Name:      targetAllocatorSecretRBACName,
	}, role); err != nil {
		t.Fatalf("TargetAllocator Secret Role was not created: %v", err)
	}
	if role.Labels[crossns.ManagedLabelKey] != crossns.ManagedLabelValue {
		t.Fatal("TargetAllocator Secret Role is not toolkit managed")
	}

	binding := &rbacv1.RoleBinding{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Namespace: "inference",
		Name:      targetAllocatorSecretRBACName + "-binding",
	}, binding); err != nil {
		t.Fatalf("toolkit TargetAllocator RoleBinding was not created: %v", err)
	}
	if binding.Labels[crossns.ManagedLabelKey] != crossns.ManagedLabelValue {
		t.Fatal("TargetAllocator RoleBinding is not toolkit managed")
	}

	bindings := &rbacv1.RoleBindingList{}
	if err := fakeClient.List(context.Background(), bindings); err != nil {
		t.Fatal(err)
	}
	var kubeSystemBindings int
	for _, item := range bindings.Items {
		if item.Namespace == "kube-system" && item.Name == targetAllocatorSecretRBACName+"-binding" {
			kubeSystemBindings++
		}
	}
	if kubeSystemBindings != 0 {
		t.Fatal("TargetAllocator RoleBinding was created in a system namespace")
	}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: v1alpha1.MonitoringInstanceName}, monitoring); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(monitoring, targetAllocatorRBACScoperFinalizer) {
		t.Fatal("TargetAllocator RBAC scoper finalizer was not added")
	}

	clusterBindings := &rbacv1.ClusterRoleBindingList{}
	if err := fakeClient.List(context.Background(), clusterBindings); err != nil {
		t.Fatal(err)
	}
	expectedClusterBindings := map[string]bool{
		"generate-processors-collector-rolebinding":                 true,
		"generate-processors-targetallocator-rolebinding":           true,
		"generate-processors-targetallocator-discovery-rolebinding": true,
		"korrel8r-query":          true,
		"korrel8r-auth-delegator": true,
	}
	for _, item := range clusterBindings.Items {
		delete(expectedClusterBindings, item.Name)
		if item.Name == "stale-managed-clusterrolebinding" {
			t.Fatal("stale managed ClusterRoleBinding was not removed")
		}
		if !isManagedByTargetAllocatorRBACScoper(&item) {
			t.Fatalf("ClusterRoleBinding %s is not managed by the scoper", item.Name)
		}
	}
	if len(expectedClusterBindings) != 0 {
		t.Fatalf("expected feature-dependent ClusterRoleBindings were not created: %v", expectedClusterBindings)
	}

	manager := &MonitoringReconciler{Client: fakeClient}
	if err := manager.verifyRBACScoper(context.Background(), monitoring, map[string]any{
		"TargetAllocatorSecretNamespaces": []string{"redhat-ods-monitoring", "inference"},
	}); err != nil {
		t.Fatalf("RBAC scoper resources failed readiness verification: %v", err)
	}
}

func TestTargetAllocatorRBACScoperTearsDownOnRemoved(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	monitoring := &v1alpha1.Monitoring{
		ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.MonitoringInstanceName},
		Spec:       v1alpha1.MonitoringSpec{Namespace: "redhat-ods-monitoring"},
	}
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      targetAllocatorSecretRBACName,
			Namespace: "inference",
			Labels: map[string]string{
				crossns.ManagedLabelKey: crossns.ManagedLabelValue,
			},
		},
	}
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      targetAllocatorSecretRBACName + "-binding",
			Namespace: "inference",
			Labels: map[string]string{
				crossns.ManagedLabelKey: crossns.ManagedLabelValue,
			},
		},
	}
	clusterBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: "generate-processors-collector-rolebinding",
			Labels: map[string]string{
				rbacScoperManagedLabelKey:              rbacScoperManagedLabelValue,
				TargetAllocatorRBACScoperOwnerLabelKey: TargetAllocatorRBACScoperOwnerLabelValue,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(monitoring, role, binding, clusterBinding).
		Build()
	scoper := &TargetAllocatorRBACScoper{
		Client:     fakeClient,
		Reconciler: crossns.New(fakeClient, crossns.OwnerLabel{}),
	}

	_, err := scoper.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: v1alpha1.MonitoringInstanceName},
	})
	if err != nil {
		t.Fatalf("TargetAllocator RBAC scoper teardown failed: %v", err)
	}

	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Namespace: role.Namespace,
		Name:      role.Name,
	}, role); err != nil && !k8serr.IsNotFound(err) {
		t.Fatalf("toolkit TargetAllocator Role was not removed: %v", err)
	}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Namespace: binding.Namespace,
		Name:      binding.Name,
	}, binding); err != nil && !k8serr.IsNotFound(err) {
		t.Fatalf("toolkit TargetAllocator RoleBinding was not removed: %v", err)
	}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: clusterBinding.Name}, clusterBinding); err != nil && !k8serr.IsNotFound(err) {
		t.Fatalf("managed ClusterRoleBinding was not removed: %v", err)
	}
}

func TestWithoutRBACScoperResourcesFiltersAllRBAC(t *testing.T) {
	resources := []unstructured.Unstructured{
		{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap"}},
		{Object: map[string]any{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role"}},
		{Object: map[string]any{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding"}},
		{Object: map[string]any{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole"}},
		{Object: map[string]any{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding"}},
	}

	filtered := withoutRBACScoperResources(resources)
	if len(filtered) != 1 || filtered[0].GetKind() != "ConfigMap" {
		t.Fatalf("expected only non-RBAC resources to remain, got %#v", filtered)
	}
}
