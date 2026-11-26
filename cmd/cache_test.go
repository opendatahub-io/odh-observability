/*
Copyright 2026.

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

package main

import (
	"testing"

	odhcache "github.com/opendatahub-io/odh-platform-utilities/pkg/cache"
	odhLabels "github.com/opendatahub-io/odh-platform-utilities/pkg/metadata/labels"
	routev1 "github.com/openshift/api/route/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	moncontroller "github.com/opendatahub-io/odh-observability/internal/controller"
)

func byObjectEntry[T client.Object](t *testing.T, opts cache.Options) cache.ByObject {
	t.Helper()
	for obj, cfg := range opts.ByObject {
		if _, ok := obj.(T); ok {
			return cfg
		}
	}
	t.Fatalf("missing ByObject entry for %T", *new(T))
	return cache.ByObject{}
}

func TestManagerCacheOptions_DefaultTransformStripsUnusedFields(t *testing.T) {
	opts, err := managerCacheOptions("redhat-ods-monitoring", "redhat-ods-applications", "odh-observability")
	if err != nil {
		t.Fatalf("managerCacheOptions failed: %v", err)
	}
	if opts.DefaultTransform == nil {
		t.Fatal("expected DefaultTransform to be set")
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cm",
			Namespace: "redhat-ods-monitoring",
			Annotations: map[string]string{
				odhcache.AnnotationLastAppliedConfiguration: `{"huge":"json"}`,
				"keep-me": "yes",
			},
			ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kubectl"}},
		},
		Data: map[string]string{"key": "value"},
	}

	transformed, err := opts.DefaultTransform(cm)
	if err != nil {
		t.Fatalf("DefaultTransform failed: %v", err)
	}
	got, ok := transformed.(*corev1.ConfigMap)
	if !ok {
		t.Fatalf("expected *corev1.ConfigMap, got %T", transformed)
	}
	if len(got.ManagedFields) != 0 {
		t.Fatalf("expected ManagedFields to be stripped, got %v", got.ManagedFields)
	}
	if _, exists := got.Annotations[odhcache.AnnotationLastAppliedConfiguration]; exists {
		t.Fatal("expected last-applied-configuration annotation to be stripped")
	}
	if got.Annotations["keep-me"] != "yes" {
		t.Fatalf("expected unrelated annotation to be preserved, got %v", got.Annotations)
	}
	if got.Data["key"] != "value" {
		t.Fatalf("expected ConfigMap data to be preserved, got %v", got.Data)
	}
}

func assertMonitoringLabelSelector(t *testing.T, context string, sel labels.Selector) {
	t.Helper()
	if sel == nil {
		t.Fatalf("%s: expected Label selector to be set", context)
	}
	if !sel.Matches(labels.Set{odhLabels.PlatformPartOf: moncontroller.MonitoringPartOf}) {
		t.Fatalf("%s: expected Label selector to match monitoring resource", context)
	}
	if sel.Matches(labels.Set{"app": "user-workload"}) {
		t.Fatalf("%s: expected Label selector to exclude unlabeled user resource", context)
	}
}

func assertNamespaceScopedMonitoringSelector(t *testing.T, name string, cfg cache.ByObject, namespaces ...string) {
	t.Helper()
	if _, allNS := cfg.Namespaces[cache.AllNamespaces]; allNS {
		t.Fatalf("%s: cache must not watch AllNamespaces when MONITORING_NAMESPACE is set", name)
	}
	if len(cfg.Namespaces) != len(namespaces) {
		t.Fatalf("%s: Namespaces len: want %d, got %d", name, len(namespaces), len(cfg.Namespaces))
	}
	for _, ns := range namespaces {
		nsCfg, ok := cfg.Namespaces[ns]
		if !ok {
			t.Fatalf("%s: missing namespace %q", name, ns)
		}
		assertMonitoringLabelSelector(t, name+"["+ns+"]", nsCfg.LabelSelector)
	}
}

func TestManagerCacheOptions_SplitNamespaces(t *testing.T) {
	const monitoringNS = "redhat-ods-monitoring"

	opts, err := managerCacheOptions(monitoringNS, "redhat-ods-applications", "odh-observability")
	if err != nil {
		t.Fatalf("managerCacheOptions failed: %v", err)
	}

	// RBAC types remain cluster-wide label-filtered (Role/RoleBinding cover per-namespace TargetAllocator secret RBAC).
	// Role and RoleBinding are additionally name-restricted so tenant-created
	// objects carrying the part-of=monitoring label cannot enter the cache.
	for _, check := range []struct {
		name          string
		cfg           cache.ByObject
		wantFieldName string
	}{
		{"Role", byObjectEntry[*rbacv1.Role](t, opts), moncontroller.TargetAllocatorSecretRBACName},
		{"RoleBinding", byObjectEntry[*rbacv1.RoleBinding](t, opts), moncontroller.TargetAllocatorSecretRBACName},
		{"ClusterRole", byObjectEntry[*rbacv1.ClusterRole](t, opts), ""},
		{"ClusterRoleBinding", byObjectEntry[*rbacv1.ClusterRoleBinding](t, opts), ""},
	} {
		assertMonitoringLabelSelector(t, check.name, check.cfg.Label)
		if check.wantFieldName == "" {
			if check.cfg.Field != nil {
				t.Fatalf("%s: expected no Field selector, got %v", check.name, check.cfg.Field)
			}
			continue
		}
		if check.cfg.Field == nil {
			t.Fatalf("%s: expected Field selector on metadata.name", check.name)
		}
		if value, ok := check.cfg.Field.RequiresExactMatch("metadata.name"); !ok || value != check.wantFieldName {
			t.Fatalf("%s: Field selector metadata.name: want %q, got (%q, %v)", check.name, check.wantFieldName, value, ok)
		}
	}

	// Deployment, Service, and Route cache all objects in monitoringNS so opted-out unlabeled singletons remain visible without APIReader.
	for _, check := range []struct {
		name string
		cfg  cache.ByObject
	}{
		{"Deployment", byObjectEntry[*appsv1.Deployment](t, opts)},
		{"Service", byObjectEntry[*corev1.Service](t, opts)},
		{"Route", byObjectEntry[*routev1.Route](t, opts)},
	} {
		if _, allNS := check.cfg.Namespaces[cache.AllNamespaces]; allNS {
			t.Fatalf("%s: cache must not watch AllNamespaces when MONITORING_NAMESPACE is set", check.name)
		}
		if len(check.cfg.Namespaces) != 1 {
			t.Fatalf("%s: Namespaces len: want 1, got %d", check.name, len(check.cfg.Namespaces))
		}
		nsCfg, ok := check.cfg.Namespaces[monitoringNS]
		if !ok {
			t.Fatalf("%s: missing namespace %q", check.name, monitoringNS)
		}
		if nsCfg.LabelSelector != nil || nsCfg.FieldSelector != nil {
			t.Fatalf("%s: expected unfiltered monitoringNS entry, got %+v", check.name, nsCfg)
		}
	}

	// Job is label-filtered inside monitoringNS; ServiceAccount is label-filtered inside monitoringNS + openshift-logging.
	assertNamespaceScopedMonitoringSelector(t, "Job", byObjectEntry[*batchv1.Job](t, opts), monitoringNS)
	assertNamespaceScopedMonitoringSelector(t, "ServiceAccount", byObjectEntry[*corev1.ServiceAccount](t, opts), monitoringNS, moncontroller.ClusterLoggingNamespace)
}

func TestManagerCacheOptions_SplitNamespaces_ConfigMapAndSecret(t *testing.T) {
	const (
		monitoringNS = "redhat-ods-monitoring"
		podNS        = "redhat-ods-applications"
		operatorName = "odh-observability"
	)

	opts, err := managerCacheOptions(monitoringNS, podNS, operatorName)
	if err != nil {
		t.Fatalf("managerCacheOptions failed: %v", err)
	}

	// ConfigMap: monitoringNS unfiltered + podNS restricted to odh-monitoring-config.
	cmCfg := byObjectEntry[*corev1.ConfigMap](t, opts)
	if _, allNS := cmCfg.Namespaces[cache.AllNamespaces]; allNS {
		t.Fatal("ConfigMap cache must not watch AllNamespaces when MONITORING_NAMESPACE is set")
	}
	if len(cmCfg.Namespaces) != 2 {
		t.Fatalf("ConfigMap Namespaces len: want 2, got %d", len(cmCfg.Namespaces))
	}
	if nsCfg, ok := cmCfg.Namespaces[monitoringNS]; !ok || nsCfg.LabelSelector != nil || nsCfg.FieldSelector != nil {
		t.Fatalf("ConfigMap monitoringNS entry unexpected: %+v", nsCfg)
	}
	if cmCfg.Namespaces[podNS].FieldSelector == nil ||
		cmCfg.Namespaces[podNS].FieldSelector.String() != "metadata.name="+moncontroller.PlatformConfigName {
		t.Fatalf("ConfigMap podNS FieldSelector: got %v", cmCfg.Namespaces[podNS].FieldSelector)
	}

	// Secret: scoped to monitoringNS and podNS (webhook cert only).
	secretCfg := byObjectEntry[*corev1.Secret](t, opts)
	if _, allNS := secretCfg.Namespaces[cache.AllNamespaces]; allNS {
		t.Fatal("Secret cache must not watch AllNamespaces when MONITORING_NAMESPACE is set")
	}
	if _, ok := secretCfg.Namespaces[monitoringNS]; !ok {
		t.Fatal("Secret cache must include monitoringNS for CA and user storage Secrets")
	}
	if secretCfg.Namespaces[podNS].FieldSelector == nil ||
		secretCfg.Namespaces[podNS].FieldSelector.String() != "metadata.name=odh-observability-webhook-cert" {
		t.Fatalf("Secret podNS FieldSelector: got %v", secretCfg.Namespaces[podNS].FieldSelector)
	}
}

func TestManagerCacheOptions_SplitNamespaces_Networking(t *testing.T) {
	const (
		monitoringNS = "redhat-ods-monitoring"
		podNS        = "redhat-ods-applications"
		operatorName = "odh-observability"
	)

	opts, err := managerCacheOptions(monitoringNS, podNS, operatorName)
	if err != nil {
		t.Fatalf("managerCacheOptions failed: %v", err)
	}

	// NetworkPolicy: monitoringNS (includes OTel policies) + openshift-logging (labeled).
	npCfg := byObjectEntry[*networkingv1.NetworkPolicy](t, opts)
	if _, allNS := npCfg.Namespaces[cache.AllNamespaces]; allNS {
		t.Fatal("NetworkPolicy cache must not watch AllNamespaces when MONITORING_NAMESPACE is set")
	}
	if _, ok := npCfg.Namespaces[monitoringNS]; !ok {
		t.Fatal("NetworkPolicy cache must include monitoringNS")
	}
	loggingCfg, ok := npCfg.Namespaces[moncontroller.ClusterLoggingNamespace]
	if !ok || loggingCfg.LabelSelector == nil ||
		!loggingCfg.LabelSelector.Matches(labels.Set{odhLabels.PlatformPartOf: moncontroller.MonitoringPartOf}) {
		t.Fatalf("NetworkPolicy openshift-logging selector unexpected: %+v", loggingCfg)
	}

	// EndpointSlice: default (kubernetes) + monitoringNS (korrel8r).
	esCfg := byObjectEntry[*discoveryv1.EndpointSlice](t, opts)
	if _, allNS := esCfg.Namespaces[cache.AllNamespaces]; allNS {
		t.Fatal("EndpointSlice cache must not watch AllNamespaces when MONITORING_NAMESPACE is set")
	}
	if !esCfg.Namespaces[metav1.NamespaceDefault].LabelSelector.Matches(labels.Set{discoveryv1.LabelServiceName: "kubernetes"}) {
		t.Fatal("EndpointSlice default selector must match kubernetes service")
	}
	if !esCfg.Namespaces[monitoringNS].LabelSelector.Matches(labels.Set{discoveryv1.LabelServiceName: moncontroller.Korrel8rServiceName}) {
		t.Fatal("EndpointSlice monitoringNS selector must match korrel8r service")
	}
	if esCfg.Namespaces[monitoringNS].LabelSelector.Matches(labels.Set{discoveryv1.LabelServiceName: "unrelated"}) {
		t.Fatal("EndpointSlice monitoringNS selector must exclude unrelated services")
	}
}

func TestManagerCacheOptions_CacheNewValidation(t *testing.T) {
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{
		corev1.SchemeGroupVersion,
		appsv1.SchemeGroupVersion,
		batchv1.SchemeGroupVersion,
		rbacv1.SchemeGroupVersion,
		networkingv1.SchemeGroupVersion,
		discoveryv1.SchemeGroupVersion,
		routev1.GroupVersion,
	})
	for _, entry := range []struct {
		gv    schema.GroupVersion
		kind  string
		scope meta.RESTScope
	}{
		{corev1.SchemeGroupVersion, "ConfigMap", meta.RESTScopeNamespace},
		{corev1.SchemeGroupVersion, "Secret", meta.RESTScopeNamespace},
		{corev1.SchemeGroupVersion, "Service", meta.RESTScopeNamespace},
		{corev1.SchemeGroupVersion, "ServiceAccount", meta.RESTScopeNamespace},
		{appsv1.SchemeGroupVersion, "Deployment", meta.RESTScopeNamespace},
		{batchv1.SchemeGroupVersion, "Job", meta.RESTScopeNamespace},
		{rbacv1.SchemeGroupVersion, "Role", meta.RESTScopeNamespace},
		{rbacv1.SchemeGroupVersion, "RoleBinding", meta.RESTScopeNamespace},
		{rbacv1.SchemeGroupVersion, "ClusterRole", meta.RESTScopeRoot},
		{rbacv1.SchemeGroupVersion, "ClusterRoleBinding", meta.RESTScopeRoot},
		{networkingv1.SchemeGroupVersion, "NetworkPolicy", meta.RESTScopeNamespace},
		{discoveryv1.SchemeGroupVersion, "EndpointSlice", meta.RESTScopeNamespace},
		{routev1.GroupVersion, "Route", meta.RESTScopeNamespace},
	} {
		mapper.Add(entry.gv.WithKind(entry.kind), entry.scope)
	}

	for _, tc := range []struct {
		name         string
		monitoringNS string
		podNS        string
		operatorName string
	}{
		{"split-namespaces", "redhat-ods-monitoring", "redhat-ods-applications", "odh-observability"},
		{"same-namespace", "opendatahub", "opendatahub", "odh-observability"},
		{"monitoring-ns-only", "redhat-ods-monitoring", "", "odh-observability"},
		{"default-monitoring-ns", metav1.NamespaceDefault, "opendatahub", "odh-observability"},
		{"unset-env", "", "", ""},
		{"pod-ns-only", "", "opendatahub", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := managerCacheOptions(tc.monitoringNS, tc.podNS, tc.operatorName)
			if err != nil {
				t.Fatalf("managerCacheOptions failed: %v", err)
			}
			opts.Scheme = scheme
			opts.Mapper = mapper

			if _, err := cache.New(&rest.Config{Host: "https://localhost"}, opts); err != nil {
				t.Fatalf("cache.New failed: %v", err)
			}
		})
	}
}

func TestManagerCacheOptions_UnsetMonitoringNamespacePreservesStandaloneMode(t *testing.T) {
	opts, err := managerCacheOptions("", "", "odh-observability")
	if err != nil {
		t.Fatalf("managerCacheOptions failed: %v", err)
	}

	for obj := range opts.ByObject {
		switch obj.(type) {
		case *corev1.Secret, *networkingv1.NetworkPolicy:
			t.Fatalf("expected %T not to be namespace-restricted when MONITORING_NAMESPACE is unset", obj)
		}
	}

	// ConfigMap, EndpointSlice, and workload/service types remain label-filtered in standalone mode.
	for _, check := range []struct {
		name string
		cfg  cache.ByObject
	}{
		{"Deployment", byObjectEntry[*appsv1.Deployment](t, opts)},
		{"Job", byObjectEntry[*batchv1.Job](t, opts)},
		{"Service", byObjectEntry[*corev1.Service](t, opts)},
		{"ServiceAccount", byObjectEntry[*corev1.ServiceAccount](t, opts)},
		{"Route", byObjectEntry[*routev1.Route](t, opts)},
		{"ConfigMap", byObjectEntry[*corev1.ConfigMap](t, opts)},
	} {
		if len(check.cfg.Namespaces) != 0 {
			t.Fatalf("%s: expected no namespace restriction when MONITORING_NAMESPACE is unset, got %v", check.name, check.cfg.Namespaces)
		}
		if check.cfg.Label == nil || !check.cfg.Label.Matches(labels.Set{odhLabels.PlatformPartOf: moncontroller.MonitoringPartOf}) {
			t.Fatalf("%s: selector must still match monitoring-managed resources", check.name)
		}
		if check.cfg.Label.Matches(labels.Set{}) {
			t.Fatalf("%s: selector must exclude unlabeled resources", check.name)
		}
	}
	esCfg := byObjectEntry[*discoveryv1.EndpointSlice](t, opts)
	if esCfg.Label == nil || !esCfg.Label.Matches(labels.Set{discoveryv1.LabelServiceName: "kubernetes"}) {
		t.Fatal("EndpointSlice selector must match kubernetes service when MONITORING_NAMESPACE is unset")
	}

	// Role and RoleBinding stay name-restricted in standalone mode too.
	for _, check := range []struct {
		name string
		cfg  cache.ByObject
	}{
		{"Role", byObjectEntry[*rbacv1.Role](t, opts)},
		{"RoleBinding", byObjectEntry[*rbacv1.RoleBinding](t, opts)},
	} {
		if value, ok := check.cfg.Field.RequiresExactMatch("metadata.name"); !ok || value != moncontroller.TargetAllocatorSecretRBACName {
			t.Fatalf("%s: expected metadata.name field selector %q, got (%q, %v)",
				check.name, moncontroller.TargetAllocatorSecretRBACName, value, ok)
		}
	}

	// When POD_NAMESPACE is set without MONITORING_NAMESPACE, ConfigMap also includes podNS unfiltered.
	withPodOpts, err := managerCacheOptions("", "opendatahub", "odh-observability")
	if err != nil {
		t.Fatalf("managerCacheOptions with podNS failed: %v", err)
	}
	cmCfg := byObjectEntry[*corev1.ConfigMap](t, withPodOpts)
	if len(cmCfg.Namespaces) != 2 {
		t.Fatalf("ConfigMap Namespaces len: want 2, got %d", len(cmCfg.Namespaces))
	}
	assertMonitoringLabelSelector(t, "ConfigMap[AllNamespaces]", cmCfg.Namespaces[cache.AllNamespaces].LabelSelector)
	if _, ok := cmCfg.Namespaces["opendatahub"]; !ok {
		t.Fatal("ConfigMap cache must include podNS when POD_NAMESPACE is set")
	}
}

func TestManagerCacheOptions_ClusterLoggingMonitoringNamespaceDeduplicates(t *testing.T) {
	opts, err := managerCacheOptions(moncontroller.ClusterLoggingNamespace, "opendatahub", "odh-observability")
	if err != nil {
		t.Fatalf("managerCacheOptions failed: %v", err)
	}

	saCfg := byObjectEntry[*corev1.ServiceAccount](t, opts)
	if len(saCfg.Namespaces) != 1 {
		t.Fatalf("ServiceAccount Namespaces len: want 1, got %d", len(saCfg.Namespaces))
	}
	sel := saCfg.Namespaces[moncontroller.ClusterLoggingNamespace].LabelSelector
	if sel == nil || !sel.Matches(labels.Set{odhLabels.PlatformPartOf: moncontroller.MonitoringPartOf}) {
		t.Fatal("ServiceAccount selector must match monitoring-managed ServiceAccounts")
	}
}

func TestManagerCacheOptions_SameNamespaceIncludesPlatformConfigMap(t *testing.T) {
	const ns = "opendatahub"
	opts, err := managerCacheOptions(ns, ns, "odh-observability")
	if err != nil {
		t.Fatalf("managerCacheOptions failed: %v", err)
	}

	cmCfg := byObjectEntry[*corev1.ConfigMap](t, opts)
	if len(cmCfg.Namespaces) != 1 {
		t.Fatalf("ConfigMap Namespaces len: want 1, got %d", len(cmCfg.Namespaces))
	}
	nsCfg, ok := cmCfg.Namespaces[ns]
	if !ok {
		t.Fatalf("ConfigMap missing namespace %q", ns)
	}
	if nsCfg.LabelSelector != nil || nsCfg.FieldSelector != nil {
		t.Fatalf("ConfigMap same-namespace entry should be unfiltered to include odh-monitoring-config, got %+v", nsCfg)
	}
}
