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

package controller

import (
	"strings"
	"testing"

	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
	pkgresources "github.com/opendatahub-io/odh-platform-utilities/pkg/resources"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
)

// RHOAIENG-99814 contract: DS namespace scoping via Perses → Thanos proxy :8443.
// Perses must not bypass to :10902. Cluster :9091 / tenancy :9092 changes are
// anti-bypass guardrails (caller token, not shared SA) — not this feature.

func TestPersesMetricsScopingContract_DataScienceDatasourceUsesAuthorizedProxy(t *testing.T) {
	t.Parallel()

	for _, apiVersion := range []string{"v1alpha1", "v1alpha2"} {
		t.Run(apiVersion, func(t *testing.T) {
			t.Parallel()
			resources := renderObservabilityTemplate(t, PersesDatasourcePrometheusTemplate, map[string]any{
				"Namespace":        testMonitoringNamespace,
				"PersesAPIVersion": apiVersion,
			})
			datasource := findRenderedResource(t, resources, "PersesDatasource", "data-science-prometheus-datasource")

			url, found, err := unstructured.NestedString(datasource.Object, "spec", "config", "plugin", "spec", "proxy", "spec", "url")
			wantURL := "https://data-science-thanos-querier-proxy." + testMonitoringNamespace + ".svc.cluster.local:8443"
			if err != nil || !found || url != wantURL {
				t.Fatalf("Data Science datasource must use authorized Thanos proxy: found=%t got=%q want=%q error=%v", found, url, wantURL, err)
			}
			if strings.Contains(url, ":10902") || strings.Contains(url, "thanos-querier-data-science-thanos-querier") {
				t.Fatalf("Data Science datasource must not bypass to raw Thanos :10902, got %q", url)
			}

			defaultValue, found, err := unstructured.NestedBool(datasource.Object, "spec", "config", "default")
			if err != nil || !found || !defaultValue {
				t.Fatalf("authorized Data Science datasource must be the default: found=%t value=%t error=%v", found, defaultValue, err)
			}

			namespaceParam, found, err := unstructured.NestedString(datasource.Object, "spec", "config", "plugin", "spec", "queryParams", "namespace")
			if err != nil || !found || namespaceParam != "${namespace:queryparam}" {
				t.Fatalf("datasource must require namespace= query param: found=%t value=%q error=%v", found, namespaceParam, err)
			}

			secretRef, found, err := unstructured.NestedString(datasource.Object, "spec", "config", "plugin", "spec", "proxy", "spec", "secret")
			if err != nil || !found || secretRef != "data-science-prometheus-datasource-secret" {
				t.Fatalf("datasource must reference TLS CA secret created by Perses operator: found=%t value=%q error=%v", found, secretRef, err)
			}

			enableTLS, found, err := unstructured.NestedBool(datasource.Object, "spec", "client", "tls", "enable")
			if err != nil || !found || !enableTLS {
				t.Fatalf("proxy datasource must enable TLS: found=%t value=%t error=%v", found, enableTLS, err)
			}

			for _, resource := range resources {
				if resource.GetKind() == "Secret" {
					t.Fatalf("Data Science datasource template must not render a bearer Secret (TLS CA is created by Perses operator), found %q", resource.GetName())
				}
			}
		})
	}
}

func TestPersesMetricsScopingContract_ClusterPrometheusUsesCallerTokenNotSharedSA(t *testing.T) {
	t.Parallel()

	resources := renderObservabilityTemplate(t, PersesDatasourceClusterPrometheusTemplate, map[string]any{
		"Namespace":        testMonitoringNamespace,
		"PersesAPIVersion": "v1alpha2",
	})
	datasource := findRenderedResource(t, resources, "PersesDatasource", "cluster-prometheus-datasource")

	defaultValue, found, err := unstructured.NestedBool(datasource.Object, "spec", "config", "default")
	if err != nil || !found || defaultValue {
		t.Fatalf("cluster datasource must not be default (DS proxy is): found=%t value=%t error=%v", found, defaultValue, err)
	}
	url, found, err := unstructured.NestedString(datasource.Object, "spec", "config", "plugin", "spec", "proxy", "spec", "url")
	if err != nil || !found || url != "https://thanos-querier.openshift-monitoring.svc:9091" {
		t.Fatalf("cluster datasource must use openshift-monitoring :9091: found=%t got=%q error=%v", found, url, err)
	}
	if _, found, err := unstructured.NestedMap(datasource.Object, "spec", "config", "plugin", "spec", "queryParams"); err != nil {
		t.Fatalf("reading cluster datasource queryParams: %v", err)
	} else if found {
		t.Fatal("cluster-wide :9091 datasource must not force namespace= query param")
	}
	secretRef, found, err := unstructured.NestedString(datasource.Object, "spec", "config", "plugin", "spec", "proxy", "spec", "secret")
	if err != nil || !found || secretRef != "cluster-prometheus-datasource-secret" {
		t.Fatalf("cluster datasource must reference TLS CA secret: found=%t value=%q error=%v", found, secretRef, err)
	}
	for _, resource := range resources {
		if resource.GetKind() == "Secret" {
			t.Fatalf("cluster datasource must not render a shared-SA Secret, found %q", resource.GetName())
		}
	}
}

func TestPersesMetricsScopingContract_TenancyDatasourceNotDefault(t *testing.T) {
	t.Parallel()

	resources := renderObservabilityTemplate(t, PersesDatasourceClusterPrometheusTenancyTemplate, map[string]any{
		"Namespace":        testMonitoringNamespace,
		"PersesAPIVersion": "v1alpha2",
	})
	datasource := findRenderedResource(t, resources, "PersesDatasource", "cluster-prometheus-tenancy-datasource")

	defaultValue, found, err := unstructured.NestedBool(datasource.Object, "spec", "config", "default")
	if err != nil || !found || defaultValue {
		t.Fatalf("tenancy datasource must not be default: found=%t value=%t error=%v", found, defaultValue, err)
	}
	secretRef, found, err := unstructured.NestedString(datasource.Object, "spec", "config", "plugin", "spec", "proxy", "spec", "secret")
	if err != nil || !found || secretRef != "cluster-prometheus-tenancy-datasource-secret" {
		t.Fatalf("tenancy datasource must reference TLS CA secret: found=%t value=%q error=%v", found, secretRef, err)
	}
	for _, resource := range resources {
		if resource.GetKind() == "Secret" {
			t.Fatalf("tenancy datasource must not render a bearer Secret (TLS CA is created by Perses operator), found %q", resource.GetName())
		}
	}
}

func TestPersesMetricsScopingContract_PersesDoesNotEnableKubernetesAuth(t *testing.T) {
	t.Parallel()

	resources := renderObservabilityTemplate(t, PersesTemplate, map[string]any{
		"Namespace":        testMonitoringNamespace,
		"PersesAPIVersion": "v1alpha2",
		"PersesImage":      "example.invalid/perses:test",
	})
	perses := findRenderedResource(t, resources, "Perses", "data-science-perses")

	if _, found, err := unstructured.NestedMap(perses.Object, "spec", "config", "security"); err != nil {
		t.Fatalf("reading Perses security config: %v", err)
	} else if found {
		t.Fatal("Perses must not enable security/auth for this metrics-proxy fix; tenancy is enforced downstream")
	}
	for _, resource := range resources {
		switch resource.GetKind() {
		case "ServiceAccount", "ClusterRole", "ClusterRoleBinding":
			t.Fatalf("Perses template must not create auth RBAC resources for this fix, found %s/%s", resource.GetKind(), resource.GetName())
		}
	}
}

func TestPersesMetricsScopingContract_NetworkPoliciesBlockDirectThanosBypass(t *testing.T) {
	t.Parallel()

	thanosPolicy := findRenderedResource(t,
		renderObservabilityTemplate(t, ThanosQuerierNetworkPolicyTemplate, map[string]any{
			"Namespace": testMonitoringNamespace,
		}),
		"NetworkPolicy", "data-science-thanos-querier-ingress")
	ingressRules := nestedSlice(t, thanosPolicy, "spec", "ingress")
	if len(ingressRules) != 1 {
		t.Fatalf("expected one Thanos Querier ingress rule, got %d", len(ingressRules))
	}
	fromValues, ok := asMap(t, ingressRules[0])["from"].([]any)
	if !ok {
		t.Fatalf("Thanos Querier ingress rule missing from: %#v", ingressRules[0])
	}
	for _, rawPeer := range fromValues {
		peer := asMap(t, rawPeer)
		labels, found, err := unstructured.NestedStringMap(asMap(t, peer["podSelector"]), "matchLabels")
		if err != nil || !found {
			t.Fatalf("invalid Thanos ingress peer: %#v error=%v", peer, err)
		}
		if labels["app.kubernetes.io/name"] == "data-science-perses" || labels["instance"] == "data-science-perses" {
			t.Fatalf("Thanos :10902 NetworkPolicy must not allow Perses (bypass): %#v", labels)
		}
	}

	proxyPolicy := findRenderedResource(t,
		renderObservabilityTemplate(t, ThanosQuerierProxyNetworkPolicyTemplate, proxyTemplateData()),
		"NetworkPolicy", "data-science-thanos-querier-proxy-ingress")
	ingress := nestedSlice(t, proxyPolicy, "spec", "ingress")
	if len(ingress) < 2 {
		t.Fatalf("Thanos proxy NetworkPolicy must allow gateway + Perses, got %d rules", len(ingress))
	}
	persesAllowed := false
	for _, rawRule := range ingress {
		rule := asMap(t, rawRule)
		fromValues, ok := rule["from"].([]any)
		if !ok {
			continue
		}
		for _, rawPeer := range fromValues {
			peer := asMap(t, rawPeer)
			podSelector, ok := peer["podSelector"].(map[string]any)
			if !ok {
				continue
			}
			labels, found, err := unstructured.NestedStringMap(podSelector, "matchLabels")
			if err != nil || !found {
				continue
			}
			if labels["instance"] == "data-science-perses" && labels["app.kubernetes.io/name"] == "data-science-perses" {
				persesAllowed = true
				ports, ok := rule["ports"].([]any)
				if !ok || len(ports) != 1 || asMap(t, ports[0])["port"] != int64(8443) {
					t.Fatalf("Perses may only reach the authorized proxy on 8443, got %#v", rule["ports"])
				}
			}
		}
	}
	if !persesAllowed {
		t.Fatal("Thanos proxy NetworkPolicy must allow Perses pods on port 8443")
	}
}

func TestPersesMetricsScopingContract_DeploySelectsOnlyScopedDatasources(t *testing.T) {
	t.Parallel()

	scheme := newActionsTestScheme(t)
	registerCRDs(scheme, gvk.PersesDatasourceV1Alpha2)
	monitoring := newMonitoring(v1alpha1.MonitoringInstanceName)
	monitoring.Spec.Metrics = &v1alpha1.Metrics{Storage: &v1alpha1.MetricsStorage{}}
	conditionsManager := conditions.NewConditionsManager(monitoring, monitoring.Generation)
	var sources []rendertemplate.TemplateSource

	_, err := deployPersesPrometheusIntegration(
		t.Context(),
		fake.NewClientBuilder().WithScheme(scheme).Build(),
		monitoring,
		conditionsManager,
		&sources,
		"v1alpha2",
		true,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Non-default cluster/tenancy datasources first, then DS default — avoids Perses 400 dual-default.
	wantPersesDatasources := []string{
		PersesDatasourceClusterPrometheusTemplate,
		PersesDatasourceClusterPrometheusTenancyTemplate,
		PersesDatasourcePrometheusTemplate,
	}
	assertTemplatePaths(t, sources, wantPersesDatasources)
	assertTemplateOrder(t, sources, wantPersesDatasources)
}

// TestPersesDatasourceApplyOrder_SortKeepsNonDefaultFirst locks the deploy-layer
// contract: WithApplyOrder sorts same-GVK objects by name, and our CR names keep
// default:false cluster/tenancy datasources before the default:true DS datasource.
func TestPersesDatasourceApplyOrder_SortKeepsNonDefaultFirst(t *testing.T) {
	t.Parallel()

	mk := func(name string) unstructured.Unstructured {
		u := unstructured.Unstructured{}
		u.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   gvk.PersesDatasource.Group,
			Version: gvk.PersesDatasource.Version,
			Kind:    gvk.PersesDatasource.Kind,
		})
		u.SetNamespace("redhat-ods-monitoring")
		u.SetName(name)
		return u
	}

	// Reverse of the desired apply order — sort must still put cluster* first.
	input := []unstructured.Unstructured{
		mk("data-science-prometheus-datasource"),
		mk("cluster-prometheus-tenancy-datasource"),
		mk("cluster-prometheus-datasource"),
	}
	sorted, err := pkgresources.SortByApplyOrder(t.Context(), input)
	if err != nil {
		t.Fatalf("SortByApplyOrder: %v", err)
	}
	want := []string{
		"cluster-prometheus-datasource",
		"cluster-prometheus-tenancy-datasource",
		"data-science-prometheus-datasource",
	}
	if len(sorted) != len(want) {
		t.Fatalf("sorted len=%d, want %d", len(sorted), len(want))
	}
	for i, name := range want {
		if sorted[i].GetName() != name {
			t.Fatalf("sorted[%d]=%q, want %q", i, sorted[i].GetName(), name)
		}
	}
}
