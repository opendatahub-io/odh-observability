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
	"strings"
	"testing"

	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
)

const (
	testMonitoringNamespace = "redhat-ods-monitoring"
)

func TestAcceleratorRecordingRulesTemplateContract(t *testing.T) {
	t.Run("renders the shipped recording rules when metrics are enabled", func(t *testing.T) {
		resources := renderObservabilityTemplate(t, AcceleratorRecordingRulesTemplate, map[string]any{
			"AcceleratorMetrics": true,
			"Namespace":          testMonitoringNamespace,
		})

		if len(resources) != 1 {
			t.Fatalf("expected one PrometheusRule, got %d resources", len(resources))
		}

		rule := resources[0]
		if rule.GetAPIVersion() != "monitoring.rhobs/v1" || rule.GetKind() != "PrometheusRule" {
			t.Fatalf("unexpected resource identity: apiVersion=%q kind=%q", rule.GetAPIVersion(), rule.GetKind())
		}
		if rule.GetName() != "data-science-accelerator-recording-rules" || rule.GetNamespace() != testMonitoringNamespace {
			t.Fatalf("unexpected PrometheusRule metadata: name=%q namespace=%q", rule.GetName(), rule.GetNamespace())
		}
		if labels := rule.GetLabels(); len(labels) != 1 || labels["platform.opendatahub.io/part-of"] != "monitoring" {
			t.Fatalf("unexpected PrometheusRule labels: %#v", labels)
		}

		groups, found, err := unstructured.NestedSlice(rule.Object, "spec", "groups")
		if err != nil || !found || len(groups) != 1 {
			t.Fatalf("expected one PrometheusRule group: found=%t error=%v groups=%v", found, err, groups)
		}
		group, ok := groups[0].(map[string]any)
		if !ok {
			t.Fatalf("PrometheusRule group has unexpected type %T", groups[0])
		}
		if group["name"] != "rhoai.accelerator.metrics" || group["interval"] != "30s" {
			t.Fatalf("unexpected PrometheusRule group: %#v", group)
		}

		rules, ok := group["rules"].([]any)
		if !ok {
			t.Fatalf("PrometheusRule rules have unexpected type %T", group["rules"])
		}
		wantRules := []struct {
			record string
			expr   string
		}{
			{record: "accelerator_gpu_utilization", expr: "DCGM_FI_DEV_GPU_UTIL * 100"},
			{record: "accelerator_memory_used_bytes", expr: "DCGM_FI_DEV_FB_USED * 1024 * 1024"},
			{record: "accelerator_temperature_celsius", expr: "DCGM_FI_DEV_GPU_TEMP"},
			{record: "accelerator_power_usage_watts", expr: "DCGM_FI_DEV_POWER_USAGE"},
		}
		if len(rules) != len(wantRules) {
			t.Fatalf("expected %d accelerator recording rules, got %d", len(wantRules), len(rules))
		}

		actualRules := make(map[string]map[string]any, len(rules))
		for _, rawRule := range rules {
			rule, ok := rawRule.(map[string]any)
			if !ok {
				t.Fatalf("recording rule has unexpected type %T", rawRule)
			}
			record, ok := rule["record"].(string)
			if !ok {
				t.Fatalf("recording rule has unexpected record type %T", rule["record"])
			}
			if _, exists := actualRules[record]; exists {
				t.Fatalf("recording rule %q is duplicated", record)
			}
			actualRules[record] = rule
		}

		for _, want := range wantRules {
			rule, found := actualRules[want.record]
			if !found {
				t.Errorf("recording rule %q is missing", want.record)
				continue
			}
			if rule["expr"] != want.expr {
				t.Errorf("recording rule %q: want expr=%q, got %#v", want.record, want.expr, rule["expr"])
			}
			labels, ok := rule["labels"].(map[string]any)
			if !ok || len(labels) != 1 || labels["vendor"] != "nvidia" {
				t.Errorf("recording rule %q must contain only the nvidia vendor label, got %#v", want.record, rule["labels"])
			}
		}
	})

	t.Run("does not render a rule when metrics are disabled", func(t *testing.T) {
		resources := renderObservabilityTemplate(t, AcceleratorRecordingRulesTemplate, map[string]any{
			"AcceleratorMetrics": false,
			"Namespace":          testMonitoringNamespace,
		})

		if len(resources) != 0 {
			t.Fatalf("expected no PrometheusRule resources when accelerator metrics are disabled, got %d", len(resources))
		}
	})
}

func TestDeployMonitoringStackWithQuerierIncludesTelemetryResources(t *testing.T) {
	scheme := newActionsTestScheme(t)
	registerCRDs(scheme, gvk.MonitoringStack, gvk.ThanosQuerier)

	monitoring := newMonitoring(v1alpha1.MonitoringInstanceName)
	monitoring.Spec.Metrics = &v1alpha1.Metrics{Storage: &v1alpha1.MetricsStorage{}}
	conditionsManager := conditions.NewConditionsManager(monitoring, monitoring.Generation)
	var sources []rendertemplate.TemplateSource

	err := deployMonitoringStackWithQuerierAndRestrictions(
		context.Background(),
		fake.NewClientBuilder().WithScheme(scheme).Build(),
		monitoring,
		conditionsManager,
		&sources,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantSources := []string{
		PrometheusWebTLSServiceTemplate,
		MonitoringStackTemplate,
		PrometheusSelfServiceMonitorTemplate,
		MonitoringStackAlertmanagerRBACTemplate,
		AcceleratorRecordingRulesTemplate,
		PrometheusRouteTemplate,
		PrometheusServiceOverrideTemplate,
		PrometheusNetworkPolicyTemplate,
		PrometheusNamespaceProxyTemplate,
		PrometheusNamespaceProxyNetworkPolicyTemplate,
		ThanosQuerierTemplate,
		ThanosQuerierProxyTemplate,
		ThanosQuerierProxyNetworkPolicyTemplate,
		ThanosQuerierRouteTemplate,
	}
	assertTemplatePaths(t, sources, wantSources)
}

func TestThanosQuerierProxyTemplateContract(t *testing.T) {
	resources := renderObservabilityTemplate(t, ThanosQuerierProxyTemplate, proxyTemplateData())

	assertClusterRoleBinding(
		t,
		findRenderedResource(t, resources, "ClusterRoleBinding", "data-science-thanos-querier-proxy"),
		"cluster-monitoring-view",
		"data-science-thanos-querier-proxy",
		testMonitoringNamespace,
	)
	assertClusterRoleBinding(
		t,
		findRenderedResource(t, resources, "ClusterRoleBinding", "data-science-thanos-querier-proxy-auth-delegator"),
		"system:auth-delegator",
		"data-science-thanos-querier-proxy",
		testMonitoringNamespace,
	)

	configMap := findRenderedResource(t, resources, "ConfigMap", "data-science-thanos-querier-proxy-config")
	config, found, err := unstructured.NestedString(configMap.Object, "data", "kube-rbac-proxy.yaml")
	if err != nil || !found {
		t.Fatalf("Thanos proxy authorization config is missing: found=%t error=%v", found, err)
	}
	for _, expected := range []string{
		"byQueryParameter:",
		"name: \"namespace\"",
		"apiGroup: metrics.k8s.io",
		"resource: pods",
		"namespace: \"{{ .Value }}\"",
	} {
		if !strings.Contains(config, expected) {
			t.Errorf("Thanos proxy config must contain %q, got %q", expected, config)
		}
	}

	deployment := findRenderedResource(t, resources, "Deployment", "data-science-thanos-querier-proxy")
	serviceAccount, found, err := unstructured.NestedString(
		deployment.Object, "spec", "template", "spec", "serviceAccountName",
	)
	if err != nil || !found || serviceAccount != "data-science-thanos-querier-proxy" {
		t.Fatalf("Thanos proxy must use its service account: found=%t value=%q error=%v", found, serviceAccount, err)
	}
	containers := nestedSliceAt(t, deployment, "spec", "template", "spec", "containers")
	if len(containers) != 2 {
		t.Fatalf("Thanos proxy must have kube-rbac-proxy and prom-label-proxy containers, got %d", len(containers))
	}
	assertContainerArgs(t, findContainer(t, containers, "kube-rbac-proxy"),
		"--secure-listen-address=0.0.0.0:8443",
		"--upstream=http://127.0.0.1:9091/",
		"--config-file=/etc/kube-rbac-proxy/kube-rbac-proxy.yaml",
		"--tls-cert-file=/etc/tls/private/tls.crt",
		"--tls-private-key-file=/etc/tls/private/tls.key",
		"--tls-min-version=VersionTLS12",
	)
	assertContainerArgs(t, findContainer(t, containers, "prom-label-proxy"),
		"--insecure-listen-address=127.0.0.1:9091",
		"--upstream=http://thanos-querier-data-science-thanos-querier."+testMonitoringNamespace+".svc.cluster.local:10902",
		"--label=namespace",
		"--enable-label-apis",
	)

	service := findRenderedResource(t, resources, "Service", "data-science-thanos-querier-proxy")
	ports := nestedSlice(t, service, "spec", "ports")
	if len(ports) != 1 || asMap(t, ports[0])["port"] != int64(8443) || asMap(t, ports[0])["targetPort"] != "https" {
		t.Fatalf("Thanos proxy Service must expose HTTPS port 8443: %#v", ports)
	}
}

func TestThanosQuerierProxyNetworkPolicyTemplateContract(t *testing.T) {
	resources := renderObservabilityTemplate(t, ThanosQuerierProxyNetworkPolicyTemplate, proxyTemplateData())
	policy := findRenderedResource(t, resources, "NetworkPolicy", "data-science-thanos-querier-proxy-ingress")

	selector, found, err := unstructured.NestedString(policy.Object, "spec", "podSelector", "matchLabels", "app")
	if err != nil || !found || selector != "data-science-thanos-querier-proxy" {
		t.Fatalf("Thanos proxy NetworkPolicy must select the proxy pods: found=%t value=%q error=%v", found, selector, err)
	}
	ingress := nestedSlice(t, policy, "spec", "ingress")
	if len(ingress) != 1 {
		t.Fatalf("Thanos proxy NetworkPolicy must have one ingress rule, got %d", len(ingress))
	}
	ingressRule := asMap(t, ingress[0])
	fromValues, ok := ingressRule["from"].([]any)
	if !ok || len(fromValues) != 1 {
		t.Fatalf("Thanos proxy NetworkPolicy must have one ingress source, got %#v", ingressRule["from"])
	}
	from := asMap(t, fromValues[0])
	namespaceSelector := asMap(t, from["namespaceSelector"])
	matchLabels := asMap(t, namespaceSelector["matchLabels"])
	if len(matchLabels) != 1 || matchLabels["policy-group.network.openshift.io/ingress"] != "" {
		t.Fatalf("Thanos proxy NetworkPolicy must select the OpenShift ingress policy group: %#v", matchLabels)
	}
	portValues, ok := ingressRule["ports"].([]any)
	if !ok || len(portValues) != 1 {
		t.Fatalf("Thanos proxy NetworkPolicy must have one port, got %#v", ingressRule["ports"])
	}
	ports := asMap(t, portValues[0])
	if ports["protocol"] != "TCP" || ports["port"] != int64(8443) {
		t.Fatalf("Thanos proxy NetworkPolicy must allow router HTTPS ingress: %#v", ports)
	}
}

func TestPrometheusNamespaceProxyNetworkPolicyTemplateContract(t *testing.T) {
	resources := renderObservabilityTemplate(t, PrometheusNamespaceProxyNetworkPolicyTemplate, proxyTemplateData())
	policy := findRenderedResource(t, resources, "NetworkPolicy", "data-science-prometheus-proxy-ingress")
	ingress := nestedSlice(t, policy, "spec", "ingress")
	if len(ingress) < 1 {
		t.Fatalf("Prometheus namespace proxy NetworkPolicy must have router ingress, got %d rules", len(ingress))
	}

	firstRule := asMap(t, ingress[0])
	fromValues, ok := firstRule["from"].([]any)
	if !ok || len(fromValues) != 1 {
		t.Fatalf("Prometheus namespace proxy NetworkPolicy router ingress must have one source, got %#v", firstRule["from"])
	}
	from := asMap(t, fromValues[0])
	namespaceSelector := asMap(t, from["namespaceSelector"])
	matchLabels := asMap(t, namespaceSelector["matchLabels"])
	if len(matchLabels) != 1 || matchLabels["policy-group.network.openshift.io/ingress"] != "" {
		t.Fatalf("Prometheus namespace proxy NetworkPolicy must select the OpenShift ingress policy group: %#v", matchLabels)
	}
}

func TestThanosQuerierRouteUsesAuthorizedProxy(t *testing.T) {
	route := findRenderedResource(t, renderObservabilityTemplate(t, ThanosQuerierRouteTemplate, proxyTemplateData()), "Route", "data-science-thanos-querier-route")

	if target, found, err := unstructured.NestedString(route.Object, "spec", "to", "name"); err != nil || !found || target != "data-science-thanos-querier-proxy" {
		t.Fatalf("Thanos Route must target the authorized proxy: found=%t value=%q error=%v", found, target, err)
	}
	if targetPort, found, err := unstructured.NestedString(route.Object, "spec", "port", "targetPort"); err != nil || !found || targetPort != "https" {
		t.Fatalf("Thanos Route must target the proxy HTTPS port: found=%t value=%q error=%v", found, targetPort, err)
	}
	if termination, found, err := unstructured.NestedString(route.Object, "spec", "tls", "termination"); err != nil || !found || termination != "reencrypt" {
		t.Fatalf("Thanos Route must re-encrypt to the proxy: found=%t value=%q error=%v", found, termination, err)
	}
}

func TestPrometheusNamespaceProxyTemplateContract(t *testing.T) {
	resources := renderObservabilityTemplate(t, PrometheusNamespaceProxyTemplate, proxyTemplateData())

	clusterRole := findRenderedResource(t, resources, "ClusterRole", "data-science-metrics-view")
	if labels := clusterRole.GetLabels(); len(labels) != 4 || labels["rbac.authorization.k8s.io/aggregate-to-view"] != "true" ||
		labels["rbac.authorization.k8s.io/aggregate-to-edit"] != "true" ||
		labels["rbac.authorization.k8s.io/aggregate-to-admin"] != "true" ||
		labels["platform.opendatahub.io/part-of"] != "monitoring" {
		t.Fatalf("metrics ClusterRole must aggregate to view/edit/admin: %#v", labels)
	}
	rules := nestedSlice(t, clusterRole, "rules")
	if len(rules) != 1 {
		t.Fatalf("expected one metrics ClusterRole rule, got %d", len(rules))
	}
	rule := asMap(t, rules[0])
	if len(rule) != 3 {
		t.Fatalf("metrics ClusterRole rule must not contain extra permissions: %#v", rule)
	}
	assertStringSet(t, rule, "apiGroups", []string{"metrics.k8s.io"})
	assertStringSet(t, rule, "resources", []string{"pods"})
	assertStringSet(t, rule, "verbs", []string{"create"})

	assertClusterRoleBinding(
		t,
		findRenderedResource(t, resources, "ClusterRoleBinding", "data-science-prometheus-namespace-proxy"),
		"cluster-monitoring-view",
		"data-science-prometheus-namespace-proxy",
		testMonitoringNamespace,
	)
	assertClusterRoleBinding(
		t,
		findRenderedResource(t, resources, "ClusterRoleBinding", "data-science-prometheus-namespace-proxy-auth-delegator"),
		"system:auth-delegator",
		"data-science-prometheus-namespace-proxy",
		testMonitoringNamespace,
	)

	configMap := findRenderedResource(t, resources, "ConfigMap", "data-science-prometheus-namespace-proxy-config")
	config, found, err := unstructured.NestedString(configMap.Object, "data", "kube-rbac-proxy.yaml")
	if err != nil || !found {
		t.Fatalf("namespace proxy authorization config is missing: found=%t error=%v", found, err)
	}
	for _, expected := range []string{
		"byQueryParameter:",
		"name: \"namespace\"",
		"apiGroup: metrics.k8s.io",
		"resource: pods",
		"namespace: \"{{ .Value }}\"",
	} {
		if !strings.Contains(config, expected) {
			t.Errorf("namespace proxy config must contain %q, got %q", expected, config)
		}
	}

	deployment := findRenderedResource(t, resources, "Deployment", "data-science-prometheus-namespace-proxy")
	serviceAccount, found, err := unstructured.NestedString(
		deployment.Object, "spec", "template", "spec", "serviceAccountName",
	)
	if err != nil || !found || serviceAccount != "data-science-prometheus-namespace-proxy" {
		t.Fatalf("namespace proxy must use its service account: found=%t value=%q error=%v", found, serviceAccount, err)
	}
	containers := nestedSliceAt(t, deployment, "spec", "template", "spec", "containers")
	if len(containers) != 2 {
		t.Fatalf("namespace proxy must have kube-rbac-proxy and prom-label-proxy containers, got %d", len(containers))
	}
	assertContainerArgs(t, asMap(t, containers[0]),
		"--secure-listen-address=0.0.0.0:8443",
		"--upstream=http://127.0.0.1:9091/",
		"--config-file=/etc/kube-rbac-proxy/kube-rbac-proxy.yaml",
		"--tls-cert-file=/etc/tls/private/tls.crt",
		"--tls-private-key-file=/etc/tls/private/tls.key",
		"--tls-min-version=VersionTLS12",
	)
	assertContainerArgs(t, findContainer(t, containers, "prom-label-proxy"),
		"--insecure-listen-address=127.0.0.1:9091",
		"--upstream=https://prometheus-operated."+testMonitoringNamespace+".svc:9090",
		"--label=namespace",
		"--enable-label-apis",
	)

	service := findRenderedResource(t, resources, "Service", "data-science-prometheus-namespace-proxy")
	ports := nestedSlice(t, service, "spec", "ports")
	if len(ports) != 1 || asMap(t, ports[0])["port"] != int64(8443) {
		t.Fatalf("namespace proxy Service must expose port 8443: %#v", ports)
	}
}

func TestPrometheusClusterProxyTemplateContract(t *testing.T) {
	resources := renderObservabilityTemplate(t, PrometheusClusterProxyTemplate, proxyTemplateData())

	secret := findRenderedResource(t, resources, "Secret", "data-science-prometheus-cluster-proxy-kube-rbac-proxy")
	config, found, err := unstructured.NestedString(secret.Object, "stringData", "config.yaml")
	if err != nil || !found {
		t.Fatalf("cluster proxy authorization config is missing: found=%t error=%v", found, err)
	}
	for _, expected := range []string{
		"apiGroup: metrics.k8s.io",
		"resource: nodes",
		"verb: get",
	} {
		if !strings.Contains(config, expected) {
			t.Errorf("cluster proxy config must contain %q, got %q", expected, config)
		}
	}

	deployment := findRenderedResource(t, resources, "Deployment", "data-science-prometheus-cluster-proxy")
	containers := nestedSliceAt(t, deployment, "spec", "template", "spec", "containers")
	if len(containers) != 1 {
		t.Fatalf("cluster proxy must have one kube-rbac-proxy container, got %d", len(containers))
	}
	assertContainerArgs(t, findContainer(t, containers, "kube-rbac-proxy"),
		"--secure-listen-address=0.0.0.0:8443",
		"--upstream=https://prometheus-operated."+testMonitoringNamespace+".svc:9090",
		"--config-file=/etc/kube-rbac-proxy/config.yaml",
		"--tls-cert-file=/etc/tls/private/tls.crt",
		"--tls-private-key-file=/etc/tls/private/tls.key",
		"--tls-min-version=VersionTLS12",
		"--upstream-ca-file=/etc/prometheus-ca/service-ca.crt",
		"--upstream-client-cert-file=/etc/prometheus-client/tls.crt",
		"--upstream-client-key-file=/etc/prometheus-client/tls.key",
		"--allow-paths=/metrics",
	)

	service := findRenderedResource(t, resources, "Service", "data-science-prometheus-cluster-proxy")
	ports := nestedSlice(t, service, "spec", "ports")
	if len(ports) != 1 || asMap(t, ports[0])["port"] != int64(8443) {
		t.Fatalf("cluster proxy Service must expose port 8443: %#v", ports)
	}
	assertClusterRoleBinding(
		t,
		findRenderedResource(t, resources, "ClusterRoleBinding", "data-science-prometheus-cluster-proxy"),
		"cluster-monitoring-view",
		"data-science-prometheus-cluster-proxy",
		testMonitoringNamespace,
	)
	assertClusterRoleBinding(
		t,
		findRenderedResource(t, resources, "ClusterRoleBinding", "data-science-prometheus-cluster-proxy-auth-delegator"),
		"system:auth-delegator",
		"data-science-prometheus-cluster-proxy",
		testMonitoringNamespace,
	)
	route := findRenderedResource(t, resources, "Route", "data-science-prometheus-cluster-proxy")
	if target, found, err := unstructured.NestedString(route.Object, "spec", "to", "name"); err != nil || !found || target != "data-science-prometheus-cluster-proxy" {
		t.Fatalf("cluster proxy Route must target its Service: found=%t value=%q error=%v", found, target, err)
	}
	if termination, found, err := unstructured.NestedString(route.Object, "spec", "tls", "termination"); err != nil || !found || termination != "reencrypt" {
		t.Fatalf("cluster proxy Route must use reencrypt TLS: found=%t value=%q error=%v", found, termination, err)
	}
}

type persesDatasourceContract struct {
	name          string
	template      string
	datasource    string
	secret        string
	url           string
	defaultValue  bool
	expectTLS     bool
	queryParamKey string
}

func TestPersesPrometheusDatasourceTemplateContracts(t *testing.T) {
	tests := []persesDatasourceContract{
		{
			name:         "data science datasource",
			template:     PersesDatasourcePrometheusTemplate,
			datasource:   "data-science-prometheus-datasource",
			url:          "http://thanos-querier-data-science-thanos-querier." + testMonitoringNamespace + ".svc.cluster.local:10902",
			defaultValue: false,
		},
		{
			name:         "cluster datasource",
			template:     PersesDatasourceClusterPrometheusTemplate,
			datasource:   "cluster-prometheus-datasource",
			secret:       "cluster-prometheus-datasource-secret",
			url:          "https://thanos-querier.openshift-monitoring.svc:9091",
			defaultValue: true,
			expectTLS:    true,
		},
		{
			name:          "cluster tenancy datasource",
			template:      PersesDatasourceClusterPrometheusTenancyTemplate,
			datasource:    "cluster-prometheus-tenancy-datasource",
			secret:        "cluster-prometheus-tenancy-datasource-secret",
			url:           "https://thanos-querier.openshift-monitoring.svc:9092",
			defaultValue:  false,
			expectTLS:     true,
			queryParamKey: "namespace",
		},
	}

	for _, apiVersion := range []string{"v1alpha1", "v1alpha2"} {
		for _, test := range tests {
			t.Run(apiVersion+"/"+test.name, func(t *testing.T) {
				resources := renderObservabilityTemplate(t, test.template, map[string]any{
					"Namespace":        testMonitoringNamespace,
					"PersesAPIVersion": apiVersion,
				})
				datasource := findRenderedResource(t, resources, "PersesDatasource", test.datasource)
				if datasource.GetAPIVersion() != "perses.dev/"+apiVersion {
					t.Fatalf("unexpected PersesDatasource apiVersion: got %q, want %q", datasource.GetAPIVersion(), "perses.dev/"+apiVersion)
				}
				assertPersesDatasourceConfig(t, datasource, test)
				assertPersesDatasourceTLS(t, datasource, test)
				assertPersesDatasourceSecret(t, resources, datasource, test)
				assertPersesDatasourceQueryParameter(t, datasource, test)
			})
		}
	}
}

func TestDeployPersesPrometheusIntegrationSelectsDatasourceTemplates(t *testing.T) {
	tests := []struct {
		name          string
		version       string
		datasourceGVK schema.GroupVersionKind
	}{
		{
			name:          "v1alpha1",
			version:       "v1alpha1",
			datasourceGVK: gvk.PersesDatasourceV1Alpha1,
		},
		{
			name:          "v1alpha2",
			version:       "v1alpha2",
			datasourceGVK: gvk.PersesDatasourceV1Alpha2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := newActionsTestScheme(t)
			registerCRDs(scheme, test.datasourceGVK)
			monitoring := newMonitoring(v1alpha1.MonitoringInstanceName)
			monitoring.Spec.Metrics = &v1alpha1.Metrics{Storage: &v1alpha1.MetricsStorage{}}
			conditionsManager := conditions.NewConditionsManager(monitoring, monitoring.Generation)
			var sources []rendertemplate.TemplateSource

			err := deployPersesPrometheusIntegration(
				context.Background(),
				fake.NewClientBuilder().WithScheme(scheme).Build(),
				monitoring,
				conditionsManager,
				&sources,
				test.version,
				true,
			)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			assertTemplatePaths(t, sources, []string{
				PersesDatasourcePrometheusTemplate,
				PersesDatasourceClusterPrometheusTemplate,
				PersesDatasourceClusterPrometheusTenancyTemplate,
			})
			condition := findCondition(monitoring, conditions.ConditionPersesPrometheusDataSourceAvailable)
			if condition == nil || condition.Status != metav1.ConditionTrue {
				t.Fatalf("PersesPrometheusDataSourceAvailable should be True, got %v", condition)
			}
		})
	}
}

func assertPersesDatasourceConfig(t *testing.T, datasource unstructured.Unstructured, contract persesDatasourceContract) {
	t.Helper()
	defaultValue, found, err := unstructured.NestedBool(datasource.Object, "spec", "config", "default")
	if err != nil || !found || defaultValue != contract.defaultValue {
		t.Fatalf("unexpected datasource default value: found=%t value=%t error=%v", found, defaultValue, err)
	}
	if plugin, found, err := unstructured.NestedString(datasource.Object, "spec", "config", "plugin", "kind"); err != nil || !found || plugin != "PrometheusDatasource" {
		t.Fatalf("datasource must use PrometheusDatasource plugin: found=%t value=%q error=%v", found, plugin, err)
	}
	if proxyKind, found, err := unstructured.NestedString(datasource.Object, "spec", "config", "plugin", "spec", "proxy", "kind"); err != nil || !found || proxyKind != "HTTPProxy" {
		t.Fatalf("datasource must use HTTPProxy: found=%t value=%q error=%v", found, proxyKind, err)
	}
	url, found, err := unstructured.NestedString(datasource.Object, "spec", "config", "plugin", "spec", "proxy", "spec", "url")
	if err != nil || !found || url != contract.url {
		t.Fatalf("unexpected datasource URL: found=%t value=%q want=%q error=%v", found, url, contract.url, err)
	}
}

func assertPersesDatasourceTLS(t *testing.T, datasource unstructured.Unstructured, contract persesDatasourceContract) {
	t.Helper()
	if !contract.expectTLS {
		if _, found, err := unstructured.NestedMap(datasource.Object, "spec", "client"); err != nil {
			t.Fatalf("reading non-TLS datasource client configuration: %v", err)
		} else if found {
			t.Fatal("non-TLS datasource must not define a client TLS configuration")
		}
		return
	}
	enableTLS, found, err := unstructured.NestedBool(datasource.Object, "spec", "client", "tls", "enable")
	if err != nil || !found || !enableTLS {
		t.Fatalf("cluster datasource must enable TLS: found=%t value=%t error=%v", found, enableTLS, err)
	}
	caName, found, err := unstructured.NestedString(datasource.Object, "spec", "client", "tls", "caCert", "name")
	if err != nil || !found || caName != "prometheus-web-tls-ca" {
		t.Fatalf("cluster datasource must use prometheus-web-tls-ca: found=%t value=%q error=%v", found, caName, err)
	}
	caType, found, err := unstructured.NestedString(datasource.Object, "spec", "client", "tls", "caCert", "type")
	if err != nil || !found || caType != "configmap" {
		t.Fatalf("cluster datasource must use a ConfigMap CA: found=%t value=%q error=%v", found, caType, err)
	}
	caNamespace, found, err := unstructured.NestedString(datasource.Object, "spec", "client", "tls", "caCert", "namespace")
	if err != nil || !found || caNamespace != datasource.GetNamespace() {
		t.Fatalf("cluster datasource must use the datasource namespace for its CA: found=%t value=%q error=%v", found, caNamespace, err)
	}
	certPath, found, err := unstructured.NestedString(datasource.Object, "spec", "client", "tls", "caCert", "certPath")
	if err != nil || !found || certPath != serviceCACertificateKey {
		t.Fatalf("cluster datasource must use service-ca.crt: found=%t value=%q error=%v", found, certPath, err)
	}
}

func assertPersesDatasourceSecret(t *testing.T, resources []unstructured.Unstructured, datasource unstructured.Unstructured, contract persesDatasourceContract) {
	t.Helper()
	if contract.secret == "" {
		return
	}
	secret := findRenderedResource(t, resources, "Secret", contract.secret)
	if secret.GetAPIVersion() != "v1" || secret.GetNamespace() != testMonitoringNamespace {
		t.Fatalf("datasource Secret must be a v1 Secret in %q: apiVersion=%q namespace=%q", testMonitoringNamespace, secret.GetAPIVersion(), secret.GetNamespace())
	}
	if secretType, found, err := unstructured.NestedString(secret.Object, "type"); err != nil || !found || secretType != "kubernetes.io/service-account-token" {
		t.Fatalf("datasource Secret must be a service-account token Secret: found=%t value=%q error=%v", found, secretType, err)
	}
	if labels := secret.GetLabels(); len(labels) != 1 || labels["platform.opendatahub.io/part-of"] != "monitoring" {
		t.Fatalf("datasource Secret must have only the monitoring ownership label: %#v", labels)
	}
	serviceAccount, found, err := unstructured.NestedString(secret.Object, "metadata", "annotations", "kubernetes.io/service-account.name")
	if err != nil || !found || serviceAccount != "data-science-prometheus-cluster-proxy" {
		t.Fatalf("datasource Secret must reference the cluster proxy service account: found=%t value=%q error=%v", found, serviceAccount, err)
	}
	secretReference, found, err := unstructured.NestedString(datasource.Object, "spec", "config", "plugin", "spec", "proxy", "spec", "secret")
	if err != nil || !found || secretReference != contract.secret {
		t.Fatalf("datasource must reference Secret %q: found=%t value=%q error=%v", contract.secret, found, secretReference, err)
	}
}

func assertPersesDatasourceQueryParameter(t *testing.T, datasource unstructured.Unstructured, contract persesDatasourceContract) {
	t.Helper()
	if contract.queryParamKey == "" {
		if _, found, err := unstructured.NestedMap(datasource.Object, "spec", "config", "plugin", "spec", "queryParams"); err != nil {
			t.Fatalf("reading non-tenancy datasource query parameters: %v", err)
		} else if found {
			t.Fatal("non-tenancy datasource must not define query parameters")
		}
		return
	}
	queryParams, found, err := unstructured.NestedMap(datasource.Object, "spec", "config", "plugin", "spec", "queryParams")
	if err != nil || !found || len(queryParams) != 1 {
		t.Fatalf("tenancy datasource must define only one query parameter: found=%t value=%#v error=%v", found, queryParams, err)
	}
	queryParam, found, err := unstructured.NestedString(datasource.Object, "spec", "config", "plugin", "spec", "queryParams", contract.queryParamKey)
	if err != nil || !found || queryParam != "${namespace:queryparam}" {
		t.Fatalf("tenancy datasource must inject the namespace query parameter: found=%t value=%q error=%v", found, queryParam, err)
	}
}

func proxyTemplateData() map[string]any {
	return map[string]any{
		"Namespace":           testMonitoringNamespace,
		"KubeRBACProxyImage":  "example.invalid/kube-rbac-proxy:test",
		"PromLabelProxyImage": "example.invalid/prom-label-proxy:test",
		"TLSMinVersion":       "VersionTLS12",
		"TLSCipherSuites":     "",
	}
}

func renderObservabilityTemplate(t *testing.T, path string, data map[string]any) []unstructured.Unstructured {
	t.Helper()
	resources, err := rendertemplate.Render(context.Background(), nil, []rendertemplate.TemplateSource{{
		FS:   resourcesFS,
		Path: path,
	}}, data)
	if err != nil {
		t.Fatalf("template %q must render as valid YAML: %v", path, err)
	}
	return resources
}

func findRenderedResource(t *testing.T, resources []unstructured.Unstructured, kind, name string) unstructured.Unstructured {
	t.Helper()
	for _, resource := range resources {
		if resource.GetKind() == kind && resource.GetName() == name {
			return resource
		}
	}
	t.Fatalf("rendered template did not contain %s %q", kind, name)
	return unstructured.Unstructured{}
}

func assertTemplatePaths(t *testing.T, sources []rendertemplate.TemplateSource, want []string) {
	t.Helper()
	paths := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		if _, exists := paths[source.Path]; exists {
			t.Errorf("template %q was added more than once", source.Path)
		}
		paths[source.Path] = struct{}{}
	}
	if len(paths) != len(want) {
		t.Fatalf("expected %d distinct templates, got %d", len(want), len(paths))
	}
	for _, path := range want {
		if _, found := paths[path]; !found {
			t.Errorf("templates do not include %q", path)
		}
	}
}

func nestedSlice(t *testing.T, resource unstructured.Unstructured, fields ...string) []any {
	t.Helper()
	return nestedSliceAt(t, resource, fields...)
}

func nestedSliceAt(t *testing.T, resource unstructured.Unstructured, fields ...string) []any {
	t.Helper()
	values, found, err := unstructured.NestedSlice(resource.Object, fields...)
	if err != nil || !found {
		t.Fatalf("resource %s/%s is missing %s: found=%t error=%v", resource.GetKind(), resource.GetName(), strings.Join(fields, "."), found, err)
	}
	return values
}

func asMap(t *testing.T, value any) map[string]any {
	t.Helper()
	result, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", value)
	}
	return result
}

func assertStringSet(t *testing.T, object map[string]any, field string, want []string) {
	t.Helper()
	got, found, err := unstructured.NestedStringSlice(object, field)
	if err != nil || !found {
		t.Fatalf("field %q is missing: found=%t error=%v", field, found, err)
	}
	wantSet := make(map[string]struct{}, len(want))
	for _, value := range want {
		wantSet[value] = struct{}{}
	}
	gotSet := make(map[string]struct{}, len(got))
	for _, value := range got {
		gotSet[value] = struct{}{}
	}
	if len(got) != len(gotSet) || len(gotSet) != len(wantSet) {
		t.Fatalf("field %q: got %v, want exactly %v", field, got, want)
	}
	for value := range wantSet {
		if _, found := gotSet[value]; !found {
			t.Fatalf("field %q: got %v, want exactly %v", field, got, want)
		}
	}
}

func assertClusterRoleBinding(t *testing.T, binding unstructured.Unstructured, roleName, subjectName, subjectNamespace string) {
	t.Helper()
	roleRef, found, err := unstructured.NestedMap(binding.Object, "roleRef")
	if err != nil || !found || len(roleRef) != 3 ||
		roleRef["apiGroup"] != "rbac.authorization.k8s.io" ||
		roleRef["kind"] != "ClusterRole" ||
		roleRef["name"] != roleName {
		t.Fatalf("unexpected ClusterRoleBinding roleRef: %#v", roleRef)
	}
	subjects := nestedSlice(t, binding, "subjects")
	if len(subjects) != 1 {
		t.Fatalf("ClusterRoleBinding must have one subject, got %d", len(subjects))
	}
	subject := asMap(t, subjects[0])
	if len(subject) != 3 || subject["kind"] != "ServiceAccount" ||
		subject["name"] != subjectName || subject["namespace"] != subjectNamespace {
		t.Fatalf("unexpected ClusterRoleBinding subject: %#v", subject)
	}
}

func findContainer(t *testing.T, containers []any, name string) map[string]any {
	t.Helper()
	for _, rawContainer := range containers {
		container := asMap(t, rawContainer)
		if container["name"] == name {
			return container
		}
	}
	t.Fatalf("container %q was not rendered", name)
	return nil
}

func assertContainerArgs(t *testing.T, container map[string]any, expected ...string) {
	t.Helper()
	args, ok := container["args"].([]any)
	if !ok {
		t.Fatalf("container %q args have unexpected type %T", container["name"], container["args"])
	}
	for _, want := range expected {
		if !containsString(args, want) {
			t.Errorf("container %q args do not contain %q: %#v", container["name"], want, args)
		}
	}
}
