package controller

import (
	"context"
	"testing"

	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

//nolint:gocyclo,maintidx // The test validates each rendered RBAC object and rule boundary.
func TestCollectorRBACAllowsTargetAllocatorSecretInformer(t *testing.T) {
	resources, err := rendertemplate.Render(context.Background(), nil, []rendertemplate.TemplateSource{{
		FS:   resourcesFS,
		Path: CollectorRBACTemplate,
	}}, map[string]any{
		"Namespace":                       "redhat-ods-monitoring",
		"Metrics":                         true,
		"TargetAllocatorServiceAccount":   targetAllocatorServiceAccountName,
		"TargetAllocatorSecretNamespaces": []string{"redhat-ods-monitoring", "team-a"},
	})
	if err != nil {
		t.Fatalf("collector RBAC template must render as valid YAML: %v", err)
	}

	var clusterRole unstructured.Unstructured
	var targetAllocatorClusterRole unstructured.Unstructured
	var targetAllocatorRole unstructured.Unstructured
	var collectorBinding unstructured.Unstructured
	var legacyTargetAllocatorBinding unstructured.Unstructured
	var targetAllocatorDiscoveryBinding unstructured.Unstructured
	var secretRoles int
	var secretRoleBindings int
	for _, resource := range resources {
		kind, _, err := unstructured.NestedString(resource.Object, "kind")
		if err != nil {
			t.Fatalf("failed to read rendered resource kind: %v", err)
		}
		name := resource.GetName()
		switch {
		case kind == "ClusterRole" && name == "generate-processors-role":
			clusterRole = resource
		case kind == "ClusterRole" && name == "generate-processors-targetallocator-role":
			targetAllocatorClusterRole = resource
		case kind == "ClusterRoleBinding" && name == "generate-processors-collector-rolebinding":
			collectorBinding = resource
		case kind == "ClusterRoleBinding" && name == "generate-processors-targetallocator-rolebinding":
			legacyTargetAllocatorBinding = resource
		case kind == "ClusterRoleBinding" && name == "generate-processors-targetallocator-discovery-rolebinding":
			targetAllocatorDiscoveryBinding = resource
		case kind == "Role" && name == "data-science-collector-targetallocator-secrets":
			secretRoles++
			targetAllocatorRole = resource
		case kind == "RoleBinding" && name == "data-science-collector-targetallocator-secrets":
			secretRoleBindings++
		}
	}
	if clusterRole.Object == nil {
		t.Fatal("collector RBAC template must render a ClusterRole")
	}
	if targetAllocatorClusterRole.Object == nil {
		t.Fatal("TargetAllocator RBAC template must render a dedicated ClusterRole")
	}

	rules, found, err := unstructured.NestedSlice(clusterRole.Object, "rules")
	if err != nil || !found {
		t.Fatalf("collector ClusterRole rules must be present: found=%t, error=%v", found, err)
	}

	var hasSecretReadRule bool
	for _, rawRule := range rules {
		rule, ok := rawRule.(map[string]any)
		if !ok {
			t.Fatalf("collector RBAC rule has unexpected type: %T", rawRule)
		}
		if containsString(rule["resources"], "secrets") {
			hasSecretReadRule = true
		}
	}

	if hasSecretReadRule {
		t.Error("collector ClusterRole must not grant Secret access")
	}
	targetAllocatorRules, found, err := unstructured.NestedSlice(targetAllocatorClusterRole.Object, "rules")
	if err != nil || !found {
		t.Fatalf("TargetAllocator ClusterRole rules must be present: found=%t, error=%v", found, err)
	}
	if !hasRBACRule(targetAllocatorRules, "", []string{"pods", "namespaces", "nodes", "nodes/metrics", "services", "endpoints"}, []string{"get", "list", "watch"}) {
		t.Error("TargetAllocator ClusterRole must allow Kubernetes target discovery")
	}
	if !hasRBACRule(targetAllocatorRules, "", []string{"configmaps"}, []string{"get"}) {
		t.Error("TargetAllocator ClusterRole must allow ConfigMap discovery")
	}
	if !hasRBACRule(targetAllocatorRules, "networking.k8s.io", []string{"ingresses"}, []string{"get", "list", "watch"}) {
		t.Error("TargetAllocator ClusterRole must allow ingress discovery")
	}
	if !hasRBACRule(targetAllocatorRules, "monitoring.coreos.com", []string{"podmonitors", "servicemonitors", "scrapeconfigs"}, []string{"get", "list", "watch"}) {
		t.Error("TargetAllocator ClusterRole must allow Prometheus monitor and ScrapeConfig discovery")
	}
	if !hasRBACRule(targetAllocatorRules, "monitoring.coreos.com", []string{"probes"}, []string{"get", "list", "watch"}) {
		t.Error("TargetAllocator ClusterRole must allow Prometheus probe discovery")
	}
	if !hasNonResourceRule(targetAllocatorRules, []string{"/metrics", "/api", "/api/*", "/apis", "/apis/*"}, "get") {
		t.Error("TargetAllocator ClusterRole must allow Kubernetes API discovery")
	}
	if secretRoles != 2 || secretRoleBindings != 2 {
		t.Errorf("expected one Secret Role and RoleBinding per namespace, got roles=%d bindings=%d", secretRoles, secretRoleBindings)
	}

	if targetAllocatorRole.Object == nil {
		t.Fatal("TargetAllocator Secret Role must render")
	}
	secretRules, found, err := unstructured.NestedSlice(targetAllocatorRole.Object, "rules")
	if err != nil || !found || len(secretRules) != 1 {
		t.Fatalf("TargetAllocator Secret Role must contain one rule: found=%t, error=%v, rules=%v", found, err, secretRules)
	}
	secretRule, ok := secretRules[0].(map[string]any)
	if !ok || !containsString(secretRule["resources"], "secrets") ||
		!containsString(secretRule["verbs"], "get") ||
		!containsString(secretRule["verbs"], "list") ||
		!containsString(secretRule["verbs"], "watch") {
		t.Errorf("TargetAllocator Secret Role must grant get/list/watch on Secrets: %v", secretRule)
	}

	if collectorBinding.Object == nil || legacyTargetAllocatorBinding.Object == nil || targetAllocatorDiscoveryBinding.Object == nil {
		t.Fatal("collector and both TargetAllocator ClusterRoleBindings must render")
	}
	subjects, found, err := unstructured.NestedSlice(targetAllocatorDiscoveryBinding.Object, "subjects")
	if err != nil || !found || len(subjects) != 1 {
		t.Fatalf("TargetAllocator ClusterRoleBinding must have one subject: found=%t, error=%v, subjects=%v", found, err, subjects)
	}
	subject, ok := subjects[0].(map[string]any)
	if !ok || subject["name"] != targetAllocatorServiceAccountName {
		t.Errorf("TargetAllocator ClusterRoleBinding must target %q", targetAllocatorServiceAccountName)
	}
	targetAllocatorRoleRef, found, err := unstructured.NestedString(targetAllocatorDiscoveryBinding.Object, "roleRef", "name")
	if err != nil || !found || targetAllocatorRoleRef != "generate-processors-targetallocator-role" {
		t.Errorf("TargetAllocator discovery binding must use the dedicated role, got %q (found=%t, error=%v)", targetAllocatorRoleRef, found, err)
	}
	legacyRoleRef, found, err := unstructured.NestedString(legacyTargetAllocatorBinding.Object, "roleRef", "name")
	if err != nil || !found || legacyRoleRef != "generate-processors-role" {
		t.Errorf("legacy TargetAllocator binding must retain its original role, got %q (found=%t, error=%v)", legacyRoleRef, found, err)
	}
}

func hasRBACRule(rules []any, apiGroup string, resources, verbs []string) bool {
	for _, rawRule := range rules {
		rule, ok := rawRule.(map[string]any)
		if !ok {
			continue
		}
		groups, _ := rule["apiGroups"].([]any)
		ruleResources, _ := rule["resources"].([]any)
		ruleVerbs, _ := rule["verbs"].([]any)
		if containsString(groups, apiGroup) && containsAllStrings(ruleResources, resources) && containsAllStrings(ruleVerbs, verbs) {
			return true
		}
	}
	return false
}

func hasNonResourceRule(rules []any, urls []string, verb string) bool {
	for _, rawRule := range rules {
		rule, ok := rawRule.(map[string]any)
		if !ok {
			continue
		}
		ruleURLs, _ := rule["nonResourceURLs"].([]any)
		verbs, _ := rule["verbs"].([]any)
		if containsAllStrings(ruleURLs, urls) && containsString(verbs, verb) {
			return true
		}
	}
	return false
}

func containsAllStrings(values []any, expected []string) bool {
	for _, value := range expected {
		if !containsString(values, value) {
			return false
		}
	}
	return true
}

func TestCollectorRBACOmitsTargetAllocatorResourcesWithoutMetrics(t *testing.T) {
	resources, err := rendertemplate.Render(context.Background(), nil, []rendertemplate.TemplateSource{{
		FS:   resourcesFS,
		Path: CollectorRBACTemplate,
	}}, map[string]any{
		"Namespace": "redhat-ods-monitoring",
		"Metrics":   false,
	})
	if err != nil {
		t.Fatalf("collector RBAC template must render without metrics: %v", err)
	}

	for _, resource := range resources {
		if resource.GetName() == targetAllocatorServiceAccountName ||
			resource.GetName() == "generate-processors-targetallocator-rolebinding" ||
			resource.GetName() == "generate-processors-targetallocator-discovery-rolebinding" ||
			resource.GetName() == "generate-processors-targetallocator-role" ||
			resource.GetName() == "data-science-collector-targetallocator-secrets" {
			t.Errorf("TargetAllocator resource %s must not render without metrics", resource.GetName())
		}
	}
}

func TestCollectorRBACDefersTargetAllocatorSecretRBACToScoper(t *testing.T) {
	resources, err := rendertemplate.Render(context.Background(), nil, []rendertemplate.TemplateSource{{
		FS:   resourcesFS,
		Path: CollectorRBACTemplate,
	}}, map[string]any{
		"Namespace":                       "redhat-ods-monitoring",
		"Metrics":                         true,
		"RBACScoperEnabled":               true,
		"TargetAllocatorServiceAccount":   targetAllocatorServiceAccountName,
		"TargetAllocatorSecretNamespaces": []string{"inference"},
	})
	if err != nil {
		t.Fatalf("collector RBAC template must render in scoper mode: %v", err)
	}

	for _, resource := range resources {
		if resource.GetName() == targetAllocatorSecretRBACName &&
			(resource.GetKind() == "Role" || resource.GetKind() == "RoleBinding") {
			t.Errorf("TargetAllocator Secret RBAC %s must be owned by the scoper, not the manager", resource.GetKind())
		}
	}
}
