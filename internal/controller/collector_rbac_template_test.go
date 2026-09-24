package controller

import (
	"context"
	"testing"

	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

//nolint:gocyclo // The test validates each rendered RBAC object and rule boundary.
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
	var targetAllocatorRole unstructured.Unstructured
	var collectorBinding unstructured.Unstructured
	var targetAllocatorBinding unstructured.Unstructured
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
		case kind == "ClusterRoleBinding" && name == "generate-processors-collector-rolebinding":
			collectorBinding = resource
		case kind == "ClusterRoleBinding" && name == "generate-processors-targetallocator-rolebinding":
			targetAllocatorBinding = resource
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

	if collectorBinding.Object == nil || targetAllocatorBinding.Object == nil {
		t.Fatal("both collector and TargetAllocator ClusterRoleBindings must render")
	}
	subjects, found, err := unstructured.NestedSlice(targetAllocatorBinding.Object, "subjects")
	if err != nil || !found || len(subjects) != 1 {
		t.Fatalf("TargetAllocator ClusterRoleBinding must have one subject: found=%t, error=%v, subjects=%v", found, err, subjects)
	}
	subject, ok := subjects[0].(map[string]any)
	if !ok || subject["name"] != targetAllocatorServiceAccountName {
		t.Errorf("TargetAllocator ClusterRoleBinding must target %q", targetAllocatorServiceAccountName)
	}
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
			resource.GetName() == "data-science-collector-targetallocator-secrets" {
			t.Errorf("TargetAllocator resource %s must not render without metrics", resource.GetName())
		}
	}
}
