package e2e_test

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestOperatorGroupTargetsNamespace(t *testing.T) {
	tests := []struct {
		name      string
		group     *unstructured.Unstructured
		namespace string
		labels    map[string]string
		want      bool
	}{
		{
			name: "explicit target",
			group: &unstructured.Unstructured{Object: map[string]any{
				"spec": map[string]any{"targetNamespaces": []any{"team-a", "team-b"}},
			}},
			namespace: "team-b",
			want:      true,
		},
		{
			name: "different explicit target",
			group: &unstructured.Unstructured{Object: map[string]any{
				"spec": map[string]any{"targetNamespaces": []any{"team-b"}},
			}},
			namespace: "team-a",
			want:      false,
		},
		{
			name:      "omitted target means all namespaces",
			group:     &unstructured.Unstructured{Object: map[string]any{}},
			namespace: "team-a",
			want:      true,
		},
		{
			name: "empty target means all namespaces",
			group: &unstructured.Unstructured{Object: map[string]any{
				"spec": map[string]any{"targetNamespaces": []any{}},
			}},
			namespace: "team-a",
			want:      true,
		},
		{
			name: "selector matches namespace labels",
			group: &unstructured.Unstructured{Object: map[string]any{
				"spec": map[string]any{"selector": map[string]any{
					"matchLabels": map[string]any{"team": "observability"},
				}},
			}},
			namespace: "team-a",
			labels:    map[string]string{"team": "observability"},
			want:      true,
		},
		{
			name: "selector excludes namespace labels",
			group: &unstructured.Unstructured{Object: map[string]any{
				"spec": map[string]any{"selector": map[string]any{
					"matchLabels": map[string]any{"team": "observability"},
				}},
			}},
			namespace: "team-a",
			labels:    map[string]string{"team": "platform"},
			want:      false,
		},
		{
			name: "explicit target takes precedence over selector",
			group: &unstructured.Unstructured{Object: map[string]any{
				"spec": map[string]any{
					"targetNamespaces": []any{"team-a"},
					"selector":         map[string]any{"matchLabels": map[string]any{"team": "other"}},
				},
			}},
			namespace: "team-a",
			labels:    map[string]string{"team": "observability"},
			want:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := operatorGroupTargetsNamespace(tc.group, tc.namespace, tc.labels); got != tc.want {
				t.Errorf("operatorGroupTargetsNamespace(%q): want %t, got %t", tc.namespace, tc.want, got)
			}
		})
	}
}

func TestInstallPlanMatchesSource(t *testing.T) {
	tests := []struct {
		name        string
		installPlan *unstructured.Unstructured
		source      string
		csvName     string
		want        bool
	}{
		{
			name: "complete OLM plan from requested source contains current CSV",
			installPlan: &unstructured.Unstructured{Object: map[string]any{
				"spec": map[string]any{
					"clusterServiceVersionNames": []any{"rhcl-operator.v1.0.0"},
				},
				"status": map[string]any{
					"phase":          "Complete",
					"catalogSources": []any{"redhat-operators"},
					"plan": []any{map[string]any{"resource": map[string]any{
						"kind": "ClusterServiceVersion", "name": "rhcl-operator.v1.0.0", "sourceName": "redhat-operators",
					}}},
				},
			}},
			source:  "redhat-operators",
			csvName: "rhcl-operator.v1.0.0",
			want:    true,
		},
		{
			name: "stale plan from previous source",
			installPlan: &unstructured.Unstructured{Object: map[string]any{
				"spec": map[string]any{
					"clusterServiceVersionNames": []any{"rhcl-operator.v1.0.0"},
				},
				"status": map[string]any{
					"phase":          "Complete",
					"catalogSources": []any{"redhat-operators", "certified-operators"},
					"plan": []any{map[string]any{"resource": map[string]any{
						"kind": "ClusterServiceVersion", "name": "rhcl-operator.v1.0.0", "sourceName": "certified-operators",
					}}},
				},
			}},
			source:  "redhat-operators",
			csvName: "rhcl-operator.v1.0.0",
			want:    false,
		},
		{
			name: "requested plan is not complete",
			installPlan: &unstructured.Unstructured{Object: map[string]any{
				"spec": map[string]any{
					"clusterServiceVersionNames": []any{"rhcl-operator.v1.0.0"},
				},
				"status": map[string]any{
					"phase": "Installing",
					"plan": []any{map[string]any{"resource": map[string]any{
						"kind": "ClusterServiceVersion", "name": "rhcl-operator.v1.0.0", "sourceName": "redhat-operators",
					}}},
				},
			}},
			source:  "redhat-operators",
			csvName: "rhcl-operator.v1.0.0",
			want:    false,
		},
		{
			name: "requested plan does not contain current CSV",
			installPlan: &unstructured.Unstructured{Object: map[string]any{
				"spec": map[string]any{
					"clusterServiceVersionNames": []any{"rhcl-operator.v1.0.1"},
				},
				"status": map[string]any{
					"phase": "Complete",
					"plan": []any{map[string]any{"resource": map[string]any{
						"kind": "ClusterServiceVersion", "name": "rhcl-operator.v1.0.1", "sourceName": "redhat-operators",
					}}},
				},
			}},
			source:  "redhat-operators",
			csvName: "rhcl-operator.v1.0.0",
			want:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := installPlanMatchesSource(tc.installPlan, tc.source, tc.csvName); got != tc.want {
				t.Errorf("installPlanMatchesSource() = %t, want %t", got, tc.want)
			}
		})
	}
}
