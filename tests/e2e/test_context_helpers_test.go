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
