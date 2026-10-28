package controller

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestOperandNetworkPolicyTemplatesContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		template      string
		policyName    string
		selector      map[string]string
		policyTypes   []any
		expectedPorts []networkPolicyPort
	}{
		{
			name:       "alertmanager",
			template:   "resources/data-science-alertmanager-network-policy.tmpl.yaml",
			policyName: "data-science-alertmanager-ingress",
			selector: map[string]string{
				"app.kubernetes.io/part-of":   "data-science-monitoringstack",
				"app.kubernetes.io/component": "alertmanager",
			},
			policyTypes:   []any{"Ingress"},
			expectedPorts: []networkPolicyPort{{protocol: "TCP", port: 9093}, {protocol: "TCP", port: 9094}, {protocol: "UDP", port: 9094}},
		},
		{
			name:       "cluster prometheus proxy",
			template:   "resources/data-science-prometheus-cluster-proxy-network-policy.tmpl.yaml",
			policyName: "data-science-prometheus-cluster-proxy-ingress",
			selector: map[string]string{
				"app": "data-science-prometheus-cluster-proxy",
			},
			policyTypes:   []any{"Ingress"},
			expectedPorts: []networkPolicyPort{{protocol: "TCP", port: 8443}},
		},
		{
			name:       "tempo monolithic",
			template:   "resources/data-science-tempo-monolithic-network-policy.tmpl.yaml",
			policyName: "data-science-tempomonolithic-ingress",
			selector: map[string]string{
				"app.kubernetes.io/name":     "tempo-monolithic",
				"app.kubernetes.io/instance": "data-science-tempomonolithic",
			},
			policyTypes: []any{"Ingress"},
			expectedPorts: []networkPolicyPort{
				{protocol: "TCP", port: 8080},
				{protocol: "TCP", port: 3200},
				{protocol: "TCP", port: 4317},
				{protocol: "TCP", port: 4318},
				{protocol: "TCP", port: 8090},
			},
		},
		{
			name:       "loki stack components",
			template:   "resources/data-science-lokistack-network-policy.tmpl.yaml",
			policyName: "data-science-lokistack-ingress",
			selector: map[string]string{
				"app.kubernetes.io/name":     "lokistack",
				"app.kubernetes.io/instance": "data-science-lokistack",
			},
			policyTypes:   []any{"Ingress"},
			expectedPorts: []networkPolicyPort{{protocol: "TCP", port: 3100}, {protocol: "TCP", port: 7946}, {protocol: "UDP", port: 7946}, {protocol: "TCP", port: 9095}},
		},
		{
			name:       "loki stack gateway",
			template:   "resources/data-science-lokistack-gateway-network-policy.tmpl.yaml",
			policyName: "data-science-lokistack-gateway-ingress",
			selector: map[string]string{
				"app.kubernetes.io/name":      "lokistack",
				"app.kubernetes.io/instance":  "data-science-lokistack",
				"app.kubernetes.io/component": "lokistack-gateway",
			},
			policyTypes:   []any{"Ingress"},
			expectedPorts: []networkPolicyPort{{protocol: "TCP", port: 8080}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resources := renderObservabilityTemplate(t, tt.template, map[string]any{
				"Namespace":              testMonitoringNamespace,
				"LokiStackName":          "data-science-lokistack",
				"UsageLogsCollectorName": "data-science-usage-logs",
			})
			policy := findRenderedResource(t, resources, "NetworkPolicy", tt.policyName)

			selector, found, err := unstructured.NestedStringMap(policy.Object, "spec", "podSelector", "matchLabels")
			if err != nil || !found || !reflect.DeepEqual(selector, tt.selector) {
				t.Fatalf("NetworkPolicy selector mismatch: got=%#v want=%#v error=%v", selector, tt.selector, err)
			}
			policyTypes := nestedSlice(t, policy, "spec", "policyTypes")
			if !reflect.DeepEqual(policyTypes, tt.policyTypes) {
				t.Fatalf("NetworkPolicy policyTypes mismatch: got=%#v want=%#v", policyTypes, tt.policyTypes)
			}
			for _, rawRule := range nestedSlice(t, policy, "spec", "ingress") {
				rule := asMap(t, rawRule)
				peers, ok := rule["from"].([]any)
				if !ok || len(peers) == 0 {
					t.Fatalf("NetworkPolicy ingress rule must declare explicit peers: %#v", rule)
				}
			}

			ports := policyPorts(t, policy)
			expectedPorts := make(map[networkPolicyPort]bool, len(tt.expectedPorts))
			for _, port := range tt.expectedPorts {
				expectedPorts[port] = true
			}
			if !reflect.DeepEqual(ports, expectedPorts) {
				t.Errorf("NetworkPolicy ports mismatch: got %#v, want %#v", ports, expectedPorts)
			}
		})
	}
}

type networkPolicyPort struct {
	protocol string
	port     int64
}

func policyPorts(t *testing.T, policy unstructured.Unstructured) map[networkPolicyPort]bool {
	t.Helper()
	ports := map[networkPolicyPort]bool{}
	ingress := nestedSlice(t, policy, "spec", "ingress")
	for _, rawRule := range ingress {
		rule := asMap(t, rawRule)
		for _, rawPort := range asSlice(t, rule["ports"]) {
			port := asMap(t, rawPort)
			protocol, ok := port["protocol"].(string)
			if !ok {
				t.Fatalf("NetworkPolicy port protocol has unexpected type %T", port["protocol"])
			}
			value, ok := port["port"].(int64)
			if !ok {
				t.Fatalf("NetworkPolicy port has unexpected type %T", port["port"])
			}
			ports[networkPolicyPort{protocol: protocol, port: value}] = true
		}
	}
	return ports
}

func TestLokiGatewayNetworkPolicyAllowsKorrel8r(t *testing.T) {
	t.Parallel()

	resources := renderObservabilityTemplate(t, "resources/data-science-lokistack-gateway-network-policy.tmpl.yaml", map[string]any{
		"Namespace":              testMonitoringNamespace,
		"LokiStackName":          "data-science-lokistack",
		"UsageLogsCollectorName": "data-science-usage-logs",
	})
	policy := findRenderedResource(t, resources, "NetworkPolicy", "data-science-lokistack-gateway-ingress")

	for _, rawRule := range nestedSlice(t, policy, "spec", "ingress") {
		for _, rawPeer := range asSlice(t, asMap(t, rawRule)["from"]) {
			labels, found, err := unstructured.NestedStringMap(asMap(t, rawPeer), "podSelector", "matchLabels")
			if err == nil && found && reflect.DeepEqual(labels, map[string]string{
				"app.kubernetes.io/name":      "korrel8r",
				"app.kubernetes.io/component": "correlation",
				"app.kubernetes.io/part-of":   "monitoring",
			}) {
				return
			}
		}
	}

	t.Fatal("Loki gateway NetworkPolicy must allow Korrel8r on its gateway port")
}

func asSlice(t *testing.T, value any) []any {
	t.Helper()
	items, ok := value.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", value)
	}
	return items
}
