package controller

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
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
		{
			// The usage logs gateway, guarded by the usage logs LokiStack deployment
			// alongside the stack it fronts. Its only peer is the collector, so unlike the
			// general-logs gateway it admits no Korrel8r and no Prometheus.
			name:       "usage logs loki stack gateway",
			template:   "resources/usage-logs-lokistack-network-policy.tmpl.yaml",
			policyName: "data-science-usage-logs-lokistack-allow",
			selector: map[string]string{
				"app.kubernetes.io/component": "lokistack-gateway",
				"app.kubernetes.io/instance":  "data-science-usage-logs-lokistack",
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
				"UsageLogsLokiStackName": "data-science-usage-logs-lokistack",
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

// TestLokiGatewayNetworkPolicyAdmitsOnlyDeclaredPeers pins the whole peer set
// of the log forwarding gateway policy. Ingress is the write path into Loki, so
// a rule added casually is a tenant anyone can write to: the contract is the
// exact three peers, each a single peer on the gateway port — Vector from CLO's
// dedicated namespace, Korrel8r for log correlation, and the in-namespace
// Prometheus for metrics scraping.
func TestLokiGatewayNetworkPolicyAdmitsOnlyDeclaredPeers(t *testing.T) {
	t.Parallel()

	resources := renderObservabilityTemplate(t, "resources/data-science-lokistack-gateway-network-policy.tmpl.yaml", map[string]any{
		"Namespace":              testMonitoringNamespace,
		"LokiStackName":          "data-science-lokistack",
		"UsageLogsCollectorName": "data-science-usage-logs",
	})
	rendered := findRenderedResource(t, resources, "NetworkPolicy", "data-science-lokistack-gateway-ingress")

	var policy networkingv1.NetworkPolicy
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(rendered.Object, &policy); err != nil {
		t.Fatalf("converting Loki gateway NetworkPolicy: %v", err)
	}

	wantPeers := map[string]bool{}
	see := func(key string) {
		if wantPeers[key] {
			t.Errorf("peer %s is declared more than once", key)
		}
		wantPeers[key] = true
	}
	for _, rule := range policy.Spec.Ingress {
		if len(rule.From) != 1 {
			t.Fatalf("every ingress rule must declare exactly one peer, got %#v", rule.From)
		}
		if len(rule.Ports) != 1 || rule.Ports[0].Protocol == nil || *rule.Ports[0].Protocol != corev1.ProtocolTCP ||
			rule.Ports[0].Port == nil || rule.Ports[0].Port.IntValue() != 8080 {
			t.Fatalf("every ingress rule must admit only the gateway port TCP/8080, got %#v", rule.Ports)
		}
		peer := rule.From[0]
		switch {
		case peer.NamespaceSelector != nil:
			if got := peer.NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"]; got != "openshift-logging" {
				t.Errorf("namespace peer must be CLO's openshift-logging, got %q", got)
			} else {
				see("openshift-logging")
			}
		case peer.PodSelector != nil:
			switch peer.PodSelector.MatchLabels["app.kubernetes.io/name"] {
			case "korrel8r":
				if peer.PodSelector.MatchLabels["app.kubernetes.io/component"] != "correlation" ||
					peer.PodSelector.MatchLabels["app.kubernetes.io/part-of"] != "monitoring" {
					t.Errorf("korrel8r peer must stay scoped to the correlation component, got %#v", peer.PodSelector)
				}
				see("korrel8r")
			case "prometheus":
				if peer.PodSelector.MatchLabels["app.kubernetes.io/instance"] != "data-science-monitoringstack" {
					t.Errorf("prometheus peer must stay scoped to the monitoring stack instance, got %#v", peer.PodSelector)
				}
				see("prometheus")
			default:
				t.Errorf("undeclared pod peer on the gateway: %#v", peer.PodSelector)
			}
		default:
			t.Fatalf("peer selects neither a namespace nor a pod: %#v", peer)
		}
	}
	for _, peer := range []string{"openshift-logging", "korrel8r", "prometheus"} {
		if !wantPeers[peer] {
			t.Errorf("gateway policy is missing its %s peer", peer)
		}
	}
}

func asSlice(t *testing.T, value any) []any {
	t.Helper()
	items, ok := value.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", value)
	}
	return items
}
