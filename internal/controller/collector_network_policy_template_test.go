package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestCollectorNetworkPolicyTemplateRestrictsIngress(t *testing.T) {
	t.Parallel()

	resources := renderObservabilityTemplate(t, CollectorMonitoringNetworkPolicyTemplate, map[string]any{
		"Namespace": testMonitoringNamespace,
	})
	policy := findRenderedResource(t, resources, "NetworkPolicy", "data-science-collector-monitoring-ingress")

	collector := &networkingv1.NetworkPolicy{}
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(policy.Object, collector))
	require.Equal(t, map[string]string{
		"app.kubernetes.io/component":  "opentelemetry-collector",
		"app.kubernetes.io/instance":   testMonitoringNamespace + ".data-science-collector",
		"app.kubernetes.io/managed-by": "opentelemetry-operator",
		"app.kubernetes.io/part-of":    "opentelemetry",
	}, collector.Spec.PodSelector.MatchLabels)
	require.Equal(t, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}, collector.Spec.PolicyTypes)
	require.Len(t, collector.Spec.Ingress, 2)

	scrapeRule := collector.Spec.Ingress[0]
	require.Equal(t, []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
		"app.kubernetes.io/instance": "data-science-monitoringstack",
		"app.kubernetes.io/name":     "prometheus",
	}}}}, scrapeRule.From)
	require.Equal(t, []networkingv1.NetworkPolicyPort{
		tcpNetworkPolicyPort(8889),
		tcpNetworkPolicyPort(8890),
	}, scrapeRule.Ports)

	otlpRule := collector.Spec.Ingress[1]
	require.Len(t, otlpRule.From, 2)
	require.Equal(t, []networkingv1.NetworkPolicyPort{
		tcpNetworkPolicyPort(4317),
		tcpNetworkPolicyPort(4318),
	}, otlpRule.Ports)

	llmRule := otlpRule.From[0]
	require.Equal(t, []networkingv1.NetworkPolicyPeer{{
		NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
			"monitoring.opendatahub.io/otlp": "true",
		}},
		PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
			"app.kubernetes.io/part-of": "llminferenceservice",
		}, MatchExpressions: []metav1.LabelSelectorRequirement{{
			Key:      "app.kubernetes.io/component",
			Operator: metav1.LabelSelectorOpIn,
			Values: []string{
				"llminferenceservice-workload",
				"llminferenceservice-workload-prefill",
				"llminferenceservice-workload-worker",
				"llminferenceservice-workload-leader",
				"llminferenceservice-workload-worker-prefill",
				"llminferenceservice-workload-leader-prefill",
				"llminferenceservice-router-scheduler",
			},
		}},
		}}}, []networkingv1.NetworkPolicyPeer{llmRule})

	isvcRule := otlpRule.From[1]
	require.Equal(t, &metav1.LabelSelector{MatchLabels: map[string]string{
		"monitoring.opendatahub.io/otlp": "true",
	}}, isvcRule.NamespaceSelector)
	require.Len(t, isvcRule.PodSelector.MatchExpressions, 1)
	require.Equal(t, "serving.kserve.io/inferenceservice", isvcRule.PodSelector.MatchExpressions[0].Key)
	require.Equal(t, metav1.LabelSelectorOpExists, isvcRule.PodSelector.MatchExpressions[0].Operator)

	namespaceSelector, err := metav1.LabelSelectorAsSelector(llmRule.NamespaceSelector)
	require.NoError(t, err)
	for _, test := range []struct {
		name   string
		labels map[string]string
		allow  bool
	}{
		{name: "unenrolled namespace"},
		{name: "enrolled namespace", labels: map[string]string{"monitoring.opendatahub.io/otlp": "true"}, allow: true},
		{name: "disabled namespace", labels: map[string]string{"monitoring.opendatahub.io/otlp": "false"}},
		{name: "pod labels on namespace", labels: map[string]string{"app.kubernetes.io/part-of": "llminferenceservice"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.allow, namespaceSelector.Matches(labels.Set(test.labels)))
		})
	}

	selector, err := metav1.LabelSelectorAsSelector(llmRule.PodSelector)
	require.NoError(t, err)
	for _, component := range []string{
		"llminferenceservice-workload",
		"llminferenceservice-workload-prefill",
		"llminferenceservice-workload-worker",
		"llminferenceservice-workload-leader",
		"llminferenceservice-workload-worker-prefill",
		"llminferenceservice-workload-leader-prefill",
		"llminferenceservice-router-scheduler",
	} {
		t.Run(component, func(t *testing.T) {
			t.Parallel()
			require.True(t, selector.Matches(labels.Set{
				"app.kubernetes.io/component": component,
				"app.kubernetes.io/part-of":   "llminferenceservice",
			}))
		})
	}
	require.False(t, selector.Matches(labels.Set{
		"app.kubernetes.io/component": "llminferenceservice-router",
		"app.kubernetes.io/part-of":   "llminferenceservice",
	}))
	require.False(t, selector.Matches(labels.Set{
		"app.kubernetes.io/component": "llminferenceservice-workload",
	}))
	isvcSelector, err := metav1.LabelSelectorAsSelector(isvcRule.PodSelector)
	require.NoError(t, err)
	require.True(t, isvcSelector.Matches(labels.Set{"serving.kserve.io/inferenceservice": "demo"}))
}

func TestCollectorTargetAllocatorNetworkPolicyTemplateRestrictsIngressAndAPIConfig(t *testing.T) {
	t.Parallel()

	resources := renderObservabilityTemplate(t, CollectorTargetAllocatorNetworkPolicyTemplate, map[string]any{
		"Namespace":              testMonitoringNamespace,
		"Korrel8rAPIServerCIDRs": []string{"172.20.0.1/32", "fd00::1/128"},
	})
	policy := findRenderedResource(t, resources, "NetworkPolicy", "data-science-collector-targetallocator-ingress")

	targetAllocator := &networkingv1.NetworkPolicy{}
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(policy.Object, targetAllocator))
	require.Equal(t, map[string]string{
		"app.kubernetes.io/component":  "opentelemetry-targetallocator",
		"app.kubernetes.io/instance":   testMonitoringNamespace + ".data-science-collector",
		"app.kubernetes.io/managed-by": "opentelemetry-operator",
		"app.kubernetes.io/part-of":    "opentelemetry",
	}, targetAllocator.Spec.PodSelector.MatchLabels)
	require.Equal(t, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress}, targetAllocator.Spec.PolicyTypes)
	require.Len(t, targetAllocator.Spec.Ingress, 1)
	require.Equal(t, []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
		"app.kubernetes.io/component":  "opentelemetry-collector",
		"app.kubernetes.io/instance":   testMonitoringNamespace + ".data-science-collector",
		"app.kubernetes.io/managed-by": "opentelemetry-operator",
		"app.kubernetes.io/part-of":    "opentelemetry",
	}}}}, targetAllocator.Spec.Ingress[0].From)
	require.Equal(t, []networkingv1.NetworkPolicyPort{tcpNetworkPolicyPort(8080), tcpNetworkPolicyPort(8443)}, targetAllocator.Spec.Ingress[0].Ports)
	require.Len(t, targetAllocator.Spec.Egress, 1)
	require.Equal(t, []networkingv1.NetworkPolicyPeer{
		{IPBlock: &networkingv1.IPBlock{CIDR: "172.20.0.1/32"}},
		{IPBlock: &networkingv1.IPBlock{CIDR: "fd00::1/128"}},
	}, targetAllocator.Spec.Egress[0].To)
	require.Equal(t, []networkingv1.NetworkPolicyPort{tcpNetworkPolicyPort(6443)}, targetAllocator.Spec.Egress[0].Ports)
}

func TestCollectorTargetAllocatorNetworkPolicyTemplateDeniesEgressWithoutAPIConfig(t *testing.T) {
	t.Parallel()

	resources := renderObservabilityTemplate(t, CollectorTargetAllocatorNetworkPolicyTemplate, map[string]any{
		"Namespace":              testMonitoringNamespace,
		"Korrel8rAPIServerCIDRs": []string{},
	})
	policy := findRenderedResource(t, resources, "NetworkPolicy", "data-science-collector-targetallocator-ingress")

	targetAllocator := &networkingv1.NetworkPolicy{}
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(policy.Object, targetAllocator))
	require.Empty(t, targetAllocator.Spec.Egress)
}

func tcpNetworkPolicyPort(port int32) networkingv1.NetworkPolicyPort {
	protocol := corev1.ProtocolTCP
	portValue := intstr.FromInt(int(port))
	return networkingv1.NetworkPolicyPort{Protocol: &protocol, Port: &portValue}
}
