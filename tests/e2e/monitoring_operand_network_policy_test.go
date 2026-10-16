package e2e_test

import (
	"testing"

	gTypes "github.com/onsi/gomega/types"
	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	"k8s.io/apimachinery/pkg/types"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega matchers
)

// ValidateOperandNetworkPolicies verifies the metric operand policies on a live
// cluster, including the managed-resource watch path used after deletion.
func (tc *MonitoringTestCtx) ValidateOperandNetworkPolicies(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	thanosEgress := `[.spec.egress[] |
  select(.to[0].podSelector.matchLabels."app.kubernetes.io/name" == "prometheus") |
  select(.ports[0].protocol == "TCP" and .ports[0].port == 10901)] | length == 1`
	thanosDNSEgress := `[.spec.egress[] |
  select(.to[0].namespaceSelector.matchLabels."kubernetes.io/metadata.name" == "openshift-dns") |
  select([.ports[] | select(.protocol == "TCP" and .port == 5353)] | length == 1) |
  select([.ports[] | select(.protocol == "UDP" and .port == 5353)] | length == 1)] | length == 1`
	policies := []struct {
		name      string
		condition gTypes.GomegaMatcher
	}{
		{
			name: "data-science-alertmanager-ingress",
			condition: And(
				jq.Match(`.metadata.labels."platform.opendatahub.io/part-of" == "monitoring"`),
				jq.Match(`.spec.podSelector.matchLabels."app.kubernetes.io/component" == "alertmanager"`),
				jq.Match(`.spec.podSelector.matchLabels."app.kubernetes.io/part-of" == "data-science-monitoringstack"`),
				jq.Match(`.spec.policyTypes == ["Ingress"]`),
				jq.Match(`[.spec.ingress[] | select((.from // []) | length == 0)] | length == 0`),
				jq.Match(`[.spec.ingress[].ports[].port] | unique | sort == [9093, 9094]`),
			),
		},
		{
			name: "data-science-prometheus-cluster-proxy-ingress",
			condition: And(
				jq.Match(`.spec.podSelector.matchLabels.app == "data-science-prometheus-cluster-proxy"`),
				jq.Match(`.spec.policyTypes == ["Ingress"]`),
				jq.Match(`.spec.ingress[0].from[0].namespaceSelector.matchLabels."policy-group.network.openshift.io/ingress" == ""`),
				jq.Match(`.spec.ingress[0].ports[0].protocol == "TCP" and .spec.ingress[0].ports[0].port == 8443`),
			),
		},
		{
			name: "data-science-thanos-querier-ingress",
			condition: And(
				jq.Match(`.spec.policyTypes == ["Ingress", "Egress"]`),
				jq.Match(`[.spec.ingress[] | select((.from // []) | length == 0)] | length == 0`),
				jq.Match("%s", thanosEgress),
				jq.Match("%s", thanosDNSEgress),
			),
		},
	}

	for _, policy := range policies {
		t.Run(policy.name, func(t *testing.T) {
			tc := tc.WithT(t)
			nn := types.NamespacedName{Name: policy.name, Namespace: tc.MonitoringNamespace}
			tc.EnsureResourceExists(
				WithMinimalObject(gvk.NetworkPolicy, nn),
				WithCondition(policy.condition),
				WithCustomErrorMsg("NetworkPolicy %s should satisfy its operand contract", policy.name),
			)

			// The NetworkPolicy watch must cause a deleted managed policy to be
			// rendered again without changing Monitoring configuration.
			tc.DeleteResource(
				WithMinimalObject(gvk.NetworkPolicy, nn),
				WithWaitForDeletion(true),
			)
			tc.EnsureResourceExists(
				WithMinimalObject(gvk.NetworkPolicy, nn),
				WithCondition(policy.condition),
				WithCustomErrorMsg("NetworkPolicy %s should be recreated after deletion", policy.name),
			)
		})
	}
}
