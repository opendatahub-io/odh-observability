package e2e_test

import (
	"testing"
	"time"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega matchers
)

// Log forwarding gets its own LokiStack, separate from the usage logs one. The
// validations below mirror the usage logs LokiStack coverage in
// monitoring_usage_logs_test.go against spec.logs instead of spec.usageLogs,
// and additionally assert that neither stack disturbs the other.

// lokiStackReadyTimeout bounds LokiStack provisioning: the Loki operator has to
// schedule the full read/write/gateway set and have it reach Ready.
const lokiStackReadyTimeout = 20 * time.Minute

// setupLogsLokiStack configures log forwarding storage and waits for its
// LokiStack to come up. The usage logs stack stays configured throughout, so
// this also establishes that the two coexist.
func (tc *MonitoringTestCtx) setupLogsLokiStack(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(func() {
		if t.Failed() {
			tc.logLokiStackDiagnostics(t, LokiStackName, UsageLogsLokiStackName)
		}
	})

	tc.setupLogsWithStorage(t, "s3", logsLokiSecretName)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionLokiStackAvailable, metav1.ConditionTrue),
			// The usage logs stack must be untouched by configuring log forwarding.
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsLokiStackAvailable, metav1.ConditionTrue),
		)),
		WithEventuallyTimeout(lokiStackReadyTimeout),
		WithCustomErrorMsg("Both LokiStacks should be available once log forwarding storage is configured"),
	)
}

// Log the state before cleanup removes the stacks and their pods.
func (tc *MonitoringTestCtx) logLokiStackDiagnostics(t *testing.T, names ...string) {
	t.Helper()
	for _, name := range names {
		stack, err := tc.fetchResource(t, gvk.LokiStack, types.NamespacedName{Name: name, Namespace: tc.MonitoringNamespace})
		if err != nil {
			t.Logf("LokiStack %s: %v", name, err)
		} else {
			status, _, _ := unstructured.NestedMap(stack.Object, "status")
			t.Logf("LokiStack %s status: %v", name, status)
		}
		pods := &corev1.PodList{}
		if err := tc.Client().List(tc.Context(), pods, client.InNamespace(tc.MonitoringNamespace),
			client.MatchingLabels{"app.kubernetes.io/instance": name}); err != nil {
			t.Logf("LokiStack %s pods: %v", name, err)
			continue
		}
		for _, pod := range pods.Items {
			t.Logf("LokiStack %s pod %s: phase=%s conditions=%v containers=%v",
				name, pod.Name, pod.Status.Phase, pod.Status.Conditions, pod.Status.ContainerStatuses)
		}
	}
}

// ValidateLogsLokiStackDeployment tests that the log forwarding LokiStack is
// deployed from spec.logs.storage with correct configuration.
func (tc *MonitoringTestCtx) ValidateLogsLokiStackDeployment(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      LokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			tc.monitoringOwnerReferencesCondition(),
			jq.Match(`.spec.size == "1x.demo"`),
			jq.Match(`.spec.storage.secret.name == "%s"`, logsLokiSecretName),
			jq.Match(`.spec.storage.secret.type == "s3"`),
			jq.Match(`.spec.storage.secret.credentialMode == "static"`),
			jq.Match(`.spec.storageClassName == "%s"`, tc.DefaultStorageClass),
			jq.Match(`.spec.tenants.mode == "openshift-logging"`),
		)),
		WithCustomErrorMsg("Log forwarding LokiStack should be created with the storage configured under spec.logs"),
	)

	// The two stacks are separate CRs with separate storage: neither secret may
	// leak into the other stack.
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      UsageLogsLokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`.spec.storage.secret.name == "%s"`, usageLogsLokiSecretName)),
		WithCustomErrorMsg("Usage logs LokiStack must keep its own storage secret when log forwarding is configured"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionLokiStackAvailable, metav1.ConditionTrue),
		),
		WithCustomErrorMsg("LokiStackAvailable condition should be True when the log forwarding LokiStack is deployed"),
	)

	// These two policies ride with the log forwarding stack, so they are only
	// rendered while spec.logs is set. They are asserted here rather than
	// alongside the usage logs policies because nothing in a usage-logs-only
	// install creates them. Their configuration — selectors, peers, ports,
	// policy types — is a template contract pinned in unit tests
	// (TestOperandNetworkPolicyTemplatesContract and
	// TestLokiGatewayNetworkPolicyAdmitsOnlyDeclaredPeers in
	// internal/controller); e2e keeps what only a live cluster can show,
	// existence and Monitoring ownership.
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
			Name:      LokiStackNetworkPolicyName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(tc.monitoringOwnerReferencesCondition()),
		WithCustomErrorMsg("LokiStack NetworkPolicy should exist and be owned by Monitoring while spec.logs is set"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
			Name:      LokiStackGatewayNetworkPolicyName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(tc.monitoringOwnerReferencesCondition()),
		WithCustomErrorMsg("LokiStack gateway NetworkPolicy should exist and be owned by Monitoring while spec.logs is set"),
	)
}

// ValidateLogsLokiStackConfiguration tests the log forwarding LokiStack OTLP
// stream labels configuration.
func (tc *MonitoringTestCtx) ValidateLogsLokiStackConfiguration(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      LokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes | length == 8`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "kubernetes_namespace_name")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "model")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "subscription")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "response_type")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "gateway_namespace_name")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "gateway_deployment_name")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "upstream_namespace_name")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "upstream_deployment_name")] | length == 1`),
		)),
		WithCustomErrorMsg("Log forwarding LokiStack should have correct OTLP stream labels"),
	)
}

// ValidateLogsLokiStackRemoved tests that removing spec.logs tears down only the
// log forwarding LokiStack. The usage logs stack is driven by spec.usageLogs and
// must survive.
func (tc *MonitoringTestCtx) ValidateLogsLokiStackRemoved(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	// Idempotent: the ClusterLogForwarder contract test removes spec.logs on its
	// way out, but it is skipped when the Cluster Logging operator is absent.
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		jq.Transform(`del(.spec.logs)`),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      LokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithEventuallyTimeout(lokiStackReadyTimeout),
	)

	// The stack's policies are rendered by the same action, so they go with it.
	// The usage logs policies are driven by spec.usageLogs and must not.
	for _, name := range []string{LokiStackNetworkPolicyName, LokiStackGatewayNetworkPolicyName} {
		tc.EnsureResourceGone(
			WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
				Name:      name,
				Namespace: tc.MonitoringNamespace,
			}),
		)
	}

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      UsageLogsLokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCustomErrorMsg("Usage logs LokiStack must survive the removal of spec.logs"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
			Name:      UsageLogsLokiStackNetworkPolicyName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCustomErrorMsg("Usage logs LokiStack NetworkPolicy must survive the removal of spec.logs"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionLokiStackAvailable, metav1.ConditionFalse),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsLokiStackAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("Removing spec.logs should only flip LokiStackAvailable to False"),
	)
}
