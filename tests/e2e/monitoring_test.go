package e2e_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega"
)

func monitoringTestSuite(t *testing.T) {
	t.Helper()

	tc, err := NewTestContext(t)
	require.NoError(t, err)

	expectedReplicas := detectExpectedReplicas(t, tc)

	monitoringServiceCtx := MonitoringTestCtx{
		TestContext:             tc,
		expectedDefaultReplicas: expectedReplicas,
	}

	tc.DefaultResourceOpts = []ResourceOpts{
		WithEventuallyTimeout(5 * time.Minute),
		WithEventuallyPollingInterval(2 * time.Second),
	}

	monitoringServiceCtx.runBaseConfigurationTests(t)
	monitoringServiceCtx.runMetricsAndMonitoringStackTests(t)
}

// ========================================================================
// Group 1: Base Configuration
// ========================================================================

func (tc *MonitoringTestCtx) runBaseConfigurationTests(t *testing.T) {
	t.Helper()

	t.Run("Group 1: Base Configuration", func(t *testing.T) {
		tc.setupBaseMonitoring(t)

		t.Cleanup(func() {
			tc.cleanupGroup(t, "")
		})

		t.Run("Test Traces default content", tc.ValidateMonitoringCRDefaultTracesContent)
	})
}

// ValidateMonitoringCRDefaultTracesContent verifies that traces stanza is omitted by default.
func (tc *MonitoringTestCtx) ValidateMonitoringCRDefaultTracesContent(t *testing.T) {
	t.Helper()

	tc.updateMonitoringConfig(withManagementState(common.Managed))

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`.spec.traces == null`),
		)),
		WithCustomErrorMsg("Expected traces stanza to be omitted by default"),
	)
}

// ========================================================================
// Group 2: Metrics & MonitoringStack
// ========================================================================

func (tc *MonitoringTestCtx) runMetricsAndMonitoringStackTests(t *testing.T) {
	t.Helper()

	t.Run("Group 2: Metrics & MonitoringStack", func(t *testing.T) {
		tc.setupMetrics(t)

		t.Cleanup(func() {
			tc.cleanupGroup(t, "")
		})

		t.Run("Test Metrics MonitoringStack CR Creation", tc.ValidateMonitoringStackCRMetricsWhenSet)
		t.Run("Test Metrics MonitoringStack CR Configuration", tc.ValidateMonitoringStackCRMetricsConfiguration)
		t.Run("Test Metrics Replicas Configuration", tc.ValidateMonitoringStackCRMetricsReplicasUpdate)
		t.Run("Test Prometheus rules lifecycle", tc.ValidatePrometheusRulesLifecycle)
		t.Run("Test Prometheus Self ServiceMonitor TLS Fix", tc.ValidatePrometheusSelfServiceMonitorTLSFix)
		t.Run("Test ownerReference and resourceVersion stability", tc.ValidateReconciliationStability)
	})
}

// ValidateMonitoringStackCRMetricsWhenSet validates that MonitoringStack CR is created when metrics are set.
func (tc *MonitoringTestCtx) ValidateMonitoringStackCRMetricsWhenSet(t *testing.T) {
	t.Helper()

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(jq.Match(`.spec.metrics != null`)),
		WithCustomErrorMsg("Monitoring resource should be updated with metrics configuration"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.MonitoringStack, types.NamespacedName{Name: MonitoringStackName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.status.conditions[] | select(.type == "Available") | .status == "%s"`, metav1.ConditionTrue)),
	)
}

// ValidateMonitoringStackCRMetricsConfiguration verifies MonitoringStack CR storage, retention, resources, and replicas.
func (tc *MonitoringTestCtx) ValidateMonitoringStackCRMetricsConfiguration(t *testing.T) {
	t.Helper()

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.MonitoringStack, types.NamespacedName{Name: MonitoringStackName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.spec.prometheusConfig.persistentVolumeClaim.resources.requests.storage == "%s"`, MetricsStorageSize),
			jq.Match(`.spec.retention == "%s"`, MetricsRetention),
			jq.Match(`.spec.resources.requests.cpu == "%s"`, MetricsCPURequest),
			jq.Match(`.spec.resources.requests.memory == "%s"`, MetricsMemoryRequest),
			jq.Match(`.spec.prometheusConfig.replicas == %d`, tc.expectedDefaultReplicas),
			monitoringOwnerReferencesCondition,
		)),
		WithCustomErrorMsg("MonitoringStack '%s' configuration validation failed", MonitoringStackName),
	)
}

// ValidateMonitoringStackCRMetricsReplicasUpdate tests that replicas are updated when metrics replicas change.
func (tc *MonitoringTestCtx) ValidateMonitoringStackCRMetricsReplicasUpdate(t *testing.T) {
	t.Helper()

	replicasTransforms := []jq.TransformFn{
		jq.Transform(`.spec.metrics.storage.size = "%s"`, MetricsStorageSize),
		jq.Transform(`.spec.metrics.storage.retention = "%s"`, MetricsRetention),
		withMetricsReplicas(1),
	}
	tc.updateMonitoringConfig(replicasTransforms...)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.MonitoringStack, types.NamespacedName{Name: MonitoringStackName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.spec.prometheusConfig.persistentVolumeClaim.resources.requests.storage == "%s"`, MetricsStorageSize),
			jq.Match(`.spec.prometheusConfig.replicas == %d`, 1),
		)),
		WithCustomErrorMsg("MonitoringStack '%s' configuration validation failed", MonitoringStackName),
	)
}

// ValidatePrometheusRulesLifecycle validates that Prometheus rules are created and deleted
// based on monitoring and alerting configuration.
func (tc *MonitoringTestCtx) ValidatePrometheusRulesLifecycle(t *testing.T) {
	t.Helper()

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
		withEmptyAlerting(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.PrometheusRule, types.NamespacedName{Name: "operator-prometheusrules", Namespace: tc.MonitoringNamespace}),
	)

	tc.resetMonitoringConfigToRemoved()

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.PrometheusRule, types.NamespacedName{Name: "operator-prometheusrules", Namespace: tc.MonitoringNamespace}),
	)

	tc.updateMonitoringConfig(withNoAlerting())
}

// ValidatePrometheusSelfServiceMonitorTLSFix tests the prometheus-self-fixed ServiceMonitor TLS configuration.
func (tc *MonitoringTestCtx) ValidatePrometheusSelfServiceMonitorTLSFix(t *testing.T) {
	t.Helper()
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.spec.metrics != null`),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionMonitoringStackAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("Monitoring resource should be ready with MonitoringStack available"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ServiceMonitor, types.NamespacedName{Name: "prometheus-self-fixed", Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.spec.endpoints[0].tlsConfig.serverName == "prometheus-operated.%s.svc"`, tc.MonitoringNamespace),
			jq.Match(`.spec.selector.matchLabels."app.kubernetes.io/name" == "data-science-monitoringstack-prometheus"`),
			jq.Match(`.spec.endpoints[0].scheme == "https"`),
			jq.Match(`.spec.endpoints[0].tlsConfig.ca.configMap.name == "prometheus-web-tls-ca"`),
			jq.Match(`.metadata.labels."platform.opendatahub.io/part-of" == "monitoring"`),
			monitoringOwnerReferencesCondition,
		)),
		WithCustomErrorMsg("prometheus-self-fixed ServiceMonitor should be created with correct TLS configuration"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ConfigMap, types.NamespacedName{Name: "prometheus-web-tls-ca", Namespace: tc.MonitoringNamespace}),
		WithCondition(
			jq.Match(`.metadata.annotations."service.beta.openshift.io/inject-cabundle" == "true"`),
		),
		WithCustomErrorMsg("prometheus-web-tls-ca ConfigMap should exist with CA injection annotation"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Service, types.NamespacedName{Name: "prometheus-operated", Namespace: tc.MonitoringNamespace}),
		WithCondition(
			jq.Match(`.metadata.annotations."service.beta.openshift.io/serving-cert-secret-name" == "prometheus-operated-tls"`),
		),
		WithCustomErrorMsg("prometheus-operated Service should have serving-cert-secret-name annotation for TLS"),
	)
}

// ValidateReconciliationStability checks that ownerReferences and resourceVersions remain stable
// across multiple reconcile loops, detecting potential reconciliation loops.
func (tc *MonitoringTestCtx) ValidateReconciliationStability(t *testing.T) {
	t.Helper()

	g := NewWithT(t)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.spec.metrics != null`),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionMonitoringStackAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("Monitoring resource should be ready before checking reconciliation stability"),
	)

	ownerRefStableCondition := And(
		monitoringOwnerReferencesCondition,
		jq.Match(`.metadata.ownerReferences[0].controller == true`),
	)

	type resource struct {
		gvk             schema.GroupVersionKind
		name            string
		ns              string
		desc            string
		ownerRef        bool
		resourceVersion bool
	}

	resources := []resource{
		{gvk.MonitoringStack, MonitoringStackName, tc.MonitoringNamespace, "MonitoringStack", true, false},
		{gvk.ClusterRoleBinding, "data-science-monitoringstack-alertmanager-prometheus-metrics-reader", "", "alertmanager ClusterRoleBinding", true, true},
		{gvk.ClusterRoleBinding, "generate-processors-collector-rolebinding", "", "collector ClusterRoleBinding", false, true},
		{gvk.Service, "data-science-collector-prometheus", tc.MonitoringNamespace, "collector prometheus Service", true, true},
		{gvk.ConfigMap, "prometheus-web-tls-ca", tc.MonitoringNamespace, "prometheus TLS CA ConfigMap", true, true},
	}

	initialVersions := make(map[string]string, len(resources))
	for _, r := range resources {
		opts := []ResourceOpts{
			WithMinimalObject(r.gvk, types.NamespacedName{Name: r.name, Namespace: r.ns}),
			WithCustomErrorMsg("%s should exist before stability check", r.desc),
		}
		if r.ownerRef {
			opts = append(opts, WithCondition(ownerRefStableCondition))
		}

		u := tc.EnsureResourceExists(opts...)
		if r.resourceVersion {
			initialVersions[r.name] = u.GetResourceVersion()
		}
	}

	g.Consistently(func(g Gomega) {
		for _, r := range resources {
			u := tc.FetchResource(
				WithMinimalObject(r.gvk, types.NamespacedName{Name: r.name, Namespace: r.ns}),
			)
			if !g.Expect(u).NotTo(BeNil(), "%s should still exist", r.desc) {
				continue
			}

			if r.ownerRef {
				g.Expect(u).To(ownerRefStableCondition,
					"%s ownerReferences should remain stable", r.desc,
				)
			}

			if r.resourceVersion {
				g.Expect(u.GetResourceVersion()).To(
					Equal(initialVersions[r.name]),
					"%s resourceVersion changed from %s — possible reconciliation loop",
					r.desc, initialVersions[r.name],
				)
			}
		}
	}).WithTimeout(1 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
}
