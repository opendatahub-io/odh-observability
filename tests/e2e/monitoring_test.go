package e2e_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	gTypes "github.com/onsi/gomega/types"
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

	monitoringServiceCtx.ensureMonitoringCRExists(t)
	monitoringServiceCtx.runBaseConfigurationTests(t)
	monitoringServiceCtx.runMetricsAndMonitoringStackTests(t)
	monitoringServiceCtx.runCollectorTests(t)
	monitoringServiceCtx.runTargetAllocatorTests(t)
	monitoringServiceCtx.runThanosQuerierTests(t)
	monitoringServiceCtx.runTracesWithPVBackendTests(t)
	monitoringServiceCtx.runTracesWithCloudStorageTests(t)
	monitoringServiceCtx.runPersesTests(t)
	monitoringServiceCtx.runNetworkingTests(t)
	monitoringServiceCtx.runWebhookTests(t)
	monitoringServiceCtx.runNegativeConditionTests(t)
	monitoringServiceCtx.runDisabledTests(t)
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

// ========================================================================
// Group 3: OpenTelemetry Collector
// ========================================================================

func (tc *MonitoringTestCtx) runCollectorTests(t *testing.T) {
	t.Helper()

	t.Run("Group 3: OpenTelemetry Collector", func(t *testing.T) {
		tc.setupMetrics(t)

		t.Cleanup(func() {
			tc.cleanupGroup(t, "")
		})

		t.Run("Test OpenTelemetry Collector Configurations", tc.ValidateOpenTelemetryCollectorConfigurations)
		t.Run("Test OpenTelemetry Collector replicas", tc.ValidateMonitoringCRCollectorReplicas)
		t.Run("Test Metrics TLS is always enabled for Prometheus exporter", tc.ValidateMetricsTLSAlwaysEnabled)
	})
}

// ValidateOpenTelemetryCollectorConfigurations consolidates all OpenTelemetry Collector configuration tests.
func (tc *MonitoringTestCtx) ValidateOpenTelemetryCollectorConfigurations(t *testing.T) {
	t.Helper()

	testCases := []struct {
		name                string
		transforms          []jq.TransformFn
		monitoringCondition gTypes.GomegaMatcher
		validation          gTypes.GomegaMatcher
	}{
		{
			name: "Basic Traces Configuration",
			transforms: []jq.TransformFn{
				withManagementState(common.Managed),
				withMonitoringTraces(TracesStorageBackendPV, "", "", DefaultRetention),
			},
			monitoringCondition: jq.Match(`.spec.traces != null`),
			validation:          jq.Match(`.spec.config.service.pipelines | has("traces")`),
		},
		{
			name: "Trace Ingestion always uses TLS via gateway",
			transforms: []jq.TransformFn{
				withManagementState(common.Managed),
				withMonitoringTraces(TracesStorageBackendPV, "", "", DefaultRetention),
			},
			monitoringCondition: jq.Match(`.spec.traces != null`),
			validation: jq.Match(`
				(.spec.config.exporters."otlp/tempo".tls.ca_file == "/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt") and
				(.spec.config.exporters."otlp/tempo".auth.authenticator == "bearertokenauth")
			`),
		},
		{
			name: "Custom Metrics Exporters",
			transforms: []jq.TransformFn{
				withManagementState(common.Managed),
				tc.withMetricsConfig(),
				withCustomMetricsExporters(),
			},
			monitoringCondition: jq.Match(`.spec.metrics != null`),
			validation: jq.Match(`
				(.spec.config.exporters | has("prometheus") and has("debug") and has("%s")) and
				(.spec.config.service.pipelines.metrics.exporters | length == 3 and contains(["prometheus", "debug", "%s"]))
			`, OtlpCustomExporter, OtlpCustomExporter),
		},
		{
			name: "Custom Traces Exporters",
			transforms: []jq.TransformFn{
				withManagementState(common.Managed),
				withMonitoringTraces(TracesStorageBackendPV, "", "", ""),
				withCustomTracesExporters(),
			},
			monitoringCondition: jq.Match(`.spec.traces != null`),
			validation: jq.Match(`
				(.spec.config.exporters | has("debug") and has("%s") and has("%s")) and
				(.spec.config.service.pipelines.traces.exporters | contains(["debug", "%s", "%s"]))
			`, OtlpHttpCustomExporter, OtlpTempoExporter, OtlpHttpCustomExporter, OtlpTempoExporter),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Helper()
			t.Cleanup(tc.resetMonitoringConfigToManaged)

			tc.updateMonitoringConfig(testCase.transforms...)

			monitoringReadyCondition := And(
				jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
				testCase.monitoringCondition,
			)

			tc.EnsureResourceExists(
				WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
				WithCondition(monitoringReadyCondition),
				WithCustomErrorMsg("Monitoring service should be ready with expected configuration before validating OpenTelemetry Collector"),
			)

			tc.ensureOpenTelemetryCollectorReady(t)

			tc.EnsureResourceExists(
				WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
					Name:      OpenTelemetryCollectorName,
					Namespace: tc.MonitoringNamespace,
				}),
				WithCondition(testCase.validation),
			)
		})
	}
}

// ValidateMonitoringCRCollectorReplicas tests that collectorReplicas is respected.
func (tc *MonitoringTestCtx) ValidateMonitoringCRCollectorReplicas(t *testing.T) {
	t.Helper()

	defaultReplicas := tc.expectedDefaultReplicas
	testReplicas := defaultReplicas + 1

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	monitoringCR := WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName})

	tc.EnsureResourceExists(
		monitoringCR,
		WithCondition(jq.Match(`.spec.collectorReplicas == %d`, defaultReplicas)),
		WithCustomErrorMsg("CollectorReplicas should be set to the default value of %d", defaultReplicas),
	)

	tc.updateMonitoringConfig(jq.Transform(`.spec.collectorReplicas = %d`, testReplicas))

	tc.EnsureResourceExists(
		monitoringCR,
		WithCondition(jq.Match(`.spec.collectorReplicas == %d`, testReplicas)),
		WithCustomErrorMsg("CollectorReplicas should be updated to %d", testReplicas),
	)
}

// ValidateMetricsTLSAlwaysEnabled validates that TLS is always enabled for the OpenTelemetry Collector Prometheus exporter.
func (tc *MonitoringTestCtx) ValidateMetricsTLSAlwaysEnabled(t *testing.T) {
	t.Helper()

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.ensureOpenTelemetryCollectorReady(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Service, types.NamespacedName{
			Name:      "data-science-collector-prometheus",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.metadata.annotations."service.beta.openshift.io/serving-cert-secret-name" == "data-science-collector-tls"`),
			jq.Match(`.spec.ports[0].name == "prometheus"`),
			jq.Match(`.spec.ports[0].port == 8889`),
		)),
		WithCustomErrorMsg("TLS Service for Prometheus exporter should exist with service-ca annotation"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Secret, types.NamespacedName{
			Name:      "data-science-collector-tls",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.type == "kubernetes.io/tls"`),
			jq.Match(`.data."tls.crt" != null`),
			jq.Match(`.data."tls.key" != null`),
		)),
		WithCustomErrorMsg("TLS Secret should be created by service-ca with certificate and key"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      OpenTelemetryCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.config.exporters.prometheus.tls.cert_file == "/etc/otel-collector/tls/tls.crt"`),
			jq.Match(`.spec.config.exporters.prometheus.tls.key_file == "/etc/otel-collector/tls/tls.key"`),
			jq.Match(`.spec.volumes[] | select(.name == "tls-certs") | .secret.secretName == "data-science-collector-tls"`),
		)),
		WithCustomErrorMsg("OpenTelemetryCollector should have TLS configured for Prometheus exporter"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ServiceMonitor, types.NamespacedName{
			Name:      "data-science-prometheus-monitor",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.endpoints[0].scheme == "https"`),
			jq.Match(`.spec.endpoints[0].tlsConfig.serverName == "data-science-collector-prometheus.%s.svc"`, tc.MonitoringNamespace),
		)),
		WithCustomErrorMsg("ServiceMonitor should always use HTTPS to scrape Prometheus exporter"),
	)
}

// ========================================================================
// Group 4: Target Allocator
// ========================================================================

func (tc *MonitoringTestCtx) runTargetAllocatorTests(t *testing.T) {
	t.Helper()

	t.Run("Group 4: Target Allocator", func(t *testing.T) {
		tc.cleanupGroup(t, "")

		t.Cleanup(func() {
			tc.cleanupGroup(t, "")
		})

		t.Run("Test Target Allocator not deployed without metrics", tc.ValidateTargetAllocatorNotDeployedWithoutMetrics)

		t.Run("With Metrics", func(t *testing.T) {
			tc.setupMetrics(t)

			t.Run("Test Target Allocator deployment with metrics", tc.ValidateTargetAllocatorDeploymentWithMetrics)
			t.Run("Test Target Allocator Service and ConfigMap", tc.ValidateTargetAllocatorServiceAndConfigMap)
			t.Run("Test Target Allocator lifecycle", tc.ValidateTargetAllocatorLifecycle)
			t.Run("Test Target Allocator RBAC configuration", tc.ValidateTargetAllocatorRBACConfiguration)
		})
	})
}

// ValidateTargetAllocatorNotDeployedWithoutMetrics tests that the Target Allocator is not deployed when metrics are not configured.
func (tc *MonitoringTestCtx) ValidateTargetAllocatorNotDeployedWithoutMetrics(t *testing.T) {
	t.Helper()
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withNoMetrics(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.spec.metrics == null`),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("Monitoring resource should be created without metrics configuration"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(jq.Match(
			`[.status.conditions[] | select(.type=="%s" and .status=="False")] | length==1`,
			conditions.ConditionOpenTelemetryCollectorAvailable,
		)),
		WithCustomErrorMsg("OpenTelemetryCollectorAvailable condition should be False when metrics are not configured"),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      OpenTelemetryCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{
			Name:      TargetAllocatorDeploymentName,
			Namespace: tc.MonitoringNamespace,
		}),
	)
}

// ValidateTargetAllocatorDeploymentWithMetrics tests that the Target Allocator is deployed and ready when metrics are configured.
func (tc *MonitoringTestCtx) ValidateTargetAllocatorDeploymentWithMetrics(t *testing.T) {
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
		)),
		WithCustomErrorMsg("Monitoring resource should be updated with metrics configuration"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      OpenTelemetryCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.targetAllocator.enabled == true`),
			jq.Match(`.spec.targetAllocator.serviceAccount == "%s"`, TargetAllocatorServiceAccount),
			jq.Match(`.spec.targetAllocator.prometheusCR.enabled == true`),
			jq.Match(`.spec.targetAllocator.prometheusCR.podMonitorSelector.matchLabels."monitoring.opendatahub.io/scrape" == "true"`),
			jq.Match(`.spec.targetAllocator.prometheusCR.serviceMonitorSelector.matchLabels."monitoring.opendatahub.io/scrape" == "true"`),
		)),
		WithCustomErrorMsg("OpenTelemetryCollector should have targetAllocator enabled with correct configuration"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{
			Name:      TargetAllocatorDeploymentName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.status.readyReplicas >= 1`),
			jq.Match(`.status.conditions[] | select(.type == "Available") | .status == "True"`),
		)),
		WithCustomErrorMsg("Target Allocator Deployment should be created and available"),
	)
}

// ValidateTargetAllocatorServiceAndConfigMap tests that the Target Allocator Service and ConfigMap are created correctly.
func (tc *MonitoringTestCtx) ValidateTargetAllocatorServiceAndConfigMap(t *testing.T) {
	t.Helper()
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Service, types.NamespacedName{
			Name:      TargetAllocatorDeploymentName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(
			jq.Match(`.spec.ports[] | select(.name == "targetallocation") | .port == 80`),
		),
		WithCustomErrorMsg("Target Allocator Service should be created"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ConfigMap, types.NamespacedName{
			Name:      TargetAllocatorDeploymentName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCustomErrorMsg("Target Allocator ConfigMap should be created by OpenTelemetry Operator"),
	)
}

// ValidateTargetAllocatorLifecycle tests the complete lifecycle of Target Allocator deployment and cleanup.
func (tc *MonitoringTestCtx) ValidateTargetAllocatorLifecycle(t *testing.T) {
	t.Helper()
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{
			Name:      TargetAllocatorDeploymentName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`.status.readyReplicas >= 1`)),
		WithCustomErrorMsg("Target Allocator should be deployed when metrics are enabled"),
	)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withNoMetrics(),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{
			Name:      TargetAllocatorDeploymentName,
			Namespace: tc.MonitoringNamespace,
		}),
	)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{
			Name:      TargetAllocatorDeploymentName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`.status.readyReplicas >= 1`)),
		WithCustomErrorMsg("Target Allocator should be recreated when metrics are re-enabled"),
	)
}

// ValidateTargetAllocatorRBACConfiguration tests that Target Allocator has correct RBAC permissions.
func (tc *MonitoringTestCtx) ValidateTargetAllocatorRBACConfiguration(t *testing.T) {
	t.Helper()
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ServiceAccount, types.NamespacedName{
			Name:      TargetAllocatorServiceAccount,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCustomErrorMsg("ServiceAccount for Target Allocator should exist"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRole, types.NamespacedName{
			Name: "generate-processors-role",
		}),
		WithCondition(And(
			jq.Match(`.rules[] | select(.apiGroups[] == "monitoring.coreos.com") | .resources | contains(["podmonitors", "servicemonitors"])`),
			jq.Match(`.rules[] | select(.apiGroups[] == "monitoring.coreos.com") | .verbs | contains(["get", "list", "watch"])`),
			jq.Match(`.rules[] | select(.apiGroups[] == "") | .resources | contains(["endpoints"])`),
			jq.Match(`.rules[] | select(.apiGroups[] == "") | .verbs | contains(["get", "list", "watch"])`),
			jq.Match(`.rules[] | select(.apiGroups[] == "discovery.k8s.io") | .resources | contains(["endpointslices"])`),
			jq.Match(`.rules[] | select(.apiGroups[] == "discovery.k8s.io") | .verbs | contains(["get", "list", "watch"])`),
		)),
		WithCustomErrorMsg("ClusterRole should grant Target Allocator permissions to watch ServiceMonitors, PodMonitors, Endpoints, and EndpointSlices"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{
			Name: "generate-processors-collector-rolebinding",
		}),
		WithCondition(And(
			jq.Match(`.roleRef.name == "generate-processors-role"`),
			jq.Match(`.subjects[0].name == "%s"`, TargetAllocatorServiceAccount),
			jq.Match(`.subjects[0].namespace == "%s"`, tc.MonitoringNamespace),
		)),
		WithCustomErrorMsg("ClusterRoleBinding should bind Target Allocator ClusterRole to ServiceAccount"),
	)
}

// ========================================================================
// Group 5: Thanos Querier
// ========================================================================

func (tc *MonitoringTestCtx) runThanosQuerierTests(t *testing.T) {
	t.Helper()

	t.Run("Group 5: Thanos Querier", func(t *testing.T) {
		tc.cleanupGroup(t, "")

		t.Cleanup(func() {
			tc.cleanupGroup(t, "")
		})

		t.Run("Test ThanosQuerier not deployed without metrics", tc.ValidateThanosQuerierNotDeployedWithoutMetrics)
		t.Run("Test ThanosQuerier deployment with metrics", tc.ValidateThanosQuerierDeployment)
		t.Run("Test Prometheus NetworkPolicy allows Thanos Querier on gRPC port", tc.ValidatePrometheusNetworkPolicyAllowsThanosQuerier)
	})
}

// ValidateThanosQuerierNotDeployedWithoutMetrics tests that ThanosQuerier is not deployed when metrics are not configured.
func (tc *MonitoringTestCtx) ValidateThanosQuerierNotDeployedWithoutMetrics(t *testing.T) {
	t.Helper()
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withNoMetrics(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.spec.metrics == null`),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("Monitoring resource should be created without metrics configuration"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(jq.Match(
			`[.status.conditions[] | select(.type=="%s" and .status=="False" and .reason=="%s")] | length==1`,
			conditions.ConditionThanosQuerierAvailable,
			conditions.MetricsNotConfiguredReason,
		)),
		WithCustomErrorMsg("ThanosQuerier condition should be False with reason MetricsNotConfigured when metrics are not configured"),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.ThanosQuerier, types.NamespacedName{Name: ThanosQuerierName, Namespace: tc.MonitoringNamespace}),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.Route, types.NamespacedName{Name: ThanosQuerierRouteName, Namespace: tc.MonitoringNamespace}),
	)
}

// ValidateThanosQuerierDeployment tests that ThanosQuerier CR and Route are created when metrics are configured.
func (tc *MonitoringTestCtx) ValidateThanosQuerierDeployment(t *testing.T) {
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
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionThanosQuerierAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("Monitoring resource should be updated with metrics configuration and ThanosQuerier should be available"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ThanosQuerier, types.NamespacedName{Name: ThanosQuerierName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.spec.selector.matchLabels."platform.opendatahub.io/part-of" == "monitoring"`),
			jq.Match(`.spec.namespaceSelector.matchNames | contains(["%s"])`, tc.MonitoringNamespace),
			jq.Match(`.spec.replicaLabels | contains(["prometheus_replica", "rule_replica"])`),
			monitoringOwnerReferencesCondition,
		)),
		WithCustomErrorMsg("ThanosQuerier CR should be created when metrics are configured"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Route, types.NamespacedName{Name: ThanosQuerierRouteName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.spec.to.name == "thanos-querier-data-science-thanos-querier"`),
			jq.Match(`.spec.tls.termination == "edge"`),
			jq.Match(`.spec.tls.insecureEdgeTerminationPolicy == "Redirect"`),
			jq.Match(`.metadata.labels.app == "thanos-querier"`),
			jq.Match(`.metadata.labels."app.kubernetes.io/name" == "thanos-querier"`),
			jq.Match(`.metadata.labels."app.kubernetes.io/component" == "querier"`),
			jq.Match(`.metadata.labels."app.kubernetes.io/part-of" == "data-science-monitoring"`),
			monitoringOwnerReferencesCondition,
		)),
		WithCustomErrorMsg("ThanosQuerier Route should be created when metrics are configured"),
	)
}

// ValidatePrometheusNetworkPolicyAllowsThanosQuerier tests that the Prometheus instance NetworkPolicy
// includes an ingress rule allowing the Thanos Querier to reach the Thanos Sidecar on gRPC port 10901.
func (tc *MonitoringTestCtx) ValidatePrometheusNetworkPolicyAllowsThanosQuerier(t *testing.T) {
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
		WithCustomErrorMsg("Monitoring resource should be ready with MonitoringStack available before checking NetworkPolicy"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
			Name:      "data-science-prometheus-instance-ingress",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.metadata.labels["platform.opendatahub.io/part-of"] == "monitoring"`),
			jq.Match(`.spec.podSelector.matchLabels["app.kubernetes.io/name"] == "prometheus"`),
			jq.Match(`.spec.podSelector.matchLabels["app.kubernetes.io/instance"] == "data-science-monitoringstack"`),
			jq.Match(`.spec.policyTypes[0] == "Ingress"`),
			jq.Match(`.spec.ingress[0].from[0].podSelector.matchLabels.app == "data-science-prometheus-namespace-proxy"`),
			jq.Match(`.spec.ingress[0].from[1].podSelector.matchLabels.app == "data-science-prometheus-cluster-proxy"`),
			jq.Match(`.spec.ingress[0].ports[0].protocol == "TCP"`),
			jq.Match(`.spec.ingress[0].ports[0].port == 9090`),
			jq.Match(`.spec.ingress[1].from[0].podSelector.matchLabels["app.kubernetes.io/part-of"] == "ThanosQuerier"`),
			jq.Match(`.spec.ingress[1].from[0].podSelector.matchLabels["app.kubernetes.io/managed-by"] == "observability-operator"`),
			jq.Match(`.spec.ingress[1].ports[0].protocol == "TCP"`),
			jq.Match(`.spec.ingress[1].ports[0].port == 10901`),
		)),
		WithCustomErrorMsg("Prometheus NetworkPolicy should allow Thanos Querier ingress on gRPC port 10901"),
	)
}

// ========================================================================
// Group 6: Traces with PV Backend
// ========================================================================

func (tc *MonitoringTestCtx) runTracesWithPVBackendTests(t *testing.T) {
	t.Helper()

	t.Run("Group 6: Traces with PV Backend", func(t *testing.T) {
		t.Cleanup(func() {
			tc.cleanupGroup(t, "")
		})

		t.Run("Test TempoMonolithic CR Creation with PV backend", tc.ValidateTempoMonolithicCRCreation)
	})
}

// ValidateTempoMonolithicCRCreation tests creation of TempoMonolithic CR with PV backend and custom retention.
func (tc *MonitoringTestCtx) ValidateTempoMonolithicCRCreation(t *testing.T) {
	t.Helper()

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withMonitoringTraces(TracesStorageBackendPV, "", TracesStorageSize1Gi, DefaultRetention),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`.spec.traces != null`),
		)),
		WithCustomErrorMsg("Monitoring resource should be updated with traces configuration"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.TempoMonolithic, types.NamespacedName{Name: TempoMonolithicName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`.spec.storage.traces.size == "%s"`, TracesStorageSize1Gi),
			jq.Match(`.spec.storage.traces.backend == "pv"`),
			jq.Match(`.spec.extraConfig.tempo.compactor.compaction.block_retention == "%s"`, FormattedRetention),
		)),
		WithCustomErrorMsg("TempoMonolithic CR should be created by controller when traces are configured"),
	)
}

// ========================================================================
// Group 7: Traces with Cloud Storage (S3 & GCS)
// ========================================================================

func (tc *MonitoringTestCtx) runTracesWithCloudStorageTests(t *testing.T) {
	t.Helper()

	t.Run("Group 7: Traces with Cloud Storage", func(t *testing.T) {
		t.Cleanup(func() {
			tc.cleanupGroup(t, "s3-secret")
			tc.cleanupGroup(t, "gcs-secret")
		})

		t.Run("Test TempoStack CR Creation with Cloud Storage", tc.ValidateTempoStackCRCreationWithCloudStorage)
		t.Run("Test Instrumentation CR Traces lifecycle", tc.ValidateInstrumentationCRTracesLifecycle)
		t.Run("Test Traces Exporters Reserved Name Validation", tc.ValidateTracesExportersReservedNameValidation)
	})
}

// ValidateTempoStackCRCreationWithCloudStorage tests creation of TempoStack CR with cloud storage backends.
func (tc *MonitoringTestCtx) ValidateTempoStackCRCreationWithCloudStorage(t *testing.T) {
	t.Helper()

	testCases := []struct {
		name                string
		backend             string
		monitoringCondition gTypes.GomegaMatcher
		monitoringErrorMsg  string
	}{
		{
			name:                "S3 backend",
			backend:             TracesStorageBackendS3,
			monitoringCondition: jq.Match(`.spec.traces != null`),
			monitoringErrorMsg:  "Monitoring resource should be updated with traces configuration",
		},
		{
			name:                "GCS backend",
			backend:             TracesStorageBackendGCS,
			monitoringCondition: jq.Match(`.spec.traces.storage.backend == "%s"`, TracesStorageBackendGCS),
			monitoringErrorMsg:  "Monitoring resource should be updated with GCS traces configuration",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			tc.validateTempoStackCreationAndPersesTLS(
				t,
				testCase.backend,
				testCase.monitoringCondition,
				testCase.monitoringErrorMsg,
			)
		})
	}
}

// validateTempoStackCreationAndPersesTLS validates both TempoStack creation with a cloud backend
// AND PersesDatasource TLS configuration in a single TempoStack lifecycle.
func (tc *MonitoringTestCtx) validateTempoStackCreationAndPersesTLS(t *testing.T, backend string, monitoringCondition gTypes.GomegaMatcher, monitoringErrorMsg string) {
	t.Helper()

	secretName := fmt.Sprintf("%s-secret", backend)

	tc.validateTempoStackCreation(t, backend, secretName, monitoringCondition, monitoringErrorMsg)
	tc.validatePersesDatasourceTLS(t, backend, secretName)

	tc.cleanupTracesConfiguration()
	tc.cleanupTempoStackAndSecret(secretName)
}

// validateTempoStackCreation creates a secret, enables traces for the given backend,
// and waits until the TempoStack is ready with the correct configuration.
func (tc *MonitoringTestCtx) validateTempoStackCreation(t *testing.T, backend, secretName string, monitoringCondition gTypes.GomegaMatcher, monitoringErrorMsg string) {
	t.Helper()

	tc.ensureMonitoringCleanSlate(t, secretName)

	tc.createDummySecret(t, backend, secretName, tc.MonitoringNamespace)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withMonitoringTraces(backend, secretName, "", DefaultRetention),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(monitoringCondition),
		WithCustomErrorMsg(monitoringErrorMsg),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.TempoStack, types.NamespacedName{
			Name:      TempoStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.storage.secret.type == "%s"`, backend),
			jq.Match(`.spec.storage.secret.name == "%s"`, secretName),
			jq.Match(`.spec.retention.global.traces == "%s"`, FormattedRetention),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
		)),
		WithEventuallyTimeout(15*time.Minute),
		WithCustomErrorMsg("TempoStack should be created by controller with %s backend", backend),
	)
}

// validatePersesDatasourceTLS enables TLS on the existing traces configuration and validates
// that the PersesDatasource is updated with the correct TLS settings.
func (tc *MonitoringTestCtx) validatePersesDatasourceTLS(t *testing.T, backend, secretName string) {
	t.Helper()

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withMonitoringTraces(backend, secretName, "", DefaultRetention),
		jq.Transform(`.spec.traces.tls.enabled = true`),
	)

	expectedTempoEndpoint := fmt.Sprintf("https://tempo-data-science-tempostack-gateway.%s.svc.cluster.local:8080/api/traces/v1/%s/tempo",
		tc.MonitoringNamespace, tc.MonitoringNamespace)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.PersesDatasource, types.NamespacedName{Name: "tempo-datasource", Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.spec.config.plugin.spec.proxy.spec.url == "%s"`, expectedTempoEndpoint),
			jq.Match(`.spec.client.tls.enable == true`),
			jq.Match(`.spec.client.tls.caCert.type == "configmap"`),
			jq.Match(`.spec.client.tls.caCert.name == "tempo-service-ca"`),
			jq.Match(`.spec.client.tls.caCert.certPath == "service-ca.crt"`),
			jq.Match(`.spec.config.plugin.spec.proxy.spec.secret == "tempo-datasource-secret"`),
		)),
		WithCustomErrorMsg("PersesDatasource should have TLS enabled for %s backend", backend),
	)
}

// ValidateInstrumentationCRTracesLifecycle tests the Instrumentation CR lifecycle with traces.
func (tc *MonitoringTestCtx) ValidateInstrumentationCRTracesLifecycle(t *testing.T) {
	t.Helper()

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withMonitoringTraces(TracesStorageBackendPV, "", "", DefaultRetention),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.spec.traces != null`),
			jq.Match(`.spec.traces.storage.retention == "%s"`, FormattedRetention),
		)),
		WithCustomErrorMsg("Monitoring resource should be updated with traces configuration"),
	)

	expectedEndpoint := fmt.Sprintf("http://%s.%s.svc.cluster.local:4317", OpenTelemetryCollectorName, tc.MonitoringNamespace)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Instrumentation, types.NamespacedName{Name: InstrumentationName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.spec != null`),
			jq.Match(`.metadata.generation >= 1`),
			jq.Match(`
				(.spec.exporter.endpoint == "%s") and
				(.spec.sampler.type == "traceidratio") and
				(.spec.sampler.argument == "0.1")
			`, expectedEndpoint),
			monitoringOwnerReferencesCondition,
		)),
		WithCustomErrorMsg("Instrumentation CR should be created with correct configuration and owner references"),
	)
}

// ValidateTracesExportersReservedNameValidation tests that reserved exporter names are rejected.
func (tc *MonitoringTestCtx) ValidateTracesExportersReservedNameValidation(t *testing.T) {
	t.Helper()

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withMonitoringTraces(TracesStorageBackendPV, "", "", ""),
		withReservedTracesExporter(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeProvisioningSucceeded, metav1.ConditionFalse),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .message | contains("reserved")`, common.ConditionTypeProvisioningSucceeded),
		)),
	)
}

// ========================================================================
// Group 8: Perses
// ========================================================================

func (tc *MonitoringTestCtx) runPersesTests(t *testing.T) {
	t.Helper()

	t.Run("Group 8: Perses", func(t *testing.T) {
		t.Cleanup(func() {
			tc.cleanupGroup(t, "")
		})

		t.Run("Perses Lifecycle", func(t *testing.T) {
			tc.setupMetrics(t)

			t.Run("Test Perses deployment when monitoring is managed", tc.ValidatePersesCRCreation)
			t.Run("Test Perses CR configuration", tc.ValidatePersesCRConfiguration)
			t.Run("Test Perses lifecycle", tc.ValidatePersesLifecycle)
			t.Run("Test Perses not deployed without metrics or traces", tc.ValidatePersesNotDeployedWithoutMetricsOrTraces)
			t.Run("Test Perses NetworkPolicy creation", tc.ValidatePersesNetworkPolicy)
		})

		t.Run("Perses Datasource with Traces", func(t *testing.T) {
			t.Run("Test Perses Datasource Creation with Traces", tc.ValidatePersesDatasourceCreationWithTraces)
			t.Run("Test Perses Datasource Configuration", tc.ValidatePersesDatasourceConfiguration)
			t.Run("Test PersesDatasource deployment with Prometheus", tc.ValidatePersesDatasourceWithPrometheus)
			t.Run("Test PersesDatasource lifecycle", tc.ValidatePersesDatasourceLifecycle)
		})
	})
}

// ValidatePersesCRCreation tests that Perses CR is created when monitoring is managed with metrics.
func (tc *MonitoringTestCtx) ValidatePersesCRCreation(t *testing.T) {
	t.Helper()

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.spec.metrics != null`),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionPersesAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("Monitoring CR should be ready with Perses available before validating Perses CR"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Perses, types.NamespacedName{Name: PersesName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			monitoringOwnerReferencesCondition,
			jq.Match(`.spec.containerPort == 8080`),
			jq.Match(`.spec.config.database.file.folder == "/perses"`),
			jq.Match(`.spec.config.database.file.extension == "yaml"`),
		)),
		WithCustomErrorMsg("Perses CR should be created with correct configuration when monitoring is managed"),
	)
}

// ValidatePersesCRConfiguration tests Perses CR configuration details.
func (tc *MonitoringTestCtx) ValidatePersesCRConfiguration(t *testing.T) {
	t.Helper()

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.spec.metrics != null`),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionPersesAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("Monitoring CR should be ready with Perses available before validating configuration"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Perses, types.NamespacedName{Name: PersesName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.spec.containerPort == 8080`),
			jq.Match(`.spec.config.database.file != null`),
			jq.Match(`.spec.storage.size == "1Gi"`),
			jq.Match(`.metadata.labels["platform.opendatahub.io/part-of"] == "monitoring"`),
		)),
		WithCustomErrorMsg("Perses CR configuration validation failed"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.StatefulSet, types.NamespacedName{Name: PersesName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.spec.replicas == 1`),
			jq.Match(`.spec.template.spec.containers[0].ports[0].containerPort == 8080`),
			jq.Match(`.spec.volumeClaimTemplates[0].spec.resources.requests.storage == "1Gi"`),
		)),
		WithCustomErrorMsg("Perses StatefulSet should be created by perses-operator with correct configuration"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Pod, types.NamespacedName{Name: PersesName + "-0", Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.status.phase == "Running"`),
			jq.Match(`.status.conditions[] | select(.type == "Ready") | .status == "True"`),
		)),
		WithCustomErrorMsg("Perses pod should be running and ready"),
	)
}

// ValidatePersesLifecycle tests the Perses CR lifecycle (create, delete, recreate).
func (tc *MonitoringTestCtx) ValidatePersesLifecycle(t *testing.T) {
	t.Helper()

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Perses, types.NamespacedName{Name: PersesName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.metadata.name == "%s"`, PersesName)),
		WithCustomErrorMsg("Perses CR should exist when monitoring is managed"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionPersesAvailable, metav1.ConditionTrue),
		),
		WithCustomErrorMsg("Monitoring CR should have PersesAvailable condition set to True"),
	)

	tc.resetMonitoringConfigToRemoved()

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.Perses, types.NamespacedName{Name: PersesName, Namespace: tc.MonitoringNamespace}),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.StatefulSet, types.NamespacedName{Name: PersesName, Namespace: tc.MonitoringNamespace}),
	)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Perses, types.NamespacedName{Name: PersesName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.metadata.name == "%s"`, PersesName)),
		WithCustomErrorMsg("Perses CR should be recreated when monitoring is re-enabled"),
	)
}

// ValidatePersesNotDeployedWithoutMetricsOrTraces tests that Perses is not deployed when neither metrics nor traces are configured.
func (tc *MonitoringTestCtx) ValidatePersesNotDeployedWithoutMetricsOrTraces(t *testing.T) {
	t.Helper()
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withNoMetrics(),
		withNoTraces(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.spec.metrics == null`),
			jq.Match(`.spec.traces == null`),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("Monitoring resource should be created without metrics or traces configuration"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(jq.Match(
			`[.status.conditions[] | select(.type=="%s" and .status=="False")] | length==1`,
			conditions.ConditionPersesAvailable,
		)),
		WithCustomErrorMsg("Perses condition should be False when neither metrics nor traces are configured"),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.Perses, types.NamespacedName{Name: PersesName, Namespace: tc.MonitoringNamespace}),
	)
}

// ValidatePersesNetworkPolicy tests the Perses NetworkPolicy creation.
func (tc *MonitoringTestCtx) ValidatePersesNetworkPolicy(t *testing.T) {
	t.Helper()

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{Name: "perses-operator-access", Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.metadata.labels["platform.opendatahub.io/part-of"] == "monitoring"`),
			jq.Match(`.spec.podSelector.matchLabels["app.kubernetes.io/managed-by"] == "perses-operator"`),
			jq.Match(`.spec.policyTypes[0] == "Ingress"`),
			jq.Match(`.spec.ingress[0].from[0].namespaceSelector.matchLabels["kubernetes.io/metadata.name"] == "openshift-cluster-observability-operator"`),
			jq.Match(`.spec.ingress[0].from[0].podSelector.matchLabels["app.kubernetes.io/name"] == "perses-operator"`),
			jq.Match(`.spec.ingress[0].ports[0].protocol == "TCP"`),
			jq.Match(`.spec.ingress[0].ports[0].port == 8080`),
		)),
		WithCustomErrorMsg("Perses NetworkPolicy should be created with correct configuration allowing perses-operator access"),
	)
}

// ValidatePersesDatasourceCreationWithTraces tests that Perses datasource is created when traces are configured.
func (tc *MonitoringTestCtx) ValidatePersesDatasourceCreationWithTraces(t *testing.T) {
	t.Helper()

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withMonitoringTraces(TracesStorageBackendPV, "", "", ""),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(jq.Match(`.spec.traces != null`)),
		WithCustomErrorMsg("Monitoring resource should be updated with traces configuration"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.PersesDatasource, types.NamespacedName{Name: "tempo-datasource", Namespace: tc.MonitoringNamespace}),
		WithCustomErrorMsg("PersesDatasource CR should be created when traces are configured"),
	)

	tc.updateMonitoringConfig(withNoTraces())

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.PersesDatasource, types.NamespacedName{
			Name:      "tempo-datasource",
			Namespace: tc.MonitoringNamespace,
		}),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(jq.Match(
			`[.status.conditions[] | select(.type=="%s" and .status=="False" and .reason=="%s")] | length==1`,
			conditions.ConditionPersesTempoDataSourceAvailable,
			conditions.TracesNotConfiguredReason,
		)),
		WithCustomErrorMsg("PersesTempoDataSourceAvailable condition should be False when traces are not configured"),
	)
}

// ValidatePersesDatasourceConfiguration tests the configuration of the Perses datasource.
func (tc *MonitoringTestCtx) ValidatePersesDatasourceConfiguration(t *testing.T) {
	t.Helper()

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withMonitoringTraces(TracesStorageBackendPV, "", "", ""),
	)

	expectedTempoEndpoint := fmt.Sprintf("https://tempo-data-science-tempomonolithic-gateway.%s.svc.cluster.local:8080/api/traces/v1/%s/tempo",
		tc.MonitoringNamespace, tc.MonitoringNamespace)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.PersesDatasource, types.NamespacedName{Name: "tempo-datasource", Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.spec.config.display.name == "RHOAI Tempo Datasource"`),
			jq.Match(`.spec.config.display.description == "Tempo datasource for distributed tracing in RHOAI"`),
			jq.Match(`.spec.config.default == false`),
			jq.Match(`.spec.config.plugin.kind == "TempoDatasource"`),
			jq.Match(`.spec.config.plugin.spec.proxy.kind == "HTTPProxy"`),
			jq.Match(`.spec.config.plugin.spec.proxy.spec.url == "%s"`, expectedTempoEndpoint),
			jq.Match(`.spec.client.tls.enable == true`),
			jq.Match(`.spec.client.tls.caCert.name == "tempo-service-ca"`),
			jq.Match(`.spec.client.tls.caCert.certPath == "service-ca.crt"`),
			jq.Match(`.spec.config.plugin.spec.proxy.spec.secret == "tempo-datasource-secret"`),
		)),
		WithCustomErrorMsg("PersesDatasource should have TLS enabled with secret reference for gateway HTTPS"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.PersesDatasource, types.NamespacedName{Name: "tempo-datasource", Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.metadata.ownerReferences | length == 1`),
			jq.Match(`.metadata.ownerReferences[0].kind == "%s"`, gvk.Monitoring.Kind),
			jq.Match(`.metadata.ownerReferences[0].name == "%s"`, MonitoringCRName),
		)),
	)

	tc.updateMonitoringConfig(withNoTraces())
}

// ValidatePersesDatasourceWithPrometheus validates that Prometheus datasource is created when both Perses and MonitoringStack are deployed.
func (tc *MonitoringTestCtx) ValidatePersesDatasourceWithPrometheus(t *testing.T) {
	t.Helper()

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionPersesAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionMonitoringStackAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionPersesPrometheusDataSourceAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("Monitoring CR should have all conditions (Perses, MonitoringStack, PersesPrometheusDataSource) set to True"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.PersesDatasource, types.NamespacedName{Name: PersesDatasourceName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			monitoringOwnerReferencesCondition,
			jq.Match(`.spec.config.default == false`),
			jq.Match(`.spec.config.plugin.kind == "PrometheusDatasource"`),
			jq.Match(`.spec.config.plugin.spec.proxy.kind == "HTTPProxy"`),
			jq.Match(`.spec.config.plugin.spec.proxy.spec.url | contains("thanos-querier-data-science-thanos-querier")`),
			jq.Match(`.spec.config.plugin.spec.proxy.spec.url | contains("%s")`, tc.MonitoringNamespace),
		)),
		WithCustomErrorMsg("Data Science PersesDatasource CR should be created with correct Thanos Querier proxy configuration"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.PersesDatasource, types.NamespacedName{Name: ClusterPrometheusDatasourceName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			monitoringOwnerReferencesCondition,
			jq.Match(`.spec.config.default == true`),
			jq.Match(`.spec.config.plugin.kind == "PrometheusDatasource"`),
			jq.Match(`.spec.config.plugin.spec.proxy.kind == "HTTPProxy"`),
			jq.Match(`.spec.config.plugin.spec.proxy.spec.url | contains("thanos-querier.openshift-monitoring")`),
			jq.Match(`.spec.client.tls.enable == true`),
			jq.Match(`.spec.config.plugin.spec.proxy.spec.secret == "%s"`, ClusterPrometheusDatasourceSecret),
		)),
		WithCustomErrorMsg("Cluster Prometheus PersesDatasource CR should be created with correct openshift-monitoring Thanos Querier configuration"),
	)
}

// ValidatePersesDatasourceLifecycle tests the complete lifecycle of PersesDatasource deployment and cleanup.
func (tc *MonitoringTestCtx) ValidatePersesDatasourceLifecycle(t *testing.T) {
	t.Helper()

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.PersesDatasource, types.NamespacedName{Name: PersesDatasourceName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.metadata.name == "%s"`, PersesDatasourceName)),
		WithCustomErrorMsg("Data Science PersesDatasource should exist when metrics are configured"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.PersesDatasource, types.NamespacedName{Name: ClusterPrometheusDatasourceName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.metadata.name == "%s"`, ClusterPrometheusDatasourceName)),
		WithCustomErrorMsg("Cluster Prometheus PersesDatasource should exist when metrics are configured"),
	)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withNoMetrics(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionPersesPrometheusDataSourceAvailable, metav1.ConditionFalse),
		),
		WithCustomErrorMsg("Monitoring CR should have PersesPrometheusDataSourceAvailable condition set to False when metrics are not configured"),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.PersesDatasource, types.NamespacedName{Name: PersesDatasourceName, Namespace: tc.MonitoringNamespace}),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.PersesDatasource, types.NamespacedName{Name: ClusterPrometheusDatasourceName, Namespace: tc.MonitoringNamespace}),
	)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.PersesDatasource, types.NamespacedName{Name: PersesDatasourceName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.metadata.name == "%s"`, PersesDatasourceName)),
		WithCustomErrorMsg("Data Science PersesDatasource should be recreated when metrics are re-enabled"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.PersesDatasource, types.NamespacedName{Name: ClusterPrometheusDatasourceName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.metadata.name == "%s"`, ClusterPrometheusDatasourceName)),
		WithCustomErrorMsg("Cluster Prometheus PersesDatasource should be recreated when metrics are re-enabled"),
	)
}

// ========================================================================
// Group 9: Advanced Networking / RBAC
// ========================================================================

func (tc *MonitoringTestCtx) runNetworkingTests(t *testing.T) {
	t.Helper()

	t.Run("Group 9: Networking and RBAC", func(t *testing.T) {
		tc.setupMetrics(t)

		t.Cleanup(func() {
			tc.cleanupGroup(t, "")
		})

		t.Run("Prometheus restricted resource configuration", tc.ValidatePrometheusRestrictedResourceConfiguration)
		t.Run("Prometheus secure proxy authentication", tc.ValidatePrometheusSecureProxyAuthentication)
		t.Run("Node metrics endpoint deployment", tc.ValidateNodeMetricsEndpointDeployment)
		t.Run("Node metrics endpoint RBAC configuration", tc.ValidateNodeMetricsEndpointRBACConfiguration)
	})
}

func (tc *MonitoringTestCtx) waitForPrometheusNamespaceProxyPrerequisites(t *testing.T) {
	t.Helper()

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.MonitoringStack, types.NamespacedName{
			Name:      MonitoringStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`.status.conditions[] | select(.type == "Available") | .status == "True"`)),
		WithCustomErrorMsg("MonitoringStack should be Available before prometheus-namespace-proxy deployment"),
	)

	t.Logf("MonitoringStack CR is Available — prerequisites met for prometheus-namespace-proxy")
}

func (tc *MonitoringTestCtx) validatePrometheusNamespaceProxyResourcesCommon(t *testing.T) {
	t.Helper()

	tc.waitForPrometheusNamespaceProxyPrerequisites(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{
			Name:      "data-science-prometheus-namespace-proxy",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.status.readyReplicas == 1`),
			jq.Match(`.spec.template.spec.containers | length == 2`),
		)),
		WithCustomErrorMsg("data-science-prometheus-namespace-proxy deployment should be created and ready"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ServiceAccount, types.NamespacedName{
			Name:      "data-science-prometheus-namespace-proxy",
			Namespace: tc.MonitoringNamespace,
		}),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{
			Name: "data-science-prometheus-namespace-proxy",
		}),
		WithCondition(And(
			jq.Match(`.roleRef.name == "cluster-monitoring-view"`),
			jq.Match(`.subjects[0].name == "data-science-prometheus-namespace-proxy"`),
			jq.Match(`.subjects[0].namespace == "%s"`, tc.MonitoringNamespace),
		)),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Service, types.NamespacedName{
			Name:      "data-science-prometheus-namespace-proxy",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.ports[0].port == 8443`),
			jq.Match(`.spec.ports[0].name == "https"`),
			jq.Match(`.metadata.annotations."service.beta.openshift.io/serving-cert-secret-name" == "data-science-prometheus-namespace-proxy-tls"`),
		)),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Route, types.NamespacedName{
			Name:      "data-science-prometheus-route",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.to.name == "data-science-prometheus-namespace-proxy"`),
			jq.Match(`.spec.tls.termination == "reencrypt"`),
			jq.Match(`.spec.tls.insecureEdgeTerminationPolicy == "Redirect"`),
		)),
	)
}

func (tc *MonitoringTestCtx) ValidatePrometheusRestrictedResourceConfiguration(t *testing.T) {
	t.Helper()

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(jq.Match(`.status.phase == "%s"`, common.PhaseReady)),
	)

	tc.validatePrometheusNamespaceProxyResourcesCommon(t)
}

func (tc *MonitoringTestCtx) ValidatePrometheusSecureProxyAuthentication(t *testing.T) {
	t.Helper()

	tc.validatePrometheusNamespaceProxyResourcesCommon(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{
			Name:      "data-science-prometheus-namespace-proxy",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.template.spec.containers[0].name == "kube-rbac-proxy"`),
			jq.Match(`(.spec.template.spec.containers[0].image | contains("kube-rbac-proxy")) or (.spec.template.spec.containers[0].image | contains("kube-auth-proxy"))`),
		)),
		WithCustomErrorMsg("data-science-prometheus-namespace-proxy deployment should contain kube-rbac-proxy container"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{
			Name: "data-science-prometheus-namespace-proxy-auth-delegator",
		}),
		WithCondition(And(
			jq.Match(`.roleRef.name == "system:auth-delegator"`),
			jq.Match(`.subjects[0].name == "data-science-prometheus-namespace-proxy"`),
			jq.Match(`.subjects[0].namespace == "%s"`, tc.MonitoringNamespace),
		)),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ConfigMap, types.NamespacedName{
			Name:      "data-science-prometheus-namespace-proxy-config",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`.data."kube-rbac-proxy.yaml" | contains("authorization")`)),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{
			Name:      "data-science-prometheus-namespace-proxy",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.template.spec.containers[0].args | map(select(contains("--upstream="))) | length == 1`),
			jq.Match(`.spec.template.spec.containers[0].args | map(select(contains("--upstream=http://127.0.0.1:9091/"))) | length == 1`),
			jq.Match(`.spec.template.spec.containers[0].args | map(select(contains("--secure-listen-address=0.0.0.0:8443"))) | length == 1`),
		)),
		WithCustomErrorMsg("kube-rbac-proxy should be configured with correct upstream and secure listen address"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{
			Name: "data-science-prometheus-namespace-proxy-auth-delegator",
		}),
		WithCondition(And(
			jq.Match(`.roleRef.name == "system:auth-delegator"`),
			jq.Match(`.subjects[0].name == "data-science-prometheus-namespace-proxy"`),
		)),
		WithCustomErrorMsg("ClusterRoleBinding should reference system:auth-delegator for authentication and authorization"),
	)
}

func (tc *MonitoringTestCtx) ValidateNodeMetricsEndpointDeployment(t *testing.T) {
	t.Helper()

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.spec.metrics != null`),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionNodeMetricsEndpointAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("Monitoring resource should be updated with metrics configuration and NodeMetricsEndpoint should be available"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{
			Name:      "data-science-prometheus-cluster-proxy",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.status.readyReplicas == 1`),
			jq.Match(`.spec.template.spec.containers | length == 1`),
			jq.Match(`.spec.template.spec.containers[0].name == "kube-rbac-proxy"`),
		)),
		WithCustomErrorMsg("data-science-prometheus-cluster-proxy deployment should be created and ready with kube-rbac-proxy"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ServiceAccount, types.NamespacedName{
			Name:      "data-science-prometheus-cluster-proxy",
			Namespace: tc.MonitoringNamespace,
		}),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Service, types.NamespacedName{
			Name:      "data-science-prometheus-cluster-proxy",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.ports[0].port == 8443`),
			jq.Match(`.spec.ports[0].name == "https"`),
			jq.Match(`.metadata.annotations."service.beta.openshift.io/serving-cert-secret-name" == "data-science-prometheus-cluster-proxy-tls"`),
		)),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Route, types.NamespacedName{
			Name:      "data-science-prometheus-cluster-proxy",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.to.name == "data-science-prometheus-cluster-proxy"`),
			jq.Match(`.spec.tls.termination == "reencrypt"`),
			jq.Match(`.spec.tls.insecureEdgeTerminationPolicy == "Redirect"`),
		)),
	)
}

func (tc *MonitoringTestCtx) ValidateNodeMetricsEndpointRBACConfiguration(t *testing.T) {
	t.Helper()

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{
			Name: "data-science-prometheus-cluster-proxy",
		}),
		WithCondition(And(
			jq.Match(`.roleRef.name == "cluster-monitoring-view"`),
			jq.Match(`.subjects[0].name == "data-science-prometheus-cluster-proxy"`),
			jq.Match(`.subjects[0].namespace == "%s"`, tc.MonitoringNamespace),
		)),
		WithCustomErrorMsg("ClusterRoleBinding should use cluster-monitoring-view role for NodeMetrics access"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{
			Name: "data-science-prometheus-cluster-proxy-auth-delegator",
		}),
		WithCondition(And(
			jq.Match(`.roleRef.name == "system:auth-delegator"`),
			jq.Match(`.subjects[0].name == "data-science-prometheus-cluster-proxy"`),
			jq.Match(`.subjects[0].namespace == "%s"`, tc.MonitoringNamespace),
		)),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Secret, types.NamespacedName{
			Name:      "data-science-prometheus-cluster-proxy-kube-rbac-proxy",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.data."config.yaml" | @base64d | contains("authorization")`),
			jq.Match(`.data."config.yaml" | @base64d | contains("metrics.k8s.io")`),
			jq.Match(`.data."config.yaml" | @base64d | contains("resource: nodes")`),
		)),
		WithCustomErrorMsg("kube-rbac-proxy config should enforce NodeMetrics access (metrics.k8s.io/nodes)"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{
			Name:      "data-science-prometheus-cluster-proxy",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.template.spec.containers[0].args | map(select(contains("--upstream="))) | length == 1`),
			jq.Match(`.spec.template.spec.containers[0].args | map(select(contains("--upstream=https://prometheus-operated"))) | length == 1`),
			jq.Match(`.spec.template.spec.containers[0].args | map(select(contains("--secure-listen-address=0.0.0.0:8443"))) | length == 1`),
			jq.Match(`.spec.template.spec.containers[0].args | map(select(contains("--config-file=/etc/kube-rbac-proxy/config.yaml"))) | length == 1`),
			jq.Match(`.spec.template.spec.containers[0].args | map(select(contains("--upstream-ca-file=/etc/prometheus-ca/service-ca.crt"))) | length == 1`),
			jq.Match(`.spec.template.spec.containers[0].args | map(select(contains("--upstream-client-cert-file=/etc/prometheus-client/tls.crt"))) | length == 1`),
			jq.Match(`.spec.template.spec.containers[0].args | map(select(contains("--upstream-client-key-file=/etc/prometheus-client/tls.key"))) | length == 1`),
		)),
		WithCustomErrorMsg("kube-rbac-proxy should be configured with correct upstream (HTTPS to prometheus-operated) and mTLS client certificates"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{
			Name:      "data-science-prometheus-cluster-proxy",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.template.spec.containers[0].name == "kube-rbac-proxy"`),
			jq.Match(`(.spec.template.spec.containers[0].image | contains("kube-rbac-proxy")) or (.spec.template.spec.containers[0].image | contains("kube-auth-proxy"))`),
		)),
		WithCustomErrorMsg("data-science-prometheus-cluster-proxy deployment should contain kube-rbac-proxy container"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{
			Name:      "data-science-prometheus-cluster-proxy",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.template.spec.volumes[] | select(.name == "prometheus-ca") | .configMap.name == "prometheus-web-tls-ca"`),
			jq.Match(`.spec.template.spec.volumes[] | select(.name == "prometheus-client-cert") | .secret.secretName == "prometheus-operated-tls"`),
			jq.Match(`.spec.template.spec.containers[0].volumeMounts[] | select(.name == "prometheus-ca") | .mountPath == "/etc/prometheus-ca"`),
			jq.Match(`.spec.template.spec.containers[0].volumeMounts[] | select(.name == "prometheus-client-cert") | .mountPath == "/etc/prometheus-client"`),
		)),
		WithCustomErrorMsg("deployment should have volumes and mounts for mTLS to Prometheus"),
	)
}

// ========================================================================
// Disabled / Cleanup
// ========================================================================

func (tc *MonitoringTestCtx) runDisabledTests(t *testing.T) {
	t.Helper()

	t.Run("Disabled: Monitoring Service Disabled", func(t *testing.T) {
		t.Run("Validate monitoring service disabled", tc.ValidateMonitoringServiceDisabled)
	})
}

func (tc *MonitoringTestCtx) ValidateMonitoringServiceDisabled(t *testing.T) {
	t.Helper()

	tc.ensureMonitoringCleanSlate(t, "")

	tc.resetMonitoringConfigToRemoved()

	for _, resource := range []struct {
		gvk                schema.GroupVersionKind
		name               string
		forceWithFinalizer bool
	}{
		{gvk: gvk.Monitoring, name: MonitoringCRName},
		{gvk: gvk.MonitoringStack, name: MonitoringStackName, forceWithFinalizer: true},
		{gvk: gvk.TempoStack, name: TempoStackName, forceWithFinalizer: true},
		{gvk: gvk.TempoMonolithic, name: TempoMonolithicName, forceWithFinalizer: true},
		{gvk: gvk.OpenTelemetryCollector, name: OpenTelemetryCollectorName},
		{gvk: gvk.Instrumentation, name: InstrumentationName},
		{gvk: gvk.Perses, name: PersesName},
		{gvk: gvk.PersesDatasource, name: PersesDatasourceName},
		{gvk: gvk.PersesDatasource, name: ClusterPrometheusDatasourceName},
	} {
		if resource.forceWithFinalizer {
			tc.DeleteResource(
				WithMinimalObject(resource.gvk, types.NamespacedName{
					Name:      resource.name,
					Namespace: tc.MonitoringNamespace,
				}),
				WithWaitForDeletion(true),
				WithRemoveFinalizersOnDelete(true),
				WithIgnoreNotFound(true),
			)
		} else {
			tc.EnsureResourceGone(
				WithMinimalObject(resource.gvk, types.NamespacedName{
					Name:      resource.name,
					Namespace: tc.MonitoringNamespace,
				}),
			)
		}
	}
}
