package e2e_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	gTypes "github.com/onsi/gomega/types"
	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	configv1 "github.com/openshift/api/config/v1"
	ocpcrypto "github.com/openshift/library-go/pkg/crypto"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	ctrlcfg "sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega
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

	monitoringServiceCtx.ensurePrerequisites(t)

	t.Run("Baseline without metrics", func(t *testing.T) {
		phaseCtx := monitoringServiceCtx.WithT(t)
		t.Cleanup(phaseCtx.resetMonitoringConfigToManaged)
		phaseCtx.resetMonitoringConfigToManaged()
		t.Run("Base Configuration", phaseCtx.runBaseConfigurationTests)
		t.Run("Negative Conditions", phaseCtx.runNegativeConditionTests)
		t.Run("Target Allocator absent", phaseCtx.ValidateTargetAllocatorNotDeployedWithoutMetrics)
		t.Run("Thanos Querier absent", phaseCtx.ValidateThanosQuerierNotDeployedWithoutMetrics)
		t.Run("Perses absent", phaseCtx.ValidatePersesNotDeployedWithoutMetricsOrTraces)
	})
	t.Run("Metrics ready", func(t *testing.T) {
		phaseCtx := monitoringServiceCtx.WithT(t)
		t.Cleanup(func() { phaseCtx.cleanupGroup(t, "") })
		phaseCtx.resetMonitoringConfigToManaged()
		phaseCtx.setupMetrics(t)
		t.Run("Metrics & MonitoringStack", phaseCtx.runMetricsAndMonitoringStackTests)
		t.Run("Korrel8r", phaseCtx.runKorrel8rTests)
		t.Run("OpenTelemetry Collector", phaseCtx.runCollectorTests)
		t.Run("Target Allocator", phaseCtx.runTargetAllocatorTests)
		t.Run("Thanos Querier", phaseCtx.runThanosQuerierTests)
		t.Run("Networking and RBAC", phaseCtx.runNetworkingTests)
	})
	t.Run("Traces with PV Backend", monitoringServiceCtx.runTracesWithPVBackendTests)
	t.Run("Traces with Cloud Storage", monitoringServiceCtx.runTracesWithCloudStorageTests)
	t.Run("Perses", monitoringServiceCtx.runPersesTests)
	t.Run("Webhooks", monitoringServiceCtx.runWebhookTests)
	t.Run("Usage Logs Collection", monitoringServiceCtx.runUsageLogsCollectionTests)
	t.Run("Lifecycle", monitoringServiceCtx.runMonitoringLifecycleTests)
	t.Run("Disabled", monitoringServiceCtx.runDisabledTests)
}

// ========================================================================
// Group 1: Base Configuration
// ========================================================================

func (tc *MonitoringTestCtx) runBaseConfigurationTests(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	t.Run("Group 1: Base Configuration", func(t *testing.T) {
		tc = tc.WithT(t)
		tc.setupBaseMonitoring(t)

		t.Run("Test Traces default content", tc.ValidateMonitoringCRDefaultTracesContent)
		t.Run("Test active operand namespace and readiness contract", tc.ValidateMonitoringOperandNamespaceContract)
	})
}

// ValidateMonitoringCRDefaultTracesContent verifies that traces stanza is omitted by default.
func (tc *MonitoringTestCtx) ValidateMonitoringCRDefaultTracesContent(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

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
	tc = tc.WithT(t)

	t.Run("Group 2: Metrics & MonitoringStack", func(t *testing.T) {
		tc = tc.WithT(t)

		t.Run("Test Metrics MonitoringStack CR Creation", tc.ValidateMonitoringStackCRMetricsWhenSet)
		t.Run("Test Metrics MonitoringStack CR Configuration", tc.ValidateMonitoringStackCRMetricsConfiguration)
		t.Run("Test Prometheus Self ServiceMonitor TLS Fix", tc.ValidatePrometheusSelfServiceMonitorTLSFix)
		t.Run("Test ownerReference and resourceVersion stability", tc.ValidateReconciliationStability)
	})
}

// ValidateMonitoringStackCRMetricsWhenSet validates that MonitoringStack CR is created when metrics are set.
func (tc *MonitoringTestCtx) ValidateMonitoringStackCRMetricsWhenSet(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

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
	tc = tc.WithT(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.MonitoringStack, types.NamespacedName{Name: MonitoringStackName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.spec.prometheusConfig.persistentVolumeClaim.resources.requests.storage == "%s"`, MetricsStorageSize),
			jq.Match(`.spec.retention == "%s"`, MetricsRetention),
			jq.Match(`.spec.resources.requests.cpu == "%s"`, MetricsCPURequest),
			jq.Match(`.spec.resources.requests.memory == "%s"`, MetricsMemoryRequest),
			jq.Match(`.spec.prometheusConfig.replicas == %d`, tc.expectedDefaultReplicas),
			tc.monitoringOwnerReferencesCondition(),
		)),
		WithCustomErrorMsg("MonitoringStack '%s' configuration validation failed", MonitoringStackName),
	)
}

// ValidateMonitoringStackCRMetricsReplicasUpdate tests that replicas are updated when metrics replicas change.
func (tc *MonitoringTestCtx) ValidateMonitoringStackCRMetricsReplicasUpdate(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

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
	tc = tc.WithT(t)

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

	tc.updateMonitoringConfig(withManagementState(common.Managed), withNoAlerting())
}

// ValidatePrometheusSelfServiceMonitorTLSFix tests the prometheus-self-fixed ServiceMonitor TLS configuration.
func (tc *MonitoringTestCtx) ValidatePrometheusSelfServiceMonitorTLSFix(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(tc.resetMonitoringConfigToMetrics)

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
			tc.monitoringOwnerReferencesCondition(),
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
		WithMinimalObject(gvk.Secret, types.NamespacedName{Name: "prometheus-web-tls-ca", Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.data."service-ca.crt" != null`)),
		WithCustomErrorMsg("prometheus-web-tls-ca Secret should contain the injected service CA"),
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
	tc = tc.WithT(t)

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
		tc.monitoringOwnerReferencesCondition(),
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
		{gvk.ClusterRoleBinding, "generate-processors-targetallocator-rolebinding", "", "legacy TargetAllocator ClusterRoleBinding", false, true},
		{gvk.ClusterRole, "generate-processors-targetallocator-role", "", "TargetAllocator ClusterRole", false, true},
		{gvk.ClusterRoleBinding, "generate-processors-targetallocator-discovery-rolebinding", "", "TargetAllocator discovery ClusterRoleBinding", false, true},
		{gvk.Service, "data-science-collector-prometheus", tc.MonitoringNamespace, "collector prometheus Service", true, true},
		{gvk.ConfigMap, "prometheus-web-tls-ca", tc.MonitoringNamespace, "prometheus TLS CA ConfigMap", true, true},
	}

	// Wait for all resources to exist with correct ownerReferences.
	for _, r := range resources {
		opts := []ResourceOpts{
			WithMinimalObject(r.gvk, types.NamespacedName{Name: r.name, Namespace: r.ns}),
			WithCustomErrorMsg("%s should exist before stability check", r.desc),
		}
		if r.ownerRef {
			opts = append(opts, WithCondition(ownerRefStableCondition))
		}
		tc.EnsureResourceExists(opts...)
	}

	// Wait for quiescence: all resourceVersions must stop changing for 30s
	// before we treat the system as settled. This accounts for our reconciler,
	// external controllers (e.g. service-ca), and any watch-triggered re-applies.
	lastVersions := make(map[string]string, len(resources))
	g.Eventually(func(g Gomega) {
		stable := true
		for _, r := range resources {
			if !r.resourceVersion {
				continue
			}
			u := tc.FetchResource(
				WithMinimalObject(r.gvk, types.NamespacedName{Name: r.name, Namespace: r.ns}),
			)
			g.Expect(u).NotTo(BeNil(), "%s should exist", r.desc)

			rv := u.GetResourceVersion()
			if prev, ok := lastVersions[r.name]; !ok || prev != rv {
				lastVersions[r.name] = rv
				stable = false
			}
		}
		g.Expect(stable).To(BeTrue(), "resources are still being modified, waiting for quiescence")
	}).WithTimeout(2 * time.Minute).WithPolling(10 * time.Second).Should(Succeed())

	// Capture baselines now that the system has quiesced.
	initialVersions := make(map[string]string, len(resources))
	for _, r := range resources {
		if !r.resourceVersion {
			continue
		}
		u := tc.FetchResource(
			WithMinimalObject(r.gvk, types.NamespacedName{Name: r.name, Namespace: r.ns}),
		)
		initialVersions[r.name] = u.GetResourceVersion()
	}

	// Assert stability: nothing should change for 1 minute.
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
	tc = tc.WithT(t)

	t.Run("Group 3: OpenTelemetry Collector", func(t *testing.T) {
		tc = tc.WithT(t)

		t.Run("Test OpenTelemetry Collector Configurations", tc.ValidateOpenTelemetryCollectorConfigurations)
		// The configuration subtests reset optional Monitoring fields after each
		// case, so restore metrics before validating the signal-specific contract.
		tc.setupMetrics(t)
		t.Run("Test signal-specific collector contract", tc.ValidateSignalSpecificCollectorContract)
		t.Run("Test OpenTelemetry Collector replicas", tc.ValidateMonitoringCRCollectorReplicas)
		t.Run("Test Metrics TLS is always enabled for Prometheus exporter", tc.ValidateMetricsTLSAlwaysEnabled)
		t.Run("Test Monitoring TLS is always enabled for internal telemetry exporter", tc.ValidateMonitoringTLSAlwaysEnabled)
	})
}

// ValidateOpenTelemetryCollectorConfigurations consolidates all OpenTelemetry Collector configuration tests.
func (tc *MonitoringTestCtx) ValidateOpenTelemetryCollectorConfigurations(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

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
				(.spec.config.service.pipelines."metrics/scrape".exporters | length == 3 and contains(["prometheus", "debug", "%s"])) and
				(.spec.config.service.pipelines."metrics/otlp".exporters | length == 3 and contains(["prometheus", "debug", "%s"]))
			`, OtlpCustomExporter, OtlpCustomExporter, OtlpCustomExporter),
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
			tc = tc.WithT(t)
			t.Cleanup(tc.resetMonitoringConfigToMetrics)

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
	tc = tc.WithT(t)

	defaultReplicas := tc.expectedDefaultReplicas
	testReplicas := defaultReplicas + 1

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
		withNoCollectorReplicas(),
	)

	collectorCR := WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
		Name:      OpenTelemetryCollectorName,
		Namespace: tc.MonitoringNamespace,
	})

	tc.EnsureResourceExists(
		collectorCR,
		WithCondition(jq.Match(`.spec.replicas == %d`, defaultReplicas)),
		WithCustomErrorMsg("OTel Collector should default to %d replicas based on node count", defaultReplicas),
	)

	tc.updateMonitoringConfig(jq.Transform(`.spec.collectorReplicas = %d`, testReplicas))

	tc.EnsureResourceExists(
		collectorCR,
		WithCondition(jq.Match(`.spec.replicas == %d`, testReplicas)),
		WithCustomErrorMsg("OTel Collector replicas should be updated to %d", testReplicas),
	)
}

// ValidateMetricsTLSAlwaysEnabled validates that TLS is always enabled for the OpenTelemetry Collector Prometheus exporter.
func (tc *MonitoringTestCtx) ValidateMetricsTLSAlwaysEnabled(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

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

// ValidateMonitoringTLSAlwaysEnabled validates that TLS is always enabled for the
// collector's internal telemetry (:8890) monitoring path.
func (tc *MonitoringTestCtx) ValidateMonitoringTLSAlwaysEnabled(t *testing.T) {
	t.Helper()

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.ensureOpenTelemetryCollectorReady(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Service, types.NamespacedName{
			Name:      "data-science-collector-monitoring",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.metadata.annotations."service.beta.openshift.io/serving-cert-secret-name" == "data-science-collector-monitoring-tls"`),
			jq.Match(`.spec.ports[0].name == "monitoring"`),
			jq.Match(`.spec.ports[0].port == 8890`),
		)),
		WithCustomErrorMsg("TLS Service for internal telemetry exporter should exist with service-ca annotation"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Secret, types.NamespacedName{
			Name:      "data-science-collector-monitoring-tls",
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
			jq.Match(`.spec.config.exporters."prometheus/monitoring".endpoint == "0.0.0.0:8890"`),
			jq.Match(`.spec.config.exporters."prometheus/monitoring".tls.cert_file == "/etc/otel-collector/monitoring-tls/tls.crt"`),
			jq.Match(`.spec.config.exporters."prometheus/monitoring".tls.key_file == "/etc/otel-collector/monitoring-tls/tls.key"`),
			jq.Match(`.spec.config.receivers."prometheus/self".config.scrape_configs[0].static_configs[0].targets[0] == "127.0.0.1:8888"`),
			jq.Match(`.spec.config.service.pipelines."metrics/internal".receivers | contains(["prometheus/self"])`),
			jq.Match(`.spec.config.service.pipelines."metrics/internal".exporters | contains(["prometheus/monitoring"])`),
			jq.Match(`.spec.volumes[] | select(.name == "monitoring-tls-certs") | .secret.secretName == "data-science-collector-monitoring-tls"`),
		)),
		WithCustomErrorMsg("OpenTelemetryCollector should self-scrape internal telemetry and re-export it over TLS on :8890"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ServiceMonitor, types.NamespacedName{
			Name:      "data-science-collector-monitor",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.endpoints[0].port == "monitoring"`),
			jq.Match(`.spec.endpoints[0].scheme == "https"`),
			jq.Match(`.spec.endpoints[0].tlsConfig.serverName == "data-science-collector-monitoring.%s.svc"`, tc.MonitoringNamespace),
		)),
		WithCustomErrorMsg("ServiceMonitor should always use HTTPS to scrape the internal telemetry exporter"),
	)
}

// ========================================================================
// Group 4: Target Allocator
// ========================================================================

func (tc *MonitoringTestCtx) runTargetAllocatorTests(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	t.Run("Group 4: Target Allocator", func(t *testing.T) {
		tc = tc.WithT(t)

		t.Run("With Metrics", func(t *testing.T) {
			tc = tc.WithT(t)

			t.Run("Test Target Allocator deployment with metrics", tc.ValidateTargetAllocatorDeploymentWithMetrics)
			t.Run("Test Target Allocator Service and ConfigMap", tc.ValidateTargetAllocatorServiceAndConfigMap)
			t.Run("Test Target Allocator RBAC configuration", tc.ValidateTargetAllocatorRBACConfiguration)
		})
	})
}

// ValidateTargetAllocatorNotDeployedWithoutMetrics tests that the Target Allocator is not deployed when metrics are not configured.
func (tc *MonitoringTestCtx) ValidateTargetAllocatorNotDeployedWithoutMetrics(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

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
	tc = tc.WithT(t)
	t.Cleanup(tc.resetMonitoringConfigToMetrics)

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
			jq.Match(`.spec.targetAllocator.mtls.enabled == true`),
			jq.Match(`.spec.targetAllocator.mtls.useCertManager == true`),
			jq.Match(`.spec.targetAllocator.allowInsecureAuthSecrets == false`),
			jq.Match(`.spec.targetAllocator.prometheusCR.enabled == true`),
			jq.Match(`.spec.targetAllocator.prometheusCR.denyFSAccessThroughSMs == true`),
			jq.Match(`.spec.targetAllocator.prometheusCR.secretNamespaces | contains(["%s"])`, tc.MonitoringNamespace),
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

	for _, certificateName := range []string{
		TargetAllocatorCACertificateName,
		TargetAllocatorClientCertificateName,
		TargetAllocatorServerCertificateName,
	} {
		tc.EnsureResourceExists(
			WithMinimalObject(gvk.CertManagerCertificate, types.NamespacedName{
				Name:      certificateName,
				Namespace: tc.MonitoringNamespace,
			}),
			WithCondition(jq.Match(`[.status.conditions[] | select(.type == "Ready" and .status == "True")] | length == 1`)),
			WithCustomErrorMsg(fmt.Sprintf("Target Allocator certificate %s should be Ready", certificateName)),
		)
	}
}

// ValidateTargetAllocatorServiceAndConfigMap tests that the Target Allocator Service and ConfigMap are created correctly.
func (tc *MonitoringTestCtx) ValidateTargetAllocatorServiceAndConfigMap(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(tc.resetMonitoringConfigToMetrics)

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
			And(
				jq.Match(`.spec.ports[] | select(.name == "targetallocation") | .port == 80`),
				jq.Match(`.spec.ports[] | select(.name == "targetallocation-https") | .port == 443`),
			),
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

	tc.g.Eventually(func() error {
		configMap := &corev1.ConfigMap{}
		if err := tc.Client().Get(tc.Context(), types.NamespacedName{
			Name:      TargetAllocatorDeploymentName,
			Namespace: tc.MonitoringNamespace,
		}, configMap); err != nil {
			return err
		}
		configYAML := configMap.Data["targetallocator.yaml"]
		if configYAML == "" {
			return errors.New("TargetAllocator config is not populated yet")
		}
		var config map[string]any
		if err := yaml.Unmarshal([]byte(configYAML), &config); err != nil {
			return fmt.Errorf("decode generated TargetAllocator config: %w", err)
		}
		prometheusConfig, ok := config["config"].(map[string]any)
		if !ok {
			return errors.New("generated TargetAllocator config has no Prometheus config")
		}
		scrapeConfigs, ok := prometheusConfig["scrape_configs"].([]any)
		if !ok {
			return errors.New("generated TargetAllocator config has no scrape_configs")
		}
		for _, rawScrapeConfig := range scrapeConfigs {
			scrapeConfig, ok := rawScrapeConfig.(map[string]any)
			if !ok || scrapeConfig["job_name"] != "dcgm-exporter-accelerator-metrics" {
				continue
			}
			if scrapeConfig["scheme"] != "http" {
				return fmt.Errorf("TargetAllocator-generated DCGM job scheme=%v, want http", scrapeConfig["scheme"])
			}
			return nil
		}
		return errors.New("TargetAllocator-generated config is missing the built-in DCGM job")
	}).WithTimeout(2*time.Minute).WithPolling(5*time.Second).Should(Succeed(),
		"TargetAllocator must preserve the built-in DCGM scrape job in generated config")
}

// ValidateTargetAllocatorLifecycle tests the complete lifecycle of Target Allocator deployment and cleanup.
func (tc *MonitoringTestCtx) ValidateTargetAllocatorLifecycle(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

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

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      OpenTelemetryCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
	)
	tc.waitForPrometheusPodsGone(t)

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
	tc = tc.WithT(t)
	t.Cleanup(tc.resetMonitoringConfigToMetrics)

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
			jq.Match(`.rules[] | select((.apiGroups // []) | contains(["monitoring.coreos.com"])) | .resources | contains(["podmonitors", "servicemonitors"])`),
			jq.Match(`.rules[] | select((.apiGroups // []) | contains(["monitoring.coreos.com"])) | .verbs | contains(["get", "list", "watch"])`),
			jq.Match(`[.rules[] | select((.apiGroups // []) | contains([""])) | select(.resources | contains(["secrets"]))] | length == 0`),
			jq.Match(`.rules[] | select((.apiGroups // []) | contains([""])) | .resources | contains(["endpoints"])`),
			jq.Match(`.rules[] | select((.apiGroups // []) | contains([""])) | .verbs | contains(["get", "list", "watch"])`),
			jq.Match(`.rules[] | select((.apiGroups // []) | contains(["discovery.k8s.io"])) | .resources | contains(["endpointslices"])`),
			jq.Match(`.rules[] | select((.apiGroups // []) | contains(["discovery.k8s.io"])) | .verbs | contains(["get", "list", "watch"])`),
		)),
		WithCustomErrorMsg("collector discovery ClusterRole should not grant Secret access"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{
			Name: "generate-processors-targetallocator-rolebinding",
		}),
		WithCondition(And(
			jq.Match(`.roleRef.name == "generate-processors-role"`),
			jq.Match(`.subjects[0].name == "%s"`, TargetAllocatorServiceAccount),
			jq.Match(`.subjects[0].namespace == "%s"`, tc.MonitoringNamespace),
		)),
		WithCustomErrorMsg("legacy TargetAllocator ClusterRoleBinding should retain its original role"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{
			Name: "generate-processors-targetallocator-discovery-rolebinding",
		}),
		WithCondition(And(
			jq.Match(`.roleRef.name == "generate-processors-targetallocator-role"`),
			jq.Match(`.subjects[0].name == "%s"`, TargetAllocatorServiceAccount),
			jq.Match(`.subjects[0].namespace == "%s"`, tc.MonitoringNamespace),
		)),
		WithCustomErrorMsg("TargetAllocator discovery ClusterRoleBinding should grant the dedicated role"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRole, types.NamespacedName{
			Name: "generate-processors-targetallocator-role",
		}),
		WithCondition(And(
			jq.Match(`.rules[] | select((.apiGroups // []) | contains(["monitoring.coreos.com"])) | .resources | contains(["podmonitors", "servicemonitors", "scrapeconfigs"])`),
			jq.Match(`.rules[] | select((.apiGroups // []) | contains(["monitoring.coreos.com"])) | .verbs | contains(["get", "list", "watch"])`),
			jq.Match(`[.rules[] | select((.apiGroups // []) | contains(["monitoring.coreos.com"])) | .resources | contains(["probes"])] | any`),
			jq.Match(`[.rules[] | select((.apiGroups // []) | contains(["monitoring.coreos.com"])) | .verbs | contains(["get", "list", "watch"])] | any`),
			jq.Match(`.rules[] | select((.apiGroups // []) | contains([""])) | .resources | contains(["nodes", "nodes/metrics", "services", "endpoints", "pods", "namespaces"])`),
			jq.Match(`.rules[] | select((.apiGroups // []) | contains([""])) | .verbs | contains(["get", "list", "watch"])`),
			jq.Match(`[.rules[] | select((.apiGroups // []) | contains([""])) | .resources | contains(["configmaps"])] | any`),
			jq.Match(`.rules[] | select((.apiGroups // []) | contains(["discovery.k8s.io"])) | .resources | contains(["endpointslices"])`),
			jq.Match(`.rules[] | select((.apiGroups // []) | contains(["networking.k8s.io"])) | .resources | contains(["ingresses"])`),
			jq.Match(`.rules[] | select(.nonResourceURLs != null) | .nonResourceURLs | contains(["/metrics", "/api", "/api/*", "/apis", "/apis/*"])`),
		)),
		WithCustomErrorMsg("TargetAllocator ClusterRole should grant the OpenTelemetry Operator discovery permissions"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Role, types.NamespacedName{
			Name:      "data-science-collector-targetallocator-secrets",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`.rules[] | select(.resources | contains(["secrets"])) | .verbs | contains(["get", "list", "watch"])`)),
		WithCustomErrorMsg("TargetAllocator namespace Role should grant namespaced Secret reads"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.RoleBinding, types.NamespacedName{
			Name:      "data-science-collector-targetallocator-secrets",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.roleRef.name == "data-science-collector-targetallocator-secrets"`),
			jq.Match(`.subjects[0].name == "%s"`, TargetAllocatorServiceAccount),
			jq.Match(`.subjects[0].namespace == "%s"`, tc.MonitoringNamespace),
		)),
		WithCustomErrorMsg("TargetAllocator Secret RoleBinding should use the dedicated ServiceAccount"),
	)
}

// ========================================================================
// Group 5: Thanos Querier
// ========================================================================

func (tc *MonitoringTestCtx) runThanosQuerierTests(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	t.Run("Group 5: Thanos Querier", func(t *testing.T) {
		tc = tc.WithT(t)
		t.Run("Test ThanosQuerier deployment with metrics", tc.ValidateThanosQuerierDeployment)
		t.Run("Test ThanosQuerier route authorization boundary", tc.ValidateThanosQuerierRouteAuthorizationBoundary)
		t.Run("Test ThanosQuerier route namespace isolation", tc.ValidateThanosQuerierRouteNamespaceIsolation)
		t.Run("Test Prometheus NetworkPolicy allows Thanos Querier on gRPC port", tc.ValidatePrometheusNetworkPolicyAllowsThanosQuerier)

		t.Run("Component Isolation — Thanos", func(t *testing.T) {
			t.Run("Test Thanos baseline fixture — record rollout identity and health", tc.ValidateThanosComponentBaselineFixture)
			t.Run("Test Thanos version boundary — UNSUPPORTED (no operand pinning), reported as SKIP", tc.ValidateThanosComponentVersionBoundaryUnsupported)
			t.Run("Test Thanos component health check with failure-attribution diagnostics", tc.ValidateThanosComponentHealthWithDiagnostics)
		})

		t.Run("Integration — Thanos ⇄ Perses datasource wiring", tc.ValidateThanosPersesDatasourceIntegration)
	})
}

// ValidateThanosQuerierNotDeployedWithoutMetrics tests that ThanosQuerier is not deployed when metrics are not configured.
func (tc *MonitoringTestCtx) ValidateThanosQuerierNotDeployedWithoutMetrics(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

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
	tc = tc.WithT(t)

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
			tc.monitoringOwnerReferencesCondition(),
		)),
		WithCustomErrorMsg("ThanosQuerier CR should be created when metrics are configured"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Route, types.NamespacedName{Name: ThanosQuerierRouteName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.spec.to.name == "%s"`, ThanosQuerierProxyName),
			jq.Match(`.spec.port.targetPort == "https"`),
			jq.Match(`.spec.tls.termination == "reencrypt"`),
			jq.Match(`.spec.tls.destinationCACertificate != null and (.spec.tls.destinationCACertificate | length) > 0`),
			jq.Match(`.spec.tls.insecureEdgeTerminationPolicy == "Redirect"`),
			jq.Match(`.metadata.labels.app == "thanos-querier"`),
			jq.Match(`.metadata.labels."app.kubernetes.io/name" == "thanos-querier"`),
			jq.Match(`.metadata.labels."app.kubernetes.io/component" == "querier"`),
			jq.Match(`.metadata.labels."app.kubernetes.io/part-of" == "data-science-monitoring"`),
			tc.monitoringOwnerReferencesCondition(),
		)),
		WithCustomErrorMsg("ThanosQuerier Route should be created when metrics are configured"),
	)
}

// ValidateThanosQuerierRouteAuthorizationBoundary verifies that the external
// Thanos route terminates at the authenticated, namespace-restricted proxy
// rather than directly at the ThanosQuerier Service.
func (tc *MonitoringTestCtx) ValidateThanosQuerierRouteAuthorizationBoundary(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{
			Name:      ThanosQuerierProxyName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.status.readyReplicas == 1`),
			jq.Match(`.spec.template.spec.serviceAccountName == "%s"`, ThanosQuerierProxyName),
			jq.Match(`.spec.template.spec.containers | map(.name) | contains(["kube-rbac-proxy", "prom-label-proxy"])`),
		)),
		WithCustomErrorMsg("Thanos Querier proxy deployment should be ready with both authorization containers"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ServiceAccount, types.NamespacedName{
			Name:      ThanosQuerierProxyName,
			Namespace: tc.MonitoringNamespace,
		}),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{
			Name: ThanosQuerierProxyName,
		}),
		WithCondition(And(
			jq.Match(`.roleRef.name == "cluster-monitoring-view"`),
			jq.Match(`.subjects[0].name == "%s"`, ThanosQuerierProxyName),
			jq.Match(`.subjects[0].namespace == "%s"`, tc.MonitoringNamespace),
		)),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{
			Name: ThanosQuerierProxyName + "-auth-delegator",
		}),
		WithCondition(And(
			jq.Match(`.roleRef.name == "system:auth-delegator"`),
			jq.Match(`.subjects[0].name == "%s"`, ThanosQuerierProxyName),
		)),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ConfigMap, types.NamespacedName{
			Name:      ThanosQuerierProxyName + "-config",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.data."kube-rbac-proxy.yaml" | contains("byQueryParameter")`),
			jq.Match(`.data."kube-rbac-proxy.yaml" | contains("metrics.k8s.io")`),
			jq.Match(`.data."kube-rbac-proxy.yaml" | contains("resource: pods")`),
		)),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Service, types.NamespacedName{
			Name:      ThanosQuerierProxyName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.ports[0].name == "https"`),
			jq.Match(`.spec.ports[0].port == 8443`),
			jq.Match(`.metadata.annotations."service.beta.openshift.io/serving-cert-secret-name" == "%s-tls"`, ThanosQuerierProxyName),
		)),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
			Name:      "data-science-thanos-querier-proxy-ingress",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.podSelector.matchLabels.app == "%s"`, ThanosQuerierProxyName),
			jq.Match(`.spec.ingress[0].from[0].namespaceSelector.matchLabels["policy-group.network.openshift.io/ingress"] == ""`),
			jq.Match(`.spec.ingress[0].ports[0].port == 8443`),
		)),
	)
}

// ValidatePrometheusNetworkPolicyAllowsThanosQuerier tests that the Prometheus instance NetworkPolicy
// includes an ingress rule allowing the Thanos Querier to reach the Thanos Sidecar on gRPC port 10901.
func (tc *MonitoringTestCtx) ValidatePrometheusNetworkPolicyAllowsThanosQuerier(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	namespaceProxyIngress := `[.spec.ingress[] |
  select([.from[]?.podSelector.matchLabels.app]
    | index("data-science-prometheus-namespace-proxy") != null) |
  .ports[] |
  select(.protocol == "TCP" and .port == 9090)] | length == 1`
	clusterProxyIngress := `[.spec.ingress[] |
  select([.from[]?.podSelector.matchLabels.app]
    | index("data-science-prometheus-cluster-proxy") != null) |
  .ports[] |
  select(.protocol == "TCP" and .port == 9090)] | length == 1`
	thanosQuerierIngress := `[.spec.ingress[] |
  select(( [.from[]?.podSelector.matchLabels]
    | map(select(.["app.kubernetes.io/part-of"] == "ThanosQuerier"
      and .["app.kubernetes.io/managed-by"] == "observability-operator"))
    | length) > 0) |
  .ports[] |
  select(.protocol == "TCP" and .port == 10901)] | length == 1`

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
			jq.Match(`.spec.policyTypes | contains(["Ingress"])`),
			jq.Match("%s", namespaceProxyIngress),
			jq.Match("%s", clusterProxyIngress),
			jq.Match("%s", thanosQuerierIngress),
		)),
		WithCustomErrorMsg("Prometheus NetworkPolicy should allow Thanos Querier ingress on gRPC port 10901"),
	)
}

func (tc *MonitoringTestCtx) assertThanosPromQLResponds(t *testing.T, query string) string {
	t.Helper()
	g := NewWithT(t)

	var host string
	g.Eventually(func() (string, error) {
		route := tc.FetchResource(
			WithMinimalObject(gvk.Route, types.NamespacedName{Name: ThanosQuerierRouteName, Namespace: tc.MonitoringNamespace}),
		)
		if route == nil {
			return "", fmt.Errorf("Route %s not found", ThanosQuerierRouteName)
		}
		var ingressHost string
		if ingress, found, _ := unstructured.NestedSlice(route.Object, "status", "ingress"); found && len(ingress) > 0 {
			if entry, ok := ingress[0].(map[string]any); ok {
				ingressHost, _, _ = unstructured.NestedString(entry, "host")
			}
		}
		if ingressHost == "" {
			// Fall back to spec.host for clusters where the admission plugin writes it back.
			ingressHost, _, _ = unstructured.NestedString(route.Object, "spec", "host")
		}
		host = ingressHost
		return host, nil
	}).ShouldNot(BeEmpty(),
		"Component=Thanos Assertion=promql-response: Route %s has no ingress host", ThanosQuerierRouteName)

	token := authorizedThanosProbeToken(t, tc,
		"thanos-baseline-"+strconv.FormatInt(time.Now().UnixNano(), 10), true)

	endpoint := url.URL{Scheme: "https", Host: host, Path: "/api/v1/query"}
	parameters := endpoint.Query()
	parameters.Set("query", query)
	parameters.Set("namespace", tc.MonitoringNamespace)
	endpoint.RawQuery = parameters.Encode()
	httpClient := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    clusterIngressCAPool(t, tc),
		}},
		Timeout: 30 * time.Second,
	}

	var seriesCount int
	g.Eventually(func() error {
		req, err := http.NewRequestWithContext(tc.Context(), http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			httpErr := fmt.Errorf("PromQL query %q returned HTTP %d (want 200)", query, resp.StatusCode)
			if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
				return httpErr
			}
			return StopTrying(httpErr.Error())
		}

		var parsed struct {
			Status string `json:"status"`
			Data   struct {
				ResultType string            `json:"resultType"`
				Result     []json.RawMessage `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &parsed); err != nil {
			return fmt.Errorf("PromQL response for %q is not valid JSON: %w", query, err)
		}
		if parsed.Status != "success" {
			return fmt.Errorf("PromQL query %q returned status=%q (want success)", query, parsed.Status)
		}
		if parsed.Data.ResultType != "vector" {
			return fmt.Errorf("PromQL query %q returned resultType=%q (want vector)", query, parsed.Data.ResultType)
		}
		if len(parsed.Data.Result) == 0 {
			return fmt.Errorf("PromQL query %q returned 0 series", query)
		}
		seriesCount = len(parsed.Data.Result)
		return nil
	}).WithTimeout(2*time.Minute).WithPolling(5*time.Second).Should(Succeed(),
		"Component=Thanos Assertion=promql-response: authenticated PromQL query did not return a successful result")

	return fmt.Sprintf("query=%q status=success series=%d", query, seriesCount)
}

func (tc *MonitoringTestCtx) ValidateThanosComponentBaselineFixture(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.ensureMetricsManaged()

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(jq.Match(
			`.status.conditions[] | select(.type == "%s") | .status == "%s"`,
			conditions.ConditionThanosQuerierAvailable, metav1.ConditionTrue,
		)),
		WithCustomErrorMsg("Component=Thanos: ThanosQuerierAvailable condition did not become True — Thanos failed independently of Perses/Prometheus"),
	)

	thanosRollout := tc.recordRolloutIdentity(t, gvk.ThanosQuerier, ThanosQuerierName)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{Name: thanosQuerierDeploymentName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.status.readyReplicas >= 1`),
			jq.Match(`.status.readyReplicas == .status.replicas`),
		)),
		WithCustomErrorMsg("Component=Thanos Assertion=query-readiness: Thanos Deployment %s has no ready replica", thanosQuerierDeploymentName),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Route, types.NamespacedName{Name: ThanosQuerierRouteName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.spec.to.name == "%s"`, ThanosQuerierProxyName),
			jq.Match(`.spec.port.targetPort == "https"`),
			jq.Match(`.spec.tls.termination == "reencrypt"`),
		)),
		WithCustomErrorMsg("Component=Thanos Assertion=promql-endpoint: Route %s does not publish the Thanos query endpoint", ThanosQuerierRouteName),
	)

	promqlResult := tc.assertThanosPromQLResponds(t, "up")

	t.Logf(
		"✓ THANOS BASELINE RECORDED (independent of Perses)\n"+
			"  • Component:          Thanos Querier\n"+
			"  • Rollout identity:   %s\n"+
			"  • CR status:          ThanosQuerierAvailable=True\n"+
			"  • Query readiness:    Deployment %s ready replicas up\n"+
			"  • PromQL endpoint:    Route %s → %s (reencrypt TLS)\n"+
			"  • PromQL response:    %s\n",
		thanosRollout, thanosQuerierDeploymentName, ThanosQuerierRouteName, ThanosQuerierProxyName, promqlResult)
}

func (tc *MonitoringTestCtx) ValidateThanosPersesDatasourceIntegration(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.ensureMetricsManaged()

	expectedThanosURL := fmt.Sprintf("http://%s.%s.svc.cluster.local:10902", thanosQuerierDeploymentName, tc.MonitoringNamespace)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.PersesDatasource, types.NamespacedName{Name: PersesDatasourceName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.spec.config.plugin.spec.proxy.kind == "HTTPProxy"`),
			jq.Match(`.spec.config.plugin.spec.proxy.spec.url == "%s"`, expectedThanosURL),
		)),
		WithCustomErrorMsg("Integration=Thanos⇄Perses Assertion=dependent-dashboards: "+
			"PersesDatasource %s does not route through HTTPProxy to Thanos Querier at %s", PersesDatasourceName, expectedThanosURL),
	)

	t.Logf(
		"✓ THANOS ⇄ PERSES DATASOURCE WIRING OK\n"+
			"  • Dependent dashboards: PersesDatasource %s → HTTPProxy → %s\n",
		PersesDatasourceName, expectedThanosURL)
}

func (tc *MonitoringTestCtx) ValidateThanosComponentVersionBoundaryUnsupported(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	rollout := tc.recordRolloutIdentity(t, gvk.ThanosQuerier, ThanosQuerierName)

	t.Skipf(
		"LIMITATION (Component=Thanos): independent version-boundary testing is UNSUPPORTED and no "+
			"version boundary is exercised here. COO packages Perses, Thanos and MonitoringStack under a "+
			"single operator version, so the Thanos operand version cannot be pinned or swapped "+
			"independently. Current rollout identity: %s. Reported as SKIP (not PASS) so this is not "+
			"mistaken for boundary coverage; revisit once operand pinning is available.",
		rollout,
	)
}

func (tc *MonitoringTestCtx) ValidateThanosComponentHealthWithDiagnostics(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.ensureMetricsManaged()

	attribution := formatFailureAttribution(failureAttribution{
		component: "Thanos",
		rollout:   tc.recordRolloutIdentity(t, gvk.ThanosQuerier, ThanosQuerierName),
		assertion: fmt.Sprintf("Deployment %s ready replicas", thanosQuerierDeploymentName),
		diagnostics: []string{
			fmt.Sprintf("oc -n %s logs deployment/%s", tc.MonitoringNamespace, thanosQuerierDeploymentName),
			fmt.Sprintf("oc -n %s describe deployment %s", tc.MonitoringNamespace, thanosQuerierDeploymentName),
			fmt.Sprintf("oc -n %s get thanosquerier %s -o yaml", tc.MonitoringNamespace, ThanosQuerierName),
		},
	})

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{Name: thanosQuerierDeploymentName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.status.readyReplicas >= 1`),
			jq.Match(`.status.readyReplicas == .status.replicas`),
		)),
		WithCustomErrorMsg(attribution),
	)

	t.Logf("✓ Thanos health check passed; failure-attribution diagnostics wired:\n%s", attribution)
}

// ========================================================================
// Group 6: Traces with PV Backend
// ========================================================================

func (tc *MonitoringTestCtx) runTracesWithPVBackendTests(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	t.Run("Group 6: Traces with PV Backend", func(t *testing.T) {
		tc = tc.WithT(t)
		t.Cleanup(func() {
			tc.cleanupGroup(t, "")
		})

		t.Run("Test TempoMonolithic CR Creation with PV backend", tc.ValidateTempoMonolithicCRCreation)
		t.Run("Test Collector MLflow integration RBAC", tc.ValidateCollectorMLflowIntegrationRBAC)
		t.Run("Test Collector Tempo trace export RBAC", tc.ValidateCollectorTempoTraceExportRBAC)
		t.Run("Test Tempo gateway tenant and authentication wiring", tc.ValidateTempoGatewayTenantConfiguration)
	})
}

// ValidateTempoMonolithicCRCreation tests creation of TempoMonolithic CR with PV backend and custom retention.
func (tc *MonitoringTestCtx) ValidateTempoMonolithicCRCreation(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

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

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
			Name:      "data-science-tempomonolithic-ingress",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.podSelector.matchLabels."app.kubernetes.io/name" == "tempo-monolithic"`),
			jq.Match(`.spec.podSelector.matchLabels."app.kubernetes.io/instance" == "data-science-tempomonolithic"`),
			jq.Match(`.spec.policyTypes == ["Ingress"]`),
			jq.Match(`[.spec.ingress[] | select((.from // []) | length == 0)] | length == 0`),
			jq.Match(`[.spec.ingress[].ports[].port] | unique | sort == [3200, 4317, 4318, 8080, 8090]`),
		)),
		WithCustomErrorMsg("TempoMonolithic NetworkPolicy should expose only the collector, Prometheus, and Perses paths"),
	)
}

// ValidateCollectorMLflowIntegrationRBAC tests that the collector SA has the MLflow trace export ClusterRole and ClusterRoleBinding.
func (tc *MonitoringTestCtx) ValidateCollectorMLflowIntegrationRBAC(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withMonitoringTraces(TracesStorageBackendPV, "", TracesStorageSize1Gi, DefaultRetention),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRole, types.NamespacedName{
			Name: "data-science-collector-mlflow-trace-export",
		}),
		WithCondition(And(
			jq.Match(`.rules | length == 1`),
			jq.Match(`(.rules[0].apiGroups | length) == 1`),
			jq.Match(`.rules[0].apiGroups[0] == "mlflow.kubeflow.org"`),
			jq.Match(`(.rules[0].resources | length) == 1`),
			jq.Match(`.rules[0].resources[0] == "experiments"`),
			jq.Match(`(.rules[0].verbs | length) == 1`),
			jq.Match(`.rules[0].verbs[0] == "update"`),
		)),
		WithCustomErrorMsg("ClusterRole should grant only experiments update on mlflow.kubeflow.org"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{
			Name: "data-science-collector-mlflow-trace-export",
		}),
		WithCondition(And(
			jq.Match(`.roleRef.apiGroup == "rbac.authorization.k8s.io"`),
			jq.Match(`.roleRef.kind == "ClusterRole"`),
			jq.Match(`.roleRef.name == "data-science-collector-mlflow-trace-export"`),
			jq.Match(`(.subjects | length) == 1`),
			jq.Match(`.subjects[0].kind == "ServiceAccount"`),
			jq.Match(`.subjects[0].name == "%s"`, CollectorServiceAccount),
			jq.Match(`.subjects[0].namespace == "%s"`, tc.MonitoringNamespace),
		)),
		WithCustomErrorMsg("ClusterRoleBinding should bind collector SA to MLflow trace export ClusterRole"),
	)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withNoTraces(),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.ClusterRole, types.NamespacedName{
			Name: "data-science-collector-mlflow-trace-export",
		}),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{
			Name: "data-science-collector-mlflow-trace-export",
		}),
	)
}

// ValidateCollectorTempoTraceExportRBAC tests that the collector SA has the Tempo trace export ClusterRole and ClusterRoleBinding.
func (tc *MonitoringTestCtx) ValidateCollectorTempoTraceExportRBAC(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withMonitoringTraces(TracesStorageBackendPV, "", TracesStorageSize1Gi, DefaultRetention),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRole, types.NamespacedName{
			Name: "data-science-collector-tempo-trace-export",
		}),
		WithCondition(And(
			jq.Match(`.rules | length == 1`),
			jq.Match(`(.rules[0].apiGroups | length) == 1`),
			jq.Match(`.rules[0].apiGroups[0] == "tempo.grafana.com"`),
			jq.Match(`(.rules[0].resources | length) == 1`),
			jq.Match(`.rules[0].resources[0] == "%s"`, tc.MonitoringNamespace),
			jq.Match(`(.rules[0].resourceNames | length) == 1`),
			jq.Match(`.rules[0].resourceNames[0] == "traces"`),
			jq.Match(`(.rules[0].verbs | length) == 1`),
			jq.Match(`.rules[0].verbs[0] == "create"`),
		)),
		WithCustomErrorMsg("ClusterRole should grant traces create on tempo.grafana.com for the monitoring namespace"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{
			Name: "data-science-collector-tempo-trace-export",
		}),
		WithCondition(And(
			jq.Match(`.roleRef.apiGroup == "rbac.authorization.k8s.io"`),
			jq.Match(`.roleRef.kind == "ClusterRole"`),
			jq.Match(`.roleRef.name == "data-science-collector-tempo-trace-export"`),
			jq.Match(`(.subjects | length) == 1`),
			jq.Match(`.subjects[0].kind == "ServiceAccount"`),
			jq.Match(`.subjects[0].name == "%s"`, CollectorServiceAccount),
			jq.Match(`.subjects[0].namespace == "%s"`, tc.MonitoringNamespace),
		)),
		WithCustomErrorMsg("ClusterRoleBinding should bind collector SA to Tempo trace export ClusterRole"),
	)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withNoTraces(),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.ClusterRole, types.NamespacedName{
			Name: "data-science-collector-tempo-trace-export",
		}),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{
			Name: "data-science-collector-tempo-trace-export",
		}),
	)
}

// ========================================================================
// Group 7: Traces with Cloud Storage (S3 & GCS)
// ========================================================================

func (tc *MonitoringTestCtx) runTracesWithCloudStorageTests(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	t.Run("Group 7: Traces with Cloud Storage", func(t *testing.T) {
		tc = tc.WithT(t)
		t.Cleanup(func() {
			for _, secretName := range []string{"s3-secret", "gcs-secret"} {
				tc.DeleteResource(
					WithMinimalObject(gvk.Secret, types.NamespacedName{Name: secretName, Namespace: tc.MonitoringNamespace}),
					WithIgnoreNotFound(true),
					WithWaitForDeletion(true),
				)
			}
		})
		t.Cleanup(func() { tc.cleanupGroup(t, "") })

		t.Run("Test TempoStack CR Creation with Cloud Storage", tc.ValidateTempoStackCRCreationWithCloudStorage)
		t.Run("Test Instrumentation CR Traces lifecycle", tc.ValidateInstrumentationCRTracesLifecycle)
		t.Run("Test Traces Exporters Reserved Name Validation", tc.ValidateTracesExportersReservedNameValidation)
	})
}

// ValidateTempoStackCRCreationWithCloudStorage tests creation of TempoStack CR with cloud storage backends.
func (tc *MonitoringTestCtx) ValidateTempoStackCRCreationWithCloudStorage(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

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
			tc = tc.WithT(t)
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
	tc = tc.WithT(t)

	secretName := fmt.Sprintf("%s-secret", backend)
	t.Cleanup(func() {
		switch backend {
		case TracesStorageBackendS3:
			tc.cleanupSeaweedFS()
		case TracesStorageBackendGCS:
			tc.cleanupFakeGCS()
		}
	})
	t.Cleanup(func() { tc.cleanupTempoStackAndSecret(secretName) })
	t.Cleanup(tc.cleanupTracesConfiguration)

	tc.validateTempoStackCreation(t, backend, secretName, monitoringCondition, monitoringErrorMsg)
	tc.validatePersesDatasourceTLS(t, backend, secretName)
	if backend == TracesStorageBackendGCS {
		endpoint := fmt.Sprintf("http://%s.%s.svc.cluster.local:%d/storage/v1/", fakeGCSServiceName, tc.MonitoringNamespace, fakeGCSPort)
		tc.EnsureResourceExistsConsistently(
			WithMinimalObject(gvk.TempoStack, types.NamespacedName{Name: TempoStackName, Namespace: tc.MonitoringNamespace}),
			WithCondition(jq.Match(`.spec.extraConfig.tempo.storage.trace.gcs.endpoint == "%s"`, endpoint)),
			WithCustomErrorMsg("TempoStack should retain the fake GCS endpoint across monitoring reconciliation"),
		)
	}
}

// validateTempoStackCreation creates a secret, enables traces for the given backend,
// and waits until the TempoStack is ready with the correct configuration.
func (tc *MonitoringTestCtx) validateTempoStackCreation(t *testing.T, backend, secretName string, monitoringCondition gTypes.GomegaMatcher, monitoringErrorMsg string) {
	t.Helper()
	tc = tc.WithT(t)

	tc.ensureMonitoringCleanSlate(t, secretName)

	switch backend {
	case TracesStorageBackendS3:
		tc.startSeaweedFS(t, tempoS3Bucket)
	case TracesStorageBackendGCS:
		tc.startFakeGCS(t)
	default:
		t.Fatalf("unsupported Tempo storage backend %q", backend)
	}
	tc.createTempoStorageSecret(t, backend, secretName, tc.MonitoringNamespace)

	tc.updateMonitoringConfigWithoutReady(
		withManagementState(common.Managed),
		withMonitoringTraces(backend, secretName, "", DefaultRetention),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(monitoringCondition),
		WithCustomErrorMsg(monitoringErrorMsg),
	)

	tempoStack := types.NamespacedName{Name: TempoStackName, Namespace: tc.MonitoringNamespace}
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.TempoStack, tempoStack),
		WithCondition(And(
			jq.Match(`.spec.storage.secret.type == "%s"`, backend),
			jq.Match(`.spec.storage.secret.name == "%s"`, secretName),
		)),
		WithEventuallyTimeout(15*time.Minute),
	)

	if backend == TracesStorageBackendGCS {
		tc.EventuallyResourcePatched(
			WithMinimalObject(gvk.TempoStack, tempoStack),
			WithMutateFunc(jq.Transform(`.spec.extraConfig.tempo.storage.trace.gcs = {
				"endpoint": "http://%s.%s.svc.cluster.local:%d/storage/v1/",
				"insecure": true
			}`, fakeGCSServiceName, tc.MonitoringNamespace, fakeGCSPort)),
		)
	}

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.TempoStack, tempoStack),
		WithCondition(And(
			jq.Match(`.spec.storage.secret.type == "%s"`, backend),
			jq.Match(`.spec.storage.secret.name == "%s"`, secretName),
			jq.Match(`.spec.retention.global.traces == "%s"`, FormattedRetention),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
		)),
		WithEventuallyTimeout(15*time.Minute),
		WithCustomErrorMsg("TempoStack should be created by controller with %s backend", backend),
	)
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue)),
		WithEventuallyTimeout(15*time.Minute),
		WithCustomErrorMsg("Monitoring should become Ready with %s traces storage", backend),
	)
	if backend == TracesStorageBackendGCS {
		endpoint := fmt.Sprintf("http://%s.%s.svc.cluster.local:%d/storage/v1/", fakeGCSServiceName, tc.MonitoringNamespace, fakeGCSPort)
		tc.EnsureResourceExists(
			WithMinimalObject(gvk.TempoStack, tempoStack),
			WithCondition(jq.Match(`.spec.extraConfig.tempo.storage.trace.gcs.endpoint == "%s"`, endpoint)),
			WithCustomErrorMsg("TempoStack should keep the fake GCS endpoint after reconciliation"),
		)
	}
}

// validatePersesDatasourceTLS enables TLS on the existing traces configuration and validates
// that the PersesDatasource is updated with the correct TLS settings.
func (tc *MonitoringTestCtx) validatePersesDatasourceTLS(t *testing.T, backend, secretName string) {
	t.Helper()
	tc = tc.WithT(t)

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
	tc = tc.WithT(t)
	expectedRetention := DefaultRetention
	if tc.ApiMode == APIModeDSC {
		expectedRetention = FormattedRetention
	}

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withMonitoringTraces(TracesStorageBackendPV, "", "", DefaultRetention),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.spec.traces != null`),
			jq.Match(`.spec.traces.storage.retention == "%s"`, expectedRetention),
		)),
		WithCustomErrorMsg("Monitoring resource should be updated with traces configuration"),
	)

	expectedEndpoint := fmt.Sprintf("http://%s.%s.svc.cluster.local:4317", OpenTelemetryCollectorServiceName, tc.MonitoringNamespace)

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
			tc.monitoringOwnerReferencesCondition(),
		)),
		WithCustomErrorMsg("Instrumentation CR should be created with correct configuration and owner references"),
	)
}

// ValidateTracesExportersReservedNameValidation tests that reserved exporter names are rejected.
func (tc *MonitoringTestCtx) ValidateTracesExportersReservedNameValidation(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	// The reserved exporter intentionally makes Monitoring not ready. Wait for
	// its validation condition below instead of requiring the Ready phase.
	tc.updateMonitoringConfigWithoutReady(
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
	tc = tc.WithT(t)

	t.Run("Group 8: Perses", func(t *testing.T) {
		tc = tc.WithT(t)
		t.Cleanup(func() {
			tc.cleanupGroup(t, "")
		})
		tc.setupMetrics(t)

		t.Run("Perses Lifecycle", func(t *testing.T) {
			tc = tc.WithT(t)

			t.Run("Test Perses deployment when monitoring is managed", tc.ValidatePersesCRCreation)
			t.Run("Test Perses CR configuration", tc.ValidatePersesCRConfiguration)
			t.Run("Test Perses NetworkPolicy creation", tc.ValidatePersesNetworkPolicy)
		})
		// The ingress probe needs a ready Perses source pod, including when this
		// subtest is selected without the preceding Perses configuration checks.
		t.Run("Test ThanosQuerier ingress NetworkPolicy connectivity", func(t *testing.T) {
			probeCtx := tc.WithT(t)
			probeCtx.EnsureResourceExists(
				WithMinimalObject(gvk.Pod, types.NamespacedName{Name: persesPodName, Namespace: tc.MonitoringNamespace}),
				WithCondition(And(
					jq.Match(`.status.phase == "Running"`),
					jq.Match(`.status.conditions[] | select(.type == "Ready") | .status == "True"`),
				)),
				WithEventuallyTimeout(prometheusTransitionTimeout),
			)
			probeCtx.ValidateThanosQuerierIngressConnectivity(t)
		})

		t.Run("Perses Datasource with Traces", func(t *testing.T) {
			t.Run("Test Perses Datasource Creation with Traces", tc.ValidatePersesDatasourceCreationWithTraces)
			t.Run("Test Perses Datasource Configuration", tc.ValidatePersesDatasourceConfiguration)
			t.Run("Test PersesDatasource deployment with Prometheus", tc.ValidatePersesDatasourceWithPrometheus)
		})

		t.Run("Component Isolation — Perses", func(t *testing.T) {
			t.Run("Test Perses baseline fixture — record rollout identity and health", tc.ValidatePersesComponentBaselineFixture)
			t.Run("Test Perses version boundary — UNSUPPORTED (no operand pinning), reported as SKIP", tc.ValidatePersesComponentVersionBoundaryUnsupported)
			t.Run("Test Perses component health check with failure-attribution diagnostics", tc.ValidatePersesComponentHealthWithDiagnostics)
		})
	})
}

const (
	thanosQuerierDeploymentName = "thanos-querier-data-science-thanos-querier"
	persesPodName               = PersesName + "-0"
)

type failureAttribution struct {
	component   string
	rollout     string
	assertion   string
	diagnostics []string
}

func formatFailureAttribution(a failureAttribution) string {
	return fmt.Sprintf(
		"FAILURE ATTRIBUTION | Component=%s | Rollout=%s | Assertion=%s | Diagnostics: %s",
		a.component, a.rollout, a.assertion, strings.Join(a.diagnostics, " ; "),
	)
}

func (tc *MonitoringTestCtx) recordRolloutIdentity(t *testing.T, componentGVK schema.GroupVersionKind, name string) string {
	t.Helper()

	u, err := tc.fetchResource(t, componentGVK, types.NamespacedName{Name: name, Namespace: tc.MonitoringNamespace})
	if err != nil || u == nil {
		return "<absent>"
	}
	return fmt.Sprintf("resourceVersion=%s generation=%d", u.GetResourceVersion(), u.GetGeneration())
}

func (tc *MonitoringTestCtx) ensureMetricsManaged() {
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)
}

func (tc *MonitoringTestCtx) ValidatePersesComponentBaselineFixture(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.ensureMetricsManaged()

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(jq.Match(
			`.status.conditions[] | select(.type == "%s") | .status == "%s"`,
			conditions.ConditionPersesAvailable, metav1.ConditionTrue,
		)),
		WithCustomErrorMsg("Component=Perses: PersesAvailable condition did not become True — Perses failed independently of Thanos/Prometheus"),
	)

	persesRollout := tc.recordRolloutIdentity(t, gvk.Perses, PersesName)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Perses, types.NamespacedName{Name: PersesName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.spec.config.database.file != null`),
			jq.Match(`.spec.config.schemas.panels_path != null`),
			jq.Match(`.spec.containerPort == 8080`),
		)),
		WithCustomErrorMsg("Component=Perses Assertion=dashboard-config: Perses CR .spec.config (database/schemas) is not configured"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.StatefulSet, types.NamespacedName{Name: PersesName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.status.readyReplicas == 1`)),
		WithCustomErrorMsg("Component=Perses Assertion=service-readiness: Perses StatefulSet %s has no ready replica", PersesName),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Pod, types.NamespacedName{Name: persesPodName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.status.phase == "Running"`),
			jq.Match(`.status.conditions[] | select(.type == "Ready") | .status == "True"`),
		)),
		WithCustomErrorMsg("Component=Perses Assertion=api Pod=%s: API pod not Running/Ready — Perses operand unhealthy", persesPodName),
	)

	t.Logf(
		"✓ PERSES BASELINE RECORDED (independent of Thanos)\n"+
			"  • Component:        Perses\n"+
			"  • Rollout identity: %s\n"+
			"  • CR status:        PersesAvailable=True\n"+
			"  • Dashboard config: .spec.config database+schemas present\n"+
			"  • Service ready:    StatefulSet %s readyReplicas=1\n"+
			"  • API:              Pod %s Running/Ready on :8080\n",
		persesRollout, PersesName, persesPodName)
}

func (tc *MonitoringTestCtx) ValidatePersesComponentVersionBoundaryUnsupported(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	rollout := tc.recordRolloutIdentity(t, gvk.Perses, PersesName)

	t.Skipf(
		"LIMITATION (Component=Perses): independent version-boundary testing is UNSUPPORTED and no "+
			"version boundary is exercised here. COO packages Perses, Thanos and MonitoringStack under a "+
			"single operator version, so the Perses operand version cannot be pinned or swapped "+
			"independently. Current rollout identity: %s. Reported as SKIP (not PASS) so this is not "+
			"mistaken for boundary coverage; revisit once operand pinning is available.",
		rollout,
	)
}

func (tc *MonitoringTestCtx) ValidatePersesComponentHealthWithDiagnostics(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.ensureMetricsManaged()

	attribution := formatFailureAttribution(failureAttribution{
		component: "Perses",
		rollout:   tc.recordRolloutIdentity(t, gvk.Perses, PersesName),
		assertion: fmt.Sprintf("Pod %s Running/Ready", persesPodName),
		diagnostics: []string{
			fmt.Sprintf("oc -n %s logs %s", tc.MonitoringNamespace, persesPodName),
			fmt.Sprintf("oc -n %s describe pod %s", tc.MonitoringNamespace, persesPodName),
			fmt.Sprintf("oc -n %s get perses %s -o yaml", tc.MonitoringNamespace, PersesName),
		},
	})

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Pod, types.NamespacedName{Name: persesPodName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.status.phase == "Running"`),
			jq.Match(`.status.conditions[] | select(.type == "Ready") | .status == "True"`),
		)),
		WithCustomErrorMsg(attribution),
	)

	t.Logf("✓ Perses health check passed; failure-attribution diagnostics wired:\n%s", attribution)
}

// ValidatePersesCRCreation tests that Perses CR is created when monitoring is managed with metrics.
func (tc *MonitoringTestCtx) ValidatePersesCRCreation(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

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
			tc.monitoringOwnerReferencesCondition(),
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
	tc = tc.WithT(t)

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
			jq.Match(`(.apiVersion | contains("v1alpha1") | not) or .spec.storage.size == "1Gi"`),
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
	tc = tc.WithT(t)

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
	tc.waitForPrometheusPodsGone(t)

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
	tc = tc.WithT(t)

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
	tc = tc.WithT(t)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{Name: "perses-operator-access", Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.metadata.labels["platform.opendatahub.io/part-of"] == "monitoring"`),
			jq.Match(`.spec.podSelector.matchLabels["app.kubernetes.io/instance"] == "data-science-perses"`),
			jq.Match(`.spec.podSelector.matchLabels["app.kubernetes.io/managed-by"] == "perses-operator"`),
			jq.Match(`.spec.podSelector.matchLabels["app.kubernetes.io/name"] == "data-science-perses"`),
			jq.Match(`.spec.policyTypes[0] == "Ingress"`),
			jq.Match(`.spec.ingress[0].from | length == 3`),
			jq.Match(`[.spec.ingress[0].from[].namespaceSelector.matchLabels["kubernetes.io/metadata.name"]] | any(. == "openshift-cluster-observability-operator")`),
			jq.Match(`[.spec.ingress[0].from[].namespaceSelector.matchLabels["kubernetes.io/metadata.name"]] | any(. == "openshift-operators")`),
			jq.Match(`[.spec.ingress[0].from[].namespaceSelector.matchLabels["kubernetes.io/metadata.name"]] | any(. == "openshift-operators-redhat")`),
			jq.Match(`[.spec.ingress[0].from[] | select(.podSelector.matchLabels["app.kubernetes.io/name"] == "perses-operator")] | length == 3`),
			jq.Match(`.spec.ingress[0].ports[0].protocol == "TCP"`),
			jq.Match(`.spec.ingress[0].ports[0].port == 8080`),
		)),
		WithCustomErrorMsg("Perses NetworkPolicy should be created with correct configuration allowing perses-operator access"),
	)
	tc.assertReadyPodLabels(t, "Perses", PersesName+"-", map[string]string{
		"app.kubernetes.io/instance":   "data-science-perses",
		"app.kubernetes.io/managed-by": "perses-operator",
		"app.kubernetes.io/name":       "data-science-perses",
	})
}

// ValidatePersesDatasourceCreationWithTraces tests that Perses datasource is created when traces are configured.
func (tc *MonitoringTestCtx) ValidatePersesDatasourceCreationWithTraces(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

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
	tc = tc.WithT(t)

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
			jq.Match(`.metadata.ownerReferences[0].name == "%s"`, tc.MonitoringCRName),
		)),
	)

	tc.updateMonitoringConfig(withNoTraces())
}

// ValidatePersesDatasourceWithPrometheus validates that Prometheus datasource is created when both Perses and MonitoringStack are deployed.
func (tc *MonitoringTestCtx) ValidatePersesDatasourceWithPrometheus(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

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
			tc.monitoringOwnerReferencesCondition(),
			jq.Match(`.status.conditions[] | select(.type == "Available") | .status == "True"`),
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
			tc.monitoringOwnerReferencesCondition(),
			jq.Match(`.status.conditions[] | select(.type == "Available") | .status == "True"`),
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
	tc = tc.WithT(t)

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
	tc.waitForPrometheusPodsGone(t)

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
	tc = tc.WithT(t)

	t.Run("Group 9: Networking and RBAC", func(t *testing.T) {
		tc = tc.WithT(t)

		t.Run("Prometheus restricted resource configuration", tc.ValidatePrometheusRestrictedResourceConfiguration)
		t.Run("Prometheus secure proxy authentication", tc.ValidatePrometheusSecureProxyAuthentication)
		t.Run("Monitoring proxy TLS profile rollout", tc.ValidateMonitoringProxyTLSProfileRollout)
		t.Run("Tenant workload metrics namespace restriction", tc.ValidateTenantWorkloadMetricsNamespaceRestriction)
		t.Run("Built-in DCGM opt-out with verified HTTPS monitor", tc.ValidateBuiltinDCGMScrapeOptOutUsesVerifiedHTTPSMonitor)
		t.Run("OTLP metric identity cannot be spoofed", tc.ValidateOTLPMetricIdentityCannotBeSpoofed)
		t.Run("Node metrics endpoint deployment", tc.ValidateNodeMetricsEndpointDeployment)
		t.Run("Node metrics endpoint RBAC configuration", tc.ValidateNodeMetricsEndpointRBACConfiguration)
		t.Run("Operator metrics authentication and authorization", tc.ValidateOperatorMetricsAuthenticationAndAuthorization)
		t.Run("Operand NetworkPolicies and lifecycle", tc.ValidateOperandNetworkPolicies)
		t.Run("Collector and TargetAllocator NetworkPolicy contract", tc.ValidateCollectorNetworkPolicyContract)
		t.Run("Collector OTLP namespace authorization", tc.ValidateCollectorOTLPNamespaceBoundary)
	})
}

func (tc *MonitoringTestCtx) waitForPrometheusNamespaceProxyPrerequisites(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

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
	tc = tc.WithT(t)

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

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
			Name:      "data-science-prometheus-proxy-ingress",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`.spec.ingress[0].from[0].namespaceSelector.matchLabels."policy-group.network.openshift.io/ingress" == ""`)),
		WithCustomErrorMsg("Prometheus namespace proxy NetworkPolicy should allow the OpenShift ingress policy group"),
	)
}

func (tc *MonitoringTestCtx) ValidatePrometheusRestrictedResourceConfiguration(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

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
	tc = tc.WithT(t)

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

func (tc *MonitoringTestCtx) ValidateMonitoringProxyTLSProfileRollout(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	tc.setupMetrics(t)

	proxyDeployments := []string{
		"data-science-prometheus-namespace-proxy",
		"data-science-prometheus-cluster-proxy",
		ThanosQuerierProxyName,
	}

	for _, deploymentName := range proxyDeployments {
		deploymentKey := types.NamespacedName{
			Name:      deploymentName,
			Namespace: tc.MonitoringNamespace,
		}
		tc.EnsureDeploymentReady(WithMinimalObject(gvk.Deployment, deploymentKey))

		deployment := tc.FetchResource(WithMinimalObject(gvk.Deployment, deploymentKey))
		containers, found, err := unstructured.NestedSlice(
			deployment.Object, "spec", "template", "spec", "containers",
		)
		require.NoError(t, err)
		require.True(t, found)

		var proxyContainer map[string]any
		for _, rawContainer := range containers {
			container, ok := rawContainer.(map[string]any)
			require.True(t, ok)
			if container["name"] == "kube-rbac-proxy" {
				proxyContainer = container
				break
			}
		}
		require.NotNil(t, proxyContainer, "kube-rbac-proxy container not found in %s", deploymentName)

		args, found, err := unstructured.NestedStringSlice(proxyContainer, "args")
		require.NoError(t, err)
		require.True(t, found)

		expected := tc.expectedKubeRBACProxyTLSArgs(t)

		minVersion, found := findProxyArg(args, "--tls-min-version=")
		require.True(t, found, "%s must set --tls-min-version", deploymentName)
		require.Equal(t, expected.minVersion, minVersion)

		cipherSuites, found := findProxyArg(args, "--tls-cipher-suites=")
		if expected.cipherSuites == "" {
			require.False(t, found, "TLS 1.3 must not restrict cipher suites in %s", deploymentName)
		} else {
			require.True(t, found, "TLS 1.2 must set cipher suites in %s", deploymentName)
			require.Equal(t, expected.cipherSuites, cipherSuites)
		}

		curvePreferences, found := findProxyArg(args, "--tls-curve-preferences=")
		if expected.curvePreferences == "" {
			require.False(t, found, "%s must omit curve preferences when no usable groups are configured", deploymentName)
		} else {
			require.True(t, found, "%s must set curve preferences", deploymentName)
			require.Equal(t, expected.curvePreferences, curvePreferences)
		}

		_, found = findProxyArg(args, "--tls-cipher-suite=")
		require.False(t, found, "%s uses kube-rbac-proxy and must use the plural cipher flag", deploymentName)
	}
}

type expectedProxyTLSArgs struct {
	minVersion       string
	cipherSuites     string
	curvePreferences string
}

func (tc *MonitoringTestCtx) expectedKubeRBACProxyTLSArgs(t *testing.T) expectedProxyTLSArgs {
	t.Helper()

	profile := tc.expectedClusterTLSProfile(t)
	spec := expectedProxyTLSProfileSpec(t, profile)
	minVersion := expectedProxyTLSMinVersion(t, spec.MinTLSVersion)

	cipherSuites := ""
	if minVersion == "VersionTLS12" {
		cipherSuites = expectedProxyTLSCipherSuites(spec.Ciphers)
	}

	return expectedProxyTLSArgs{
		minVersion:       minVersion,
		cipherSuites:     cipherSuites,
		curvePreferences: tc.expectedProxyTLSCurvePreferences(t, spec.Groups),
	}
}

func (tc *MonitoringTestCtx) expectedClusterTLSProfile(t *testing.T) *configv1.TLSSecurityProfile {
	t.Helper()

	apiServer := &configv1.APIServer{}
	err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: "cluster"}, apiServer)
	if k8serr.IsNotFound(err) || meta.IsNoMatchError(err) {
		return nil
	}
	require.NoError(t, err, "failed to get cluster APIServer configuration")

	switch apiServer.Spec.TLSAdherence {
	case configv1.TLSAdherencePolicyNoOpinion, configv1.TLSAdherencePolicyLegacyAdheringComponentsOnly:
		return nil
	default:
		return apiServer.Spec.TLSSecurityProfile
	}
}

func expectedProxyTLSProfileSpec(t *testing.T, profile *configv1.TLSSecurityProfile) configv1.TLSProfileSpec {
	t.Helper()

	if profile == nil || profile.Type == "" {
		return *configv1.TLSProfiles[configv1.TLSProfileIntermediateType]
	}
	if profile.Type == configv1.TLSProfileCustomType {
		require.NotNil(t, profile.Custom, "strict custom TLS profile must define a specification")
		return profile.Custom.TLSProfileSpec
	}

	spec, ok := configv1.TLSProfiles[profile.Type]
	require.True(t, ok, "unsupported strict TLS profile type %q", profile.Type)
	return *spec
}

func expectedProxyTLSMinVersion(t *testing.T, minVersion configv1.TLSProtocolVersion) string {
	t.Helper()

	switch minVersion {
	case configv1.VersionTLS12:
		return "VersionTLS12"
	case configv1.VersionTLS13:
		return "VersionTLS13"
	default:
		t.Fatalf("kube-rbac-proxy cannot represent minimum TLS version %q", minVersion)
		return ""
	}
}

func expectedProxyTLSCipherSuites(ciphers []string) string {
	ianaCiphers := ocpcrypto.OpenSSLToIANACipherSuites(ciphers)
	filtered := make([]string, 0, len(ianaCiphers))
	for _, cipher := range ianaCiphers {
		code, err := ocpcrypto.CipherSuite(cipher)
		if err != nil || code&0xff00 == 0x1300 {
			continue
		}
		filtered = append(filtered, cipher)
	}
	if len(filtered) == 0 {
		filtered = ocpcrypto.OpenSSLToIANACipherSuites(
			configv1.TLSProfiles[configv1.TLSProfileIntermediateType].Ciphers,
		)
	}
	return strings.Join(filtered, ",")
}

func (tc *MonitoringTestCtx) expectedProxyTLSCurvePreferences(t *testing.T, groups []configv1.TLSGroup) string {
	t.Helper()

	if len(groups) == 0 {
		return ""
	}

	curves, _ := ocpcrypto.TLSGroupsToCurveIDs(groups)
	fipsEnabled := tc.expectedClusterFIPSEnabled(t)
	allowedCurves := map[int]struct{}{
		23: {}, 24: {}, 25: {}, 29: {}, 4588: {},
	}
	if fipsEnabled {
		allowedCurves = map[int]struct{}{23: {}, 24: {}, 25: {}}
	}

	seen := make(map[int]struct{}, len(curves))
	curveIDs := make([]string, 0, len(curves))
	for _, curve := range curves {
		if _, ok := allowedCurves[int(curve)]; !ok {
			continue
		}
		if _, ok := seen[int(curve)]; ok {
			continue
		}
		seen[int(curve)] = struct{}{}
		curveIDs = append(curveIDs, strconv.Itoa(int(curve)))
	}

	require.NotEmpty(t, curveIDs, "strict TLS profile groups contain no kube-rbac-proxy-supported curves")
	return strings.Join(curveIDs, ",")
}

func (tc *MonitoringTestCtx) expectedClusterFIPSEnabled(t *testing.T) bool {
	t.Helper()

	configMap := &corev1.ConfigMap{}
	err := tc.Client().Get(tc.Context(), types.NamespacedName{
		Namespace: "kube-system",
		Name:      "cluster-config-v1",
	}, configMap)
	if k8serr.IsNotFound(err) {
		return false
	}
	require.NoError(t, err, "failed to get cluster install configuration")

	installConfig := configMap.Data["install-config"]
	if installConfig == "" {
		return false
	}

	var parsedInstallConfig struct {
		FIPS bool `yaml:"fips"`
	}
	if err := yaml.Unmarshal([]byte(installConfig), &parsedInstallConfig); err != nil {
		return strings.Contains(strings.ToLower(installConfig), "fips: true")
	}
	return parsedInstallConfig.FIPS
}

func findProxyArg(args []string, prefix string) (string, bool) {
	for _, arg := range args {
		if value, found := strings.CutPrefix(arg, prefix); found {
			return value, true
		}
	}
	return "", false
}

func (tc *MonitoringTestCtx) ValidateNodeMetricsEndpointDeployment(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

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
	tc = tc.WithT(t)

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

func (tc *MonitoringTestCtx) ValidateOperatorMetricsAuthenticationAndAuthorization(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.verifyOperatorMetricsRBAC(t)

	// The suite creates this ServiceMonitor during module-mode setup only.
	// DSC runs the same test, and the chart leaves the CA out unless
	// metrics.serviceMonitor.enabled is set, so create it here for both modes.
	tc.ensureOperatorServiceMonitor(t)

	// ensureOperatorDeploymentReady returns MONITORING_NAMESPACE. The metrics
	// Service and its DNS name live in the operator Deployment namespace.
	// NetworkPolicy accepts :8443 only from the monitoring namespace, so the
	// probe has to run in a pod there rather than in the test process.
	monitoringNS := tc.ensureOperatorDeploymentReady(t)
	operatorNS := tc.operatorDeploymentNamespace(t)
	serviceHost := fmt.Sprintf("odh-observability-metrics.%s.svc:8443", operatorNS)
	tc.waitForOperatorMetricsCA(t, monitoringNS)
	tc.ensureOperatorMetricsProbePod(t, monitoringNS)

	t.Run("Unauthenticated request returns 401", func(t *testing.T) {
		statusCode := tc.probeOperatorMetrics(t, monitoringNS, serviceHost, "")
		if statusCode != http.StatusUnauthorized {
			t.Errorf("unauthenticated request to /metrics should return 401, got %d", statusCode)
		}
	})

	t.Run("Unauthorized request returns 403", func(t *testing.T) {
		unauthorizedSA := tc.createServiceAccountForMetricsTest(t, operatorNS, "metrics-unauthorized-test")
		unauthorizedToken := tc.getServiceAccountToken(t, operatorNS, unauthorizedSA)

		statusCode := tc.probeOperatorMetrics(t, monitoringNS, serviceHost, unauthorizedToken)
		if statusCode != http.StatusForbidden {
			t.Errorf("unauthorized request to /metrics should return 403, got %d", statusCode)
		}
	})

	t.Run("Authorized request returns 200", func(t *testing.T) {
		prometheusSA := fmt.Sprintf("%s-prometheus", MonitoringStackName)
		tokenNamespace := tc.MonitoringNamespace
		if tokenNamespace == "" {
			tokenNamespace = monitoringNS
		}
		authorizedToken := tc.getServiceAccountToken(t, tokenNamespace, prometheusSA)

		var statusCode int
		tc.g.Eventually(func() error {
			statusCode = tc.probeOperatorMetrics(t, monitoringNS, serviceHost, authorizedToken)
			if statusCode != http.StatusOK {
				return fmt.Errorf("authorized request to /metrics should return 200, got %d", statusCode)
			}
			return nil
		}).WithTimeout(30*time.Second).Should(Succeed(),
			"Prometheus ServiceAccount should be able to scrape operator metrics")
	})
}

func (tc *MonitoringTestCtx) verifyOperatorMetricsRBAC(t *testing.T) {
	t.Helper()

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRole, types.NamespacedName{
			Name: "odh-observability-metrics-reader",
		}),
		WithCondition(And(
			jq.Match(`[.rules[] | select(.nonResourceURLs[]? == "/metrics")] | any`),
			jq.Match(`[.rules[] | select(.verbs[]? == "get")] | any`),
		)),
		WithCustomErrorMsg("ClusterRole odh-observability-metrics-reader should grant GET on /metrics"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{
			Name: "odh-observability-metrics-reader",
		}),
		WithCondition(And(
			jq.Match(`.roleRef.name == "odh-observability-metrics-reader"`),
			jq.Match(`[.subjects[] | select(.kind == "ServiceAccount" and .name == "%s")] | any`, fmt.Sprintf("%s-prometheus", MonitoringStackName)),
			jq.Match(`[.subjects[] | select(.namespace == "%s")] | any`, tc.MonitoringNamespace),
		)),
		WithCustomErrorMsg("ClusterRoleBinding should bind Prometheus SA to metrics-reader role"),
	)

	t.Logf("Operator metrics RBAC resources verified")
}

func (tc *MonitoringTestCtx) createServiceAccountForMetricsTest(t *testing.T, namespace, name string) string {
	t.Helper()

	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
	if err := tc.Client().Create(tc.Context(), sa); err != nil && !k8serr.IsAlreadyExists(err) {
		t.Fatalf("failed to create test ServiceAccount %s/%s: %v", namespace, name, err)
	}
	t.Cleanup(func() {
		if err := tc.Client().Delete(tc.Context(), sa); err != nil && !k8serr.IsNotFound(err) {
			t.Logf("failed to clean up test ServiceAccount %s/%s: %v", namespace, name, err)
		}
	})
	return name
}

func (tc *MonitoringTestCtx) getServiceAccountToken(t *testing.T, namespace, serviceAccountName string) string {
	t.Helper()

	config, err := ctrlcfg.GetConfig()
	if err != nil {
		t.Fatalf("failed to load Kubernetes config for token: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("failed to create Kubernetes client for token: %v", err)
	}

	tokenRequest, err := clientset.CoreV1().ServiceAccounts(namespace).CreateToken(
		tc.Context(),
		serviceAccountName,
		&authenticationv1.TokenRequest{},
		metav1.CreateOptions{},
	)
	if err != nil {
		t.Fatalf("failed to create token for ServiceAccount %s/%s: %v", namespace, serviceAccountName, err)
	}
	if tokenRequest.Status.Token == "" {
		t.Fatalf("TokenRequest for ServiceAccount %s/%s returned empty token", namespace, serviceAccountName)
	}
	return tokenRequest.Status.Token
}

const (
	operatorMetricsProbePodName = "operator-metrics-auth-probe"
	operatorMetricsCAConfigMap  = "odh-observability-metrics-ca"
)

func (tc *MonitoringTestCtx) waitForOperatorMetricsCA(t *testing.T, monitoringNS string) {
	t.Helper()

	tc.g.Eventually(func() error {
		caConfigMap := &corev1.ConfigMap{}
		if err := tc.Client().Get(tc.Context(), types.NamespacedName{
			Name:      operatorMetricsCAConfigMap,
			Namespace: monitoringNS,
		}, caConfigMap); err != nil {
			return err
		}
		if caConfigMap.Data["service-ca.crt"] == "" {
			return fmt.Errorf("service-ca.crt is not injected into %s/%s yet", monitoringNS, operatorMetricsCAConfigMap)
		}
		return nil
	}).WithTimeout(2*time.Minute).WithPolling(5*time.Second).Should(Succeed(),
		"operator metrics CA ConfigMap should exist in the monitoring namespace")
}

func (tc *MonitoringTestCtx) ensureOperatorMetricsProbePod(t *testing.T, monitoringNS string) {
	t.Helper()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      operatorMetricsProbePodName,
			Namespace: monitoringNS,
		},
		Spec: corev1.PodSpec{
			RestartPolicy:   corev1.RestartPolicyAlways,
			SecurityContext: fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name:            "curl",
				Image:           fakeGCSClientImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{"sleep", "3600"},
				SecurityContext: fixtureContainerSecurityContext(),
				VolumeMounts: []corev1.VolumeMount{{
					Name:      "metrics-ca",
					MountPath: "/ca",
					ReadOnly:  true,
				}},
			}},
			Volumes: []corev1.Volume{{
				Name: "metrics-ca",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: operatorMetricsCAConfigMap},
					},
				},
			}},
		},
	}
	if err := tc.Client().Create(tc.Context(), pod); err != nil && !k8serr.IsAlreadyExists(err) {
		t.Fatalf("failed to create operator metrics probe pod: %v", err)
	}
	t.Cleanup(func() {
		if err := tc.Client().Delete(tc.Context(), pod); err != nil && !k8serr.IsNotFound(err) {
			t.Logf("failed to delete operator metrics probe pod: %v", err)
		}
	})

	tc.g.Eventually(func() error {
		current := &corev1.Pod{}
		if err := tc.Client().Get(tc.Context(), types.NamespacedName{
			Name:      operatorMetricsProbePodName,
			Namespace: monitoringNS,
		}, current); err != nil {
			return err
		}
		if current.Status.Phase != corev1.PodRunning {
			return fmt.Errorf("probe pod is %s", current.Status.Phase)
		}
		for _, container := range current.Status.ContainerStatuses {
			if container.Name == "curl" && container.Ready {
				return nil
			}
		}
		return errors.New("curl container is not ready")
	}).WithTimeout(2*time.Minute).WithPolling(5*time.Second).Should(Succeed(),
		"operator metrics probe pod should be running in the monitoring namespace")
}

func (tc *MonitoringTestCtx) probeOperatorMetrics(t *testing.T, monitoringNS, serviceHost, token string) int {
	t.Helper()

	args := []string{
		"curl", "--noproxy", "*", "-sS",
		"--connect-timeout", "5", "--max-time", "10",
		"--cacert", "/ca/service-ca.crt",
		"-o", "/dev/null", "-w", "%{http_code}",
	}
	if token != "" {
		args = append(args, "-H", "Authorization: Bearer "+token)
	}
	args = append(args, "https://"+serviceHost+"/metrics")

	stdout, stderr, err := tc.execOperatorMetricsProbe(t, monitoringNS, args)
	statusCode, convErr := strconv.Atoi(strings.TrimSpace(stdout))
	if err != nil || convErr != nil || statusCode == 0 {
		t.Logf("operator metrics probe from %s/%s failed: status=%q err=%v stderr=%s",
			monitoringNS, operatorMetricsProbePodName, strings.TrimSpace(stdout), err, strings.TrimSpace(stderr))
		return 0
	}
	t.Logf("operator metrics probe from %s: status=%d", monitoringNS, statusCode)
	return statusCode
}

func (tc *MonitoringTestCtx) execOperatorMetricsProbe(t *testing.T, monitoringNS string, command []string) (string, string, error) {
	t.Helper()

	cfg, err := ctrlcfg.GetConfig()
	if err != nil {
		return "", "", fmt.Errorf("load kubeconfig for pod exec: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return "", "", fmt.Errorf("create Kubernetes client for pod exec: %w", err)
	}
	request := clientset.CoreV1().RESTClient().Post().
		Resource("pods").Name(operatorMetricsProbePodName).
		Namespace(monitoringNS).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "curl",
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, clientgoscheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(cfg, http.MethodPost, request.URL())
	if err != nil {
		return "", "", fmt.Errorf("create Kubernetes pod exec stream: %w", err)
	}
	ctx, cancel := context.WithTimeout(tc.Context(), 30*time.Second)
	defer cancel()

	var stdout, stderr bytes.Buffer
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return stdout.String(), stderr.String(), err
}

// ========================================================================
// Lifecycle transitions and final cleanup
// ========================================================================

// Keep disruptive off/on checks after the steady-state integrations. Each
// lifecycle case starts with ready metrics, so a focused -run selection still
// has its own fixture and the full suite never tears down Prometheus between
// Target Allocator and Thanos route checks.
func (tc *MonitoringTestCtx) runMonitoringLifecycleTests(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	t.Run("Group 13: Lifecycle", func(t *testing.T) {
		tc = tc.WithT(t)
		t.Run("Metrics replicas", func(t *testing.T) {
			caseCtx := tc.WithT(t)
			caseCtx.setupMetrics(t)
			caseCtx.ValidateMonitoringStackCRMetricsReplicasUpdate(t)
		})
		t.Run("Target Allocator", func(t *testing.T) {
			caseCtx := tc.WithT(t)
			caseCtx.setupMetrics(t)
			caseCtx.ValidateTargetAllocatorLifecycle(t)
		})
		t.Run("Perses", func(t *testing.T) {
			caseCtx := tc.WithT(t)
			caseCtx.setupMetrics(t)
			caseCtx.ValidatePersesLifecycle(t)
		})
		t.Run("Perses datasource", func(t *testing.T) {
			caseCtx := tc.WithT(t)
			caseCtx.setupMetrics(t)
			caseCtx.ValidatePersesDatasourceLifecycle(t)
		})
		t.Run("Prometheus rules", func(t *testing.T) {
			caseCtx := tc.WithT(t)
			caseCtx.setupMetrics(t)
			caseCtx.ValidatePrometheusRulesLifecycle(t)
		})
		t.Run("Webhook when monitoring disabled", func(t *testing.T) {
			caseCtx := tc.WithT(t)
			caseCtx.setupMetrics(t)
			caseCtx.ValidateWebhookSkipsWhenMonitoringDisabled(t)
		})
	})
}

func (tc *MonitoringTestCtx) runDisabledTests(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	t.Run("Disabled: Monitoring Service Disabled", func(t *testing.T) {
		t.Run("Validate monitoring service disabled", tc.ValidateMonitoringServiceDisabled)
	})
}

func (tc *MonitoringTestCtx) ValidateMonitoringServiceDisabled(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.ensureMonitoringCleanSlate(t, "")

	tc.resetMonitoringConfigToRemoved()

	tc.ensureMonitoringDisabled()

	for _, resource := range []struct {
		gvk       schema.GroupVersionKind
		name      string
		namespace string
	}{
		{gvk: gvk.MonitoringStack, name: MonitoringStackName, namespace: tc.MonitoringNamespace},
		{gvk: gvk.TempoStack, name: TempoStackName, namespace: tc.MonitoringNamespace},
		{gvk: gvk.TempoMonolithic, name: TempoMonolithicName, namespace: tc.MonitoringNamespace},
		{gvk: gvk.OpenTelemetryCollector, name: OpenTelemetryCollectorName, namespace: tc.MonitoringNamespace},
		{gvk: gvk.OpenTelemetryCollector, name: UsageLogsCollectorName, namespace: tc.MonitoringNamespace},
		{gvk: gvk.Instrumentation, name: InstrumentationName, namespace: tc.MonitoringNamespace},
		{gvk: gvk.Perses, name: PersesName, namespace: tc.MonitoringNamespace},
		{gvk: gvk.PersesDatasource, name: PersesDatasourceName, namespace: tc.MonitoringNamespace},
		{gvk: gvk.PersesDatasource, name: ClusterPrometheusDatasourceName, namespace: tc.MonitoringNamespace},
		{gvk: gvk.NetworkPolicy, name: UsageLogsCollectorNetworkPolicyName, namespace: tc.MonitoringNamespace},
		{gvk: gvk.NetworkPolicy, name: UsageLogsLokiStackNetworkPolicyName, namespace: tc.MonitoringNamespace},
	} {
		tc.EnsureResourceGone(
			WithMinimalObject(resource.gvk, types.NamespacedName{
				Name:      resource.name,
				Namespace: resource.namespace,
			}),
		)
	}
}
