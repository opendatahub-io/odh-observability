package e2e_test

import (
	"testing"
	"time"

	gTypes "github.com/onsi/gomega/types"
	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega
)

// Observability deployment modes (product Mode 1/2/4). Mode 3 (Removed) is covered by Disabled.
// Mode 2 is the primary gap; Mode 1/4 are explicit mode-shaped happy paths (not a second storage suite).
//
// Mode 4 "metrics storage + custom exporters" (existing-style Custom) is intentionally not
// duplicated here: it is already covered by the "Custom Metrics Exporters" case under
// Metrics ready / OpenTelemetry Collector (withMetricsConfig + withCustomMetricsExporters).
func (tc *MonitoringTestCtx) runObservabilityModesTests(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	t.Run("Group: Observability Modes", func(t *testing.T) {
		tc = tc.WithT(t)
		tc.resetMonitoringConfigToManaged()

		// Mode 2 first (primary gap, no built-in stack). Mode 1 last so Prometheus/Tempo/Perses
		// teardown does not block exporters-only cases.
		t.Run("Mode 2 metrics exporters only", tc.ValidateMode2MetricsExportersOnly)
		t.Run("Mode 2 traces exporters only", tc.ValidateMode2TracesExportersOnly)
		t.Run("Mode 2 combined exporters only distinct names", tc.ValidateMode2CombinedExportersOnly)
		t.Run("Mode 4 metrics only with storage", tc.ValidateMode4MetricsOnlyWithStorage)
		t.Run("Mode 4 traces only with storage", tc.ValidateMode4TracesOnlyWithStorage)
		t.Run("Mode 1 all built-in metrics and traces with storage", tc.ValidateMode1AllBuiltInReady)
	})
}

func (tc *MonitoringTestCtx) ensurePersesStatefulSetReady(t *testing.T) {
	t.Helper()

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.StatefulSet, types.NamespacedName{Name: PersesName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.status.readyReplicas == 1`)),
		WithEventuallyTimeout(15*time.Minute),
		WithCustomErrorMsg("Perses StatefulSet %s should have readyReplicas == 1", PersesName),
	)
}

func (tc *MonitoringTestCtx) ensureBuiltInTracesOperandsGone(t *testing.T) {
	t.Helper()

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.TempoMonolithic, types.NamespacedName{Name: TempoMonolithicName, Namespace: tc.MonitoringNamespace}),
	)
	tc.EnsureResourceGone(
		WithMinimalObject(gvk.TempoStack, types.NamespacedName{Name: TempoStackName, Namespace: tc.MonitoringNamespace}),
	)
	tc.EnsureResourceGone(
		WithMinimalObject(gvk.Instrumentation, types.NamespacedName{Name: InstrumentationName, Namespace: tc.MonitoringNamespace}),
	)
}

func (tc *MonitoringTestCtx) ensureBuiltInMetricsOperandsGone(t *testing.T) {
	t.Helper()

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.MonitoringStack, types.NamespacedName{Name: MonitoringStackName, Namespace: tc.MonitoringNamespace}),
	)
	tc.EnsureResourceGone(
		WithMinimalObject(gvk.ThanosQuerier, types.NamespacedName{Name: ThanosQuerierName, Namespace: tc.MonitoringNamespace}),
	)
}

func (tc *MonitoringTestCtx) ensureCollectorPipelineAbsent(t *testing.T, pipeline string) {
	t.Helper()

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name: OpenTelemetryCollectorName, Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(collectorPipelineAbsent(pipeline)),
		WithCustomErrorMsg("Collector must not define the %s pipeline", pipeline),
	)
}

func collectorPipelineAbsent(pipeline string) *jq.Matcher {
	return jq.Match(`.spec.config.service.pipelines | has("%s") | not`, pipeline)
}

func collectorMetricsPipelinesInclude(exporter string) *jq.Matcher {
	return jq.Match(`
		(.spec.config.service.pipelines."metrics/scrape".exporters // [] | contains(["%s"])) and
		(.spec.config.service.pipelines."metrics/otlp".exporters // [] | contains(["%s"]))
	`, exporter, exporter)
}

func collectorMetricsPipelinesOnly(exporter string) *jq.Matcher {
	return jq.Match(`
		(.spec.config.service.pipelines."metrics/scrape".exporters == ["%s"]) and
		(.spec.config.service.pipelines."metrics/otlp".exporters == ["%s"])
	`, exporter, exporter)
}

// usageLogsMustBeAbsent fails if stale usageLogs config is present (Modes runs before the
// Usage Logs group; if suite order drifts, fail loud instead of flaking later).
//
//nolint:ireturn // Gomega matchers are returned as the GomegaMatcher interface.
func usageLogsMustBeAbsent() gTypes.GomegaMatcher {
	return jq.Match(`.spec.usageLogs == null`)
}

// ValidateMode1AllBuiltInReady checks Mode 1: metrics+traces storage → stack, Tempo, Instrumentation, collector, Perses ready.
func (tc *MonitoringTestCtx) ValidateMode1AllBuiltInReady(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(func() {
		tc.cleanupGroup(t, "")
	})

	tc.waitForPrometheusPodsGone(t)
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
		withMonitoringTraces(TracesStorageBackendPV, "", TracesStorageSize1Gi, DefaultRetention),
	)
	tc.waitForPrometheusReady(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionMonitoringStackAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionTempoAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionInstrumentationAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionOpenTelemetryCollectorAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionPersesAvailable, metav1.ConditionTrue),
			jq.Match(`.spec.metrics.storage != null`),
			jq.Match(`.spec.traces.storage != null`),
			usageLogsMustBeAbsent(),
		)),
		WithEventuallyTimeout(15*time.Minute),
		WithCustomErrorMsg("Mode 1: Monitoring should be Ready with MonitoringStack, Tempo, Instrumentation, collector, and Perses available"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.MonitoringStack, types.NamespacedName{Name: MonitoringStackName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.status.conditions[] | select(.type == "Available") | .status == "%s"`, metav1.ConditionTrue)),
		WithCustomErrorMsg("Mode 1: MonitoringStack should be Available"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.TempoMonolithic, types.NamespacedName{Name: TempoMonolithicName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue)),
		WithEventuallyTimeout(15*time.Minute),
		WithCustomErrorMsg("Mode 1: TempoMonolithic should be Ready"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Instrumentation, types.NamespacedName{Name: InstrumentationName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.metadata.name == "%s"`, InstrumentationName)),
		WithCustomErrorMsg("Mode 1: Instrumentation should be deployed"),
	)

	tc.ensureOpenTelemetryCollectorReady(t)

	// Mode 1 must wire built-in backends — Mode 2 forbids these; Mode 1 must require them.
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name: OpenTelemetryCollectorName, Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(jq.Match(`
			(.spec.config.exporters | has("prometheus") and has("otlp/tempo")) and
			(.spec.config.service.pipelines.traces.exporters | contains(["otlp/tempo"]))
		`), collectorMetricsPipelinesInclude("prometheus"))),
		WithCustomErrorMsg("Mode 1: collector metrics/traces pipelines must include built-in prometheus and otlp/tempo exporters"),
	)

	tc.ensurePersesStatefulSetReady(t)
}

// ValidateMode2MetricsExportersOnly checks exporters-only metrics: collector Ready; no stack/Thanos/Perses/Tempo/Instrumentation.
func (tc *MonitoringTestCtx) ValidateMode2MetricsExportersOnly(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(func() {
		tc.cleanupGroup(t, "")
	})

	tc.waitForPrometheusPodsGone(t)
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withMetricsExportersOnly(OtlpHttpMetricsExternalExporter),
		withNoTraces(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`.spec.metrics.storage == null`),
			jq.Match(`.spec.metrics.exporters != null`),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionOpenTelemetryCollectorAvailable, metav1.ConditionTrue),
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .status=="False" and .reason=="%s")] | length==1`,
				conditions.ConditionMonitoringStackAvailable, conditions.MetricsNotConfiguredReason),
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .status=="False" and .reason=="%s")] | length==1`,
				conditions.ConditionThanosQuerierAvailable, conditions.MetricsNotConfiguredReason),
			// Perses keys off *.storage, not "metrics/traces nil"; exporters-only still uses this reason.
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .status=="False" and .reason=="%s")] | length==1`,
				conditions.ConditionPersesAvailable, conditions.MetricsAndTracesNotConfiguredReason),
			usageLogsMustBeAbsent(),
		)),
		WithCustomErrorMsg("Mode 2 metrics: Monitoring Ready with collector; stack/Thanos/Perses not configured"),
	)

	tc.ensureOpenTelemetryCollectorReady(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name: OpenTelemetryCollectorName, Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(jq.Match(`
			(.spec.config.exporters | has("%s")) and
			(.spec.config.exporters | has("prometheus") | not)
		`, OtlpHttpMetricsExternalExporter), collectorMetricsPipelinesOnly(OtlpHttpMetricsExternalExporter))),
		WithCustomErrorMsg("Mode 2 metrics: metrics pipeline exporters must be exactly [%s]", OtlpHttpMetricsExternalExporter),
	)
	tc.ensureCollectorPipelineAbsent(t, "traces")

	tc.ensureBuiltInMetricsOperandsGone(t)
	tc.EnsureResourceGone(
		WithMinimalObject(gvk.Perses, types.NamespacedName{Name: PersesName, Namespace: tc.MonitoringNamespace}),
	)
	tc.ensureBuiltInTracesOperandsGone(t)
}

// ValidateMode2TracesExportersOnly checks exporters-only traces: Instrumentation + collector; no Tempo/Perses/stack.
func (tc *MonitoringTestCtx) ValidateMode2TracesExportersOnly(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(func() {
		tc.cleanupGroup(t, "")
	})

	tc.waitForPrometheusPodsGone(t)
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withNoMetrics(),
		withTracesExportersOnly(OtlpHttpTracesExternalExporter),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`.spec.traces.storage == null`),
			jq.Match(`.spec.traces.exporters != null`),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionOpenTelemetryCollectorAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionInstrumentationAvailable, metav1.ConditionTrue),
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .status=="False" and .reason=="%s")] | length==1`,
				conditions.ConditionTempoAvailable, conditions.TracesNotConfiguredReason),
			// Perses keys off *.storage, not "metrics/traces nil"; exporters-only still uses this reason.
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .status=="False" and .reason=="%s")] | length==1`,
				conditions.ConditionPersesAvailable, conditions.MetricsAndTracesNotConfiguredReason),
			usageLogsMustBeAbsent(),
		)),
		WithCustomErrorMsg("Mode 2 traces: Monitoring Ready with Instrumentation+collector; Tempo/Perses not configured"),
	)

	tc.ensureOpenTelemetryCollectorReady(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Instrumentation, types.NamespacedName{Name: InstrumentationName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.metadata.name == "%s"`, InstrumentationName)),
		WithCustomErrorMsg("Mode 2 traces: Instrumentation should be deployed"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name: OpenTelemetryCollectorName, Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`
			(.spec.config.exporters | has("%s")) and
			(.spec.config.exporters | has("otlp/tempo") | not) and
			(.spec.config.service.pipelines.traces.exporters | length == 1 and contains(["%s"]))
		`, OtlpHttpTracesExternalExporter, OtlpHttpTracesExternalExporter)),
		WithCustomErrorMsg("Mode 2 traces: traces pipeline exporters must be exactly [%s]", OtlpHttpTracesExternalExporter),
	)
	tc.ensureCollectorPipelineAbsent(t, "metrics/scrape")
	tc.ensureCollectorPipelineAbsent(t, "metrics/otlp")

	// Exporters-only traces never select a Tempo backend, so neither CR was ever created.
	// Not using ensureBuiltInTracesOperandsGone here: it also asserts Instrumentation gone,
	// but Instrumentation is deployed in this case (asserted above) — traces are configured,
	// just without built-in storage.
	tc.EnsureResourceGone(
		WithMinimalObject(gvk.TempoMonolithic, types.NamespacedName{Name: TempoMonolithicName, Namespace: tc.MonitoringNamespace}),
	)
	tc.EnsureResourceGone(
		WithMinimalObject(gvk.TempoStack, types.NamespacedName{Name: TempoStackName, Namespace: tc.MonitoringNamespace}),
	)
	tc.ensureBuiltInMetricsOperandsGone(t)
	tc.EnsureResourceGone(
		WithMinimalObject(gvk.Perses, types.NamespacedName{Name: PersesName, Namespace: tc.MonitoringNamespace}),
	)
}

// ValidateMode2CombinedExportersOnly checks combined Mode 2 with distinct exporter names.
func (tc *MonitoringTestCtx) ValidateMode2CombinedExportersOnly(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(func() {
		tc.cleanupGroup(t, "")
	})

	tc.waitForPrometheusPodsGone(t)
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withMetricsExportersOnly(OtlpHttpMetricsExternalExporter),
		withTracesExportersOnly(OtlpHttpTracesExternalExporter),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`.spec.metrics.storage == null`),
			jq.Match(`.spec.traces.storage == null`),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionOpenTelemetryCollectorAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionInstrumentationAvailable, metav1.ConditionTrue),
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .status=="False" and .reason=="%s")] | length==1`,
				conditions.ConditionMonitoringStackAvailable, conditions.MetricsNotConfiguredReason),
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .status=="False" and .reason=="%s")] | length==1`,
				conditions.ConditionTempoAvailable, conditions.TracesNotConfiguredReason),
			// Perses keys off *.storage, not "metrics/traces nil"; exporters-only still uses this reason.
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .status=="False" and .reason=="%s")] | length==1`,
				conditions.ConditionPersesAvailable, conditions.MetricsAndTracesNotConfiguredReason),
			usageLogsMustBeAbsent(),
		)),
		WithCustomErrorMsg("Mode 2 combined: Monitoring Ready without built-in storage operands"),
	)

	tc.ensureOpenTelemetryCollectorReady(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Instrumentation, types.NamespacedName{Name: InstrumentationName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.metadata.name == "%s"`, InstrumentationName)),
		WithCustomErrorMsg("Mode 2 combined: Instrumentation should be deployed"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name: OpenTelemetryCollectorName, Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(jq.Match(`
			(.spec.config.exporters | has("%s") and has("%s")) and
			(.spec.config.exporters | has("prometheus") | not) and
			(.spec.config.exporters | has("otlp/tempo") | not) and
			(.spec.config.service.pipelines.traces.exporters | length == 1 and contains(["%s"]))
		`, OtlpHttpMetricsExternalExporter, OtlpHttpTracesExternalExporter,
			OtlpHttpTracesExternalExporter), collectorMetricsPipelinesOnly(OtlpHttpMetricsExternalExporter))),
		WithCustomErrorMsg("Mode 2 combined: metrics/traces pipelines must be exactly the distinct custom exporters"),
	)

	tc.ensureBuiltInMetricsOperandsGone(t)
	tc.EnsureResourceGone(
		WithMinimalObject(gvk.TempoMonolithic, types.NamespacedName{Name: TempoMonolithicName, Namespace: tc.MonitoringNamespace}),
	)
	tc.EnsureResourceGone(
		WithMinimalObject(gvk.TempoStack, types.NamespacedName{Name: TempoStackName, Namespace: tc.MonitoringNamespace}),
	)
	tc.EnsureResourceGone(
		WithMinimalObject(gvk.Perses, types.NamespacedName{Name: PersesName, Namespace: tc.MonitoringNamespace}),
	)
}

// ValidateMode4MetricsOnlyWithStorage checks metrics-only Custom with storage (no traces).
func (tc *MonitoringTestCtx) ValidateMode4MetricsOnlyWithStorage(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(func() {
		tc.cleanupGroup(t, "")
	})

	tc.waitForPrometheusPodsGone(t)
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
		withNoTraces(),
	)
	tc.waitForPrometheusReady(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionMonitoringStackAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionPersesAvailable, metav1.ConditionTrue),
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .status=="False" and .reason=="%s")] | length==1`,
				conditions.ConditionTempoAvailable, conditions.TracesNotConfiguredReason),
			jq.Match(`.spec.traces == null`),
			usageLogsMustBeAbsent(),
		)),
		WithCustomErrorMsg("Mode 4 metrics-only: MonitoringStack+Perses Ready; Tempo not configured"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.MonitoringStack, types.NamespacedName{Name: MonitoringStackName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.status.conditions[] | select(.type == "Available") | .status == "%s"`, metav1.ConditionTrue)),
	)

	tc.ensureOpenTelemetryCollectorReady(t)
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name: OpenTelemetryCollectorName, Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.config.exporters | has("prometheus")`),
			collectorMetricsPipelinesInclude("prometheus"),
		)),
		WithCustomErrorMsg("Mode 4 metrics-only: collector metrics pipeline must include built-in prometheus exporter"),
	)
	tc.ensureCollectorPipelineAbsent(t, "traces")

	tc.ensurePersesStatefulSetReady(t)
	tc.ensureBuiltInTracesOperandsGone(t)
}

// ValidateMode4TracesOnlyWithStorage checks traces-only Custom with storage (no metrics).
func (tc *MonitoringTestCtx) ValidateMode4TracesOnlyWithStorage(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(func() {
		tc.cleanupGroup(t, "")
	})

	tc.waitForPrometheusPodsGone(t)
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withNoMetrics(),
		withMonitoringTraces(TracesStorageBackendPV, "", TracesStorageSize1Gi, DefaultRetention),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionTempoAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionInstrumentationAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionPersesAvailable, metav1.ConditionTrue),
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .status=="False" and .reason=="%s")] | length==1`,
				conditions.ConditionMonitoringStackAvailable, conditions.MetricsNotConfiguredReason),
			jq.Match(`.spec.metrics == null`),
			usageLogsMustBeAbsent(),
		)),
		WithEventuallyTimeout(15*time.Minute),
		WithCustomErrorMsg("Mode 4 traces-only: Tempo+Instrumentation+Perses Ready; MonitoringStack not configured"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.TempoMonolithic, types.NamespacedName{Name: TempoMonolithicName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue)),
		WithEventuallyTimeout(15*time.Minute),
	)
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Instrumentation, types.NamespacedName{Name: InstrumentationName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.metadata.name == "%s"`, InstrumentationName)),
	)

	tc.ensureOpenTelemetryCollectorReady(t)
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name: OpenTelemetryCollectorName, Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`
			(.spec.config.exporters | has("otlp/tempo")) and
			(.spec.config.service.pipelines.traces.exporters | contains(["otlp/tempo"]))
		`)),
		WithCustomErrorMsg("Mode 4 traces-only: collector traces pipeline must include built-in otlp/tempo exporter"),
	)
	tc.ensureCollectorPipelineAbsent(t, "metrics/scrape")
	tc.ensureCollectorPipelineAbsent(t, "metrics/otlp")

	tc.ensurePersesStatefulSetReady(t)
	tc.ensureBuiltInMetricsOperandsGone(t)
}
