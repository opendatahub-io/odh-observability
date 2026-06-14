package e2e_test

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega"
)

func (tc *MonitoringTestCtx) runNegativeConditionTests(t *testing.T) {
	t.Helper()

	t.Run("Group 12: Negative Conditions", func(t *testing.T) {
		t.Run("Metrics negative conditions", tc.ValidateMonitoringMetricsNegativeConditions)
		t.Run("Traces negative conditions", tc.ValidateMonitoringTracesNegativeConditions)
		t.Run("Alerting negative conditions", tc.ValidateMonitoringAlertingNegativeConditions)
		t.Run("Perses negative conditions", tc.ValidateMonitoringPersesNegativeConditions)
		t.Run("NodeMetrics negative conditions", tc.ValidateMonitoringNodeMetricsNegativeConditions)
		t.Run("OpenTelemetry negative conditions", tc.ValidateMonitoringOpenTelemetryNegativeConditions)
	})
}

func (tc *MonitoringTestCtx) ValidateMonitoringMetricsNegativeConditions(t *testing.T) {
	t.Helper()
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withNoMetrics(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .reason=="%s")] | length==1`,
				conditions.ConditionMonitoringStackAvailable, conditions.MetricsNotConfiguredReason),
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .reason=="%s")] | length==1`,
				conditions.ConditionThanosQuerierAvailable, conditions.MetricsNotConfiguredReason),
		)),
	)
}

func (tc *MonitoringTestCtx) ValidateMonitoringTracesNegativeConditions(t *testing.T) {
	t.Helper()
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withNoTraces(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .reason=="%s")] | length==1`,
				conditions.ConditionTempoAvailable, conditions.TracesNotConfiguredReason),
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .reason=="%s")] | length==1`,
				conditions.ConditionInstrumentationAvailable, conditions.TracesNotConfiguredReason),
		)),
	)
}

func (tc *MonitoringTestCtx) ValidateMonitoringAlertingNegativeConditions(t *testing.T) {
	t.Helper()
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withNoAlerting(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(jq.Match(`[.status.conditions[] | select(.type=="%s" and .reason=="%s")] | length==1`,
			conditions.ConditionAlertingAvailable, conditions.AlertingNotConfiguredReason)),
	)
}

func (tc *MonitoringTestCtx) ValidateMonitoringPersesNegativeConditions(t *testing.T) {
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
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .status=="%s")] | length==1`,
				common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .reason=="%s")] | length==1`,
				conditions.ConditionPersesAvailable, conditions.MetricsAndTracesNotConfiguredReason),
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .reason=="%s")] | length==1`,
				conditions.ConditionPersesTempoDataSourceAvailable, conditions.TracesNotConfiguredReason),
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .reason=="%s")] | length==1`,
				conditions.ConditionPersesPrometheusDataSourceAvailable, conditions.MetricsNotConfiguredReason),
		)),
	)
}

func (tc *MonitoringTestCtx) ValidateMonitoringNodeMetricsNegativeConditions(t *testing.T) {
	t.Helper()
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withNoMetrics(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .status=="%s")] | length==1`,
				common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .reason=="%s")] | length==1`,
				conditions.ConditionNodeMetricsEndpointAvailable, conditions.MetricsNotConfiguredReason),
		)),
	)
}

func (tc *MonitoringTestCtx) ValidateMonitoringOpenTelemetryNegativeConditions(t *testing.T) {
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
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .status=="%s")] | length==1`,
				common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`[.status.conditions[] | select(.type=="%s" and .reason=="%s")] | length==1`,
				conditions.ConditionOpenTelemetryCollectorAvailable, conditions.MetricsAndTracesNotConfiguredReason),
		)),
	)
}
