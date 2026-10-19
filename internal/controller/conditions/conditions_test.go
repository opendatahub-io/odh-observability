/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package conditions

import (
	"testing"

	platformcommon "github.com/opendatahub-io/odh-platform-utilities/api/common"
	libconditions "github.com/opendatahub-io/odh-platform-utilities/pkg/controller/conditions"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type testAccessor struct {
	conditions []platformcommon.Condition
}

func (a *testAccessor) GetConditions() []platformcommon.Condition  { return a.conditions }
func (a *testAccessor) SetConditions(c []platformcommon.Condition) { a.conditions = c }

func newTestCM() (*ConditionsManager, *testAccessor) {
	acc := &testAccessor{}
	return NewConditionsManager(acc, 1), acc
}

func conditionStatus(acc platformcommon.ConditionsAccessor, condType string) metav1.ConditionStatus {
	c := libconditions.FindStatusCondition(acc, condType)
	if c == nil {
		return ""
	}
	return c.Status
}

func ready(acc platformcommon.ConditionsAccessor) metav1.ConditionStatus {
	return conditionStatus(acc, string(platformcommon.ConditionTypeReady))
}

func degraded(acc platformcommon.ConditionsAccessor) metav1.ConditionStatus {
	return conditionStatus(acc, string(platformcommon.ConditionTypeDegraded))
}

func provisioning(acc platformcommon.ConditionsAccessor) metav1.ConditionStatus {
	return conditionStatus(acc, string(platformcommon.ConditionTypeProvisioningSucceeded))
}

func markAllNotConfigured(cm *ConditionsManager) {
	cm.MarkNotConfigured(ConditionMonitoringStackAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionThanosQuerierAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionTempoAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionInstrumentationAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionOpenTelemetryCollectorAvailable, MetricsAndTracesNotConfiguredReason, MetricsAndTracesNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionAlertingAvailable, AlertingNotConfiguredReason, AlertingNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionPersesAvailable, MetricsAndTracesNotConfiguredReason, MetricsAndTracesNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionPersesTempoDataSourceAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionPersesPrometheusDataSourceAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionNodeMetricsEndpointAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
}

func markAllTrue(cm *ConditionsManager) {
	cm.MarkTrue(ConditionMonitoringStackAvailable)
	cm.MarkTrue(ConditionThanosQuerierAvailable)
	cm.MarkTrue(ConditionTempoAvailable)
	cm.MarkTrue(ConditionInstrumentationAvailable)
	cm.MarkTrue(ConditionOpenTelemetryCollectorAvailable)
	cm.MarkTrue(ConditionAlertingAvailable)
	cm.MarkTrue(ConditionPersesAvailable)
	cm.MarkTrue(ConditionPersesTempoDataSourceAvailable)
	cm.MarkTrue(ConditionPersesPrometheusDataSourceAvailable)
	cm.MarkTrue(ConditionNodeMetricsEndpointAvailable)
}

// TestAggregateReady_NothingConfigured: a CR with no features enabled should be
// Ready=True and Degraded=False. The operator is running; there's nothing to provision.
func TestAggregateReady_NothingConfigured(t *testing.T) {
	cm, acc := newTestCM()
	cm.MarkTrue(ConditionMonitoringDependenciesReady)
	markAllNotConfigured(cm)

	cm.AggregateReady()

	if got := ready(acc); got != metav1.ConditionTrue {
		t.Errorf("Ready: want True, got %s", got)
	}
	if got := degraded(acc); got != metav1.ConditionFalse {
		t.Errorf("Degraded: want False, got %s", got)
	}
	if got := provisioning(acc); got != metav1.ConditionTrue {
		t.Errorf("ProvisioningSucceeded: want True, got %s", got)
	}
}

// TestAggregateReady_AllFeaturesWorking: all features configured and healthy.
func TestAggregateReady_AllFeaturesWorking(t *testing.T) {
	cm, acc := newTestCM()
	cm.MarkTrue(ConditionMonitoringDependenciesReady)
	markAllTrue(cm)

	cm.AggregateReady()

	if got := ready(acc); got != metav1.ConditionTrue {
		t.Errorf("Ready: want True, got %s", got)
	}
	if got := degraded(acc); got != metav1.ConditionFalse {
		t.Errorf("Degraded: want False, got %s", got)
	}
	if got := provisioning(acc); got != metav1.ConditionTrue {
		t.Errorf("ProvisioningSucceeded: want True, got %s", got)
	}
}

// TestAggregateReady_ConfiguredFeatureFailing: metrics is configured but the
// MonitoringStack CRD is missing. Should be Ready=False, Degraded=True.
func TestAggregateReady_ConfiguredFeatureFailing(t *testing.T) {
	cm, acc := newTestCM()
	cm.MarkTrue(ConditionMonitoringDependenciesReady)

	// Metrics configured but CRD absent (real failure, not "not configured").
	cm.MarkFalse(ConditionMonitoringStackAvailable, "MonitoringStackCRDNotFoundReason", "MonitoringStack CRD not found")
	cm.MarkFalse(ConditionThanosQuerierAvailable, "ThanosQuerierCRDNotFoundReason", "ThanosQuerier CRD not found")

	// Everything else not configured (info severity).
	cm.MarkNotConfigured(ConditionTempoAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionInstrumentationAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionOpenTelemetryCollectorAvailable, MetricsAndTracesNotConfiguredReason, MetricsAndTracesNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionAlertingAvailable, AlertingNotConfiguredReason, AlertingNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionPersesAvailable, MetricsAndTracesNotConfiguredReason, MetricsAndTracesNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionPersesTempoDataSourceAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionPersesPrometheusDataSourceAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionNodeMetricsEndpointAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)

	cm.AggregateReady()

	if got := ready(acc); got != metav1.ConditionFalse {
		t.Errorf("Ready: want False, got %s", got)
	}
	if got := degraded(acc); got != metav1.ConditionTrue {
		t.Errorf("Degraded: want True, got %s", got)
	}
	if got := provisioning(acc); got != metav1.ConditionTrue {
		t.Errorf("ProvisioningSucceeded: want True, got %s", got)
	}
}

// TestAggregateReady_PreconditionsFailed: required operators not installed.
// Should be Ready=False, Degraded=False, ProvisioningSucceeded=False.
func TestAggregateReady_PreconditionsFailed(t *testing.T) {
	cm, acc := newTestCM()
	cm.MarkFalse(ConditionMonitoringDependenciesReady, MissingOperatorReason, "OpenTelemetry operator not found")
	markAllNotConfigured(cm)

	cm.AggregateReady()

	if got := ready(acc); got != metav1.ConditionFalse {
		t.Errorf("Ready: want False, got %s", got)
	}
	if got := degraded(acc); got != metav1.ConditionFalse {
		t.Errorf("Degraded: want False, got %s", got)
	}
	if got := provisioning(acc); got != metav1.ConditionFalse {
		t.Errorf("ProvisioningSucceeded: want False, got %s", got)
	}
}

// TestAggregateReady_MixedNotConfiguredAndFailing: some features not configured,
// one configured feature failing. Degraded=True, Ready=False.
func TestAggregateReady_MixedNotConfiguredAndFailing(t *testing.T) {
	cm, acc := newTestCM()
	cm.MarkTrue(ConditionMonitoringDependenciesReady)

	// Traces configured, but Tempo CRD is missing (real failure).
	cm.MarkFalse(ConditionTempoAvailable, "TempoMonolithicCRDNotFoundReason", "TempoMonolithic CRD not found")
	cm.MarkFalse(ConditionInstrumentationAvailable, "InstrumentationCRDNotFoundReason", "Instrumentation CRD not found")
	cm.MarkTrue(ConditionOpenTelemetryCollectorAvailable)

	// Metrics not configured (info severity).
	cm.MarkNotConfigured(ConditionMonitoringStackAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionThanosQuerierAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionAlertingAvailable, AlertingNotConfiguredReason, AlertingNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionPersesAvailable, MetricsAndTracesNotConfiguredReason, MetricsAndTracesNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionPersesTempoDataSourceAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionPersesPrometheusDataSourceAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionNodeMetricsEndpointAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)

	cm.AggregateReady()

	if got := ready(acc); got != metav1.ConditionFalse {
		t.Errorf("Ready: want False, got %s", got)
	}
	if got := degraded(acc); got != metav1.ConditionTrue {
		t.Errorf("Degraded: want True, got %s", got)
	}
}

// TestMarkUnknown_CarriesReasonAndMessage: an Unknown condition has to say why
// it could not be observed. Callers distinguish "the readiness check failed"
// from "still starting up" by reason, and an Unknown that reported neither
// would be indistinguishable from an unset condition in the status.
func TestMarkUnknown_CarriesReasonAndMessage(t *testing.T) {
	cm, acc := newTestCM()

	cm.MarkUnknown(ConditionLokiStackAvailable, "ReadinessCheckFailed", "apiserver timed out")

	c := libconditions.FindStatusCondition(acc, ConditionLokiStackAvailable)
	if c == nil {
		t.Fatalf("condition %s not set", ConditionLokiStackAvailable)
	}
	if c.Status != metav1.ConditionUnknown {
		t.Errorf("Status: want Unknown, got %s", c.Status)
	}
	if c.Reason != "ReadinessCheckFailed" {
		t.Errorf("Reason: want ReadinessCheckFailed, got %q", c.Reason)
	}
	if c.Message != "apiserver timed out" {
		t.Errorf("Message: want %q, got %q", "apiserver timed out", c.Message)
	}
	// Severity Info would exclude the condition from Degraded aggregation,
	// which is only correct for features the user turned off.
	if c.Severity != "" {
		t.Errorf("Severity: want empty, got %q", c.Severity)
	}
	// Stamped so the status says when the stack was last observable, not when
	// it was last looked at.
	if c.LastTransitionTime.IsZero() {
		t.Error("LastTransitionTime must be stamped")
	}
	if c.ObservedGeneration != 1 {
		t.Errorf("ObservedGeneration: want 1, got %d", c.ObservedGeneration)
	}
}

// TestAggregateReady_ConfiguredFeatureInitializing: a configured feature still
// in Unknown state should produce Ready=Unknown, Degraded=False.
func TestAggregateReady_ConfiguredFeatureInitializing(t *testing.T) {
	cm, acc := newTestCM()
	cm.MarkTrue(ConditionMonitoringDependenciesReady)

	// Simulate a configured feature that hasn't finished initializing.
	cm.MarkUnknown(ConditionMonitoringStackAvailable, "Progressing", "")

	// Mark everything else as not configured.
	cm.MarkNotConfigured(ConditionThanosQuerierAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionTempoAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionInstrumentationAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionOpenTelemetryCollectorAvailable, MetricsAndTracesNotConfiguredReason, MetricsAndTracesNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionAlertingAvailable, AlertingNotConfiguredReason, AlertingNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionPersesAvailable, MetricsAndTracesNotConfiguredReason, MetricsAndTracesNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionPersesTempoDataSourceAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionPersesPrometheusDataSourceAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
	cm.MarkNotConfigured(ConditionNodeMetricsEndpointAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)

	cm.AggregateReady()

	if got := ready(acc); got != metav1.ConditionUnknown {
		t.Errorf("Ready: want Unknown, got %s", got)
	}
	if got := degraded(acc); got != metav1.ConditionFalse {
		t.Errorf("Degraded: want False, got %s", got)
	}
	// The aggregated reason stays Progressing whatever the feature's own
	// reason was: Ready summarises, the feature condition carries the detail.
	if c := libconditions.FindStatusCondition(acc, string(platformcommon.ConditionTypeReady)); c == nil ||
		c.Reason != "Progressing" {
		t.Errorf("Ready reason: want Progressing, got %v", c)
	}
}

// TestPhase_Ready: Phase() returns PhaseReady when Ready=True.
func TestPhase_Ready(t *testing.T) {
	cm, _ := newTestCM()
	cm.MarkTrue(ConditionMonitoringDependenciesReady)
	markAllNotConfigured(cm)
	cm.AggregateReady()

	if got := cm.Phase(); got != platformcommon.PhaseReady {
		t.Errorf("Phase: want %q, got %q", platformcommon.PhaseReady, got)
	}
}

// TestPhase_NotReady: Phase() returns PhaseNotReady when Ready=False.
func TestPhase_NotReady(t *testing.T) {
	cm, _ := newTestCM()
	cm.MarkFalse(ConditionMonitoringDependenciesReady, MissingOperatorReason, "missing")
	markAllNotConfigured(cm)
	cm.AggregateReady()

	if got := cm.Phase(); got != platformcommon.PhaseNotReady {
		t.Errorf("Phase: want %q, got %q", platformcommon.PhaseNotReady, got)
	}
}
