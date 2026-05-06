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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// conditionStatus returns the status of the named condition, or "" if absent.
func conditionStatus(cm *ConditionsManager, condType string) metav1.ConditionStatus {
	for _, c := range cm.All() {
		if c.Type == condType {
			return c.Status
		}
	}
	return ""
}

func ready(cm *ConditionsManager) metav1.ConditionStatus {
	return conditionStatus(cm, string(platformcommon.ConditionTypeReady))
}

func degraded(cm *ConditionsManager) metav1.ConditionStatus {
	return conditionStatus(cm, string(platformcommon.ConditionTypeDegraded))
}

func provisioning(cm *ConditionsManager) metav1.ConditionStatus {
	return conditionStatus(cm, string(platformcommon.ConditionTypeProvisioningSucceeded))
}

// markAllNotConfigured simulates a Monitoring CR with no features enabled.
func markAllNotConfigured(cm *ConditionsManager) {
	cm.MarkFalse(ConditionMonitoringStackAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
	cm.MarkFalse(ConditionThanosQuerierAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
	cm.MarkFalse(ConditionTempoAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkFalse(ConditionInstrumentationAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkFalse(ConditionOpenTelemetryCollectorAvailable, MetricsAndTracesNotConfiguredReason, MetricsAndTracesNotConfiguredMessage)
	cm.MarkFalse(ConditionAlertingAvailable, AlertingNotConfiguredReason, AlertingNotConfiguredMessage)
	cm.MarkFalse(ConditionPersesAvailable, MetricsAndTracesNotConfiguredReason, MetricsAndTracesNotConfiguredMessage)
	cm.MarkFalse(ConditionPersesTempoDataSourceAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkFalse(ConditionPersesPrometheusDataSourceAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
	cm.MarkFalse(ConditionNodeMetricsEndpointAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
}

// markAllTrue simulates a fully configured and working Monitoring CR.
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
	cm := NewConditionsManager(1)
	cm.MarkTrue(ConditionMonitoringAvailable)
	markAllNotConfigured(cm)

	cm.AggregateReady()

	if got := ready(cm); got != metav1.ConditionTrue {
		t.Errorf("Ready: want True, got %s", got)
	}
	if got := degraded(cm); got != metav1.ConditionFalse {
		t.Errorf("Degraded: want False, got %s", got)
	}
	if got := provisioning(cm); got != metav1.ConditionTrue {
		t.Errorf("ProvisioningSucceeded: want True, got %s", got)
	}
}

// TestAggregateReady_AllFeaturesWorking: all features configured and healthy.
func TestAggregateReady_AllFeaturesWorking(t *testing.T) {
	cm := NewConditionsManager(1)
	cm.MarkTrue(ConditionMonitoringAvailable)
	markAllTrue(cm)

	cm.AggregateReady()

	if got := ready(cm); got != metav1.ConditionTrue {
		t.Errorf("Ready: want True, got %s", got)
	}
	if got := degraded(cm); got != metav1.ConditionFalse {
		t.Errorf("Degraded: want False, got %s", got)
	}
	if got := provisioning(cm); got != metav1.ConditionTrue {
		t.Errorf("ProvisioningSucceeded: want True, got %s", got)
	}
}

// TestAggregateReady_ConfiguredFeatureFailing: metrics is configured but the
// MonitoringStack CRD is missing. Should be Ready=True, Degraded=True.
func TestAggregateReady_ConfiguredFeatureFailing(t *testing.T) {
	cm := NewConditionsManager(1)
	cm.MarkTrue(ConditionMonitoringAvailable)

	// Metrics configured but CRD absent.
	cm.MarkFalse(ConditionMonitoringStackAvailable, "MonitoringStackCRDNotFoundReason", "MonitoringStack CRD not found")
	cm.MarkFalse(ConditionThanosQuerierAvailable, "ThanosQuerierCRDNotFoundReason", "ThanosQuerier CRD not found")

	// Everything else not configured.
	cm.MarkFalse(ConditionTempoAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkFalse(ConditionInstrumentationAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkFalse(ConditionOpenTelemetryCollectorAvailable, MetricsAndTracesNotConfiguredReason, MetricsAndTracesNotConfiguredMessage)
	cm.MarkFalse(ConditionAlertingAvailable, AlertingNotConfiguredReason, AlertingNotConfiguredMessage)
	cm.MarkFalse(ConditionPersesAvailable, MetricsAndTracesNotConfiguredReason, MetricsAndTracesNotConfiguredMessage)
	cm.MarkFalse(ConditionPersesTempoDataSourceAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkFalse(ConditionPersesPrometheusDataSourceAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
	cm.MarkFalse(ConditionNodeMetricsEndpointAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)

	cm.AggregateReady()

	if got := ready(cm); got != metav1.ConditionTrue {
		t.Errorf("Ready: want True, got %s", got)
	}
	if got := degraded(cm); got != metav1.ConditionTrue {
		t.Errorf("Degraded: want True, got %s", got)
	}
	if got := provisioning(cm); got != metav1.ConditionTrue {
		t.Errorf("ProvisioningSucceeded: want True, got %s", got)
	}
}

// TestAggregateReady_PreconditionsFailed: required operators not installed.
// Should be Ready=False, Degraded=False, ProvisioningSucceeded=False.
func TestAggregateReady_PreconditionsFailed(t *testing.T) {
	cm := NewConditionsManager(1)
	cm.MarkFalse(ConditionMonitoringAvailable, MissingOperatorReason, "OpenTelemetry operator not found")
	markAllNotConfigured(cm)

	cm.AggregateReady()

	if got := ready(cm); got != metav1.ConditionFalse {
		t.Errorf("Ready: want False, got %s", got)
	}
	if got := degraded(cm); got != metav1.ConditionFalse {
		t.Errorf("Degraded: want False, got %s", got)
	}
	if got := provisioning(cm); got != metav1.ConditionFalse {
		t.Errorf("ProvisioningSucceeded: want False, got %s", got)
	}
}

// TestAggregateReady_MixedNotConfiguredAndFailing: some features not configured,
// one configured feature failing. Degraded=True, Ready=True.
func TestAggregateReady_MixedNotConfiguredAndFailing(t *testing.T) {
	cm := NewConditionsManager(1)
	cm.MarkTrue(ConditionMonitoringAvailable)

	// Traces configured, but Tempo CRD is missing.
	cm.MarkFalse(ConditionTempoAvailable, "TempoMonolithicCRDNotFoundReason", "TempoMonolithic CRD not found")
	cm.MarkFalse(ConditionInstrumentationAvailable, "InstrumentationCRDNotFoundReason", "Instrumentation CRD not found")
	cm.MarkTrue(ConditionOpenTelemetryCollectorAvailable)

	// Metrics not configured.
	cm.MarkFalse(ConditionMonitoringStackAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
	cm.MarkFalse(ConditionThanosQuerierAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
	cm.MarkFalse(ConditionAlertingAvailable, AlertingNotConfiguredReason, AlertingNotConfiguredMessage)
	cm.MarkFalse(ConditionPersesAvailable, MetricsAndTracesNotConfiguredReason, MetricsAndTracesNotConfiguredMessage)
	cm.MarkFalse(ConditionPersesTempoDataSourceAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkFalse(ConditionPersesPrometheusDataSourceAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
	cm.MarkFalse(ConditionNodeMetricsEndpointAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)

	cm.AggregateReady()

	if got := ready(cm); got != metav1.ConditionTrue {
		t.Errorf("Ready: want True, got %s", got)
	}
	if got := degraded(cm); got != metav1.ConditionTrue {
		t.Errorf("Degraded: want True, got %s", got)
	}
}

// TestAggregateReady_ConfiguredFeatureInitializing: a configured feature still
// in Unknown state should produce Ready=Unknown, Degraded=False.
func TestAggregateReady_ConfiguredFeatureInitializing(t *testing.T) {
	cm := NewConditionsManager(1)
	cm.MarkTrue(ConditionMonitoringAvailable)

	// MonitoringStackAvailable stays Unknown (default from NewConditionsManager)
	// to simulate a configured feature that hasn't finished initializing yet.
	// Mark everything else as not configured.
	cm.MarkFalse(ConditionThanosQuerierAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
	cm.MarkFalse(ConditionTempoAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkFalse(ConditionInstrumentationAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkFalse(ConditionOpenTelemetryCollectorAvailable, MetricsAndTracesNotConfiguredReason, MetricsAndTracesNotConfiguredMessage)
	cm.MarkFalse(ConditionAlertingAvailable, AlertingNotConfiguredReason, AlertingNotConfiguredMessage)
	cm.MarkFalse(ConditionPersesAvailable, MetricsAndTracesNotConfiguredReason, MetricsAndTracesNotConfiguredMessage)
	cm.MarkFalse(ConditionPersesTempoDataSourceAvailable, TracesNotConfiguredReason, TracesNotConfiguredMessage)
	cm.MarkFalse(ConditionPersesPrometheusDataSourceAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)
	cm.MarkFalse(ConditionNodeMetricsEndpointAvailable, MetricsNotConfiguredReason, MetricsNotConfiguredMessage)

	cm.AggregateReady()

	if got := ready(cm); got != metav1.ConditionUnknown {
		t.Errorf("Ready: want Unknown, got %s", got)
	}
	if got := degraded(cm); got != metav1.ConditionFalse {
		t.Errorf("Degraded: want False, got %s", got)
	}
}

// TestPhase_Ready: Phase() returns PhaseReady when Ready=True.
func TestPhase_Ready(t *testing.T) {
	cm := NewConditionsManager(1)
	cm.MarkTrue(ConditionMonitoringAvailable)
	markAllNotConfigured(cm)
	cm.AggregateReady()

	if got := cm.Phase(); got != platformcommon.PhaseReady {
		t.Errorf("Phase: want %q, got %q", platformcommon.PhaseReady, got)
	}
}

// TestPhase_NotReady: Phase() returns PhaseNotReady when Ready=False.
func TestPhase_NotReady(t *testing.T) {
	cm := NewConditionsManager(1)
	cm.MarkFalse(ConditionMonitoringAvailable, MissingOperatorReason, "missing")
	markAllNotConfigured(cm)
	cm.AggregateReady()

	if got := cm.Phase(); got != platformcommon.PhaseNotReady {
		t.Errorf("Phase: want %q, got %q", platformcommon.PhaseNotReady, got)
	}
}
