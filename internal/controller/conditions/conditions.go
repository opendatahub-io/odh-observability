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

// Package conditions provides condition type constants and a ConditionsManager
// for the monitoring controller.
package conditions

import (
	platformcommon "github.com/opendatahub-io/odh-platform-utilities/api/common"
	libconditions "github.com/opendatahub-io/odh-platform-utilities/pkg/controller/conditions"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Monitoring-specific condition type constants.
const (
	// ConditionMonitoringAvailable is the top-level condition indicating whether
	// all prerequisite operators are installed and monitoring is operational.
	ConditionMonitoringAvailable = "MonitoringAvailable"

	// ConditionMonitoringStackAvailable indicates the MonitoringStack CR is deployed.
	ConditionMonitoringStackAvailable = "MonitoringStackAvailable"

	// ConditionThanosQuerierAvailable indicates the ThanosQuerier CR is deployed.
	ConditionThanosQuerierAvailable = "ThanosQuerierAvailable"

	// ConditionTempoAvailable indicates the Tempo CR is deployed.
	ConditionTempoAvailable = "TempoAvailable"

	// ConditionInstrumentationAvailable indicates the Instrumentation CR is deployed.
	ConditionInstrumentationAvailable = "InstrumentationAvailable"

	// ConditionOpenTelemetryCollectorAvailable indicates the OTel collector is deployed.
	ConditionOpenTelemetryCollectorAvailable = "OpenTelemetryCollectorAvailable"

	// ConditionAlertingAvailable indicates Prometheus alerting rules are deployed.
	ConditionAlertingAvailable = "AlertingAvailable"

	// ConditionPersesAvailable indicates the Perses CR is deployed.
	ConditionPersesAvailable = "PersesAvailable"

	// ConditionPersesTempoDataSourceAvailable indicates the Perses Tempo datasource is deployed.
	ConditionPersesTempoDataSourceAvailable = "PersesTempoDataSourceAvailable"

	// ConditionPersesPrometheusDataSourceAvailable indicates the Perses Prometheus datasource is deployed.
	ConditionPersesPrometheusDataSourceAvailable = "PersesPrometheusDataSourceAvailable"

	// ConditionNodeMetricsEndpointAvailable indicates the node metrics proxy is deployed.
	ConditionNodeMetricsEndpointAvailable = "NodeMetricsEndpointAvailable"
)

// Reason constants.
const (
	MissingOperatorReason                = "MissingOperator"
	MetricsNotConfiguredReason           = "MetricsNotConfigured"
	TracesNotConfiguredReason            = "TracesNotConfigured"
	AlertingNotConfiguredReason          = "AlertingNotConfigured"
	MetricsAndTracesNotConfiguredReason  = "MetricsAndTracesNotConfigured"
	MetricsAndTracesNotConfiguredMessage = "Metrics and traces are not configured in Monitoring CR"
)

// Message constants.
const (
	MetricsNotConfiguredMessage  = "Metrics not configured in Monitoring CR"
	TracesNotConfiguredMessage   = "Traces not configured in Monitoring CR"
	AlertingNotConfiguredMessage = "Alerting not configured in Monitoring CR"

	TempoOperatorMissingMessage                 = "Tempo operator must be installed for traces configuration"
	COOMissingMessage                           = "ClusterObservability operator must be installed for metrics configuration"
	OpenTelemetryCollectorOperatorMissingMessage = "OpenTelemetryCollector operator must be installed for OpenTelemetry configuration"
)

// featureConditionTypes lists the feature-specific condition types that
// participate in the Ready/Degraded aggregation.
var featureConditionTypes = map[string]bool{
	ConditionMonitoringStackAvailable:           true,
	ConditionThanosQuerierAvailable:             true,
	ConditionTempoAvailable:                     true,
	ConditionInstrumentationAvailable:           true,
	ConditionOpenTelemetryCollectorAvailable:     true,
	ConditionAlertingAvailable:                  true,
	ConditionPersesAvailable:                    true,
	ConditionPersesTempoDataSourceAvailable:     true,
	ConditionPersesPrometheusDataSourceAvailable: true,
	ConditionNodeMetricsEndpointAvailable:        true,
}

// ConditionsManager manages the set of conditions for a Monitoring CR reconcile cycle.
// It operates directly on the CR's status via ConditionsAccessor, ensuring proper
// LastTransitionTime handling across reconciles.
type ConditionsManager struct {
	accessor   platformcommon.ConditionsAccessor
	generation int64
}

// NewConditionsManager creates a ConditionsManager bound to the given accessor.
func NewConditionsManager(accessor platformcommon.ConditionsAccessor, generation int64) *ConditionsManager {
	return &ConditionsManager{
		accessor:   accessor,
		generation: generation,
	}
}

// MarkTrue sets a condition to True.
func (cm *ConditionsManager) MarkTrue(condType string) {
	libconditions.SetStatusCondition(cm.accessor, platformcommon.Condition{
		Type:               condType,
		Status:             metav1.ConditionTrue,
		Reason:             "Available",
		ObservedGeneration: cm.generation,
	})
}

// MarkFalse sets a condition to False with the given reason and message.
func (cm *ConditionsManager) MarkFalse(condType, reason, message string) {
	libconditions.SetStatusCondition(cm.accessor, platformcommon.Condition{
		Type:               condType,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: cm.generation,
	})
}

// MarkNotConfigured sets a condition to False with ConditionSeverityInfo,
// indicating the feature is intentionally not configured by the user.
// Info-severity conditions are excluded from Degraded aggregation.
func (cm *ConditionsManager) MarkNotConfigured(condType, reason, message string) {
	libconditions.SetStatusCondition(cm.accessor, platformcommon.Condition{
		Type:               condType,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		Severity:           platformcommon.ConditionSeverityInfo,
		ObservedGeneration: cm.generation,
	})
}

// MarkUnknown sets a condition to Unknown.
func (cm *ConditionsManager) MarkUnknown(condType string) {
	libconditions.SetStatusCondition(cm.accessor, platformcommon.Condition{
		Type:               condType,
		Status:             metav1.ConditionUnknown,
		Reason:             "Progressing",
		ObservedGeneration: cm.generation,
	})
}

// AggregateReady computes and sets the Ready, ProvisioningSucceeded, and Degraded
// top-level conditions based on the monitoring-specific conditions.
//
// Conditions with ConditionSeverityInfo (intentionally not configured features)
// are excluded from Degraded aggregation — they represent user intent, not failures.
//
// Ready = True when MonitoringAvailable is True and no configured feature is actively failing.
// ProvisioningSucceeded = True when MonitoringAvailable is True (manifests applied without error).
// Degraded = True when a configured feature is failing (CRD missing, operator unavailable, etc.).
func (cm *ConditionsManager) AggregateReady() {
	monAvail := libconditions.FindStatusCondition(cm.accessor, ConditionMonitoringAvailable)
	if monAvail == nil || monAvail.Status != metav1.ConditionTrue {
		cm.MarkFalse(string(platformcommon.ConditionTypeProvisioningSucceeded),
			"PreconditionsFailed", "Required operators are not installed")
		cm.MarkFalse(string(platformcommon.ConditionTypeReady),
			"PreconditionsFailed", "Required operators are not installed")
		cm.MarkFalse(string(platformcommon.ConditionTypeDegraded), "NotDegraded", "")
		return
	}

	anyFailing := false
	anyUnknown := false

	for _, c := range cm.accessor.GetConditions() {
		if !featureConditionTypes[c.Type] {
			continue
		}
		switch c.Status {
		case metav1.ConditionFalse:
			if c.Severity != platformcommon.ConditionSeverityInfo {
				anyFailing = true
			}
		case metav1.ConditionUnknown:
			anyUnknown = true
		}
	}

	cm.MarkTrue(string(platformcommon.ConditionTypeProvisioningSucceeded))

	switch {
	case anyFailing:
		cm.MarkTrue(string(platformcommon.ConditionTypeReady))
		cm.MarkTrue(string(platformcommon.ConditionTypeDegraded))
	case anyUnknown:
		cm.MarkUnknown(string(platformcommon.ConditionTypeReady))
		cm.MarkFalse(string(platformcommon.ConditionTypeDegraded), "NotDegraded", "")
	default:
		cm.MarkTrue(string(platformcommon.ConditionTypeReady))
		cm.MarkFalse(string(platformcommon.ConditionTypeDegraded), "NotDegraded", "")
	}
}

// Phase returns the top-level lifecycle phase derived from the Ready condition.
func (cm *ConditionsManager) Phase() platformcommon.Phase {
	if libconditions.IsStatusConditionTrue(cm.accessor, string(platformcommon.ConditionTypeReady)) {
		return platformcommon.PhaseReady
	}
	return platformcommon.PhaseNotReady
}
