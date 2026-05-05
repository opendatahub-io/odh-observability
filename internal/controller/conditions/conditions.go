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
	"sort"
	"time"

	platformcommon "github.com/opendatahub-io/odh-platform-utilities/api/common"
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
	MissingOperatorReason                    = "MissingOperator"
	MetricsNotConfiguredReason               = "MetricsNotConfigured"
	TracesNotConfiguredReason                = "TracesNotConfigured"
	AlertingNotConfiguredReason              = "AlertingNotConfigured"
	MetricsAndTracesNotConfiguredReason      = "MetricsAndTracesNotConfigured"
	MetricsAndTracesNotConfiguredMessage     = "Metrics and traces are not configured in Monitoring CR"
)

// Message constants.
const (
	MetricsNotConfiguredMessage  = "Metrics not configured in Monitoring CR"
	TracesNotConfiguredMessage   = "Traces not configured in Monitoring CR"
	AlertingNotConfiguredMessage = "Alerting not configured in Monitoring CR"

	TempoOperatorMissingMessage                  = "Tempo operator must be installed for traces configuration"
	COOMissingMessage                            = "ClusterObservability operator must be installed for metrics configuration"
	OpenTelemetryCollectorOperatorMissingMessage = "OpenTelemetryCollector operator must be installed for OpenTelemetry configuration"
)

// allConditionTypes lists all condition types managed by this controller.
// The ConditionsManager initialises these to Unknown on each reconcile.
var allConditionTypes = []string{
	string(platformcommon.ConditionTypeReady),
	string(platformcommon.ConditionTypeProvisioningSucceeded),
	string(platformcommon.ConditionTypeDegraded),
	ConditionMonitoringAvailable,
	ConditionMonitoringStackAvailable,
	ConditionThanosQuerierAvailable,
	ConditionTempoAvailable,
	ConditionInstrumentationAvailable,
	ConditionOpenTelemetryCollectorAvailable,
	ConditionAlertingAvailable,
	ConditionPersesAvailable,
	ConditionPersesTempoDataSourceAvailable,
	ConditionPersesPrometheusDataSourceAvailable,
	ConditionNodeMetricsEndpointAvailable,
}

// ConditionsManager manages the set of conditions for a Monitoring CR reconcile cycle.
type ConditionsManager struct {
	conditions map[string]platformcommon.Condition
	generation int64
}

// NewConditionsManager creates a ConditionsManager with all conditions initialised to Unknown.
func NewConditionsManager(generation int64) *ConditionsManager {
	cm := &ConditionsManager{
		conditions: make(map[string]platformcommon.Condition, len(allConditionTypes)),
		generation: generation,
	}
	for _, ct := range allConditionTypes {
		cm.set(platformcommon.Condition{
			Type:               ct,
			Status:             metav1.ConditionUnknown,
			Reason:             "Initializing",
			Message:            "",
			LastTransitionTime: metav1.NewTime(time.Now()),
			ObservedGeneration: generation,
		})
	}
	return cm
}

// MarkTrue sets a condition to True.
func (cm *ConditionsManager) MarkTrue(condType string) {
	cm.set(platformcommon.Condition{
		Type:               condType,
		Status:             metav1.ConditionTrue,
		Reason:             "Available",
		LastTransitionTime: metav1.NewTime(time.Now()),
		ObservedGeneration: cm.generation,
	})
}

// MarkFalse sets a condition to False with the given reason and message.
func (cm *ConditionsManager) MarkFalse(condType, reason, message string) {
	cm.set(platformcommon.Condition{
		Type:               condType,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.NewTime(time.Now()),
		ObservedGeneration: cm.generation,
	})
}

// MarkUnknown sets a condition to Unknown.
func (cm *ConditionsManager) MarkUnknown(condType string) {
	cm.set(platformcommon.Condition{
		Type:               condType,
		Status:             metav1.ConditionUnknown,
		Reason:             "Progressing",
		LastTransitionTime: metav1.NewTime(time.Now()),
		ObservedGeneration: cm.generation,
	})
}

// AggregateReady computes and sets the Ready, ProvisioningSucceeded, and Degraded
// top-level conditions based on the monitoring-specific conditions.
//
// Conditions that are False because a feature is deliberately not configured
// (MetricsNotConfigured, TracesNotConfigured, etc.) are excluded from Degraded
// aggregation — they represent user intent, not failures.
//
// Ready = True when MonitoringAvailable is True and no configured feature is actively failing.
// ProvisioningSucceeded = True when MonitoringAvailable is True (manifests applied without error).
// Degraded = True when a configured feature is failing (CRD missing, operator unavailable, etc.).
func (cm *ConditionsManager) AggregateReady() {
	// MonitoringAvailable must be True for provisioning to succeed.
	monAvail, ok := cm.conditions[ConditionMonitoringAvailable]
	if !ok || monAvail.Status != metav1.ConditionTrue {
		cm.MarkFalse(string(platformcommon.ConditionTypeProvisioningSucceeded),
			"PreconditionsFailed", "Required operators are not installed")
		cm.MarkFalse(string(platformcommon.ConditionTypeReady),
			"PreconditionsFailed", "Required operators are not installed")
		cm.MarkFalse(string(platformcommon.ConditionTypeDegraded), "NotDegraded", "")
		return
	}

	// notConfiguredReasons are intentional absences — the user has not enabled a
	// feature. They do not indicate failure and must not contribute to Degraded.
	notConfiguredReasons := map[string]bool{
		MetricsNotConfiguredReason:          true,
		TracesNotConfiguredReason:           true,
		AlertingNotConfiguredReason:         true,
		MetricsAndTracesNotConfiguredReason: true,
	}

	featureConditions := []string{
		ConditionMonitoringStackAvailable,
		ConditionThanosQuerierAvailable,
		ConditionTempoAvailable,
		ConditionInstrumentationAvailable,
		ConditionOpenTelemetryCollectorAvailable,
		ConditionAlertingAvailable,
		ConditionPersesAvailable,
		ConditionPersesTempoDataSourceAvailable,
		ConditionPersesPrometheusDataSourceAvailable,
		ConditionNodeMetricsEndpointAvailable,
	}

	anyFailing := false // a configured feature is actively failing
	anyUnknown := false // a configured feature is still initialising

	for _, ct := range featureConditions {
		c, ok := cm.conditions[ct]
		if !ok {
			continue
		}
		switch c.Status {
		case metav1.ConditionFalse:
			if !notConfiguredReasons[c.Reason] {
				anyFailing = true
			}
		case metav1.ConditionUnknown:
			anyUnknown = true
		}
	}

	// Provisioning succeeded: MonitoringAvailable is True, so manifests applied cleanly.
	cm.MarkTrue(string(platformcommon.ConditionTypeProvisioningSucceeded))

	switch {
	case anyFailing:
		// A configured feature is failing; the module is operational for working
		// features but degraded overall.
		cm.MarkTrue(string(platformcommon.ConditionTypeReady))
		cm.MarkTrue(string(platformcommon.ConditionTypeDegraded))
	case anyUnknown:
		// Configured features are still initialising.
		cm.MarkUnknown(string(platformcommon.ConditionTypeReady))
		cm.MarkFalse(string(platformcommon.ConditionTypeDegraded), "NotDegraded", "")
	default:
		// All configured features are working (or nothing is configured).
		cm.MarkTrue(string(platformcommon.ConditionTypeReady))
		cm.MarkFalse(string(platformcommon.ConditionTypeDegraded), "NotDegraded", "")
	}
}

// Phase returns the top-level lifecycle phase derived from the Ready condition.
func (cm *ConditionsManager) Phase() platformcommon.Phase {
	c, ok := cm.conditions[string(platformcommon.ConditionTypeReady)]
	if ok && c.Status == metav1.ConditionTrue {
		return platformcommon.PhaseReady
	}
	return platformcommon.PhaseNotReady
}

// All returns the current conditions as a slice, sorted by Type for deterministic output.
func (cm *ConditionsManager) All() []platformcommon.Condition {
	result := make([]platformcommon.Condition, 0, len(cm.conditions))
	for _, c := range cm.conditions {
		result = append(result, c)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Type < result[j].Type
	})
	return result
}

func (cm *ConditionsManager) set(c platformcommon.Condition) {
	// Preserve LastTransitionTime if status hasn't changed.
	if existing, ok := cm.conditions[c.Type]; ok {
		if existing.Status == c.Status {
			c.LastTransitionTime = existing.LastTransitionTime
		}
	}
	cm.conditions[c.Type] = c
}
