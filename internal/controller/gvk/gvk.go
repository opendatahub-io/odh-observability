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

// Package gvk provides GroupVersionKind constants for external CRDs used by
// the monitoring controller.
package gvk

import "k8s.io/apimachinery/pkg/runtime/schema"

//nolint:gochecknoglobals
var (
	// Monitoring CR (services.platform.opendatahub.io/v1alpha1).
	Monitoring = schema.GroupVersionKind{
		Group:   "services.platform.opendatahub.io",
		Version: "v1alpha1",
		Kind:    "Monitoring",
	}

	// MonitoringStack (monitoring.rhobs/v1alpha1) from Cluster Observability Operator.
	MonitoringStack = schema.GroupVersionKind{
		Group:   "monitoring.rhobs",
		Version: "v1alpha1",
		Kind:    "MonitoringStack",
	}

	// ThanosQuerier (monitoring.rhobs/v1alpha1).
	ThanosQuerier = schema.GroupVersionKind{
		Group:   "monitoring.rhobs",
		Version: "v1alpha1",
		Kind:    "ThanosQuerier",
	}

	// TempoMonolithic (tempo.grafana.com/v1alpha1).
	TempoMonolithic = schema.GroupVersionKind{
		Group:   "tempo.grafana.com",
		Version: "v1alpha1",
		Kind:    "TempoMonolithic",
	}

	// TempoStack (tempo.grafana.com/v1alpha1).
	TempoStack = schema.GroupVersionKind{
		Group:   "tempo.grafana.com",
		Version: "v1alpha1",
		Kind:    "TempoStack",
	}

	// OpenTelemetryCollector (opentelemetry.io/v1beta1).
	OpenTelemetryCollector = schema.GroupVersionKind{
		Group:   "opentelemetry.io",
		Version: "v1beta1",
		Kind:    "OpenTelemetryCollector",
	}

	// Instrumentation (opentelemetry.io/v1alpha1).
	Instrumentation = schema.GroupVersionKind{
		Group:   "opentelemetry.io",
		Version: "v1alpha1",
		Kind:    "Instrumentation",
	}

	// ServiceMonitor (monitoring.rhobs/v1) for RHOBS stack.
	ServiceMonitor = schema.GroupVersionKind{
		Group:   "monitoring.rhobs",
		Version: "v1",
		Kind:    "ServiceMonitor",
	}

	// CoreosServiceMonitor (monitoring.coreos.com/v1) — webhook target.
	CoreosServiceMonitor = schema.GroupVersionKind{
		Group:   "monitoring.coreos.com",
		Version: "v1",
		Kind:    "ServiceMonitor",
	}

	// CoreosPodMonitor (monitoring.coreos.com/v1) — webhook target.
	CoreosPodMonitor = schema.GroupVersionKind{
		Group:   "monitoring.coreos.com",
		Version: "v1",
		Kind:    "PodMonitor",
	}

	// PrometheusRule (monitoring.rhobs/v1).
	PrometheusRule = schema.GroupVersionKind{
		Group:   "monitoring.rhobs",
		Version: "v1",
		Kind:    "PrometheusRule",
	}

	// PersesV1Alpha1 (perses.dev/v1alpha1).
	PersesV1Alpha1 = schema.GroupVersionKind{
		Group:   "perses.dev",
		Version: "v1alpha1",
		Kind:    "Perses",
	}

	// PersesV1Alpha2 (perses.dev/v1alpha2).
	PersesV1Alpha2 = schema.GroupVersionKind{
		Group:   "perses.dev",
		Version: "v1alpha2",
		Kind:    "Perses",
	}

	// PersesDatasourceV1Alpha1 (perses.dev/v1alpha1).
	PersesDatasourceV1Alpha1 = schema.GroupVersionKind{
		Group:   "perses.dev",
		Version: "v1alpha1",
		Kind:    "PersesDatasource",
	}

	// PersesDatasourceV1Alpha2 (perses.dev/v1alpha2).
	PersesDatasourceV1Alpha2 = schema.GroupVersionKind{
		Group:   "perses.dev",
		Version: "v1alpha2",
		Kind:    "PersesDatasource",
	}

	// PersesDashboardV1Alpha1 (perses.dev/v1alpha1).
	PersesDashboardV1Alpha1 = schema.GroupVersionKind{
		Group:   "perses.dev",
		Version: "v1alpha1",
		Kind:    "PersesDashboard",
	}

	// PersesDashboardV1Alpha2 (perses.dev/v1alpha2).
	PersesDashboardV1Alpha2 = schema.GroupVersionKind{
		Group:   "perses.dev",
		Version: "v1alpha2",
		Kind:    "PersesDashboard",
	}

	// CertManagerIssuer (cert-manager.io/v1).
	CertManagerIssuer = schema.GroupVersionKind{
		Group:   "cert-manager.io",
		Version: "v1",
		Kind:    "Issuer",
	}

	// CertManagerCertificate (cert-manager.io/v1).
	CertManagerCertificate = schema.GroupVersionKind{
		Group:   "cert-manager.io",
		Version: "v1",
		Kind:    "Certificate",
	}

	// ValidatingAdmissionPolicy (admissionregistration.k8s.io/v1).
	ValidatingAdmissionPolicy = schema.GroupVersionKind{
		Group:   "admissionregistration.k8s.io",
		Version: "v1",
		Kind:    "ValidatingAdmissionPolicy",
	}

	// ValidatingAdmissionPolicyBinding (admissionregistration.k8s.io/v1).
	ValidatingAdmissionPolicyBinding = schema.GroupVersionKind{
		Group:   "admissionregistration.k8s.io",
		Version: "v1",
		Kind:    "ValidatingAdmissionPolicyBinding",
	}
)
