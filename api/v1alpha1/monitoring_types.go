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

package v1alpha1

import (
	platformcommon "github.com/opendatahub-io/odh-platform-utilities/api/common"
	resource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
)

const (
	MonitoringServiceName = "monitoring"
	// MonitoringInstanceName is the singleton name enforced by CEL validation.
	MonitoringInstanceName = "default-monitoring"
	MonitoringKind         = "Monitoring"
)

// Compile-time check that Monitoring implements PlatformObject.
var _ platformcommon.PlatformObject = (*Monitoring)(nil)

// MonitoringSpec defines the desired state of Monitoring.
// +kubebuilder:validation:XValidation:rule="has(self.alerting) ? (has(self.metrics) && has(self.metrics.storage)) : true",message="Alerting configuration requires metrics.storage to be configured"
// +kubebuilder:validation:XValidation:rule="!has(self.collectorReplicas) || (self.collectorReplicas > 0 && ((has(self.metrics) && self.metrics.storage != null) || self.traces != null))",message="CollectorReplicas can only be set when metrics.storage or traces are configured, and must be > 0"
type MonitoringSpec struct {
	// ManagementState controls whether the operator actively manages the module (Managed) or removes it (Removed).
	platformcommon.ManagementSpec `json:",inline"`

	// Namespace is the target namespace where monitoring resources are deployed.
	// +kubebuilder:default=opendatahub
	// +kubebuilder:validation:Pattern="^([a-z0-9]([-a-z0-9]*[a-z0-9])?)?$"
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Namespace is immutable"
	Namespace string `json:"namespace,omitempty"`

	// Metrics configures metrics collection via the MonitoringStack operator.
	Metrics *Metrics `json:"metrics,omitempty"`

	// Traces configures distributed tracing via the Tempo operator.
	Traces *Traces `json:"traces,omitempty"`

	// Alerting configures Prometheus alerting rules.
	Alerting *Alerting `json:"alerting,omitempty"`

	// CollectorReplicas specifies the number of replicas in the OpenTelemetry collector.
	// Defaults to 1 on single-node clusters and 2 on multi-node clusters.
	// +kubebuilder:validation:Minimum=0
	CollectorReplicas int32 `json:"collectorReplicas,omitempty"`
}

// Metrics defines the desired state of metrics collection.
// +kubebuilder:validation:XValidation:rule="has(self.storage) || !has(self.replicas) || self.replicas == 0",message="Non-zero replicas require metrics.storage to be configured"
type Metrics struct {
	Storage *MetricsStorage `json:"storage,omitempty"`
	// Replicas specifies the number of replicas in the MonitoringStack.
	// +kubebuilder:validation:Minimum=0
	Replicas int32 `json:"replicas,omitempty"`
	// Exporters defines custom metrics exporters for external observability tools.
	// The configuration follows the OpenTelemetry Collector exporter format.
	// Reserved names 'prometheus' and 'otlp/tempo' cannot be used.
	// Maximum 10 exporters allowed.
	// +optional
	// +kubebuilder:validation:XValidation:rule="!('prometheus' in self)",message="exporter name 'prometheus' is reserved and cannot be used"
	// +kubebuilder:validation:XValidation:rule="!('otlp/tempo' in self)",message="exporter name 'otlp/tempo' is reserved and cannot be used"
	// +kubebuilder:validation:XValidation:rule="size(self) <= 10",message="maximum 10 exporters allowed"
	Exporters map[string]runtime.RawExtension `json:"exporters,omitempty"`
}

// MetricsStorage defines the storage configuration for the MonitoringStack.
type MetricsStorage struct {
	// Size specifies the PVC storage size (e.g. "5Gi", "10Mi").
	Size resource.Quantity `json:"size,omitempty"`
	// Retention specifies how long metrics data is retained (e.g. "1d", "2w").
	Retention string `json:"retention,omitempty"`
}

// Traces defines the configuration for distributed traces collection.
type Traces struct {
	Storage TracesStorage `json:"storage"`
	// SampleRatio determines the sampling rate for traces (0.0–1.0).
	// +kubebuilder:validation:Pattern="^(0(\\.[0-9]+)?|1(\\.0+)?)$"
	SampleRatio string `json:"sampleRatio,omitempty"`
	// TLS configures TLS for Tempo gRPC connections.
	// +optional
	TLS *TracesTLS `json:"tls,omitempty"`
	// Exporters defines custom trace exporters for external observability tools.
	// +optional
	Exporters map[string]runtime.RawExtension `json:"exporters,omitempty"`
}

// TracesTLS defines TLS configuration for Tempo ingestion and query APIs.
type TracesTLS struct {
	// Enabled enables TLS for Tempo OTLP ingestion (gRPC/HTTP) and query APIs.
	Enabled bool `json:"enabled,omitempty"`
	// CertificateSecret is the name of the Secret containing TLS certificates.
	CertificateSecret string `json:"certificateSecret,omitempty"`
	// CAConfigMap is the name of the ConfigMap containing the CA certificate.
	CAConfigMap string `json:"caConfigMap,omitempty"`
}

// Storage backend type constants.
const (
	StorageBackendPV  = "pv"
	StorageBackendS3  = "s3"
	StorageBackendGCS = "gcs"
)

// TracesStorage defines the storage backend for Tempo.
// +kubebuilder:validation:XValidation:rule="self.backend != 'pv' ? (has(self.secret) && self.secret != \"\") : true",message="When backend is s3 or gcs, the 'secret' field must be specified and non-empty"
// +kubebuilder:validation:XValidation:rule="self.backend != 'pv' ? !has(self.size) : true",message="Size is supported when backend is pv only"
type TracesStorage struct {
	// Backend defines the storage type: "pv", "s3", or "gcs".
	// +kubebuilder:validation:Enum="pv";"s3";"gcs"
	Backend string `json:"backend"`
	// Size specifies storage size (PV backend only).
	// +optional
	Size string `json:"size,omitempty"`
	// Secret is the name of the Secret with storage credentials (non-PV backends).
	// +optional
	Secret string `json:"secret,omitempty"`
	// Retention specifies how long trace data is retained (e.g. "60m", "10h").
	Retention metav1.Duration `json:"retention,omitempty"`
}

// Alerting configures Prometheus alerting rules.
type Alerting struct{}

// MonitoringStatus defines the observed state of Monitoring.
type MonitoringStatus struct {
	platformcommon.Status `json:",inline"`

	// Releases lists the deployed component versions.
	platformcommon.ComponentReleaseStatus `json:",inline"`

	// URL is the dashboard endpoint when available.
	URL string `json:"url,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:validation:XValidation:rule="self.metadata.name == 'default-monitoring'",message="Monitoring name must be default-monitoring"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`,description="Ready"
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].reason`,description="Reason"
// +kubebuilder:printcolumn:name="URL",type=string,JSONPath=`.status.url`,description="URL"

// Monitoring is the Schema for the monitorings API.
type Monitoring struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MonitoringSpec   `json:"spec,omitempty"`
	Status MonitoringStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// MonitoringList contains a list of Monitoring.
type MonitoringList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Monitoring `json:"items"`
}

func (m *Monitoring) GetStatus() *platformcommon.Status {
	return &m.Status.Status
}

func (m *Monitoring) GetConditions() []platformcommon.Condition {
	return m.Status.Status.Conditions
}

func (m *Monitoring) SetConditions(conditions []platformcommon.Condition) {
	m.Status.Status.Conditions = conditions
}

func (m *Monitoring) GetReleaseStatus() *platformcommon.ComponentReleaseStatus {
	return &m.Status.ComponentReleaseStatus
}

func (m *Monitoring) SetReleaseStatus(s platformcommon.ComponentReleaseStatus) {
	m.Status.ComponentReleaseStatus = s
}

func init() {
	SchemeBuilder.Register(&Monitoring{}, &MonitoringList{})
}
