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
// +kubebuilder:validation:XValidation:rule="!has(self.collectorReplicas) || (self.collectorReplicas > 0 && ((has(self.metrics) && (has(self.metrics.storage) || (has(self.metrics.exporters) && size(self.metrics.exporters) > 0))) || has(self.traces)))",message="CollectorReplicas can only be set when metrics (storage or exporters) or traces are configured, and must be > 0"
// +kubebuilder:validation:XValidation:rule="has(self.logs) ? has(self.logs.storage) : true",message="Log forwarding requires logs.storage to be configured for the LokiStack instance"
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

	// UsageLogs configures usage log collection and forwarding to Loki.
	UsageLogs *UsageLogs `json:"usageLogs,omitempty"`

	// Alerting configures Prometheus alerting rules.
	Alerting *Alerting `json:"alerting,omitempty"`

	// Logs configures cluster log forwarding via the ClusterLogForwarder operator.
	Logs *Logs `json:"logs,omitempty"`

	// CollectorReplicas specifies the number of replicas in the OpenTelemetry collector.
	// Defaults to 1 on single-node clusters and 2 on multi-node clusters.
	// +kubebuilder:validation:Minimum=0
	CollectorReplicas int32 `json:"collectorReplicas,omitempty"`
}

// Metrics defines the desired state of metrics collection.
// Built-in MonitoringStack requires storage; exporters-only configs (external backends) omit storage.
// +kubebuilder:validation:XValidation:rule="has(self.storage) || (has(self.exporters) && size(self.exporters) > 0)",message="metrics requires storage and/or at least one exporter"
// +kubebuilder:validation:XValidation:rule="has(self.storage) || !has(self.replicas) || self.replicas == 0",message="Non-zero replicas require metrics.storage to be configured"
type Metrics struct {
	// Storage configures the built-in MonitoringStack backend. Omit when using exporters only (external observability).
	// +optional
	Storage *MetricsStorage `json:"storage,omitempty"`
	// BuiltinDCGMScrapeEnabled controls the built-in HTTP scrape for DCGM exporter metrics.
	// It defaults to enabled; set false when collecting DCGM through a ServiceMonitor or PodMonitor instead.
	// +optional
	BuiltinDCGMScrapeEnabled *bool `json:"builtinDCGMScrapeEnabled,omitempty"`
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
// Built-in Tempo requires storage; exporters-only configs (external backends) omit storage.
// +kubebuilder:validation:XValidation:rule="has(self.storage) || (has(self.exporters) && size(self.exporters) > 0)",message="traces requires storage and/or at least one exporter"
type Traces struct {
	// Storage configures the built-in Tempo backend. Omit when using exporters only (external observability).
	// +optional
	Storage *TracesStorage `json:"storage,omitempty"`
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

// Logs defines the configuration for cluster log forwarding via the ClusterLogForwarder operator.
type Logs struct {
	// Storage configures the LokiStack storage backend for log forwarding.
	// Required: the operator deploys a LokiStack dedicated to log forwarding, separate from the
	// one usageLogs.storage deploys.
	// The two stacks must use different object storage buckets. Both serve the "application" tenant and
	// Loki keys objects by tenant rather than by stack, so a shared bucket makes them overwrite each
	// other's index and chunks. Separate secrets are not sufficient; the operator compares the resolved
	// endpoint and bucketnames and refuses to reconcile when they overlap.
	// +kubebuilder:validation:Required
	Storage *LokiStorageConfig `json:"storage"`

	// Size selects the log-forwarding LokiStack size. The default is 1x.extra-small;
	// 1x.demo uses fewer resources but does not provide redundancy.
	// +optional
	// +kubebuilder:default="1x.extra-small"
	// +kubebuilder:validation:Enum="1x.demo";"1x.pico";"1x.extra-small";"1x.small";"1x.medium"
	Size string `json:"size,omitempty"`

	// InferenceNamespaces lists the namespaces whose application logs should be forwarded to Loki.
	// +kubebuilder:validation:items:Pattern=`^[a-z0-9]([a-z0-9\-]{0,61}[a-z0-9])?$`
	// +kubebuilder:validation:items:MaxLength=63
	InferenceNamespaces []string `json:"inferenceNamespaces,omitempty"`
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

// UsageLogs defines the configuration for usage log collection.
// A cluster-local store requires storage; forwarding-only configs (a fleet hub) omit storage.
// +kubebuilder:validation:XValidation:rule="has(self.storage) || (has(self.exporters) && size(self.exporters) > 0)",message="usageLogs requires storage and/or at least one exporter"
type UsageLogs struct {
	// Storage configures the LokiStack storage backend (S3).
	// When configured, the operator deploys a LokiStack instance and auto-configures the collector endpoint.
	// This stack is separate from the one logs.storage deploys and must use a different object storage
	// bucket; see logs.storage for why a shared bucket is rejected.
	// Omit when forwarding only: no LokiStack is deployed and no object storage is needed.
	// +optional
	Storage *LokiStorageConfig `json:"storage,omitempty"`

	// Size selects the LokiStack t-shirt size for the usage-log store. The default
	// 1x.extra-small requests 1 CPU and 5.25Gi per ingester across three ingesters,
	// which does not schedule on smaller clusters; the LokiStack then never reports
	// Ready and the usage-logs collector is never deployed. 1x.demo carries identical
	// ingestion limits with a single ingester and no resource requests at all, so it
	// fits where the default cannot, but it has no redundancy. Ignored when storage is
	// omitted, since no LokiStack is deployed.
	// +optional
	// +kubebuilder:default="1x.extra-small"
	// +kubebuilder:validation:Enum="1x.demo";"1x.pico";"1x.extra-small";"1x.small";"1x.medium"
	Size string `json:"size,omitempty"`

	// Exporters defines additional usage-log exporters, for forwarding a copy of every
	// usage record to an observability backend outside this cluster — typically a
	// LokiStack on an ACM hub that aggregates a fleet. The configuration follows the
	// OpenTelemetry Collector exporter format. They are added alongside the local
	// write, which is left untouched, making it a dual-write rather than a redirect;
	// with storage omitted they are the only destination and nothing is written locally.
	// Reserved name 'otlphttp/loki' cannot be used.
	// Maximum 10 exporters allowed.
	// +optional
	// +kubebuilder:validation:XValidation:rule="!('otlphttp/loki' in self)",message="exporter name 'otlphttp/loki' is reserved and cannot be used"
	// +kubebuilder:validation:XValidation:rule="size(self) <= 10",message="maximum 10 exporters allowed"
	Exporters map[string]runtime.RawExtension `json:"exporters,omitempty"`

	// CredentialSecret names a Secret in the monitoring namespace holding a bearer
	// token under the key 'token', for authenticating to the Exporters' endpoint. It
	// is mounted as a file and read through a bearertokenauth extension named
	// 'bearertokenauth/remote', which an exporter selects with
	// auth: {authenticator: bearertokenauth/remote}.
	//
	// The token is never written into the collector object, and because the extension
	// re-reads the file, rotating the Secret reaches a running collector — projecting
	// a credential as an environment variable would not, since env vars are read once
	// at process start. A token also cannot be inlined into an exporter's headers:
	// exporter values are capped at 1024 characters and a ServiceAccount token is
	// longer than that.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	CredentialSecret string `json:"credentialSecret,omitempty"`

	// Attributes are resource attributes stamped onto records sent to Exporters only.
	// They never reach the local write, so a cluster identity added here cannot
	// disturb the spoke-local pipeline or its dashboards. Set cluster_name and
	// cluster_id here to attribute records to the cluster that produced them; the
	// gateway already uses cluster_name for its own upstream cluster, so these are
	// applied after the pipeline's cleanup stage rather than merged into it.
	// +optional
	// +kubebuilder:validation:MaxProperties=20
	Attributes map[string]string `json:"attributes,omitempty"`
}

// LokiStorageConfig defines storage configuration for LokiStack.
type LokiStorageConfig struct {
	// Type specifies the storage backend: "s3".
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=s3
	Type string `json:"type"`

	// SecretName is the name of the Secret containing storage credentials.
	// For S3: must contain keys: access_key_id, access_key_secret, bucketnames, endpoint, region, insecure, s3ForcePathStyle
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	// +kubebuilder:validation:MaxLength=253
	SecretName string `json:"secretName"`

	// CredentialMode specifies how credentials are provided to LokiStack.
	// Valid values: "static", "token", "token-cco".
	// +optional
	// +kubebuilder:default="static"
	// +kubebuilder:validation:Enum=static;token;token-cco
	CredentialMode string `json:"credentialMode,omitempty"`

	// StorageClassName specifies the storage class for LokiStack PVCs.
	// +optional
	// +kubebuilder:default="gp3-csi"
	// +kubebuilder:validation:MaxLength=253
	StorageClassName string `json:"storageClassName,omitempty"`
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

	// UsageLogsEndpoint is the Loki OTLP endpoint for sending usage logs.
	// Populated when usage-log storage is configured and its LokiStack is ready.
	// +optional
	UsageLogsEndpoint string `json:"usageLogsEndpoint,omitempty"`

	// LogsEndpoint is the Loki query endpoint for forwarded application logs.
	// Populated when logs is configured and its LokiStack is ready. Unlike
	// usageLogsEndpoint this is a read endpoint: the ClusterLogForwarder
	// addresses the LokiStack by name rather than by URL, so there is no write
	// endpoint to publish.
	// +optional
	LogsEndpoint string `json:"logsEndpoint,omitempty"`

	// UsageLogsForwarding names the remote usage-log exporters carried by the deployed
	// collector. It reports what passed validation and was rendered, so it is empty
	// when no collector is deployed, which the UsageLogsCollectorAvailable condition
	// explains.
	// +optional
	UsageLogsForwarding []string `json:"usageLogsForwarding,omitempty"`
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
