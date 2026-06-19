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

package controller

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/hashicorp/go-multierror"
	"gopkg.in/yaml.v3"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	"github.com/opendatahub-io/odh-platform-utilities/pkg/cluster/openshift"
)

const (
	opentelemetryOperator        = "opentelemetry-operator"
	clusterObservabilityOperator = "cluster-observability-operator"
	tempoOperator                = "tempo-operator"

	defaultStorageSize = "5Gi"
	defaultRetention   = "90d"

	defaultTracesSampleRatio = "0.1"
	defaultTracesBackend     = "pv"
	defaultTracesRetention   = "2160h"

	defaultCPULimit      = "1"
	defaultMemoryLimit   = "512Mi"
	defaultCPURequest    = "100m"
	defaultMemoryRequest = "256Mi"

	defaultCollectorCPULimit      = "1"
	defaultCollectorMemoryLimit   = "256Mi"
	defaultCollectorCPURequest    = "100m"
	defaultCollectorMemoryRequest = "256Mi"

	defaultTempoCPULimit      = "1"
	defaultTempoMemoryLimit   = "256Mi"
	defaultTempoCPURequest    = "100m"
	defaultTempoMemoryRequest = "256Mi"

	persesV1Alpha2 = "v1alpha2"

	// Security limits for exporter configurations.
	maxConfigFields      = 50    // Maximum number of fields in an exporter config.
	maxNestingDepth      = 10    // Maximum nesting depth to prevent deeply nested objects.
	maxStringLength      = 1024  // Maximum length for string values.
	maxArrayLength       = 100   // Maximum length for array values.
	maxExporterSize      = 10240 // Maximum size per exporter config (10KB).
	maxTotalExporterSize = 51200 // Maximum total size for all exporters combined (50KB).
)

var componentIDRE = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*(?:/[A-Za-z0-9][A-Za-z0-9_-]*)?$`)

// buildTemplateData constructs the data map passed to all YAML templates.
// persesAPIVersion is pre-resolved by the reconciler (via resolvePersesAPIVersion)
// to avoid redundant API calls; pass "" to use the default.
func buildTemplateData(ctx context.Context, c client.Client, monitoring *v1alpha1.Monitoring, persesAPIVersion string) (map[string]any, error) {
	log := logf.FromContext(ctx)
	operatorNamespace := os.Getenv("POD_NAMESPACE")

	if persesAPIVersion == "" {
		persesAPIVersion = persesV1Alpha2
	}

	isSNO, err := openshift.IsSingleNodeCluster(ctx, c)
	if err != nil {
		log.Error(err, "Failed to detect SNO cluster, defaulting to multi-node replica count")
	}

	templateData := map[string]any{
		"Namespace":            monitoring.Spec.Namespace,
		"Traces":               monitoring.Spec.Traces != nil,
		"Metrics":              monitoring.Spec.Metrics != nil,
		"AcceleratorMetrics":   monitoring.Spec.Metrics != nil,
		"OperatorNamespace":    operatorNamespace,
		"OperatorName":         getEnvOrDefault("OPERATOR_NAME", "odh-observability"),
		"OperatorPodPrefix":    getEnvOrDefault("OPERATOR_POD_PREFIX", "odh-observability"),
		"MetricsExporters":     make(map[string]string),
		"MetricsExporterNames": []string{},
		"PersesImage":          getPersesImage(),
		"PersesAPIVersion":     persesAPIVersion,
	}

	addResourceData(templateData)
	addImageURLs(templateData)

	if metrics := monitoring.Spec.Metrics; metrics != nil {
		if err := addMetricsData(metrics, isSNO, templateData); err != nil {
			return nil, err
		}
	}

	if traces := monitoring.Spec.Traces; traces != nil {
		if err := addTracesTemplateData(templateData, traces, monitoring.Spec.Namespace); err != nil {
			return nil, err
		}
	}

	// Apply SNO-aware defaulting when CollectorReplicas is unset.
	collectorReplicas := monitoring.Spec.CollectorReplicas
	if collectorReplicas == 0 {
		if isSNO {
			collectorReplicas = 1
		} else {
			collectorReplicas = 2
		}
	}
	templateData["CollectorReplicas"] = collectorReplicas

	return templateData, nil
}

// checkMonitoringPreconditions verifies that prerequisite operators are installed.
// Returns a multierror listing all missing operators.
func checkMonitoringPreconditions(ctx context.Context, c client.Client, monitoring *v1alpha1.Monitoring) error {
	var allErrors *multierror.Error

	if monitoring.Spec.Metrics != nil || monitoring.Spec.Traces != nil {
		if info, err := operatorExists(ctx, c, opentelemetryOperator); err != nil {
			return err
		} else if info == nil {
			allErrors = multierror.Append(allErrors, errors.New(conditions.OpenTelemetryCollectorOperatorMissingMessage))
		}
	}

	if monitoring.Spec.Metrics != nil {
		if info, err := operatorExists(ctx, c, clusterObservabilityOperator); err != nil {
			return err
		} else if info == nil {
			allErrors = multierror.Append(allErrors, errors.New(conditions.COOMissingMessage))
		}
	}

	if monitoring.Spec.Traces != nil {
		if info, err := operatorExists(ctx, c, tempoOperator); err != nil {
			return err
		} else if info == nil {
			allErrors = multierror.Append(allErrors, errors.New(conditions.TempoOperatorMissingMessage))
		}
	}

	return allErrors.ErrorOrNil()
}

// operatorExists checks for an OLM OperatorCondition with the given name prefix.
// Returns a non-nil sentinel when found, nil when absent.
func operatorExists(ctx context.Context, c client.Client, prefix string) (*struct{}, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "operators.coreos.com",
		Version: "v2",
		Kind:    "OperatorConditionList",
	})
	if err := c.List(ctx, list); err != nil {
		if k8serr.IsNotFound(err) || meta.IsNoMatchError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("listing OperatorConditions: %w", err)
	}
	for _, item := range list.Items {
		// Match exactly or with a "." separator (OLM names OperatorConditions as
		// "<name>.<version>") to avoid false positives from similarly-prefixed operators.
		name := item.GetName()
		if name == prefix || strings.HasPrefix(name, prefix+".") {
			return &struct{}{}, nil
		}
	}
	return nil, nil
}

func addMetricsData(metrics *v1alpha1.Metrics, isSNO bool, templateData map[string]any) error {
	addStorageData(metrics, templateData)
	addReplicasData(metrics, isSNO, templateData)
	return addExportersData(metrics, templateData)
}

func addResourceData(templateData map[string]any) {
	templateData["CPULimit"] = defaultCPULimit
	templateData["MemoryLimit"] = defaultMemoryLimit
	templateData["CPURequest"] = defaultCPURequest
	templateData["MemoryRequest"] = defaultMemoryRequest

	templateData["CollectorCPULimit"] = defaultCollectorCPULimit
	templateData["CollectorMemoryLimit"] = defaultCollectorMemoryLimit
	templateData["CollectorCPURequest"] = defaultCollectorCPURequest
	templateData["CollectorMemoryRequest"] = defaultCollectorMemoryRequest

	templateData["TempoCPULimit"] = defaultTempoCPULimit
	templateData["TempoMemoryLimit"] = defaultTempoMemoryLimit
	templateData["TempoCPURequest"] = defaultTempoCPURequest
	templateData["TempoMemoryRequest"] = defaultTempoMemoryRequest
}

func addStorageData(metrics *v1alpha1.Metrics, templateData map[string]any) {
	if metrics.Storage != nil {
		templateData["StorageSize"] = getResourceValueOrDefault(metrics.Storage.Size.String(), defaultStorageSize)
		templateData["StorageRetention"] = getStringValueOrDefault(metrics.Storage.Retention, defaultRetention)
	} else {
		templateData["StorageSize"] = defaultStorageSize
		templateData["StorageRetention"] = defaultRetention
	}
}

func addReplicasData(metrics *v1alpha1.Metrics, isSNO bool, templateData map[string]any) {
	allowedByConfig := metrics.Storage != nil

	switch {
	case metrics.Replicas != 0 && allowedByConfig:
		templateData["Replicas"] = strconv.Itoa(int(metrics.Replicas))
	case allowedByConfig:
		if isSNO {
			templateData["Replicas"] = "1"
		} else {
			templateData["Replicas"] = "2"
		}
	default:
		// Storage not configured: MonitoringStack is not deployed, so Replicas is
		// not used in rendered templates. Set a safe default to avoid a nil template
		// expansion if this path is ever reached unexpectedly.
		templateData["Replicas"] = "1"
	}
}

func addExportersData(metrics *v1alpha1.Metrics, templateData map[string]any) error {
	validatedExporters := make(map[string]string)
	exporterNames := make([]string, 0)

	if len(metrics.Exporters) == 0 {
		templateData["MetricsExporters"] = validatedExporters
		templateData["MetricsExporterNames"] = exporterNames
		return nil
	}

	var err error
	validatedExporters, err = validateExporters(metrics.Exporters)
	if err != nil {
		return err
	}

	for name := range validatedExporters {
		exporterNames = append(exporterNames, name)
	}
	sort.Strings(exporterNames)

	templateData["MetricsExporters"] = validatedExporters
	templateData["MetricsExporterNames"] = exporterNames

	return nil
}

func addTracesTemplateData(templateData map[string]any, traces *v1alpha1.Traces, namespace string) error {
	templateData["OtlpEndpoint"] = fmt.Sprintf("http://data-science-collector.%s.svc.cluster.local:4317", namespace)
	templateData["SampleRatio"] = getStringValueOrDefault(traces.SampleRatio, defaultTracesSampleRatio)

	backend := getStringValueOrDefault(traces.Storage.Backend, defaultTracesBackend)
	templateData["Backend"] = backend

	retention := traces.Storage.Retention.Duration.String()
	if traces.Storage.Retention.Duration == 0 {
		retention = defaultTracesRetention
	}
	templateData["TracesRetention"] = retention

	tlsEnabled := determineTLSEnabled(traces)
	templateData["TempoTLSEnabled"] = tlsEnabled

	if tlsEnabled {
		templateData["TempoCertificateSecret"] = traces.TLS.CertificateSecret
		templateData["TempoCAConfigMap"] = traces.TLS.CAConfigMap
	} else {
		templateData["TempoCertificateSecret"] = ""
		templateData["TempoCAConfigMap"] = ""
	}

	switch backend {
	case v1alpha1.StorageBackendPV:
		templateData["TempoEndpoint"] = fmt.Sprintf("tempo-data-science-tempomonolithic-gateway.%s.svc.cluster.local:4317", namespace)
		templateData["TempoQueryEndpoint"] = fmt.Sprintf("https://tempo-data-science-tempomonolithic-gateway.%s.svc.cluster.local:8080/api/traces/v1/%s/tempo", namespace, namespace)
		templateData["Size"] = traces.Storage.Size
	case v1alpha1.StorageBackendS3, v1alpha1.StorageBackendGCS:
		templateData["TempoEndpoint"] = fmt.Sprintf("tempo-data-science-tempostack-gateway.%s.svc.cluster.local:4317", namespace)
		templateData["TempoQueryEndpoint"] = fmt.Sprintf("https://tempo-data-science-tempostack-gateway.%s.svc.cluster.local:8080/api/traces/v1/%s/tempo", namespace, namespace)
		templateData["Secret"] = traces.Storage.Secret
	}

	validatedExporters := make(map[string]string)
	exporterNames := make([]string, 0)
	if traces.Exporters != nil {
		var err error
		validatedExporters, err = validateExporters(traces.Exporters)
		if err != nil {
			return err
		}
		for n := range validatedExporters {
			exporterNames = append(exporterNames, n)
		}
		sort.Strings(exporterNames)
	}
	templateData["TracesExporters"] = validatedExporters
	templateData["TracesExporterNames"] = exporterNames

	return nil
}

func addImageURLs(templateData map[string]any) {
	templateData["KubeRBACProxyImage"] = getEnvOrDefault(
		"RELATED_IMAGE_ODH_KUBE_RBAC_PROXY_IMAGE",
		"quay.io/brancz/kube-rbac-proxy:v0.20.0",
	)
	templateData["PromLabelProxyImage"] = getEnvOrDefault(
		"RELATED_IMAGE_OSE_PROM_LABEL_PROXY_IMAGE",
		"quay.io/prometheuscommunity/prom-label-proxy:v0.12.1",
	)
	templateData["CLIImage"] = getEnvOrDefault(
		"RELATED_IMAGE_CLI_IMAGE",
		"quay.io/openshift/origin-cli:4.17",
	)
}

func getEnvOrDefault(envVar, defaultVal string) string {
	if v := os.Getenv(envVar); v != "" {
		return v
	}
	return defaultVal
}

// getPersesImage returns the Perses image from environment variable or default.
// For RHOAI deployments, this is set via the CSV (via RHOAI-Build-Config/bundle/additional-images-patch.yaml).
// For ODH deployments, this uses the default value below.
//
// This image version must stay compatible with the Cluster Observability Operator (COO) version
// that we depend on. When upgrading COO, verify Perses image compatibility and update accordingly.
// The current image is compatible with COO 1.5.0.
func getPersesImage() string {
	if image := os.Getenv("RELATED_IMAGE_PERSES_IMAGE"); image != "" {
		return image
	}

	return "registry.redhat.io/cluster-observability-operator/perses-rhel9:1.5.0-1781116652@sha256:27553fd6d4b4983475a0d9a4ccc7d7fa63b1bd4b48f0e5cb2d18963fe232cfd5"
}

// resolvePersesAPIVersion probes the cluster for the installed Perses CRD API version.
func resolvePersesAPIVersion(ctx context.Context, c client.Client) (string, bool, error) {
	gk := schema.GroupKind{Group: "perses.dev", Kind: "Perses"}

	found, err := hasCRDWithVersion(ctx, c, gk, persesV1Alpha2)
	if err != nil {
		return "", false, err
	}
	if found {
		return persesV1Alpha2, true, nil
	}

	found, err = hasCRDWithVersion(ctx, c, gk, "v1alpha1")
	if err != nil {
		return "", false, err
	}
	if found {
		return "v1alpha1", true, nil
	}

	return "", false, nil
}

// hasCRDWithVersion checks whether a CRD for the given GroupKind + version exists.
func hasCRDWithVersion(ctx context.Context, c client.Client, gk schema.GroupKind, version string) (bool, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   gk.Group,
		Version: version,
		Kind:    gk.Kind + "List",
	})
	if err := c.List(ctx, list, &client.ListOptions{Limit: 1}); err != nil {
		if k8serr.IsNotFound(err) || meta.IsNoMatchError(err) {
			return false, nil
		}
		return false, fmt.Errorf("hasCRDWithVersion: %w", err)
	}
	return true, nil
}

// persesGVKs returns the Perses, PersesDatasource, and PersesDashboard GVKs for the given version.
func persesGVKs(version string) (schema.GroupVersionKind, schema.GroupVersionKind, schema.GroupVersionKind) {
	if version == persesV1Alpha2 {
		return gvk.PersesV1Alpha2, gvk.PersesDatasourceV1Alpha2, gvk.PersesDashboardV1Alpha2
	}
	return gvk.PersesV1Alpha1, gvk.PersesDatasourceV1Alpha1, gvk.PersesDashboardV1Alpha1
}

func determineTLSEnabled(traces *v1alpha1.Traces) bool {
	if traces.TLS != nil {
		return traces.TLS.Enabled
	}
	return false
}

func getResourceValueOrDefault(value, defaultValue string) string {
	if value == "" || value == "0" {
		return defaultValue
	}
	return value
}

func getStringValueOrDefault(value, defaultValue string) string {
	if value == "" {
		return defaultValue
	}
	return value
}

// --- Exporter validation ---

func validateExporters(exporters map[string]runtime.RawExtension) (map[string]string, error) {
	validatedExporters := make(map[string]string)

	totalSize := 0
	for name, rawConfig := range exporters {
		if isReservedName(name) {
			return nil, fmt.Errorf("exporter name '%s' is reserved and cannot be used", name)
		}

		if !componentIDRE.MatchString(name) {
			return nil, fmt.Errorf(
				"invalid exporter name '%s': must match OpenTelemetry component ID format %q",
				name, componentIDRE.String(),
			)
		}

		raw := rawBytesFrom(rawConfig)
		if len(raw) == 0 {
			continue
		}

		if len(raw) > maxExporterSize {
			return nil, fmt.Errorf("exporter '%s' config exceeds maximum size of %d bytes (actual: %d bytes)",
				name, maxExporterSize, len(raw))
		}

		totalSize += len(raw)
		if totalSize > maxTotalExporterSize {
			return nil, fmt.Errorf("total exporter config size exceeds maximum of %d bytes (actual: %d bytes)",
				maxTotalExporterSize, totalSize)
		}

		var config map[string]any
		if err := yaml.Unmarshal(raw, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal exporter config for '%s': %w", name, err)
		}
		if config == nil {
			config = map[string]any{}
		}

		if err := validateExporterConfigSecurity(name, config); err != nil {
			return nil, err
		}

		if err := validateExporterSchema(name, config); err != nil {
			return nil, err
		}

		configYAML, err := yaml.Marshal(config)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal exporter config for '%s': %w", name, err)
		}
		validatedExporters[name] = strings.TrimSpace(string(configYAML))
	}

	return validatedExporters, nil
}

func rawBytesFrom(rawConfig runtime.RawExtension) []byte {
	if len(rawConfig.Raw) > 0 {
		return rawConfig.Raw
	}
	if rawConfig.Object != nil {
		b, err := yaml.Marshal(rawConfig.Object)
		if err == nil {
			return b
		}
	}
	return nil
}

func isReservedName(n string) bool {
	return n == "otlp/tempo" || n == "prometheus"
}

func isLocalServiceEndpoint(endpoint string) bool {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return false
	}
	if host == "localhost" || host == "::1" || strings.HasPrefix(host, "127.") {
		return true
	}
	if strings.HasSuffix(host, ".svc.cluster.local") || strings.HasSuffix(host, ".svc") {
		return true
	}
	if !strings.Contains(host, ".") && !strings.Contains(host, ":") {
		return true
	}
	return false
}

func validateExporterConfigSecurity(name string, config map[string]any) error {
	if len(config) > maxConfigFields {
		return fmt.Errorf("exporter '%s' has too many fields (%d), maximum allowed is %d", name, len(config), maxConfigFields)
	}
	return validateConfigDepthAndTypes(config, 1, name)
}

func validateConfigDepthAndTypes(obj any, depth int, exporterName string) error {
	if depth > maxNestingDepth {
		return fmt.Errorf("exporter '%s' config nesting too deep (max %d levels)", exporterName, maxNestingDepth)
	}

	switch v := obj.(type) {
	case map[string]any:
		if len(v) > maxConfigFields {
			return fmt.Errorf("exporter '%s' config object has too many fields at depth %d", exporterName, depth)
		}
		for key, value := range v {
			if len(key) > maxStringLength {
				return fmt.Errorf("exporter '%s' config key too long at depth %d", exporterName, depth)
			}
			if err := validateConfigDepthAndTypes(value, depth+1, exporterName); err != nil {
				return err
			}
		}
	case []any:
		if len(v) > maxArrayLength {
			return fmt.Errorf("exporter '%s' config array too long (%d items) at depth %d", exporterName, len(v), depth)
		}
		for _, item := range v {
			if err := validateConfigDepthAndTypes(item, depth+1, exporterName); err != nil {
				return err
			}
		}
	case string:
		if len(v) > maxStringLength {
			return fmt.Errorf("exporter '%s' config string value too long at depth %d", exporterName, depth)
		}
	case int, int32, int64, float32, float64, bool, nil:
		// safe types
	default:
		return fmt.Errorf("exporter '%s' config contains unsupported type %T at depth %d", exporterName, v, depth)
	}

	return nil
}

// ExporterSchema defines the validation schema for an exporter type.
type ExporterSchema struct {
	RequiredFields []string
	AllowedFields  []string
	FieldTypes     map[string]FieldType
	FieldRules     map[string][]ValidationRule
}

// FieldType defines the expected type and constraints for a field.
type FieldType struct {
	Type          string
	Pattern       *regexp.Regexp
	MinLength     *int
	MaxLength     *int
	AllowedValues []string
}

// ValidationRule defines custom validation logic for a field.
type ValidationRule struct {
	Name     string
	Validate func(field string, value any) error
}

//nolint:gochecknoglobals
var metricsExporterSchemas = map[string]ExporterSchema{
	"otlp": {
		RequiredFields: []string{"endpoint"},
		AllowedFields: []string{
			"endpoint", "headers", "tls", "compression", "timeout",
			"retry_on_failure", "sending_queue", "balancer_name",
		},
		FieldTypes: map[string]FieldType{
			"endpoint": {
				Type:      "string",
				Pattern:   regexp.MustCompile(`^https?://[a-zA-Z0-9.-]+(:[0-9]+)?(/.*)?$`),
				MinLength: intPtr(1),
				MaxLength: intPtr(2048),
			},
			"headers":     {Type: "map[string]string"},
			"tls":         {Type: "object"},
			"compression": {Type: "string", AllowedValues: []string{"gzip", "snappy", "zstd", "none"}},
			"timeout": {
				Type:      "string",
				Pattern:   regexp.MustCompile(`^\d+[smh]$`),
				MaxLength: intPtr(10),
			},
		},
		FieldRules: map[string][]ValidationRule{
			"endpoint": {secureEndpointRule()},
		},
	},
	"otlphttp": {
		RequiredFields: []string{"endpoint"},
		AllowedFields: []string{
			"endpoint", "headers", "tls", "compression", "timeout",
			"retry_on_failure", "sending_queue",
		},
		FieldTypes: map[string]FieldType{
			"endpoint": {
				Type:      "string",
				Pattern:   regexp.MustCompile(`^https?://[a-zA-Z0-9.-]+(:[0-9]+)?(/.*)?$`),
				MinLength: intPtr(1),
				MaxLength: intPtr(2048),
			},
			"headers":     {Type: "map[string]string"},
			"compression": {Type: "string", AllowedValues: []string{"gzip", "none"}},
			"timeout": {
				Type:      "string",
				Pattern:   regexp.MustCompile(`^\d+[smh]$`),
				MaxLength: intPtr(10),
			},
		},
		FieldRules: map[string][]ValidationRule{
			"endpoint": {secureEndpointRule()},
		},
	},
	"debug": {
		AllowedFields: []string{"verbosity", "sampling_initial", "sampling_thereafter"},
		FieldTypes: map[string]FieldType{
			"verbosity":           {Type: "string", AllowedValues: []string{"basic", "normal", "detailed"}},
			"sampling_initial":    {Type: "int"},
			"sampling_thereafter": {Type: "int"},
		},
	},
	"prometheusremotewrite": {
		RequiredFields: []string{"endpoint"},
		AllowedFields: []string{
			"endpoint", "headers", "tls", "remote_timeout", "retry_on_failure",
			"sending_queue", "write_relabel_configs", "resource_to_telemetry_conversion",
		},
		FieldTypes: map[string]FieldType{
			"endpoint": {
				Type:      "string",
				Pattern:   regexp.MustCompile(`^https?://[a-zA-Z0-9.-]+(:[0-9]+)?(/.*)?$`),
				MinLength: intPtr(1),
				MaxLength: intPtr(2048),
			},
			"headers": {Type: "map[string]string"},
			"tls":     {Type: "object"},
			"remote_timeout": {
				Type:      "string",
				Pattern:   regexp.MustCompile(`^\d+[smh]$`),
				MaxLength: intPtr(10),
			},
		},
		FieldRules: map[string][]ValidationRule{
			"endpoint": {secureEndpointRule()},
		},
	},
}

func secureEndpointRule() ValidationRule {
	return ValidationRule{
		Name: "secure_endpoint_check",
		Validate: func(_ string, value any) error {
			if str, ok := value.(string); ok {
				if strings.HasPrefix(str, "http://") && !isLocalServiceEndpoint(str) {
					return errors.New("insecure HTTP endpoints not allowed for external services")
				}
			}
			return nil
		},
	}
}

func validateExporterSchema(exporterName string, config map[string]any) error {
	exporterType := getExporterType(exporterName)
	s, exists := metricsExporterSchemas[exporterType]
	if !exists {
		return nil
	}
	return s.Validate(exporterName, config)
}

func getExporterType(exporterName string) string {
	if before, _, ok := strings.Cut(exporterName, "/"); ok {
		return before
	}
	return exporterName
}

// Validate validates an exporter config against the schema.
func (s ExporterSchema) Validate(exporterName string, config map[string]any) error {
	for _, required := range s.RequiredFields {
		if _, exists := config[required]; !exists {
			return fmt.Errorf("exporter '%s' missing required field: %s", exporterName, required)
		}
	}

	for field := range config {
		if !slices.Contains(s.AllowedFields, field) {
			return fmt.Errorf("exporter '%s' contains disallowed field: %s (allowed: %v)",
				exporterName, field, s.AllowedFields)
		}
	}

	for field, value := range config {
		if fieldType, exists := s.FieldTypes[field]; exists {
			if err := validateFieldTypeAndConstraints(exporterName, field, value, fieldType); err != nil {
				return err
			}
		}

		if rules, exists := s.FieldRules[field]; exists {
			for _, rule := range rules {
				if err := rule.Validate(field, value); err != nil {
					return fmt.Errorf("exporter '%s' field '%s' failed rule '%s': %w",
						exporterName, field, rule.Name, err)
				}
			}
		}
	}

	return nil
}

func validateFieldTypeAndConstraints(exporterName, field string, value any, fieldType FieldType) error {
	if err := validateFieldTypeStrict(value, fieldType.Type); err != nil {
		return fmt.Errorf("exporter '%s' field '%s': %w", exporterName, field, err)
	}

	if str, ok := value.(string); ok && fieldType.Type == "string" {
		if fieldType.MinLength != nil && len(str) < *fieldType.MinLength {
			return fmt.Errorf("exporter '%s' field '%s': minimum length %d, got %d",
				exporterName, field, *fieldType.MinLength, len(str))
		}
		if fieldType.MaxLength != nil && len(str) > *fieldType.MaxLength {
			return fmt.Errorf("exporter '%s' field '%s': maximum length %d, got %d",
				exporterName, field, *fieldType.MaxLength, len(str))
		}
		if fieldType.Pattern != nil && !fieldType.Pattern.MatchString(str) {
			return fmt.Errorf("exporter '%s' field '%s': does not match required pattern",
				exporterName, field)
		}
		if len(fieldType.AllowedValues) > 0 && !slices.Contains(fieldType.AllowedValues, str) {
			return fmt.Errorf("exporter '%s' field '%s': must be one of %v, got '%s'",
				exporterName, field, fieldType.AllowedValues, str)
		}
	}

	return nil
}

func validateFieldTypeStrict(value any, expectedType string) error {
	switch expectedType {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string, got %T", value)
		}
	case "int":
		switch value.(type) {
		case int, int32, int64, float64:
		default:
			return fmt.Errorf("expected int, got %T", value)
		}
	case "bool":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected bool, got %T", value)
		}
	case "object":
		if _, ok := value.(map[string]any); !ok {
			return fmt.Errorf("expected object, got %T", value)
		}
	case "array":
		if _, ok := value.([]any); !ok {
			return fmt.Errorf("expected array, got %T", value)
		}
	case "map[string]string":
		if m, ok := value.(map[string]any); ok {
			for _, v := range m {
				if _, ok := v.(string); !ok {
					return fmt.Errorf("map value must be string, got %T", v)
				}
			}
		} else {
			return fmt.Errorf("expected map[string]string, got %T", value)
		}
	default:
		return fmt.Errorf("unsupported field type: %s", expectedType)
	}
	return nil
}

func intPtr(i int) *int {
	return &i
}
