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
	"embed"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"slices"
	"strings"

	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
)

const (
	MonitoringStackTemplate                          = "resources/monitoring-stack.tmpl.yaml"
	MonitoringAdmissionPoliciesTemplate              = "resources/monitoring-admission-policies.tmpl.yaml"
	MonitoringStackAlertmanagerRBACTemplate          = "resources/monitoringstack-alertmanager-rbac.tmpl.yaml"
	TempoMonolithicTemplate                          = "resources/tempo-monolithic.tmpl.yaml"
	TempoStackTemplate                               = "resources/tempo-stack.tmpl.yaml"
	OpenTelemetryCollectorTemplate                   = "resources/opentelemetry-collector.tmpl.yaml"
	CollectorServiceMonitorsTemplate                 = "resources/collector-servicemonitors.tmpl.yaml"
	CollectorMonitorServiceTemplate                  = "resources/collector-monitor-service.tmpl.yaml"
	CollectorPrometheusServiceTemplate               = "resources/collector-prometheus-service.tmpl.yaml"
	CollectorMonitoringNetworkPolicyTemplate         = "resources/collector-monitoring-network-policy.tmpl.yaml"
	CollectorTargetAllocatorNetworkPolicyTemplate    = "resources/collector-targetallocator-network-policy.tmpl.yaml"
	CollectorRBACTemplate                            = "resources/collector-rbac.tmpl.yaml"
	CollectorMLflowRBACTemplate                      = "resources/collector-mlflow-rbac.tmpl.yaml"
	CollectorTempoRBACTemplate                       = "resources/collector-tempo-rbac.tmpl.yaml"
	PrometheusRouteTemplate                          = "resources/data-science-prometheus-route.tmpl.yaml"
	InstrumentationTemplate                          = "resources/instrumentation.tmpl.yaml"
	PrometheusNamespaceProxyTemplate                 = "resources/data-science-prometheus-namespace-proxy.tmpl.yaml"
	PrometheusNamespaceProxyNetworkPolicyTemplate    = "resources/data-science-prometheus-namespace-proxy-network-policy.tmpl.yaml"
	PrometheusServiceOverrideTemplate                = "resources/data-science-prometheus-service-override.tmpl.yaml"
	PrometheusNetworkPolicyTemplate                  = "resources/data-science-prometheus-network-policy.tmpl.yaml"
	AlertmanagerNetworkPolicyTemplate                = "resources/data-science-alertmanager-network-policy.tmpl.yaml"
	PrometheusClusterProxyNetworkPolicyTemplate      = "resources/data-science-prometheus-cluster-proxy-network-policy.tmpl.yaml"
	ThanosQuerierProxyTemplate                       = "resources/data-science-thanos-querier-proxy.tmpl.yaml"
	ThanosQuerierProxyNetworkPolicyTemplate          = "resources/data-science-thanos-querier-proxy-network-policy.tmpl.yaml"
	PrometheusWebTLSServiceTemplate                  = "resources/prometheus-web-tls-service.tmpl.yaml"
	PrometheusSelfServiceMonitorTemplate             = "resources/prometheus-self-servicemonitor.tmpl.yaml"
	ThanosQuerierTemplate                            = "resources/thanos-querier-cr.tmpl.yaml"
	ThanosQuerierNetworkPolicyTemplate               = "resources/data-science-thanos-querier-network-policy.tmpl.yaml"
	TempoMonolithicNetworkPolicyTemplate             = "resources/data-science-tempo-monolithic-network-policy.tmpl.yaml"
	ThanosQuerierRouteTemplate                       = "resources/thanos-querier-route.tmpl.yaml"
	PersesTemplate                                   = "resources/perses.tmpl.yaml"
	PersesTempoDatasourceTemplate                    = "resources/perses-tempo-datasource.tmpl.yaml"
	PersesTempoDashboardV1Alpha1Template             = "resources/perses-tempo-dashboard-v1alpha1.tmpl.yaml"
	PersesTempoDashboardV1Alpha2Template             = "resources/perses-tempo-dashboard-v1alpha2.tmpl.yaml"
	PersesDatasourcePrometheusTemplate               = "resources/perses-datasource-prometheus.tmpl.yaml"
	PersesDatasourceClusterPrometheusTemplate        = "resources/perses-datasource-cluster-prometheus.tmpl.yaml"
	PersesDatasourceClusterPrometheusTenancyTemplate = "resources/perses-datasource-cluster-prometheus-tenancy.tmpl.yaml"
	PrometheusClusterProxyTemplate                   = "resources/data-science-prometheus-cluster-proxy.tmpl.yaml"
	TempoServiceCAConfigMapTemplate                  = "resources/tempo-service-ca-configmap.tmpl.yaml"
	PersesOperatorAccessNetworkPolicyTemplate        = "resources/perses-operator-access-network-policy.tmpl.yaml"
	OperatorPrometheusRulesTemplate                  = "monitoring/operator-prometheusrules.tmpl.yaml"
	AcceleratorRecordingRulesTemplate                = "resources/accelerator-recording-rules.tmpl.yaml"
	UsageLogsOpenTelemetryCollectorTemplate          = "resources/usage-logs-opentelemetry-collector.tmpl.yaml"
	UsageLogsOpenTelemetryCollectorRBACTemplate      = "resources/usage-logs-opentelemetry-collector-rbac.tmpl.yaml"
	UsageLogsOpenTelemetryCollectorNPTemplate        = "resources/usage-logs-opentelemetry-collector-network-policy.tmpl.yaml"
	UsageLogsLokiStackNPTemplate                     = "resources/usage-logs-lokistack-network-policy.tmpl.yaml"
	UsageLogsLokiStackTemplate                       = "resources/usage-logs-loki-stack.tmpl.yaml"
	LokiStackNetworkPolicyTemplate                   = "resources/data-science-lokistack-network-policy.tmpl.yaml"
	LokiStackGatewayNetworkPolicyTemplate            = "resources/data-science-lokistack-gateway-network-policy.tmpl.yaml"
	LokiStackTemplate                                = "resources/loki-stack.tmpl.yaml"
	ClusterLogForwarderTemplate                      = "resources/cluster-log-forwarder.tmpl.yaml"
	ClusterLogForwarderRBACTemplate                  = "resources/cluster-log-forwarder-rbac.tmpl.yaml"
	ClusterLogForwarderNetworkPolicyTemplate         = "resources/cluster-log-forwarder-network-policy.tmpl.yaml"
	Korrel8rDeploymentTemplate                       = "resources/korrel8r-deployment.tmpl.yaml"
	Korrel8rConfigTemplate                           = "resources/korrel8r-config.tmpl.yaml"
	Korrel8rRBACTemplate                             = "resources/korrel8r-rbac.tmpl.yaml"
	Korrel8rNetworkPolicyTemplate                    = "resources/korrel8r-network-policy.tmpl.yaml"

	PersesTempoDatasourceName = "tempo-datasource"
	PersesTempoDashboardName  = "data-science-tempo-traces"

	defaultMonitoringNamespace           = "opendatahub"
	clusterLoggingNamespace              = "openshift-logging"
	usageLogsLokiStackResourceName       = "data-science-usage-logs-lokistack"
	lokiStackResourceName                = "data-science-lokistack"
	clusterLogForwarderResourceName      = "data-science-cluster-log-forwarder"
	Korrel8rServiceName                  = "korrel8r"
	ClusterLogForwarderNetworkPolicyName = "data-science-cluster-log-forwarder-allow"
	conditionTypeReady                   = "Ready"
	conditionStatusTrue                  = "True"
	schemeHTTP                           = "http"
	schemeHTTPS                          = "https"
)

//go:embed resources monitoring
var resourcesFS embed.FS

// defaultPortForScheme maps a URL scheme to the port it implies when none is
// written, so both spellings of one endpoint canonicalize alike.
var defaultPortForScheme = map[string]string{schemeHTTP: "80", schemeHTTPS: "443"}

func src(path string) rendertemplate.TemplateSource {
	return rendertemplate.TemplateSource{FS: resourcesFS, Path: path}
}

// deployMonitoringAdmissionPolicies adds the ValidatingAdmissionPolicy templates.
func deployMonitoringAdmissionPolicies(
	_ context.Context,
	_ client.Client,
	_ *v1alpha1.Monitoring,
	_ *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
) error {
	*sources = append(*sources, src(MonitoringAdmissionPoliciesTemplate))
	return nil
}

// deployMonitoringStackWithQuerierAndRestrictions deploys MonitoringStack + ThanosQuerier
// when built-in metrics storage is configured. Exporters-only metrics skip the local stack.
func deployMonitoringStackWithQuerierAndRestrictions(
	ctx context.Context,
	c client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
) error {
	if monitoring.Spec.Metrics == nil || monitoring.Spec.Metrics.Storage == nil {
		cm.MarkNotConfigured(conditions.ConditionMonitoringStackAvailable, conditions.MetricsNotConfiguredReason, conditions.MetricsNotConfiguredMessage)
		cm.MarkNotConfigured(conditions.ConditionThanosQuerierAvailable, conditions.MetricsNotConfiguredReason, conditions.MetricsNotConfiguredMessage)
		return nil
	}

	// Check both CRDs; if either is missing mark both conditions false.
	msExists, err := hasCRD(ctx, c, gvk.MonitoringStack)
	if err != nil {
		return fmt.Errorf("checking MonitoringStack CRD: %w", err)
	}
	tqExists, err := hasCRD(ctx, c, gvk.ThanosQuerier)
	if err != nil {
		return fmt.Errorf("checking ThanosQuerier CRD: %w", err)
	}

	if !msExists || !tqExists {
		if !msExists {
			cm.MarkFalse(conditions.ConditionMonitoringStackAvailable,
				"MonitoringStackCRDNotFoundReason", "MonitoringStack CRD not found (atomic deployment requires all CRDs)")
		}
		if !tqExists {
			cm.MarkFalse(conditions.ConditionThanosQuerierAvailable,
				"ThanosQuerierCRDNotFoundReason", "ThanosQuerier CRD not found (atomic deployment requires all CRDs)")
		}
		return nil
	}

	cm.MarkTrue(conditions.ConditionMonitoringStackAvailable)
	cm.MarkTrue(conditions.ConditionThanosQuerierAvailable)

	*sources = append(*sources,
		src(PrometheusWebTLSServiceTemplate),
		src(MonitoringStackTemplate),
		src(PrometheusSelfServiceMonitorTemplate),
		src(MonitoringStackAlertmanagerRBACTemplate),
		src(AcceleratorRecordingRulesTemplate),
		src(PrometheusRouteTemplate),
		src(PrometheusServiceOverrideTemplate),
		src(PrometheusNetworkPolicyTemplate),
		src(AlertmanagerNetworkPolicyTemplate),
		src(PrometheusClusterProxyNetworkPolicyTemplate),
		src(PrometheusNamespaceProxyTemplate),
		src(PrometheusNamespaceProxyNetworkPolicyTemplate),
		src(ThanosQuerierTemplate),
		src(ThanosQuerierNetworkPolicyTemplate),
		src(ThanosQuerierProxyTemplate),
		src(ThanosQuerierProxyNetworkPolicyTemplate),
		src(ThanosQuerierRouteTemplate),
	)
	return nil
}

// deployTracingStack deploys Instrumentation whenever traces are configured, and
// Tempo only when built-in storage is configured. Exporters-only traces still get
// Instrumentation so workloads send OTLP to the collector.
func deployTracingStack(
	ctx context.Context,
	c client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
) error {
	if monitoring.Spec.Traces == nil {
		cm.MarkNotConfigured(conditions.ConditionTempoAvailable, conditions.TracesNotConfiguredReason, conditions.TracesNotConfiguredMessage)
		cm.MarkNotConfigured(conditions.ConditionInstrumentationAvailable, conditions.TracesNotConfiguredReason, conditions.TracesNotConfiguredMessage)
		return nil
	}

	traces := monitoring.Spec.Traces

	instrExists, err := hasCRD(ctx, c, gvk.Instrumentation)
	if err != nil {
		return fmt.Errorf("checking Instrumentation CRD: %w", err)
	}
	if !instrExists {
		cm.MarkFalse(conditions.ConditionInstrumentationAvailable,
			"InstrumentationCRDNotFoundReason", "Instrumentation CRD not found")
	} else {
		cm.MarkTrue(conditions.ConditionInstrumentationAvailable)
		*sources = append(*sources, src(InstrumentationTemplate))
	}

	// Exporters-only (no storage): skip built-in Tempo; collector forwards externally.
	if traces.Storage == nil {
		cm.MarkNotConfigured(conditions.ConditionTempoAvailable, conditions.TracesNotConfiguredReason, conditions.TracesNotConfiguredMessage)
		return nil
	}

	tempoGVK := gvk.TempoStack
	tempoTemplate := TempoStackTemplate
	if traces.Storage.Backend == v1alpha1.StorageBackendPV {
		tempoGVK = gvk.TempoMonolithic
		tempoTemplate = TempoMonolithicTemplate
	}

	tempoExists, err := hasCRD(ctx, c, tempoGVK)
	if err != nil {
		return fmt.Errorf("checking %s CRD: %w", tempoGVK.Kind, err)
	}
	if !tempoExists {
		cm.MarkFalse(conditions.ConditionTempoAvailable,
			tempoGVK.Kind+"CRDNotFoundReason",
			fmt.Sprintf("%s CRD not found", tempoGVK.Kind))
		return nil
	}

	cm.MarkTrue(conditions.ConditionTempoAvailable)
	*sources = append(*sources, src(tempoTemplate))
	if tempoGVK == gvk.TempoMonolithic {
		*sources = append(*sources, src(TempoMonolithicNetworkPolicyTemplate))
	}
	return nil
}

// deployOpenTelemetryCollector deploys the OTel collector when metrics or traces are configured.
func deployOpenTelemetryCollector(
	ctx context.Context,
	c client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
) error {
	if monitoring.Spec.Metrics == nil && monitoring.Spec.Traces == nil {
		cm.MarkNotConfigured(conditions.ConditionOpenTelemetryCollectorAvailable,
			conditions.MetricsAndTracesNotConfiguredReason,
			conditions.MetricsAndTracesNotConfiguredMessage)
		return nil
	}

	otcExists, err := hasCRD(ctx, c, gvk.OpenTelemetryCollector)
	if err != nil {
		return fmt.Errorf("checking OpenTelemetryCollector CRD: %w", err)
	}
	if !otcExists {
		cm.MarkFalse(conditions.ConditionOpenTelemetryCollectorAvailable,
			gvk.OpenTelemetryCollector.Kind+"CRDNotFoundReason",
			fmt.Sprintf("%s CRD not found", gvk.OpenTelemetryCollector.Kind))
		return nil
	}

	cm.MarkTrue(conditions.ConditionOpenTelemetryCollectorAvailable)

	*sources = append(*sources,
		src(CollectorRBACTemplate),
		src(OpenTelemetryCollectorTemplate),
		// Service for internal telemetry re-exported on :8890 with TLS (always-on)
		src(CollectorMonitorServiceTemplate),
		src(CollectorMonitoringNetworkPolicyTemplate),
		src(CollectorTargetAllocatorNetworkPolicyTemplate),
	)

	// ServiceMonitors (monitoring.rhobs) and the local prometheus Service (:8889)
	// are only for MonitoringStack scrape. Exporters-only omits storage and must
	// not require the COO ServiceMonitor CRD.
	if monitoring.Spec.Metrics != nil && monitoring.Spec.Metrics.Storage != nil {
		*sources = append(*sources,
			src(CollectorServiceMonitorsTemplate),
			src(CollectorPrometheusServiceTemplate),
		)
	}

	if monitoring.Spec.Traces != nil {
		*sources = append(*sources, src(CollectorMLflowRBACTemplate))
		if monitoring.Spec.Traces.Storage != nil {
			*sources = append(*sources, src(CollectorTempoRBACTemplate))
		}
	}

	return nil
}

// deployOperatorMonitoring deploys PrometheusRules for operator self-monitoring
// (webhook health, pod availability). These rules are internal monitoring for the
// operator itself and are deployed regardless of user alerting config.
func deployOperatorMonitoring(
	ctx context.Context,
	c client.Client,
	_ *v1alpha1.Monitoring,
	_ *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
) error {
	// Check if PrometheusRule CRD exists (installed by RHOBS/Prometheus Operator)
	exists, err := hasCRD(ctx, c, gvk.PrometheusRule)
	if err != nil {
		return fmt.Errorf("checking PrometheusRule CRD: %w", err)
	}
	if !exists {
		// Silently skip if CRD not found - operator monitoring is best-effort
		return nil
	}

	*sources = append(*sources, src(OperatorPrometheusRulesTemplate))
	return nil
}

// deployAlerting deploys operator-level PrometheusRules when alerting is configured.
// Per-component rules are intentionally dropped in the standalone module.
func deployAlerting(
	ctx context.Context,
	c client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
) error {
	if monitoring.Spec.Alerting == nil {
		cm.MarkNotConfigured(conditions.ConditionAlertingAvailable,
			conditions.AlertingNotConfiguredReason, conditions.AlertingNotConfiguredMessage)
		return nil
	}

	exists, err := hasCRD(ctx, c, gvk.PrometheusRule)
	if err != nil {
		return fmt.Errorf("checking PrometheusRule CRD: %w", err)
	}
	if !exists {
		cm.MarkFalse(conditions.ConditionAlertingAvailable,
			"PrometheusRuleCRDNotFoundReason", "PrometheusRule CRD not found")
		return nil
	}

	cm.MarkTrue(conditions.ConditionAlertingAvailable)
	return nil
}

// deployPerses deploys the Perses CR when built-in metrics or traces storage is configured.
// Exporters-only configs have no local Thanos/Tempo query backends for Perses datasources.
// persesVersion and persesFound are pre-resolved by the reconciler to avoid
// redundant API calls across the three Perses action functions.
func deployPerses(
	ctx context.Context,
	c client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
	persesVersion string,
	persesFound bool,
) error {
	hasMetricsStorage := monitoring.Spec.Metrics != nil && monitoring.Spec.Metrics.Storage != nil
	hasTracesStorage := monitoring.Spec.Traces != nil && monitoring.Spec.Traces.Storage != nil
	if !hasMetricsStorage && !hasTracesStorage {
		cm.MarkNotConfigured(conditions.ConditionPersesAvailable,
			conditions.MetricsAndTracesNotConfiguredReason,
			"Perses requires metrics.storage or traces.storage to be configured")
		return nil
	}

	if !persesFound {
		cm.MarkFalse(conditions.ConditionPersesAvailable,
			"PersesCRDNotFoundReason",
			"Perses CRD not found in any supported version (v1alpha2, v1alpha1)")
		return nil
	}

	persesGVK, _, _ := persesGVKs(persesVersion)
	exists, err := hasCRD(ctx, c, persesGVK)
	if err != nil {
		return fmt.Errorf("checking Perses CRD: %w", err)
	}
	if !exists {
		cm.MarkFalse(conditions.ConditionPersesAvailable,
			"PersesCRDNotFoundReason",
			fmt.Sprintf("%s CRD not found", persesGVK.Kind))
		return nil
	}

	cm.MarkTrue(conditions.ConditionPersesAvailable)
	*sources = append(*sources, src(PersesTemplate), src(PersesOperatorAccessNetworkPolicyTemplate))
	return nil
}

// deployPersesTempoIntegration deploys the Perses Tempo datasource + dashboard when traces are configured.
// persesVersion and persesFound are pre-resolved by the reconciler to avoid
// redundant API calls across the three Perses action functions.
func deployPersesTempoIntegration(
	ctx context.Context,
	c client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
	persesVersion string,
	persesFound bool,
) error {
	_, datasourceGVK, dashboardGVK := persesGVKs(persesVersion)
	namespace := monitoringNamespace(monitoring)

	var datasourceExists, dashboardExists bool
	var err error
	if persesFound {
		datasourceExists, err = hasCRD(ctx, c, datasourceGVK)
		if err != nil {
			return fmt.Errorf("checking PersesDatasource CRD: %w", err)
		}
		dashboardExists, err = hasCRD(ctx, c, dashboardGVK)
		if err != nil {
			return fmt.Errorf("checking PersesDashboard CRD: %w", err)
		}
	}

	// Perses Tempo datasource targets built-in Tempo; exporters-only has no Tempo query API.
	if monitoring.Spec.Traces == nil || monitoring.Spec.Traces.Storage == nil {
		// Clean up existing Tempo datasource + dashboard if storage is removed or traces deconfigured.
		if datasourceExists {
			ds := &unstructured.Unstructured{}
			ds.SetGroupVersionKind(datasourceGVK)
			ds.SetName(PersesTempoDatasourceName)
			ds.SetNamespace(namespace)
			if err := c.Delete(ctx, ds); err != nil && !k8serr.IsNotFound(err) {
				return fmt.Errorf("deleting PersesDatasource: %w", err)
			}
		}
		if dashboardExists {
			db := &unstructured.Unstructured{}
			db.SetGroupVersionKind(dashboardGVK)
			db.SetName(PersesTempoDashboardName)
			db.SetNamespace(namespace)
			if err := c.Delete(ctx, db); err != nil && !k8serr.IsNotFound(err) {
				return fmt.Errorf("deleting PersesDashboard: %w", err)
			}
		}
		cm.MarkNotConfigured(conditions.ConditionPersesTempoDataSourceAvailable,
			conditions.TracesNotConfiguredReason, conditions.TracesNotConfiguredMessage)
		return nil
	}

	if !datasourceExists {
		cm.MarkFalse(conditions.ConditionPersesTempoDataSourceAvailable,
			datasourceGVK.Kind+"CRDNotFoundReason",
			fmt.Sprintf("%s CRD not found", datasourceGVK.Kind))
		return nil
	}

	cm.MarkTrue(conditions.ConditionPersesTempoDataSourceAvailable)

	*sources = append(*sources, src(PersesTempoDatasourceTemplate), src(TempoServiceCAConfigMapTemplate))

	if dashboardExists {
		dashboardTemplate := PersesTempoDashboardV1Alpha1Template
		if persesVersion == persesV1Alpha2 {
			dashboardTemplate = PersesTempoDashboardV1Alpha2Template
		}
		*sources = append(*sources, src(dashboardTemplate))
	}

	return nil
}

// deployPersesPrometheusIntegration deploys the Perses Prometheus datasource when
// built-in metrics storage is configured (Thanos Querier URL).
// persesVersion and persesFound are pre-resolved by the reconciler to avoid
// redundant API calls across the three Perses action functions.
func deployPersesPrometheusIntegration(
	ctx context.Context,
	c client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
	persesVersion string,
	persesFound bool,
) error {
	if monitoring.Spec.Metrics == nil || monitoring.Spec.Metrics.Storage == nil {
		cm.MarkNotConfigured(conditions.ConditionPersesPrometheusDataSourceAvailable,
			conditions.MetricsNotConfiguredReason,
			"Prometheus datasource requires metrics.storage to be configured")
		return nil
	}

	if !persesFound {
		cm.MarkFalse(conditions.ConditionPersesPrometheusDataSourceAvailable,
			"PersesDatasourceCRDNotFoundReason",
			"PersesDatasource CRD not found in any supported version")
		return nil
	}

	_, datasourceGVK, _ := persesGVKs(persesVersion)
	exists, err := hasCRD(ctx, c, datasourceGVK)
	if err != nil {
		return fmt.Errorf("checking PersesDatasource CRD: %w", err)
	}
	if !exists {
		cm.MarkFalse(conditions.ConditionPersesPrometheusDataSourceAvailable,
			datasourceGVK.Kind+"CRDNotFoundReason",
			fmt.Sprintf("%s CRD not found", datasourceGVK.Kind))
		return nil
	}

	cm.MarkTrue(conditions.ConditionPersesPrometheusDataSourceAvailable)
	*sources = append(*sources,
		src(PersesDatasourcePrometheusTemplate),
		src(PersesDatasourceClusterPrometheusTemplate),
		src(PersesDatasourceClusterPrometheusTenancyTemplate),
	)
	return nil
}

// deployNodeMetricsEndpoint deploys the node metrics cluster proxy when built-in
// metrics storage is configured (part of the MonitoringStack query path).
func deployNodeMetricsEndpoint(
	_ context.Context,
	_ client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
) error {
	if monitoring.Spec.Metrics == nil || monitoring.Spec.Metrics.Storage == nil {
		cm.MarkNotConfigured(conditions.ConditionNodeMetricsEndpointAvailable,
			conditions.MetricsNotConfiguredReason, conditions.MetricsNotConfiguredMessage)
		return nil
	}

	cm.MarkTrue(conditions.ConditionNodeMetricsEndpointAvailable)
	*sources = append(*sources, src(PrometheusClusterProxyTemplate))
	return nil
}

// deployUsageLogsCollector deploys the usage logs OpenTelemetry collector when usage logs are configured.
func deployUsageLogsCollector(
	ctx context.Context,
	c client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
) error {
	if monitoring.Spec.UsageLogs == nil || monitoring.Spec.UsageLogs.Storage == nil {
		cm.MarkNotConfigured(conditions.ConditionUsageLogsCollectorAvailable,
			"UsageLogsNotConfigured", "Usage logs not configured in Monitoring CR")
		return nil
	}

	// Wait for LokiStack endpoint to be available in status (set only when Loki is ready)
	if monitoring.Status.UsageLogsEndpoint == "" {
		cm.MarkFalse(conditions.ConditionUsageLogsCollectorAvailable,
			"LokiStackNotReady",
			"Waiting for LokiStack to be ready")
		return nil
	}

	otcExists, err := hasCRD(ctx, c, gvk.OpenTelemetryCollector)
	if err != nil {
		return fmt.Errorf("checking OpenTelemetryCollector CRD: %w", err)
	}
	if !otcExists {
		cm.MarkFalse(conditions.ConditionUsageLogsCollectorAvailable,
			"OpenTelemetryCollectorCRDNotFound",
			"OpenTelemetryCollector CRD not found")
		return nil
	}

	cm.MarkTrue(conditions.ConditionUsageLogsCollectorAvailable)
	*sources = append(*sources,
		src(UsageLogsOpenTelemetryCollectorTemplate),
		src(UsageLogsOpenTelemetryCollectorRBACTemplate),
		src(UsageLogsOpenTelemetryCollectorNPTemplate),
		src(UsageLogsLokiStackNPTemplate),
	)

	return nil
}

// deployLokiStack deploys the usage logs LokiStack when usage logs storage is
// configured and the log forwarding LokiStack when logs are configured. The two
// stacks are separate CRs with independent storage, so each is rendered and
// reported on only when its own feature is configured.
//
// When the storage isolation check rejects the configuration, the stacks go to
// withheld rather than sources: they are not applied, but the caller still
// feeds them to garbage collection so an already-running stack is treated as
// desired instead of deleted. The returned error degrades Loki only; the rest
// of the reconciliation continues without it.
func deployLokiStack(
	ctx context.Context,
	c client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
	withheld *[]rendertemplate.TemplateSource,
) error {
	needsUsageLogsLoki := monitoring.Spec.UsageLogs != nil && monitoring.Spec.UsageLogs.Storage != nil
	needsLoki := monitoring.Spec.Logs != nil

	if !needsUsageLogsLoki {
		cm.MarkNotConfigured(conditions.ConditionUsageLogsLokiStackAvailable,
			"LokiNotRequired", "Usage logs storage not configured")
	}
	if !needsLoki {
		cm.MarkNotConfigured(conditions.ConditionLokiStackAvailable,
			"LokiNotRequired", "Log forwarding not configured")
	}
	if !needsUsageLogsLoki && !needsLoki {
		return nil
	}

	lokiExists, err := hasCRD(ctx, c, gvk.LokiStack)
	if err != nil {
		return fmt.Errorf("checking LokiStack CRD: %w", err)
	}
	if !lokiExists {
		if needsUsageLogsLoki {
			cm.MarkFalse(conditions.ConditionUsageLogsLokiStackAvailable,
				conditions.MissingOperatorReason,
				"LokiStack operator must be installed for usage logs storage configuration")
		}
		if needsLoki {
			cm.MarkFalse(conditions.ConditionLokiStackAvailable,
				conditions.MissingOperatorReason,
				"LokiStack operator must be installed for log forwarding configuration")
		}
		return nil
	}

	// Withhold both stacks when they would write to the same bucket, or when
	// that question cannot be answered, so the conflict is reported instead of
	// silently corrupting log storage. Readiness is not polled: there is nothing
	// to report readiness about while the configuration is rejected.
	if err := checkLokiStorageIsolation(ctx, c, monitoring); err != nil {
		// Both outcomes withhold the stacks; only the reason differs. An error
		// that does not name one can only have come from the check itself.
		reason := lokiStorageCheckFailedReason
		var storageErr lokiStorageError
		if errors.As(err, &storageErr) {
			reason = storageErr.conditionReason()
		}
		cm.MarkFalse(conditions.ConditionUsageLogsLokiStackAvailable, reason, err.Error())
		cm.MarkFalse(conditions.ConditionLokiStackAvailable, reason, err.Error())
		*withheld = append(*withheld, lokiStackSources(needsUsageLogsLoki, needsLoki)...)
		return err
	}

	if needsUsageLogsLoki {
		ready, err := isLokiStackReady(ctx, c, monitoring, usageLogsLokiStackResourceName)
		if err != nil {
			return fmt.Errorf("checking usage logs LokiStack readiness: %w", err)
		}
		if ready {
			cm.MarkTrue(conditions.ConditionUsageLogsLokiStackAvailable)
		} else {
			cm.MarkFalse(conditions.ConditionUsageLogsLokiStackAvailable,
				"LokiStackNotReady",
				"Usage logs LokiStack is not ready yet")
		}
	}

	if needsLoki {
		ready, err := isLokiStackReady(ctx, c, monitoring, lokiStackResourceName)
		if err != nil {
			return fmt.Errorf("checking LokiStack readiness: %w", err)
		}
		if ready {
			cm.MarkTrue(conditions.ConditionLokiStackAvailable)
		} else {
			cm.MarkFalse(conditions.ConditionLokiStackAvailable,
				"LokiStackNotReady",
				"LokiStack is not ready yet")
		}
	}

	*sources = append(*sources, lokiStackSources(needsUsageLogsLoki, needsLoki)...)

	return nil
}

// lokiStackSources lists the templates each LokiStack contributes. Both the
// applied path and the withheld path draw from it, so a template added here is
// also retained by garbage collection when the storage check rejects the
// configuration.
func lokiStackSources(needsUsageLogsLoki, needsLoki bool) []rendertemplate.TemplateSource {
	var sources []rendertemplate.TemplateSource
	if needsUsageLogsLoki {
		sources = append(sources, src(UsageLogsLokiStackTemplate))
	}
	if needsLoki {
		sources = append(sources,
			src(LokiStackTemplate),
			src(LokiStackNetworkPolicyTemplate),
			src(LokiStackGatewayNetworkPolicyTemplate),
		)
	}
	return sources
}

// checkLokiStorageIsolation rejects a configuration that points both LokiStacks
// at the same object store bucket.
//
// Both stacks serve the "application" tenant, and Loki keys its objects by
// tenant ID rather than by LokiStack name, so two stacks sharing a bucket
// address the same index and chunk objects and overwrite each other. The
// LokiStack API exposes no per-stack object prefix, so separate buckets are the
// only isolation boundary available. Comparing SecretName would not catch this:
// two different Secrets can carry the same bucketnames.
//
// It returns a *lokiStorageConflictError when the stacks overlap and a
// *lokiStorageCheckError when a Secret could not be read. An unreadable Secret
// is reported as a failed check rather than treated as isolated: a guard that
// answers "no conflict" when it could not look is no guard at all.
func checkLokiStorageIsolation(ctx context.Context, c client.Client, monitoring *v1alpha1.Monitoring) error {
	usageLogs := monitoring.Spec.UsageLogs
	logs := monitoring.Spec.Logs
	if usageLogs == nil || usageLogs.Storage == nil || logs == nil || logs.Storage == nil {
		return nil
	}

	namespace := monitoringNamespace(monitoring)
	usageLogsLocations, err := lokiStorageLocations(ctx, c, namespace, usageLogs.Storage.SecretName)
	if err != nil {
		return &lokiStorageCheckError{err: err}
	}
	logsLocations, err := lokiStorageLocations(ctx, c, namespace, logs.Storage.SecretName)
	if err != nil {
		return &lokiStorageCheckError{err: err}
	}

	shared := make([]string, 0, len(usageLogsLocations))
	for _, location := range usageLogsLocations {
		if slices.Contains(logsLocations, location) {
			shared = append(shared, location)
		}
	}
	if len(shared) == 0 {
		return nil
	}

	return &lokiStorageConflictError{
		usageLogsSecret: usageLogs.Storage.SecretName,
		logsSecret:      logs.Storage.SecretName,
		shared:          shared,
	}
}

// lokiStorageLocations resolves a LokiStack storage Secret to the canonical
// object store locations it writes to: the same bucket name on two different
// endpoints is two different buckets.
//
// A Secret that does not exist yet returns no locations, since it cannot be
// shown to conflict and the Loki operator reports the missing credentials
// itself. Any other read failure is returned, so the caller withholds both
// stacks instead of concluding they are isolated.
func lokiStorageLocations(ctx context.Context, c client.Client, namespace, secretName string) ([]string, error) {
	if secretName == "" {
		return nil, nil
	}

	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, secret); err != nil {
		if k8serr.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading LokiStack storage secret %s/%s: %w", namespace, secretName, err)
	}

	// bucketnames holds a comma-separated list; the stacks conflict if any one
	// bucket is common to both.
	endpoint := string(secret.Data["endpoint"])
	var locations []string
	for bucket := range strings.SplitSeq(string(secret.Data["bucketnames"]), ",") {
		if bucket = strings.TrimSpace(bucket); bucket != "" {
			locations = append(locations, lokiStorageLocation(endpoint, bucket))
		}
	}
	return locations, nil
}

// lokiStorageLocation canonicalizes an (endpoint, bucket) pair into a
// comparable object store address, so that two spellings of one location
// compare equal instead of slipping past the overlap check.
//
// The comparison key is (scheme, host, port, path prefix, bucket). Credentials
// are deliberately excluded: two identities writing to one bucket still
// collide, so comparing access keys would only mask the conflict. The scheme is
// kept, because http and https are configured as separate backends. DNS aliases
// pointing at one server are not detected; that needs resolution, not parsing.
func lokiStorageLocation(endpoint, bucket string) string {
	endpoint = strings.TrimSpace(endpoint)
	bucket = strings.TrimSpace(bucket)

	// A bare "host" or "host:port" would parse as a scheme, so supply the
	// authority marker that makes url.Parse read it as one.
	toParse := endpoint
	if !strings.Contains(toParse, "://") {
		toParse = "//" + toParse
	}
	parsed, err := url.Parse(toParse)
	if err != nil {
		// An unparseable endpoint still has to compare equal to itself.
		return endpoint + "/" + bucket
	}

	// Schemes and hostnames are case-insensitive (RFC 4343).
	scheme := strings.ToLower(parsed.Scheme)
	host := strings.ToLower(parsed.Hostname())

	// An explicitly written default port names the same endpoint as none at all.
	port := parsed.Port()
	if port == defaultPortForScheme[scheme] {
		port = ""
	}
	if port != "" {
		host = net.JoinHostPort(host, port)
	}

	// Virtual-host-style addressing folds the bucket into the hostname; strip it
	// so that form matches the path-style spelling of the same location.
	if bucket != "" {
		host = strings.TrimPrefix(host, strings.ToLower(bucket)+".")
	}

	// path.Clean collapses repeated slashes and resolves dot segments. Prefix
	// case is significant on self-hosted gateways, so it is left alone.
	prefix := path.Clean("/" + parsed.Path)
	if prefix == "/" {
		prefix = ""
	}

	return scheme + "://" + host + prefix + "/" + bucket
}

// deployClusterLogForwarder deploys the ClusterLogForwarder when logs are configured.
func deployClusterLogForwarder(
	ctx context.Context,
	c client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
) error {
	if monitoring.Spec.Logs == nil {
		cm.MarkNotConfigured(conditions.ConditionClusterLogForwarderAvailable,
			"LogsNotConfigured", "Logs not configured in Monitoring CR")
		return nil
	}

	logExists, err := hasCRD(ctx, c, gvk.ClusterLogForwarder)
	if err != nil {
		return fmt.Errorf("checking ClusterLogForwarder CRD: %w", err)
	}
	if !logExists {
		cm.MarkFalse(conditions.ConditionClusterLogForwarderAvailable,
			"ClusterLogForwarderCRDNotFound",
			"ClusterLogForwarder CRD not found")
		return nil
	}

	lokiExists, err := hasCRD(ctx, c, gvk.LokiStack)
	if err != nil {
		return fmt.Errorf("checking LokiStack CRD: %w", err)
	}
	if !lokiExists {
		cm.MarkFalse(conditions.ConditionClusterLogForwarderAvailable,
			"LokiStackCRDNotFound",
			"LokiStack CRD not found")
		return nil
	}

	// Keep the CLF resources in the desired state while Loki is starting or
	// recovering. Otherwise garbage collection treats them as stale and
	// removes the cross-namespace resources during a transient Loki outage.
	*sources = append(*sources,
		src(ClusterLogForwarderTemplate),
		src(ClusterLogForwarderRBACTemplate),
		src(ClusterLogForwarderNetworkPolicyTemplate),
	)

	lokiReady, err := isLokiStackReady(ctx, c, monitoring, lokiStackResourceName)
	if err != nil {
		return fmt.Errorf("checking LokiStack readiness for log forwarding: %w", err)
	}
	if !lokiReady {
		cm.MarkFalse(conditions.ConditionClusterLogForwarderAvailable,
			"LokiStackNotReady",
			"LokiStack must be ready before ClusterLogForwarder can be deployed")
		return nil
	}

	logReady, err := isClusterLogForwarderReady(ctx, c)
	if err != nil {
		return fmt.Errorf("checking ClusterLogForwarder readiness: %w", err)
	}

	if logReady {
		cm.MarkTrue(conditions.ConditionClusterLogForwarderAvailable)
	} else {
		cm.MarkFalse(conditions.ConditionClusterLogForwarderAvailable,
			"ClusterLogForwarderNotReady",
			"ClusterLogForwarder is not ready yet")
	}

	return nil
}

// deployKorrel8r deploys the RHOAI-owned correlation service whenever at least
// one observability signal is configured. The service itself handles backend
// availability independently, so a missing optional backend must not prevent
// the other configured stores from being exposed.
func deployKorrel8r(
	ctx context.Context,
	c client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
) error {
	if monitoring.Spec.Metrics == nil && monitoring.Spec.Traces == nil && monitoring.Spec.Logs == nil {
		cm.MarkNotConfigured(conditions.ConditionKorrel8rAvailable,
			"Korrel8rNotConfigured", "Metrics, traces, and logs are not configured in Monitoring CR")
		return nil
	}

	ready, err := korrel8rReady(ctx, c, monitoringNamespace(monitoring))
	if err != nil {
		return fmt.Errorf("checking Korrel8r readiness: %w", err)
	}
	if ready {
		cm.MarkTrue(conditions.ConditionKorrel8rAvailable)
	} else {
		cm.MarkFalse(conditions.ConditionKorrel8rAvailable,
			"Korrel8rNotReady",
			"Korrel8r Deployment or Service endpoint is not ready")
	}
	*sources = append(*sources,
		src(Korrel8rConfigTemplate),
		src(Korrel8rDeploymentTemplate),
		src(Korrel8rRBACTemplate),
		src(Korrel8rNetworkPolicyTemplate),
	)
	return nil
}

// korrel8rReady reports whether the rendered Deployment has ready replicas and
// its Service has at least one ready endpoint. The endpoint check prevents a
// selector or Service drift from being reported as an available service.
func korrel8rReady(ctx context.Context, c client.Client, namespace string) (bool, error) {
	deployment := &appsv1.Deployment{}
	if err := c.Get(ctx, types.NamespacedName{Name: Korrel8rServiceName, Namespace: namespace}, deployment); err != nil {
		if k8serr.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("getting Korrel8r Deployment: %w", err)
	}
	desiredReplicas := int32(1)
	if deployment.Spec.Replicas != nil {
		desiredReplicas = *deployment.Spec.Replicas
	}
	if deployment.Status.ReadyReplicas < desiredReplicas {
		return false, nil
	}

	service := &corev1.Service{}
	if err := c.Get(ctx, types.NamespacedName{Name: Korrel8rServiceName, Namespace: namespace}, service); err != nil {
		if k8serr.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("getting Korrel8r Service: %w", err)
	}

	endpointSlices := &discoveryv1.EndpointSliceList{}
	if err := c.List(ctx, endpointSlices,
		client.InNamespace(namespace),
		client.MatchingLabels{discoveryv1.LabelServiceName: service.Name}); err != nil {
		return false, fmt.Errorf("listing Korrel8r endpoint slices: %w", err)
	}
	for _, endpointSlice := range endpointSlices.Items {
		for _, endpoint := range endpointSlice.Endpoints {
			if endpoint.Conditions.Ready != nil && *endpoint.Conditions.Ready {
				return true, nil
			}
		}
	}
	return false, nil
}

// deployWebhookInfrastructure reports the webhook availability condition.
// The webhook Service, cert-manager Issuer+Certificate, and
// MutatingWebhookConfiguration are deployed by the Helm chart, not the
// reconciler. This function only checks whether the TLS secret has been
// provisioned by cert-manager so the condition reflects reality.
func deployWebhookInfrastructure(
	ctx context.Context,
	c client.Client,
	_ *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	_ *[]rendertemplate.TemplateSource,
) error {
	log := logf.FromContext(ctx)

	operatorName := getEnvOrDefault("OPERATOR_NAME", "odh-observability")
	operatorNamespace := os.Getenv("POD_NAMESPACE")
	secretName := operatorName + "-webhook-cert"

	secret := &corev1.Secret{}
	err := c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: operatorNamespace}, secret)
	if err != nil {
		if k8serr.IsNotFound(err) {
			log.Info("webhook TLS secret not yet provisioned by cert-manager, waiting", "secret", secretName)
			cm.MarkNotConfigured(conditions.ConditionWebhookAvailable,
				"TLSSecretPending",
				fmt.Sprintf("Waiting for cert-manager to provision TLS secret %s/%s", operatorNamespace, secretName))
			return nil
		}
		return fmt.Errorf("checking webhook TLS secret: %w", err)
	}

	if len(secret.Data["tls.crt"]) == 0 || len(secret.Data["tls.key"]) == 0 {
		log.Info("webhook TLS secret exists but certificate data not yet populated", "secret", secretName)
		cm.MarkNotConfigured(conditions.ConditionWebhookAvailable,
			"TLSSecretPending",
			"TLS secret exists but certificate data not yet populated by cert-manager")
		return nil
	}

	cm.MarkTrue(conditions.ConditionWebhookAvailable)
	return nil
}

func monitoringNamespace(monitoring *v1alpha1.Monitoring) string {
	if monitoring.Spec.Namespace == "" {
		if configured := os.Getenv("MONITORING_NAMESPACE"); configured != "" {
			return configured
		}
		return defaultMonitoringNamespace
	}
	return monitoring.Spec.Namespace
}

func hasReadyCondition(obj *unstructured.Unstructured) (bool, error) {
	conds, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}

	for _, cond := range conds {
		condMap, ok := cond.(map[string]any)
		if !ok {
			continue
		}

		condType, _, _ := unstructured.NestedString(condMap, "type")
		condStatus, _, _ := unstructured.NestedString(condMap, "status")
		if (condType == conditionTypeReady || strings.HasSuffix(condType, "/"+conditionTypeReady)) && condStatus == conditionStatusTrue {
			return true, nil
		}
	}

	return false, nil
}

// isLokiStackReady checks if the named LokiStack CR exists and is in a Ready state.
func isLokiStackReady(ctx context.Context, c client.Client, monitoring *v1alpha1.Monitoring, name string) (bool, error) {
	lokiStack := &unstructured.Unstructured{}
	lokiStack.SetGroupVersionKind(gvk.LokiStack)

	err := c.Get(ctx, types.NamespacedName{
		Name:      name,
		Namespace: monitoringNamespace(monitoring),
	}, lokiStack)
	if err != nil {
		if k8serr.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}

	ready, err := hasReadyCondition(lokiStack)
	if err != nil {
		return false, fmt.Errorf("malformed LokiStack status.conditions: %w", err)
	}
	return ready, nil
}

// isClusterLogForwarderReady checks if the ClusterLogForwarder CR exists and is in a Ready state.
func isClusterLogForwarderReady(ctx context.Context, c client.Client) (bool, error) {
	clusterLogForwarder := &unstructured.Unstructured{}
	clusterLogForwarder.SetGroupVersionKind(gvk.ClusterLogForwarder)

	err := c.Get(ctx, types.NamespacedName{
		Name:      clusterLogForwarderResourceName,
		Namespace: clusterLoggingNamespace,
	}, clusterLogForwarder)
	if err != nil {
		if k8serr.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}

	ready, err := hasReadyCondition(clusterLogForwarder)
	if err != nil {
		return false, fmt.Errorf("malformed ClusterLogForwarder status.conditions: %w", err)
	}
	return ready, nil
}
