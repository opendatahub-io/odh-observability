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

	libconditions "github.com/opendatahub-io/odh-platform-utilities/pkg/controller/conditions"
	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
)

//go:embed resources monitoring
var resourcesFS embed.FS

// defaultWebPorts are the ports http and https imply when none is written. The
// storage comparison key carries no scheme, so either one names the same
// endpoint as no port at all.
var defaultWebPorts = map[string]struct{}{"80": {}, "443": {}}

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
	usageLogs := monitoring.Spec.UsageLogs

	// Storage drives the cluster-local write and exporters drive forwarding. Either
	// alone is a working configuration, so the collector is unconfigured only when
	// neither is asked for.
	if usageLogs == nil || (usageLogs.Storage == nil && len(usageLogs.Exporters) == 0) {
		cm.MarkNotConfigured(conditions.ConditionUsageLogsCollectorAvailable,
			"UsageLogsNotConfigured", "Usage logs not configured in Monitoring CR")
		return nil
	}

	// Wait for LokiStack endpoint to be available in status (set only when Loki is
	// ready). Only the local write depends on it: a cluster that just forwards has no
	// LokiStack being deployed, so waiting here would wait forever.
	if usageLogs.Storage != nil && monitoring.Status.UsageLogsEndpoint == "" {
		cm.MarkFalse(conditions.ConditionUsageLogsCollectorAvailable,
			"LokiStackNotReady",
			"Waiting for LokiStack to be ready")
		return nil
	}

	// A named credential is a promise the Secret exists. Rendering the collector
	// before it does leaves the pod in ContainerCreating on FailedMount, which says
	// nothing about the cause — and with no local write to fall back on it takes the
	// whole pipeline down. Gated on exporters to match the template, which mounts the
	// Secret only when there is something to authenticate.
	if usageLogs.CredentialSecret != "" && len(usageLogs.Exporters) > 0 {
		credentialExists, err := usageLogsCredentialExists(ctx, c, monitoring)
		if err != nil {
			return err
		}
		if !credentialExists {
			cm.MarkFalse(conditions.ConditionUsageLogsCollectorAvailable,
				conditions.CredentialNotFoundReason,
				fmt.Sprintf("Waiting for Secret %q, which carries the remote-write credential",
					usageLogs.CredentialSecret))
			return nil
		}
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

	// The two modes fail in entirely different ways, so a bare True that cannot tell
	// them apart is not much of a status.
	if usageLogs.Storage == nil {
		cm.MarkTrueWithReason(conditions.ConditionUsageLogsCollectorAvailable,
			"ForwardingOnly",
			fmt.Sprintf("Forwarding usage logs to %d remote exporter(s); no cluster-local store",
				len(usageLogs.Exporters)))
	} else {
		cm.MarkTrue(conditions.ConditionUsageLogsCollectorAvailable)
	}

	*sources = append(*sources,
		src(UsageLogsOpenTelemetryCollectorTemplate),
		src(UsageLogsOpenTelemetryCollectorRBACTemplate),
		src(UsageLogsOpenTelemetryCollectorNPTemplate),
	)

	return nil
}

// deployUsageLogsLokiStack renders the usage logs pipeline: the LokiStack that
// stores the logs, then the collector that ships them. The collector keeps its
// own gates — it renders only once the endpoint of a ready stack was published
// by an earlier round — so on the first round, or while the stack is starting,
// only the stack renders, exactly as when the two were separate actions.
//
// The reconciler runs it only when validateStorageIsolation has not withheld
// componentLokiUsageLogs, so by the time it is called its storage has either
// been proven disjoint from every other claimant's or was never contested.
// The collector's templates carry the same component label as the stack's, so
// withholding freezes the pipeline as one unit: nothing renders, the sweep
// shields all of it, and it resumes when the storage is fixed.
func deployUsageLogsLokiStack(
	ctx context.Context,
	c client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
) error {
	if err := deployLokiStack(ctx, c, monitoring, cm, sources, lokiStackDeployment{
		configured:   monitoring.Spec.UsageLogs != nil && monitoring.Spec.UsageLogs.Storage != nil,
		resourceName: usageLogsLokiStackResourceName,
		condition:    conditions.ConditionUsageLogsLokiStackAvailable,
		description:  "usage logs LokiStack",
		feature:      "usage logs storage",
		templates: []string{
			UsageLogsLokiStackTemplate,
			UsageLogsLokiStackNPTemplate,
		},
	}); err != nil {
		return err
	}

	return deployUsageLogsCollector(ctx, c, monitoring, cm, sources)
}

// deployRHOAILogsLokiStack renders the log forwarding pipeline: the LokiStack
// that stores the logs, then the ClusterLogForwarder that feeds it. The
// forwarder keeps its own CRD and readiness gates, and its templates stay in
// the desired set while the stack is starting, exactly as when it was a
// separate action.
//
// It is gated on Spec.Logs alone rather than on Spec.Logs.Storage, because the
// CRD marks Storage required within Logs; the nil check in storageClaims is
// there for the isolation comparison, which must not read through a nil.
//
// Like its counterpart above, the reconciler runs it only when
// validateStorageIsolation has not withheld componentLokiLogs, and the
// forwarder's templates carry the stack's component label so withholding
// freezes the pipeline as one unit.
func deployRHOAILogsLokiStack(
	ctx context.Context,
	c client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
) error {
	if err := deployLokiStack(ctx, c, monitoring, cm, sources, lokiStackDeployment{
		configured:   monitoring.Spec.Logs != nil,
		resourceName: lokiStackResourceName,
		condition:    conditions.ConditionLokiStackAvailable,
		description:  "LokiStack",
		feature:      "log forwarding",
		templates: []string{
			LokiStackTemplate,
			LokiStackNetworkPolicyTemplate,
			LokiStackGatewayNetworkPolicyTemplate,
		},
	}); err != nil {
		return err
	}

	return deployClusterLogForwarder(ctx, c, monitoring, cm, sources)
}

// deployLokiStacks renders each logs pipeline whose component the storage gate
// left standing: its LokiStack and the workload that ships into it.
//
// The withheld check is the only thing separating these from the other deploy
// actions: past it they are ordinary actions whose errors abort the round,
// which is what keeps them free of any shield of their own. It walks
// lokiComponents rather than calling the two in sequence so that the set of
// components the gate can withhold and the set the reconcile deploys cannot
// drift apart. A withheld pipeline is skipped whole — its templates carry the
// component label, so the sweep shields the stack and its dependent together —
// and the dependent's condition is marked with the gate's verdict, because the
// folded action that would normally report on it never runs.
func deployLokiStacks(
	ctx context.Context,
	c client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
	withheldComponents map[string]struct{},
) error {
	for _, component := range lokiComponents {
		var err error
		switch component {
		case componentLokiUsageLogs:
			if _, withheld := withheldComponents[component]; withheld {
				markWithheldPipeline(monitoring, cm,
					conditions.ConditionUsageLogsLokiStackAvailable,
					conditions.ConditionUsageLogsCollectorAvailable,
					"usage logs collector")
				continue
			}
			err = deployUsageLogsLokiStack(ctx, c, monitoring, cm, sources)
		case componentLokiLogs:
			if _, withheld := withheldComponents[component]; withheld {
				markWithheldPipeline(monitoring, cm,
					conditions.ConditionLokiStackAvailable,
					conditions.ConditionClusterLogForwarderAvailable,
					"cluster log forwarder")
				continue
			}
			err = deployRHOAILogsLokiStack(ctx, c, monitoring, cm, sources)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// markWithheldPipeline copies the storage gate's verdict from a stack's
// condition onto the condition of the workload that ships into that stack.
//
// The gate has just written the stack's condition when this runs, and the
// folded action that would normally report on the dependent is being skipped,
// so without this a previously-True condition would stand while the pipeline
// sits frozen. The dependent carries the same verdict because it is withheld
// for the same reason: in the desired set and in the shield, the pipeline is
// one unit.
func markWithheldPipeline(
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	stackCondition, dependentCondition, dependent string,
) {
	stack := libconditions.FindStatusCondition(monitoring, stackCondition)
	if stack == nil {
		return
	}
	message := fmt.Sprintf("%s withheld with its LokiStack: %s",
		capitalizeFirst(dependent), stack.Message)
	switch stack.Status {
	case metav1.ConditionFalse:
		cm.MarkFalse(dependentCondition, stack.Reason, message)
	case metav1.ConditionUnknown:
		cm.MarkUnknown(dependentCondition, stack.Reason, message)
	default:
		// True is not a verdict the gate writes: it withholds only on a
		// refused or unobservable configuration. Leave the dependent alone
		// rather than invent a verdict the stack does not carry.
	}
}

// lokiStackDeployment is everything that differs between the two stacks, so
// the deploy path below can be written once.
//
// The gateway NetworkPolicy belongs in templates rather than with the workload
// that dials through it: a policy that guards a gateway has to exist for as
// long as the gateway does, including the window before its client is up.
type lokiStackDeployment struct {
	configured   bool
	resourceName string
	condition    string
	description  string // names the stack in condition messages
	feature      string // names the spec field that asks for it
	templates    []string
}

// deployLokiStack renders one LokiStack and reports on it. The two stacks are
// independent of one another — separate CRs whose storage
// validateStorageIsolation has already proven disjoint — so nothing here reads
// the other stack's state.
//
// Rendering comes before the readiness observation on purpose. The templates
// are a function of the spec, which the gate already accepted, so a failed
// readiness Get must not withhold them — withholding is for gates, and a stack
// that is never applied is a stack that never becomes ready. The same
// reasoning is why an absent stack renders: NotFound is a determinate "not
// ready", not a failure to look.
//
// This is an ordinary action. Every state it can report on — not configured,
// operator absent, stack not ready — marks its condition and returns nil, the
// way the ten actions ahead of it do. It returns an error only when the
// apiserver failed to answer, and that aborts the round from the call site,
// which is also the contract those ten keep.
//
// The two aborts cost different things, and only one of them needs the abort
// to be safe. A failed readiness check has already appended its templates, so
// the round it costs is just a round. A failed CRD check has appended nothing,
// and a template missing from the desired set is indistinguishable, to the
// collector, from one the user deleted — the abort is what keeps that branch
// from needing a garbage collection shield, by reaching neither Deploy nor
// collectGarbage.
//
// Both error returns mark the condition before returning, which the other
// hasCRD call sites do not. The deferred patch in Reconcile writes status on
// error paths too, so the mark lands; without it the previous round's True
// would stand and report a stack healthy on a check that never completed.
func deployLokiStack(
	ctx context.Context,
	c client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
	stack lokiStackDeployment,
) error {
	// Configured is tested before the CRD, so a stack nobody asked for reports
	// LokiNotRequired at Info severity whether or not the operator happens to
	// be installed. The reverse order would call a cluster without Loki
	// degraded for declining to run a feature it was never asked to run.
	if !stack.configured {
		cm.MarkNotConfigured(stack.condition, "LokiNotRequired",
			fmt.Sprintf("%s not configured", capitalizeFirst(stack.feature)))
		return nil
	}

	lokiCRDExists, err := hasCRD(ctx, c, gvk.LokiStack)
	if err != nil {
		cm.MarkUnknown(stack.condition, lokiCRDCheckFailedReason,
			fmt.Sprintf("Cannot determine whether the LokiStack CRD is installed: %v", err))
		return fmt.Errorf("checking LokiStack CRD: %w", err)
	}
	if !lokiCRDExists {
		cm.MarkFalse(stack.condition, conditions.MissingOperatorReason,
			fmt.Sprintf("LokiStack operator must be installed for %s configuration", stack.feature))
		return nil
	}

	for _, template := range stack.templates {
		*sources = append(*sources, src(template))
	}

	ready, err := isLokiStackReady(ctx, c, monitoring, stack.resourceName)
	if err != nil {
		cm.MarkUnknown(stack.condition, lokiReadinessCheckFailedReason,
			fmt.Sprintf("Cannot determine whether the %s is ready: %v", stack.description, err))
		return fmt.Errorf("checking %s readiness: %w", stack.description, err)
	}
	if ready {
		cm.MarkTrue(stack.condition)
	} else {
		cm.MarkFalse(stack.condition, "LokiStackNotReady",
			fmt.Sprintf("%s is not ready yet", capitalizeFirst(stack.description)))
	}
	return nil
}

// capitalizeFirst uppercases the leading byte of a condition message built
// from a stack description. The descriptions read as nouns mid-sentence
// ("Cannot determine whether the usage logs LokiStack…") and as sentence
// openers elsewhere.
func capitalizeFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// storageClaim is one component's claim on an object store location: who wants
// it, which Secret resolves it, and which condition reports on it. Adding a
// claimant to the isolation check is adding an entry.
type storageClaim struct {
	description string
	secretName  string
	condition   string

	// component is the garbage collection shield this claimant's resources
	// carry, in the componentLabel on its templates. The gate withholds it
	// when the claimant's storage cannot be accepted, which is what keeps a
	// configuration the operator refused to render from reading to the
	// collector as a deletion. A future claimant joins by labelling its
	// templates and naming the value here.
	component string
}

// storageClaims lists every component that writes to object storage. Both
// entries are LokiStacks today; the shape is what lets a third consumer join
// without the check growing a special case for it.
func storageClaims(monitoring *v1alpha1.Monitoring) []storageClaim {
	var claims []storageClaim
	if usageLogs := monitoring.Spec.UsageLogs; usageLogs != nil && usageLogs.Storage != nil {
		claims = append(claims, storageClaim{
			description: fmt.Sprintf("usageLogs.storage (secret %q)", usageLogs.Storage.SecretName),
			secretName:  usageLogs.Storage.SecretName,
			condition:   conditions.ConditionUsageLogsLokiStackAvailable,
			component:   componentLokiUsageLogs,
		})
	}
	if logs := monitoring.Spec.Logs; logs != nil && logs.Storage != nil {
		claims = append(claims, storageClaim{
			description: fmt.Sprintf("logs.storage (secret %q)", logs.Storage.SecretName),
			secretName:  logs.Storage.SecretName,
			condition:   conditions.ConditionLokiStackAvailable,
			component:   componentLokiLogs,
		})
	}
	return claims
}

// validateStorageIsolation is the gate in front of every component that writes to
// object storage. A rejected configuration withholds all of them, because the
// comparison is the only thing standing between two writers and a shared
// bucket, and a stack that is never created cannot corrupt anything.
//
// It is the only step in the Loki path that refuses to render something the
// spec asked for, and therefore the only one that owns a garbage collection
// shield. On failure it hands back the component of every claimant it just
// refused, because resources missing from the desired set otherwise read to
// the collector as "the user deleted these".
//
// The set is built from the claims, which are built from the spec, so a stack
// the user removed is not a claimant, is not withheld, and is collected on
// schedule even while a surviving claimant is in conflict.
//
// It reports on each claimant's own condition, with the reason and status
// carried by the error: a conflict is a determinate answer and reports False,
// a check that could not run establishes nothing and reports Unknown. Unknown
// matters because publishLokiEndpoints holds the published endpoint there
// instead of clearing it, and clearing it takes the usage logs collector and
// its policies down with it over one failed API call.
func validateStorageIsolation(
	ctx context.Context,
	c client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
) (map[string]struct{}, error) {
	claims := storageClaims(monitoring)
	err := checkStorageIsolation(ctx, c, monitoringNamespace(monitoring), claims)
	if err == nil {
		return nil, nil
	}

	// An error that names neither reason nor status can only have come from
	// the check itself, so fall back to the generic unobservable reporting.
	reason, status := storageCheckFailedReason, metav1.ConditionUnknown
	var storageErr storageError
	if errors.As(err, &storageErr) {
		reason, status = storageErr.conditionReason(), storageErr.conditionStatus()
	}

	withheld := make(map[string]struct{}, len(claims))
	for _, claim := range claims {
		if status == metav1.ConditionUnknown {
			cm.MarkUnknown(claim.condition, reason, err.Error())
		} else {
			cm.MarkFalse(claim.condition, reason, err.Error())
		}
		withheld[claim.component] = struct{}{}
	}
	return withheld, err
}

// checkStorageIsolation rejects a configuration that points two claimants at
// the same object store bucket.
//
// Both LokiStacks serve the "application" tenant, and Loki keys its objects by
// tenant ID rather than by LokiStack name, so two stacks sharing a bucket
// address the same index and chunk objects and overwrite each other. The
// LokiStack API exposes no per-stack object prefix, so separate buckets are the
// only isolation boundary available. Comparing SecretName would not catch this:
// two different Secrets can carry the same bucketnames.
//
// It returns a *storageConflictError when claimants overlap and a
// *storageCheckError when a Secret could not be resolved. An unresolvable
// Secret is reported as a failed check rather than treated as isolated: a
// guard that answers "no conflict" when it could not look is no guard at all.
//
// Fewer than two claimants returns nil without reading anything. A collision
// needs two writers, so a single-stack install is left alone however its
// storage is configured — including while its Secret is still missing, which
// the Loki operator reports on the stack itself.
func checkStorageIsolation(ctx context.Context, c client.Client, namespace string, claims []storageClaim) error {
	if len(claims) < 2 {
		return nil
	}

	// claimedBy maps a canonical location to the claimants that resolve to it,
	// so a third claimant is detected against both of the first two rather
	// than only against its immediate predecessor.
	claimedBy := map[string][]string{}
	var order []string
	for _, claim := range claims {
		locations, err := lokiStorageLocations(ctx, c, namespace, claim.secretName)
		if err != nil {
			return newStorageCheckError(err)
		}
		// One claimant naming a location twice — a bucketnames list repeating a
		// bucket, in any spelling that canonicalizes the same way — is still a
		// single claim on it. Counting both entries would read one writer as
		// two and report a stack as conflicting with itself. Deduping here
		// rather than by claimant name keeps the length test below sound even
		// if two claims ever share a description.
		claimed := map[string]struct{}{}
		for _, location := range locations {
			if _, repeated := claimed[location]; repeated {
				continue
			}
			claimed[location] = struct{}{}

			if len(claimedBy[location]) == 0 {
				order = append(order, location)
			}
			claimedBy[location] = append(claimedBy[location], claim.description)
		}
	}

	var shared []string
	claimants := make([]string, 0, len(claims))
	for _, location := range order {
		if len(claimedBy[location]) < 2 {
			continue
		}
		shared = append(shared, location)
		for _, claimant := range claimedBy[location] {
			if !slices.Contains(claimants, claimant) {
				claimants = append(claimants, claimant)
			}
		}
	}
	if len(shared) == 0 {
		return nil
	}

	return &storageConflictError{claimants: claimants, shared: shared}
}

// lokiStorageLocations resolves a LokiStack storage Secret to the canonical
// object store locations it writes to: the same bucket name on two different
// endpoints is two different buckets.
//
// Every way of not arriving at a location is an error, including a Secret that
// does not exist yet, one that names no bucket, and one whose endpoint is
// empty, hostless, or unparseable. Returning "no locations" there would make
// two unresolvable stacks compare as isolated, and the comparison is the only
// thing standing between them and a shared bucket. The cost of failing closed
// is that neither stack is created until both Secrets resolve; the storage
// Secret watch in SetupWithManager is what keeps that from stalling until the
// next periodic requeue.
func lokiStorageLocations(ctx context.Context, c client.Client, namespace, secretName string) ([]string, error) {
	if secretName == "" {
		return nil, fmt.Errorf("LokiStack storage secret name is empty: %w", errStorageSecretInvalid)
	}

	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, secret); err != nil {
		return nil, fmt.Errorf("reading LokiStack storage secret %s/%s: %w", namespace, secretName, err)
	}

	// The endpoint names the backend; without it a location cannot be named
	// either. An empty one once canonicalized to "///bucket", a key no valid
	// endpoint produces, and compared as isolated from the real endpoint
	// beside it — the same fail-closed rule as a bucketnames list with nothing
	// usable in it.
	endpoint := strings.TrimSpace(string(secret.Data["endpoint"]))
	if endpoint == "" {
		return nil, fmt.Errorf("LokiStack storage secret %s/%s has an empty endpoint: %w",
			namespace, secretName, errStorageSecretInvalid)
	}

	// bucketnames holds a comma-separated list; the stacks conflict if any one
	// bucket is common to both.
	var locations []string
	for bucket := range strings.SplitSeq(string(secret.Data["bucketnames"]), ",") {
		if bucket = strings.TrimSpace(bucket); bucket == "" {
			continue
		}
		location, err := canonicalStorageLocation(endpoint, bucket)
		if err != nil {
			// The sentinel is wrapped here rather than inside the canonicalizer
			// so the classification reports a Secret the user must fix rather
			// than a check that happened to fail. The cause stays wrapped
			// alongside it, so what was wrong with the endpoint remains
			// readable in the condition message.
			return nil, fmt.Errorf("LokiStack storage secret %s/%s: %w: %w",
				namespace, secretName, err, errStorageSecretInvalid)
		}
		locations = append(locations, location)
	}
	if len(locations) == 0 {
		return nil, fmt.Errorf("LokiStack storage secret %s/%s has no usable bucketnames entry: %w",
			namespace, secretName, errStorageSecretInvalid)
	}
	return locations, nil
}

// canonicalStorageLocation canonicalizes an (endpoint, bucket) pair into a
// comparable object store address, so that two spellings of one location
// compare equal instead of slipping past the overlap check.
//
// Nothing here is Loki-specific, and the name says so: it is string handling
// over an object store address, which is what lets a non-Loki claimant join
// storageClaims without the comparison growing a case for it.
//
// The comparison key is (host, port, path prefix, bucket).
//
// An endpoint that is empty, hostless, or unparseable is an error rather than
// a key. Each of those canonicalizes — or fails to canonicalize — to an
// address no valid endpoint can equal: the empty one to "///bucket", which
// compares as isolated from any real endpoint beside it and would let a
// claimant that names no backend past the gate. A malformed address fails
// closed, the same way a Secret that names no bucket does.
//
// The scheme is excluded. http and https are transports to one gateway rather
// than separate backends — MinIO, Ceph RGW, ODF and S3 all serve the same
// buckets over both — so keeping it in the key would let two claimants reach
// one bucket under two spellings and pass the comparison. Excluding it is also
// what lets an endpoint written bare, the spelling the Loki operator takes for
// an in-cluster gateway, compare equal to either spelling that names it.
//
// Bucket case and the DNS root dot are folded for the same reason. An S3
// bucket name is a DNS label, so a backend that distinguishes "my-bucket" from
// "MY-BUCKET" is out of spec, and "minio.odh.svc.cluster.local." is an
// ordinary in-cluster spelling of the host without the dot. Where a spelling
// is ambiguous the gate fails closed, because a conflict it invents is loud
// and the user can rename a bucket, while one it misses is silent corruption.
//
// Credentials are deliberately excluded: two identities writing to one bucket
// still collide, so comparing access keys would only mask the conflict. DNS
// aliases pointing at one server are not detected; that needs resolution, not
// parsing.
//
// The endpoint is also never echoed into an error. It is Secret data and may
// carry credentials in its userinfo, and these errors surface in status
// conditions and logs that far more principals can read than the Secret
// itself. The Secret-naming wrapper in lokiStorageLocations is what keeps the
// message diagnosable. The url.Parse cause is not wrapped for the same
// reason: its message quotes the raw URL it failed on.
func canonicalStorageLocation(endpoint, bucket string) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", errors.New("storage endpoint is empty")
	}
	bucket = strings.ToLower(strings.TrimSpace(bucket))

	// A bare "host" or "host:port" would parse as a scheme, so supply the
	// authority marker that makes url.Parse read it as one.
	toParse := endpoint
	if !strings.Contains(toParse, "://") {
		toParse = "//" + toParse
	}
	parsed, err := url.Parse(toParse)
	if err != nil {
		return "", errors.New("storage endpoint is not a valid URL")
	}

	// Hostnames are case-insensitive (RFC 4343) and the DNS root dot names the
	// same host as its absence.
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if host == "" {
		return "", errors.New("storage endpoint URL names no host")
	}

	// Virtual-host-style addressing folds the bucket into the hostname; strip it
	// so that form matches the path-style spelling of the same location. The
	// fold runs on the bare hostname, before the port goes back on, so it does
	// not depend on how the ported spelling of the host is written.
	if bucket != "" {
		host = strings.TrimPrefix(host, bucket+".")
	}

	// An explicitly written default port names the same endpoint as none at
	// all, whichever scheme carried it, since the scheme is not in the key.
	port := parsed.Port()
	if _, isDefault := defaultWebPorts[port]; isDefault {
		port = ""
	}
	if port != "" {
		// JoinHostPort restores the brackets url.Parse stripped from an IPv6
		// literal. A bracketless literal survives the portless path instead,
		// which is only ever compared against another key, never reparsed.
		host = net.JoinHostPort(host, port)
	}

	// path.Clean collapses repeated slashes and resolves dot segments. Prefix
	// case is significant on self-hosted gateways, so it is left alone.
	prefix := path.Clean("/" + parsed.Path)
	if prefix == "/" {
		prefix = ""
	}

	return "//" + host + prefix + "/" + bucket, nil
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

// usageLogsCredentialExists reports whether the Secret named by
// usageLogs.credentialSecret is present. Absent is the normal state on a fresh spoke,
// where the chart names the Secret and ACM delivers it afterwards.
func usageLogsCredentialExists(ctx context.Context, c client.Client, monitoring *v1alpha1.Monitoring) (bool, error) {
	secret := &corev1.Secret{}
	err := c.Get(ctx, types.NamespacedName{
		Name:      monitoring.Spec.UsageLogs.CredentialSecret,
		Namespace: monitoringNamespace(monitoring),
	}, secret)
	switch {
	case k8serr.IsNotFound(err):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("getting usage logs credential Secret: %w", err)
	}

	return true, nil
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
