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
	"fmt"
	"os"

	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
	corev1 "k8s.io/api/core/v1"
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
	CollectorRBACTemplate                            = "resources/collector-rbac.tmpl.yaml"
	CollectorMLflowRBACTemplate                      = "resources/collector-mlflow-rbac.tmpl.yaml"
	CollectorTempoRBACTemplate                       = "resources/collector-tempo-rbac.tmpl.yaml"
	PrometheusRouteTemplate                          = "resources/data-science-prometheus-route.tmpl.yaml"
	InstrumentationTemplate                          = "resources/instrumentation.tmpl.yaml"
	PrometheusNamespaceProxyTemplate                 = "resources/data-science-prometheus-namespace-proxy.tmpl.yaml"
	PrometheusNamespaceProxyNetworkPolicyTemplate    = "resources/data-science-prometheus-namespace-proxy-network-policy.tmpl.yaml"
	PrometheusServiceOverrideTemplate                = "resources/data-science-prometheus-service-override.tmpl.yaml"
	PrometheusNetworkPolicyTemplate                  = "resources/data-science-prometheus-network-policy.tmpl.yaml"
	PrometheusWebTLSServiceTemplate                  = "resources/prometheus-web-tls-service.tmpl.yaml"
	PrometheusSelfServiceMonitorTemplate             = "resources/prometheus-self-servicemonitor.tmpl.yaml"
	ThanosQuerierTemplate                            = "resources/thanos-querier-cr.tmpl.yaml"
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
	UsageLogsOpenTelemetryCollectorTemplate          = "resources/usage-logs-opentelemetry-collector.tmpl.yaml"
	UsageLogsOpenTelemetryCollectorRBACTemplate      = "resources/usage-logs-opentelemetry-collector-rbac.tmpl.yaml"
	LokiStackTemplate                                = "resources/loki-stack.tmpl.yaml"
	ClusterLogForwarderTemplate                      = "resources/cluster-log-forwarder.tmpl.yaml"
	ClusterLogForwarderRBACTemplate                  = "resources/cluster-log-forwarder-rbac.tmpl.yaml"

	PersesTempoDatasourceName = "tempo-datasource"
	PersesTempoDashboardName  = "data-science-tempo-traces"

	defaultMonitoringNamespace = "opendatahub"
	conditionTypeReady         = "Ready"
	conditionStatusTrue        = "True"
)

//go:embed resources monitoring
var resourcesFS embed.FS

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

// deployMonitoringStackWithQuerierAndRestrictions deploys MonitoringStack + ThanosQuerier.
func deployMonitoringStackWithQuerierAndRestrictions(
	ctx context.Context,
	c client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
) error {
	if monitoring.Spec.Metrics == nil {
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
		src(PrometheusRouteTemplate),
		src(PrometheusServiceOverrideTemplate),
		src(PrometheusNetworkPolicyTemplate),
		src(PrometheusNamespaceProxyTemplate),
		src(PrometheusNamespaceProxyNetworkPolicyTemplate),
		src(ThanosQuerierTemplate),
		src(ThanosQuerierRouteTemplate),
	)
	return nil
}

// deployTracingStack deploys Tempo + Instrumentation based on storage backend.
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
	instrExists, err := hasCRD(ctx, c, gvk.Instrumentation)
	if err != nil {
		return fmt.Errorf("checking Instrumentation CRD: %w", err)
	}

	if !tempoExists || !instrExists {
		if !tempoExists {
			cm.MarkFalse(conditions.ConditionTempoAvailable,
				tempoGVK.Kind+"CRDNotFoundReason",
				fmt.Sprintf("%s CRD not found (atomic deployment requires all CRDs)", tempoGVK.Kind))
		}
		if !instrExists {
			cm.MarkFalse(conditions.ConditionInstrumentationAvailable,
				"InstrumentationCRDNotFoundReason", "Instrumentation CRD not found (atomic deployment requires all CRDs)")
		}
		return nil
	}

	cm.MarkTrue(conditions.ConditionTempoAvailable)
	cm.MarkTrue(conditions.ConditionInstrumentationAvailable)

	*sources = append(*sources, src(tempoTemplate), src(InstrumentationTemplate))
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
		src(OpenTelemetryCollectorTemplate),
		src(CollectorRBACTemplate),
		src(CollectorServiceMonitorsTemplate),
		// Service for internal telemetry re-exported on :8890 with TLS (always-on)
		src(CollectorMonitorServiceTemplate),
		src(CollectorMonitoringNetworkPolicyTemplate),
	)

	if monitoring.Spec.Metrics != nil {
		*sources = append(*sources, src(CollectorPrometheusServiceTemplate))
	}

	if monitoring.Spec.Traces != nil {
		*sources = append(*sources,
			src(CollectorMLflowRBACTemplate),
			src(CollectorTempoRBACTemplate),
		)
	}

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
	*sources = append(*sources, src(OperatorPrometheusRulesTemplate))
	return nil
}

// deployPerses deploys the Perses CR when metrics or traces are configured.
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
	if monitoring.Spec.Metrics == nil && monitoring.Spec.Traces == nil {
		cm.MarkNotConfigured(conditions.ConditionPersesAvailable,
			conditions.MetricsAndTracesNotConfiguredReason,
			"Perses requires at least Metrics or Traces to be configured")
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

	if monitoring.Spec.Traces == nil {
		// Clean up existing Tempo datasource + dashboard if traces are deconfigured.
		if datasourceExists {
			ds := &unstructured.Unstructured{}
			ds.SetGroupVersionKind(datasourceGVK)
			ds.SetName(PersesTempoDatasourceName)
			ds.SetNamespace(monitoring.Spec.Namespace)
			if err := c.Delete(ctx, ds); err != nil && !k8serr.IsNotFound(err) {
				return fmt.Errorf("deleting PersesDatasource: %w", err)
			}
		}
		if dashboardExists {
			db := &unstructured.Unstructured{}
			db.SetGroupVersionKind(dashboardGVK)
			db.SetName(PersesTempoDashboardName)
			db.SetNamespace(monitoring.Spec.Namespace)
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

// deployPersesPrometheusIntegration deploys the Perses Prometheus datasource when metrics are configured.
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
	if monitoring.Spec.Metrics == nil {
		cm.MarkNotConfigured(conditions.ConditionPersesPrometheusDataSourceAvailable,
			conditions.MetricsNotConfiguredReason,
			"Prometheus datasource requires metrics configuration")
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

// deployNodeMetricsEndpoint deploys the node metrics cluster proxy when metrics are configured.
func deployNodeMetricsEndpoint(
	_ context.Context,
	_ client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
) error {
	if monitoring.Spec.Metrics == nil {
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
	)

	return nil
}

// deployLokiStack deploys LokiStack when usage logs storage or log forwarding is configured.
func deployLokiStack(
	ctx context.Context,
	c client.Client,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	sources *[]rendertemplate.TemplateSource,
) error {
	needsLoki := monitoring.Spec.Logs != nil ||
		(monitoring.Spec.UsageLogs != nil && monitoring.Spec.UsageLogs.Storage != nil)
	if !needsLoki {
		cm.MarkNotConfigured(conditions.ConditionLokiStackAvailable,
			"LokiNotRequired", "Neither usage logs storage nor log forwarding configured")
		return nil
	}

	lokiExists, err := hasCRD(ctx, c, gvk.LokiStack)
	if err != nil {
		return fmt.Errorf("checking LokiStack CRD: %w", err)
	}
	if !lokiExists {
		cm.MarkFalse(conditions.ConditionLokiStackAvailable,
			conditions.MissingOperatorReason,
			"LokiStack operator must be installed for usage logs storage configuration")
		return nil
	}

	// Check if LokiStack is actually ready
	lokiReady, err := isLokiStackReady(ctx, c, monitoring)
	if err != nil {
		return fmt.Errorf("checking LokiStack readiness: %w", err)
	}

	if lokiReady {
		cm.MarkTrue(conditions.ConditionLokiStackAvailable)
	} else {
		cm.MarkFalse(conditions.ConditionLokiStackAvailable,
			"LokiStackNotReady",
			"LokiStack is not ready yet")
	}

	*sources = append(*sources, src(LokiStackTemplate))
	return nil
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

	lokiReady, err := isLokiStackReady(ctx, c, monitoring)
	if err != nil {
		return fmt.Errorf("checking LokiStack readiness for log forwarding: %w", err)
	}
	if !lokiReady {
		cm.MarkFalse(conditions.ConditionClusterLogForwarderAvailable,
			"LokiStackNotReady",
			"LokiStack must be ready before ClusterLogForwarder can be deployed")
		return nil
	}

	logReady, err := isClusterLogForwarderReady(ctx, c, monitoring)
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

	*sources = append(*sources,
		src(ClusterLogForwarderTemplate),
		src(ClusterLogForwarderRBACTemplate),
	)
	return nil
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
		if condType == conditionTypeReady && condStatus == conditionStatusTrue {
			return true, nil
		}
	}

	return false, nil
}

// isLokiStackReady checks if the LokiStack CR exists and is in a Ready state.
func isLokiStackReady(ctx context.Context, c client.Client, monitoring *v1alpha1.Monitoring) (bool, error) {
	lokiStack := &unstructured.Unstructured{}
	lokiStack.SetGroupVersionKind(gvk.LokiStack)

	err := c.Get(ctx, types.NamespacedName{
		Name:      "data-science-lokistack",
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
func isClusterLogForwarderReady(ctx context.Context, c client.Client, monitoring *v1alpha1.Monitoring) (bool, error) {
	clusterLogForwarder := &unstructured.Unstructured{}
	clusterLogForwarder.SetGroupVersionKind(gvk.ClusterLogForwarder)

	err := c.Get(ctx, types.NamespacedName{
		Name:      "data-science-cluster-log-forwarder",
		Namespace: monitoringNamespace(monitoring),
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
