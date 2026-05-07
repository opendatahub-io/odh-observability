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

	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
)

const (
	MonitoringStackTemplate                       = "resources/monitoring-stack.tmpl.yaml"
	MonitoringAdmissionPoliciesTemplate           = "resources/monitoring-admission-policies.tmpl.yaml"
	MonitoringStackAlertmanagerRBACTemplate       = "resources/monitoringstack-alertmanager-rbac.tmpl.yaml"
	TempoMonolithicTemplate                       = "resources/tempo-monolithic.tmpl.yaml"
	TempoStackTemplate                            = "resources/tempo-stack.tmpl.yaml"
	OpenTelemetryCollectorTemplate                = "resources/opentelemetry-collector.tmpl.yaml"
	CollectorServiceMonitorsTemplate              = "resources/collector-servicemonitors.tmpl.yaml"
	CollectorPrometheusServiceTemplate            = "resources/collector-prometheus-service.tmpl.yaml"
	CollectorRBACTemplate                         = "resources/collector-rbac.tmpl.yaml"
	PrometheusRouteTemplate                       = "resources/data-science-prometheus-route.tmpl.yaml"
	InstrumentationTemplate                       = "resources/instrumentation.tmpl.yaml"
	PrometheusNamespaceProxyTemplate              = "resources/data-science-prometheus-namespace-proxy.tmpl.yaml"
	PrometheusNamespaceProxyNetworkPolicyTemplate = "resources/data-science-prometheus-namespace-proxy-network-policy.tmpl.yaml"
	PrometheusServiceOverrideTemplate             = "resources/data-science-prometheus-service-override.tmpl.yaml"
	PrometheusNetworkPolicyTemplate               = "resources/data-science-prometheus-network-policy.tmpl.yaml"
	PrometheusWebTLSServiceTemplate               = "resources/prometheus-web-tls-service.tmpl.yaml"
	PrometheusSelfServiceMonitorTemplate          = "resources/prometheus-self-servicemonitor.tmpl.yaml"
	ThanosQuerierTemplate                         = "resources/thanos-querier-cr.tmpl.yaml"
	ThanosQuerierRouteTemplate                    = "resources/thanos-querier-route.tmpl.yaml"
	PersesTemplate                                = "resources/perses.tmpl.yaml"
	PersesTempoDatasourceTemplate                 = "resources/perses-tempo-datasource.tmpl.yaml"
	PersesTempoDashboardV1Alpha1Template          = "resources/perses-tempo-dashboard-v1alpha1.tmpl.yaml"
	PersesTempoDashboardV1Alpha2Template          = "resources/perses-tempo-dashboard-v1alpha2.tmpl.yaml"
	PersesDatasourcePrometheusTemplate            = "resources/perses-datasource-prometheus.tmpl.yaml"
	PersesDatasourceClusterPrometheusTemplate     = "resources/perses-datasource-cluster-prometheus.tmpl.yaml"
	PrometheusClusterProxyTemplate                = "resources/data-science-prometheus-cluster-proxy.tmpl.yaml"
	TempoServiceCAConfigMapTemplate               = "resources/tempo-service-ca-configmap.tmpl.yaml"
	PersesOperatorAccessNetworkPolicyTemplate     = "resources/perses-operator-access-network-policy.tmpl.yaml"
	OperatorPrometheusRulesTemplate               = "monitoring/operator-prometheusrules.tmpl.yaml"

	PersesTempoDatasourceName = "tempo-datasource"
	PersesTempoDashboardName  = "data-science-tempo-traces"
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
	)

	if monitoring.Spec.Metrics != nil {
		*sources = append(*sources, src(CollectorPrometheusServiceTemplate))
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
