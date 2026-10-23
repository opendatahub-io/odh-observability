package e2e_test

import (
	"context"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gTypes "github.com/onsi/gomega/types"
	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	"github.com/opendatahub-io/odh-platform-utilities/pkg/cluster/olm"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega matchers
)

// Constants for monitoring resource names.
const (
	MonitoringStackName                  = "data-science-monitoringstack"
	OpenTelemetryCollectorName           = "data-science-collector"
	OpenTelemetryCollectorServiceName    = OpenTelemetryCollectorName + "-collector"
	TargetAllocatorDeploymentName        = "data-science-collector-targetallocator"
	TargetAllocatorCACertificateName     = OpenTelemetryCollectorName + "-ca-cert"
	TargetAllocatorClientCertificateName = OpenTelemetryCollectorName + "-ta-client-cert"
	TargetAllocatorServerCertificateName = OpenTelemetryCollectorName + "-ta-server-cert"
	CollectorServiceAccount              = "data-science-collector-collector"
	TargetAllocatorServiceAccount        = "data-science-collector-targetallocator"
	TempoMonolithicName                  = "data-science-tempomonolithic"
	TempoStackName                       = "data-science-tempostack"
	InstrumentationName                  = "data-science-instrumentation"
	ThanosQuerierName                    = "data-science-thanos-querier"
	ThanosQuerierRouteName               = "data-science-thanos-querier-route"
	ThanosQuerierProxyName               = "data-science-thanos-querier-proxy"
	PersesName                           = "data-science-perses"
	PersesDatasourceName                 = "data-science-prometheus-datasource"
	ClusterPrometheusDatasourceName      = "cluster-prometheus-datasource"
	ClusterPrometheusDatasourceSecret    = "cluster-prometheus-datasource-secret"
	UsageLogsCollectorName               = "data-science-usage-logs"
	UsageLogsCollectorServiceAccount     = "data-science-usage-logs-collector"
	LokiStackName                        = "data-science-lokistack"

	UsageLogsCollectorNetworkPolicyName = UsageLogsCollectorName + "-collector-allow"
	UsageLogsLokiStackNetworkPolicyName = LokiStackName + "-allow"
)

func cooOperatorChannel() string {
	if testOpts.cooChannel != "" {
		return testOpts.cooChannel
	}
	return observabilityOpChannel
}

// OLM operator constants for dependent operators.
const (
	observabilityOpName      = "cluster-observability-operator"
	observabilityOpNamespace = "openshift-cluster-observability-operator"
	observabilityOpChannel   = "stable"

	tempoOpName      = "tempo-product"
	tempoOpNamespace = "openshift-tempo-operator"
	tempoOpChannel   = "stable"

	opentelemetryOpName        = "opentelemetry-product"
	opentelemetryOpNamespace   = "openshift-opentelemetry-operator"
	opentelemetryOpChannel     = "stable"
	otelOperatorServiceAccount = "opentelemetry-operator-controller-manager"
	otelCertManagerRole        = "odh-observability-opentelemetry-cert-manager"

	certManagerOpName      = "openshift-cert-manager-operator"
	certManagerOpPackage   = "cert-manager-operator"
	certManagerOpNamespace = "cert-manager-operator"
	certManagerOpChannel   = "stable-v1"

	leaderWorkerSetOpName      = "leader-worker-set"
	leaderWorkerSetOpNamespace = "openshift-lws-operator"
	leaderWorkerSetOpChannel   = "stable-v1.0"

	connectivityLinkOpName      = "rhcl-operator"
	connectivityLinkOpNamespace = "openshift-operators"
	connectivityLinkOpChannel   = "stable"
	connectivityLinkOpSource    = "redhat-operators"
	kuadrantResourceNamespace   = "kuadrant-system"

	lokiOpName      = "loki-operator"
	lokiOpNamespace = "openshift-operators-redhat"

	// defaultInferenceMonitoringNamespace is the literal spec.monitoring.namespace written in
	// tests/e2e/prerequisites/inference/dsci.yaml, and defaultInferenceApplicationsNamespace the
	// literal spec.applicationsNamespace. renderManifest rewrites both to -monitoring-namespace
	// and -applications-namespace, so these are the substitution source, not just a default:
	// changing one without changing the manifest breaks the rewrite silently.
	//
	// The ODH layout puts both in opendatahub, which is what all three CRDs default to; run with
	// -monitoring-namespace=opendatahub -applications-namespace=opendatahub to get it.
	defaultInferenceMonitoringNamespace   = "redhat-ods-monitoring"
	defaultInferenceApplicationsNamespace = "redhat-ods-applications"

	clusterIngressCANamespace = "openshift-config-managed"
	clusterIngressCAConfigMap = "default-ingress-cert"
	clusterIngressCAKey       = "ca-bundle.crt"

	// platformOperatorRHOAI and platformOperatorODH name the platform operator Deployment and
	// webhook Service for inference prerequisite waits. detectPlatformOperator picks whichever
	// layout is installed.
	platformOperatorRHOAINamespace      = "redhat-ods-operator"
	platformOperatorRHOAIDeployment     = "rhods-operator"
	platformOperatorRHOAIWebhookService = "rhods-operator-service"
	platformOperatorODHNamespace        = "opendatahub-operator-system"
	platformOperatorODHDeployment       = "opendatahub-operator-controller-manager"
	platformOperatorODHWebhookService   = "opendatahub-operator-webhook-service"
	// lokiStorageType mirrors spec.monitoring.usageLogs.storage.type there.
	lokiStorageType = "s3"
	// lokiS3SecretName mirrors spec.monitoring.usageLogs.storage.secretName there. The test-
	// prefix is load-bearing: the suite overwrites this Secret and then deletes it, so the name
	// must not be one a real S3 backend would use.
	lokiS3SecretName = "test-loki-s3-secret"
	// lokiCredentialMode mirrors spec.monitoring.usageLogs.storage.credentialMode there.
	lokiCredentialMode = "static"
	// lokiStorageClassName is the literal spec.monitoring.usageLogs.storage.storageClassName the
	// inference manifests carry. It is a placeholder, not a requirement: renderManifest
	// substitutes the cluster's detected default class for it, the same way it substitutes the
	// redhat-ods-* namespaces. Nothing requires a cluster to have a class by this name.
	lokiStorageClassName = "gp3-csi"

	// maasConfigCRName is the singleton cluster-scoped MaaS Config the maas-controller bootstraps
	// once the DSC enables aigateway.modelsAsAService.
	maasConfigCRName = "default"

	// maasUsageLogEnvoyFilter is the EnvoyFilter the maas-controller creates on the shared
	// gateway once Config/default has spec.usageLogging=true. It is what emits usage logs to the
	// collector, and the only object of the several that flip creates that this repo depends on.
	maasUsageLogEnvoyFilter          = "maas-model-access-logs"
	maasUsageLogEnvoyFilterNamespace = "openshift-ingress"

	// maasTenantResourceAttribute is the resource attribute the collector maps to
	// kubernetes_namespace_name, which drives LokiStack per-tenant RBAC
	// (internal/controller/resources/usage-logs-opentelemetry-collector.tmpl.yaml:45-54).
	maasTenantResourceAttribute = "service.namespace"
)

// Constants for common test values.
const (
	DefaultRetention       = "5m"
	FormattedRetention     = "5m0s"
	MetricsStorageSize     = "1Gi"
	MetricsRetention       = "1h"
	OtlpCustomExporter     = "otlp/custom"
	OtlpHttpCustomExporter = "otlphttp/custom"
	OtlpTempoExporter      = "otlp/tempo"
	MetricsCPURequest      = "100m"
	MetricsMemoryRequest   = "256Mi"

	TracesStorageBackendPV  = "pv"
	TracesStorageBackendS3  = "s3"
	TracesStorageBackendGCS = "gcs"
	TracesStorageSize1Gi    = "1Gi"
)

// monitoringOwnerReferencesCondition validates ownership by the configured Monitoring CR.
//
//nolint:ireturn // Gomega's And matcher returns the GomegaMatcher interface.
func (tc *TestContext) monitoringOwnerReferencesCondition() gTypes.GomegaMatcher {
	return And(
		jq.Match(`.metadata.ownerReferences | length == 1`),
		jq.Match(`.metadata.ownerReferences[0].kind == "%s"`, gvk.Monitoring.Kind),
		jq.Match(`.metadata.ownerReferences[0].name == "%s"`, tc.MonitoringCRName),
	)
}

// rebaseForDSCI wraps transforms so they operate on DSCI's .spec.monitoring
// as if it were .spec on a Monitoring CR. This lets every existing transform
// function work unchanged in DSC mode.
func rebaseForDSCI(transforms ...jq.TransformFn) jq.TransformFn {
	return func(dsci *unstructured.Unstructured) error {
		monitoring, _, _ := unstructured.NestedMap(dsci.Object, "spec", "monitoring")
		if monitoring == nil {
			monitoring = map[string]any{}
		}

		temp := &unstructured.Unstructured{
			Object: map[string]any{"spec": monitoring},
		}

		for _, t := range transforms {
			if err := t(temp); err != nil {
				return err
			}
		}

		// gojq emits Go ints for whole numbers, while Kubernetes unstructured
		// objects require int64 values when SetNestedField deep-copies them.
		return unstructured.SetNestedField(dsci.Object, normalizeJQNumbers(temp.Object["spec"]), "spec", "monitoring")
	}
}

func normalizeJQNumbers(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			typed[key] = normalizeJQNumbers(child)
		}
	case []any:
		for index, child := range typed {
			typed[index] = normalizeJQNumbers(child)
		}
	case int:
		return int64(typed)
	}

	return value
}

// patchViaDSCI patches the DSCI's .spec.monitoring using rebaseForDSCI and waits
// for the Monitoring CR to reach the expected phase, or to be deleted when
// monitoring is Removed. This is the single entry point for DSC-mode mutations.
func (tc *MonitoringTestCtx) patchViaDSCI(expectedPhase common.Phase, transforms ...jq.TransformFn) {
	dsci := tc.EventuallyResourcePatched(
		WithMinimalObject(gvk.DSCInitialization, types.NamespacedName{Name: tc.DSCICRName}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			return rebaseForDSCI(transforms...)(u)
		}),
	)
	state, _, err := unstructured.NestedString(dsci.Object, "spec", "monitoring", "managementState")
	if err != nil {
		tc.t.Fatalf("read DSCI monitoring managementState: %v", err)
	}
	if state == string(common.Removed) {
		tc.EnsureResourceGone(
			WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		)
		return
	}

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(jq.Match(`.status.phase == "%s"`, expectedPhase)),
	)
}

// ensureMonitoringCRExists creates the Monitoring CR if it does not already exist.
// Readiness is checked after the suite clears any pre-existing optional config.
func (tc *MonitoringTestCtx) ensureMonitoringCRExists(t *testing.T) {
	t.Helper()

	if tc.ApiMode == APIModeDSC {
		tc.EventuallyResourcePatched(
			WithMinimalObject(gvk.DSCInitialization, types.NamespacedName{Name: tc.DSCICRName}),
			WithMutateFunc(rebaseForDSCI(withManagementState(common.Managed))),
		)
		tc.EnsureResourceExists(
			WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		)
		return
	}

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			if u.GetResourceVersion() == "" && tc.MonitoringNamespace != "" {
				if err := unstructured.SetNestedField(u.Object, tc.MonitoringNamespace, "spec", "namespace"); err != nil {
					return err
				}
			}
			return jq.TransformPipeline(withManagementState(common.Managed))(u)
		}),
		WithCondition(jq.Match(`.spec.managementState == "%s"`, common.Managed)),
		WithCustomErrorMsg("Monitoring CR should exist and be managed"),
	)
}

// updateMonitoringConfig patches the Monitoring CR with the given transforms
// and waits for the CR to reach Ready status.
// In DSC mode, it patches the DSCI instead and waits for the module handler
// to propagate changes to the Monitoring CR.
func (tc *MonitoringTestCtx) updateMonitoringConfig(transforms ...jq.TransformFn) {
	if tc.ApiMode == APIModeDSC {
		tc.patchViaDSCI(common.PhaseReady, transforms...)
		return
	}

	tc.updateMonitoringConfigWithOptions(WithMutateFunc(func(u *unstructured.Unstructured) error {
		return jq.TransformPipeline(transforms...)(u)
	}))
}

// updateMonitoringConfigWithoutReady applies configuration whose dependent
// operands need additional test setup before Monitoring can become Ready.
func (tc *MonitoringTestCtx) updateMonitoringConfigWithoutReady(transforms ...jq.TransformFn) {
	if tc.ApiMode == APIModeDSC {
		tc.EventuallyResourcePatched(
			WithMinimalObject(gvk.DSCInitialization, types.NamespacedName{Name: tc.DSCICRName}),
			WithMutateFunc(rebaseForDSCI(transforms...)),
		)
		return
	}

	tc.EventuallyResourcePatched(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithMutateFunc(jq.TransformPipeline(transforms...)),
	)
}

// updateMonitoringConfigWithOptions patches the Monitoring CR with advanced options.
func (tc *MonitoringTestCtx) updateMonitoringConfigWithOptions(opts ...ResourceOpts) {
	baseOpts := make([]ResourceOpts, 0, 2+len(opts))
	baseOpts = append(baseOpts,
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.phase == "%s"`, common.PhaseReady),
			jq.Match(`.status.observedGeneration >= .metadata.generation`),
		)),
	)
	tc.EventuallyResourcePatched(append(baseOpts, opts...)...)
}

// setupBaseMonitoring sets up Monitoring CR with managementState=Managed (no metrics, no traces).
func (tc *MonitoringTestCtx) setupBaseMonitoring(t *testing.T) {
	t.Helper()
	tc.updateMonitoringConfig(withManagementState(common.Managed))
}

// setupMetrics enables metrics configuration with default storage settings.
func (tc *MonitoringTestCtx) setupMetrics(t *testing.T) {
	t.Helper()
	current, err := tc.fetchResource(t, gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName})
	if err != nil && !k8serr.IsNotFound(err) {
		t.Fatalf("read Monitoring CR before enabling metrics: %v", err)
	}
	metricsPresent := false
	if err == nil {
		_, metricsPresent, err = unstructured.NestedMap(current.Object, "spec", "metrics")
		if err != nil {
			t.Fatalf("read Monitoring metrics configuration: %v", err)
		}
	}
	if !metricsPresent {
		tc.waitForPrometheusPodsGone(t)
	}
	transforms := []jq.TransformFn{
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	}
	if tc.expectedDefaultReplicas > 0 {
		transforms = append(transforms, withMetricsReplicas(tc.expectedDefaultReplicas))
	}
	tc.updateMonitoringConfig(transforms...)
	tc.waitForPrometheusReady(t)
}

// Restore the default metrics fixture after a subtest without tearing down the
// shared Prometheus and Thanos workloads used by the rest of the metrics phase.
func (tc *MonitoringTestCtx) resetMonitoringConfigToMetrics() {
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
		jq.Transform(`del(.spec.traces, .spec.alerting, .spec.collectorReplicas, .spec.usageLogs, .spec.logs)`),
	)
	tc.waitForPrometheusReady(tc.t)
}

// cleanupGroup performs group-level cleanup, resetting monitoring to a clean state.
func (tc *MonitoringTestCtx) cleanupGroup(t *testing.T, secretName string) {
	t.Helper()

	tc.resetMonitoringConfigToManaged()

	tc.DeleteResource(
		WithMinimalObject(gvk.TempoMonolithic, types.NamespacedName{
			Name:      TempoMonolithicName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithWaitForDeletion(true),
		WithIgnoreNotFound(true),
	)

	tc.DeleteResource(
		WithMinimalObject(gvk.TempoStack, types.NamespacedName{
			Name:      TempoStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithWaitForDeletion(true),
		WithIgnoreNotFound(true),
	)

	if secretName != "" {
		tc.DeleteResource(
			WithMinimalObject(gvk.Secret, types.NamespacedName{
				Name:      secretName,
				Namespace: tc.MonitoringNamespace,
			}),
			WithIgnoreNotFound(true),
			WithWaitForDeletion(true),
		)
	}
}

// resetMonitoringConfigToManaged deletes optional config fields and sets managementState=Managed.
func (tc *MonitoringTestCtx) resetMonitoringConfigToManaged() {
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		jq.Transform(`del(.spec.metrics, .spec.traces, .spec.alerting, .spec.collectorReplicas, .spec.usageLogs, .spec.logs)`),
	)

	tc.EnsureResourcesGone(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      OpenTelemetryCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.MonitoringStack, types.NamespacedName{
			Name:      MonitoringStackName,
			Namespace: tc.MonitoringNamespace,
		}),
	)
	tc.waitForPrometheusPodsGone(tc.t)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.ThanosQuerier, types.NamespacedName{
			Name:      ThanosQuerierName,
			Namespace: tc.MonitoringNamespace,
		}),
	)
	tc.EnsureResourceGone(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name: UsageLogsCollectorName, Namespace: tc.MonitoringNamespace,
		}),
	)
	tc.EnsureResourceGone(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name: LokiStackName, Namespace: tc.MonitoringNamespace,
		}),
		WithEventuallyTimeout(15*time.Minute),
	)
}

// resetMonitoringConfigToRemoved deletes optional config fields and sets managementState=Removed.
// In DSC mode, it patches the DSCI and waits for the Monitoring CR to be deleted.
func (tc *MonitoringTestCtx) resetMonitoringConfigToRemoved() {
	if tc.ApiMode == APIModeDSC {
		tc.patchViaDSCI(common.PhaseNotReady,
			withManagementState(common.Removed),
			jq.Transform(`del(.spec.metrics, .spec.traces, .spec.alerting, .spec.collectorReplicas, .spec.usageLogs, .spec.logs)`),
		)

		tc.EnsureResourcesGone(
			WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
				Name:      OpenTelemetryCollectorName,
				Namespace: tc.MonitoringNamespace,
			}),
		)

		return
	}

	tc.updateMonitoringConfigWithOptions(
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			return jq.TransformPipeline(
				withManagementState(common.Removed),
				jq.Transform(`del(.spec.metrics, .spec.traces, .spec.alerting, .spec.collectorReplicas, .spec.usageLogs, .spec.logs)`),
			)(u)
		}),
		WithCondition(jq.Match(`.status.phase == "%s"`, common.PhaseNotReady)),
	)

	tc.EnsureResourcesGone(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      OpenTelemetryCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
	)
}

func (tc *MonitoringTestCtx) ensureMonitoringDisabled() {
	monitoring := WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName})
	if tc.ApiMode == APIModeDSC {
		tc.EnsureResourceGone(monitoring)
		return
	}

	tc.EnsureResourceExists(
		monitoring,
		WithCondition(jq.Match(`.status.phase == "%s"`, common.PhaseNotReady)),
	)
}

// ensureMonitoringCleanSlate sets monitoring to Removed and cleans up all resources.
func (tc *MonitoringTestCtx) ensureMonitoringCleanSlate(t *testing.T, secretName string) {
	t.Helper()

	tc.resetMonitoringConfigToRemoved()

	tc.DeleteResource(
		WithMinimalObject(gvk.MonitoringStack, types.NamespacedName{
			Name:      MonitoringStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithWaitForDeletion(true),
		WithIgnoreNotFound(true),
	)

	tc.DeleteResource(
		WithMinimalObject(gvk.TempoMonolithic, types.NamespacedName{
			Name:      TempoMonolithicName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithWaitForDeletion(true),
		WithIgnoreNotFound(true),
	)

	tc.cleanupTempoStackAndSecret(secretName)
}

// ensureOpenTelemetryCollectorReady waits for the OTel Collector deployment to have at least one ready replica.
func (tc *TestContext) ensureOpenTelemetryCollectorReady(t *testing.T) {
	t.Helper()

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      OpenTelemetryCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`.status.scale.statusReplicas | split("/") | map(tonumber) | min > 0`)),
		WithCustomErrorMsg("OpenTelemetry Collector should have at least one ready replica"),
	)
}

// cleanupTempoStackAndSecret removes TempoStack and optionally an associated secret.
// Operand-only: it never touches the Monitoring CR, so a test that configured traces
// declaratively can still tear the operand down.
func (tc *TestContext) cleanupTempoStackAndSecret(secretName string) {
	tc.DeleteResource(
		WithMinimalObject(gvk.TempoStack, types.NamespacedName{
			Name:      TempoStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithWaitForDeletion(true),
		WithIgnoreNotFound(true),
		WithEventuallyTimeout(15*time.Minute),
	)

	if secretName != "" {
		tc.DeleteResource(
			WithMinimalObject(gvk.Secret, types.NamespacedName{
				Name:      secretName,
				Namespace: tc.MonitoringNamespace,
			}),
			WithIgnoreNotFound(true),
			WithWaitForDeletion(true),
		)
	}
}

// setupUsageLogsWithStorage creates a secret and configures usage logs with storage.
func (tc *MonitoringTestCtx) setupUsageLogsWithStorage(t *testing.T, storageType, secretName string) {
	t.Helper()
	tc.createLokiS3Secret(t, secretName, tc.MonitoringNamespace)
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withUsageLogsStorage(storageType, secretName, tc.DefaultStorageClass),
	)
}

// cleanupTracesConfiguration resets traces configuration.
func (tc *MonitoringTestCtx) cleanupTracesConfiguration() {
	tc.updateMonitoringConfig(withManagementState(common.Managed), withNoTraces())
}

// platformOperator identifies the ODH or RHOAI platform operator Deployment and webhook Service.
type platformOperator struct {
	Namespace      string
	Deployment     string
	WebhookService string
}

// clusterIngressCAPEM returns the PEM bundle used to verify OpenShift Route certificates.
func clusterIngressCAPEM(t *testing.T, ctx context.Context, c client.Client) string {
	t.Helper()

	configMap := &corev1.ConfigMap{}
	if err := c.Get(ctx, types.NamespacedName{
		Name:      clusterIngressCAConfigMap,
		Namespace: clusterIngressCANamespace,
	}, configMap); err != nil {
		t.Fatalf("failed to read the cluster ingress CA ConfigMap: %v", err)
	}
	caBundle := configMap.Data[clusterIngressCAKey]
	if caBundle == "" {
		t.Fatalf("cluster ingress CA ConfigMap %q is missing %q", clusterIngressCAConfigMap, clusterIngressCAKey)
	}
	return caBundle
}

// ingressCAPool returns a cert pool for HTTPS clients calling OpenShift Routes (system CAs plus
// the cluster ingress certificate bundle).
func ingressCAPool(t *testing.T, ctx context.Context, c client.Client) *x509.CertPool {
	t.Helper()

	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		rootCAs = x509.NewCertPool()
	}

	caBundle := clusterIngressCAPEM(t, ctx, c)
	if !rootCAs.AppendCertsFromPEM([]byte(caBundle)) {
		t.Fatalf("cluster ingress CA ConfigMap %q does not contain a valid PEM certificate bundle", clusterIngressCAConfigMap)
	}
	return rootCAs
}

// writeClusterIngressCAFile writes the cluster ingress CA bundle for tools (for example gcx)
// that read trust via GRAFANA_TLS_CA_FILE.
func writeClusterIngressCAFile(t *testing.T, ctx context.Context, c client.Client) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "openshift-ingress-ca.pem")
	if err := os.WriteFile(path, []byte(clusterIngressCAPEM(t, ctx, c)), 0o600); err != nil {
		t.Fatalf("failed to write cluster ingress CA bundle for OATS/gcx: %v", err)
	}
	return path
}

func detectPlatformOperator(ctx context.Context, c client.Client) (platformOperator, error) {
	layouts := []platformOperator{
		{
			Namespace:      platformOperatorRHOAINamespace,
			Deployment:     platformOperatorRHOAIDeployment,
			WebhookService: platformOperatorRHOAIWebhookService,
		},
		{
			Namespace:      platformOperatorODHNamespace,
			Deployment:     platformOperatorODHDeployment,
			WebhookService: platformOperatorODHWebhookService,
		},
	}
	for _, layout := range layouts {
		deployment := &unstructured.Unstructured{}
		deployment.SetGroupVersionKind(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"})
		err := c.Get(ctx, types.NamespacedName{Namespace: layout.Namespace, Name: layout.Deployment}, deployment)
		if k8serr.IsNotFound(err) {
			continue
		}
		if err != nil {
			return platformOperator{}, fmt.Errorf("checking platform operator %s/%s: %w", layout.Namespace, layout.Deployment, err)
		}
		return layout, nil
	}
	return platformOperator{}, fmt.Errorf(
		"platform operator deployment not found (tried %s/%s and %s/%s)",
		platformOperatorRHOAINamespace, platformOperatorRHOAIDeployment,
		platformOperatorODHNamespace, platformOperatorODHDeployment,
	)
}

// detectExpectedReplicas queries the cluster node count to determine expected Prometheus replicas.
func detectExpectedReplicas(t *testing.T, tc *TestContext) int {
	t.Helper()

	items := tc.EnsureResourcesExist(
		WithMinimalObject(gvk.Namespace, types.NamespacedName{}),
		WithCondition(Not(BeEmpty())),
	)

	nodeList := &unstructured.UnstructuredList{}
	nodeList.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("NodeList"))
	err := tc.client.List(tc.ctx, nodeList)
	require.NoError(t, err, "failed to list cluster nodes for replica detection")
	_ = items

	schedulable := 0
	for i := range nodeList.Items {
		unschedulable, _, _ := unstructured.NestedBool(nodeList.Items[i].Object, "spec", "unschedulable")
		if !unschedulable {
			schedulable++
		}
	}

	if schedulable <= 1 {
		t.Logf("detected single schedulable node — expecting 1 replica")
		return 1
	}

	t.Logf("detected %d schedulable nodes — expecting 2 replicas", schedulable)
	return 2
}

// createTempoStorageSecret creates a test secret for a locally hosted Tempo backend.
func (tc *TestContext) createTempoStorageSecret(t *testing.T, backendType, secretName, namespace string) {
	t.Helper()

	switch backendType {
	case "s3":
		tc.createTempoS3Secret(t, secretName, namespace)
	case "gcs":
		tc.createTempoGCSSecret(t, secretName, namespace)
	default:
		tc.g.Fail(fmt.Sprintf("Unsupported backend type: %s", backendType))
	}
}

// createTempoS3Secret creates an S3 secret compatible with TempoStack operator.
func (tc *TestContext) createTempoS3Secret(t *testing.T, secretName, namespace string) {
	t.Helper()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"access_key_id":     []byte(seaweedFSAccessKey),
			"access_key_secret": []byte(seaweedFSSecretKey),
			"bucket":            []byte(tempoS3Bucket),
			"endpoint":          fmt.Appendf(nil, "http://%s.%s.svc.cluster.local:%d", seaweedFSServiceName, namespace, seaweedFSS3Port),
		},
	}

	data := make(map[string]any, len(secret.Data))
	for k, v := range secret.Data {
		data[k] = string(v)
	}

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Secret, types.NamespacedName{Name: secret.Name, Namespace: secret.Namespace}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.Object["type"] = string(secret.Type)
			u.Object["stringData"] = data
			return nil
		}),
	)
}

// createTempoGCSSecret creates a GCS secret compatible with TempoStack operator.
func (tc *TestContext) createTempoGCSSecret(t *testing.T, secretName, namespace string) {
	t.Helper()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"bucketname": []byte(fakeGCSBucket),
			"key.json": []byte(`{
				"type": "service_account",
				"project_id": "fake-test-project-not-real",
				"private_key_id": "test-key-id-fake",
				"private_key": "-----BEGIN PRIVATE KEY-----\nTEST-FAKE-KEY-NOT-REAL\n-----END PRIVATE KEY-----\n",
				"client_email": "test-fake@fake-project.iam.gserviceaccount.com"
			}`),
		},
	}

	data := make(map[string]any, len(secret.Data))
	for k, v := range secret.Data {
		data[k] = string(v)
	}

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Secret, types.NamespacedName{Name: secret.Name, Namespace: secret.Namespace}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.Object["type"] = string(secret.Type)
			u.Object["stringData"] = data
			return nil
		}),
	)
}

// createLokiS3Secret creates an S3 secret compatible with LokiStack operator.
func (tc *TestContext) createLokiS3Secret(t *testing.T, secretName, namespace string) {
	t.Helper()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"access_key_id":     []byte(seaweedFSAccessKey),
			"access_key_secret": []byte(seaweedFSSecretKey),
			"bucketnames":       []byte(lokiS3Bucket),
			"endpoint":          fmt.Appendf(nil, "http://%s.%s.svc.cluster.local:%d", seaweedFSServiceName, namespace, seaweedFSS3Port),
			"region":            []byte("us-east-1"),
			"insecure":          []byte("true"),
			"s3ForcePathStyle":  []byte("true"),
		},
	}

	data := make(map[string]any, len(secret.Data))
	for k, v := range secret.Data {
		data[k] = string(v)
	}

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Secret, types.NamespacedName{Name: secret.Name, Namespace: secret.Namespace}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.Object["type"] = string(secret.Type)
			u.Object["stringData"] = data
			return nil
		}),
	)
}

// Transform functions — all paths operate on .spec.* directly (not .spec.monitoring.*).

func withManagementState(state common.ManagementState) jq.TransformFn {
	return jq.Transform(`.spec.managementState = "%s"`, state)
}

func (tc *TestContext) withMetricsConfig() jq.TransformFn {
	return jq.Transform(`.spec.metrics = {
        "storage": {
            "size": "%s",
            "retention": "%s"
        }
    }`, MetricsStorageSize, MetricsRetention)
}

func withMetricsReplicas(replicas int) jq.TransformFn {
	return func(u *unstructured.Unstructured) error {
		return unstructured.SetNestedField(u.Object, int64(replicas), "spec", "metrics", "replicas")
	}
}

func withNamespace(namespace string) jq.TransformFn {
	return jq.Transform(`.spec.namespace = "%s"`, namespace)
}

func withEmptyMetrics() jq.TransformFn {
	return jq.Transform(`.spec.metrics = {}`)
}

func withEmptyAlerting() jq.TransformFn {
	return jq.Transform(`.spec.alerting = {}`)
}

func withNoMetrics() jq.TransformFn {
	return jq.Transform(`del(.spec.metrics)`)
}

func withNoAlerting() jq.TransformFn {
	return jq.Transform(`del(.spec.alerting)`)
}

func withNoTraces() jq.TransformFn {
	return jq.Transform(`del(.spec.traces)`)
}

func withNoUsageLogs() jq.TransformFn {
	return jq.Transform(`del(.spec.usageLogs)`)
}

func withLogsStorage(storageType, secretName, storageClassName string) jq.TransformFn {
	if storageClassName == "" {
		return jq.Transform(`.spec.logs = {"storage": {"type": "%s", "secretName": "%s"}}`, storageType, secretName)
	}
	return jq.Transform(`.spec.logs = {"storage": {"type": "%s", "secretName": "%s", "storageClassName": "%s"}}`, storageType, secretName, storageClassName)
}

func withUsageLogsStorage(storageType, secretName, storageClassName string) jq.TransformFn {
	if storageClassName == "" {
		return jq.Transform(`.spec.usageLogs.storage = {"type": "%s", "secretName": "%s"}`, storageType, secretName)
	}
	return jq.Transform(`.spec.usageLogs.storage = {"type": "%s", "secretName": "%s", "storageClassName": "%s"}`, storageType, secretName, storageClassName)
}

func withNoCollectorReplicas() jq.TransformFn {
	return jq.Transform(`del(.spec.collectorReplicas)`)
}

func withCustomMetricsExporters() jq.TransformFn {
	return jq.Transform(`.spec.metrics.exporters = {
		"debug": {
			"verbosity": "detailed"
		},
        "%s": {
			"endpoint": "http://custom-backend:4317",
			"tls": {
				"insecure": true
			}
		}
	}`, OtlpCustomExporter)
}

func withCustomTracesExporters() jq.TransformFn {
	return jq.Transform(`.spec.traces.exporters = {
        "debug": {
            "verbosity": "detailed"
        },
        "%s": {
            "endpoint": "http://custom-endpoint:4318",
            "headers": {
                "api-key": "secret-key"
            }
        }
    }`, OtlpHttpCustomExporter)
}

func withReservedTracesExporter() jq.TransformFn {
	return jq.Transform(`.spec.traces.exporters = {
        "%s": {
            "endpoint": "http://malicious-endpoint:4317"
        }
    }`, OtlpTempoExporter)
}

func withMonitoringTraces(backend, secret, size, retention string) jq.TransformFn {
	transforms := []jq.TransformFn{
		jq.Transform(`.spec.traces = {
        "storage": {
            "backend": "%s"
        },
        "exporters": null
    }`, backend),
	}

	if secret != "" {
		transforms = append(transforms, jq.Transform(`.spec.traces.storage.secret = "%s"`, secret))
	}

	if retention != "" {
		transforms = append(transforms, jq.Transform(`.spec.traces.storage.retention = "%s"`, retention))
	}

	if backend == TracesStorageBackendPV && size != "" {
		transforms = append(transforms, jq.Transform(`.spec.traces.storage.size = "%s"`, size))
	}

	return jq.TransformPipeline(transforms...)
}

// registerMonitoringRestore records only the field the suite mutates. In DSC
// mode the real DSCI must already exist; module mode may start without a CR.
func (tc *MonitoringTestCtx) registerMonitoringRestore(t *testing.T) {
	t.Helper()

	kind := gvk.Monitoring
	nn := types.NamespacedName{Name: tc.MonitoringCRName}
	fieldPath := []string{"spec"}
	if tc.ApiMode == APIModeDSC {
		kind = gvk.DSCInitialization
		nn.Name = tc.DSCICRName
		fieldPath = []string{"spec", "monitoring"}
	}

	original, err := tc.fetchResource(t, kind, nn)
	existed := err == nil
	if err != nil && (tc.ApiMode != APIModeModule || !k8serr.IsNotFound(err)) {
		t.Fatalf("failed to record original %s %s: %v", kind.Kind, nn.Name, err)
	}

	var originalValue map[string]any
	var hadValue bool
	if existed {
		originalValue, hadValue, err = unstructured.NestedMap(original.Object, fieldPath...)
		require.NoError(t, err)
	}

	t.Cleanup(func() {
		if !existed {
			tc.DeleteResource(
				WithMinimalObject(kind, nn),
				WithIgnoreNotFound(true),
				WithWaitForDeletion(true),
				WithEventuallyTimeout(15*time.Minute),
			)
			return
		}

		tc.EventuallyResourcePatched(
			WithMinimalObject(kind, nn),
			WithMutateFunc(func(current *unstructured.Unstructured) error {
				if hadValue {
					return unstructured.SetNestedMap(current.Object, originalValue, fieldPath...)
				}
				unstructured.RemoveNestedField(current.Object, fieldPath...)
				return nil
			}),
			WithCustomErrorMsg("failed to restore original %s %s configuration", kind.Kind, nn.Name),
		)
	})
}

// ensurePrerequisites verifies that the operator is running, the CRD is
// registered, installs dependent operators (when enabled), and ensures the
// Monitoring CR exists before any test groups run. The monitoring namespace
// is auto-detected from the CR's spec.namespace unless overridden via flag.
func (tc *MonitoringTestCtx) ensurePrerequisites(t *testing.T) {
	t.Helper()
	tc.ensurePrerequisitesWithMonitoringReset(t, true)
}

// ensurePrerequisitesPreservingMonitoring validates the live monitoring stack
// without rewriting its configuration. Upgrade survivability depends on the
// same operands remaining available across the operator transition.
func (tc *MonitoringTestCtx) ensurePrerequisitesPreservingMonitoring(t *testing.T) {
	t.Helper()
	tc.ensurePrerequisitesWithMonitoringReset(t, false)
}

func (tc *MonitoringTestCtx) ensurePrerequisitesWithMonitoringReset(t *testing.T, resetMonitoring bool) {
	t.Helper()

	operatorMonitoringNamespace := tc.ensureOperatorDeploymentReady(t)
	if tc.ApiMode == APIModeModule && tc.MonitoringNamespace == "" {
		_, err := tc.fetchResource(t, gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName})
		switch {
		case err == nil:
			tc.MonitoringNamespace = tc.detectMonitoringNamespace(t)
		case k8serr.IsNotFound(err):
			tc.MonitoringNamespace = operatorMonitoringNamespace
		default:
			t.Fatalf("failed to check existing Monitoring CR %s: %v", tc.MonitoringCRName, err)
		}
	}
	tc.ensureCRDExists(t, gvk.Monitoring)
	tc.DefaultStorageClass = tc.ensureDefaultStorageClass(t)

	if tc.ApiMode == APIModeDSC {
		tc.ensureCRDExists(t, gvk.DSCInitialization)
	}
	if resetMonitoring {
		tc.registerMonitoringRestore(t)
	}

	if testOpts.installOperators {
		tc.installDependentOperators(t)
	}
	tc.ensureCRDExists(t, gvk.CertManagerCertificate)
	for _, name := range []string{
		"cluster-observability-operator",
		"tempo-operator",
		"opentelemetry-operator",
		certManagerOpPackage,
		"loki-operator",
	} {
		if testOpts.installOperators {
			tc.g.Eventually(func() error {
				return operatorExists(tc.Context(), tc.Client(), name)
			}).WithTimeout(tc.Timeouts.olmOperationTimeout).Should(Succeed(),
				"required OLM operator %s should be installed", name)
		} else {
			err := operatorExists(tc.Context(), tc.Client(), name)
			require.NoError(t, err, "required OLM operator %s is missing", name)
		}
	}
	for _, required := range []schema.GroupVersionKind{
		gvk.MonitoringStack,
		gvk.TempoMonolithic,
		gvk.TempoStack,
		gvk.OpenTelemetryCollector,
		gvk.Instrumentation,
		gvk.LokiStack,
		gvk.Perses,
		gvk.ThanosQuerier,
		gvk.PodMonitorCoreos,
		gvk.ServiceMonitorCoreos,
		gvk.ServiceMonitor,
	} {
		tc.ensureCRDExists(t, required)
	}

	// Operator metrics use monitoring.rhobs/v1 (gvk.ServiceMonitor), which COO
	// installs. ServiceMonitorCoreos above is the webhook target and a different CRD.
	// Create the operator ServiceMonitor in Module mode only.
	if tc.ApiMode == APIModeModule && resetMonitoring {
		tc.ensureOperatorServiceMonitor(t)
	}

	if resetMonitoring {
		tc.ensureMonitoringCRExists(t)
	} else {
		if tc.ApiMode == APIModeDSC {
			tc.EnsureResourceExists(
				WithMinimalObject(gvk.DSCInitialization, types.NamespacedName{Name: tc.DSCICRName}),
				WithCustomErrorMsg("upgrade survivability requires an existing DSCI"),
			)
		}
		tc.EnsureResourceExists(
			WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
			WithCustomErrorMsg("upgrade survivability requires an existing Monitoring CR"),
		)
	}

	if tc.MonitoringNamespace == "" {
		tc.MonitoringNamespace = tc.detectMonitoringNamespace(t)
		t.Logf("auto-detected monitoring namespace: %s", tc.MonitoringNamespace)
	}

	if resetMonitoring {
		tc.ensureNamespaceExists(tc.MonitoringNamespace)
		tc.resetMonitoringConfigToManaged()
	} else {
		tc.EnsureResourceExists(
			WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: tc.MonitoringNamespace}),
			WithCustomErrorMsg("active monitoring namespace %s must already exist", tc.MonitoringNamespace),
		)
	}
}

// operatorExists checks the OLM OperatorCondition API version served by the
// cluster before falling back to the shared OLM detector. Some OpenShift OLM
// environments serve OperatorCondition as v1, while newer environments may
// expose the v2 or ClusterExtension APIs used by olm.OperatorExists.
func operatorExists(ctx context.Context, cli client.Reader, operatorPrefix string) error {
	conditions := &unstructured.UnstructuredList{}
	conditions.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "operators.coreos.com", Version: "v1", Kind: "OperatorCondition",
	})
	if err := cli.List(ctx, conditions); err == nil {
		expectedPrefix := operatorPrefix + "."
		for _, condition := range conditions.Items {
			if strings.HasPrefix(condition.GetName(), expectedPrefix) {
				return nil
			}
		}
	} else if !meta.IsNoMatchError(err) {
		return err
	}

	_, err := olm.OperatorExists(ctx, cli, operatorPrefix)
	return err
}

// installDependentOperators installs required OLM operators. Cert-manager is
// installed first because the OpenTelemetry Operator detects cert-manager only
// during startup. The remaining operators are installed in parallel sub-tests.
// Each operator gets its own namespace, OperatorGroup, and Subscription when it
// is not already present.
func (tc *TestContext) installDependentOperators(t *testing.T) {
	t.Helper()

	type operator struct {
		namespace string
		name      string
		channel   string
	}

	t.Run(certManagerOpName, func(t *testing.T) {
		t.Helper()
		tc.EnsureOperatorInstalled(certManagerOpNamespace, certManagerOpName, certManagerOpChannel)
	})
	// The OpenTelemetry Operator checks the cert-manager API group and its own
	// permissions while starting its webhook. Do not start it until the CRD is
	// visible, otherwise its one-time autodetection can cache cert-manager as
	// unavailable for the rest of the test.
	tc.g.Eventually(func() error {
		certificates := &unstructured.UnstructuredList{}
		certificates.SetGroupVersionKind(gvk.CertManagerCertificate)
		return tc.client.List(tc.ctx, certificates, client.Limit(1))
	}).WithTimeout(tc.Timeouts.olmOperationTimeout).Should(Succeed(),
		"cert-manager Certificate API should be discoverable before OpenTelemetry starts")
	// Some Red Hat OpenTelemetry Operator CSVs omit the cert-manager rules
	// required by the operator's autodetection. Create the same supplemental
	// binding shipped by the production Helm chart before the OTel deployment
	// starts so TargetAllocator mTLS can use cert-manager.
	tc.ensureOpenTelemetryCertManagerRBAC(t)

	operators := []operator{
		{observabilityOpNamespace, observabilityOpName, cooOperatorChannel()},
		{tempoOpNamespace, tempoOpName, tempoOpChannel},
		{opentelemetryOpNamespace, opentelemetryOpName, opentelemetryOpChannel},
		// cert-manager is preinstalled by the CI pipeline from a different catalog
		// source than redhat-operators, so EnsureOperatorInstalled cannot match it.
	}

	t.Run("install-dependent-operators", func(t *testing.T) {
		for _, op := range operators {
			t.Run(op.name, func(t *testing.T) {
				t.Parallel()
				tc.EnsureOperatorInstalled(op.namespace, op.name, op.channel)
			})
		}
	})

	tc.restartOpenTelemetryOperator(t)
	for _, name := range []string{
		"opentelemetry-operator-controller-manager",
		"opentelemetry-operator-webhook",
	} {
		tc.EnsureDeploymentReady(
			WithMinimalObject(gvk.Deployment, types.NamespacedName{
				Name:      name,
				Namespace: opentelemetryOpNamespace,
			}),
			WithEventuallyTimeout(tc.Timeouts.olmOperationTimeout),
			WithCustomErrorMsg("OpenTelemetry Operator deployment %s should be ready", name),
		)
	}

	tc.ensureLokiOperatorInstalled(t)
}

// ensureOpenTelemetryCertManagerRBAC supplies the certificate permissions
// required by the OpenTelemetry Operator's cert-manager autodetection. This
// mirrors charts/odh-observability/templates/opentelemetry-cert-manager-rbac.yaml;
// the permissions are intentionally limited to the resources and verbs checked
// by the upstream operator.
func (tc *TestContext) ensureOpenTelemetryCertManagerRBAC(t *testing.T) {
	t.Helper()

	rules := []any{map[string]any{
		"apiGroups": []any{"cert-manager.io"},
		"resources": []any{"issuers", "certificaterequests", "certificates"},
		"verbs":     []any{"create", "get", "list", "watch", "update", "patch", "delete"},
	}}
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.ClusterRole, types.NamespacedName{Name: otelCertManagerRole}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			return unstructured.SetNestedSlice(u.Object, rules, "rules")
		}),
		WithEventuallyTimeout(tc.Timeouts.olmOperationTimeout),
		WithCustomErrorMsg("OpenTelemetry Operator cert-manager ClusterRole should exist"),
	)

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{Name: otelCertManagerRole}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			if err := unstructured.SetNestedMap(u.Object, map[string]any{
				"apiGroup": "rbac.authorization.k8s.io",
				"kind":     "ClusterRole",
				"name":     otelCertManagerRole,
			}, "roleRef"); err != nil {
				return err
			}
			return unstructured.SetNestedSlice(u.Object, []any{map[string]any{
				"kind":      "ServiceAccount",
				"name":      otelOperatorServiceAccount,
				"namespace": opentelemetryOpNamespace,
			}}, "subjects")
		}),
		WithEventuallyTimeout(tc.Timeouts.olmOperationTimeout),
		WithCustomErrorMsg("OpenTelemetry Operator cert-manager ClusterRoleBinding should exist"),
	)
}

// restartOpenTelemetryOperator forces a fresh cert-manager autodetection. This
// is needed when the OTel Operator was already installed before the dependency
// was made available; new installations also take the same deterministic path.
func (tc *TestContext) restartOpenTelemetryOperator(t *testing.T) {
	t.Helper()
	rolloutToken := time.Now().UTC().Format(time.RFC3339Nano)

	for _, name := range []string{
		"opentelemetry-operator-controller-manager",
		"opentelemetry-operator-webhook",
	} {
		deployment, err := tc.fetchResource(t, gvk.Deployment, types.NamespacedName{
			Name:      name,
			Namespace: opentelemetryOpNamespace,
		})
		if k8serr.IsNotFound(err) {
			continue
		}
		require.NoError(t, err, "failed to fetch OpenTelemetry Operator deployment %s", name)

		original := deployment.DeepCopy()
		annotations, _, err := unstructured.NestedStringMap(deployment.Object, "spec", "template", "metadata", "annotations")
		require.NoError(t, err, "failed to read OpenTelemetry Operator deployment %s annotations", name)
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations["e2e.opendatahub.io/cert-manager-rbac"] = rolloutToken
		require.NoError(t,
			unstructured.SetNestedStringMap(deployment.Object, annotations, "spec", "template", "metadata", "annotations"),
			"failed to update OpenTelemetry Operator deployment %s annotations", name)
		require.NoError(t, tc.client.Patch(tc.ctx, deployment, client.MergeFrom(original)),
			"failed to restart OpenTelemetry Operator deployment %s", name)
	}
}

// ensureLokiOperatorInstalled installs the Loki Operator unless the LokiStack CRD is
// already served. Required by both the usage logs and log forwarding features.
func (tc *TestContext) ensureLokiOperatorInstalled(t *testing.T) {
	t.Helper()

	// Loki channels are versioned, so use the cluster catalog's default channel.
	// A preinstalled Loki Operator may not have a PackageManifest in the catalog.
	lokiStacks := &unstructured.UnstructuredList{}
	lokiStacks.SetGroupVersionKind(gvk.LokiStack)
	if err := tc.Client().List(tc.Context(), lokiStacks); err == nil {
		t.Log("LokiStack CRD is already available")
		return
	} else if !meta.IsNoMatchError(err) && !k8serr.IsNotFound(err) {
		t.Fatalf("failed to check LokiStack CRD: %v", err)
	}

	manifests := &unstructured.UnstructuredList{}
	manifests.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "packages.operators.coreos.com", Version: "v1", Kind: "PackageManifestList",
	})
	if err := tc.Client().List(tc.Context(), manifests,
		client.InNamespace("openshift-marketplace"),
		client.MatchingLabels{"catalog": "redhat-operators"},
	); err != nil {
		t.Fatalf("Loki Operator is required; failed to list redhat-operators PackageManifests: %v", err)
	}
	for _, manifest := range manifests.Items {
		if manifest.GetName() != lokiOpName {
			continue
		}
		channel, _, err := unstructured.NestedString(manifest.Object, "status", "defaultChannel")
		if err != nil || channel == "" {
			t.Fatalf("redhat-operators Loki PackageManifest has no default channel: %v", err)
		}
		t.Logf("installing Loki Operator from redhat-operators channel %s", channel)
		tc.EnsureOperatorInstalled(lokiOpNamespace, lokiOpName, channel)
		return
	}
	t.Fatalf("Loki Operator is required; %s PackageManifest is missing from redhat-operators", lokiOpName)
}

// ensureOperatorServiceMonitor creates the operator's ServiceMonitor after COO is installed.
// This enables metrics scraping for the odh-observability operator itself.
func (tc *TestContext) ensureOperatorServiceMonitor(t *testing.T) {
	t.Helper()

	// Get the operator deployment to find its namespace
	deployments := &unstructured.UnstructuredList{}
	deployments.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "apps",
		Version: "v1",
		Kind:    "DeploymentList",
	})
	if err := tc.client.List(tc.ctx, deployments, client.MatchingFields{"metadata.name": "odh-observability"}); err != nil {
		t.Fatalf("failed to list odh-observability Deployment: %v", err)
	}
	if len(deployments.Items) == 0 {
		t.Fatalf("odh-observability Deployment not found")
	}
	operatorNS := deployments.Items[0].GetNamespace()
	monitoringNS := monitoringNamespaceFromDeployment(&deployments.Items[0])
	if monitoringNS == "" {
		monitoringNS = tc.MonitoringNamespace
	}
	if monitoringNS == "" {
		monitoringNS = operatorNS
	}
	// Module-mode setup creates this namespace only after the ServiceMonitor.
	// Konflux sets MONITORING_NAMESPACE to redhat-ods-monitoring before that
	// namespace exists, so create it here first.
	tc.ensureNamespaceExists(monitoringNS)

	// Create the CA ConfigMap for service-ca injection
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.ConfigMap, types.NamespacedName{
			Name:      "odh-observability-metrics-ca",
			Namespace: monitoringNS,
		}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			annotations := map[string]any{
				"service.beta.openshift.io/inject-cabundle": "true",
			}
			return unstructured.SetNestedMap(u.Object, annotations, "metadata", "annotations")
		}),
		WithCustomErrorMsg("operator metrics CA ConfigMap should be created"),
	)

	// Create the ServiceMonitor for operator metrics
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.ServiceMonitor, types.NamespacedName{
			Name:      "odh-observability-metrics",
			Namespace: monitoringNS,
		}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			spec := map[string]any{
				"endpoints": []any{
					map[string]any{
						"path":            "/metrics",
						"port":            "https",
						"scheme":          "https",
						"bearerTokenFile": "/var/run/secrets/kubernetes.io/serviceaccount/token",
						"tlsConfig": map[string]any{
							"ca": map[string]any{
								"configMap": map[string]any{
									"name": "odh-observability-metrics-ca",
									"key":  "service-ca.crt",
								},
							},
							"serverName": fmt.Sprintf("odh-observability-metrics.%s.svc", operatorNS),
						},
					},
				},
				"selector": map[string]any{
					"matchLabels": map[string]any{
						"app.kubernetes.io/name":     "odh-observability",
						"app.kubernetes.io/instance": "odh-observability",
					},
				},
				"namespaceSelector": map[string]any{
					"matchNames": []any{operatorNS},
				},
			}
			return unstructured.SetNestedMap(u.Object, spec, "spec")
		}),
		WithCustomErrorMsg("operator ServiceMonitor should be created after COO CRD is available"),
	)

	t.Logf("created operator ServiceMonitor in namespace %s for Service in %s", monitoringNS, operatorNS)
}

func monitoringNamespaceFromDeployment(deployment *unstructured.Unstructured) string {
	containers, _, _ := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	for _, raw := range containers {
		container, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		envs, _, _ := unstructured.NestedSlice(container, "env")
		for _, rawEnv := range envs {
			env, ok := rawEnv.(map[string]any)
			if !ok || env["name"] != "MONITORING_NAMESPACE" {
				continue
			}
			if value, ok := env["value"].(string); ok {
				return value
			}
		}
	}
	return ""
}

// Suppress unused warnings for transform functions used in later commits.
var (
	_ = withNamespace
	_ = withEmptyMetrics
	_ = withNoCollectorReplicas
)
