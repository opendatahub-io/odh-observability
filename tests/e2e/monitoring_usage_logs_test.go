package e2e_test

import (
	"testing"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega matchers
)

// ========================================================================
// Group 11: Usage Logs Collection
// ========================================================================

func (tc *MonitoringTestCtx) runUsageLogsCollectionTests(t *testing.T) {
	t.Helper()

	// TODO: LokiStack requires a working S3-compatible backend (e.g. MinIO).
	// The current test secret uses fake credentials against real AWS S3, so
	// LokiStack pods never become healthy and these tests always time out.
	// Re-enable once a MinIO fixture or real object storage is available.
	t.Skip("Skipped: requires S3-compatible storage backend (MinIO) for LokiStack")

	t.Run("Group 11: Usage Logs Collection", func(t *testing.T) {
		tc = tc.WithT(t)
		t.Cleanup(func() {
			tc.cleanupGroup(t, "")
		})

		// Test 1: Validate not deployed without config (modifies state, run first)
		t.Run("Test Usage Logs Collector not deployed without usage logs config", tc.ValidateUsageLogsCollectorNotDeployedWithoutConfig)

		// Setup shared resources once for validation tests
		secretName := "test-loki-shared-secret"
		t.Run("Setup shared UsageLogs resources", func(t *testing.T) {
			tc = tc.WithT(t)
			tc.setupUsageLogsWithStorage(t, "s3", secretName)

			// Wait for everything to be ready
			tc.EnsureResourceExists(
				WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
				WithCondition(And(
					jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
					jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsCollectorAvailable, metav1.ConditionTrue),
					jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionLokiStackAvailable, metav1.ConditionTrue),
				)),
				WithCustomErrorMsg("Shared setup: Monitoring should be ready with UsageLogs"),
			)

			tc.EnsureDeploymentReady(
				WithMinimalObject(gvk.Deployment, types.NamespacedName{
					Name:      UsageLogsCollectorName + "-collector",
					Namespace: tc.MonitoringNamespace,
				}),
			)
		})

		// All validation tests run against the same shared resources (read-only)
		// LokiStack tests run first since OTEL depends on LokiStack
		t.Run("Validate LokiStack deployment", tc.ValidateUsageLogsLokiStackDeployment)
		t.Run("Validate LokiStack configuration", tc.ValidateUsageLogsLokiStackConfiguration)
		t.Run("Validate Usage Logs Collector deployment", tc.ValidateUsageLogsCollectorDeployment)
		t.Run("Validate Usage Logs Collector configuration", tc.ValidateUsageLogsCollectorConfiguration)
		t.Run("Validate Usage Logs Collector RBAC", tc.ValidateUsageLogsCollectorRBACConfiguration)

		// Lifecycle test modifies state, run last
		t.Run("Test Usage Logs lifecycle (LokiStack + Collector)", tc.ValidateUsageLogsLifecycle)
	})
}

// ValidateUsageLogsCollectorNotDeployedWithoutConfig tests that the logs collector is not deployed when logs are not configured.
func (tc *MonitoringTestCtx) ValidateUsageLogsCollectorNotDeployedWithoutConfig(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withNoUsageLogs(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.spec.usageLogs == null`),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("Monitoring resource should be created without logs configuration"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(jq.Match(
			`[.status.conditions[] | select(.type=="%s" and .status=="False")] | length==1`,
			conditions.ConditionUsageLogsCollectorAvailable,
		)),
		WithCustomErrorMsg("UsageLogsCollectorAvailable condition should be False when logs are not configured"),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      UsageLogsCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      LokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
	)
}

// ValidateUsageLogsCollectorDeployment tests that the logs collector is deployed and ready when logs are configured.
func (tc *MonitoringTestCtx) ValidateUsageLogsCollectorDeployment(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.spec.usageLogs.storage.type == "s3"`),
			jq.Match(`.spec.usageLogs.storage.credentialMode == "static"`),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsCollectorAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionLokiStackAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("Monitoring resource should be ready with logs configuration and UsageLogsCollector and LokiStack available"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      UsageLogsCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.mode == "deployment"`),
			jq.Match(`.spec.replicas == 2`),
			monitoringOwnerReferencesCondition,
		)),
		WithCustomErrorMsg("Logs OpenTelemetryCollector should be created in deployment mode with 2 replicas"),
	)

	tc.EnsureDeploymentReady(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{
			Name:      UsageLogsCollectorName + "-collector",
			Namespace: tc.MonitoringNamespace,
		}),
	)
}

// ValidateUsageLogsCollectorConfiguration validates the logs collector configuration details.
func (tc *MonitoringTestCtx) ValidateUsageLogsCollectorConfiguration(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      UsageLogsCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			// Verify receivers
			jq.Match(`.spec.config.receivers.otlp.protocols.grpc.endpoint == "0.0.0.0:4317"`),
			jq.Match(`.spec.config.receivers.otlp.protocols.http.endpoint == "0.0.0.0:4318"`),

			// Verify processors
			jq.Match(`.spec.config.processors.k8sattributes != null`),
			jq.Match(`.spec.config.processors.k8sattributes.auth_type == "serviceAccount"`),
			jq.Match(`.spec.config.processors."groupbyattrs/maas" != null`),
			jq.Match(`.spec.config.processors.batch != null`),

			// Verify exporter endpoint (auto-generated from LokiStack)
			jq.Match(`.spec.config.exporters."otlphttp/loki".endpoint | test("https://data-science-lokistack-gateway-http\\..+\\.svc\\.cluster\\.local:8080/api/logs/v1/application/otlp")`),
			jq.Match(`
				(.spec.config.exporters."otlphttp/loki".tls.ca_file == "/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt") and
				(.spec.config.exporters."otlphttp/loki".auth.authenticator == "bearertokenauth")
			`),
			jq.Match(`.spec.config.exporters."otlphttp/loki".headers."X-Scope-OrgID" == "application"`),

			// Verify pipeline
			jq.Match(`.spec.config.service.pipelines.logs.receivers | contains(["otlp"])`),
			jq.Match(`.spec.config.service.pipelines.logs.processors | contains(["resource", "k8sattributes", "groupbyattrs/maas", "batch"])`),
			jq.Match(`.spec.config.service.pipelines.logs.exporters | contains(["otlphttp/loki"])`),
		)),
		WithCustomErrorMsg("Logs collector should have correct OTLP receivers, processors, and Loki exporter configuration"),
	)
}

// ValidateUsageLogsCollectorRBACConfiguration tests that the logs collector has correct RBAC permissions.
func (tc *MonitoringTestCtx) ValidateUsageLogsCollectorRBACConfiguration(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ServiceAccount, types.NamespacedName{
			Name:      UsageLogsCollectorServiceAccount,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCustomErrorMsg("ServiceAccount for logs collector should exist"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRole, types.NamespacedName{
			Name: UsageLogsCollectorName + "-processor",
		}),
		WithCondition(And(
			// k8sattributes processor requires pod/namespace metadata access
			jq.Match(`.rules[] | select(.apiGroups[] == "") | .resources | contains(["pods", "namespaces"])`),
			jq.Match(`.rules[] | select(.apiGroups[] == "") | .verbs | contains(["get", "watch", "list"])`),
			jq.Match(`.rules[] | select(.apiGroups[] == "apps") | .resources | contains(["replicasets"])`),
			jq.Match(`.rules[] | select(.apiGroups[] == "apps") | .verbs | contains(["get", "watch", "list"])`),
		)),
		WithCustomErrorMsg("ClusterRole should grant logs collector permissions for k8sattributes processor"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{
			Name: UsageLogsCollectorName + "-processor",
		}),
		WithCondition(And(
			jq.Match(`.roleRef.name == "%s"`, UsageLogsCollectorName+"-processor"),
			jq.Match(`.subjects[0].name == "%s"`, UsageLogsCollectorServiceAccount),
			jq.Match(`.subjects[0].namespace == "%s"`, tc.MonitoringNamespace),
		)),
		WithCustomErrorMsg("ClusterRoleBinding should bind logs collector ClusterRole to ServiceAccount"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{
			Name: UsageLogsCollectorName + "-loki-writer",
		}),
		WithCondition(And(
			jq.Match(`.roleRef.name == "lokistack-application-logs-writer"`),
			jq.Match(`.subjects[0].name == "%s"`, UsageLogsCollectorServiceAccount),
			jq.Match(`.subjects[0].namespace == "%s"`, tc.MonitoringNamespace),
		)),
		WithCustomErrorMsg("ClusterRoleBinding should bind lokistack-application-logs-writer role to logs collector ServiceAccount"),
	)
}

// ValidateUsageLogsLifecycle tests the complete lifecycle of usage logs (LokiStack + collector) deployment and cleanup.
func (tc *MonitoringTestCtx) ValidateUsageLogsLifecycle(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	secretName := "test-loki-lifecycle-secret"
	t.Cleanup(func() { tc.cleanupLokiStackAndSecret(secretName) })

	// Step 1: Enable usage logs
	tc.setupUsageLogsWithStorage(t, "s3", secretName)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      LokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCustomErrorMsg("LokiStack should be deployed when usage logs are enabled"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      UsageLogsCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`.spec.config.exporters."otlphttp/loki" != null`)),
		WithCustomErrorMsg("Logs collector should be deployed when usage logs are enabled"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionLokiStackAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsCollectorAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("LokiStackAvailable and UsageLogsCollectorAvailable conditions should be True when usage logs are enabled"),
	)

	// Step 2: Disable usage logs
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withNoUsageLogs(),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      LokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      UsageLogsCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionLokiStackAvailable, metav1.ConditionFalse),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsCollectorAvailable, metav1.ConditionFalse),
		)),
		WithCustomErrorMsg("LokiStackAvailable and UsageLogsCollectorAvailable conditions should be False when usage logs are disabled"),
	)

	// Step 3: Re-enable usage logs
	tc.setupUsageLogsWithStorage(t, "s3", secretName)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      LokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCustomErrorMsg("LokiStack should be recreated when usage logs are re-enabled"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      UsageLogsCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`.spec.config.exporters."otlphttp/loki" != null`)),
		WithCustomErrorMsg("Logs collector should be recreated when usage logs are re-enabled"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionLokiStackAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsCollectorAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("LokiStackAvailable and UsageLogsCollectorAvailable conditions should be True when usage logs are re-enabled"),
	)
}

// ValidateUsageLogsLokiStackDeployment tests that LokiStack is deployed with correct configuration.
func (tc *MonitoringTestCtx) ValidateUsageLogsLokiStackDeployment(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	// Verify LokiStack CR is created
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      LokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			monitoringOwnerReferencesCondition,
			jq.Match(`.spec.size == "1x.extra-small"`),
			jq.Match(`.spec.storage.secret.type == "s3"`),
			jq.Match(`.spec.storage.secret.credentialMode == "static"`),
			jq.Match(`.spec.storageClassName == "gp3-csi"`),
			jq.Match(`.spec.tenants.mode == "openshift-logging"`),
		)),
		WithCustomErrorMsg("LokiStack should be created with correct storage configuration"),
	)

	// Verify Monitoring condition
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionLokiStackAvailable, metav1.ConditionTrue),
		),
		WithCustomErrorMsg("LokiStackAvailable condition should be True when LokiStack is deployed"),
	)
}

// ValidateUsageLogsLokiStackConfiguration tests LokiStack with OTLP stream labels configuration.
func (tc *MonitoringTestCtx) ValidateUsageLogsLokiStackConfiguration(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      LokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes | length == 4`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "kubernetes_namespace_name")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "model")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "subscription")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "response_type")] | length == 1`),
		)),
		WithCustomErrorMsg("LokiStack should have correct OTLP stream labels"),
	)
}
