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

	t.Run("Group 11: Usage Logs Collection", func(t *testing.T) {
		tc = tc.WithT(t)
		const sharedSecretName = "test-loki-shared-secret"
		const lifecycleSecretName = "test-loki-lifecycle-secret"
		t.Cleanup(tc.cleanupSeaweedFS)
		t.Cleanup(func() {
			for _, secretName := range []string{sharedSecretName, lifecycleSecretName} {
				tc.DeleteResource(
					WithMinimalObject(gvk.Secret, types.NamespacedName{Name: secretName, Namespace: tc.MonitoringNamespace}),
					WithIgnoreNotFound(true),
					WithWaitForDeletion(true),
				)
			}
		})
		t.Cleanup(func() { tc.cleanupGroup(t, "") })

		// Test 1: Validate not deployed without config (modifies state, run first)
		t.Run("Test Usage Logs Collector not deployed without usage logs config", tc.ValidateUsageLogsCollectorNotDeployedWithoutConfig)

		// Setup shared resources once for validation tests
		tc.startSeaweedFS(t, lokiS3Bucket)
		t.Run("Setup shared UsageLogs resources", func(t *testing.T) {
			tc = tc.WithT(t)
			tc.setupUsageLogsWithStorage(t, "s3", sharedSecretName)

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
		t.Run("Validate Usage Logs Collector network policies", tc.ValidateUsageLogsCollectorNetworkPolicies)

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

	for _, name := range []string{UsageLogsCollectorNetworkPolicyName, UsageLogsLokiStackNetworkPolicyName} {
		tc.EnsureResourceGone(
			WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
				Name:      name,
				Namespace: tc.MonitoringNamespace,
			}),
		)
	}
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
			tc.monitoringOwnerReferencesCondition(),
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
			// The operator-managed NetworkPolicy is disabled; ingress is governed by
			// the NetworkPolicies this operator renders (see ValidateUsageLogsCollectorNetworkPolicies).
			jq.Match(`.spec.networkPolicy.enabled == false`),

			// Verify receivers
			jq.Match(`.spec.config.receivers.otlp.protocols.grpc.endpoint == "0.0.0.0:4317"`),
			jq.Match(`.spec.config.receivers.otlp.protocols.http.endpoint == "0.0.0.0:4318"`),

			// Verify processors
			jq.Match(`.spec.config.processors.k8sattributes != null`),
			jq.Match(`.spec.config.processors.k8sattributes.auth_type == "serviceAccount"`),
			jq.Match(`.spec.config.processors."groupbyattrs/maas" != null`),
			jq.Match(`.spec.config.processors.batch != null`),

			// log_type is pinned to the Loki application tenant. It must NOT be copied
			// from log_name: MaaS relies on log_name keeping its envoy access-logger value.
			jq.Match(`
				[.spec.config.processors.resource.attributes[] | select(.key == "log_type")] ==
				[{"action": "upsert", "key": "log_type", "value": "application"}]
			`),
			// The AI tenant namespace arrives on service.namespace and drives LokiStack
			// RBAC via kubernetes_namespace_name. insert (not upsert) so a namespace
			// already set by the sender wins.
			jq.Match(`
				[.spec.config.processors.resource.attributes[] | select(.key == "kubernetes_namespace_name")] ==
				[{"action": "insert", "key": "kubernetes_namespace_name", "from_attribute": "service.namespace"}]
			`),
			// Cleanup must preserve log_name for MaaS consumers.
			jq.Match(`
				[.spec.config.processors."transform/cleanup".log_statements[].statements[] | select(test("log_name"))] | length == 0
			`),
			jq.Match(`
				[.spec.config.processors."transform/cleanup".log_statements[].statements[] | select(test("service.namespace"))] | length == 1
			`),

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
			// Loki application tenant write access for the otlphttp/loki exporter
			jq.Match(`
				.rules[] | select(.apiGroups[] == "loki.grafana.com") |
				(.resources | contains(["application"])) and
				(.resourceNames | contains(["logs"])) and
				(.verbs | contains(["create"]))
			`),
		)),
		WithCustomErrorMsg("ClusterRole should grant logs collector permissions for k8sattributes processor and Loki writes"),
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
}

// ValidateUsageLogsCollectorNetworkPolicies tests the NetworkPolicies that gate traffic into the
// collector and into the LokiStack gateway. These stand in for RBAC on the write path: write
// authorization cannot be delegated to Loki, so reachability is restricted instead.
func (tc *MonitoringTestCtx) ValidateUsageLogsCollectorNetworkPolicies(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
			Name:      UsageLogsCollectorNetworkPolicyName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.podSelector.matchLabels."app.kubernetes.io/name" == "%s"`, UsageLogsCollectorName+"-collector"),
			jq.Match(`.spec.policyTypes == ["Ingress"]`),
			// OTLP ingest is reachable only from the ingress policy group (the MaaS gateway).
			jq.Match(`
				[.spec.ingress[] | select(.from != null)] | length == 1
			`),
			jq.Match(`
				[.spec.ingress[] | select(.from != null) |
					.from[0].namespaceSelector.matchLabels."network.openshift.io/policy-group" == "ingress"] == [true]
			`),
			jq.Match(`
				[.spec.ingress[] | select(.from != null) | .ports[] | select(.protocol == "TCP") | .port] == [4317, 4318]
			`),
			// Internal telemetry scraping stays open cluster-wide (no from selector).
			jq.Match(`
				[.spec.ingress[] | select(.from == null) | .ports[] | select(.protocol == "TCP") | .port] == [8888]
			`),
		)),
		WithCustomErrorMsg("Usage logs collector NetworkPolicy should allow OTLP only from the ingress policy group and expose the metrics port"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
			Name:      UsageLogsLokiStackNetworkPolicyName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			// Guards the gateway pods that receive the connection.
			jq.Match(`.spec.podSelector.matchLabels."app.kubernetes.io/component" == "lokistack-gateway"`),
			jq.Match(`.spec.podSelector.matchLabels."app.kubernetes.io/instance" == "%s"`, LokiStackName),
			jq.Match(`.spec.policyTypes == ["Ingress"]`),
			jq.Match(`
				[.spec.ingress[].from[] | .podSelector.matchLabels."app.kubernetes.io/name"] == ["%s"]
			`, UsageLogsCollectorName+"-collector"),
			jq.Match(`
				[.spec.ingress[].ports[] | select(.protocol == "TCP") | .port] == [8080]
			`),
		)),
		WithCustomErrorMsg("LokiStack gateway NetworkPolicy should only admit the usage logs collector on 8080"),
	)
}

// ValidateUsageLogsLifecycle tests the complete lifecycle of usage logs (LokiStack + collector) deployment and cleanup.
func (tc *MonitoringTestCtx) ValidateUsageLogsLifecycle(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	secretName := "test-loki-lifecycle-secret"

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

	for _, name := range []string{UsageLogsCollectorNetworkPolicyName, UsageLogsLokiStackNetworkPolicyName} {
		tc.EnsureResourceExists(
			WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
				Name:      name,
				Namespace: tc.MonitoringNamespace,
			}),
			WithCustomErrorMsg("NetworkPolicy %s should be deployed when usage logs are enabled", name),
		)
	}

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

	for _, name := range []string{UsageLogsCollectorNetworkPolicyName, UsageLogsLokiStackNetworkPolicyName} {
		tc.EnsureResourceGone(
			WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
				Name:      name,
				Namespace: tc.MonitoringNamespace,
			}),
		)
	}

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

	for _, name := range []string{UsageLogsCollectorNetworkPolicyName, UsageLogsLokiStackNetworkPolicyName} {
		tc.EnsureResourceExists(
			WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
				Name:      name,
				Namespace: tc.MonitoringNamespace,
			}),
			WithCustomErrorMsg("NetworkPolicy %s should be recreated when usage logs are re-enabled", name),
		)
	}

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
			tc.monitoringOwnerReferencesCondition(),
			jq.Match(`.spec.size == "1x.extra-small"`),
			jq.Match(`.spec.storage.secret.type == "s3"`),
			jq.Match(`.spec.storage.secret.credentialMode == "static"`),
			jq.Match(`.spec.storageClassName == "%s"`, tc.DefaultStorageClass),
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
			jq.Match(`.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes | length == 8`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "kubernetes_namespace_name")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "model")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "subscription")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "response_type")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "gateway_namespace_name")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "gateway_deployment_name")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "upstream_namespace_name")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "upstream_deployment_name")] | length == 1`),
		)),
		WithCustomErrorMsg("LokiStack should have correct OTLP stream labels"),
	)
}
