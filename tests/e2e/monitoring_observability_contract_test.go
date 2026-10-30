package e2e_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega
)

const (
	clusterLoggingNamespace        = "openshift-logging"
	clusterLogForwarderName        = "data-science-cluster-log-forwarder"
	clusterLogForwarderPolicyName  = clusterLogForwarderName + "-allow"
	clusterLogForwarderServiceAcct = clusterLogForwarderName + "-collector"
	// The two LokiStacks have independent storage, so each gets its own secret
	// and its own bucket. Nothing is shared between them.
	usageLogsLokiSecretName = "test-usage-logs-loki-secret"
	logsLokiSecretName      = "test-logs-loki-secret"
)

// ValidateMonitoringOperandNamespaceContract verifies the namespace handoff used by
// the platform integration. Standalone module mode intentionally has no DSCI, so
// only the Monitoring-side contract is applicable there.
func (tc *MonitoringTestCtx) ValidateMonitoringOperandNamespaceContract(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	monitoring := tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.spec.namespace != null and .spec.namespace != ""`),
			jq.Match(`.status.phase == "%s"`, common.PhaseReady),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeProvisioningSucceeded, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionMonitoringDependenciesReady, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("Monitoring must be Ready with dependencies established before namespace discovery"),
	)

	operandNamespace, found, err := unstructured.NestedString(monitoring.Object, "spec", "namespace")
	if err != nil || !found || operandNamespace == "" {
		t.Fatalf("Monitoring.spec.namespace is missing: found=%t error=%v", found, err)
	}
	if operandNamespace != tc.MonitoringNamespace {
		t.Fatalf("test context namespace %q does not match active Monitoring operand namespace %q", tc.MonitoringNamespace, operandNamespace)
	}

	if tc.ApiMode != APIModeDSC {
		t.Skipf("DSCI namespace handoff is not established in standalone module mode; Monitoring operand namespace is %q", operandNamespace)
	}

	dsci := tc.EnsureResourceExists(
		WithMinimalObject(gvk.DSCInitialization, types.NamespacedName{Name: tc.DSCICRName}),
		WithCondition(jq.Match(`.spec.monitoring.namespace != null and .spec.monitoring.namespace != ""`)),
		WithCustomErrorMsg("active DSCI must select the Monitoring operand namespace"),
	)
	dsciNamespace, found, err := unstructured.NestedString(dsci.Object, "spec", "monitoring", "namespace")
	if err != nil || !found || dsciNamespace == "" {
		t.Fatalf("DSCI.spec.monitoring.namespace is missing: found=%t error=%v", found, err)
	}
	if dsciNamespace != operandNamespace {
		t.Fatalf("DSCI namespace %q does not match Monitoring namespace %q", dsciNamespace, operandNamespace)
	}
	t.Logf("active operand namespace discovered from DSCI/Monitoring: %s", operandNamespace)
}

// ValidateSignalSpecificCollectorContract covers the operator-owned collector
// contract for metrics. It deliberately checks the rendered receivers,
// processors, exporter TLS files, and the internal, scrape, and OTLP metrics
// pipelines, rather than treating the OpenTelemetryCollector CR's existence as
// ingestion evidence.
func (tc *MonitoringTestCtx) ValidateSignalSpecificCollectorContract(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.ensureOpenTelemetryCollectorReady(t)
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name: OpenTelemetryCollectorName, Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.metadata.labels."platform.opendatahub.io/part-of" == "monitoring"`),
			jq.Match(`.spec.config.receivers | has("prometheus") and has("prometheus/self") and has("otlp")`),
			jq.Match(`(.spec.config.receivers.prometheus.config.scrape_configs // [] | any(.[]; .job_name == "data-science-collector-prometheus")) | not`),
			jq.Match(`.spec.config.processors | has("memory_limiter") and has("k8sattributes") and has("k8sattributes/otlp")`),
			jq.Match(`.spec.config.processors | has("transform/sanitize_otlp_identity") and has("resourcedetection") and has("batch")`),
			jq.Match(`.spec.config.exporters.prometheus.endpoint == "0.0.0.0:8889"`),
			jq.Match(`.spec.config.exporters.prometheus.tls.cert_file == "/etc/otel-collector/tls/tls.crt"`),
			jq.Match(`.spec.config.exporters.prometheus.tls.key_file == "/etc/otel-collector/tls/tls.key"`),
			jq.Match(`.spec.config.service.pipelines."metrics/internal".receivers | contains(["prometheus/self"])`),
			jq.Match(`.spec.config.service.pipelines."metrics/internal".exporters | contains(["prometheus/monitoring"])`),
			jq.Match(`.spec.config.service.pipelines."metrics/scrape".receivers | contains(["prometheus"]) and (contains(["otlp"]) | not)`),
			jq.Match(`.spec.config.service.pipelines."metrics/scrape".exporters | contains(["prometheus"])`),
			jq.Match(`.spec.config.service.pipelines."metrics/otlp".receivers | contains(["otlp"]) and (contains(["prometheus"]) | not)`),
			jq.Match(`.spec.config.service.pipelines."metrics/otlp".processors | contains(["transform/sanitize_otlp_identity", "k8sattributes/otlp"])`),
			jq.Match(`.spec.config.service.pipelines."metrics/otlp".exporters | contains(["prometheus"])`),
			jq.Match(`.spec.volumes[] | select(.name == "tls-certs") | .secret.secretName == "data-science-collector-tls"`),
		)),
		WithCustomErrorMsg("metrics collector contract must retain Prometheus receiver, TLS export, and signal pipelines, without the unused generic pod scrape job"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ServiceAccount, types.NamespacedName{
			Name: CollectorServiceAccount, Namespace: tc.MonitoringNamespace,
		}),
		WithCustomErrorMsg("metrics collector ServiceAccount should exist for bearer-token and Kubernetes metadata access"),
	)
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{Name: "generate-processors-collector-rolebinding"}),
		WithCondition(And(
			jq.Match(`.roleRef.name == "generate-processors-role"`),
			jq.Match(`.subjects | any(.[]; .kind == "ServiceAccount" and .name == "%s" and .namespace == "%s")`, CollectorServiceAccount, tc.MonitoringNamespace),
		)),
		WithCustomErrorMsg("collector discovery permissions must be bound to the collector ServiceAccount"),
	)
}

// ValidateTempoGatewayTenantConfiguration verifies the gateway-facing trace
// contract. Query and ingestion traffic must use the service CA, the active
// monitoring tenant, and caller authentication; a Tempo CR alone is not enough.
func (tc *MonitoringTestCtx) ValidateTempoGatewayTenantConfiguration(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withMonitoringTraces(TracesStorageBackendPV, "", TracesStorageSize1Gi, DefaultRetention),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionTempoAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionInstrumentationAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionOpenTelemetryCollectorAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("trace signal must have Tempo, Instrumentation, and collector conditions available"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.TempoMonolithic, types.NamespacedName{Name: TempoMonolithicName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`.spec.multitenancy.enabled == true`),
			jq.Match(`.spec.multitenancy.mode == "openshift"`),
			jq.Match(`.spec.multitenancy.authentication[0].tenantId == "%s"`, tc.MonitoringNamespace),
			jq.Match(`.spec.multitenancy.authentication[0].tenantName == "%s"`, tc.MonitoringNamespace),
			tc.monitoringOwnerReferencesCondition(),
		)),
		WithCustomErrorMsg("TempoMonolithic must expose the active monitoring tenant and owner reference"),
	)

	gatewayServiceName := "tempo-data-science-tempomonolithic-gateway"
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Service, types.NamespacedName{Name: gatewayServiceName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.spec.ports | [.[] | select(.port == 8080)] | length == 1`),
			jq.Match(`.spec.ports | [.[] | select(.port == 4317)] | length == 1`),
		)),
		WithCustomErrorMsg("Tempo gateway Service must expose authenticated query and OTLP ports"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ConfigMap, types.NamespacedName{Name: "tempo-service-ca", Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.metadata.annotations."service.beta.openshift.io/inject-cabundle" == "true"`)),
		WithCustomErrorMsg("Tempo service CA ConfigMap must be service-CA managed"),
	)
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Instrumentation, types.NamespacedName{Name: InstrumentationName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.spec.exporter.endpoint == "http://%s.%s.svc.cluster.local:4317"`, OpenTelemetryCollectorServiceName, tc.MonitoringNamespace)),
	)
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{Name: OpenTelemetryCollectorName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.spec.config.exporters."otlp/tempo".tls.ca_file == "/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt"`),
			jq.Match(`.spec.config.exporters."otlp/tempo".auth.authenticator == "bearertokenauth"`),
			jq.Match(`.spec.config.exporters."otlp/tempo".headers."X-Scope-OrgID" == "%s"`, tc.MonitoringNamespace),
			jq.Match(`.spec.config.service.pipelines.traces.receivers | contains(["otlp"])`),
			jq.Match(`.spec.config.service.pipelines.traces.processors | contains(["memory_limiter", "k8sattributes", "resourcedetection", "batch"])`),
			jq.Match(`.spec.config.service.pipelines.traces.exporters | contains(["otlp/tempo"])`),
		)),
		WithCustomErrorMsg("trace collector must forward through the TLS Tempo gateway with the active tenant"),
	)
}

// ValidateMonitoringPartialSignalConditions ensures that disabling metrics does
// not remove an independently healthy trace path. The assertion is intentionally
// about rendered conditions/resources, not telemetry availability.
func (tc *MonitoringTestCtx) ValidateMonitoringPartialSignalConditions(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withNoMetrics(),
		withMonitoringTraces(TracesStorageBackendPV, "", TracesStorageSize1Gi, DefaultRetention),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.phase == "%s"`, common.PhaseReady),
			jq.Match(
				`.status.conditions[] | select(.type == "%s" and .reason == "%s") | .status == "%s"`,
				conditions.ConditionMonitoringStackAvailable, conditions.MetricsNotConfiguredReason, metav1.ConditionFalse,
			),
			jq.Match(
				`.status.conditions[] | select(.type == "%s" and .reason == "%s") | .status == "%s"`,
				conditions.ConditionThanosQuerierAvailable, conditions.MetricsNotConfiguredReason, metav1.ConditionFalse,
			),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionTempoAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionInstrumentationAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionOpenTelemetryCollectorAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("disabling metrics must preserve healthy trace and collector conditions"),
	)
	tc.EnsureResourceGone(WithMinimalObject(gvk.MonitoringStack, types.NamespacedName{Name: MonitoringStackName, Namespace: tc.MonitoringNamespace}))
	tc.EnsureResourceGone(WithMinimalObject(gvk.ThanosQuerier, types.NamespacedName{Name: ThanosQuerierName, Namespace: tc.MonitoringNamespace}))
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.TempoMonolithic, types.NamespacedName{Name: TempoMonolithicName, Namespace: tc.MonitoringNamespace}),
		WithCustomErrorMsg("Tempo must remain rendered when metrics are disabled independently"),
	)
}

// ValidateClusterLogForwarderContract skips only when the target cluster lacks
// the optional Cluster Logging prerequisites. Once log forwarding is configured,
// Monitoring must report the ClusterLogForwarder contract as available.
func (tc *MonitoringTestCtx) ValidateClusterLogForwarderContract(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	if !tc.clusterLogForwarderCRDAvailable(t) {
		t.Skipf("ClusterLogForwarder contract is not established: ClusterLogForwarder CRD is unavailable")
	}
	if err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: "openshift-service-ca.crt", Namespace: tc.MonitoringNamespace}, &corev1.ConfigMap{}); err != nil {
		if k8serr.IsNotFound(err) {
			t.Skipf("ClusterLogForwarder contract is not established: service CA ConfigMap is unavailable in %s", tc.MonitoringNamespace)
		}
		t.Fatalf("failed to read service CA ConfigMap: %v", err)
	}

	t.Cleanup(func() {
		// Preserve the usageLogs setup for the remaining usage-log tests.
		tc.updateMonitoringConfig(withManagementState(common.Managed), jq.Transform(`del(.spec.logs)`))
	})

	// The ClusterLogForwarder targets the log forwarding LokiStack, a different
	// CR from the usage logs one, and the operator withholds the contract until
	// that stack is ready. setupLogsLokiStack is a no-op beyond the wait when
	// the stack is already up from an earlier test in the group.
	tc.setupLogsLokiStack(t)

	status, reason, message, found := tc.waitForMonitoringCondition(t, conditions.ConditionClusterLogForwarderAvailable)
	if !found {
		t.Fatalf("ClusterLogForwarder contract must report Monitoring condition %s after log forwarding was configured", conditions.ConditionClusterLogForwarderAvailable)
	}
	if status != metav1.ConditionTrue {
		t.Fatalf(
			"ClusterLogForwarder condition %s is not True after log forwarding was configured: "+
				"status=%s reason=%s message=%s",
			conditions.ConditionClusterLogForwarderAvailable, status, reason, message,
		)
	}

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterLogForwarder, types.NamespacedName{Name: clusterLogForwarderName, Namespace: clusterLoggingNamespace}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "Ready") | .status == "True"`),
			jq.Match(`.spec.serviceAccount.name == "%s"`, clusterLogForwarderServiceAcct),
			jq.Match(`.spec.inputs[0].type == "application"`),
			jq.Match(`.spec.inputs[0].application.includes | map(.namespace) | contains(["%s"])`, tc.MonitoringNamespace),
			jq.Match(`.spec.outputs[0].type == "lokiStack"`),
			jq.Match(`.spec.outputs[0].lokiStack.target.name == "%s"`, LokiStackName),
			jq.Match(`.spec.outputs[0].lokiStack.target.namespace == "%s"`, tc.MonitoringNamespace),
			jq.Match(`.spec.outputs[0].lokiStack.authentication.token.from == "serviceAccount"`),
			jq.Match(`.spec.outputs[0].tls.ca.configMapName == "openshift-service-ca.crt"`),
			jq.Match(`.spec.outputs[0].tls.ca.key == "service-ca.crt"`),
			jq.Match(`.spec.pipelines[0].inputRefs | contains(["inference-app-logs"])`),
			jq.Match(`.spec.pipelines[0].outputRefs | contains(["data-science-lokistack"])`),
		)),
		WithCustomErrorMsg("ClusterLogForwarder must route application logs to the active Loki application tenant"),
	)

	for _, binding := range []string{
		clusterLogForwarderServiceAcct + "-collect-app-logs",
		clusterLogForwarderServiceAcct + "-loki-writer",
	} {
		tc.EnsureResourceExists(
			WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{Name: binding}),
			WithCondition(jq.Match(`.subjects | any(.[]; .kind == "ServiceAccount" and .name == "%s" and .namespace == "%s")`, clusterLogForwarderServiceAcct, clusterLoggingNamespace)),
			WithCustomErrorMsg("ClusterLogForwarder ServiceAccount must retain its collection and Loki writer binding"),
		)
	}

	var vector *appsv1.DaemonSet
	tc.g.Eventually(func() error {
		list := &appsv1.DaemonSetList{}
		if err := tc.Client().List(tc.Context(), list, client.InNamespace(clusterLoggingNamespace)); err != nil {
			return err
		}
		for i := range list.Items {
			candidate := &list.Items[i]
			for _, owner := range candidate.OwnerReferences {
				if owner.Kind == gvk.ClusterLogForwarder.Kind && owner.Name == clusterLogForwarderName {
					vector = candidate
					desired := candidate.Status.DesiredNumberScheduled
					if desired > 0 && candidate.Status.NumberReady == desired {
						return nil
					}
					return fmt.Errorf("Vector DaemonSet %s has ready=%d desired=%d", candidate.Name, candidate.Status.NumberReady, desired)
				}
			}
		}
		return errors.New("Vector DaemonSet owned by ClusterLogForwarder was not found")
	}).WithTimeout(5*time.Minute).WithPolling(5*time.Second).Should(Succeed(),
		"ClusterLogForwarder readiness requires an owned Vector DaemonSet with all desired pods ready")
	if vector != nil {
		t.Logf("ClusterLogForwarder evidence: name=%s vector_ready=%d vector_desired=%d", clusterLogForwarderName, vector.Status.NumberReady, vector.Status.DesiredNumberScheduled)
		for _, owner := range vector.OwnerReferences {
			if owner.Kind == gvk.Monitoring.Kind {
				t.Fatalf("Vector DaemonSet must remain owned by ClusterLogForwarder, found Monitoring owner %q", owner.Name)
			}
		}
	}

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{Name: clusterLogForwarderPolicyName, Namespace: clusterLoggingNamespace}),
		WithCondition(And(
			tc.monitoringOwnerReferencesCondition(),
			jq.Match(`.spec.podSelector.matchLabels."app.kubernetes.io/component" == "collector"`),
			jq.Match(`.spec.podSelector.matchLabels."app.kubernetes.io/instance" == "%s"`, clusterLogForwarderName),
			jq.Match(`.spec.podSelector.matchLabels."app.kubernetes.io/name" == "vector"`),
			jq.Match(`.spec.podSelector.matchLabels."app.kubernetes.io/part-of" == "cluster-logging"`),
			jq.Match(`.spec.policyTypes | contains(["Ingress", "Egress"])`),
			jq.Match(`
				any(.spec.ingress[]?;
					any(.from[]?;
						.namespaceSelector.matchLabels."kubernetes.io/metadata.name" == "openshift-monitoring" and
						.podSelector.matchLabels."app.kubernetes.io/instance" == "k8s") and
					any(.ports[]?; .protocol == "TCP" and .port == 24231))
			`),
			jq.Match(`any(.spec.egress[]?; any(.to[]?; .namespaceSelector.matchLabels."kubernetes.io/metadata.name" == "openshift-dns") and any(.ports[]?; .port == 53))`),
			jq.Match(`any(.spec.egress[]?; any(.to[]?; .ipBlock.cidr != null) and any(.ports[]?; .protocol == "TCP" and .port == 6443))`),
			jq.Match(`
				any(.spec.egress[]?;
					any(.to[]?;
						.namespaceSelector.matchLabels."kubernetes.io/metadata.name" == "%s" and
						.podSelector.matchLabels."app.kubernetes.io/component" == "lokistack-gateway") and
					any(.ports[]?; .protocol == "TCP" and .port == 8080))
			`, tc.MonitoringNamespace),
		)),
		WithCustomErrorMsg("Vector NetworkPolicy must be owned by Monitoring and restrict Prometheus, DNS, Kubernetes API, and Loki traffic"),
	)

	// A delete event must enqueue the singleton and recreate the cross-namespace policy.
	tc.DeleteResource(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{Name: clusterLogForwarderPolicyName, Namespace: clusterLoggingNamespace}),
		WithWaitForDeletion(true),
	)
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{Name: clusterLogForwarderPolicyName, Namespace: clusterLoggingNamespace}),
		WithCondition(tc.monitoringOwnerReferencesCondition()),
		WithCustomErrorMsg("deleting the Vector NetworkPolicy must trigger cross-namespace reconciliation"),
	)

	// Direct pod-log access remains a fallback only; it must not replace the
	// historical Loki store when logs are configured.
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ConfigMap, types.NamespacedName{Name: "korrel8r-config", Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.data."korrel8r.yaml" | contains("domain: log") and contains("direct: true") and contains("application")`)),
		WithCustomErrorMsg("Korrel8r log store must retain the application tenant and direct pod-log fallback"),
	)

	// Removing logs must remove both the ClusterLogForwarder and its companion policy.
	tc.updateMonitoringConfigWithoutReady(
		withManagementState(common.Managed),
		jq.Transform(`del(.spec.logs)`),
	)
	tc.EnsureResourceGone(WithMinimalObject(gvk.ClusterLogForwarder, types.NamespacedName{Name: clusterLogForwarderName, Namespace: clusterLoggingNamespace}))
	tc.EnsureResourceGone(WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{Name: clusterLogForwarderPolicyName, Namespace: clusterLoggingNamespace}))
}

func (tc *MonitoringTestCtx) clusterLogForwarderCRDAvailable(t *testing.T) bool {
	t.Helper()
	items := &unstructured.UnstructuredList{}
	items.SetGroupVersionKind(gvk.ClusterLogForwarder)
	err := tc.Client().List(tc.Context(), items, client.Limit(1))
	if err == nil {
		return true
	}
	if meta.IsNoMatchError(err) || k8serr.IsNotFound(err) {
		return false
	}
	t.Fatalf("failed to discover ClusterLogForwarder CRD: %v", err)
	return false
}

func (tc *MonitoringTestCtx) waitForMonitoringCondition(t *testing.T, conditionType string) (metav1.ConditionStatus, string, string, bool) {
	t.Helper()
	var latestStatus metav1.ConditionStatus
	var latestReason, latestMessage string
	var found bool
	tc.g.Eventually(func() error {
		monitoring := &unstructured.Unstructured{}
		monitoring.SetGroupVersionKind(gvk.Monitoring)
		if err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: tc.MonitoringCRName}, monitoring); err != nil {
			if k8serr.IsNotFound(err) {
				return err
			}
			return StopErr(err, "failed to read Monitoring resource %s", tc.MonitoringCRName)
		}

		status, reason, message, conditionFound := monitoringCondition(monitoring, conditionType)
		if conditionFound {
			latestStatus, latestReason, latestMessage, found = status, reason, message, true
			if status == metav1.ConditionTrue {
				return nil
			}
		}
		return fmt.Errorf("Monitoring condition %s is not True", conditionType)
	}).WithContext(tc.Context()).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(Succeed())
	return latestStatus, latestReason, latestMessage, found
}

func monitoringCondition(obj *unstructured.Unstructured, conditionType string) (metav1.ConditionStatus, string, string, bool) {
	conditionsList, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil || !found {
		return "", "", "", false
	}
	var status metav1.ConditionStatus
	var reason, message string
	var conditionFound bool
	for _, item := range conditionsList {
		condition, ok := item.(map[string]any)
		if !ok || condition["type"] != conditionType {
			continue
		}
		conditionStatus, _ := condition["status"].(string)
		status = metav1.ConditionStatus(conditionStatus)
		reason, _ = condition["reason"].(string)
		message, _ = condition["message"].(string)
		conditionFound = true
	}
	return status, reason, message, conditionFound
}
