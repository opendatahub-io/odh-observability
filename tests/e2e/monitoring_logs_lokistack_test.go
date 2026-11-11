package e2e_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega matchers
)

// Log forwarding gets its own LokiStack, separate from the usage logs one. The
// validations below mirror the usage logs LokiStack coverage in
// monitoring_usage_logs_test.go against spec.logs instead of spec.usageLogs,
// and additionally assert that neither stack disturbs the other.

// lokiStackReadyTimeout bounds LokiStack provisioning: the Loki operator has to
// schedule the full read/write/gateway set and have it reach Ready.
const lokiStackReadyTimeout = 20 * time.Minute

// setupLogsLokiStack configures log forwarding storage and waits for its
// LokiStack to come up. The usage logs stack stays configured throughout, so
// this also establishes that the two coexist.
func (tc *MonitoringTestCtx) setupLogsLokiStack(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(func() {
		if t.Failed() {
			tc.logLokiStackDiagnostics(t, LokiStackName, UsageLogsLokiStackName)
		}
	})

	tc.setupLogsWithStorage(t, "s3", logsLokiSecretName)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionLokiStackAvailable, metav1.ConditionTrue),
			// The usage logs stack must be untouched by configuring log forwarding.
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsLokiStackAvailable, metav1.ConditionTrue),
		)),
		WithEventuallyTimeout(lokiStackReadyTimeout),
		WithCustomErrorMsg("Both LokiStacks should be available once log forwarding storage is configured"),
	)
}

// Log the state before cleanup removes the stacks and their pods.
func (tc *MonitoringTestCtx) logLokiStackDiagnostics(t *testing.T, names ...string) {
	t.Helper()
	for _, name := range names {
		stack, err := tc.fetchResource(t, gvk.LokiStack, types.NamespacedName{Name: name, Namespace: tc.MonitoringNamespace})
		if err != nil {
			t.Logf("LokiStack %s: %v", name, err)
		} else {
			status, _, _ := unstructured.NestedMap(stack.Object, "status")
			t.Logf("LokiStack %s status: %v", name, status)
		}
		pods := &corev1.PodList{}
		if err := tc.Client().List(tc.Context(), pods, client.InNamespace(tc.MonitoringNamespace),
			client.MatchingLabels{"app.kubernetes.io/instance": name}); err != nil {
			t.Logf("LokiStack %s pods: %v", name, err)
			continue
		}
		for _, pod := range pods.Items {
			t.Logf("LokiStack %s pod %s: phase=%s conditions=%v containers=%v",
				name, pod.Name, pod.Status.Phase, pod.Status.Conditions, pod.Status.ContainerStatuses)
		}
	}
}

// ValidateLogsLokiStackDeployment tests that the log forwarding LokiStack is
// deployed from spec.logs.storage with correct configuration.
func (tc *MonitoringTestCtx) ValidateLogsLokiStackDeployment(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      LokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			tc.monitoringOwnerReferencesCondition(),
			jq.Match(`.spec.size == "1x.demo"`),
			jq.Match(`.spec.storage.secret.name == "%s"`, logsLokiSecretName),
			jq.Match(`.spec.storage.secret.type == "s3"`),
			jq.Match(`.spec.storage.secret.credentialMode == "static"`),
			jq.Match(`.spec.storageClassName == "%s"`, tc.DefaultStorageClass),
			jq.Match(`.spec.tenants.mode == "openshift-logging"`),
		)),
		WithCustomErrorMsg("Log forwarding LokiStack should be created with the storage configured under spec.logs"),
	)

	// The two stacks are separate CRs with separate storage: neither secret may
	// leak into the other stack.
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      UsageLogsLokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`.spec.storage.secret.name == "%s"`, usageLogsLokiSecretName)),
		WithCustomErrorMsg("Usage logs LokiStack must keep its own storage secret when log forwarding is configured"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionLokiStackAvailable, metav1.ConditionTrue),
		),
		WithCustomErrorMsg("LokiStackAvailable condition should be True when the log forwarding LokiStack is deployed"),
	)

	// These two policies ride with the log forwarding stack, so they are only
	// rendered while spec.logs is set. They are asserted here rather than
	// alongside the usage logs policies because nothing in a usage-logs-only
	// install creates them. Their configuration — selectors, peers, ports,
	// policy types — is a template contract pinned in unit tests
	// (TestOperandNetworkPolicyTemplatesContract and
	// TestLokiGatewayNetworkPolicyAdmitsOnlyDeclaredPeers in
	// internal/controller); e2e keeps what only a live cluster can show,
	// existence and Monitoring ownership.
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
			Name:      LokiStackNetworkPolicyName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(tc.monitoringOwnerReferencesCondition()),
		WithCustomErrorMsg("LokiStack NetworkPolicy should exist and be owned by Monitoring while spec.logs is set"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
			Name:      LokiStackGatewayNetworkPolicyName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(tc.monitoringOwnerReferencesCondition()),
		WithCustomErrorMsg("LokiStack gateway NetworkPolicy should exist and be owned by Monitoring while spec.logs is set"),
	)
}

// ValidateApplicationLogsProjectRBAC checks Stage-1 project log query access:
// the RHOAI-owned aggregate ClusterRole exists, its rules land on OpenShift
// admin/edit/view, a project view binding authorizes the namespaced SAR (and
// is denied cluster-scoped), and the LokiStack gateway returns 200 for that
// view token vs 403 without the project role — the story's HTTP failure mode.
func (tc *MonitoringTestCtx) ValidateApplicationLogsProjectRBAC(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	const applicationLogsViewRole = "data-science-application-logs-view"
	// jq.Match requires a boolean; select alone yields the rule object.
	const lokiApplicationLogsRule = `([.rules[] | select(
		.apiGroups == ["loki.grafana.com"] and
		.resources == ["application"] and
		.resourceNames == ["logs"] and
		.verbs == ["get"]
	)] | length) == 1`

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRole, types.NamespacedName{Name: applicationLogsViewRole}),
		WithCondition(And(
			jq.Match(`.metadata.labels["rbac.authorization.k8s.io/aggregate-to-admin"] == "true"`),
			jq.Match(`.metadata.labels["rbac.authorization.k8s.io/aggregate-to-edit"] == "true"`),
			jq.Match(`.metadata.labels["rbac.authorization.k8s.io/aggregate-to-view"] == "true"`),
			jq.Match(`.metadata.labels["platform.opendatahub.io/component"] == "loki-logs"`),
			jq.Match(lokiApplicationLogsRule),
		)),
		WithCustomErrorMsg("data-science-application-logs-view must exist and aggregate into OpenShift project roles while spec.logs is set"),
	)

	for _, roleName := range []string{"admin", "edit", "view"} {
		tc.EnsureResourceExists(
			WithMinimalObject(gvk.ClusterRole, types.NamespacedName{Name: roleName}),
			WithCondition(jq.Match(lokiApplicationLogsRule)),
			WithCustomErrorMsg("OpenShift %s ClusterRole must receive get application.loki.grafana.com/logs via aggregation", roleName),
		)
	}

	probeName := "application-logs-view-probe"
	serviceAccount := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name: probeName, Namespace: tc.MonitoringNamespace,
	}}
	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: probeName, Namespace: tc.MonitoringNamespace},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "view",
		},
		Subjects: []rbacv1.Subject{{
			Kind: "ServiceAccount", Name: probeName, Namespace: tc.MonitoringNamespace,
		}},
	}
	// Delete-and-recreate so a leftover RoleBinding cannot silently keep a wrong RoleRef.
	for _, obj := range []client.Object{roleBinding, serviceAccount} {
		if err := tc.Client().Delete(tc.Context(), obj); err != nil && !k8serr.IsNotFound(err) {
			t.Fatalf("failed to clear application-logs view probe %T: %v", obj, err)
		}
	}
	tc.g.Eventually(func(g Gomega) {
		err := tc.Client().Get(tc.Context(), client.ObjectKeyFromObject(serviceAccount), &corev1.ServiceAccount{})
		g.Expect(k8serr.IsNotFound(err)).To(BeTrue(), "probe ServiceAccount should be gone: %v", err)
		err = tc.Client().Get(tc.Context(), client.ObjectKeyFromObject(roleBinding), &rbacv1.RoleBinding{})
		g.Expect(k8serr.IsNotFound(err)).To(BeTrue(), "probe RoleBinding should be gone: %v", err)
	}).Should(Succeed())

	if err := tc.Client().Create(tc.Context(), serviceAccount); err != nil {
		t.Fatalf("failed to create application-logs view probe ServiceAccount: %v", err)
	}
	if err := tc.Client().Create(tc.Context(), roleBinding); err != nil {
		t.Fatalf("failed to create application-logs view probe RoleBinding: %v", err)
	}
	t.Cleanup(func() {
		for _, obj := range []client.Object{roleBinding, serviceAccount} {
			if err := tc.Client().Delete(tc.Context(), obj); err != nil && !k8serr.IsNotFound(err) {
				t.Logf("failed to clean up application-logs view probe %T: %v", obj, err)
			}
		}
	})

	probeUser := fmt.Sprintf("system:serviceaccount:%s:%s", tc.MonitoringNamespace, probeName)
	tc.g.Eventually(func(g Gomega) {
		namespaced, err := subjectCanGetApplicationLogs(tc, probeUser, tc.MonitoringNamespace)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(namespaced).To(BeTrue(),
			"project view binding must authorize namespaced get application.loki.grafana.com/logs")

		clusterScoped, err := subjectCanGetApplicationLogs(tc, probeUser, "")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(clusterScoped).To(BeFalse(),
			"project view binding must not authorize cluster-scoped get application.loki.grafana.com/logs")
	}).Should(Succeed())

	tc.assertApplicationLogsGatewayStage1(t, probeName)
}

// assertApplicationLogsGatewayStage1 hits the LokiStack gateway the way the
// story fails today: project view token must not get Stage-1 403; a token
// without project roles must. Also asserts Stage-2: view in this project still
// gets 403 for a foreign namespace selector (rule has no LokiStack identity, so
// Stage-1 alone must not widen cross-namespace data). The probe pod wears
// Korrel8r labels so the gateway NetworkPolicy admits the request.
func (tc *MonitoringTestCtx) assertApplicationLogsGatewayStage1(t *testing.T, viewProbeSA string) {
	t.Helper()
	tc = tc.WithT(t)

	gatewayService := LokiStackName + "-gateway-http"
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Service, types.NamespacedName{Name: gatewayService, Namespace: tc.MonitoringNamespace}),
		WithCustomErrorMsg("log-forwarding LokiStack gateway Service must exist before Stage-1 HTTP probe"),
	)

	deniedName := "application-logs-denied-probe"
	deniedSA := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name: deniedName, Namespace: tc.MonitoringNamespace,
	}}
	if err := tc.Client().Delete(tc.Context(), deniedSA); err != nil && !k8serr.IsNotFound(err) {
		t.Fatalf("failed to clear denied gateway probe ServiceAccount: %v", err)
	}
	tc.g.Eventually(func(g Gomega) {
		err := tc.Client().Get(tc.Context(), client.ObjectKeyFromObject(deniedSA), &corev1.ServiceAccount{})
		g.Expect(k8serr.IsNotFound(err)).To(BeTrue(), "denied probe ServiceAccount should be gone: %v", err)
	}).Should(Succeed())
	if err := tc.Client().Create(tc.Context(), deniedSA); err != nil {
		t.Fatalf("failed to create denied gateway probe ServiceAccount: %v", err)
	}
	t.Cleanup(func() {
		if err := tc.Client().Delete(tc.Context(), deniedSA); err != nil && !k8serr.IsNotFound(err) {
			t.Logf("failed to clean up denied gateway probe ServiceAccount: %v", err)
		}
	})

	viewToken := serviceAccountToken(t, tc, viewProbeSA)
	deniedToken := serviceAccountToken(t, tc, deniedName)

	// Stream LogQL must use query_range; instant /query returns 400 for selectors.
	gatewayBase := fmt.Sprintf(
		"https://%s.%s.svc.cluster.local:8080/api/logs/v1/application/loki/api/v1/query_range",
		gatewayService, tc.MonitoringNamespace,
	)
	logQL := fmt.Sprintf(`{kubernetes_namespace_name=%q}`, tc.MonitoringNamespace)
	// Namespace the probe SA has no RoleBinding in — Stage-2 must still deny.
	foreignLogQL := `{kubernetes_namespace_name="default"}`
	probeScript := fmt.Sprintf(`set -eu
fail() { printf 'FAILED: %%s\n' "$1" > /dev/termination-log; exit 1; }
ca=/var/run/secrets/service-ca/service-ca.crt
base=%q
query=%q
foreign=%q
start=$(( $(date +%%s) - 3600 ))000000000
end=$(date +%%s)000000000
curl_q() {
  tok="$1"; out="$2"; q="$3"
  curl --noproxy '*' -sS --max-time 30 --cacert "$ca" \
    -H "Authorization: Bearer ${tok}" -G \
    --data-urlencode "query=${q}" \
    --data-urlencode "start=${start}" \
    --data-urlencode "end=${end}" \
    --data-urlencode "limit=1" \
    -o "$out" -w '%%{http_code}' "$base"
}

status=$(curl_q "$VIEW_TOKEN" /tmp/view.body "$query") || fail "view gateway request transport"
[ "$status" = "200" ] || fail "view token got HTTP $status want 200 (Stage-1 must pass for project view)"

status=$(curl_q "$DENIED_TOKEN" /tmp/denied.body "$query") || fail "denied gateway request transport"
[ "$status" = "403" ] || fail "denied token got HTTP $status want 403 (Stage-1 must reject without project role)"

status=$(curl_q "$VIEW_TOKEN" /tmp/foreign.body "$foreign") || fail "foreign-ns gateway request transport"
[ "$status" = "403" ] || fail "view token foreign ns got HTTP $status want 403 (Stage-2 must deny other namespaces)"

printf 'OK: gateway Stage-1 view=200 denied=403 Stage-2 foreign=403\n' > /dev/termination-log
exit 0
`, gatewayBase, logQL, foreignLogQL)

	probeName := "application-logs-gateway-probe"
	tc.DeleteResource(
		WithMinimalObject(gvk.Pod, types.NamespacedName{Name: probeName, Namespace: tc.MonitoringNamespace}),
		WithIgnoreNotFound(true),
		WithWaitForDeletion(true),
	)
	probe := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      probeName,
			Namespace: tc.MonitoringNamespace,
			// Match data-science-lokistack-gateway-ingress Korrel8r peer so the
			// NetworkPolicy admits this probe without widening production RBAC.
			Labels: map[string]string{
				"app.kubernetes.io/name":      "korrel8r",
				"app.kubernetes.io/component": "correlation",
				"app.kubernetes.io/part-of":   "monitoring",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			AutomountServiceAccountToken: new(false),
			SecurityContext:              fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name:    "gateway-stage1-probe",
				Image:   fakeGCSClientImage,
				Command: []string{"/bin/sh", "-ec"},
				Args:    []string{probeScript},
				Env: []corev1.EnvVar{
					{Name: "VIEW_TOKEN", Value: viewToken},
					{Name: "DENIED_TOKEN", Value: deniedToken},
				},
				VolumeMounts: []corev1.VolumeMount{{
					Name: "service-ca", MountPath: "/var/run/secrets/service-ca", ReadOnly: true,
				}},
				SecurityContext:          fixtureContainerSecurityContext(),
				TerminationMessagePolicy: corev1.TerminationMessageReadFile,
			}},
			Volumes: []corev1.Volume{{
				Name: "service-ca",
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "openshift-service-ca.crt"},
					Items:                []corev1.KeyToPath{{Key: "service-ca.crt", Path: "service-ca.crt"}},
				}},
			}},
		},
	}
	tc.createFixtureResource(t, gvk.Pod, probe)
	t.Cleanup(func() {
		tc.DeleteResource(
			WithMinimalObject(gvk.Pod, types.NamespacedName{Name: probeName, Namespace: tc.MonitoringNamespace}),
			WithIgnoreNotFound(true),
			WithWaitForDeletion(true),
		)
	})

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Pod, types.NamespacedName{Name: probeName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.status.phase == "Succeeded" or .status.phase == "Failed"`)),
		WithEventuallyTimeout(3*time.Minute),
		WithCustomErrorMsg("Loki gateway Stage-1 probe should reach a terminal phase"),
	)

	completed := &corev1.Pod{}
	if err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: probeName, Namespace: tc.MonitoringNamespace}, completed); err != nil {
		t.Fatalf("failed to read Loki gateway Stage-1 probe result: %v", err)
	}
	message := ""
	if len(completed.Status.ContainerStatuses) > 0 && completed.Status.ContainerStatuses[0].State.Terminated != nil {
		message = completed.Status.ContainerStatuses[0].State.Terminated.Message
	}
	t.Logf("Loki gateway Stage-1 evidence: %q", message)
	if completed.Status.Phase != corev1.PodSucceeded || !strings.HasPrefix(message, "OK:") {
		t.Fatalf("Loki gateway Stage-1 probe failed: phase=%s message=%q", completed.Status.Phase, message)
	}
}

func subjectCanGetApplicationLogs(tc *MonitoringTestCtx, user, namespace string) (bool, error) {
	review := &authorizationv1.SubjectAccessReview{
		Spec: authorizationv1.SubjectAccessReviewSpec{
			User: user,
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Namespace: namespace,
				Group:     "loki.grafana.com",
				Resource:  "application",
				Name:      "logs",
				Verb:      "get",
			},
		},
	}
	if err := tc.Client().Create(tc.Context(), review); err != nil {
		return false, fmt.Errorf("SubjectAccessReview for %s namespace=%q: %w", user, namespace, err)
	}
	return review.Status.Allowed, nil
}

// ValidateLogsLokiStackConfiguration tests the log forwarding LokiStack OTLP
// stream labels configuration.
func (tc *MonitoringTestCtx) ValidateLogsLokiStackConfiguration(t *testing.T) {
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
		WithCustomErrorMsg("Log forwarding LokiStack should have correct OTLP stream labels"),
	)
}

// ValidateLogsLokiStackLifecycle mirrors ValidateUsageLogsLifecycle for
// spec.logs: tear down on remove (incl. data-science-application-logs-view),
// then re-enable and assert the stack + Stage-1 ClusterRole come back. Usage
// logs must survive the whole toggle.
func (tc *MonitoringTestCtx) ValidateLogsLokiStackLifecycle(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	const applicationLogsViewRole = "data-science-application-logs-view"

	// Idempotent: the ClusterLogForwarder contract test removes spec.logs on its
	// way out, but it is skipped when the Cluster Logging operator is absent.
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		jq.Transform(`del(.spec.logs)`),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      LokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithEventuallyTimeout(lokiStackReadyTimeout),
	)

	// The stack's policies and application-logs RBAC are rendered by the same
	// action, so they go with it. The usage logs policies are driven by
	// spec.usageLogs and must not.
	for _, name := range []string{LokiStackNetworkPolicyName, LokiStackGatewayNetworkPolicyName} {
		tc.EnsureResourceGone(
			WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
				Name:      name,
				Namespace: tc.MonitoringNamespace,
			}),
		)
	}
	tc.EnsureResourceGone(
		WithMinimalObject(gvk.ClusterRole, types.NamespacedName{Name: applicationLogsViewRole}),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      UsageLogsLokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCustomErrorMsg("Usage logs LokiStack must survive the removal of spec.logs"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
			Name:      UsageLogsLokiStackNetworkPolicyName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCustomErrorMsg("Usage logs LokiStack NetworkPolicy must survive the removal of spec.logs"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionLokiStackAvailable, metav1.ConditionFalse),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsLokiStackAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("Removing spec.logs should only flip LokiStackAvailable to False"),
	)

	// Re-enable log forwarding (same secret/bucket as setupLogsLokiStack).
	tc.setupLogsWithStorage(t, "s3", logsLokiSecretName)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      LokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithEventuallyTimeout(lokiStackReadyTimeout),
		WithCustomErrorMsg("Log forwarding LokiStack should be recreated when spec.logs is re-enabled"),
	)
	for _, name := range []string{LokiStackNetworkPolicyName, LokiStackGatewayNetworkPolicyName} {
		tc.EnsureResourceExists(
			WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
				Name:      name,
				Namespace: tc.MonitoringNamespace,
			}),
			WithCustomErrorMsg("NetworkPolicy %s should be recreated when spec.logs is re-enabled", name),
		)
	}
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRole, types.NamespacedName{Name: applicationLogsViewRole}),
		WithCondition(And(
			jq.Match(`.metadata.labels["rbac.authorization.k8s.io/aggregate-to-admin"] == "true"`),
			jq.Match(`.metadata.labels["rbac.authorization.k8s.io/aggregate-to-edit"] == "true"`),
			jq.Match(`.metadata.labels["rbac.authorization.k8s.io/aggregate-to-view"] == "true"`),
			jq.Match(`.metadata.labels["platform.opendatahub.io/component"] == "loki-logs"`),
		)),
		WithCustomErrorMsg("data-science-application-logs-view should be recreated when spec.logs is re-enabled"),
	)
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionLokiStackAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsLokiStackAvailable, metav1.ConditionTrue),
		)),
		WithEventuallyTimeout(lokiStackReadyTimeout),
		WithCustomErrorMsg("Both LokiStacks should be available after re-enabling spec.logs"),
	)
}
