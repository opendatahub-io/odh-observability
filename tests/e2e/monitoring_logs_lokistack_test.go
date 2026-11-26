package e2e_test

import (
	"fmt"
	"strconv"
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

// ValidateApplicationLogsProjectRBAC checks Stage-1 project log query access.
// Cluster-wide Stage-1 is Auth's data-science-admingroupcluster-role; this role
// only adds project Stage-1 via OpenShift admin/edit/view aggregation (no CRB).
func (tc *MonitoringTestCtx) ValidateApplicationLogsProjectRBAC(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	const applicationLogsViewRole = "data-science-application-logs-view"
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

	tc.assertAuthAdminGroupClusterRoleUntouched(t)
	tc.assertNoClusterRoleBindingTo(t, applicationLogsViewRole)

	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	for _, roleName := range []string{"view", "edit", "admin"} {
		saName := dns1123("app-logs-" + roleName + "-" + runID)
		tc.ensureProjectRoleProbe(t, saName, roleName)
		probeUser := fmt.Sprintf("system:serviceaccount:%s:%s", tc.MonitoringNamespace, saName)
		tc.g.Eventually(func(g Gomega) {
			namespaced, err := subjectCanGetApplicationLogs(tc, probeUser, tc.MonitoringNamespace)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(namespaced).To(BeTrue(),
				"project %s binding must authorize namespaced get application.loki.grafana.com/logs", roleName)

			clusterScoped, err := subjectCanGetApplicationLogs(tc, probeUser, "")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(clusterScoped).To(BeFalse(),
				"project %s binding must not authorize cluster-scoped get application.loki.grafana.com/logs", roleName)
		}).Should(Succeed())
	}

	tc.assertApplicationLogsGatewayStage1(t, dns1123("app-logs-view-"+runID), runID)
}

func dns1123(name string) string {
	if len(name) <= 63 {
		return name
	}
	return strings.TrimRight(name[:63], "-.")
}

func ignoreNotFoundOrForbidden(err error) bool {
	return err == nil || k8serr.IsNotFound(err) || k8serr.IsForbidden(err)
}

func (tc *MonitoringTestCtx) ensureProjectRoleProbe(t *testing.T, saName, clusterRoleName string) {
	t.Helper()
	tc.ensureProbeServiceAccount(t, saName)
	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: saName, Namespace: tc.MonitoringNamespace},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: clusterRoleName},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: saName, Namespace: tc.MonitoringNamespace}},
	}
	existing := &rbacv1.RoleBinding{}
	err := tc.Client().Get(tc.Context(), client.ObjectKeyFromObject(rb), existing)
	switch {
	case k8serr.IsNotFound(err):
		if err := tc.Client().Create(tc.Context(), rb); err != nil {
			t.Fatalf("create project role probe RoleBinding %s: %v", saName, err)
		}
	case err != nil:
		t.Fatalf("get project role probe RoleBinding %s: %v", saName, err)
	default:
		if existing.RoleRef.Name != clusterRoleName || existing.RoleRef.Kind != "ClusterRole" {
			if delErr := tc.Client().Delete(tc.Context(), existing); !ignoreNotFoundOrForbidden(delErr) {
				t.Fatalf("replace RoleBinding %s: %v", saName, delErr)
			}
			rb.ResourceVersion = ""
			if err := tc.Client().Create(tc.Context(), rb); err != nil && !k8serr.IsAlreadyExists(err) {
				t.Fatalf("recreate RoleBinding %s: %v", saName, err)
			}
		}
	}
	t.Cleanup(func() {
		_ = tc.Client().Delete(tc.Context(), rb)
	})
}

func (tc *MonitoringTestCtx) ensureProbeServiceAccount(t *testing.T, name string) {
	t.Helper()
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: tc.MonitoringNamespace}}
	err := tc.Client().Get(tc.Context(), client.ObjectKeyFromObject(sa), &corev1.ServiceAccount{})
	if k8serr.IsNotFound(err) {
		if err := tc.Client().Create(tc.Context(), sa); err != nil && !k8serr.IsAlreadyExists(err) {
			t.Fatalf("create probe SA %s: %v", name, err)
		}
	} else if err != nil {
		t.Fatalf("get probe SA %s: %v", name, err)
	}
	t.Cleanup(func() {
		if err := tc.Client().Delete(tc.Context(), sa); !ignoreNotFoundOrForbidden(err) {
			t.Logf("cleanup probe SA %s: %v", name, err)
		}
	})
}

func (tc *MonitoringTestCtx) assertNoClusterRoleBindingTo(t *testing.T, clusterRoleName string) {
	t.Helper()
	var bindings rbacv1.ClusterRoleBindingList
	if err := tc.Client().List(tc.Context(), &bindings); err != nil {
		t.Fatalf("list ClusterRoleBindings: %v", err)
	}
	for i := range bindings.Items {
		ref := bindings.Items[i].RoleRef
		if ref.Kind == "ClusterRole" && ref.Name == clusterRoleName {
			t.Fatalf("must not bind ClusterRole %q; found ClusterRoleBinding %q", clusterRoleName, bindings.Items[i].Name)
		}
	}
}

// assertAuthAdminGroupClusterRoleUntouched fails only if Monitoring owns or
// rewrites Auth's cluster-wide admin-group role. Auth absence is not a
// Monitoring Stage-1 failure.
func (tc *MonitoringTestCtx) assertAuthAdminGroupClusterRoleUntouched(t *testing.T) {
	t.Helper()
	const adminGroupClusterRole = "data-science-admingroupcluster-role"
	role := &rbacv1.ClusterRole{}
	err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: adminGroupClusterRole}, role)
	switch {
	case k8serr.IsNotFound(err):
		t.Logf("%s absent; skipping Auth cluster-wide ownership check", adminGroupClusterRole)
		return
	case err != nil:
		t.Fatalf("get %s: %v", adminGroupClusterRole, err)
	}
	for _, owner := range role.OwnerReferences {
		if owner.Kind == "Monitoring" {
			t.Fatalf("%s must not be owned by Monitoring (owner %s/%s)", adminGroupClusterRole, owner.Kind, owner.Name)
		}
	}
	if partOf := role.Labels["platform.opendatahub.io/part-of"]; partOf != "" && partOf != "auth" {
		t.Fatalf("%s part-of label must stay auth when set, got %q", adminGroupClusterRole, partOf)
	}
}

// assertApplicationLogsGatewayStage1 proves Stage-1 allow/deny and Stage-2
// foreign-NS isolation on the logs gateway: seed own+foreign markers (job-
// scoped LogQL), view sees own marker, foreign does not leak, unbound SA 403.
func (tc *MonitoringTestCtx) assertApplicationLogsGatewayStage1(t *testing.T, viewProbeSA, runID string) {
	t.Helper()
	tc = tc.WithT(t)

	gatewayService := LokiStackName + "-gateway-http"
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Service, types.NamespacedName{Name: gatewayService, Namespace: tc.MonitoringNamespace}),
		WithCustomErrorMsg("log-forwarding LokiStack gateway Service must exist before Stage-1 HTTP probe"),
	)

	deniedName := dns1123("app-logs-denied-" + runID)
	writerName := dns1123("app-logs-writer-" + runID)
	tokenSecretName := dns1123("app-logs-tokens-" + runID)
	probeName := dns1123("app-logs-gw-probe-" + runID)
	foreignNS := dns1123("app-logs-fx-" + runID)
	const probeJob = "application-logs-rbac-probe"

	tc.ensureProbeServiceAccount(t, deniedName)
	tc.ensureProbeServiceAccount(t, writerName)
	tc.ensureApplicationLogsWriterRBAC(t, writerName)
	tc.ensureProbeNamespace(t, foreignNS)
	tc.createApplicationLogsProbeTokenSecret(t, tokenSecretName, map[string]string{
		"view-token":   serviceAccountToken(t, tc, viewProbeSA),
		"denied-token": serviceAccountToken(t, tc, deniedName),
		"writer-token": serviceAccountToken(t, tc, writerName),
	})

	marker := fmt.Sprintf("rhoaieng-99813-marker-%s", runID)
	ownLogQL := fmt.Sprintf(`{kubernetes_namespace_name=%q,job=%q}`, tc.MonitoringNamespace, probeJob)
	foreignLogQL := fmt.Sprintf(`{kubernetes_namespace_name=%q,job=%q}`, foreignNS, probeJob)
	gatewayBase := fmt.Sprintf(
		"https://%s.%s.svc.cluster.local:8080/api/logs/v1/application/loki/api/v1",
		gatewayService, tc.MonitoringNamespace,
	)
	script := fmt.Sprintf(`set -eu
fail() { printf 'FAILED: %%s\n' "$1" > /dev/termination-log; exit 1; }
ca=/var/run/secrets/service-ca/service-ca.crt
tokdir=/var/run/secrets/gateway-probe-tokens
VIEW_TOKEN=$(cat "$tokdir/view-token")
DENIED_TOKEN=$(cat "$tokdir/denied-token")
WRITER_TOKEN=$(cat "$tokdir/writer-token")
base=%q
own_q=%q
foreign_q=%q
marker=%q
own_ns=%q
foreign_ns=%q
job=%q
start=$(( $(date +%%s) - 3600 ))000000000
end=$(( $(date +%%s) + 120 ))000000000
ts=$(( $(date +%%s) * 1000000000 ))

push_line() {
  ns="$1"
  body=$(printf '{"streams":[{"stream":{"kubernetes_namespace_name":"%%s","job":"%%s"},"values":[["%%s","%%s"]]}]}' "$ns" "$job" "$ts" "$marker")
  code=$(curl --noproxy '*' -sS --max-time 60 --cacert "$ca" \
    -H "Authorization: Bearer ${WRITER_TOKEN}" -H "Content-Type: application/json" \
    -o /tmp/push.body -w '%%{http_code}' \
    -X POST --data-binary "$body" "$base/push") || fail "push transport ns=$ns"
  [ "$code" = "204" ] || [ "$code" = "200" ] || fail "push ns=$ns HTTP $code"
}
push_line "$own_ns"
push_line "$foreign_ns"

curl_q() {
  tok="$1"; out="$2"; q="$3"
  curl --noproxy '*' -sS --max-time 60 --cacert "$ca" \
    -H "Authorization: Bearer ${tok}" -G \
    --data-urlencode "query=${q}" \
    --data-urlencode "start=${start}" \
    --data-urlencode "end=${end}" \
    --data-urlencode "limit=100" \
    -o "$out" -w '%%{http_code}' "$base/query_range"
}

found=0
for _ in $(seq 1 30); do
  status=$(curl_q "$VIEW_TOKEN" /tmp/view.body "$own_q") || fail "view query transport"
  [ "$status" = "200" ] || fail "view token got HTTP $status want 200"
  if grep -q "$marker" /tmp/view.body; then found=1; break; fi
  sleep 2
done
[ "$found" = "1" ] || fail "view query 200 but own marker missing"

status=$(curl_q "$DENIED_TOKEN" /tmp/denied.body "$own_q") || fail "denied query transport"
[ "$status" = "403" ] || fail "denied token got HTTP $status want 403"

status=$(curl_q "$VIEW_TOKEN" /tmp/foreign.body "$foreign_q") || fail "foreign query transport"
if [ "$status" = "403" ]; then
  :
elif [ "$status" = "200" ]; then
  grep -q "$marker" /tmp/foreign.body && fail "foreign query leaked seeded marker"
else
  fail "foreign query got HTTP $status want 403 or 200-without-marker"
fi

printf 'OK: own marker; denied=403; foreign isolated\n' > /dev/termination-log
exit 0
`, gatewayBase, ownLogQL, foreignLogQL, marker, tc.MonitoringNamespace, foreignNS, probeJob)

	tc.runSecretMountedGatewayProbe(t, probeName, tokenSecretName, script, map[string]string{
		"app.kubernetes.io/name":      "korrel8r",
		"app.kubernetes.io/component": "correlation",
		"app.kubernetes.io/part-of":   "monitoring",
	}, "gateway-stage1-probe", 3*time.Minute, "Loki gateway Stage-1")
}

func (tc *MonitoringTestCtx) ensureProbeNamespace(t *testing.T, name string) {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	err := tc.Client().Get(tc.Context(), client.ObjectKeyFromObject(ns), &corev1.Namespace{})
	if k8serr.IsNotFound(err) {
		if err := tc.Client().Create(tc.Context(), ns); err != nil && !k8serr.IsAlreadyExists(err) {
			t.Fatalf("create probe namespace %s: %v", name, err)
		}
	} else if err != nil {
		t.Fatalf("get probe namespace %s: %v", name, err)
	}
	t.Cleanup(func() {
		_ = tc.Client().Delete(tc.Context(), ns)
	})
}

func (tc *MonitoringTestCtx) ensureApplicationLogsWriterRBAC(t *testing.T, writerName string) {
	t.Helper()
	writerRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: writerName},
		Rules: []rbacv1.PolicyRule{{
			APIGroups:     []string{"loki.grafana.com"},
			Resources:     []string{"application"},
			ResourceNames: []string{"logs"},
			Verbs:         []string{"create"},
		}},
	}
	writerBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: writerName},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: writerName},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: writerName, Namespace: tc.MonitoringNamespace}},
	}
	if err := tc.Client().Create(tc.Context(), writerRole); err != nil && !k8serr.IsAlreadyExists(err) {
		t.Fatalf("create writer ClusterRole: %v", err)
	}
	if err := tc.Client().Create(tc.Context(), writerBinding); err != nil && !k8serr.IsAlreadyExists(err) {
		t.Fatalf("create writer ClusterRoleBinding: %v", err)
	}
	t.Cleanup(func() {
		_ = tc.Client().Delete(tc.Context(), writerBinding)
		_ = tc.Client().Delete(tc.Context(), writerRole)
	})
}

func (tc *MonitoringTestCtx) createApplicationLogsProbeTokenSecret(t *testing.T, name string, data map[string]string) {
	t.Helper()
	tokenSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: tc.MonitoringNamespace},
		Type:       corev1.SecretTypeOpaque,
		StringData: data,
	}
	existing := &corev1.Secret{}
	err := tc.Client().Get(tc.Context(), client.ObjectKeyFromObject(tokenSecret), existing)
	switch {
	case err == nil:
		existing.StringData = data
		existing.Data = nil
		if err := tc.Client().Update(tc.Context(), existing); err != nil {
			t.Fatalf("update probe token Secret %s: %v", name, err)
		}
	case k8serr.IsNotFound(err):
		tc.createFixtureResource(t, gvk.Secret, tokenSecret)
	default:
		t.Fatalf("get probe token Secret %s: %v", name, err)
	}
	t.Cleanup(func() {
		tc.DeleteResource(
			WithMinimalObject(gvk.Secret, types.NamespacedName{Name: name, Namespace: tc.MonitoringNamespace}),
			WithIgnoreNotFound(true),
			WithWaitForDeletion(true),
		)
	})
}

func (tc *MonitoringTestCtx) runSecretMountedGatewayProbe(
	t *testing.T,
	probeName, tokenSecretName, script string,
	labels map[string]string,
	containerName string,
	timeout time.Duration,
	logLabel string,
) {
	t.Helper()
	tc.DeleteResource(
		WithMinimalObject(gvk.Pod, types.NamespacedName{Name: probeName, Namespace: tc.MonitoringNamespace}),
		WithIgnoreNotFound(true),
		WithWaitForDeletion(true),
	)
	probe := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      probeName,
			Namespace: tc.MonitoringNamespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			AutomountServiceAccountToken: new(false),
			SecurityContext:              fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name:    containerName,
				Image:   fakeGCSClientImage,
				Command: []string{"/bin/sh", "-ec"},
				Args:    []string{script},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "service-ca", MountPath: "/var/run/secrets/service-ca", ReadOnly: true},
					{Name: "probe-tokens", MountPath: "/var/run/secrets/gateway-probe-tokens", ReadOnly: true},
				},
				SecurityContext:          fixtureContainerSecurityContext(),
				TerminationMessagePolicy: corev1.TerminationMessageReadFile,
			}},
			Volumes: []corev1.Volume{
				{
					Name: "service-ca",
					VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "openshift-service-ca.crt"},
						Items:                []corev1.KeyToPath{{Key: "service-ca.crt", Path: "service-ca.crt"}},
					}},
				},
				{
					Name: "probe-tokens",
					VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
						SecretName: tokenSecretName,
					}},
				},
			},
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
		WithEventuallyTimeout(timeout),
		WithCustomErrorMsg("%s probe should reach a terminal phase", logLabel),
	)
	completed := &corev1.Pod{}
	if err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: probeName, Namespace: tc.MonitoringNamespace}, completed); err != nil {
		t.Fatalf("failed to read %s probe result: %v", logLabel, err)
	}
	message := ""
	if len(completed.Status.ContainerStatuses) > 0 && completed.Status.ContainerStatuses[0].State.Terminated != nil {
		message = completed.Status.ContainerStatuses[0].State.Terminated.Message
	}
	t.Logf("%s evidence: %q", logLabel, message)
	if completed.Status.Phase != corev1.PodSucceeded || !strings.HasPrefix(message, "OK:") {
		t.Fatalf("%s probe failed: phase=%s message=%q", logLabel, completed.Status.Phase, message)
	}
}

func subjectCanGetApplicationLogs(tc *MonitoringTestCtx, user, namespace string) (bool, error) {
	review := &authorizationv1.SubjectAccessReview{
		Spec: authorizationv1.SubjectAccessReviewSpec{
			User:   user,
			Groups: subjectGroupsForUser(user),
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

// subjectGroupsForUser mirrors the groups kube-apiserver attaches to a real
// ServiceAccount token so SAR results match gateway TokenReviews.
func subjectGroupsForUser(user string) []string {
	const saPrefix = "system:serviceaccount:"
	if !strings.HasPrefix(user, saPrefix) {
		return []string{"system:authenticated"}
	}
	rest := strings.TrimPrefix(user, saPrefix)
	saNS, _, ok := strings.Cut(rest, ":")
	if !ok || saNS == "" {
		return []string{"system:authenticated"}
	}
	return []string{
		"system:authenticated",
		"system:serviceaccounts",
		"system:serviceaccounts:" + saNS,
	}
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

	// Reverse coupling (SAR only): project view loses Stage-1 when the role is gone.
	denySA := dns1123("app-logs-deny-after-" + strconv.FormatInt(time.Now().UnixNano(), 10))
	tc.ensureProjectRoleProbe(t, denySA, "view")
	denyUser := fmt.Sprintf("system:serviceaccount:%s:%s", tc.MonitoringNamespace, denySA)
	tc.g.Eventually(func(g Gomega) {
		allowed, err := subjectCanGetApplicationLogs(tc, denyUser, tc.MonitoringNamespace)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(allowed).To(BeFalse(),
			"after spec.logs removal, project view must lose get application.loki.grafana.com/logs")
	}).Should(Succeed())

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
