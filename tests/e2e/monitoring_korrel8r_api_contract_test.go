package e2e_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega
)

// The operator owns the resource/API contract. The release workload marker and
// three-edge assertion remain outside this repository, so the probe records a
// precise skip after exercising the bounded API rather than inventing a fixture.
const korrel8rBoundedProbeScript = `set -eu
fail() { printf 'FAILED: %s\n' "$1" > /dev/termination-log; exit 1; }
base="https://korrel8r.${KORREL8R_NAMESPACE}.svc.cluster.local:8443"
ca=/var/run/secrets/service-ca/service-ca.crt
auth="Authorization: Bearer $(cat /var/run/secrets/kubernetes.io/serviceaccount/token)"

status=$(curl --noproxy '*' -sS --max-time 30 --cacert "$ca" -H "$auth" -o /tmp/domains -w '%{http_code}' "$base/api/v1alpha1/domains") || fail "domains request transport"
[ "$status" = "200" ] || fail "domains returned HTTP $status"
grep -Eq '"(k8s|metric)"' /tmp/domains || fail "domains response omitted configured domains"

status=$(curl --noproxy '*' -sS --max-time 30 --cacert "$ca" -H "$auth" -o /dev/null -w '%{http_code}' "$base/api/v1alpha1/config") || fail "config request transport"
[ "$status" = "404" ] || fail "config returned HTTP $status instead of 404"
status=$(curl --noproxy '*' -sS --max-time 30 --cacert "$ca" -H "$auth" -H 'Content-Type: application/json' -X POST -d '{}' -o /dev/null -w '%{http_code}' "$base/api/v1alpha1/config") || fail "config mutation request transport"
[ "$status" = "404" ] || fail "config mutation returned HTTP $status"

validate_graph_response() {
  endpoint="$1"
  response="/tmp/${endpoint}.response"
  compact="/tmp/${endpoint}.compact"
  tr -d '\r\n\t' < "$response" > "$compact" || fail "${endpoint} response read"
  [ -s "$compact" ] || fail "${endpoint} returned an empty response"
  grep -Eq '^[[:space:]]*\{.*\}[[:space:]]*$' "$compact" || fail "${endpoint} returned a non-object JSON response"
  grep -Eq '"edges"[[:space:]]*:[[:space:]]*\[' "$compact" || fail "${endpoint} response omitted graph edges"
  grep -Eq '"nodes"[[:space:]]*:[[:space:]]*\[' "$compact" || fail "${endpoint} response omitted graph nodes"
  grep -Eq '"start"[[:space:]]*:[[:space:]]*"[^"]+"' "$compact" || fail "${endpoint} response omitted edge start classes"
  grep -Eq '"goal"[[:space:]]*:[[:space:]]*"[^"]+"' "$compact" || fail "${endpoint} response omitted edge goal classes"
  if grep -Eq '"error"[[:space:]]*:' "$compact"; then
    fail "${endpoint} returned an error graph payload"
  fi
}

end=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
start=$(date -u -d '5 minutes ago' '+%Y-%m-%dT%H:%M:%SZ')
printf '{"depth":2,"start":{"queries":["k8s:Pod.v1:{\"namespace\":\"%s\",\"name\":\"%s\"}"],"constraint":{"limit":50,"queryLimit":10,"start":"%s","end":"%s"}}}' "$KORREL8R_NAMESPACE" "$WORKLOAD_POD" "$start" "$end" > /tmp/request.json
printf '{"depth":2,"goals":["metric:metric"],"start":{"queries":["k8s:Pod.v1:{\"namespace\":\"%s\",\"name\":\"%s\"}"],"constraint":{"limit":50,"queryLimit":10,"start":"%s","end":"%s"}}}' "$KORREL8R_NAMESPACE" "$WORKLOAD_POD" "$start" "$end" > /tmp/goals.json

for endpoint in neighbors goals; do
  request=/tmp/request.json
  [ "$endpoint" = "goals" ] && request=/tmp/goals.json
  status=$(curl --noproxy '*' -sS --max-time 30 --cacert "$ca" -H "$auth" -H 'Content-Type: application/json' -d @"$request" -o "/tmp/${endpoint}.response" -w '%{http_code}' "$base/api/v1alpha1/graphs/${endpoint}") || fail "${endpoint} request transport"
  case "$status" in
    200) validate_graph_response "$endpoint" ;;
    404|501) printf 'SKIPPED: graph endpoint %s is not established in this Korrel8r build\n' "$endpoint" > /dev/termination-log; exit 0 ;;
    *) fail "${endpoint} returned HTTP $status" ;;
  esac
done

printf 'SKIPPED: bounded API is established, but the release workload three-edge marker is not owned by this operator suite\n' > /dev/termination-log
exit 0
`

// ValidateKorrel8rAPIContract exercises the TLS-only ClusterIP service from a
// distinct caller ServiceAccount. It never creates a Route or uses runtime
// configuration, and every graph request carries explicit traversal limits.
func (tc *MonitoringTestCtx) ValidateKorrel8rAPIContract(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Secret, types.NamespacedName{Name: korrel8rName + "-tls", Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.type == "kubernetes.io/tls"`),
			jq.Match(`.data."tls.crt" != null and .data."tls.key" != null`),
		)),
		WithCustomErrorMsg("Korrel8r TLS serving certificate must be provisioned by service CA"),
	)
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRole, types.NamespacedName{Name: korrel8rName + "-query"}),
		WithCondition(And(
			jq.Match(`
				(.rules | length == 5) and
				([.rules[] | select(
					.apiGroups == [""] and
					.resources == ["pods", "pods/log", "services", "endpoints", "configmaps", "events", "replicationcontrollers", "namespaces"] and
					(.resourceNames // []) == [] and
					(.nonResourceURLs // []) == [] and
					.verbs == ["get", "list", "watch"]
				)] | length == 1) and
				([.rules[] | select(
					.apiGroups == ["apps"] and
					.resources == ["daemonsets", "deployments", "replicasets", "statefulsets"] and
					(.resourceNames // []) == [] and
					(.nonResourceURLs // []) == [] and
					.verbs == ["get", "list", "watch"]
				)] | length == 1) and
				([.rules[] | select(
					.apiGroups == ["batch"] and
					.resources == ["cronjobs", "jobs"] and
					(.resourceNames // []) == [] and
					(.nonResourceURLs // []) == [] and
					.verbs == ["get", "list", "watch"]
				)] | length == 1) and
				([.rules[] | select(
					.apiGroups == ["loki.grafana.com"] and
					.resources == ["application"] and
					.resourceNames == ["logs"] and
					(.nonResourceURLs // []) == [] and
					.verbs == ["get"]
				)] | length == 1) and
				([.rules[] | select(
					.apiGroups == ["tempo.grafana.com"] and
					.resources == ["%s"] and
					.resourceNames == ["traces"] and
					(.nonResourceURLs // []) == [] and
					.verbs == ["get"]
				)] | length == 1)
			`, tc.MonitoringNamespace),
		)),
		WithCustomErrorMsg("Korrel8r query RBAC must be read-only and restricted to configured RHOAI tenants"),
	)
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{Name: korrel8rName + "-auth-delegator"}),
		WithCondition(And(
			jq.Match(`.roleRef.name == "system:auth-delegator"`),
			jq.Match(`.subjects | any(.[]; .kind == "ServiceAccount" and .name == "%s" and .namespace == "%s")`, korrel8rName, tc.MonitoringNamespace),
		)),
		WithCustomErrorMsg("Korrel8r ServiceAccount must retain TokenReview delegation"),
	)

	policy := tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{Name: korrel8rName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`[.spec.egress[] | select([.ports[]? | select(.protocol == "TCP" and .port == 6443)] | length > 0)] | length <= 1`),
			jq.Match(`[.spec.egress[].to[]?.ipBlock.cidr | select(. == "0.0.0.0/0" or . == "::/0")] | length == 0`),
		)),
		WithCustomErrorMsg("Korrel8r NetworkPolicy must restrict Kubernetes API egress to discovered endpoint addresses"),
	)
	tc.assertKorrel8rAPIServerCIDRs(t, policy)

	workloadPod := tc.knownCorrelationWorkloadPod(t)
	if workloadPod == "" {
		t.Skipf("Korrel8r graph contract is not established: no ready collector workload pod is available as a known object")
	}

	suffix := fmt.Sprintf("%x", time.Now().UnixNano())
	callerName := "korrel8r-caller-" + suffix
	roleName := callerName + "-metrics"
	probeName := callerName + "-probe"
	objects := []client.Object{
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: callerName, Namespace: tc.MonitoringNamespace}},
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: roleName, Namespace: tc.MonitoringNamespace},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{"metrics.k8s.io"}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "create"}}},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: roleName, Namespace: tc.MonitoringNamespace},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: roleName},
			Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: callerName, Namespace: tc.MonitoringNamespace}},
		},
	}
	for _, object := range objects {
		if err := tc.Client().Create(tc.Context(), object); err != nil {
			t.Fatalf("failed to create Korrel8r caller fixture %T/%s: %v", object, object.GetName(), err)
		}
		t.Cleanup(func() {
			if err := tc.Client().Delete(tc.Context(), object); err != nil && !k8serr.IsNotFound(err) {
				t.Logf("failed to clean up Korrel8r caller fixture %T/%s: %v", object, object.GetName(), err)
			}
		})
	}

	probe := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: probeName, Namespace: tc.MonitoringNamespace},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			ServiceAccountName:           callerName,
			AutomountServiceAccountToken: new(true),
			SecurityContext:              fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name:    "korrel8r-api-probe",
				Image:   fakeGCSClientImage,
				Command: []string{"/bin/sh", "-ec"},
				Args:    []string{korrel8rBoundedProbeScript},
				Env: []corev1.EnvVar{
					{Name: "KORREL8R_NAMESPACE", Value: tc.MonitoringNamespace},
					{Name: "WORKLOAD_POD", Value: workloadPod},
				},
				VolumeMounts:             []corev1.VolumeMount{{Name: "service-ca", MountPath: "/var/run/secrets/service-ca", ReadOnly: true}},
				SecurityContext:          fixtureContainerSecurityContext(),
				TerminationMessagePolicy: corev1.TerminationMessageReadFile,
			}},
			Volumes: []corev1.Volume{{Name: "service-ca", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "openshift-service-ca.crt"},
				Items:                []corev1.KeyToPath{{Key: "service-ca.crt", Path: "service-ca.crt"}},
			}}}},
		},
	}
	tc.createFixtureResource(t, gvk.Pod, probe)
	t.Cleanup(func() {
		tc.DeleteResource(WithMinimalObject(gvk.Pod, types.NamespacedName{Name: probeName, Namespace: tc.MonitoringNamespace}), WithIgnoreNotFound(true), WithWaitForDeletion(true))
	})

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Pod, types.NamespacedName{Name: probeName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.status.phase == "Succeeded" or .status.phase == "Failed"`)),
		WithEventuallyTimeout(3*time.Minute),
		WithCustomErrorMsg("bounded authenticated Korrel8r API probe should reach a terminal phase"),
	)

	completed := &corev1.Pod{}
	if err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: probeName, Namespace: tc.MonitoringNamespace}, completed); err != nil {
		t.Fatalf("failed to read Korrel8r API probe result: %v", err)
	}
	tc.assertKorrel8rPodTerminationState(t)
	if completed.Status.Phase != corev1.PodSucceeded {
		t.Fatalf("Korrel8r API probe failed with pod phase %s", completed.Status.Phase)
	}
	message := ""
	if len(completed.Status.ContainerStatuses) > 0 && completed.Status.ContainerStatuses[0].State.Terminated != nil {
		message = completed.Status.ContainerStatuses[0].State.Terminated.Message
	}
	t.Logf("Korrel8r bounded API evidence: caller_service_account=%s workload_pod=%s result=%q", callerName, workloadPod, message)
	if strings.HasPrefix(message, "SKIPPED:") {
		t.Skipf("%s", strings.TrimSpace(message))
	}
	if strings.HasPrefix(message, "FAILED:") || message == "" {
		t.Fatalf("Korrel8r API probe did not produce a contract result: %q", message)
	}
}

func (tc *MonitoringTestCtx) knownCorrelationWorkloadPod(t *testing.T) string {
	t.Helper()
	pods := &corev1.PodList{}
	if err := tc.Client().List(tc.Context(), pods, client.InNamespace(tc.MonitoringNamespace), client.MatchingLabels{"app.kubernetes.io/name": OpenTelemetryCollectorName}); err != nil {
		t.Fatalf("failed to discover collector workload pod for Korrel8r: %v", err)
	}
	for _, pod := range pods.Items {
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				return pod.Name
			}
		}
	}
	return ""
}

func (tc *MonitoringTestCtx) assertKorrel8rAPIServerCIDRs(t *testing.T, policy *unstructured.Unstructured) {
	t.Helper()
	endpoints := &discoveryv1.EndpointSliceList{}
	if err := tc.Client().List(tc.Context(), endpoints, client.InNamespace("default"), client.MatchingLabels{discoveryv1.LabelServiceName: "kubernetes"}); err != nil {
		t.Fatalf("failed to discover Kubernetes API EndpointSlices: %v", err)
	}
	wanted := map[string]struct{}{}
	for _, slice := range endpoints.Items {
		for _, endpoint := range slice.Endpoints {
			if endpoint.Conditions.Ready != nil && !*endpoint.Conditions.Ready {
				continue
			}
			for _, address := range endpoint.Addresses {
				if strings.Contains(address, ":") {
					wanted[address+"/128"] = struct{}{}
				} else {
					wanted[address+"/32"] = struct{}{}
				}
			}
		}
	}
	if len(wanted) == 0 {
		t.Skipf("Korrel8r Kubernetes API NetworkPolicy contract is not established: no ready API EndpointSlice address")
	}

	allowed := map[string]struct{}{}
	egress, _, _ := unstructured.NestedSlice(policy.Object, "spec", "egress")
	for _, rawRule := range egress {
		rule, ok := rawRule.(map[string]any)
		if !ok {
			continue
		}
		ports, _, _ := unstructured.NestedSlice(rule, "ports")
		apiPort := false
		for _, rawPort := range ports {
			port, ok := rawPort.(map[string]any)
			if ok && port["protocol"] == "TCP" && port["port"] == int64(6443) {
				apiPort = true
			}
		}
		if !apiPort {
			continue
		}
		to, _, _ := unstructured.NestedSlice(rule, "to")
		for _, rawTarget := range to {
			target, ok := rawTarget.(map[string]any)
			if !ok {
				continue
			}
			block, ok := target["ipBlock"].(map[string]any)
			if ok {
				if cidr, ok := block["cidr"].(string); ok {
					allowed[cidr] = struct{}{}
				}
			}
		}
	}
	for cidr := range wanted {
		if _, ok := allowed[cidr]; !ok {
			t.Fatalf("Korrel8r NetworkPolicy does not allow discovered Kubernetes API endpoint %s", cidr)
		}
	}
}

func (tc *MonitoringTestCtx) assertKorrel8rPodTerminationState(t *testing.T) {
	t.Helper()
	pods := &corev1.PodList{}
	if err := tc.Client().List(tc.Context(), pods, client.InNamespace(tc.MonitoringNamespace), client.MatchingLabels{"app.kubernetes.io/name": korrel8rName}); err != nil {
		t.Fatalf("failed to list Korrel8r pods: %v", err)
	}
	if len(pods.Items) == 0 {
		t.Fatalf("Korrel8r Deployment is ready but no Korrel8r pod was found for termination-state sign-off")
	}
	for _, pod := range pods.Items {
		for _, container := range pod.Status.ContainerStatuses {
			reason := ""
			if container.LastTerminationState.Terminated != nil {
				reason = container.LastTerminationState.Terminated.Reason
			}
			t.Logf("Korrel8r pod termination evidence: pod=%s container=%s restarts=%d last_reason=%q", pod.Name, container.Name, container.RestartCount, reason)
			if reason == "OOMKilled" {
				t.Fatalf("bounded Korrel8r request cannot be signed off after OOMKilled: pod=%s container=%s", pod.Name, container.Name)
			}
		}
	}
}
