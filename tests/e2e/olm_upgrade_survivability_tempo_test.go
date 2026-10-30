package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	ctrlcfg "sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega
)

// assertTempoTraceHealth validates the live trace path without changing the
// Monitoring spec: Tempo must already be configured, accept a trace through
// the collector, and return that trace through its authenticated gateway.
func (tc *MonitoringTestCtx) assertTempoTraceHealth(
	t *testing.T, report *olmUpgradeSurvivabilityReport, stage string,
) {
	t.Helper()
	tc = tc.WithT(t)
	failureMsg := fmt.Sprintf("Tempo should accept and return traces %s", stage)
	defer func() {
		if t.Failed() && report.FailedAssertion == "" {
			report.FailedAssertion = failureMsg
		}
	}()

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.spec.traces != null`),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionTempoAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionOpenTelemetryCollectorAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("%s; configure traces before running a Tempo upgrade", failureMsg),
	)

	gatewayName := tc.readyTempoGateway(t, failureMsg)
	tc.probeTempoTraceRoundTrip(t, gatewayName, failureMsg)
}

func (tc *MonitoringTestCtx) readyTempoGateway(t *testing.T, failureMsg string) string {
	t.Helper()
	for _, candidate := range []struct {
		gvk  schema.GroupVersionKind
		name string
	}{
		{gvk: gvk.TempoMonolithic, name: TempoMonolithicName},
		{gvk: gvk.TempoStack, name: TempoStackName},
	} {
		_, err := tc.fetchResource(t, candidate.gvk, types.NamespacedName{Name: candidate.name, Namespace: tc.MonitoringNamespace})
		if k8serr.IsNotFound(err) {
			continue
		}
		if err != nil {
			t.Fatalf("%s: read %s %s: %v", failureMsg, candidate.gvk.Kind, candidate.name, err)
		}
		tc.EnsureResourceExists(
			WithMinimalObject(candidate.gvk, types.NamespacedName{Name: candidate.name, Namespace: tc.MonitoringNamespace}),
			WithCondition(jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue)),
			WithCustomErrorMsg("%s: %s %s is not Ready", failureMsg, candidate.gvk.Kind, candidate.name),
		)
		return "tempo-" + candidate.name + "-gateway"
	}
	t.Fatalf("%s: no TempoMonolithic or TempoStack operand exists in %s", failureMsg, tc.MonitoringNamespace)
	return ""
}

func tempoTracePayload(traceID, spanID string, startTime, endTime int64) string {
	return fmt.Sprintf(`{"resourceSpans":[{`+
		`"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"olm-upgrade-survivability"}}]},`+
		`"scopeSpans":[{"spans":[{"traceId":"%s","spanId":"%s","name":"olm-upgrade-survivability",`+
		`"startTimeUnixNano":"%d","endTimeUnixNano":"%d"}]}]}]}`,
		traceID, spanID, startTime, endTime)
}

func (tc *MonitoringTestCtx) probeTempoTraceRoundTrip(t *testing.T, gatewayName, failureMsg string) {
	t.Helper()
	suffix := fmt.Sprintf("%x", time.Now().UnixNano())
	traceID := fmt.Sprintf("%032x", time.Now().UnixNano())
	spanID := fmt.Sprintf("%016x", time.Now().UnixNano())
	probeNamespace := "olm-tempo-probe-" + suffix
	queryName := "olm-tempo-query-" + suffix
	serviceAccountName := queryName
	roleName := queryName

	tc.createFixtureResource(t, gvk.Namespace, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: probeNamespace,
		Labels: map[string]string{
			otlpNamespaceLabel: "true",
		},
	}})
	t.Cleanup(func() {
		tc.DeleteResource(WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: probeNamespace}), WithIgnoreNotFound(true))
	})

	now := time.Now().UnixNano()
	payload := tempoTracePayload(traceID, spanID, now, now+1_000_000)
	collectorEndpoint := fmt.Sprintf("http://data-science-collector-collector.%s.svc.cluster.local:4318/v1/traces", tc.MonitoringNamespace)
	ingestScript := fmt.Sprintf(`set -eu
status=$(curl --noproxy '*' -sS -o /tmp/response -w '%%{http_code}' \
  -H 'Content-Type: application/json' --data-binary %q %q)
cat /tmp/response
test "$status" = 200`, payload, collectorEndpoint)
	ingestPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "ingest", Namespace: probeNamespace, Labels: otlpCallerNetworkPolicyLabels("llminferenceservice-workload")},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: new(false), SecurityContext: fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name: "curl", Image: fakeGCSClientImage, Command: []string{"/bin/sh", "-c"}, Args: []string{ingestScript},
				SecurityContext: fixtureContainerSecurityContext(), TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
			}},
		},
	}
	tc.createFixtureResource(t, gvk.Pod, ingestPod)
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Pod, types.NamespacedName{Name: ingestPod.Name, Namespace: ingestPod.Namespace}),
		WithCondition(jq.Match(`.status.phase == "Succeeded"`)),
		WithEventuallyTimeout(5*time.Minute),
		WithCustomErrorMsg("%s: OTLP trace ingestion probe failed", failureMsg),
	)

	t.Cleanup(func() {
		tc.DeleteResource(WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{Name: roleName}), WithIgnoreNotFound(true))
		tc.DeleteResource(WithMinimalObject(gvk.ClusterRole, types.NamespacedName{Name: roleName}), WithIgnoreNotFound(true))
	})
	tc.createFixtureResource(t, gvk.ServiceAccount, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name: serviceAccountName, Namespace: probeNamespace,
	}})
	queryToken, err := tc.requestTempoProbeToken(probeNamespace, serviceAccountName)
	if err != nil {
		t.Fatalf("%s: request dedicated Tempo query token: %v", failureMsg, err)
	}
	tc.createFixtureResource(t, gvk.ClusterRole, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: roleName},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{"tempo.grafana.com"}, Resources: []string{tc.MonitoringNamespace},
			ResourceNames: []string{"traces"}, Verbs: []string{"get"},
		}},
	})
	tc.createFixtureResource(t, gvk.ClusterRoleBinding, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: roleName},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: roleName},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: serviceAccountName, Namespace: probeNamespace}},
	})
	queryURL := fmt.Sprintf("https://%s.%s.svc.cluster.local:8080/api/traces/v1/%s/tempo/api/traces/%s",
		gatewayName, tc.MonitoringNamespace, tc.MonitoringNamespace, traceID)
	queryScript := fmt.Sprintf(`set -eu
for attempt in $(seq 1 120); do
  [ ! -s /var/run/secrets/query-token/token ] || break
  sleep 1
done
test -s /var/run/secrets/query-token/token
token=$(cat /var/run/secrets/query-token/token)
for attempt in $(seq 1 60); do
  if status=$(curl --noproxy '*' --cacert /var/run/secrets/service-ca/service-ca.crt -sS \
    -o /tmp/trace -w '%%{http_code}' -H "Authorization: Bearer $token" %q); then
    if [ "$status" = 200 ]; then cat /tmp/trace; exit 0; fi
  else
    status=connection-error
  fi
  sleep 5
done
printf 'last HTTP status=%%s\n' "$status"
[ ! -f /tmp/trace ] || cat /tmp/trace
exit 1`, queryURL)
	queryPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: queryName, Namespace: tc.MonitoringNamespace, Labels: map[string]string{
			"instance": "data-science-perses", "app.kubernetes.io/name": "data-science-perses",
		}},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: new(false), SecurityContext: fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name: "curl", Image: fakeGCSClientImage, Command: []string{"/bin/sh", "-c"}, Args: []string{queryScript},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "service-ca", MountPath: "/var/run/secrets/service-ca", ReadOnly: true},
					{Name: "query-token", MountPath: "/var/run/secrets/query-token"},
				},
				SecurityContext: fixtureContainerSecurityContext(), TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
			}},
			Volumes: []corev1.Volume{
				{Name: "service-ca", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "openshift-service-ca.crt"},
					Items:                []corev1.KeyToPath{{Key: "service-ca.crt", Path: "service-ca.crt"}},
				}}},
				{Name: "query-token", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			},
		},
	}
	tc.createFixtureResource(t, gvk.Pod, queryPod)
	t.Cleanup(func() {
		tc.DeleteResource(WithMinimalObject(gvk.Pod, types.NamespacedName{Name: queryName, Namespace: tc.MonitoringNamespace}), WithIgnoreNotFound(true))
	})
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Pod, types.NamespacedName{Name: queryName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.status.phase == "Running"`)),
		WithEventuallyTimeout(2*time.Minute),
		WithCustomErrorMsg("%s: Tempo query probe pod should start", failureMsg),
	)
	var lastTokenStreamErr error
	var lastTokenStreamStderr string
	err = wait.PollUntilContextTimeout(tc.Context(), 2*time.Second, 90*time.Second, true, func(ctx context.Context) (bool, error) {
		lastTokenStreamStderr, lastTokenStreamErr = tc.streamTempoQueryToken(t, ctx, queryName, queryToken)
		return lastTokenStreamErr == nil, nil
	})
	if err != nil {
		t.Fatalf("%s: stream token to Tempo query probe: %v (last exec error=%v, stderr=%s)",
			failureMsg, err, lastTokenStreamErr, lastTokenStreamStderr)
	}
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Pod, types.NamespacedName{Name: queryName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.status.phase == "Succeeded"`)),
		WithEventuallyTimeout(6*time.Minute),
		WithCustomErrorMsg("%s: trace %s was not queryable through Tempo", failureMsg, traceID),
	)
}

func (tc *MonitoringTestCtx) streamTempoQueryToken(
	t *testing.T, parentCtx context.Context, podName, token string,
) (string, error) {
	t.Helper()
	cfg, err := ctrlcfg.GetConfig()
	if err != nil {
		return "", fmt.Errorf("load kubeconfig for pod exec: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return "", fmt.Errorf("create Kubernetes client for pod exec: %w", err)
	}
	request := clientset.CoreV1().RESTClient().Post().
		Resource("pods").Name(podName).Namespace(tc.MonitoringNamespace).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "curl", Command: []string{"/bin/sh", "-c", "umask 077; cat > /var/run/secrets/query-token/token"},
			Stdin: true, Stderr: true,
		}, clientgoscheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(cfg, http.MethodPost, request.URL())
	if err != nil {
		return "", fmt.Errorf("create pod exec stream: %w", err)
	}
	ctx, cancel := context.WithTimeout(parentCtx, 30*time.Second)
	defer cancel()
	var stderr bytes.Buffer
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin: bytes.NewBufferString(token), Stderr: &stderr,
	})
	return stderr.String(), err
}

func (tc *MonitoringTestCtx) requestTempoProbeToken(namespace, serviceAccountName string) (string, error) {
	cfg, err := ctrlcfg.GetConfig()
	if err != nil {
		return "", fmt.Errorf("load kubeconfig for TokenRequest: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return "", fmt.Errorf("create Kubernetes client for TokenRequest: %w", err)
	}
	expirationSeconds := int64(600)
	tokenRequest, err := clientset.CoreV1().ServiceAccounts(namespace).CreateToken(
		tc.Context(), serviceAccountName,
		&authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{
			ExpirationSeconds: &expirationSeconds,
			Audiences:         []string{"https://kubernetes.default.svc"},
		}}, metav1.CreateOptions{},
	)
	if err != nil {
		return "", err
	}
	if tokenRequest.Status.Token == "" {
		return "", fmt.Errorf("TokenRequest for service account %s/%s returned an empty token", namespace, serviceAccountName)
	}
	return tokenRequest.Status.Token, nil
}
