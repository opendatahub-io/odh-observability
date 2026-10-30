package e2e_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcfg "sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega
)

// ValidatePersesDatasourceProxyNamespaceScoping proves Perses →
// data-science-thanos-querier-proxy enforces the DS namespace gate:
//   - caller Authorization is forwarded (no shared SA token)
//   - missing namespace= fails closed
//   - caller with metrics.k8s.io Role only in the monitoring NS cannot
//     query a foreign namespace (SAR isolation on the namespace= param)
//   - repeated namespace=a&namespace=b is allowed only when SAR succeeds for every ns
//     (Thanos proxy has no --regex-match; Project=All list expansion)
//
// This is authz on namespace=, not a PromQL label-rewrite proof.
// Probes run inside the Perses pod via the Kubernetes exec API (no `oc`).
func (tc *MonitoringTestCtx) ValidatePersesDatasourceProxyNamespaceScoping(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.PersesDatasource, types.NamespacedName{Name: PersesDatasourceName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.spec.config.plugin.spec.proxy.spec.url | contains("data-science-thanos-querier-proxy")`),
			jq.Match(`.spec.config.plugin.spec.proxy.spec.url | contains(":8443")`),
			jq.Match(`.spec.config.plugin.spec.queryParams.namespace == "${namespace:queryparam}"`),
		)),
		WithCustomErrorMsg("DS PersesDatasource must point at the namespace-enforcing proxy with namespace="),
	)

	persesPod := resolvePersesPodName(t, tc)
	identitySuffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	authorizedName := "perses-proxy-authorized-" + identitySuffix

	registerCleanup := func(object client.Object) {
		t.Cleanup(func() {
			if err := tc.Client().Delete(tc.Context(), object); err != nil && !k8serr.IsNotFound(err) {
				t.Logf("failed to clean up %T %s: %v", object, object.GetName(), err)
			}
		})
	}

	metricsRole := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      authorizedName + "-metrics",
			Namespace: tc.MonitoringNamespace,
		},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{"metrics.k8s.io"},
			Resources: []string{"pods"},
			Verbs:     []string{"get"},
		}},
	}
	if err := tc.Client().Create(tc.Context(), metricsRole); err != nil {
		t.Fatalf("failed to create Perses proxy probe metrics Role: %v", err)
	}
	registerCleanup(metricsRole)

	authorizedToken := authorizedThanosProbeToken(t, tc, authorizedName, false)
	metricsBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      authorizedName + "-metrics",
			Namespace: tc.MonitoringNamespace,
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     metricsRole.Name,
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      authorizedName,
			Namespace: tc.MonitoringNamespace,
		}},
	}
	if err := tc.Client().Create(tc.Context(), metricsBinding); err != nil {
		t.Fatalf("failed to create Perses proxy probe metrics RoleBinding: %v", err)
	}
	registerCleanup(metricsBinding)

	withNS := probePersesDatasourceProxy(t, tc, persesPod, authorizedToken, tc.MonitoringNamespace)
	t.Logf("perses→proxy authorized+namespace http=%d body=%q", withNS.status, trimProbeBody(withNS.body))
	if withNS.status != http.StatusOK {
		t.Fatalf("Perses datasource proxy with caller token + namespace= must return 200, got %d body=%q", withNS.status, withNS.body)
	}
	assertPersesPostAllowed(t, tc, persesPod, authorizedToken, tc.MonitoringNamespace)

	// Namespace isolation: Role is only in MonitoringNamespace — foreign NS must deny.
	foreignNS := fmt.Sprintf("perses-proxy-ns-deny-%s", identitySuffix)
	foreignNamespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: foreignNS}}
	if err := tc.Client().Create(tc.Context(), foreignNamespace); err != nil {
		t.Fatalf("failed to create foreign namespace for cross-NS deny: %v", err)
	}
	registerCleanup(foreignNamespace)
	crossNS := probePersesDatasourceProxy(t, tc, persesPod, authorizedToken, foreignNS)
	t.Logf("perses→proxy authorized-but-foreign-namespace http=%d body=%q", crossNS.status, trimProbeBody(crossNS.body))
	if crossNS.status == http.StatusOK {
		t.Fatalf("Perses datasource proxy must deny cross-namespace query (authorized only for %s, queried %s), got 200", tc.MonitoringNamespace, foreignNS)
	}
	if crossNS.status != http.StatusForbidden && crossNS.status != http.StatusUnauthorized {
		t.Fatalf("Perses datasource proxy cross-namespace deny must be 403 or 401, got %d", crossNS.status)
	}

	// Multi-ns deny: authorized for monitoring NS only — mixing in foreign must fail.
	multiDeny := probePersesDatasourceProxy(t, tc, persesPod, authorizedToken, tc.MonitoringNamespace, foreignNS)
	t.Logf("perses→proxy multi-ns deny (allowed+foreign) http=%d body=%q", multiDeny.status, trimProbeBody(multiDeny.body))
	if multiDeny.status == http.StatusOK {
		t.Fatalf("Perses datasource proxy must deny namespace=%s&namespace=%s when caller lacks SAR on foreign NS, got 200", tc.MonitoringNamespace, foreignNS)
	}
	if multiDeny.status != http.StatusForbidden && multiDeny.status != http.StatusUnauthorized {
		t.Fatalf("Perses datasource proxy multi-ns deny must be 403 or 401, got %d", multiDeny.status)
	}
	assertPersesPostMultiNamespaceRejected(t, tc, persesPod, authorizedToken, foreignNS)

	// Grant access to a second namespace; repeated authorized values must succeed.
	secondNS := fmt.Sprintf("perses-proxy-ns-allow-%s", identitySuffix)
	secondNamespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: secondNS}}
	if err := tc.Client().Create(tc.Context(), secondNamespace); err != nil {
		t.Fatalf("failed to create second namespace for multi-ns test: %v", err)
	}
	registerCleanup(secondNamespace)
	secondRole := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: authorizedName + "-metrics-second", Namespace: secondNS},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{"metrics.k8s.io"},
			Resources: []string{"pods"},
			Verbs:     []string{"get"},
		}},
	}
	if err := tc.Client().Create(tc.Context(), secondRole); err != nil {
		t.Fatalf("failed to create second-NS metrics Role: %v", err)
	}
	registerCleanup(secondRole)
	secondBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: authorizedName + "-metrics-second", Namespace: secondNS},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     secondRole.Name,
		},
		Subjects: []rbacv1.Subject{{
			Kind: "ServiceAccount", Name: authorizedName, Namespace: tc.MonitoringNamespace,
		}},
	}
	if err := tc.Client().Create(tc.Context(), secondBinding); err != nil {
		t.Fatalf("failed to create second-NS metrics RoleBinding: %v", err)
	}
	registerCleanup(secondBinding)
	// Wait for the second grant to take effect before checking both namespaces.
	tc.g.Eventually(func() int {
		return probePersesDatasourceProxy(t, tc, persesPod, authorizedToken, secondNS).status
	}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Equal(http.StatusOK))
	multi := probePersesDatasourceProxy(t, tc, persesPod, authorizedToken, tc.MonitoringNamespace, secondNS)
	t.Logf("perses→proxy multi-ns allow http=%d body=%q", multi.status, trimProbeBody(multi.body))
	if multi.status != http.StatusOK {
		t.Fatalf("Perses datasource proxy with both namespaces authorized must return 200, got %d body=%q",
			multi.status, trimProbeBody(multi.body))
	}
	assertPersesPostAllowed(t, tc, persesPod, authorizedToken, tc.MonitoringNamespace, secondNS)
	assertPersesPostBodyNamespaceRejected(t, tc, persesPod, authorizedToken, foreignNS)

	missingNS := probePersesDatasourceProxy(t, tc, persesPod, authorizedToken)
	t.Logf("perses→proxy authorized-missing-namespace http=%d body=%q", missingNS.status, trimProbeBody(missingNS.body))
	if missingNS.status == http.StatusOK {
		t.Fatalf("Perses datasource proxy without namespace= must fail closed, got 200")
	}
	if missingNS.status != http.StatusBadRequest && missingNS.status != http.StatusForbidden {
		t.Fatalf("Perses datasource proxy without namespace= must be 400 or 403, got %d", missingNS.status)
	}

	noToken := probePersesDatasourceProxy(t, tc, persesPod, "", tc.MonitoringNamespace)
	t.Logf("perses→proxy no-token http=%d body=%q", noToken.status, trimProbeBody(noToken.body))
	if noToken.status == http.StatusOK {
		t.Fatalf("Perses datasource proxy without Authorization must not succeed, got 200")
	}
	if noToken.status != http.StatusUnauthorized && noToken.status != http.StatusForbidden {
		t.Fatalf("Perses datasource proxy without Authorization must be 401 or 403, got %d", noToken.status)
	}

	deniedSA := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name: "perses-proxy-denied-" + identitySuffix, Namespace: tc.MonitoringNamespace,
	}}
	if err := tc.Client().Create(tc.Context(), deniedSA); err != nil {
		t.Fatalf("failed to create denied Perses proxy SA: %v", err)
	}
	registerCleanup(deniedSA)
	deniedToken := serviceAccountToken(t, tc, deniedSA.Name)
	denied := probePersesDatasourceProxy(t, tc, persesPod, deniedToken, tc.MonitoringNamespace)
	t.Logf("perses→proxy denied-sa http=%d body=%q", denied.status, trimProbeBody(denied.body))
	if denied.status != http.StatusForbidden && denied.status != http.StatusUnauthorized {
		t.Fatalf("Perses datasource proxy deny for SA without metrics.k8s.io access must be 403 or 401, got %d", denied.status)
	}
}

type persesProxyProbe struct {
	status int
	body   string
}

func assertPersesPostAllowed(t *testing.T, tc *MonitoringTestCtx, pod, token string, namespaces ...string) {
	t.Helper()
	probe := probePersesDatasourceProxyPost(t, tc, pod, token, "", namespaces...)
	if probe.status != http.StatusOK {
		t.Fatalf("Perses form POST for %v must return 200, got %d body=%q", namespaces, probe.status, trimProbeBody(probe.body))
	}
}

func assertPersesPostBodyNamespaceRejected(t *testing.T, tc *MonitoringTestCtx, pod, token, foreignNS string) {
	t.Helper()
	probe := probePersesDatasourceProxyPost(t, tc, pod, token, foreignNS, tc.MonitoringNamespace)
	if probe.status != http.StatusForbidden {
		t.Fatalf("Perses form POST with a body namespace must be forbidden, got %d body=%q", probe.status, trimProbeBody(probe.body))
	}
}

func assertPersesPostMultiNamespaceRejected(t *testing.T, tc *MonitoringTestCtx, pod, token, foreignNS string) {
	t.Helper()
	probe := probePersesDatasourceProxyPost(t, tc, pod, token, "", tc.MonitoringNamespace, foreignNS)
	if probe.status != http.StatusForbidden && probe.status != http.StatusUnauthorized {
		t.Fatalf("Perses form POST including an unauthorized namespace must be denied, got %d body=%q", probe.status, trimProbeBody(probe.body))
	}
}

func resolvePersesPodName(t *testing.T, tc *MonitoringTestCtx) string {
	t.Helper()
	var podName string
	// Poll: updateMonitoringConfig can roll Perses; List may return terminating
	// or not-Ready pods, and Items[0] is unordered.
	tc.g.Eventually(func() error {
		cfg, err := ctrlcfg.GetConfig()
		if err != nil {
			return fmt.Errorf("kubeconfig: %w", err)
		}
		clientset, err := kubernetes.NewForConfig(cfg)
		if err != nil {
			return fmt.Errorf("clientset: %w", err)
		}
		pods, err := clientset.CoreV1().Pods(tc.MonitoringNamespace).List(tc.Context(), metav1.ListOptions{
			LabelSelector: "app.kubernetes.io/name=data-science-perses",
		})
		if err != nil {
			return fmt.Errorf("list Perses pods: %w", err)
		}
		for i := range pods.Items {
			pod := &pods.Items[i]
			if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
				continue
			}
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
					podName = pod.Name
					return nil
				}
			}
		}
		return fmt.Errorf("no Ready Running Perses pod (count=%d)", len(pods.Items))
	}).WithTimeout(3 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
	return podName
}

// probePersesDatasourceProxy hits the Perses→DS proxy path.
// Pass zero namespaces to omit namespace=; one or more for repeated namespace= params.
func probePersesDatasourceProxy(t *testing.T, tc *MonitoringTestCtx, pod, token string, namespaces ...string) persesProxyProbe {
	t.Helper()
	return probePersesDatasourceProxyRequest(t, tc, pod, token, false, "", namespaces...)
}

func probePersesDatasourceProxyPost(t *testing.T, tc *MonitoringTestCtx, pod, token, formNamespace string, namespaces ...string) persesProxyProbe {
	t.Helper()
	return probePersesDatasourceProxyRequest(t, tc, pod, token, true, formNamespace, namespaces...)
}

func probePersesDatasourceProxyRequest(t *testing.T, tc *MonitoringTestCtx, pod, token string, post bool, formNamespace string, namespaces ...string) persesProxyProbe {
	t.Helper()
	var pathBuilder strings.Builder
	fmt.Fprintf(&pathBuilder,
		"/proxy/projects/%s/datasources/%s/api/v1/query?",
		url.PathEscape(tc.MonitoringNamespace),
		url.PathEscape(PersesDatasourceName),
	)
	if !post {
		pathBuilder.WriteString("query=" + url.QueryEscape(thanosRouteProbeQuery))
	}
	for _, namespace := range namespaces {
		pathBuilder.WriteString("&namespace=")
		pathBuilder.WriteString(url.QueryEscape(namespace))
	}
	curlURL := "http://127.0.0.1:8080" + pathBuilder.String()

	script := `curl -sS --connect-timeout 5 --max-time 30 -o /tmp/perses-ds-proxy-probe.body -w '%{http_code}'`
	if token != "" {
		script += ` -H 'Authorization: Bearer ` + strings.ReplaceAll(token, `'`, `'\''`) + `'`
	}
	if post {
		script += ` --data-urlencode 'query=` + thanosRouteProbeQuery + `'`
		if formNamespace != "" {
			script += ` --data-urlencode 'namespace=` + strings.ReplaceAll(formNamespace, `'`, `'\''`) + `'`
		}
	}
	script += ` '` + strings.ReplaceAll(curlURL, `'`, `'\''`) + `'`

	stdout, stderr, err := execInPersesPod(t, tc, pod, []string{"sh", "-c", script})
	if err != nil {
		t.Fatalf("Perses datasource proxy exec failed: %v stderr=%s stdout=%s", err, stderr, stdout)
	}
	status, err := strconv.Atoi(strings.TrimSpace(stdout))
	if err != nil {
		t.Fatalf("parsing Perses proxy HTTP status %q (stderr=%s): %v", stdout, stderr, err)
	}
	bodyOut, _, bodyErr := execInPersesPod(t, tc, pod, []string{"cat", "/tmp/perses-ds-proxy-probe.body"})
	if bodyErr != nil {
		t.Logf("reading Perses proxy probe body: %v", bodyErr)
	}
	return persesProxyProbe{status: status, body: bodyOut}
}

func execInPersesPod(t *testing.T, tc *MonitoringTestCtx, pod string, command []string) (string, string, error) {
	t.Helper()
	cfg, err := ctrlcfg.GetConfig()
	if err != nil {
		return "", "", err
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return "", "", err
	}
	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod).
		Namespace(tc.MonitoringNamespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Command: command,
			Stdout:  true,
			Stderr:  true,
		}, scheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(cfg, http.MethodPost, req.URL())
	if err != nil {
		return "", "", err
	}
	var stdout, stderr bytes.Buffer
	err = executor.StreamWithContext(tc.Context(), remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return stdout.String(), stderr.String(), err
}

func trimProbeBody(body string) string {
	body = strings.TrimSpace(body)
	if len(body) > 180 {
		return body[:180] + "…"
	}
	return body
}
