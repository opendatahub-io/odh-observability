package e2e_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega
)

// ValidateTrustedDCGMAttribution exercises the isolated allocator path without
// requiring GPU hardware. The fixture is deliberately created in the trusted
// GPU-operator namespace, never in a tenant namespace.
func (tc *MonitoringTestCtx) ValidateTrustedDCGMAttribution(t *testing.T) {
	t.Helper()
	tc.validateTrustedDCGMAttribution(t, false)
}

func (tc *MonitoringTestCtx) ValidateTrustedDCGMTLSAttribution(t *testing.T) {
	t.Helper()
	tc.validateTrustedDCGMAttribution(t, true)
}

func (tc *MonitoringTestCtx) validateTrustedDCGMAttribution(t *testing.T, useTLS bool) { //nolint:gocyclo // Exercises both HTTP and HTTPS end to end.
	t.Helper()
	tc = tc.WithT(t)
	const operatorNamespace = "nvidia-gpu-operator"
	if err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: operatorNamespace}, &corev1.Namespace{}); err != nil {
		if k8serr.IsNotFound(err) {
			t.Skip("GPU operator namespace is not installed")
		}
		t.Fatalf("read GPU operator namespace: %v", err)
	}
	tc.setupMetrics(t)
	id := fmt.Sprintf("%x", time.Now().UnixNano())
	const serverName = "dcgm-fixture.example.test"
	caName := "dcgm-fixture-ca-" + id
	certName := "dcgm-fixture-cert-" + id
	if useTLS {
		certificate, err := newDCGMHTTPSFixtureCertificate(serverName)
		if err != nil {
			t.Fatalf("create DCGM TLS fixture: %v", err)
		}
		for _, secret := range []*corev1.Secret{
			{ObjectMeta: metav1.ObjectMeta{Name: caName, Namespace: tc.MonitoringNamespace}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{"ca.crt": certificate.caPEM}},
			{
				ObjectMeta: metav1.ObjectMeta{Name: certName, Namespace: operatorNamespace}, Type: corev1.SecretTypeTLS,
				Data: map[string][]byte{"tls.crt": certificate.certificatePEM, "tls.key": certificate.privateKeyPEM},
			},
		} {
			tc.createFixtureResource(t, gvk.Secret, secret)
			t.Cleanup(func() {
				tc.DeleteResource(WithMinimalObject(gvk.Secret, types.NamespacedName{Name: secret.Name, Namespace: secret.Namespace}), WithIgnoreNotFound(true), WithWaitForDeletion(true))
			})
		}
	}
	// Reset the collector configuration before deleting the CA Secret.
	t.Cleanup(tc.resetMonitoringConfigToMetrics)
	if useTLS {
		tc.updateMonitoringConfig(withManagementState(common.Managed), tc.withMetricsConfig(), jq.Transform(
			`.spec.collectorReplicas = 2 | del(.spec.metrics.builtinDCGMScrapeEnabled) | .spec.metrics.builtinDCGMTLS = {"caSecretName": %q, "serverName": %q}`,
			caName, serverName))
	} else {
		tc.updateMonitoringConfig(withManagementState(common.Managed), tc.withMetricsConfig(), jq.Transform(
			`.spec.collectorReplicas = 2 | del(.spec.metrics.builtinDCGMScrapeEnabled, .spec.metrics.builtinDCGMTLS)`))
	}
	tc.ensureOpenTelemetryCollectorReady(t)

	namespace := "tests-dcgm-workload-" + id
	foreign := "tests-dcgm-foreign-" + id
	for _, name := range []string{namespace, foreign} {
		tc.createFixtureResource(t, gvk.Namespace, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}})
		t.Cleanup(func() {
			tc.DeleteResource(WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: name}), WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))
		})
	}
	podName := "nvidia-dcgm-exporter-e2e-" + id
	// The fixture also attempts to supply reserved labels; the isolated scrape
	// must replace them and must not invent a pod for the incomplete sample.
	data := fmt.Sprintf("# TYPE DCGM_FI_DEV_GPU_UTIL gauge\n"+
		"DCGM_FI_DEV_GPU_UTIL{fixture_id=%q,fixture_state=\"complete\",namespace=%q,pod=\"model\",odh__dcgm__source=\"forged\",odh__dcgm__workload__pod=\"wrong\"} 75\n"+
		"DCGM_FI_DEV_GPU_UTIL{fixture_id=%q,fixture_state=\"missing\",namespace=%q,odh__dcgm__workload__pod=\"wrong\"} 30\n", id, namespace, id, namespace)
	server := fmt.Sprintf(`from http.server import BaseHTTPRequestHandler, HTTPServer
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  data = %q.encode()
  self.send_response(200)
  self.send_header('Content-Type', 'text/plain; version=0.0.4')
  self.send_header('Content-Length', str(len(data)))
  self.end_headers()
  self.wfile.write(data)
httpd = HTTPServer(('0.0.0.0', 9400), Handler)
`, data)
	container := corev1.Container{
		Name: "fixture", Image: "registry.access.redhat.com/ubi9/python-311:latest",
		SecurityContext: fixtureContainerSecurityContext(), Ports: []corev1.ContainerPort{{ContainerPort: 9400}},
	}
	podSpec := corev1.PodSpec{SecurityContext: fixturePodSecurityContext()}
	if useTLS {
		server += `import ssl
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain('/etc/dcgm-fixture/tls.crt', '/etc/dcgm-fixture/tls.key')
httpd.socket = context.wrap_socket(httpd.socket, server_side=True)
`
		container.VolumeMounts = []corev1.VolumeMount{{Name: "tls", MountPath: "/etc/dcgm-fixture", ReadOnly: true}}
		podSpec.Volumes = []corev1.Volume{{Name: "tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: certName}}}}
	}
	server += "httpd.serve_forever()"
	container.Command = []string{"python3", "-c", server}
	podSpec.Containers = []corev1.Container{container}
	tc.createFixtureResource(t, gvk.Pod, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: operatorNamespace, Labels: map[string]string{"app": "odh-dcgm-fixture"}},
		Spec:       podSpec,
	})
	t.Cleanup(func() {
		tc.DeleteResource(WithMinimalObject(gvk.Pod, types.NamespacedName{Name: podName, Namespace: operatorNamespace}),
			WithIgnoreNotFound(true), WithWaitForDeletion(true), WithEventuallyTimeout(5*time.Minute))
	})
	viewer := tc.metricsViewerToken(t, "dcgm-trusted-viewer", namespace)
	foreignViewer := tc.metricsViewerToken(t, "dcgm-trusted-foreign-viewer", foreign)
	monitorViewer := tc.metricsViewerToken(t, "dcgm-trusted-monitor-viewer", tc.MonitoringNamespace)
	rootCAs := clusterIngressCAPool(t, tc)
	host := tc.fetchPrometheusRouteHost(t)
	query := fmt.Sprintf(`accelerator_gpu_utilization{fixture_id=%q,fixture_state="complete"}`, id)
	tc.g.Eventually(func() error {
		status, response, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, viewer, namespace, query)
		if err != nil {
			return err
		}
		if status != http.StatusOK || len(response.Data.Result) != 1 {
			return fmt.Errorf("expected one sharded trusted GPU sample: status=%d count=%d", status, len(response.Data.Result))
		}
		result := response.Data.Result[0]
		if result.Metric["namespace"] != namespace || result.Metric["pod"] != "model" || result.Metric["odh_dcgm_source"] != "verified" ||
			len(result.Value) != 2 || result.Value[1] != "75" {
			return fmt.Errorf("incorrect trusted GPU attribution or value: %v %v", result.Metric, result.Value)
		}
		return nil
	}, 10*time.Minute, 15*time.Second).Should(Succeed())
	status, response, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, foreignViewer, foreign, query)
	if err != nil || status != http.StatusOK || len(response.Data.Result) != 0 {
		t.Fatalf("foreign tenant can see trusted GPU sample: status=%d count=%d error=%v", status, len(response.Data.Result), err)
	}
	missingQuery := fmt.Sprintf(`accelerator_gpu_utilization{fixture_id=%q,fixture_state="missing"}`, id)
	tc.g.Eventually(func() error {
		status, response, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, monitorViewer, tc.MonitoringNamespace, missingQuery)
		if err != nil {
			return err
		}
		if status != http.StatusOK || len(response.Data.Result) != 1 || response.Data.Result[0].Metric["odh_dcgm_workload_pod"] != "" {
			return fmt.Errorf("incomplete attribution must remain in monitoring namespace without forged pod: status=%d results=%v", status, response.Data.Result)
		}
		return nil
	}, 10*time.Minute, 15*time.Second).Should(Succeed())
	status, response, err = queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, viewer, namespace, missingQuery)
	if err != nil || status != http.StatusOK || len(response.Data.Result) != 0 {
		t.Fatalf("incomplete attribution was promoted: status=%d results=%v error=%v", status, response.Data.Result, err)
	}
	if useTLS {
		tc.updateMonitoringConfig(jq.Transform(`.spec.metrics.builtinDCGMTLS.serverName = "wrong.example.test"`))
		tc.ensureOpenTelemetryCollectorReady(t)
		failed := `up{exported_job="rhoai-accelerator-metrics",component="odh-dcgm-fixture"} == 0`
		tc.g.Eventually(func() error {
			status, response, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, host, monitorViewer, tc.MonitoringNamespace, failed)
			if err != nil {
				return err
			}
			if status != http.StatusOK || len(response.Data.Result) == 0 {
				return fmt.Errorf("mismatched trusted DCGM hostname must fail: status=%d results=%v", status, response.Data.Result)
			}
			return nil
		}, 10*time.Minute, 15*time.Second).Should(Succeed())
	}
}
