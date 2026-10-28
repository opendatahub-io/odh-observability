package e2e_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega matchers
)

const (
	dcgmHTTPSExporterName = "dcgm-https-exporter"
	dcgmHTTPSMonitorName  = "dcgm-https-exporter"
	dcgmHTTPSTLSSecret    = "dcgm-https-tls"
	dcgmHTTPSCASecret     = "dcgm-https-ca"
	dcgmHTTPSServicePort  = int32(9090)
	dcgmHTTPSMetricFilter = "(DCGM_FI_DEV_GPU_TEMP|DCGM_FI_DEV_GPU_UTIL|DCGM_FI_PROF_GR_ENGINE_ACTIVE|" +
		"DCGM_FI_DEV_MEM_COPY_UTIL|DCGM_FI_DEV_FB_USED|DCGM_FI_DEV_FB_FREE|DCGM_FI_DEV_POWER_USAGE|" +
		"DCGM_FI_DEV_SM_CLOCK|DCGM_FI_DEV_MEM_CLOCK)"
)

// ValidateBuiltinDCGMScrapeOptOutUsesVerifiedHTTPSMonitor proves the opt-out
// leaves TargetAllocator monitor discovery active, a selected HTTPS PodMonitor
// in a workload namespace can scrape a DCGM-shaped metric under its
// namespace-scoped query label, and a hostname mismatch makes the scrape fail.
// The Prometheus fixture's build-info metric is renamed to a canonical DCGM
// name by the test-only monitor relabeling, so no GPU hardware or NVIDIA
// exporter is required.
func (tc *MonitoringTestCtx) ValidateBuiltinDCGMScrapeOptOutUsesVerifiedHTTPSMonitor(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	tc.setupMetrics(t)
	t.Cleanup(tc.resetMonitoringConfigToMetrics)

	namespace := fmt.Sprintf("tests-monitoring-dcgm-https-%x", time.Now().UnixNano())
	serviceDNS := fmt.Sprintf("%s.%s.svc", dcgmHTTPSExporterName, namespace)
	tlsCert, err := newDCGMHTTPSFixtureCertificate(serviceDNS)
	if err != nil {
		t.Fatalf("generate HTTPS fixture certificate: %v", err)
	}

	tc.createFixtureResource(t, gvk.Namespace, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   namespace,
			Labels: map[string]string{ODHLabelMonitoring: "true"},
		},
	})
	t.Cleanup(func() {
		tc.DeleteResource(
			WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: namespace}),
			WithIgnoreNotFound(true),
			WithWaitForDeletion(true),
			WithEventuallyTimeout(5*time.Minute),
		)
	})

	tc.createFixtureResource(t, gvk.Secret, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: dcgmHTTPSTLSSecret, Namespace: namespace},
		Type:       corev1.SecretTypeTLS,
		StringData: map[string]string{
			"tls.crt": string(tlsCert.certificatePEM),
			"tls.key": string(tlsCert.privateKeyPEM),
		},
	})
	tc.createFixtureResource(t, gvk.Secret, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: dcgmHTTPSCASecret, Namespace: namespace},
		Type:       corev1.SecretTypeOpaque,
		StringData: map[string]string{"ca.crt": string(tlsCert.caPEM)},
	})
	tc.createFixtureResource(t, gvk.ConfigMap, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: dcgmHTTPSExporterName, Namespace: namespace},
		Data: map[string]string{
			"web-config.yml": "tls_server_config:\n  cert_file: /etc/prometheus/tls/tls.crt\n  key_file: /etc/prometheus/tls/tls.key\n",
		},
	})
	tc.createFixtureResource(t, gvk.Pod, dcgmHTTPSExporterPod(namespace))
	tc.createFixtureResource(t, gvk.Service, dcgmHTTPSExporterService(namespace))
	monitor := dcgmHTTPSExporterPodMonitor(namespace, serviceDNS)
	tc.EventuallyResourceCreated(
		WithMinimalObject(gvk.PodMonitorCoreos, types.NamespacedName{Name: dcgmHTTPSMonitorName, Namespace: namespace}),
		WithObjectContent(monitor.Object),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Pod, types.NamespacedName{Name: dcgmHTTPSExporterName, Namespace: namespace}),
		WithCondition(jq.Match(`.status.phase == "Running" and any(.status.conditions[]; .type == "Ready" and .status == "True")`)),
		WithEventuallyTimeout(10*time.Minute),
		WithCustomErrorMsg("HTTPS DCGM fixture Pod must become Ready"),
	)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
		jq.Transform(`.spec.metrics.builtinDCGMScrapeEnabled = false`),
	)
	tc.ensureOpenTelemetryCollectorReady(t)
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{Name: OpenTelemetryCollectorName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`(.spec.config.receivers.prometheus.config.scrape_configs // [] | any(.[]; .job_name == "dcgm-exporter-accelerator-metrics")) | not`),
			jq.Match(`.spec.config.service.pipelines."metrics/scrape".receivers | contains(["prometheus"])`),
			jq.Match(`.spec.config.service.pipelines."metrics/scrape".processors | contains(["transform/gpu_metrics"])`),
			jq.Match(`.spec.config.receivers.prometheus.target_allocator != null`),
			jq.Match(`.spec.targetAllocator.enabled == true`),
			jq.Match(`.spec.targetAllocator.prometheusCR.podMonitorSelector.matchLabels."monitoring.opendatahub.io/scrape" == "true"`),
		)),
		WithCustomErrorMsg("built-in DCGM opt-out must preserve collector pipeline and selected monitor discovery"),
	)

	viewerToken := tc.metricsViewerToken(t, "dcgm-https-viewer", namespace)
	rootCAs := clusterIngressCAPool(t, tc)
	routeHost := tc.fetchPrometheusRouteHost(t)
	metricQuery := fmt.Sprintf(`DCGM_FI_DEV_GPU_UTIL{namespace=%q,job="rhoai-accelerator-metrics",component="nvidia-dcgm-exporter"}`, namespace)
	tc.g.Eventually(func() error {
		status, response, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, routeHost, viewerToken, namespace, metricQuery)
		if err != nil {
			return err
		}
		if status != 200 {
			return fmt.Errorf("DCGM-shaped metric query returned HTTP %d", status)
		}
		for _, result := range response.Data.Result {
			if result.Metric["job"] == "rhoai-accelerator-metrics" &&
				result.Metric["node"] != "" &&
				result.Metric["component"] == "nvidia-dcgm-exporter" {
				return nil
			}
		}
		return errors.New("no HTTPS-scraped DCGM metric with expected namespace/node/component/job labels yet")
	}, 10*time.Minute, 15*time.Second).Should(Succeed(), "selected HTTPS PodMonitor must deliver DCGM-shaped metrics with expected labels")

	// Keep the CA trusted but break the certificate identity. Prometheus should
	// expose up == 0 for this target, proving hostname verification is enforced.
	tc.EventuallyResourcePatched(
		WithMinimalObject(gvk.PodMonitorCoreos, types.NamespacedName{Name: dcgmHTTPSMonitorName, Namespace: namespace}),
		WithMutateFunc(func(monitor *unstructured.Unstructured) error {
			endpoints, found, err := unstructured.NestedSlice(monitor.Object, "spec", "podMetricsEndpoints")
			if err != nil {
				return fmt.Errorf("read PodMonitor endpoints: %w", err)
			}
			if !found || len(endpoints) != 1 {
				return fmt.Errorf("PodMonitor endpoint count: found=%t count=%d", found, len(endpoints))
			}
			endpoint, ok := endpoints[0].(map[string]any)
			if !ok {
				return fmt.Errorf("PodMonitor endpoint has unexpected type %T", endpoints[0])
			}
			tlsConfig, ok := endpoint["tlsConfig"].(map[string]any)
			if !ok {
				return fmt.Errorf("PodMonitor TLS config has unexpected type %T", endpoint["tlsConfig"])
			}
			tlsConfig["serverName"] = "wrong.example.invalid"
			endpoints[0] = endpoint
			return unstructured.SetNestedSlice(monitor.Object, endpoints, "spec", "podMetricsEndpoints")
		}),
	)
	failedScrapeQuery := fmt.Sprintf(`up{namespace=%q,job="rhoai-accelerator-metrics",component="nvidia-dcgm-exporter"} == 0`, namespace)
	tc.g.Eventually(func() error {
		status, response, err := queryMetricsViaNamespaceProxy(tc.Context(), rootCAs, routeHost, viewerToken, namespace, failedScrapeQuery)
		if err != nil {
			return err
		}
		if status != 200 {
			return fmt.Errorf("failed-scrape query returned HTTP %d", status)
		}
		if len(response.Data.Result) == 0 {
			return errors.New("no up == 0 series for HTTPS target with mismatched server name yet")
		}
		return nil
	}, 10*time.Minute, 15*time.Second).Should(Succeed(), "HTTPS target with a mismatched server name must fail certificate verification")

	// Remove the override and verify nil restores the built-in HTTP job.
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
		jq.Transform(`del(.spec.metrics.builtinDCGMScrapeEnabled)`),
	)
	tc.ensureOpenTelemetryCollectorReady(t)
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{Name: OpenTelemetryCollectorName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.spec.config.receivers.prometheus.config.scrape_configs | any(.[]; .job_name == "dcgm-exporter-accelerator-metrics" and .scheme == "http")`)),
		WithCustomErrorMsg("omitting built-in DCGM setting must restore the default HTTP scrape"),
	)
}

type dcgmHTTPSFixtureCertificate struct {
	caPEM          []byte
	certificatePEM []byte
	privateKeyPEM  []byte
}

func newDCGMHTTPSFixtureCertificate(serverName string) (dcgmHTTPSFixtureCertificate, error) {
	var fixture dcgmHTTPSFixtureCertificate
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fixture, fmt.Errorf("generate CA key: %w", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(now.UnixNano()),
		Subject:               pkix.Name{CommonName: "dcgm-https-e2e-ca"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return fixture, fmt.Errorf("create CA certificate: %w", err)
	}
	caCertificate, err := x509.ParseCertificate(caDER)
	if err != nil {
		return fixture, fmt.Errorf("parse CA certificate: %w", err)
	}

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fixture, fmt.Errorf("generate server key: %w", err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano() + 1),
		Subject:      pkix.Name{CommonName: serverName},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{serverName},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCertificate, &serverKey.PublicKey, caKey)
	if err != nil {
		return fixture, fmt.Errorf("create server certificate: %w", err)
	}
	privateKeyDER, err := x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		return fixture, fmt.Errorf("marshal server key: %w", err)
	}
	fixture.caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	fixture.certificatePEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER})
	fixture.privateKeyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKeyDER})
	return fixture, nil
}

func dcgmHTTPSExporterPod(namespace string) *corev1.Pod {
	automountToken := false
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dcgmHTTPSExporterName,
			Namespace: namespace,
			Labels:    map[string]string{"app": "nvidia-dcgm-exporter"},
		},
		Spec: corev1.PodSpec{
			AutomountServiceAccountToken: &automountToken,
			SecurityContext:              fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name:  dcgmHTTPSExporterName,
				Image: tenantMetricsImage,
				Args: []string{
					"--config.file=/etc/prometheus/prometheus.yml",
					"--storage.tsdb.path=/prometheus",
					"--web.listen-address=0.0.0.0:9090",
					"--web.config.file=/etc/prometheus/web-config.yml",
				},
				Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: dcgmHTTPSServicePort}},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler:        corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString("metrics")}},
					InitialDelaySeconds: 5,
					PeriodSeconds:       5,
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
					Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("200m"), corev1.ResourceMemory: resource.MustParse("512Mi")},
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "tls", MountPath: "/etc/prometheus/tls", ReadOnly: true},
					{Name: "web-config", MountPath: "/etc/prometheus/web-config.yml", SubPath: "web-config.yml", ReadOnly: true},
					{Name: "data", MountPath: "/prometheus"},
				},
				SecurityContext: fixtureContainerSecurityContext(),
			}},
			Volumes: []corev1.Volume{
				{Name: "tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: dcgmHTTPSTLSSecret}}},
				{
					Name: "web-config",
					VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: dcgmHTTPSExporterName},
					}},
				},
				{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			},
		},
	}
}

func dcgmHTTPSExporterService(namespace string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: dcgmHTTPSExporterName, Namespace: namespace},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "nvidia-dcgm-exporter"},
			Ports: []corev1.ServicePort{{
				Name:       "metrics",
				Port:       dcgmHTTPSServicePort,
				TargetPort: intstr.FromString("metrics"),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}
}

func dcgmHTTPSExporterPodMonitor(namespace, serverName string) *unstructured.Unstructured {
	monitor := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "monitoring.coreos.com/v1",
		"kind":       "PodMonitor",
		"metadata": map[string]any{
			"name":      dcgmHTTPSMonitorName,
			"namespace": namespace,
			"labels":    map[string]any{ODHLabelMonitoring: "true"},
		},
		"spec": map[string]any{
			"namespaceSelector": map[string]any{"matchNames": []any{namespace}},
			"selector":          map[string]any{"matchLabels": map[string]any{"app": "nvidia-dcgm-exporter"}},
			"podMetricsEndpoints": []any{map[string]any{
				"port":   "metrics",
				"path":   "/metrics",
				"scheme": "https",
				"tlsConfig": map[string]any{
					"ca": map[string]any{
						"secret": map[string]any{"name": dcgmHTTPSCASecret, "key": "ca.crt"},
					},
					"serverName": serverName,
				},
				"relabelings": []any{
					// Set namespace from Kubernetes discovery metadata so the
					// namespace-restricted proxy can query this fixture.
					map[string]any{"action": "replace", "sourceLabels": []any{"__meta_kubernetes_namespace"}, "targetLabel": "namespace"},
					map[string]any{"action": "replace", "sourceLabels": []any{"__meta_kubernetes_pod_node_name"}, "targetLabel": "node"},
					map[string]any{"action": "replace", "sourceLabels": []any{"__meta_kubernetes_pod_label_app"}, "targetLabel": "component"},
					map[string]any{"action": "replace", "replacement": "rhoai-accelerator-metrics", "targetLabel": "job"},
				},
				"metricRelabelings": []any{
					map[string]any{
						"action":       "replace",
						"sourceLabels": []any{"__name__"},
						"regex":        "prometheus_build_info",
						"targetLabel":  "__name__",
						"replacement":  "DCGM_FI_DEV_GPU_UTIL",
					},
					map[string]any{
						"action":       "keep",
						"sourceLabels": []any{"__name__"},
						"regex":        dcgmHTTPSMetricFilter,
					},
				},
			}},
		},
	}}
	monitor.SetGroupVersionKind(gvk.PodMonitorCoreos)
	return monitor
}
