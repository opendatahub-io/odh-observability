package controller

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestMainCollectorOTLPReceiversUseServingTLSCertificate(t *testing.T) {
	t.Parallel()

	collector := renderCollectorTemplate(t)
	assertCollectorOTLPTLS(t, collector, "data-science-collector-collector-headless-tls")
}

func TestUsageLogsCollectorOTLPReceiversUseServingTLSCertificate(t *testing.T) {
	t.Parallel()

	resources := renderObservabilityTemplate(t, UsageLogsOpenTelemetryCollectorTemplate, map[string]any{
		"Namespace":                 "redhat-ods-monitoring",
		"UsageLogsCollectorName":    "data-science-usage-logs",
		"UsageLogs":                 true,
		"UsageLogsEndpoint":         "https://loki.example.svc.cluster.local:8080/otlp",
		"UsageLogsExporters":        map[string]string{},
		"UsageLogsExporterNames":    []string{},
		"UsageLogsCredentialSecret": "",
		"UsageLogsAttributes":       map[string]string{},
		"UsageLogsAttributeNames":   []string{},
		"CollectorReplicas":         1,
		"CollectorCPULimit":         "500m",
		"CollectorMemoryLimit":      "1Gi",
		"CollectorCPURequest":       "100m",
		"CollectorMemoryRequest":    "256Mi",
	})
	if len(resources) != 1 {
		t.Fatalf("expected one usage logs collector, got %d", len(resources))
	}

	assertCollectorOTLPTLS(t, resources[0], "data-science-usage-logs-collector-headless-tls")
}

func TestInstrumentationUsesHTTPSAndTrustsServiceCA(t *testing.T) {
	t.Parallel()

	resources := renderObservabilityTemplate(t, InstrumentationTemplate, map[string]any{
		"Namespace":    "redhat-ods-monitoring",
		"OtlpEndpoint": "https://data-science-collector-collector-headless.redhat-ods-monitoring.svc.cluster.local:4317",
		"SampleRatio":  "1",
	})
	if len(resources) != 1 {
		t.Fatalf("expected one Instrumentation resource, got %d", len(resources))
	}
	instrumentation := resources[0]

	endpoint, found, err := unstructured.NestedString(instrumentation.Object, "spec", "exporter", "endpoint")
	if err != nil || !found || endpoint != "https://data-science-collector-collector-headless.redhat-ods-monitoring.svc.cluster.local:4317" {
		t.Fatalf("Instrumentation endpoint must use HTTPS: found=%t endpoint=%q error=%v", found, endpoint, err)
	}
	caFile, found, err := unstructured.NestedString(instrumentation.Object, "spec", "exporter", "tls", "ca_file")
	if err != nil || !found || caFile != "/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt" {
		t.Fatalf("Instrumentation must trust the OpenShift service CA: found=%t ca_file=%q error=%v", found, caFile, err)
	}
}

func assertCollectorOTLPTLS(t *testing.T, collector unstructured.Unstructured, secretName string) {
	t.Helper()

	servingCertSecret, found, err := unstructured.NestedString(collector.Object, "metadata", "annotations", "service.beta.openshift.io/serving-cert-secret-name")
	if err != nil || found {
		t.Errorf("collector must leave serving-cert annotation to the generated headless Service: found=%t secret=%q error=%v", found, servingCertSecret, err)
	}

	mounts, found, err := unstructured.NestedSlice(collector.Object, "spec", "volumeMounts")
	if err != nil || !found || !hasOTLPTLSVolumeMount(mounts) {
		t.Errorf("collector must mount its OTLP serving certificate at /etc/otel-collector/otlp-tls: found=%t mounts=%v error=%v", found, mounts, err)
	}
	volumes, found, err := unstructured.NestedSlice(collector.Object, "spec", "volumes")
	if err != nil || !found || !hasOTLPTLSVolume(volumes, secretName) {
		t.Errorf("collector must mount serving certificate Secret %q: found=%t volumes=%v error=%v", secretName, found, volumes, err)
	}

	for _, protocol := range []string{"grpc", "http"} {
		tlsConfig, found, err := unstructured.NestedMap(collector.Object, "spec", "config", "receivers", "otlp", "protocols", protocol, "tls")
		if err != nil || !found {
			t.Errorf("OTLP %s receiver must enable TLS: found=%t config=%v error=%v", protocol, found, tlsConfig, err)
			continue
		}
		if tlsConfig["cert_file"] != "/etc/otel-collector/otlp-tls/tls.crt" || tlsConfig["key_file"] != "/etc/otel-collector/otlp-tls/tls.key" {
			t.Errorf("OTLP %s receiver must use the mounted serving certificate and key: got %v", protocol, tlsConfig)
		}
		if tlsConfig["min_version"] != "1.2" {
			t.Errorf("OTLP %s receiver must require TLS 1.2 or newer: got min_version=%v", protocol, tlsConfig["min_version"])
		}
		if tlsConfig["reload_interval"] != "5m" {
			t.Errorf("OTLP %s receiver must reload rotated serving certificates every 5m: got reload_interval=%v", protocol, tlsConfig["reload_interval"])
		}
	}
}

func hasOTLPTLSVolumeMount(mounts []any) bool {
	for _, raw := range mounts {
		mount, ok := raw.(map[string]any)
		if ok && mount["name"] == "otlp-tls-certs" && mount["mountPath"] == "/etc/otel-collector/otlp-tls" && mount["readOnly"] == true {
			return true
		}
	}
	return false
}

func hasOTLPTLSVolume(volumes []any, secretName string) bool {
	for _, raw := range volumes {
		volume, ok := raw.(map[string]any)
		if !ok || volume["name"] != "otlp-tls-certs" {
			continue
		}
		secret, ok := volume["secret"].(map[string]any)
		if ok && secret["secretName"] == secretName {
			return true
		}
	}
	return false
}
