package controller

import (
	"context"
	"testing"

	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
)

func TestUsageLogsReceiverRendersTLS(t *testing.T) {
	ctx := context.Background()
	scheme := newActionsTestScheme(t)
	registerCRDs(scheme, gvk.OpenTelemetryCollector)
	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.UsageLogs = &v1alpha1.UsageLogs{Storage: &v1alpha1.LokiStorageConfig{Type: "s3", SecretName: "storage"}}
	m.Status.UsageLogsEndpoint = "https://loki.example/otlp"
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(kubernetesAPIServerEndpointSlice()).Build()

	data, err := buildTemplateData(ctx, client, m, "")
	if err != nil {
		t.Fatal(err)
	}
	cm := conditions.NewConditionsManager(m, m.Generation)
	var sources []rendertemplate.TemplateSource
	if err := deployUsageLogsCollector(ctx, client, m, cm, &sources); err != nil {
		t.Fatal(err)
	}
	objects, err := rendertemplate.Render(ctx, scheme, sources, data)
	if err != nil {
		t.Fatal(err)
	}
	byKind := map[string]*unstructured.Unstructured{}
	for i := range objects {
		byKind[objects[i].GetKind()] = objects[i].DeepCopy()
	}
	collector := byKind["OpenTelemetryCollector"]
	if collector == nil {
		t.Fatalf("expected usage logs collector; got %v", byKind)
	}
	if byKind["Service"] != nil {
		t.Fatal("usage logs collector must use the OpenTelemetry Operator's generated headless Service")
	}
	if usageLogsReceiverTLSSecretName != collector.GetName()+"-collector-headless-tls" {
		t.Fatal("serving certificate Secret must match the OpenTelemetry Operator's generated headless Service")
	}
	assertUsageLogsTLSMounts(t, collector)
	for _, protocol := range []string{"grpc", "http"} {
		receiver, found, err := unstructured.NestedMap(collector.Object, "spec", "config", "receivers", "otlp", "protocols", protocol)
		if err != nil || !found {
			t.Fatalf("missing %s receiver: %v", protocol, err)
		}
		tls, ok := receiver["tls"].(map[string]any)
		if !ok ||
			tls["cert_file"] != "/etc/otel-collector/otlp-tls/tls.crt" ||
			tls["key_file"] != "/etc/otel-collector/otlp-tls/tls.key" {
			t.Errorf("%s receiver must load the mounted serving certificate", protocol)
		}
		if tls["reload_interval"] != "1h" {
			t.Errorf("%s receiver must reload renewed serving certificates", protocol)
		}
	}
}

func assertUsageLogsTLSMounts(t *testing.T, collector *unstructured.Unstructured) {
	t.Helper()

	volumes, found, err := unstructured.NestedSlice(collector.Object, "spec", "volumes")
	if err != nil || !found {
		t.Fatalf("collector must mount serving certificate Secret: %v", err)
	}
	secretVolumes := map[string]string{}
	for _, value := range volumes {
		volume, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("expected volume object, got %T", value)
		}
		secret, ok := volume["secret"].(map[string]any)
		if ok {
			name, nameOK := volume["name"].(string)
			secretName, secretNameOK := secret["secretName"].(string)
			if !nameOK || !secretNameOK {
				t.Fatalf("expected string volume and Secret names, got volume=%v", volume)
			}
			secretVolumes[name] = secretName
		}
	}
	if secretVolumes["usage-logs-otlp-tls"] != usageLogsReceiverTLSSecretName {
		t.Fatalf("collector must mount the serving certificate: %v", secretVolumes)
	}
	mounts, found, err := unstructured.NestedSlice(collector.Object, "spec", "volumeMounts")
	if err != nil || !found {
		t.Fatalf("collector must mount serving certificate files: %v", err)
	}
	certMountFound := false
	for _, value := range mounts {
		mount, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("expected volume mount object, got %T", value)
		}
		if mount["name"] == "usage-logs-otlp-tls" &&
			mount["mountPath"] == "/etc/otel-collector/otlp-tls" &&
			mount["readOnly"] == true {
			certMountFound = true
		}
	}
	if !certMountFound {
		t.Fatal("collector must mount the serving certificate read-only at its configured path")
	}
}
