package controller

import (
	"fmt"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
)

func TestTrustedDCGMUsesOnlyAllocatedBuiltinTargets(t *testing.T) { //nolint:gocyclo // Checks all parts of the isolated allocation contract.
	for _, storage := range []bool{false, true} {
		collector := renderCollectorTemplateWithExporters(t, true, true, storage, 2, []string{"debug"})
		job := renderedPrometheusScrapeJobsForReceiver(t, collector, "prometheus/trusted_dcgm")["trusted-dcgm-exporter"]
		if job == nil || job["scheme"] != "http" || job["follow_redirects"] != false {
			t.Fatalf("trusted DCGM must scrape HTTP without following redirects: %v", job)
		}
		sd, found, err := unstructured.NestedSlice(job, "http_sd_configs")
		if err != nil || !found || len(sd) != 1 {
			t.Fatalf("expected one allocator SD config: %v %v %v", sd, found, err)
		}
		config, ok := sd[0].(map[string]any)
		allocatorURL := "https://data-science-collector-targetallocator.redhat-ods-monitoring.svc:443" +
			"/jobs/dcgm-exporter-accelerator-metrics/targets?collector_id=${env:POD_NAME}"
		if !ok || config["url"] != allocatorURL {
			t.Fatalf("trusted DCGM must use this replica's builtin-job allocation: %v", sd[0])
		}
		tls, found, err := unstructured.NestedMap(config, "tls_config")
		if err != nil || !found || tls["ca_file"] != "/tls/ca.crt" || tls["cert_file"] != "/tls/tls.crt" || tls["key_file"] != "/tls/tls.key" || tls["insecure_skip_verify"] == true {
			t.Fatalf("allocator SD must authenticate with verified mTLS: %v", tls)
		}
		rules := renderedPrometheusRelabelRules(t, job)
		for guard, regex := range map[string]string{"__meta_kubernetes_namespace": "nvidia-gpu-operator", "__meta_kubernetes_pod_name": "^nvidia-dcgm-exporter.*$"} {
			matched := false
			for _, rule := range rules {
				if rule["action"] == "keep" && rule["regex"] == regex && containsString(rule["source_labels"], guard) {
					matched = true
				}
			}
			if !matched {
				t.Errorf("trusted DCGM must check discovery metadata %s", guard)
			}
		}
		metricRules, found, err := unstructured.NestedSlice(job, "metric_relabel_configs")
		if err != nil || !found || len(metricRules) != 5 {
			t.Fatalf("trusted DCGM provenance relabeling missing: %v", metricRules)
		}
		for i, expected := range []string{
			"action:keep", "action:labeldrop", "target_label:odh_dcgm_workload_namespace", "target_label:odh_dcgm_workload_pod", "target_label:odh_dcgm_source",
		} {
			if !strings.Contains(fmt.Sprint(metricRules[i]), expected) {
				t.Errorf("unexpected trusted DCGM metric relabel %d: %v", i, metricRules[i])
			}
		}
		for i, source := range map[int]string{2: "namespace", 3: "pod"} {
			rule, ok := metricRules[i].(map[string]any)
			if !ok || !containsString(rule["source_labels"], source) || rule["regex"] != "(.+)" {
				t.Errorf("trusted workload field must only be copied from nonempty %s: %v", source, metricRules[i])
			}
		}
		marker, ok := metricRules[4].(map[string]any)
		if !ok {
			t.Fatalf("unexpected trusted marker type: %T", metricRules[4])
		}
		if marker["replacement"] != "verified" {
			t.Errorf("trusted provenance must be set by the isolated receiver: %v", marker)
		}
		pipeline := metricsPipelines(t, collector)["metrics/trusted_dcgm"]
		if !containsString(pipeline["receivers"], "prometheus/trusted_dcgm") || !containsString(pipeline["exporters"], "debug") ||
			containsString(pipeline["processors"], "transform/sanitize_provenance_markers") {
			t.Errorf("isolated pipeline must preserve provenance and custom export: %v", pipeline)
		}
		if containsString(pipeline["exporters"], "prometheus") != storage {
			t.Errorf("trusted DCGM built-in export does not match storage=%t: %v", storage, pipeline)
		}
		shared := renderedPrometheusScrapeJobs(t, collector)["dcgm-exporter-accelerator-metrics"]
		sharedRules, _, _ := unstructured.NestedSlice(shared, "metric_relabel_configs")
		if len(sharedRules) != 1 || !strings.Contains(fmt.Sprint(sharedRules[0]), "action:drop") || !strings.Contains(fmt.Sprint(sharedRules[0]), "regex:.+") {
			t.Errorf("shared job must not export duplicate DCGM samples: %v", sharedRules)
		}
	}
}

func TestTrustedDCGMHTTPSConfig(t *testing.T) {
	tls := &v1alpha1.DCGMTLS{CASecretName: "dcgm-ca", ServerName: "dcgm.example.test"}
	collector := renderCollectorTemplateWithTLS(t, true, true, true, 2, nil, tls)
	for receiver, name := range map[string]string{"prometheus": "dcgm-exporter-accelerator-metrics", "prometheus/trusted_dcgm": "trusted-dcgm-exporter"} {
		job := renderedPrometheusScrapeJobsForReceiver(t, collector, receiver)[name]
		tlsConfig, found, err := unstructured.NestedMap(job, "tls_config")
		if err != nil || !found || job["scheme"] != "https" || tlsConfig["ca_file"] != "/etc/otel-collector/dcgm-tls/ca.crt" ||
			tlsConfig["server_name"] != tls.ServerName || tlsConfig["insecure_skip_verify"] == true {
			t.Errorf("receiver %s must verify DCGM TLS with the configured CA and name: %v", receiver, job)
		}
	}
	volumes, found, err := unstructured.NestedSlice(collector.Object, "spec", "volumes")
	if err != nil || !found {
		t.Fatalf("collector volumes missing: %v", err)
	}
	mounts, found, err := unstructured.NestedSlice(collector.Object, "spec", "volumeMounts")
	if err != nil || !found {
		t.Fatalf("collector volume mounts missing: %v", err)
	}
	var volumeFound, mountFound bool
	for _, item := range volumes {
		volume, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("invalid collector volume: %v", item)
		}
		secret, _, _ := unstructured.NestedMap(volume, "secret")
		volumeFound = volumeFound || (volume["name"] == "dcgm-ca" && secret["secretName"] == tls.CASecretName)
	}
	for _, item := range mounts {
		mount, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("invalid collector volume mount: %v", item)
		}
		mountFound = mountFound || (mount["name"] == "dcgm-ca" && mount["mountPath"] == "/etc/otel-collector/dcgm-tls" && mount["readOnly"] == true)
	}
	if !volumeFound || !mountFound {
		t.Errorf("trusted DCGM CA must be mounted read-only: volumes=%v mounts=%v", volumes, mounts)
	}
}

func TestBuiltinDCGMTLSValidation(t *testing.T) {
	disabled := false
	for _, test := range []struct {
		name    string
		metrics *v1alpha1.Metrics
		wantErr bool
	}{
		{"default HTTP", &v1alpha1.Metrics{}, false},
		{"valid HTTPS", &v1alpha1.Metrics{BuiltinDCGMTLS: &v1alpha1.DCGMTLS{CASecretName: "dcgm-ca", ServerName: "dcgm.example.test"}}, false},
		{"missing CA", &v1alpha1.Metrics{BuiltinDCGMTLS: &v1alpha1.DCGMTLS{ServerName: "dcgm.example.test"}}, true},
		{"missing name", &v1alpha1.Metrics{BuiltinDCGMTLS: &v1alpha1.DCGMTLS{CASecretName: "dcgm-ca"}}, true},
		{"malformed name", &v1alpha1.Metrics{BuiltinDCGMTLS: &v1alpha1.DCGMTLS{CASecretName: "dcgm-ca", ServerName: "example.com\nother: true"}}, true},
		{"malformed secret", &v1alpha1.Metrics{BuiltinDCGMTLS: &v1alpha1.DCGMTLS{CASecretName: "../secret", ServerName: "dcgm.example.test"}}, true},
		{"disabled builtin", &v1alpha1.Metrics{BuiltinDCGMScrapeEnabled: &disabled, BuiltinDCGMTLS: &v1alpha1.DCGMTLS{CASecretName: "dcgm-ca", ServerName: "dcgm.example.test"}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := map[string]any{}
			err := addMetricsData(test.metrics, false, data)
			if (err != nil) != test.wantErr {
				t.Fatalf("validation error=%v wantErr=%t", err, test.wantErr)
			}
			if err == nil && test.metrics.BuiltinDCGMTLS != nil && data["BuiltinDCGMTLS"] != test.metrics.BuiltinDCGMTLS {
				t.Errorf("TLS configuration not passed to collector template: %v", data)
			}
		})
	}
}

func TestTrustedDCGMOmittedWhenBuiltinDisabled(t *testing.T) {
	for _, opts := range [][2]bool{{false, false}, {true, false}, {false, true}} {
		collector := renderCollectorTemplateWithOptions(t, opts[0], opts[1])
		_, found, err := unstructured.NestedMap(collector.Object, "spec", "config", "receivers", "prometheus/trusted_dcgm")
		if err != nil || found {
			t.Errorf("trusted receiver must be absent when accelerator=%t builtin=%t: found=%t error=%v", opts[0], opts[1], found, err)
		}
		if _, found := metricsPipelines(t, collector)["metrics/trusted_dcgm"]; found {
			t.Error("trusted pipeline must be absent with the trusted receiver")
		}
	}
}
