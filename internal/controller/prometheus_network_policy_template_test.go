package controller

import (
	"context"
	"fmt"
	"testing"

	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestPrometheusNetworkPolicyAllowsPrometheusSelfScrape(t *testing.T) {
	resources, err := rendertemplate.Render(context.Background(), nil, []rendertemplate.TemplateSource{{
		FS:   resourcesFS,
		Path: PrometheusNetworkPolicyTemplate,
	}}, map[string]any{
		"Namespace": "redhat-ods-monitoring",
	})
	if err != nil {
		t.Fatalf("Prometheus NetworkPolicy template must render as valid YAML: %v", err)
	}

	var policy unstructured.Unstructured
	for _, resource := range resources {
		kind, _, err := unstructured.NestedString(resource.Object, "kind")
		if err != nil {
			t.Fatalf("failed to read rendered resource kind: %v", err)
		}
		if kind == "NetworkPolicy" {
			policy = resource
			break
		}
	}
	if policy.Object == nil {
		t.Fatal("Prometheus NetworkPolicy template must render a NetworkPolicy")
	}

	ingress, found, err := unstructured.NestedSlice(policy.Object, "spec", "ingress")
	if err != nil || !found {
		t.Fatalf("Prometheus NetworkPolicy ingress must be present: found=%t, error=%v", found, err)
	}

	for _, rawRule := range ingress {
		rule, ok := rawRule.(map[string]any)
		if !ok {
			t.Fatalf("Prometheus NetworkPolicy ingress rule has unexpected type: %T", rawRule)
		}

		from, ok := rule["from"].([]any)
		if !ok {
			continue
		}
		for _, rawPeer := range from {
			peer, ok := rawPeer.(map[string]any)
			if !ok {
				continue
			}
			labels, found, err := unstructured.NestedStringMap(peer, "podSelector", "matchLabels")
			if err != nil || !found || labels["app.kubernetes.io/name"] != "prometheus" ||
				labels["app.kubernetes.io/instance"] != "data-science-monitoringstack" {
				continue
			}

			ports, ok := rule["ports"].([]any)
			if !ok {
				continue
			}
			for _, rawPort := range ports {
				port, ok := rawPort.(map[string]any)
				if ok && fmt.Sprint(port["port"]) == "9090" && port["protocol"] == "TCP" {
					return
				}
			}
		}
	}

	t.Error("Prometheus NetworkPolicy must allow Prometheus pods to scrape Prometheus on TCP port 9090")
}
