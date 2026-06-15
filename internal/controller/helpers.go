/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"encoding/base64"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	routev1 "github.com/openshift/api/route/v1"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
)

const fieldManager = "odh-observability-controller"

// hasCRD checks whether a CRD for the given GVK is installed.
// Uses a cheap List to avoid the retry backoff of CustomResourceDefinitionExists.
func hasCRD(ctx context.Context, c client.Client, g schema.GroupVersionKind) (bool, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   g.Group,
		Version: g.Version,
		Kind:    g.Kind + "List",
	})

	if err := c.List(ctx, list, &client.ListOptions{Limit: 1}); err != nil {
		if meta.IsNoMatchError(err) || errors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("hasCRD: %w", err)
	}

	return true, nil
}

// syncPrometheusWebTLSCA copies the service-ca.crt from the prometheus-web-tls-ca ConfigMap
// into a same-named Secret so MonitoringStack can consume it.
// This is a workaround until COO-1270 allows MonitoringStack to read CA from a ConfigMap.
func syncPrometheusWebTLSCA(ctx context.Context, c client.Client, monitoring *v1alpha1.Monitoring) error {
	log := logf.FromContext(ctx).WithName("syncPrometheusWebTLSCA")

	if monitoring.Spec.Metrics == nil {
		return nil
	}

	namespace := monitoring.Spec.Namespace

	var configMap unstructured.Unstructured
	configMap.SetAPIVersion("v1")
	configMap.SetKind("ConfigMap")

	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: "prometheus-web-tls-ca"}, &configMap); err != nil {
		if errors.IsNotFound(err) {
			log.V(1).Info("CA ConfigMap not found yet, will sync when created")
			return nil
		}
		return fmt.Errorf("failed to get CA ConfigMap: %w", err)
	}

	data, found, err := unstructured.NestedStringMap(configMap.Object, "data")
	if err != nil {
		return fmt.Errorf("failed to extract data from ConfigMap: %w", err)
	}
	if !found {
		log.V(1).Info("ConfigMap data field not found, service-ca operator may not have injected CA yet")
		return nil
	}

	caCert, found := data["service-ca.crt"]
	if !found || caCert == "" {
		log.V(1).Info("service-ca.crt not found in ConfigMap, service-ca operator may not have injected CA yet")
		return nil
	}

	secret := &unstructured.Unstructured{}
	secret.SetAPIVersion("v1")
	secret.SetKind("Secret")
	secret.SetNamespace(namespace)
	secret.SetName("prometheus-web-tls-ca")
	// Do not label with PlatformPartOf — the Secret is not rendered via templates
	// and is not in the GC desired set, so the label would cause GC to delete it
	// every reconcile. The source ConfigMap has the label for Watch-based drift detection.

	if err := unstructured.SetNestedField(secret.Object, "Opaque", "type"); err != nil {
		return fmt.Errorf("failed to set secret type: %w", err)
	}
	// Use the `data` field with base64-encoded value so SSA field ownership is
	// consistent with what the API server actually stores (it converts stringData
	// to data on admission, creating a field-manager mismatch on subsequent applies).
	encoded := base64.StdEncoding.EncodeToString([]byte(caCert))
	if err := unstructured.SetNestedField(secret.Object, map[string]any{"service-ca.crt": encoded}, "data"); err != nil {
		return fmt.Errorf("failed to set secret data: %w", err)
	}

	if err := c.Patch(ctx, secret, client.Apply, client.FieldOwner(fieldManager), client.ForceOwnership); err != nil {
		return fmt.Errorf("failed to apply CA Secret: %w", err)
	}

	log.Info("Synced CA from ConfigMap to Secret", "namespace", namespace)
	return nil
}

const thanosQuerierRouteName = "data-science-thanos-querier-route"

// syncStatusURL fetches the Thanos Querier route and updates monitoring.Status.URL.
// When metrics are not configured the URL is cleared.
func syncStatusURL(ctx context.Context, c client.Client, monitoring *v1alpha1.Monitoring) {
	if monitoring.Spec.Metrics == nil {
		monitoring.Status.URL = ""
		return
	}

	log := logf.FromContext(ctx).WithName("syncStatusURL")
	route := &routev1.Route{}
	if err := c.Get(ctx, client.ObjectKey{
		Namespace: monitoring.Spec.Namespace,
		Name:      thanosQuerierRouteName,
	}, route); err != nil {
		if !errors.IsNotFound(err) {
			log.Error(err, "Failed to fetch Thanos Querier route for status URL")
		}
		monitoring.Status.URL = ""
		return
	}

	if len(route.Status.Ingress) > 0 && route.Status.Ingress[0].Host != "" {
		monitoring.Status.URL = "https://" + route.Status.Ingress[0].Host
	}
}
