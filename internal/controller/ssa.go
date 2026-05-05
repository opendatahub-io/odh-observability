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
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
)

const (
	fieldManager = "odh-observability-controller"
	// partOfLabel is the label key used to track resources owned by this controller.
	partOfLabel = "platform.opendatahub.io/part-of"
	// partOfValue is the label value for all resources owned by this controller.
	partOfValue = "monitoring"
	// instanceNameAnnotation records which Monitoring CR owns the resource.
	instanceNameAnnotation = "platform.opendatahub.io/instance.name"
)

// applyResources applies all desired resources to the cluster using Server-Side Apply.
// Each resource gets the part-of label and instance-name annotation stamped on it.
func applyResources(ctx context.Context, c client.Client, owner *v1alpha1.Monitoring, desired []unstructured.Unstructured) error {
	log := logf.FromContext(ctx)
	for i := range desired {
		obj := &desired[i]

		// Stamp ownership labels and annotations.
		labels := obj.GetLabels()
		if labels == nil {
			labels = make(map[string]string)
		}
		labels[partOfLabel] = partOfValue
		obj.SetLabels(labels)

		annotations := obj.GetAnnotations()
		if annotations == nil {
			annotations = make(map[string]string)
		}
		annotations[instanceNameAnnotation] = owner.Name
		obj.SetAnnotations(annotations)

		log.V(1).Info("Applying resource", "gvk", obj.GroupVersionKind(), "name", obj.GetName(), "namespace", obj.GetNamespace())

		if err := c.Patch(ctx, obj, client.Apply, client.FieldOwner(fieldManager), client.ForceOwnership); err != nil {
			return fmt.Errorf("failed to apply %s %s/%s: %w", obj.GetKind(), obj.GetNamespace(), obj.GetName(), err)
		}
	}
	return nil
}

// garbageCollect deletes owned resources that are no longer in the desired set.
// It iterates over all GVKs that the controller manages and removes resources
// labelled with part-of=monitoring that are absent from desired.
func garbageCollect(ctx context.Context, c client.Client, desired []unstructured.Unstructured) error {
	log := logf.FromContext(ctx)

	// Build a lookup set of desired resources.
	type key struct {
		gvk       schema.GroupVersionKind
		namespace string
		name      string
	}
	desiredSet := make(map[key]struct{}, len(desired))
	for i := range desired {
		obj := &desired[i]
		desiredSet[key{
			gvk:       obj.GroupVersionKind(),
			namespace: obj.GetNamespace(),
			name:      obj.GetName(),
		}] = struct{}{}
	}

	// All GVKs whose resources may need GC.
	allGVKs := []schema.GroupVersionKind{
		gvk.MonitoringStack,
		gvk.ThanosQuerier,
		gvk.TempoMonolithic,
		gvk.TempoStack,
		gvk.OpenTelemetryCollector,
		gvk.Instrumentation,
		gvk.ServiceMonitor,
		gvk.PrometheusRule,
		gvk.PersesV1Alpha1,
		gvk.PersesV1Alpha2,
		gvk.PersesDatasourceV1Alpha1,
		gvk.PersesDatasourceV1Alpha2,
		gvk.PersesDashboardV1Alpha1,
		gvk.PersesDashboardV1Alpha2,
		gvk.ValidatingAdmissionPolicy,
		gvk.ValidatingAdmissionPolicyBinding,
		// Core Kubernetes resource types.
		{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "Role"},
		{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "RoleBinding"},
		{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRole"},
		{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRoleBinding"},
		{Group: "networking.k8s.io", Version: "v1", Kind: "NetworkPolicy"},
		{Group: "apps", Version: "v1", Kind: "Deployment"},
		{Group: "batch", Version: "v1", Kind: "Job"},
		{Group: "", Version: "v1", Kind: "ConfigMap"},
		{Group: "", Version: "v1", Kind: "Secret"},
		{Group: "", Version: "v1", Kind: "Service"},
		{Group: "", Version: "v1", Kind: "ServiceAccount"},
		{Group: "route.openshift.io", Version: "v1", Kind: "Route"},
	}

	selector := client.MatchingLabels{partOfLabel: partOfValue}

	for _, g := range allGVKs {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   g.Group,
			Version: g.Version,
			Kind:    g.Kind + "List",
		})

		if err := c.List(ctx, list, selector); err != nil {
			if errors.IsNotFound(err) || meta.IsNoMatchError(err) {
				continue // CRD not installed — nothing to GC
			}
			log.Error(err, "Failed to list resources for GC", "gvk", g)
			continue
		}

		for i := range list.Items {
			obj := &list.Items[i]
			k := key{
				gvk:       obj.GroupVersionKind(),
				namespace: obj.GetNamespace(),
				name:      obj.GetName(),
			}
			if _, ok := desiredSet[k]; !ok {
				log.Info("Garbage collecting resource", "gvk", g, "name", obj.GetName(), "namespace", obj.GetNamespace())
				if err := c.Delete(ctx, obj); err != nil && !errors.IsNotFound(err) {
					log.Error(err, "Failed to delete stale resource", "gvk", g, "name", obj.GetName())
				}
			}
		}
	}

	return nil
}

// deleteAllOwned removes all resources owned by this controller (used on Removed state).
func deleteAllOwned(ctx context.Context, c client.Client) error {
	return garbageCollect(ctx, c, nil) // empty desired → delete everything
}

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
	secret.SetLabels(map[string]string{partOfLabel: partOfValue})

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
