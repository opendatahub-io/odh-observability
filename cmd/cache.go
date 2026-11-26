/*
Copyright 2026.

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

package main

import (
	"fmt"
	"os"

	odhcache "github.com/opendatahub-io/odh-platform-utilities/pkg/cache"
	odhLabels "github.com/opendatahub-io/odh-platform-utilities/pkg/metadata/labels"
	routev1 "github.com/openshift/api/route/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	moncontroller "github.com/opendatahub-io/odh-observability/internal/controller"
)

const (
	defaultOperatorName = "odh-observability"
	kubernetesSvcName   = "kubernetes"
)

func managerCacheOptionsFromEnv() (cache.Options, error) {
	operatorName := os.Getenv("OPERATOR_NAME")
	if operatorName == "" {
		operatorName = defaultOperatorName
	}
	return managerCacheOptions(
		os.Getenv("MONITORING_NAMESPACE"),
		os.Getenv("POD_NAMESPACE"),
		operatorName,
	)
}

// managerCacheOptions restricts controller-runtime informer caches so cluster-wide
// user resources (such as ConfigMaps, Secrets, Services, and Roles) cannot bloat
// memory or trigger OOMKill, while keeping every resource read or watched by the
// reconciler and admission webhook visible.
func managerCacheOptions(monitoringNS, podNS, operatorName string) (cache.Options, error) {
	if operatorName == "" {
		operatorName = defaultOperatorName
	}

	monitoringSelector := labels.SelectorFromSet(labels.Set{
		odhLabels.PlatformPartOf: moncontroller.MonitoringPartOf,
	})
	managedOnly := cache.ByObject{Label: monitoringSelector}

	// Role and RoleBinding remain cluster-wide because TargetAllocator's secret
	// Roles/RoleBindings are rendered into every non-system namespace and listed
	// cluster-wide during GC. The metadata.name field selector keeps
	// tenant-created objects carrying the part-of=monitoring label out of the
	// cache: data-science-collector-targetallocator-secrets is the only
	// namespaced RBAC name the controller renders, reads, or collects.
	targetAllocatorRBACOnly := cache.ByObject{
		Label: monitoringSelector,
		Field: fields.OneTermEqualSelector("metadata.name", moncontroller.TargetAllocatorSecretRBACName),
	}

	monitoringNSUnfiltered := monitoringNamespaceUnfilteredCacheByObject(monitoringNS, monitoringSelector)

	endpointSliceCache, err := endpointSliceCacheByObject(monitoringNS)
	if err != nil {
		return cache.Options{}, err
	}

	byObject := map[client.Object]cache.ByObject{
		// See targetAllocatorRBACOnly above for why Role and RoleBinding stay
		// cluster-wide but name- and label-restricted.
		&rbacv1.Role{}:               targetAllocatorRBACOnly,
		&rbacv1.RoleBinding{}:        targetAllocatorRBACOnly,
		&rbacv1.ClusterRole{}:        managedOnly,
		&rbacv1.ClusterRoleBinding{}: managedOnly,
		&appsv1.Deployment{}:         monitoringNSUnfiltered,
		&batchv1.Job{}:               jobCacheByObject(monitoringNS, monitoringSelector),
		&corev1.Service{}:            monitoringNSUnfiltered,
		&corev1.ServiceAccount{}:     serviceAccountCacheByObject(monitoringNS, monitoringSelector),
		&routev1.Route{}:             monitoringNSUnfiltered,
		&corev1.ConfigMap{}:          configMapCacheByObject(monitoringNS, podNS, monitoringSelector),
		&discoveryv1.EndpointSlice{}: endpointSliceCache,
	}

	// Secret and NetworkPolicy watches/lookups include unlabeled resources in the
	// effective monitoring namespace (user storage/credential Secrets, the
	// service-CA Secret, and legacy OTel Operator NetworkPolicies). When
	// MONITORING_NAMESPACE is unset (standalone/local dev), Monitoring.spec.namespace
	// is unconstrained, so only scope those caches when MONITORING_NAMESPACE is known.
	if monitoringNS != "" {
		byObject[&corev1.Secret{}] = secretCacheByObject(monitoringNS, podNS, operatorName+"-webhook-cert")
		byObject[&networkingv1.NetworkPolicy{}] = networkPolicyCacheByObject(monitoringNS, monitoringSelector)
	}

	return cache.Options{
		DefaultTransform: odhcache.StripUnusedFields(),
		ByObject:         byObject,
	}, nil
}

// monitoringNamespaceUnfilteredCacheByObject caches all objects of the given
// type within monitoringNS when MONITORING_NAMESPACE is set so opted-out
// unlabeled singletons (opendatahub.io/managed: "false") remain readable from
// the cache without bypassing it via APIReader. When MONITORING_NAMESPACE is
// unset (standalone/local dev), it falls back to cluster-wide label filtering.
func monitoringNamespaceUnfilteredCacheByObject(monitoringNS string, monitoringSelector labels.Selector) cache.ByObject {
	if monitoringNS == "" {
		return cache.ByObject{Label: monitoringSelector}
	}
	return cache.ByObject{
		Namespaces: map[string]cache.Config{
			monitoringNS: {},
		},
	}
}

func jobCacheByObject(monitoringNS string, monitoringSelector labels.Selector) cache.ByObject {
	if monitoringNS == "" {
		return cache.ByObject{Label: monitoringSelector}
	}
	return cache.ByObject{
		Namespaces: map[string]cache.Config{
			monitoringNS: {LabelSelector: monitoringSelector},
		},
	}
}

// serviceAccountCacheByObject restricts labeled ServiceAccount informers to
// monitoringNS and openshift-logging (log-forwarder SA) so user namespaces
// carrying a copied part-of=monitoring label cannot populate the cache.
func serviceAccountCacheByObject(monitoringNS string, monitoringSelector labels.Selector) cache.ByObject {
	if monitoringNS == "" {
		return cache.ByObject{Label: monitoringSelector}
	}
	namespaces := map[string]cache.Config{
		monitoringNS: {LabelSelector: monitoringSelector},
	}
	if monitoringNS != moncontroller.ClusterLoggingNamespace {
		namespaces[moncontroller.ClusterLoggingNamespace] = cache.Config{
			LabelSelector: monitoringSelector,
		}
	}
	return cache.ByObject{Namespaces: namespaces}
}

// configMapCacheByObject caches monitoringNS ConfigMaps (including unlabeled
// opted-out ConfigMaps and single-namespace installs where POD_NAMESPACE ==
// MONITORING_NAMESPACE) plus only odh-monitoring-config in podNS when split.
func configMapCacheByObject(monitoringNS, podNS string, monitoringSelector labels.Selector) cache.ByObject {
	if monitoringNS == "" {
		if podNS == "" {
			return cache.ByObject{Label: monitoringSelector}
		}
		return cache.ByObject{
			Namespaces: map[string]cache.Config{
				cache.AllNamespaces: {LabelSelector: monitoringSelector},
				podNS:               {},
			},
		}
	}
	namespaces := map[string]cache.Config{
		monitoringNS: {},
	}
	if podNS != "" && podNS != monitoringNS {
		namespaces[podNS] = cache.Config{
			FieldSelector: fields.OneTermEqualSelector("metadata.name", moncontroller.PlatformConfigName),
		}
	}
	return cache.ByObject{Namespaces: namespaces}
}

func secretCacheByObject(monitoringNS, podNS, webhookCertSecretName string) cache.ByObject {
	namespaces := map[string]cache.Config{
		monitoringNS: {},
	}
	if podNS != "" && podNS != monitoringNS {
		namespaces[podNS] = cache.Config{
			FieldSelector: fields.OneTermEqualSelector("metadata.name", webhookCertSecretName),
		}
	}
	return cache.ByObject{Namespaces: namespaces}
}

func networkPolicyCacheByObject(monitoringNS string, monitoringSelector labels.Selector) cache.ByObject {
	namespaces := map[string]cache.Config{
		monitoringNS: {},
	}
	if monitoringNS != moncontroller.ClusterLoggingNamespace {
		namespaces[moncontroller.ClusterLoggingNamespace] = cache.Config{
			LabelSelector: monitoringSelector,
		}
	}
	return cache.ByObject{Namespaces: namespaces}
}

// endpointSliceCacheByObject restricts EndpointSlices to the default namespace
// ("kubernetes" service, read by addKorrel8rAPIServerData) and monitoringNS
// ("korrel8r" service, read by korrel8rReady). If MONITORING_NAMESPACE is set to
// "default", both service names are combined in the single "default" entry.
func endpointSliceCacheByObject(monitoringNS string) (cache.ByObject, error) {
	if monitoringNS == "" || monitoringNS == metav1.NamespaceDefault {
		req, err := labels.NewRequirement(
			discoveryv1.LabelServiceName,
			selection.In,
			[]string{kubernetesSvcName, moncontroller.Korrel8rServiceName},
		)
		if err != nil {
			return cache.ByObject{}, fmt.Errorf("building EndpointSlice label requirement: %w", err)
		}
		selector := labels.NewSelector().Add(*req)
		if monitoringNS == "" {
			return cache.ByObject{Label: selector}, nil
		}
		return cache.ByObject{
			Namespaces: map[string]cache.Config{
				metav1.NamespaceDefault: {LabelSelector: selector},
			},
		}, nil
	}

	return cache.ByObject{
		Namespaces: map[string]cache.Config{
			metav1.NamespaceDefault: {
				LabelSelector: labels.SelectorFromSet(labels.Set{
					discoveryv1.LabelServiceName: kubernetesSvcName,
				}),
			},
			monitoringNS: {
				LabelSelector: labels.SelectorFromSet(labels.Set{
					discoveryv1.LabelServiceName: moncontroller.Korrel8rServiceName,
				}),
			},
		},
	}, nil
}
