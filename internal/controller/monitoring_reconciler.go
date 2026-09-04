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
	"errors"
	"fmt"
	"os"
	"time"

	platformcommon "github.com/opendatahub-io/odh-platform-utilities/api/common"
	"github.com/opendatahub-io/odh-platform-utilities/pkg/controller/gc"
	"github.com/opendatahub-io/odh-platform-utilities/pkg/deploy"
	odhLabels "github.com/opendatahub-io/odh-platform-utilities/pkg/metadata/labels"
	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
	configv1 "github.com/openshift/api/config/v1"
	routev1 "github.com/openshift/api/route/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
)

const (
	monitoringFinalizer = "monitoring.opendatahub.io/cleanup"
	platformType        = "OpenDataHub"
	platformConfigName  = "odh-" + v1alpha1.MonitoringServiceName + "-config"
	platformVersionKey  = "platformVersion"
)

func operatorVersion() string {
	return os.Getenv("OPERATOR_VERSION")
}

// MonitoringReconciler reconciles a Monitoring object.
type MonitoringReconciler struct {
	client.Client

	Scheme          *runtime.Scheme
	Deployer        *deploy.Deployer
	DynamicClient   dynamic.Interface
	DiscoveryClient discovery.DiscoveryInterface
}

// +kubebuilder:rbac:groups=services.platform.opendatahub.io,resources=monitorings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=services.platform.opendatahub.io,resources=monitorings/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=services.platform.opendatahub.io,resources=monitorings/finalizers,verbs=update
// +kubebuilder:rbac:groups=monitoring.rhobs,resources=monitoringstacks;thanosqueriers;servicemonitors;prometheusrules,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tempo.grafana.com,resources=tempomonolithics;tempostacks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=loki.grafana.com,resources=lokistacks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=observability.openshift.io,resources=clusterlogforwarders,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=logging.openshift.io;observability.openshift.io,resources=logs,resourceNames=application,verbs=collect
// +kubebuilder:rbac:groups=serving.kserve.io,resources=inferenceservices;llminferenceservices,verbs=get;list;watch
// +kubebuilder:rbac:groups=opentelemetry.io,resources=opentelemetrycollectors;instrumentations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=perses.dev,resources=perses;persesdatasources;persesdashboards,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=validatingadmissionpolicies;validatingadmissionpolicybindings;mutatingwebhookconfigurations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cert-manager.io,resources=issuers;certificates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings;clusterroles;clusterrolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps;secrets;services;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=namespaces;nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=route.openshift.io,resources=routes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operators.coreos.com,resources=operatorconditions,verbs=get;list;watch
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch
// +kubebuilder:rbac:groups=config.openshift.io,resources=apiservers,verbs=get;list;watch

func (r *MonitoringReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	monitoring := &v1alpha1.Monitoring{}
	if err := r.Get(ctx, req.NamespacedName, monitoring); err != nil {
		if k8serr.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Handle finalizer for cleanup on deletion. The ODH module handler's
	// two-phase cleanup relies on the finalizer keeping the CR alive while
	// deleteAllOwned runs.
	if !monitoring.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(monitoring, monitoringFinalizer) {
			log.Info("Monitoring CR is being deleted, cleaning up owned resources")
			if err := r.deleteAllOwned(ctx, monitoring); err != nil {
				log.Error(err, "Failed to delete owned resources during finalizer cleanup")
				return ctrl.Result{}, err
			}
			patch := client.MergeFrom(monitoring.DeepCopy())
			controllerutil.RemoveFinalizer(monitoring, monitoringFinalizer)
			if err := r.Patch(ctx, monitoring, patch); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(monitoring, monitoringFinalizer) {
		patch := client.MergeFrom(monitoring.DeepCopy())
		controllerutil.AddFinalizer(monitoring, monitoringFinalizer)
		if err := r.Patch(ctx, monitoring, patch); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Snapshot status for patch at end.
	orig := monitoring.DeepCopy()

	result, reconcileErr := r.reconcile(ctx, monitoring)

	// Always patch status.
	if patchErr := r.patchStatus(ctx, orig, monitoring); patchErr != nil {
		log.Error(patchErr, "Failed to patch Monitoring status")
		if reconcileErr != nil {
			return ctrl.Result{}, errors.Join(reconcileErr, patchErr)
		}
		return ctrl.Result{}, patchErr
	}

	return result, reconcileErr
}

func (r *MonitoringReconciler) reconcile(ctx context.Context, monitoring *v1alpha1.Monitoring) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	cm := conditions.NewConditionsManager(monitoring, monitoring.Generation)

	platformVersion, err := r.readPlatformVersion(ctx)
	if err != nil {
		log.Error(err, "Failed to read platform version from ConfigMap", "name", platformConfigName)
		return ctrl.Result{}, err
	}
	stampPlatform := false

	defer func() {
		monitoring.Status.ObservedGeneration = monitoring.Generation
		monitoring.Status.Phase = cm.Phase()
		// Upsert the module identity without replacing the whole releases
		// slice, so a failed reconcile cannot wipe a previously stamped
		// platform version (empty rv is treated as "not gating" by the
		// platform DAG checker).
		monitoring.GetReleaseStatus().SetRelease(platformcommon.ComponentRelease{
			Name:    v1alpha1.MonitoringServiceName,
			RepoURL: "https://github.com/opendatahub-io/odh-observability",
			Version: operatorVersion(),
		})
		if stampPlatform && platformVersion != "" {
			monitoring.GetReleaseStatus().SetPlatformRelease(platformVersion)
		}
	}()

	// Handle Removed state.
	if monitoring.Spec.ManagementState == platformcommon.Removed {
		log.Info("ManagementState is Removed, deleting all owned resources")
		if err := r.deleteAllOwned(ctx, monitoring); err != nil {
			log.Error(err, "Failed to delete owned resources, will retry on next reconcile")
		}
		cm.MarkFalse(string(platformcommon.ConditionTypeReady), "Removed", "Monitoring is in Removed state")
		cm.MarkFalse(string(platformcommon.ConditionTypeProvisioningSucceeded), "Removed", "Monitoring is in Removed state")
		cm.MarkFalse(string(platformcommon.ConditionTypeDegraded), "NotDegraded", "")
		return ctrl.Result{}, nil
	}

	// Check prerequisite operators.
	if err := checkMonitoringPreconditions(ctx, r.Client, monitoring); err != nil {
		cm.MarkFalse(conditions.ConditionMonitoringAvailable,
			conditions.MissingOperatorReason,
			fmt.Sprintf("Monitoring preconditions failed: %s", err.Error()))
		cm.AggregateReady()
		log.Error(err, "Monitoring preconditions failed")
		return ctrl.Result{}, nil
	}
	cm.MarkTrue(conditions.ConditionMonitoringAvailable)

	// Pre-resolve Perses API version once to avoid redundant API calls across
	// the three Perses action functions.
	persesVersion, persesFound, err := resolvePersesAPIVersion(ctx, r.Client)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolving Perses API version: %w", err)
	}

	// Build template data.
	data, err := buildTemplateData(ctx, r.Client, monitoring, persesVersion)
	if err != nil {
		cm.MarkFalse(string(platformcommon.ConditionTypeProvisioningSucceeded), "ConfigurationError", err.Error())
		cm.MarkFalse(string(platformcommon.ConditionTypeReady), "ConfigurationError", err.Error())
		cm.MarkFalse(string(platformcommon.ConditionTypeDegraded), "NotDegraded", "")
		return ctrl.Result{}, nil
	}

	// Run non-Perses action functions to collect template sources.
	var sources []rendertemplate.TemplateSource
	for _, action := range []func(context.Context, client.Client, *v1alpha1.Monitoring, *conditions.ConditionsManager, *[]rendertemplate.TemplateSource) error{
		deployWebhookInfrastructure,
		deployMonitoringAdmissionPolicies,
		deployMonitoringStackWithQuerierAndRestrictions,
		deployTracingStack,
		deployOpenTelemetryCollector,
		deployUsageLogsCollector,
		deployLokiStack,
		deployAlerting,
		deployNodeMetricsEndpoint,
		deployClusterLogForwarder,
	} {
		if err := action(ctx, r.Client, monitoring, cm, &sources); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Perses actions share the pre-resolved API version.
	if err := deployPerses(ctx, r.Client, monitoring, cm, &sources, persesVersion, persesFound); err != nil {
		return ctrl.Result{}, err
	}
	if err := deployPersesTempoIntegration(ctx, r.Client, monitoring, cm, &sources, persesVersion, persesFound); err != nil {
		return ctrl.Result{}, err
	}
	if err := deployPersesPrometheusIntegration(ctx, r.Client, monitoring, cm, &sources, persesVersion, persesFound); err != nil {
		return ctrl.Result{}, err
	}

	// Render all template sources.
	desired, err := rendertemplate.Render(ctx, r.Scheme, sources, data)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("rendering templates: %w", err)
	}

	// Apply desired resources via the deploy library (SSA with caching).
	if err := r.Deployer.Deploy(ctx, deploy.DeployInput{
		Client:    r.Client,
		Owner:     monitoring,
		Release:   deploy.ReleaseInfo{Type: platformType, Version: operatorVersion()},
		Resources: desired,
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("applying resources: %w", err)
	}

	// Garbage-collect stale owned resources via API discovery.
	if err := r.collectGarbage(ctx, monitoring, desired); err != nil {
		log.Error(err, "Garbage collection encountered errors")
	}

	// Sync Prometheus CA ConfigMap → Secret (workaround for COO-1270).
	if err := syncPrometheusWebTLSCA(ctx, r.Client, monitoring); err != nil {
		log.Error(err, "Failed to sync Prometheus web TLS CA")
		cm.MarkFalse(conditions.ConditionMonitoringStackAvailable,
			"PrometheusCAConfigSyncFailed",
			fmt.Sprintf("Failed to sync Prometheus web TLS CA: %v", err))
	}

	// Populate status.url from the Thanos Querier route.
	if err := syncStatusURL(ctx, r.Client, monitoring); err != nil {
		log.Error(err, "Failed to sync status URL")
	}

	// Update usageLogsEndpoint in status only when LokiStack is ready
	requeueNeeded := false
	if monitoring.Spec.UsageLogs != nil && monitoring.Spec.UsageLogs.Storage != nil {
		lokiReady, err := isLokiStackReady(ctx, r.Client, monitoring)
		if err != nil {
			log.Error(err, "Failed to check LokiStack readiness")
			cm.MarkFalse(string(platformcommon.ConditionTypeReady), "LokiStackReadinessCheckFailed", err.Error())
			return ctrl.Result{}, err
		}
		if lokiReady {
			endpoint, _ := data["UsageLogsEndpoint"].(string)
			monitoring.Status.UsageLogsEndpoint = endpoint
		} else {
			monitoring.Status.UsageLogsEndpoint = ""
			requeueNeeded = true // Requeue to check again when LokiStack becomes ready
		}
	} else {
		monitoring.Status.UsageLogsEndpoint = ""
	}

	cm.AggregateReady()

	// Stamp the platform version only after reconciliation has applied
	// desired state without returning an error. Early exits (Removed,
	// preconditions, configuration errors) and error returns leave the
	// previous handshake value untouched.
	stampPlatform = true

	if requeueNeeded {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Periodic requeue when logs are enabled so that namespace discovery
	// picks up new InferenceService/LLMInferenceService deployments without
	// a dedicated watch (the CRDs are optional and a static watch would
	// crash the controller if KServe is not installed).
	if monitoring.Spec.Logs != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
	}

	return ctrl.Result{}, nil
}

// resourceKey identifies a Kubernetes resource for desired-set comparison.
type resourceKey struct {
	gvk       schema.GroupVersionKind
	namespace string
	name      string
}

// collectGarbage deletes owned resources not in the desired set using API discovery.
func (r *MonitoringReconciler) collectGarbage(ctx context.Context, monitoring *v1alpha1.Monitoring, desired []unstructured.Unstructured) error {
	if monitoring.Spec.Namespace == "" {
		return errors.New("monitoring.Spec.Namespace is empty, cannot safely perform garbage collection")
	}

	desiredSet := make(map[resourceKey]struct{}, len(desired))
	for i := range desired {
		obj := &desired[i]
		desiredSet[resourceKey{
			gvk:       obj.GroupVersionKind(),
			namespace: obj.GetNamespace(),
			name:      obj.GetName(),
		}] = struct{}{}
	}

	collector := gc.New(
		gc.WithLabel(odhLabels.PlatformPartOf, "monitoring"),
		gc.InNamespace(monitoring.Spec.Namespace),
		gc.WithDeletePropagationPolicy(metav1.DeletePropagationBackground),
		gc.WithObjectPredicate(func(_ gc.RunParams, obj unstructured.Unstructured) (bool, error) {
			k := resourceKey{
				gvk:       obj.GroupVersionKind(),
				namespace: obj.GetNamespace(),
				name:      obj.GetName(),
			}
			_, inDesired := desiredSet[k]
			return !inDesired, nil
		}),
	)

	return collector.Run(ctx, gc.RunParams{
		Client:          r.Client,
		DynamicClient:   r.DynamicClient,
		DiscoveryClient: r.DiscoveryClient,
		Owner:           monitoring,
		Version:         operatorVersion(),
		PlatformType:    platformType,
	})
}

// deleteAllOwned removes all resources owned by this controller (used on Removed state).
func (r *MonitoringReconciler) deleteAllOwned(ctx context.Context, monitoring *v1alpha1.Monitoring) error {
	if monitoring.Spec.Namespace == "" {
		return errors.New("monitoring.Spec.Namespace is empty, cannot safely delete owned resources")
	}

	collector := gc.New(
		gc.WithLabel(odhLabels.PlatformPartOf, "monitoring"),
		gc.InNamespace(monitoring.Spec.Namespace),
		gc.WithDeletePropagationPolicy(metav1.DeletePropagationBackground),
	)

	return collector.Run(ctx, gc.RunParams{
		Client:          r.Client,
		DynamicClient:   r.DynamicClient,
		DiscoveryClient: r.DiscoveryClient,
		Owner:           monitoring,
		Version:         operatorVersion(),
		PlatformType:    platformType,
	})
}

// patchStatus uses MergePatch to update the status subresource.
func (r *MonitoringReconciler) patchStatus(ctx context.Context, orig, updated *v1alpha1.Monitoring) error {
	patch := client.MergeFrom(orig)
	return r.Status().Patch(ctx, updated, patch)
}

// readPlatformVersion reads the platformVersion from the platform config
// ConfigMap. A missing ConfigMap is standalone mode and returns an empty
// version. Any other Get error (permissions, connectivity) is returned so
// reconcile retries instead of treating it as standalone.
func (r *MonitoringReconciler) readPlatformVersion(ctx context.Context) (string, error) {
	ns := os.Getenv("POD_NAMESPACE")
	if ns == "" {
		return "", nil
	}

	var cm corev1.ConfigMap
	if err := r.Get(ctx, types.NamespacedName{Name: platformConfigName, Namespace: ns}, &cm); err != nil {
		if k8serr.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("reading platform config ConfigMap %s/%s: %w", ns, platformConfigName, err)
	}

	return cm.Data[platformVersionKey], nil
}

func singletonRequests(_ context.Context, _ client.Object) []reconcile.Request {
	return []reconcile.Request{
		{NamespacedName: types.NamespacedName{Name: v1alpha1.MonitoringInstanceName}},
	}
}

// isPlatformConfigMap matches the platform-written handshake ConfigMap by name
// and operator namespace. That ConfigMap is labeled part-of=platform (or
// unlabeled), so it is not covered by the managed-resource predicate. The
// namespace check keeps same-named ConfigMaps in other namespaces from
// enqueueing Monitoring.
func isPlatformConfigMap(obj client.Object) bool {
	ns := os.Getenv("POD_NAMESPACE")
	if ns == "" {
		return false
	}
	return obj.GetName() == platformConfigName && obj.GetNamespace() == ns
}

// SetupWithManager registers the controller with the manager.
func (r *MonitoringReconciler) SetupWithManager(mgr ctrl.Manager) error {
	toSingleton := handler.EnqueueRequestsFromMapFunc(singletonRequests)

	// Enqueue when any resource stamped by this controller changes (drift detection).
	// Monitoring is cluster-scoped so it cannot own namespace-scoped resources via
	// OwnerReferences — label-based Watches replace Owns() here.
	// The prometheus-web-tls-ca ConfigMap is created by our template with this label,
	// so CA rotation events are also covered without a separate watch.
	managedPredicate := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetLabels()[odhLabels.PlatformPartOf] == "monitoring"
	})
	platformConfigPredicate := predicate.NewPredicateFuncs(isPlatformConfigMap)

	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Monitoring{}).
		// Watch managed resources for drift detection.
		Watches(&rbacv1.Role{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&rbacv1.RoleBinding{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&rbacv1.ClusterRole{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&rbacv1.ClusterRoleBinding{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&networkingv1.NetworkPolicy{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&appsv1.Deployment{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&batchv1.Job{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&corev1.ConfigMap{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&corev1.ConfigMap{}, toSingleton, builder.WithPredicates(platformConfigPredicate)).
		Watches(&corev1.Secret{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&corev1.Service{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&corev1.ServiceAccount{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&routev1.Route{}, toSingleton, builder.WithPredicates(managedPredicate)).
		// Watch CRDs to react when optional operators are installed / removed.
		Watches(&extv1.CustomResourceDefinition{}, toSingleton).
		// Reconcile when cluster APIServer TLS profile changes.
		Watches(&configv1.APIServer{}, toSingleton, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Complete(r)
}
