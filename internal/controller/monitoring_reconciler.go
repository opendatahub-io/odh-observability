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
	"slices"
	"time"

	platformcommon "github.com/opendatahub-io/odh-platform-utilities/api/common"
	libconditions "github.com/opendatahub-io/odh-platform-utilities/pkg/controller/conditions"
	"github.com/opendatahub-io/odh-platform-utilities/pkg/controller/gc"
	"github.com/opendatahub-io/odh-platform-utilities/pkg/deploy"
	odhLabels "github.com/opendatahub-io/odh-platform-utilities/pkg/metadata/labels"
	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
	configv1 "github.com/openshift/api/config/v1"
	routev1 "github.com/openshift/api/route/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
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
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	controllergvk "github.com/opendatahub-io/odh-observability/internal/controller/gvk"
)

const (
	monitoringFinalizer                   = "monitoring.opendatahub.io/cleanup"
	monitoringPartOf                      = "monitoring"
	platformType                          = "OpenDataHub"
	platformConfigName                    = "odh-" + v1alpha1.MonitoringServiceName + "-config"
	platformVersionKey                    = "platformVersion"
	targetAllocatorSecretRBACName         = "data-science-collector-targetallocator-secrets"
	openTelemetryCollectorComponent       = "opentelemetry-collector"
	openTelemetryTargetAllocatorComponent = "opentelemetry-targetallocator"
	openTelemetryPartOf                   = "opentelemetry"
	collectorOperatorNetworkPolicyName    = "data-science-collector-collector-networkpolicy"
	targetAllocatorOperatorPolicyName     = "data-science-collector-targetallocator-networkpolicy"

	// componentLabel names the subsystem a rendered resource belongs to, so
	// garbage collection can shield one subsystem's resources without having to
	// skip the sweep for everything else. It is deliberately not
	// app.kubernetes.io/component: that key is already used by these templates
	// as a pod selector for operand workloads (alertmanager, lokistack-gateway)
	// and as a workload role on Korrel8r and the querier, neither of which is a
	// subsystem boundary.
	componentLabel = "platform.opendatahub.io/component"

	// The two LokiStacks are two components, not one. They have separate
	// storage, separate conditions and separate endpoints, and withholding one
	// from collection on the other's account would stop the user's deletion of
	// it from taking effect for as long as the other kept failing.
	componentLokiUsageLogs = "loki-usage-logs"
	componentLokiLogs      = "loki-logs"
)

// lokiComponents is every component value a LokiStack-owned resource can
// carry, in the order the reconcile deploys them. The reconcile loop walks it
// to decide which stacks the storage gate left standing, and
// shouldCollectOwnedResource reads it for the by-kind fallback that covers
// resources predating componentLabel.
var lokiComponents = []string{componentLokiUsageLogs, componentLokiLogs}

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
// +kubebuilder:rbac:groups=monitoring.rhobs,resources=monitoringstacks;thanosqueriers;servicemonitors;podmonitors;prometheusrules,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tempo.grafana.com,resources=tempomonolithics;tempostacks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=loki.grafana.com,resources=lokistacks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=observability.openshift.io,resources=clusterlogforwarders,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=logging.openshift.io;observability.openshift.io,resources=logs,resourceNames=application,verbs=collect
// +kubebuilder:rbac:groups=serving.kserve.io,resources=inferenceservices;llminferenceservices,verbs=get;list;watch
// +kubebuilder:rbac:groups=opentelemetry.io,resources=opentelemetrycollectors;instrumentations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=perses.dev,resources=perses;persesdatasources;persesdashboards,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=validatingadmissionpolicies;validatingadmissionpolicybindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings;clusterroles;clusterrolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=podmonitors;servicemonitors,verbs=*
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=scrapeconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps;secrets;services;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=namespaces;nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods;pods/log;endpoints;events;replicationcontrollers,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=daemonsets;replicasets;statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch
// +kubebuilder:rbac:groups=loki.grafana.com,resources=application,resourceNames=logs,verbs=get
// +kubebuilder:rbac:groups=tempo.grafana.com,resources=opendatahub;redhat-ods-monitoring,resourceNames=traces,verbs=get
// +kubebuilder:rbac:groups=route.openshift.io,resources=routes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=route.openshift.io,resources=routes/custom-host,verbs=create
// +kubebuilder:rbac:groups=operators.coreos.com,resources=operatorconditions,verbs=get;list;watch
// +kubebuilder:rbac:groups=olm.operatorframework.io,resources=clusterextensions,verbs=get;list;watch
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch
// +kubebuilder:rbac:groups=config.openshift.io,resources=apiservers,verbs=get;list;watch
// +kubebuilder:rbac:groups=authentication.k8s.io,resources=tokenreviews,verbs=create
// +kubebuilder:rbac:groups=authorization.k8s.io,resources=subjectaccessreviews,verbs=create
// +kubebuilder:rbac:urls=/metrics;/api;/api/*;/apis;/apis/*,verbs=get

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

//nolint:nonamedreturns // The deferred status update needs the error from every return path.
func (r *MonitoringReconciler) reconcile(ctx context.Context, monitoring *v1alpha1.Monitoring) (result ctrl.Result, reconcileErr error) {
	log := logf.FromContext(ctx)
	cm := conditions.NewConditionsManager(monitoring, monitoring.Generation)
	var platformVersion string
	stampPlatform := false

	defer func() {
		if reconcileErr != nil {
			message := reconcileErr.Error()
			cm.MarkFalse(string(platformcommon.ConditionTypeReady), "ReconcileFailed", message)
			cm.MarkFalse(string(platformcommon.ConditionTypeProvisioningSucceeded), "ReconcileFailed", message)
			cm.MarkTrue(string(platformcommon.ConditionTypeDegraded))
		}
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

	var err error
	platformVersion, err = r.readPlatformVersion(ctx)
	if err != nil {
		log.Error(err, "Failed to read platform version from ConfigMap", "name", platformConfigName)
		return ctrl.Result{}, err
	}

	// Handle Removed state.
	if monitoring.Spec.ManagementState == platformcommon.Removed {
		log.Info("ManagementState is Removed, deleting all owned resources")
		if err := r.deleteAllOwned(ctx, monitoring); err != nil {
			log.Error(err, "Failed to delete owned resources during Removed state cleanup")
			return ctrl.Result{}, err
		}
		cm.MarkNotConfigured(conditions.ConditionMonitoringDependenciesReady, "Removed", "Monitoring is in Removed state")
		cm.MarkNotConfigured(conditions.ConditionMonitoringAvailable, "Removed", "Monitoring is in Removed state")
		cm.MarkFalse(string(platformcommon.ConditionTypeReady), "Removed", "Monitoring is in Removed state")
		cm.MarkFalse(string(platformcommon.ConditionTypeProvisioningSucceeded), "Removed", "Monitoring is in Removed state")
		cm.MarkFalse(string(platformcommon.ConditionTypeDegraded), "NotDegraded", "")
		return ctrl.Result{}, nil
	}

	if err := validateMonitoringNamespace(monitoring); err != nil {
		message := err.Error()
		cm.MarkFalse(conditions.ConditionMonitoringAvailable, "MonitoringNamespaceMismatch", message)
		cm.MarkFalse(conditions.ConditionKorrel8rAvailable, "MonitoringNamespaceMismatch", message)
		cm.MarkFalse(string(platformcommon.ConditionTypeProvisioningSucceeded), "MonitoringNamespaceMismatch", message)
		cm.MarkFalse(string(platformcommon.ConditionTypeReady), "MonitoringNamespaceMismatch", message)
		cm.MarkFalse(string(platformcommon.ConditionTypeDegraded), "NotDegraded", "")
		return ctrl.Result{}, nil
	}

	// Check prerequisite operators.
	if err := checkMonitoringPreconditions(ctx, r.Client, monitoring); err != nil {
		var missingOperators *missingOperatorsError
		if !errors.As(err, &missingOperators) {
			return ctrl.Result{}, err
		}
		message := fmt.Sprintf("Required monitoring dependencies are missing: %s", err.Error())
		cm.MarkFalse(conditions.ConditionMonitoringDependenciesReady,
			conditions.MissingOperatorReason,
			message)
		cm.MarkFalse(conditions.ConditionMonitoringAvailable,
			conditions.MissingOperatorReason,
			message)
		cm.AggregateReady()
		log.Error(err, "Monitoring preconditions failed")
		return ctrl.Result{}, nil
	}
	cm.MarkTrue(conditions.ConditionMonitoringDependenciesReady)
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
		if errors.Is(err, errProxyTLSResolutionFailed) {
			return ctrl.Result{}, err
		}
		markConfigurationError(cm, err)

		return ctrl.Result{}, nil
	}

	desired, withheldComponents, storageErr, err := r.renderMonitoringResources(ctx, monitoring, cm, data, persesVersion, persesFound)
	if err != nil {
		return ctrl.Result{}, err
	}

	// The OpenShift service-ca operator fills the injected ConfigMap
	// asynchronously. Apply that ConfigMap first and copy its CA into the
	// Secret before allowing MonitoringStack to create Prometheus pods.
	caReady, err := r.preparePrometheusWebTLSCA(ctx, monitoring, desired)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("preparing Prometheus web TLS CA: %w", err)
	}
	deployResources := desired
	if !caReady {
		cm.MarkFalse(conditions.ConditionMonitoringStackAvailable,
			"PrometheusCAUnavailable", "Waiting for OpenShift service CA injection and Prometheus CA Secret")
		deployResources = withoutMonitoringStack(desired)
	}

	// Apply desired resources via the deploy library (SSA with caching).
	if err := r.Deployer.Deploy(ctx, deploy.DeployInput{
		Client:    r.Client,
		Owner:     monitoring,
		Release:   deploy.ReleaseInfo{Type: platformType, Version: operatorVersion()},
		Resources: deployResources,
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("applying resources: %w", err)
	}
	if err := r.cleanupOpenTelemetryOperatorNetworkPolicies(ctx, monitoringNamespace(monitoring)); err != nil {
		return ctrl.Result{}, fmt.Errorf("cleaning up OpenTelemetry Operator NetworkPolicies: %w", err)
	}

	// Garbage-collect stale owned resources via API discovery. Desired state
	// is already applied, so a failed sweep must not fail the round — the
	// deferred handler would mark the whole Monitoring degraded over leftover
	// cleanup — but neither may it be dropped: the requeue at the end of this
	// function is what guarantees the sweep is retried.
	gcFailed := false
	if err := r.collectGarbage(ctx, monitoring, desired, withheldComponents); err != nil {
		log.Error(err, "Garbage collection encountered errors")
		gcFailed = true
	}

	r.syncPostDeployResources(ctx, monitoring, cm)

	requeueNeeded := publishLokiEndpoints(monitoring, data)

	setUsageLogsForwardingStatus(monitoring, data)

	cm.AggregateReady()

	// Surface the withheld storage configuration now that every other feature
	// has been reconciled and the shielded sweep has run. Returning it at all
	// is what puts the round into backoff rather than reporting success on a
	// spec the operator refused to render.
	if storageErr != nil {
		return ctrl.Result{}, storageErr
	}

	// Stamp the platform version only after reconciliation has applied
	// desired state without returning an error. Early exits (Removed,
	// preconditions, configuration errors) and error returns leave the
	// previous handshake value untouched.
	stampPlatform = caReady

	if !caReady {
		cm.MarkFalse(string(platformcommon.ConditionTypeProvisioningSucceeded),
			"PrometheusCAUnavailable", "Waiting for Prometheus CA Secret before applying MonitoringStack")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if requeueNeeded || usageLogsAwaitingCredential(monitoring) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Periodic requeue when logs are enabled so that namespace discovery
	// picks up new InferenceService/LLMInferenceService deployments without
	// a dedicated watch (the CRDs are optional and a static watch would
	// crash the controller if KServe is not installed).
	if monitoring.Spec.Logs != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
	}

	// Every earlier return after the garbage collection sweep either already
	// requeues (CA, endpoints, logs) or returns an error and lands in backoff.
	// Bare success is the one path that would strand a failed sweep until some
	// unrelated watch event fires, so it carries the retry here — without it,
	// stale owned resources could outlive their desired state indefinitely
	// while status reports success.
	if gcFailed {
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	return ctrl.Result{}, nil
}

// cleanupOpenTelemetryOperatorNetworkPolicies removes policies left behind by
// the OpenTelemetry Operator when its NetworkPolicy generation is disabled.
// The operator owns these objects through the TargetAllocator/collector, so
// the module garbage collector cannot safely identify them as its own stale
// resources. Deleting only the well-known, label-verified objects avoids
// touching user-created policies.
func (r *MonitoringReconciler) cleanupOpenTelemetryOperatorNetworkPolicies(ctx context.Context, namespace string) error {
	policies := []struct {
		name      string
		component string
	}{
		{name: collectorOperatorNetworkPolicyName, component: openTelemetryCollectorComponent},
		{name: targetAllocatorOperatorPolicyName, component: openTelemetryTargetAllocatorComponent},
	}

	for _, item := range policies {
		policy := &networkingv1.NetworkPolicy{}
		err := r.Get(ctx, types.NamespacedName{Name: item.name, Namespace: namespace}, policy)
		if k8serr.IsNotFound(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("getting %s/%s: %w", namespace, item.name, err)
		}
		if !isOpenTelemetryOperatorNetworkPolicy(policy, namespace, item.component) {
			continue
		}
		if err := r.Delete(ctx, policy); err != nil && !k8serr.IsNotFound(err) {
			return fmt.Errorf("deleting %s/%s: %w", namespace, item.name, err)
		}
	}
	return nil
}

func isOpenTelemetryOperatorNetworkPolicy(policy *networkingv1.NetworkPolicy, namespace, component string) bool {
	labels := policy.GetLabels()
	return labels["app.kubernetes.io/component"] == component &&
		labels["app.kubernetes.io/instance"] == namespace+".data-science-collector" &&
		labels["app.kubernetes.io/managed-by"] == opentelemetryOperator &&
		labels["app.kubernetes.io/part-of"] == openTelemetryPartOf
}

func isOpenTelemetryOperatorNetworkPolicyEvent(obj client.Object) bool {
	policy, ok := obj.(*networkingv1.NetworkPolicy)
	namespace := getEnvOrDefault("MONITORING_NAMESPACE", defaultMonitoringNamespace)
	if !ok || policy.Namespace != namespace {
		return false
	}
	switch policy.Name {
	case collectorOperatorNetworkPolicyName:
		return isOpenTelemetryOperatorNetworkPolicy(policy, namespace, openTelemetryCollectorComponent)
	case targetAllocatorOperatorPolicyName:
		return isOpenTelemetryOperatorNetworkPolicy(policy, namespace, openTelemetryTargetAllocatorComponent)
	default:
		return false
	}
}

func (r *MonitoringReconciler) renderMonitoringResources(
	ctx context.Context,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
	data map[string]any,
	persesVersion string,
	persesFound bool,
) ([]unstructured.Unstructured, map[string]struct{}, error, error) {
	// Run the action functions to collect template sources. The three Perses
	// ones are closures because they also take the pre-resolved API version;
	// they are otherwise ordinary actions and belong in the same list.
	var sources []rendertemplate.TemplateSource
	// The usage logs collector and the ClusterLogForwarder are not in this
	// list: each deploys from inside its LokiStack's action, behind the
	// storage gate, so a withheld pipeline is skipped as one unit.
	for _, action := range []func(context.Context, client.Client, *v1alpha1.Monitoring, *conditions.ConditionsManager, *[]rendertemplate.TemplateSource) error{
		deployWebhookInfrastructure,
		deployMonitoringAdmissionPolicies,
		deployMonitoringStackWithQuerierAndRestrictions,
		deployTracingStack,
		deployOpenTelemetryCollector,
		deployAlerting,
		deployOperatorMonitoring,
		deployNodeMetricsEndpoint,
		deployKorrel8r,
		func(ctx context.Context, c client.Client, m *v1alpha1.Monitoring,
			cm *conditions.ConditionsManager, sources *[]rendertemplate.TemplateSource) error {
			return deployPerses(ctx, c, m, cm, sources, persesVersion, persesFound)
		},
		func(ctx context.Context, c client.Client, m *v1alpha1.Monitoring,
			cm *conditions.ConditionsManager, sources *[]rendertemplate.TemplateSource) error {
			return deployPersesTempoIntegration(ctx, c, m, cm, sources, persesVersion, persesFound)
		},
		func(ctx context.Context, c client.Client, m *v1alpha1.Monitoring,
			cm *conditions.ConditionsManager, sources *[]rendertemplate.TemplateSource) error {
			return deployPersesPrometheusIntegration(ctx, c, m, cm, sources, persesVersion, persesFound)
		},
	} {
		if err := action(ctx, r.Client, monitoring, cm, &sources); err != nil {
			return nil, nil, nil, err
		}
	}

	// The storage gate runs ahead of every claimant it reports on. Its error
	// is held rather than returned: a rejected storage configuration is a
	// verdict on the spec, not a failure of the reconcile, so Deploy, the
	// shielded sweep, the endpoints and the Ready aggregation all still run.
	withheldComponents, storageErr := validateStorageIsolation(ctx, r.Client, monitoring, cm)

	if err := deployLokiStacks(ctx, r.Client, monitoring, cm, &sources, withheldComponents); err != nil {
		return nil, nil, nil, err
	}

	desired, err := rendertemplate.Render(ctx, r.Scheme, sources, data)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("rendering templates: %w", err)
	}
	return desired, withheldComponents, storageErr, nil
}

func (r *MonitoringReconciler) preparePrometheusWebTLSCA(
	ctx context.Context,
	monitoring *v1alpha1.Monitoring,
	desired []unstructured.Unstructured,
) (bool, error) {
	stackFound := false
	var caConfigMap *unstructured.Unstructured
	namespace := monitoringNamespace(monitoring)
	for i := range desired {
		obj := &desired[i]
		if obj.GroupVersionKind() == controllergvk.MonitoringStack && obj.GetNamespace() == namespace {
			stackFound = true
		}
		if obj.GroupVersionKind() == controllergvk.ConfigMap && obj.GetNamespace() == namespace && obj.GetName() == prometheusWebTLSCAConfigMapName {
			caConfigMap = obj
		}
	}
	if !stackFound {
		return true, nil
	}
	if caConfigMap == nil {
		return false, errors.New("MonitoringStack requires the Prometheus CA ConfigMap template")
	}
	if err := r.Deployer.Deploy(ctx, deploy.DeployInput{
		Client:    r.Client,
		Owner:     monitoring,
		Release:   deploy.ReleaseInfo{Type: platformType, Version: operatorVersion()},
		Resources: []unstructured.Unstructured{*caConfigMap},
	}); err != nil {
		return false, fmt.Errorf("applying Prometheus CA ConfigMap: %w", err)
	}
	return syncPrometheusWebTLSCA(ctx, r.Client, monitoring)
}

func withoutMonitoringStack(desired []unstructured.Unstructured) []unstructured.Unstructured {
	filtered := make([]unstructured.Unstructured, 0, len(desired))
	for _, obj := range desired {
		if obj.GroupVersionKind() != controllergvk.MonitoringStack {
			filtered = append(filtered, obj)
		}
	}
	return filtered
}

// publishLokiEndpoints records the endpoint of each LokiStack on the status,
// and reports whether a short requeue is needed to pick up one that is not
// ready yet.
//
// An endpoint is published only once its stack reports ready. The stack's own
// deploy already observed that readiness and recorded it on the condition, so
// this reads the condition back instead of re-fetching either CR. The two
// stacks are evaluated separately here for the same reason they deploy
// separately: one being unobservable says nothing about the other. LokiStack
// is not watched (only core kinds are), hence the requeue: nothing else would
// bring the reconciler back when a stack finishes coming up.
//
// Clearing the endpoint of a stack that is confirmed not ready is deliberate.
// These fields are what other components dial, and a stale address outlives the
// stack it pointed at.
//
// A stack whose readiness could not be observed is the one case that holds its
// endpoint instead. The condition is Unknown there, meaning nothing was learned
// about the stack — and the published endpoint is load-bearing: clearing
// Status.UsageLogsEndpoint makes deployUsageLogsCollector return early, which
// drops the collector and its policies from the desired set and has garbage
// collection delete them. A single apiserver timeout should not tear down a
// healthy pipeline.
func publishLokiEndpoints(monitoring *v1alpha1.Monitoring, data map[string]any) bool {
	requeueNeeded := false

	publish := func(configured bool, condition, dataKey string, endpoint *string) {
		switch {
		case !configured:
			// The user turned the feature off: clear regardless of condition.
			*endpoint = ""
		case libconditions.IsStatusConditionTrue(monitoring, condition):
			*endpoint, _ = data[dataKey].(string)
		case libconditions.IsStatusConditionPresentAndEqual(monitoring, condition, metav1.ConditionUnknown):
			// Could not observe: hold the last known endpoint. On a first
			// reconcile that holds "", which is what it already was.
			//
			// Every Unknown that reaches here today arrives with an error
			// behind it — the stack action returned one and aborted before
			// this ran, or the storage gate held one and returns below — and
			// both get controller-runtime backoff rather than this requeue.
			// Set it anyway so an Unknown that arrives without an error is
			// still polled instead of waiting for the periodic round.
			requeueNeeded = true
		default:
			*endpoint = ""
			requeueNeeded = true
		}
	}

	// The usage logs collector writes to its stack, so it needs the OTLP
	// ingestion endpoint. Nothing writes to the log forwarding stack by URL —
	// the ClusterLogForwarder addresses it by name — so that one publishes the
	// gateway query endpoint instead.
	publish(
		monitoring.Spec.UsageLogs != nil && monitoring.Spec.UsageLogs.Storage != nil,
		conditions.ConditionUsageLogsLokiStackAvailable,
		"UsageLogsEndpoint",
		&monitoring.Status.UsageLogsEndpoint,
	)
	publish(
		monitoring.Spec.Logs != nil,
		conditions.ConditionLokiStackAvailable,
		"LokiQueryEndpoint",
		&monitoring.Status.LogsEndpoint,
	)

	return requeueNeeded
}

func (r *MonitoringReconciler) syncPostDeployResources(
	ctx context.Context,
	monitoring *v1alpha1.Monitoring,
	cm *conditions.ConditionsManager,
) {
	log := logf.FromContext(ctx)

	if err := syncThanosQuerierRouteDestinationCA(ctx, r.Client, monitoring); err != nil {
		log.Error(err, "Failed to sync Thanos Querier Route destination CA")
		cm.MarkFalse(conditions.ConditionMonitoringStackAvailable,
			"ThanosCAConfigSyncFailed",
			fmt.Sprintf("Failed to sync Thanos Querier Route destination CA: %v", err))
	}

	// Populate status.url from the Thanos Querier route.
	if err := syncStatusURL(ctx, r.Client, monitoring); err != nil {
		log.Error(err, "Failed to sync status URL")
	}
}

// resourceKey identifies a Kubernetes resource for desired-set comparison.
type resourceKey struct {
	gvk       schema.GroupVersionKind
	namespace string
	name      string
}

// collectGarbage deletes owned resources not in the desired set using API
// discovery. Resources belonging to a withheld component are left alone: their
// subsystem did not get to contribute to the desired set this round, so their
// absence from it says nothing about whether they are still wanted.
func (r *MonitoringReconciler) collectGarbage(
	ctx context.Context,
	monitoring *v1alpha1.Monitoring,
	desired []unstructured.Unstructured,
	withheldComponents map[string]struct{},
) error {
	namespace := monitoringNamespace(monitoring)

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
		gc.WithLabel(odhLabels.PlatformPartOf, monitoringPartOf),
		gc.InNamespace(namespace),
		gc.WithDeletePropagationPolicy(metav1.DeletePropagationBackground),
		gc.WithObjectPredicate(func(_ gc.RunParams, obj unstructured.Unstructured) (bool, error) {
			return shouldCollectOwnedResource(
				monitoring, obj, desiredSet, namespace, withheldComponents), nil
		}),
	)

	params := gc.RunParams{
		Client:          r.Client,
		DynamicClient:   r.DynamicClient,
		DiscoveryClient: r.DiscoveryClient,
		Owner:           monitoring,
		Version:         operatorVersion(),
		PlatformType:    platformType,
	}
	if err := collector.Run(ctx, params); err != nil {
		return err
	}

	return r.collectTargetAllocatorRBACGarbage(ctx, params, desiredSet, namespace)
}

// deleteAllOwned removes all resources owned by this controller (used on Removed state).
func (r *MonitoringReconciler) deleteAllOwned(ctx context.Context, monitoring *v1alpha1.Monitoring) error {
	namespace := monitoringNamespace(monitoring)

	collector := gc.New(
		gc.WithLabel(odhLabels.PlatformPartOf, monitoringPartOf),
		gc.InNamespace(namespace),
		gc.WithDeletePropagationPolicy(metav1.DeletePropagationBackground),
		gc.WithObjectPredicate(func(_ gc.RunParams, obj unstructured.Unstructured) (bool, error) {
			return isOwnedByMonitoring(monitoring, &obj) && isInMonitoringScope(&obj, namespace), nil
		}),
	)

	params := gc.RunParams{
		Client:          r.Client,
		DynamicClient:   r.DynamicClient,
		DiscoveryClient: r.DiscoveryClient,
		Owner:           monitoring,
		Version:         operatorVersion(),
		PlatformType:    platformType,
	}
	if err := collector.Run(ctx, params); err != nil {
		return err
	}

	return r.collectTargetAllocatorRBACGarbage(ctx, params, nil, namespace)
}

func (r *MonitoringReconciler) collectTargetAllocatorRBACGarbage(
	ctx context.Context,
	params gc.RunParams,
	desiredSet map[resourceKey]struct{},
	namespace string,
) error {
	collector := gc.New(
		gc.WithLabel(odhLabels.PlatformPartOf, monitoringPartOf),
		// Role and RoleBinding listing remains cluster-wide, but the
		// SelfSubjectRulesReview used by the collector requires a namespace.
		gc.InNamespace(namespace),
		gc.WithDeletePropagationPolicy(metav1.DeletePropagationBackground),
		gc.WithTypePredicate(func(_ gc.RunParams, gvk schema.GroupVersionKind) (bool, error) {
			return gvk == controllergvk.Role || gvk == controllergvk.RoleBinding, nil
		}),
		gc.WithObjectPredicate(func(_ gc.RunParams, obj unstructured.Unstructured) (bool, error) {
			return shouldCollectTargetAllocatorRBAC(params.Owner, obj, desiredSet), nil
		}),
	)

	return collector.Run(ctx, params)
}

func shouldCollectOwnedResource(
	monitoring client.Object,
	obj unstructured.Unstructured,
	desiredSet map[resourceKey]struct{},
	namespace string,
	withheldComponents map[string]struct{},
) bool {
	if !isOwnedByMonitoring(monitoring, &obj) || !isInMonitoringScope(&obj, namespace) {
		return false
	}

	if _, isWithheld := withheldComponents[obj.GetLabels()[componentLabel]]; isWithheld {
		return false
	}

	// A LokiStack created before componentLabel existed does not carry it, so an
	// upgrade landing straight into a storage conflict would not be shielded by
	// the label alone. LokiStack is the one resource here that cannot be
	// recreated without losing logs, and its GVK belongs to no other component,
	// so shield it by kind as well. The sibling NetworkPolicies are stateless
	// and come back with the next clean apply, so they get no such fallback.
	//
	// Any Loki component being withheld is the right trigger, coarse as it is.
	// An unlabelled stack cannot say which component it belongs to, and its
	// name does not settle it either: data-science-lokistack was the usage
	// logs stack before this split and is the log forwarding stack after it.
	// Shielding one stack for a round longer than strictly necessary costs a
	// delayed deletion; guessing wrong costs the logs.
	if obj.GroupVersionKind() == controllergvk.LokiStack {
		for _, component := range lokiComponents {
			if _, lokiWithheld := withheldComponents[component]; lokiWithheld {
				return false
			}
		}
	}

	k := resourceKey{
		gvk:       obj.GroupVersionKind(),
		namespace: obj.GetNamespace(),
		name:      obj.GetName(),
	}
	_, inDesired := desiredSet[k]
	return !inDesired
}

func shouldCollectTargetAllocatorRBAC(
	monitoring client.Object,
	obj unstructured.Unstructured,
	desiredSet map[resourceKey]struct{},
) bool {
	if !isTargetAllocatorSecretRBAC(obj) || !isOwnedByMonitoring(monitoring, &obj) {
		return false
	}

	k := resourceKey{
		gvk:       obj.GroupVersionKind(),
		namespace: obj.GetNamespace(),
		name:      obj.GetName(),
	}
	_, inDesired := desiredSet[k]
	return !inDesired
}

func isOwnedByMonitoring(monitoring client.Object, obj metav1.Object) bool {
	if monitoring.GetUID() == "" {
		return false
	}
	controller := metav1.GetControllerOf(obj)
	return controller != nil && controller.UID == monitoring.GetUID()
}

func isInMonitoringScope(obj metav1.Object, namespace string) bool {
	return obj.GetNamespace() == "" || obj.GetNamespace() == namespace || obj.GetNamespace() == clusterLoggingNamespace
}

func isTargetAllocatorSecretRBAC(obj unstructured.Unstructured) bool {
	return obj.GetName() == targetAllocatorSecretRBACName &&
		(obj.GroupVersionKind() == controllergvk.Role || obj.GroupVersionKind() == controllergvk.RoleBinding)
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

// validateMonitoringNamespace ensures the controller's RBAC was installed for
// the same operand namespace selected by the Monitoring CR. The Helm chart
// injects MONITORING_NAMESPACE from its monitoringNamespace value; an unset
// value keeps standalone controller tests and non-Helm development workflows
// permissive.
func validateMonitoringNamespace(monitoring *v1alpha1.Monitoring) error {
	configured := os.Getenv("MONITORING_NAMESPACE")
	if configured == "" || monitoringNamespace(monitoring) == configured {
		return nil
	}
	return fmt.Errorf("effective Monitoring namespace %q does not match configured monitoring namespace %q", monitoringNamespace(monitoring), configured)
}

func singletonRequests(_ context.Context, _ client.Object) []reconcile.Request {
	return []reconcile.Request{
		{NamespacedName: types.NamespacedName{Name: v1alpha1.MonitoringInstanceName}},
	}
}

// storageSecretRequests enqueues Monitoring when one of the LokiStack storage
// Secrets named in its spec changes.
//
// Those Secrets belong to the user, so they carry no part-of label and the
// managed-resource predicate never matches them. Without this, a Secret
// arriving after the Monitoring CR — the ordinary case for GitOps, where apply
// order is not guaranteed — is not noticed until the periodic requeue, and the
// storage isolation check fails closed for that whole window.
//
// The names come from the spec rather than a label, so this has to be a map
// function: a predicate sees only the Secret.
func (r *MonitoringReconciler) storageSecretRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	monitoring := &v1alpha1.Monitoring{}
	if err := r.Get(ctx, types.NamespacedName{Name: v1alpha1.MonitoringInstanceName}, monitoring); err != nil {
		// Including NotFound: the CR is gone or not yet cached, so there is
		// nothing to enqueue and nothing worth logging on an event stream that
		// carries every Secret in the cluster.
		return nil
	}

	if obj.GetNamespace() != monitoringNamespace(monitoring) {
		return nil
	}

	// UsageLogs and Logs are each optional; Storage is required only within
	// whichever of them is present.
	name := obj.GetName()
	usageLogs := monitoring.Spec.UsageLogs
	logs := monitoring.Spec.Logs
	matches := (usageLogs != nil && usageLogs.Storage != nil && usageLogs.Storage.SecretName == name) ||
		(logs != nil && logs.Storage != nil && logs.Storage.SecretName == name)
	if !matches {
		return nil
	}

	return singletonRequests(ctx, obj)
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

// markConfigurationError reports a template-data failure that leaves the service
// unconfigurable. A rejected usage-log exporter is attributed to its own condition as
// well: the failure aborts the reconcile before the collector action runs, so otherwise
// that condition keeps whatever the previous pass left — True, if the exporter was added
// to a cluster that was already forwarding, which is a green condition over a dead
// pipeline.
func markConfigurationError(cm *conditions.ConditionsManager, err error) {
	if errors.Is(err, errUsageLogsExporterInvalid) {
		cm.MarkFalse(conditions.ConditionUsageLogsCollectorAvailable, "ConfigurationError", err.Error())
	}

	cm.MarkFalse(string(platformcommon.ConditionTypeProvisioningSucceeded), "ConfigurationError", err.Error())
	cm.MarkFalse(string(platformcommon.ConditionTypeReady), "ConfigurationError", err.Error())
	cm.MarkFalse(string(platformcommon.ConditionTypeDegraded), "NotDegraded", "")
}

// setUsageLogsForwardingStatus publishes the exporters the collector actually carries.
// Gated on the collector having been rendered, so status cannot advertise forwarding
// from a cluster where it was withheld — for a credential that never arrived, say.
// Kept out of reconcile so that function stays under the cyclomatic complexity limit.
func setUsageLogsForwardingStatus(monitoring *v1alpha1.Monitoring, data map[string]any) {
	monitoring.Status.UsageLogsForwarding = nil

	condition := findUsageLogsCollectorCondition(monitoring)
	if condition == nil || condition.Status != metav1.ConditionTrue {
		return
	}

	if names, ok := data["UsageLogsExporterNames"].([]string); ok && len(names) > 0 {
		monitoring.Status.UsageLogsForwarding = slices.Clone(names)
	}
}

func findUsageLogsCollectorCondition(monitoring *v1alpha1.Monitoring) *platformcommon.Condition {
	for i := range monitoring.Status.Conditions {
		if monitoring.Status.Conditions[i].Type == conditions.ConditionUsageLogsCollectorAvailable {
			return &monitoring.Status.Conditions[i]
		}
	}

	return nil
}

// usageLogsAwaitingCredential reports whether the usage-log collector was withheld
// this pass for a credential Secret that has not arrived. No watch brings us back for
// it: ACM's config-policy-controller writes that Secret without the labels the
// managed-resource predicates match, so the reconcile has to return on its own.
func usageLogsAwaitingCredential(monitoring *v1alpha1.Monitoring) bool {
	condition := findUsageLogsCollectorCondition(monitoring)

	return condition != nil &&
		condition.Status == metav1.ConditionFalse &&
		condition.Reason == conditions.CredentialNotFoundReason
}

func isPrometheusWebTLSCASecret(obj client.Object) bool {
	return obj.GetName() == prometheusWebTLSCAConfigMapName &&
		obj.GetNamespace() == getEnvOrDefault("MONITORING_NAMESPACE", defaultMonitoringNamespace)
}

func isKorrel8rEndpointSlice(obj client.Object) bool {
	serviceName := obj.GetLabels()[discoveryv1.LabelServiceName]
	if obj.GetNamespace() == metav1.NamespaceDefault && serviceName == kubernetesServiceName {
		return true
	}
	return obj.GetNamespace() == getEnvOrDefault("MONITORING_NAMESPACE", defaultMonitoringNamespace) &&
		serviceName == Korrel8rServiceName
}

func namespaceWatchPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool { return true },
		DeleteFunc: func(event.DeleteEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldNamespace, oldOK := e.ObjectOld.(*corev1.Namespace)
			newNamespace, newOK := e.ObjectNew.(*corev1.Namespace)
			if !oldOK || !newOK {
				return false
			}
			return (oldNamespace.DeletionTimestamp != nil) != (newNamespace.DeletionTimestamp != nil) ||
				isSystemNamespace(oldNamespace) != isSystemNamespace(newNamespace)
		},
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
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
		return obj.GetLabels()[odhLabels.PlatformPartOf] == monitoringPartOf
	})
	// An in-flight OpenTelemetry reconcile can recreate a legacy policy after
	// cleanup. Observe those policies too so cleanup does not depend on an
	// unrelated Monitoring update or periodic log-discovery reconciliation.
	networkPolicyPredicate := predicate.Or(managedPredicate,
		predicate.NewPredicateFuncs(isOpenTelemetryOperatorNetworkPolicyEvent))
	platformConfigPredicate := predicate.NewPredicateFuncs(isPlatformConfigMap)

	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Monitoring{}).
		// Watch managed resources for drift detection.
		Watches(&rbacv1.Role{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&rbacv1.RoleBinding{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&rbacv1.ClusterRole{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&rbacv1.ClusterRoleBinding{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&networkingv1.NetworkPolicy{}, toSingleton, builder.WithPredicates(networkPolicyPredicate)).
		Watches(&appsv1.Deployment{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&batchv1.Job{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&corev1.ConfigMap{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&corev1.ConfigMap{}, toSingleton, builder.WithPredicates(platformConfigPredicate)).
		Watches(&corev1.Secret{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&corev1.Secret{}, toSingleton, builder.WithPredicates(predicate.NewPredicateFuncs(isPrometheusWebTLSCASecret))).
		// The LokiStack storage Secrets are the user's, so they carry no
		// part-of label and the predicate above skips them. They gate the
		// storage isolation check, which fails closed until both resolve.
		// Delete events matter as much as creates for that reason. The Secret
		// informer already exists for the watch above, so this costs an event
		// handler rather than a second cache.
		Watches(&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.storageSecretRequests),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{})).
		Watches(&corev1.Service{}, toSingleton, builder.WithPredicates(managedPredicate)).
		Watches(&discoveryv1.EndpointSlice{}, toSingleton, builder.WithPredicates(predicate.NewPredicateFuncs(isKorrel8rEndpointSlice))).
		Watches(&corev1.ServiceAccount{}, toSingleton, builder.WithPredicates(managedPredicate)).
		// Namespace creation or deletion changes the TargetAllocator Secret Role allowlist.
		Watches(&corev1.Namespace{}, toSingleton, builder.WithPredicates(namespaceWatchPredicate())).
		Watches(&routev1.Route{}, toSingleton, builder.WithPredicates(managedPredicate)).
		// Watch CRDs to react when optional operators are installed / removed.
		Watches(&extv1.CustomResourceDefinition{}, toSingleton).
		// Reconcile when cluster APIServer TLS profile changes.
		Watches(&configv1.APIServer{}, toSingleton, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Complete(r)
}
