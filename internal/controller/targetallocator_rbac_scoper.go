package controller

import (
	"context"
	"errors"
	"fmt"

	platformcommon "github.com/opendatahub-io/odh-platform-utilities/api/common"
	"github.com/opendatahub-io/operator-rbac-toolkit/pkg/crossns"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
)

const (
	targetAllocatorRBACScoperFinalizer       = "monitoring.opendatahub.io/rbac-scoper-cleanup"
	TargetAllocatorRBACScoperOwnerLabelKey   = "app.kubernetes.io/name"
	TargetAllocatorRBACScoperOwnerLabelValue = "odh-observability-targetallocator-rbac"
	rbacVerbGet                              = "get"
	rbacVerbList                             = "list"
	rbacVerbWatch                            = "watch"
	rbacScoperManagedLabelKey                = "operator-rbac-toolkit.io/managed"
	rbacScoperManagedLabelValue              = "true"
	rbacKindClusterRole                      = "ClusterRole"
	collectorServiceAccountName              = "data-science-collector-collector"
	collectorClusterRoleBindingName          = "generate-processors-collector-rolebinding"
	collectorClusterRoleName                 = "generate-processors-role"
	targetAllocatorLegacyBindingName         = "generate-processors-targetallocator-rolebinding"
	targetAllocatorDiscoveryBindingName      = "generate-processors-targetallocator-discovery-rolebinding"
	targetAllocatorDiscoveryClusterRoleName  = "generate-processors-targetallocator-role"
	mlflowTraceExportBindingName             = "data-science-collector-mlflow-trace-export"
	tempoTraceExportBindingName              = "data-science-collector-tempo-trace-export"
	usageLogsProcessorBindingName            = "data-science-usage-logs-processor"
)

// TargetAllocatorRBACScoper moves the TargetAllocator Secret RoleBindings into
// a separate ServiceAccount trust domain. The manager keeps its operational
// permissions but does not create these cross-namespace RBAC objects.
type TargetAllocatorRBACScoper struct {
	client.Client

	Reconciler *crossns.Reconciler
}

type clusterRoleBindingSpec struct {
	name             string
	roleRef          string
	subjectKind      string
	subjectName      string
	subjectNamespace string
}

func (r *TargetAllocatorRBACScoper) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	monitoring := &v1alpha1.Monitoring{}
	if err := r.Get(ctx, req.NamespacedName, monitoring); err != nil {
		if k8serr.IsNotFound(err) {
			return ctrl.Result{}, r.Reconciler.Teardown(ctx)
		}
		return ctrl.Result{}, err
	}

	if !monitoring.DeletionTimestamp.IsZero() {
		if err := r.Reconciler.Teardown(ctx); err != nil {
			return ctrl.Result{}, fmt.Errorf("tearing down TargetAllocator RBAC: %w", err)
		}
		if controllerutil.ContainsFinalizer(monitoring, targetAllocatorRBACScoperFinalizer) {
			patch := client.MergeFrom(monitoring.DeepCopy())
			controllerutil.RemoveFinalizer(monitoring, targetAllocatorRBACScoperFinalizer)
			if err := r.Patch(ctx, monitoring, patch); err != nil {
				return ctrl.Result{}, fmt.Errorf("removing TargetAllocator RBAC scoper finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(monitoring, targetAllocatorRBACScoperFinalizer) {
		patch := client.MergeFrom(monitoring.DeepCopy())
		controllerutil.AddFinalizer(monitoring, targetAllocatorRBACScoperFinalizer)
		if err := r.Patch(ctx, monitoring, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("adding TargetAllocator RBAC scoper finalizer: %w", err)
		}
	}

	if monitoring.Spec.ManagementState == platformcommon.Removed {
		if err := r.reconcileClusterRoleBindings(ctx, nil); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Reconciler.Teardown(ctx); err != nil {
			return ctrl.Result{}, fmt.Errorf("tearing down TargetAllocator RBAC: %w", err)
		}
		return ctrl.Result{}, nil
	}

	if err := r.reconcileClusterRoleBindings(ctx, desiredClusterRoleBindings(monitoring)); err != nil {
		return ctrl.Result{}, err
	}

	if monitoring.Spec.Metrics == nil {
		return ctrl.Result{}, r.Reconciler.Teardown(ctx)
	}

	namespaces, err := listAllNamespaces(ctx, r.Client, monitoringNamespace(monitoring))
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("listing TargetAllocator Secret namespaces: %w", err)
	}

	ruleSets := make([]crossns.RuleSet, 0, len(namespaces))
	for _, namespace := range namespaces {
		ruleSets = append(ruleSets, crossns.RuleSet{
			RoleName:  targetAllocatorSecretRBACName,
			Namespace: namespace,
			Rules: []rbacv1.PolicyRule{{
				APIGroups: []string{""},
				Resources: []string{"secrets"},
				Verbs:     []string{rbacVerbGet, rbacVerbList, rbacVerbWatch},
			}},
		})
	}

	if err := r.Reconciler.Apply(ctx, crossns.SubjectRef{
		Name:      targetAllocatorServiceAccountName,
		Namespace: monitoringNamespace(monitoring),
	}, ruleSets); err != nil {
		return ctrl.Result{}, fmt.Errorf("applying TargetAllocator RBAC: %w", err)
	}

	if err := r.removeLegacyTargetAllocatorBindings(ctx); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func desiredClusterRoleBindings(monitoring *v1alpha1.Monitoring) []clusterRoleBindingSpec {
	namespace := monitoringNamespace(monitoring)
	metrics := monitoring.Spec.Metrics != nil
	metricsStorage := metrics && monitoring.Spec.Metrics.Storage != nil
	traces := monitoring.Spec.Traces != nil
	tracesStorage := traces && monitoring.Spec.Traces.Storage != nil
	logs := monitoring.Spec.Logs != nil
	usageLogs := monitoring.Spec.UsageLogs != nil && monitoring.Spec.UsageLogs.Storage != nil

	var bindings []clusterRoleBindingSpec
	if metrics || traces {
		bindings = append(bindings, clusterRoleBindingSpec{
			name:             collectorClusterRoleBindingName,
			roleRef:          collectorClusterRoleName,
			subjectKind:      rbacv1.ServiceAccountKind,
			subjectName:      collectorServiceAccountName,
			subjectNamespace: namespace,
		})
	}

	if metrics {
		bindings = append(bindings,
			clusterRoleBindingSpec{
				name:             targetAllocatorLegacyBindingName,
				roleRef:          collectorClusterRoleName,
				subjectKind:      rbacv1.ServiceAccountKind,
				subjectName:      targetAllocatorServiceAccountName,
				subjectNamespace: namespace,
			},
			clusterRoleBindingSpec{
				name:             targetAllocatorDiscoveryBindingName,
				roleRef:          targetAllocatorDiscoveryClusterRoleName,
				subjectKind:      rbacv1.ServiceAccountKind,
				subjectName:      targetAllocatorServiceAccountName,
				subjectNamespace: namespace,
			})
	}
	if metricsStorage {
		bindings = append(bindings, clusterRoleBindingSpec{
			name:             "data-science-monitoringstack-alertmanager-prometheus-metrics-reader",
			roleRef:          "cluster-monitoring-view",
			subjectKind:      rbacv1.ServiceAccountKind,
			subjectName:      "data-science-monitoringstack-alertmanager",
			subjectNamespace: namespace,
		})
	}
	if traces {
		bindings = append(bindings, clusterRoleBindingSpec{
			name:             mlflowTraceExportBindingName,
			roleRef:          mlflowTraceExportBindingName,
			subjectKind:      rbacv1.ServiceAccountKind,
			subjectName:      collectorServiceAccountName,
			subjectNamespace: namespace,
		})
	}
	if tracesStorage {
		bindings = append(bindings, clusterRoleBindingSpec{
			name:             tempoTraceExportBindingName,
			roleRef:          tempoTraceExportBindingName,
			subjectKind:      rbacv1.ServiceAccountKind,
			subjectName:      collectorServiceAccountName,
			subjectNamespace: namespace,
		})
	}
	if logs {
		bindings = append(bindings,
			clusterRoleBindingSpec{
				name:             "data-science-cluster-log-forwarder-collector-collect-app-logs",
				roleRef:          "collect-application-logs",
				subjectKind:      rbacv1.ServiceAccountKind,
				subjectName:      clusterLogForwarderResourceName + "-collector",
				subjectNamespace: clusterLoggingNamespace,
			},
			clusterRoleBindingSpec{
				name:             "data-science-cluster-log-forwarder-collector-loki-writer",
				roleRef:          "cluster-logging-write-application-logs",
				subjectKind:      rbacv1.ServiceAccountKind,
				subjectName:      clusterLogForwarderResourceName + "-collector",
				subjectNamespace: clusterLoggingNamespace,
			})
	}
	if metrics || traces || logs {
		bindings = append(bindings,
			clusterRoleBindingSpec{
				name:             Korrel8rServiceName + "-query",
				roleRef:          Korrel8rServiceName + "-query",
				subjectKind:      rbacv1.ServiceAccountKind,
				subjectName:      Korrel8rServiceName,
				subjectNamespace: namespace,
			},
			clusterRoleBindingSpec{
				name:             Korrel8rServiceName + "-auth-delegator",
				roleRef:          "system:auth-delegator",
				subjectKind:      rbacv1.ServiceAccountKind,
				subjectName:      Korrel8rServiceName,
				subjectNamespace: namespace,
			})
	}
	if usageLogs {
		bindings = append(bindings, clusterRoleBindingSpec{
			name:             usageLogsProcessorBindingName,
			roleRef:          usageLogsProcessorBindingName,
			subjectKind:      rbacv1.ServiceAccountKind,
			subjectName:      "data-science-usage-logs-collector",
			subjectNamespace: namespace,
		})
	}
	return bindings
}

func (r *TargetAllocatorRBACScoper) reconcileClusterRoleBindings(
	ctx context.Context,
	specs []clusterRoleBindingSpec,
) error {
	desired := make(map[string]clusterRoleBindingSpec, len(specs))
	for _, spec := range specs {
		desired[spec.name] = spec
		if err := r.applyClusterRoleBinding(ctx, spec); err != nil {
			return err
		}
	}

	bindings := &rbacv1.ClusterRoleBindingList{}
	if err := r.List(ctx, bindings, r.managedLabels()); err != nil {
		return fmt.Errorf("listing managed ClusterRoleBindings: %w", err)
	}
	for i := range bindings.Items {
		binding := bindings.Items[i]
		if _, found := desired[binding.Name]; found {
			continue
		}
		if err := r.Delete(ctx, &binding); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("deleting stale managed ClusterRoleBinding %s: %w", binding.Name, err)
		}
	}
	return nil
}

func (r *TargetAllocatorRBACScoper) applyClusterRoleBinding(ctx context.Context, spec clusterRoleBindingSpec) error {
	labels := r.managedLabels()
	subject := rbacv1.Subject{
		Kind:      spec.subjectKind,
		Name:      spec.subjectName,
		Namespace: spec.subjectNamespace,
	}
	roleRef := rbacv1.RoleRef{
		APIGroup: rbacv1.GroupName,
		Kind:     rbacKindClusterRole,
		Name:     spec.roleRef,
	}

	existing := &rbacv1.ClusterRoleBinding{}
	err := r.Get(ctx, client.ObjectKey{Name: spec.name}, existing)
	if k8serr.IsNotFound(err) {
		return r.Create(ctx, &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: spec.name, Labels: labels},
			RoleRef:    roleRef,
			Subjects:   []rbacv1.Subject{subject},
		})
	}
	if err != nil {
		return fmt.Errorf("getting ClusterRoleBinding %s: %w", spec.name, err)
	}

	if existing.RoleRef != roleRef {
		if err := r.Delete(ctx, existing); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("deleting drifted ClusterRoleBinding %s: %w", spec.name, err)
		}
		binding := existing.DeepCopy()
		binding.ResourceVersion = ""
		binding.RoleRef = roleRef
		binding.Subjects = []rbacv1.Subject{subject}
		binding.Labels = labels
		return r.Create(ctx, binding)
	}

	changed := false
	if len(existing.Subjects) != 1 || existing.Subjects[0] != subject {
		existing.Subjects = []rbacv1.Subject{subject}
		changed = true
	}
	if existing.Labels == nil {
		existing.Labels = map[string]string{}
	}
	for key, value := range labels {
		if existing.Labels[key] != value {
			existing.Labels[key] = value
			changed = true
		}
	}
	if changed {
		return r.Update(ctx, existing)
	}
	return nil
}

func (r *TargetAllocatorRBACScoper) managedLabels() client.MatchingLabels {
	return client.MatchingLabels{
		rbacScoperManagedLabelKey:              rbacScoperManagedLabelValue,
		TargetAllocatorRBACScoperOwnerLabelKey: TargetAllocatorRBACScoperOwnerLabelValue,
	}
}

func (r *MonitoringReconciler) verifyRBACScoper(
	ctx context.Context,
	monitoring *v1alpha1.Monitoring,
	data map[string]any,
) error {
	if !targetAllocatorRBACScoperEnabled() {
		return nil
	}

	for _, spec := range desiredClusterRoleBindings(monitoring) {
		binding := &rbacv1.ClusterRoleBinding{}
		if err := r.Get(ctx, client.ObjectKey{Name: spec.name}, binding); err != nil {
			return fmt.Errorf("getting managed ClusterRoleBinding %s: %w", spec.name, err)
		}
		if !isManagedByTargetAllocatorRBACScoper(binding) {
			return fmt.Errorf("ClusterRoleBinding %s is not managed by the RBAC scoper", spec.name)
		}
	}

	if monitoring.Spec.Metrics == nil {
		return nil
	}
	namespaces, ok := data["TargetAllocatorSecretNamespaces"].([]string)
	if !ok {
		return errors.New("TargetAllocator Secret namespace set is unavailable")
	}
	for _, namespace := range namespaces {
		binding := &rbacv1.RoleBinding{}
		err := r.Get(ctx, types.NamespacedName{
			Namespace: namespace,
			Name:      targetAllocatorSecretRBACName + "-binding",
		}, binding)
		if err != nil {
			return fmt.Errorf("getting TargetAllocator Secret RoleBinding in namespace %s: %w", namespace, err)
		}
		if binding.Labels[crossns.ManagedLabelKey] != crossns.ManagedLabelValue {
			return fmt.Errorf("TargetAllocator Secret RoleBinding in namespace %s is not toolkit managed", namespace)
		}
	}
	return nil
}

func (r *TargetAllocatorRBACScoper) removeLegacyTargetAllocatorBindings(ctx context.Context) error {
	bindings := &rbacv1.RoleBindingList{}
	if err := r.List(ctx, bindings); err != nil {
		return fmt.Errorf("listing legacy TargetAllocator RoleBindings: %w", err)
	}

	for i := range bindings.Items {
		binding := bindings.Items[i]
		if binding.Name != targetAllocatorSecretRBACName {
			continue
		}
		if err := r.Delete(ctx, &binding); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("deleting legacy TargetAllocator RoleBinding %s/%s: %w", binding.Namespace, binding.Name, err)
		}
	}
	return nil
}

func (r *MonitoringReconciler) cleanupTargetAllocatorScoperRBAC(ctx context.Context) error {
	labels := client.MatchingLabels{
		crossns.ManagedLabelKey:                crossns.ManagedLabelValue,
		TargetAllocatorRBACScoperOwnerLabelKey: TargetAllocatorRBACScoperOwnerLabelValue,
	}

	roles := &rbacv1.RoleList{}
	if err := r.List(ctx, roles, labels); err != nil {
		return fmt.Errorf("listing toolkit TargetAllocator Roles: %w", err)
	}
	for i := range roles.Items {
		role := roles.Items[i]
		if role.Name != targetAllocatorSecretRBACName {
			continue
		}
		if err := r.Delete(ctx, &role); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("deleting toolkit TargetAllocator Role %s/%s: %w", role.Namespace, role.Name, err)
		}
	}

	bindings := &rbacv1.RoleBindingList{}
	if err := r.List(ctx, bindings, labels); err != nil {
		return fmt.Errorf("listing toolkit TargetAllocator RoleBindings: %w", err)
	}
	for i := range bindings.Items {
		binding := bindings.Items[i]
		if binding.Name != targetAllocatorSecretRBACName+"-binding" {
			continue
		}
		if err := r.Delete(ctx, &binding); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("deleting toolkit TargetAllocator RoleBinding %s/%s: %w", binding.Namespace, binding.Name, err)
		}
	}
	return nil
}

func (r *TargetAllocatorRBACScoper) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Monitoring{}).
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(r.monitoringRequests)).
		Watches(&rbacv1.RoleBinding{}, handler.EnqueueRequestsFromMapFunc(r.managedRBACRequests)).
		Watches(&rbacv1.ClusterRoleBinding{}, handler.EnqueueRequestsFromMapFunc(r.managedRBACRequests)).
		Complete(r)
}

func (r *TargetAllocatorRBACScoper) managedRBACRequests(ctx context.Context, object client.Object) []reconcile.Request {
	if !isManagedByTargetAllocatorRBACScoper(object) {
		return nil
	}
	return r.monitoringRequests(ctx, object)
}

func isManagedByTargetAllocatorRBACScoper(object client.Object) bool {
	labels := object.GetLabels()
	return labels[rbacScoperManagedLabelKey] == rbacScoperManagedLabelValue &&
		labels[TargetAllocatorRBACScoperOwnerLabelKey] == TargetAllocatorRBACScoperOwnerLabelValue
}

func (r *TargetAllocatorRBACScoper) monitoringRequests(ctx context.Context, _ client.Object) []reconcile.Request {
	monitoringList := &v1alpha1.MonitoringList{}
	if err := r.List(ctx, monitoringList); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(monitoringList.Items))
	for i := range monitoringList.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&monitoringList.Items[i]),
		})
	}
	return requests
}

var _ reconcile.Reconciler = (*TargetAllocatorRBACScoper)(nil)
