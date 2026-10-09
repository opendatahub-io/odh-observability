package e2e_test

import (
	"fmt"
	"testing"
	"time"

	gTypes "github.com/onsi/gomega/types"
	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	"github.com/opendatahub-io/odh-platform-utilities/pkg/cluster/olm"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega matchers
)

// Constants for monitoring resource names.
const (
	MonitoringStackName               = "data-science-monitoringstack"
	OpenTelemetryCollectorName        = "data-science-collector"
	TargetAllocatorDeploymentName     = "data-science-collector-targetallocator"
	CollectorServiceAccount           = "data-science-collector-collector"
	TargetAllocatorServiceAccount     = "data-science-collector-targetallocator"
	TempoMonolithicName               = "data-science-tempomonolithic"
	TempoStackName                    = "data-science-tempostack"
	InstrumentationName               = "data-science-instrumentation"
	ThanosQuerierName                 = "data-science-thanos-querier"
	ThanosQuerierRouteName            = "data-science-thanos-querier-route"
	ThanosQuerierProxyName            = "data-science-thanos-querier-proxy"
	PersesName                        = "data-science-perses"
	PersesDatasourceName              = "data-science-prometheus-datasource"
	ClusterPrometheusDatasourceName   = "cluster-prometheus-datasource"
	ClusterPrometheusDatasourceSecret = "cluster-prometheus-datasource-secret"
	UsageLogsCollectorName            = "data-science-usage-logs"
	UsageLogsCollectorServiceAccount  = "data-science-usage-logs-collector"
	LokiStackName                     = "data-science-lokistack"
)

func cooOperatorChannel() string {
	if testOpts.cooChannel != "" {
		return testOpts.cooChannel
	}
	return observabilityOpChannel
}

// OLM operator constants for dependent operators.
const (
	observabilityOpName      = "cluster-observability-operator"
	observabilityOpNamespace = "openshift-cluster-observability-operator"
	observabilityOpChannel   = "stable"

	tempoOpName      = "tempo-product"
	tempoOpNamespace = "openshift-tempo-operator"
	tempoOpChannel   = "stable"

	opentelemetryOpName      = "opentelemetry-product"
	opentelemetryOpNamespace = "openshift-opentelemetry-operator"
	opentelemetryOpChannel   = "stable"

	certManagerOpName      = "openshift-cert-manager-operator"
	certManagerOpNamespace = "cert-manager-operator"
	certManagerOpChannel   = "stable-v1"

	leaderWorkerSetOpName      = "leader-worker-set"
	leaderWorkerSetOpNamespace = "openshift-lws-operator"
	leaderWorkerSetOpChannel   = "stable-v1.0"

	connectivityLinkOpName      = "rhcl-operator"
	connectivityLinkOpNamespace = "openshift-operators"
	connectivityLinkOpChannel   = "stable"
	connectivityLinkOpSource    = "redhat-operators"
	kuadrantResourceNamespace   = "kuadrant-system"

	lokiOpName      = "loki-operator"
	lokiOpNamespace = "openshift-operators-redhat"
)

// Constants for common test values.
const (
	DefaultRetention       = "5m"
	FormattedRetention     = "5m0s"
	MetricsStorageSize     = "1Gi"
	MetricsRetention       = "1h"
	OtlpCustomExporter     = "otlp/custom"
	OtlpHttpCustomExporter = "otlphttp/custom"
	OtlpTempoExporter      = "otlp/tempo"
	MetricsCPURequest      = "100m"
	MetricsMemoryRequest   = "256Mi"

	TracesStorageBackendPV  = "pv"
	TracesStorageBackendS3  = "s3"
	TracesStorageBackendGCS = "gcs"
	TracesStorageSize1Gi    = "1Gi"
)

// monitoringOwnerReferencesCondition validates ownership by the configured Monitoring CR.
//
//nolint:ireturn // Gomega's And matcher returns the GomegaMatcher interface.
func (tc *MonitoringTestCtx) monitoringOwnerReferencesCondition() gTypes.GomegaMatcher {
	return And(
		jq.Match(`.metadata.ownerReferences | length == 1`),
		jq.Match(`.metadata.ownerReferences[0].kind == "%s"`, gvk.Monitoring.Kind),
		jq.Match(`.metadata.ownerReferences[0].name == "%s"`, tc.MonitoringCRName),
	)
}

// jsonCompatibleValue converts values produced by gojq (e.g. int) into types
// accepted by unstructured.SetNestedField / runtime.DeepCopyJSON.
func jsonCompatibleValue(v any) any {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case map[string]any:
		for k, val := range x {
			x[k] = jsonCompatibleValue(val)
		}
		return x
	case []any:
		for i, val := range x {
			x[i] = jsonCompatibleValue(val)
		}
		return x
	default:
		return v
	}
}

// rebaseForDSCI wraps transforms so they operate on DSCI's .spec.monitoring
// as if it were .spec on a Monitoring CR. This lets every existing transform
// function work unchanged in DSC mode.
func rebaseForDSCI(transforms ...jq.TransformFn) jq.TransformFn {
	return func(dsci *unstructured.Unstructured) error {
		monitoring, _, _ := unstructured.NestedMap(dsci.Object, "spec", "monitoring")
		if monitoring == nil {
			monitoring = map[string]any{}
		}

		temp := &unstructured.Unstructured{
			Object: map[string]any{"spec": monitoring},
		}

		for _, t := range transforms {
			if err := t(temp); err != nil {
				return err
			}
		}

		spec, _ := temp.Object["spec"].(map[string]any)
		if spec == nil {
			spec = map[string]any{}
		}
		return unstructured.SetNestedField(dsci.Object, jsonCompatibleValue(spec), "spec", "monitoring")
	}
}

// patchViaDSCI patches the DSCI's .spec.monitoring using rebaseForDSCI and waits
// for the Monitoring CR to reach the expected phase. This is the single entry
// point for all DSC-mode DSCI mutations.
func (tc *MonitoringTestCtx) patchViaDSCI(expectedPhase common.Phase, transforms ...jq.TransformFn) {
	tc.EventuallyResourcePatched(
		WithMinimalObject(gvk.DSCInitialization, types.NamespacedName{Name: tc.DSCICRName}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			return rebaseForDSCI(transforms...)(u)
		}),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(jq.Match(`.status.phase == "%s"`, expectedPhase)),
	)
}

// ensureMonitoringCRExists creates the Monitoring CR if it does not already exist.
// Readiness is checked after the suite clears any pre-existing optional config.
func (tc *MonitoringTestCtx) ensureMonitoringCRExists(t *testing.T) {
	t.Helper()

	if tc.ApiMode == APIModeDSC {
		tc.EventuallyResourcePatched(
			WithMinimalObject(gvk.DSCInitialization, types.NamespacedName{Name: tc.DSCICRName}),
			WithMutateFunc(rebaseForDSCI(withManagementState(common.Managed))),
		)
		tc.EnsureResourceExists(
			WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		)
		return
	}

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			if u.GetResourceVersion() == "" && tc.MonitoringNamespace != "" {
				if err := unstructured.SetNestedField(u.Object, tc.MonitoringNamespace, "spec", "namespace"); err != nil {
					return err
				}
			}
			return jq.TransformPipeline(withManagementState(common.Managed))(u)
		}),
		WithCondition(jq.Match(`.spec.managementState == "%s"`, common.Managed)),
		WithCustomErrorMsg("Monitoring CR should exist and be managed"),
	)
}

// updateMonitoringConfig patches the Monitoring CR with the given transforms
// and waits for the CR to reach Ready status.
// In DSC mode, it patches the DSCI instead and waits for the module handler
// to propagate changes to the Monitoring CR.
func (tc *MonitoringTestCtx) updateMonitoringConfig(transforms ...jq.TransformFn) {
	if tc.ApiMode == APIModeDSC {
		tc.patchViaDSCI(common.PhaseReady, transforms...)
		return
	}

	tc.updateMonitoringConfigWithOptions(WithMutateFunc(func(u *unstructured.Unstructured) error {
		return jq.TransformPipeline(transforms...)(u)
	}))
}

// updateMonitoringConfigWithoutReady applies configuration whose dependent
// operands need additional test setup before Monitoring can become Ready.
func (tc *MonitoringTestCtx) updateMonitoringConfigWithoutReady(transforms ...jq.TransformFn) {
	if tc.ApiMode == APIModeDSC {
		tc.EventuallyResourcePatched(
			WithMinimalObject(gvk.DSCInitialization, types.NamespacedName{Name: tc.DSCICRName}),
			WithMutateFunc(rebaseForDSCI(transforms...)),
		)
		return
	}

	tc.EventuallyResourcePatched(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithMutateFunc(jq.TransformPipeline(transforms...)),
	)
}

// updateMonitoringConfigWithOptions patches the Monitoring CR with advanced options.
func (tc *MonitoringTestCtx) updateMonitoringConfigWithOptions(opts ...ResourceOpts) {
	baseOpts := make([]ResourceOpts, 0, 2+len(opts))
	baseOpts = append(baseOpts,
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(jq.Match(`.status.phase == "%s"`, common.PhaseReady)),
	)
	tc.EventuallyResourcePatched(append(baseOpts, opts...)...)
}

// setupBaseMonitoring sets up Monitoring CR with managementState=Managed (no metrics, no traces).
func (tc *MonitoringTestCtx) setupBaseMonitoring(t *testing.T) {
	t.Helper()
	tc.updateMonitoringConfig(withManagementState(common.Managed))
}

// setupMetrics enables metrics configuration with default storage settings.
func (tc *MonitoringTestCtx) setupMetrics(t *testing.T) {
	t.Helper()
	transforms := []jq.TransformFn{
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	}
	if tc.expectedDefaultReplicas > 0 {
		transforms = append(transforms, withMetricsReplicas(tc.expectedDefaultReplicas))
	}
	tc.updateMonitoringConfig(transforms...)
}

// cleanupGroup performs group-level cleanup, resetting monitoring to a clean state.
func (tc *MonitoringTestCtx) cleanupGroup(t *testing.T, secretName string) {
	t.Helper()

	tc.resetMonitoringConfigToManaged()

	tc.DeleteResource(
		WithMinimalObject(gvk.TempoMonolithic, types.NamespacedName{
			Name:      TempoMonolithicName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithWaitForDeletion(true),
		WithIgnoreNotFound(true),
	)

	tc.DeleteResource(
		WithMinimalObject(gvk.TempoStack, types.NamespacedName{
			Name:      TempoStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithWaitForDeletion(true),
		WithIgnoreNotFound(true),
	)

	if secretName != "" {
		tc.DeleteResource(
			WithMinimalObject(gvk.Secret, types.NamespacedName{
				Name:      secretName,
				Namespace: tc.MonitoringNamespace,
			}),
			WithIgnoreNotFound(true),
			WithWaitForDeletion(true),
		)
	}
}

// resetMonitoringConfigToManaged deletes optional config fields and sets managementState=Managed.
func (tc *MonitoringTestCtx) resetMonitoringConfigToManaged() {
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		jq.Transform(`del(.spec.metrics, .spec.traces, .spec.alerting, .spec.collectorReplicas, .spec.usageLogs, .spec.logs)`),
	)

	tc.EnsureResourcesGone(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      OpenTelemetryCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.MonitoringStack, types.NamespacedName{
			Name:      MonitoringStackName,
			Namespace: tc.MonitoringNamespace,
		}),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.ThanosQuerier, types.NamespacedName{
			Name:      ThanosQuerierName,
			Namespace: tc.MonitoringNamespace,
		}),
	)
	tc.EnsureResourceGone(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name: UsageLogsCollectorName, Namespace: tc.MonitoringNamespace,
		}),
	)
	tc.EnsureResourceGone(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name: LokiStackName, Namespace: tc.MonitoringNamespace,
		}),
		WithEventuallyTimeout(15*time.Minute),
	)
}

// resetMonitoringConfigToRemoved deletes optional config fields and sets managementState=Removed.
// In DSC mode, it patches the DSCI and waits for the Monitoring CR to reflect NotReady.
func (tc *MonitoringTestCtx) resetMonitoringConfigToRemoved() {
	if tc.ApiMode == APIModeDSC {
		tc.patchViaDSCI(common.PhaseNotReady,
			withManagementState(common.Removed),
			jq.Transform(`del(.spec.metrics, .spec.traces, .spec.alerting, .spec.collectorReplicas, .spec.usageLogs, .spec.logs)`),
		)

		tc.EnsureResourcesGone(
			WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
				Name:      OpenTelemetryCollectorName,
				Namespace: tc.MonitoringNamespace,
			}),
		)

		return
	}

	tc.updateMonitoringConfigWithOptions(
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			return jq.TransformPipeline(
				withManagementState(common.Removed),
				jq.Transform(`del(.spec.metrics, .spec.traces, .spec.alerting, .spec.collectorReplicas, .spec.usageLogs, .spec.logs)`),
			)(u)
		}),
		WithCondition(jq.Match(`.status.phase == "%s"`, common.PhaseNotReady)),
	)

	tc.EnsureResourcesGone(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      OpenTelemetryCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
	)
}

// ensureMonitoringCleanSlate sets monitoring to Removed and cleans up all resources.
func (tc *MonitoringTestCtx) ensureMonitoringCleanSlate(t *testing.T, secretName string) {
	t.Helper()

	tc.resetMonitoringConfigToRemoved()

	tc.DeleteResource(
		WithMinimalObject(gvk.MonitoringStack, types.NamespacedName{
			Name:      MonitoringStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithWaitForDeletion(true),
		WithIgnoreNotFound(true),
	)

	tc.DeleteResource(
		WithMinimalObject(gvk.TempoMonolithic, types.NamespacedName{
			Name:      TempoMonolithicName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithWaitForDeletion(true),
		WithIgnoreNotFound(true),
	)

	tc.cleanupTempoStackAndSecret(secretName)
}

// ensureOpenTelemetryCollectorReady waits for the OTel Collector deployment to have at least one ready replica.
func (tc *MonitoringTestCtx) ensureOpenTelemetryCollectorReady(t *testing.T) {
	t.Helper()

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      OpenTelemetryCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`.status.scale.statusReplicas | split("/") | map(tonumber) | min > 0`)),
		WithCustomErrorMsg("OpenTelemetry Collector should have at least one ready replica"),
	)
}

// cleanupTempoStackAndSecret removes TempoStack and optionally an associated secret.
func (tc *MonitoringTestCtx) cleanupTempoStackAndSecret(secretName string) {
	tc.DeleteResource(
		WithMinimalObject(gvk.TempoStack, types.NamespacedName{
			Name:      TempoStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithWaitForDeletion(true),
		WithIgnoreNotFound(true),
		WithEventuallyTimeout(15*time.Minute),
	)

	if secretName != "" {
		tc.DeleteResource(
			WithMinimalObject(gvk.Secret, types.NamespacedName{
				Name:      secretName,
				Namespace: tc.MonitoringNamespace,
			}),
			WithIgnoreNotFound(true),
			WithWaitForDeletion(true),
		)
	}
}

// setupUsageLogsWithStorage creates a secret and configures usage logs with storage.
func (tc *MonitoringTestCtx) setupUsageLogsWithStorage(t *testing.T, storageType, secretName string) {
	t.Helper()
	tc.createLokiS3Secret(t, secretName, tc.MonitoringNamespace)
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withUsageLogsStorage(storageType, secretName, tc.DefaultStorageClass),
	)
}

// cleanupTracesConfiguration resets traces configuration.
func (tc *MonitoringTestCtx) cleanupTracesConfiguration() {
	tc.updateMonitoringConfig(withManagementState(common.Managed), withNoTraces())
}

// detectExpectedReplicas queries the cluster node count to determine expected Prometheus replicas.
func detectExpectedReplicas(t *testing.T, tc *TestContext) int {
	t.Helper()

	items := tc.EnsureResourcesExist(
		WithMinimalObject(gvk.Namespace, types.NamespacedName{}),
		WithCondition(Not(BeEmpty())),
	)

	nodeList := &unstructured.UnstructuredList{}
	nodeList.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("NodeList"))
	err := tc.client.List(tc.ctx, nodeList)
	require.NoError(t, err, "failed to list cluster nodes for replica detection")
	_ = items

	schedulable := 0
	for i := range nodeList.Items {
		unschedulable, _, _ := unstructured.NestedBool(nodeList.Items[i].Object, "spec", "unschedulable")
		if !unschedulable {
			schedulable++
		}
	}

	if schedulable <= 1 {
		t.Logf("detected single schedulable node — expecting 1 replica")
		return 1
	}

	t.Logf("detected %d schedulable nodes — expecting 2 replicas", schedulable)
	return 2
}

// createTempoStorageSecret creates a test secret for a locally hosted Tempo backend.
func (tc *MonitoringTestCtx) createTempoStorageSecret(t *testing.T, backendType, secretName, namespace string) {
	t.Helper()

	switch backendType {
	case "s3":
		tc.createTempoS3Secret(t, secretName, namespace)
	case "gcs":
		tc.createTempoGCSSecret(t, secretName, namespace)
	default:
		tc.g.Fail(fmt.Sprintf("Unsupported backend type: %s", backendType))
	}
}

// createTempoS3Secret creates an S3 secret compatible with TempoStack operator.
func (tc *MonitoringTestCtx) createTempoS3Secret(t *testing.T, secretName, namespace string) {
	t.Helper()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"access_key_id":     []byte(seaweedFSAccessKey),
			"access_key_secret": []byte(seaweedFSSecretKey),
			"bucket":            []byte(tempoS3Bucket),
			"endpoint":          fmt.Appendf(nil, "http://%s.%s.svc.cluster.local:%d", seaweedFSServiceName, namespace, seaweedFSS3Port),
		},
	}

	data := make(map[string]any, len(secret.Data))
	for k, v := range secret.Data {
		data[k] = string(v)
	}

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Secret, types.NamespacedName{Name: secret.Name, Namespace: secret.Namespace}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.Object["type"] = string(secret.Type)
			u.Object["stringData"] = data
			return nil
		}),
	)
}

// createTempoGCSSecret creates a GCS secret compatible with TempoStack operator.
func (tc *MonitoringTestCtx) createTempoGCSSecret(t *testing.T, secretName, namespace string) {
	t.Helper()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"bucketname": []byte(fakeGCSBucket),
			"key.json": []byte(`{
				"type": "service_account",
				"project_id": "fake-test-project-not-real",
				"private_key_id": "test-key-id-fake",
				"private_key": "-----BEGIN PRIVATE KEY-----\nTEST-FAKE-KEY-NOT-REAL\n-----END PRIVATE KEY-----\n",
				"client_email": "test-fake@fake-project.iam.gserviceaccount.com"
			}`),
		},
	}

	data := make(map[string]any, len(secret.Data))
	for k, v := range secret.Data {
		data[k] = string(v)
	}

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Secret, types.NamespacedName{Name: secret.Name, Namespace: secret.Namespace}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.Object["type"] = string(secret.Type)
			u.Object["stringData"] = data
			return nil
		}),
	)
}

// createLokiS3Secret creates an S3 secret compatible with LokiStack operator.
func (tc *MonitoringTestCtx) createLokiS3Secret(t *testing.T, secretName, namespace string) {
	t.Helper()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"access_key_id":     []byte(seaweedFSAccessKey),
			"access_key_secret": []byte(seaweedFSSecretKey),
			"bucketnames":       []byte(lokiS3Bucket),
			"endpoint":          fmt.Appendf(nil, "http://%s.%s.svc.cluster.local:%d", seaweedFSServiceName, namespace, seaweedFSS3Port),
			"region":            []byte("us-east-1"),
			"insecure":          []byte("true"),
			"s3ForcePathStyle":  []byte("true"),
		},
	}

	data := make(map[string]any, len(secret.Data))
	for k, v := range secret.Data {
		data[k] = string(v)
	}

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Secret, types.NamespacedName{Name: secret.Name, Namespace: secret.Namespace}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			u.Object["type"] = string(secret.Type)
			u.Object["stringData"] = data
			return nil
		}),
	)
}

// Transform functions — all paths operate on .spec.* directly (not .spec.monitoring.*).

func withManagementState(state common.ManagementState) jq.TransformFn {
	return jq.Transform(`.spec.managementState = "%s"`, state)
}

func (tc *MonitoringTestCtx) withMetricsConfig() jq.TransformFn {
	return jq.Transform(`.spec.metrics = {
        "storage": {
            "size": "%s",
            "retention": "%s"
        }
    }`, MetricsStorageSize, MetricsRetention)
}

func withMetricsReplicas(replicas int) jq.TransformFn {
	return func(u *unstructured.Unstructured) error {
		return unstructured.SetNestedField(u.Object, int64(replicas), "spec", "metrics", "replicas")
	}
}

func withNamespace(namespace string) jq.TransformFn {
	return jq.Transform(`.spec.namespace = "%s"`, namespace)
}

func withEmptyMetrics() jq.TransformFn {
	return jq.Transform(`.spec.metrics = {}`)
}

func withEmptyAlerting() jq.TransformFn {
	return jq.Transform(`.spec.alerting = {}`)
}

func withNoMetrics() jq.TransformFn {
	return jq.Transform(`del(.spec.metrics)`)
}

func withNoAlerting() jq.TransformFn {
	return jq.Transform(`del(.spec.alerting)`)
}

func withNoTraces() jq.TransformFn {
	return jq.Transform(`del(.spec.traces)`)
}

func withNoUsageLogs() jq.TransformFn {
	return jq.Transform(`del(.spec.usageLogs)`)
}

func withUsageLogsStorage(storageType, secretName, storageClassName string) jq.TransformFn {
	if storageClassName == "" {
		return jq.Transform(`.spec.usageLogs.storage = {"type": "%s", "secretName": "%s"}`, storageType, secretName)
	}
	return jq.Transform(`.spec.usageLogs.storage = {"type": "%s", "secretName": "%s", "storageClassName": "%s"}`, storageType, secretName, storageClassName)
}

func withNoCollectorReplicas() jq.TransformFn {
	return jq.Transform(`del(.spec.collectorReplicas)`)
}

func withCustomMetricsExporters() jq.TransformFn {
	return jq.Transform(`.spec.metrics.exporters = {
		"debug": {
			"verbosity": "detailed"
		},
        "%s": {
			"endpoint": "http://custom-backend:4317",
			"tls": {
				"insecure": true
			}
		}
	}`, OtlpCustomExporter)
}

func withCustomTracesExporters() jq.TransformFn {
	return jq.Transform(`.spec.traces.exporters = {
        "debug": {
            "verbosity": "detailed"
        },
        "%s": {
            "endpoint": "http://custom-endpoint:4318",
            "headers": {
                "api-key": "secret-key"
            }
        }
    }`, OtlpHttpCustomExporter)
}

func withReservedTracesExporter() jq.TransformFn {
	return jq.Transform(`.spec.traces.exporters = {
        "%s": {
            "endpoint": "http://malicious-endpoint:4317"
        }
    }`, OtlpTempoExporter)
}

func withMonitoringTraces(backend, secret, size, retention string) jq.TransformFn {
	transforms := []jq.TransformFn{
		jq.Transform(`.spec.traces = {
        "storage": {
            "backend": "%s"
        },
        "exporters": null
    }`, backend),
	}

	if secret != "" {
		transforms = append(transforms, jq.Transform(`.spec.traces.storage.secret = "%s"`, secret))
	}

	if retention != "" {
		transforms = append(transforms, jq.Transform(`.spec.traces.storage.retention = "%s"`, retention))
	}

	if backend == TracesStorageBackendPV && size != "" {
		transforms = append(transforms, jq.Transform(`.spec.traces.storage.size = "%s"`, size))
	}

	return jq.TransformPipeline(transforms...)
}

// registerMonitoringRestore records only the field the suite mutates. In DSC
// mode the real DSCI must already exist; module mode may start without a CR.
func (tc *MonitoringTestCtx) registerMonitoringRestore(t *testing.T) {
	t.Helper()

	kind := gvk.Monitoring
	nn := types.NamespacedName{Name: tc.MonitoringCRName}
	fieldPath := []string{"spec"}
	if tc.ApiMode == APIModeDSC {
		kind = gvk.DSCInitialization
		nn.Name = tc.DSCICRName
		fieldPath = []string{"spec", "monitoring"}
	}

	original, err := tc.fetchResource(t, kind, nn)
	existed := err == nil
	if err != nil && (tc.ApiMode != APIModeModule || !k8serr.IsNotFound(err)) {
		t.Fatalf("failed to record original %s %s: %v", kind.Kind, nn.Name, err)
	}

	var originalValue map[string]any
	var hadValue bool
	if existed {
		originalValue, hadValue, err = unstructured.NestedMap(original.Object, fieldPath...)
		require.NoError(t, err)
	}

	t.Cleanup(func() {
		if !existed {
			tc.DeleteResource(
				WithMinimalObject(kind, nn),
				WithIgnoreNotFound(true),
				WithWaitForDeletion(true),
				WithEventuallyTimeout(15*time.Minute),
			)
			return
		}

		tc.EventuallyResourcePatched(
			WithMinimalObject(kind, nn),
			WithMutateFunc(func(current *unstructured.Unstructured) error {
				if hadValue {
					return unstructured.SetNestedMap(current.Object, originalValue, fieldPath...)
				}
				unstructured.RemoveNestedField(current.Object, fieldPath...)
				return nil
			}),
			WithCustomErrorMsg("failed to restore original %s %s configuration", kind.Kind, nn.Name),
		)
	})
}

// ensurePrerequisites verifies that the operator is running, the CRD is
// registered, installs dependent operators (when enabled), and ensures the
// Monitoring CR exists before any test groups run. The monitoring namespace
// is auto-detected from the CR's spec.namespace unless overridden via flag.
func (tc *MonitoringTestCtx) ensurePrerequisites(t *testing.T) {
	t.Helper()

	operatorMonitoringNamespace := tc.ensureOperatorDeploymentReady(t)
	if tc.ApiMode == APIModeModule && tc.MonitoringNamespace == "" {
		_, err := tc.fetchResource(t, gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName})
		switch {
		case err == nil:
			tc.MonitoringNamespace = tc.detectMonitoringNamespace(t)
		case k8serr.IsNotFound(err):
			tc.MonitoringNamespace = operatorMonitoringNamespace
		default:
			t.Fatalf("failed to check existing Monitoring CR %s: %v", tc.MonitoringCRName, err)
		}
	}
	tc.ensureCRDExists(t, gvk.Monitoring)
	tc.DefaultStorageClass = tc.ensureDefaultStorageClass(t)

	if tc.ApiMode == APIModeDSC {
		tc.ensureCRDExists(t, gvk.DSCInitialization)
	}
	tc.registerMonitoringRestore(t)

	if testOpts.installOperators {
		tc.installDependentOperators(t)
	}
	tc.ensureCRDExists(t, gvk.CertManagerCertificate)
	for _, name := range []string{
		"cluster-observability-operator",
		"tempo-operator",
		"opentelemetry-operator",
		"loki-operator",
	} {
		if testOpts.installOperators {
			tc.g.Eventually(func() error {
				_, err := olm.OperatorExists(tc.Context(), tc.Client(), name)
				return err
			}).WithTimeout(tc.Timeouts.olmOperationTimeout).Should(Succeed(),
				"required OLM operator %s should be installed", name)
		} else {
			_, err := olm.OperatorExists(tc.Context(), tc.Client(), name)
			require.NoError(t, err, "required OLM operator %s is missing", name)
		}
	}
	for _, required := range []schema.GroupVersionKind{
		gvk.MonitoringStack,
		gvk.TempoMonolithic,
		gvk.TempoStack,
		gvk.OpenTelemetryCollector,
		gvk.Instrumentation,
		gvk.LokiStack,
		gvk.Perses,
		gvk.ThanosQuerier,
		gvk.CoreosPodMonitor,
		gvk.CoreosServiceMonitor,
	} {
		tc.ensureCRDExists(t, required)
	}

	tc.ensureMonitoringCRExists(t)

	if tc.MonitoringNamespace == "" {
		tc.MonitoringNamespace = tc.detectMonitoringNamespace(t)
		t.Logf("auto-detected monitoring namespace: %s", tc.MonitoringNamespace)
	}

	tc.ensureNamespaceExists(tc.MonitoringNamespace)
	tc.resetMonitoringConfigToManaged()
}

// installDependentOperators installs required OLM operators in parallel sub-tests.
// Each operator gets its own namespace, OperatorGroup, and Subscription when it is
// not already present.
func (tc *MonitoringTestCtx) installDependentOperators(t *testing.T) {
	t.Helper()

	type operator struct {
		namespace string
		name      string
		channel   string
	}

	operators := []operator{
		{observabilityOpNamespace, observabilityOpName, cooOperatorChannel()},
		{tempoOpNamespace, tempoOpName, tempoOpChannel},
		{opentelemetryOpNamespace, opentelemetryOpName, opentelemetryOpChannel},
		{certManagerOpNamespace, certManagerOpName, certManagerOpChannel},
	}

	t.Run("install-dependent-operators", func(t *testing.T) {
		for _, op := range operators {
			t.Run(op.name, func(t *testing.T) {
				t.Parallel()
				tc.EnsureOperatorInstalled(op.namespace, op.name, op.channel)
			})
		}
	})

	// Loki channels are versioned, so use the cluster catalog's default channel.
	// A preinstalled Loki Operator may not have a PackageManifest in the catalog.
	lokiStacks := &unstructured.UnstructuredList{}
	lokiStacks.SetGroupVersionKind(gvk.LokiStack)
	if err := tc.Client().List(tc.Context(), lokiStacks); err == nil {
		t.Log("LokiStack CRD is already available")
		return
	} else if !meta.IsNoMatchError(err) && !k8serr.IsNotFound(err) {
		t.Fatalf("failed to check LokiStack CRD: %v", err)
	}

	manifests := &unstructured.UnstructuredList{}
	manifests.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "packages.operators.coreos.com", Version: "v1", Kind: "PackageManifestList",
	})
	if err := tc.Client().List(tc.Context(), manifests,
		client.InNamespace("openshift-marketplace"),
		client.MatchingLabels{"catalog": "redhat-operators"},
	); err != nil {
		t.Fatalf("Loki Operator is required; failed to list redhat-operators PackageManifests: %v", err)
	}
	for _, manifest := range manifests.Items {
		if manifest.GetName() != lokiOpName {
			continue
		}
		channel, _, err := unstructured.NestedString(manifest.Object, "status", "defaultChannel")
		if err != nil || channel == "" {
			t.Fatalf("redhat-operators Loki PackageManifest has no default channel: %v", err)
		}
		t.Logf("installing Loki Operator from redhat-operators channel %s", channel)
		tc.EnsureOperatorInstalled(lokiOpNamespace, lokiOpName, channel)
		return
	}
	t.Fatalf("Loki Operator is required; %s PackageManifest is missing from redhat-operators", lokiOpName)
}

// Suppress unused warnings for transform functions used in later commits.
var (
	_ = withNamespace
	_ = withEmptyMetrics
	_ = withNoCollectorReplicas
)
