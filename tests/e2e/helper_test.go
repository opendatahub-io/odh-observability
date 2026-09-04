package e2e_test

import (
	"fmt"
	"testing"
	"time"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega matchers
)

// Constants for monitoring resource names.
const (
	MonitoringCRName                  = "default-monitoring"
	MonitoringStackName               = "data-science-monitoringstack"
	OpenTelemetryCollectorName        = "data-science-collector"
	TargetAllocatorDeploymentName     = "data-science-collector-targetallocator"
	TargetAllocatorServiceAccount     = "data-science-collector-collector"
	TempoMonolithicName               = "data-science-tempomonolithic"
	TempoStackName                    = "data-science-tempostack"
	InstrumentationName               = "data-science-instrumentation"
	ThanosQuerierName                 = "data-science-thanos-querier"
	ThanosQuerierRouteName            = "data-science-thanos-querier-route"
	PersesName                        = "data-science-perses"
	PersesDatasourceName              = "data-science-prometheus-datasource"
	ClusterPrometheusDatasourceName   = "cluster-prometheus-datasource"
	ClusterPrometheusDatasourceSecret = "cluster-prometheus-datasource-secret"
	UsageLogsCollectorName            = "usage-logs"
	UsageLogsCollectorServiceAccount  = "usage-logs-collector"
	LokiStackName                     = "data-science-lokistack"
)

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

// monitoringOwnerReferencesCondition validates owner references point to the Monitoring CR.
var monitoringOwnerReferencesCondition = And(
	jq.Match(`.metadata.ownerReferences | length == 1`),
	jq.Match(`.metadata.ownerReferences[0].kind == "%s"`, gvk.Monitoring.Kind),
	jq.Match(`.metadata.ownerReferences[0].name == "%s"`, MonitoringCRName),
)

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

		return unstructured.SetNestedField(dsci.Object, temp.Object["spec"], "spec", "monitoring")
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
// In DSC mode, it patches the DSCI to enable monitoring and waits for the
// module handler to create the Monitoring CR.
func (tc *MonitoringTestCtx) ensureMonitoringCRExists(t *testing.T) {
	t.Helper()

	if tc.ApiMode == APIModeDSC {
		tc.patchViaDSCI(common.PhaseReady, withManagementState(common.Managed))
		return
	}

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			return jq.TransformPipeline(withManagementState(common.Managed))(u)
		}),
		WithCondition(jq.Match(`.status.phase == "%s"`, common.PhaseReady)),
		WithCustomErrorMsg("Monitoring CR should exist and reach Ready phase"),
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
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)
}

// cleanupGroup performs group-level cleanup, resetting monitoring to a clean state.
func (tc *MonitoringTestCtx) cleanupGroup(t *testing.T, secretName string) {
	t.Helper()

	tc.resetMonitoringConfigToManaged()

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

	tc.DeleteResource(
		WithMinimalObject(gvk.TempoMonolithic, types.NamespacedName{
			Name:      TempoMonolithicName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithWaitForDeletion(true),
		WithRemoveFinalizersOnDelete(true),
		WithIgnoreNotFound(true),
	)

	tc.DeleteResource(
		WithMinimalObject(gvk.TempoStack, types.NamespacedName{
			Name:      TempoStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithWaitForDeletion(true),
		WithRemoveFinalizersOnDelete(true),
		WithIgnoreNotFound(true),
	)
}

// resetMonitoringConfigToManaged deletes optional config fields and sets managementState=Managed.
func (tc *MonitoringTestCtx) resetMonitoringConfigToManaged() {
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		jq.Transform(`del(.spec.metrics, .spec.traces, .spec.alerting, .spec.collectorReplicas, .spec.usageLogs)`),
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
}

// resetMonitoringConfigToRemoved deletes optional config fields and sets managementState=Removed.
// In DSC mode, it patches the DSCI and waits for the Monitoring CR to reflect NotReady.
func (tc *MonitoringTestCtx) resetMonitoringConfigToRemoved() {
	if tc.ApiMode == APIModeDSC {
		tc.patchViaDSCI(common.PhaseNotReady,
			withManagementState(common.Removed),
			jq.Transform(`del(.spec.metrics, .spec.traces, .spec.alerting, .spec.collectorReplicas, .spec.usageLogs)`),
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
				jq.Transform(`del(.spec.metrics, .spec.traces, .spec.alerting, .spec.collectorReplicas, .spec.usageLogs)`),
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
		WithRemoveFinalizersOnDelete(true),
		WithIgnoreNotFound(true),
	)

	tc.DeleteResource(
		WithMinimalObject(gvk.TempoMonolithic, types.NamespacedName{
			Name:      TempoMonolithicName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithWaitForDeletion(true),
		WithRemoveFinalizersOnDelete(true),
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
		WithRemoveFinalizersOnDelete(true),
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

// cleanupLokiStackAndSecret removes LokiStack and optionally an associated secret.
func (tc *MonitoringTestCtx) cleanupLokiStackAndSecret(secretName string) {
	tc.DeleteResource(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      LokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithWaitForDeletion(true),
		WithRemoveFinalizersOnDelete(true),
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
		withUsageLogsStorage(storageType, secretName, ""),
	)
}

// cleanupTracesConfiguration resets traces configuration.
func (tc *MonitoringTestCtx) cleanupTracesConfiguration() {
	tc.updateMonitoringConfig(withNoTraces())
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

// createDummySecret creates a test secret for the specified backend type.
// For Tempo backends, this routes to tempo-specific helpers.
//
// Deprecated: Use createTempoS3Secret, createTempoGCSSecret, or createLokiS3Secret directly.
func (tc *MonitoringTestCtx) createDummySecret(t *testing.T, backendType, secretName, namespace string) {
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
			"access_key_id":     []byte("fake-access-key"),
			"access_key_secret": []byte("fake-secret-key"),
			"bucket":            []byte("fake-bucket"),
			"endpoint":          []byte("https://s3.amazonaws.com"),
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
			"access_key_id":     []byte("fake-access-key"),
			"access_key_secret": []byte("fake-secret-key"),
			"bucketnames":       []byte("fake-bucket"),
			"endpoint":          []byte("https://s3.us-east-1.amazonaws.com"),
			"region":            []byte("us-east-1"),
			"insecure":          []byte("false"),
			"s3ForcePathStyle":  []byte("false"),
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
	return jq.Transform(`.spec.metrics.replicas = %d`, replicas)
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

// ensurePrerequisites verifies that the operator is running, the CRD is
// registered, installs dependent operators (when enabled), and ensures the
// Monitoring CR exists before any test groups run. The monitoring namespace
// is auto-detected from the CR's spec.namespace unless overridden via flag.
func (tc *MonitoringTestCtx) ensurePrerequisites(t *testing.T) {
	t.Helper()

	tc.ensureOperatorPodRunning(t)
	tc.ensureCRDExists(t, gvk.Monitoring)

	if tc.ApiMode == APIModeDSC {
		tc.ensureCRDExists(t, gvk.DSCInitialization)
	}

	if testOpts.installOperators {
		tc.installDependentOperators(t)
	}

	tc.ensureMonitoringCRExists(t)

	if tc.MonitoringNamespace == "" {
		tc.MonitoringNamespace = tc.detectMonitoringNamespace(t)
		t.Logf("auto-detected monitoring namespace: %s", tc.MonitoringNamespace)
	}

	tc.ensureNamespaceExists(tc.MonitoringNamespace)
}

// installDependentOperators installs the three required OLM operators
// in parallel sub-tests. Each operator gets its own namespace + OperatorGroup + Subscription.
func (tc *MonitoringTestCtx) installDependentOperators(t *testing.T) {
	t.Helper()

	type operator struct {
		namespace string
		name      string
		channel   string
	}

	operators := []operator{
		{observabilityOpNamespace, observabilityOpName, observabilityOpChannel},
		{tempoOpNamespace, tempoOpName, tempoOpChannel},
		{opentelemetryOpNamespace, opentelemetryOpName, opentelemetryOpChannel},
	}

	t.Run("install-dependent-operators", func(t *testing.T) {
		for _, op := range operators {
			t.Run(op.name, func(t *testing.T) {
				t.Parallel()
				tc.EnsureOperatorInstalled(op.namespace, op.name, op.channel)
			})
		}
	})
}

// Suppress unused warnings for transform functions used in later commits.
var (
	_ = withNamespace
	_ = withEmptyMetrics
	_ = withNoCollectorReplicas
)
