package e2e_test

import (
	"bytes"
	"context"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"
)

func readInferenceManifest(t *testing.T, name string) map[string]any {
	t.Helper()
	file, err := os.Open(filepath.Join("prerequisites", "inference", name))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })

	manifest := map[string]any{}
	require.NoError(t, utilyaml.NewYAMLOrJSONDecoder(file, 4096).Decode(&manifest))
	return manifest
}

func TestLeaderWorkerSetOperatorManifest(t *testing.T) {
	manifest := readInferenceManifest(t, "lwsoperator.yaml")
	require.Equal(t, "operator.openshift.io/v1", manifest["apiVersion"])
	require.Equal(t, "LeaderWorkerSetOperator", manifest["kind"])

	metadata, ok := manifest["metadata"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "cluster", metadata["name"])
	require.Equal(t, "openshift-lws-operator", metadata["namespace"])
}

func TestConnectivityLinkOperatorConfiguration(t *testing.T) {
	require.Equal(t, "rhcl-operator", connectivityLinkOpName)
	require.Equal(t, "openshift-operators", connectivityLinkOpNamespace)
	require.Equal(t, "stable", connectivityLinkOpChannel)
	require.Equal(t, "redhat-operators", connectivityLinkOpSource)
}

func TestDSCIManifest(t *testing.T) {
	manifest := readInferenceManifest(t, "dsci.yaml")
	require.Equal(t, "dscinitialization.opendatahub.io/v2", manifest["apiVersion"])
	require.Equal(t, "DSCInitialization", manifest["kind"])

	spec, ok := manifest["spec"].(map[string]any)
	require.True(t, ok)
	_, hasAlerting := spec["alerting"]
	require.False(t, hasAlerting)
	// applicationsNamespace and monitoring.namespace are both immutable, so a desync with the Go
	// constants cannot be patched away on a cluster that already ran an older revision.
	require.Equal(t, defaultInferenceApplicationsNamespace, spec["applicationsNamespace"])

	// setupInferencePrerequisites provisions the S3 backend from these values, so a
	// desync here silently leaves the LokiStack pointing at a Secret nobody creates.
	monitoring, ok := spec["monitoring"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, defaultInferenceMonitoringNamespace, monitoring["namespace"])
	usageLogs, ok := monitoring["usageLogs"].(map[string]any)
	require.True(t, ok)
	storage, ok := usageLogs["storage"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, lokiStorageType, storage["type"])
	require.Equal(t, lokiS3SecretName, storage["secretName"])
	require.Equal(t, lokiStorageClassName, storage["storageClassName"])
}

func TestMonitoringManifest(t *testing.T) {
	manifest := readInferenceManifest(t, "monitoring.yaml")
	require.Equal(t, "services.platform.opendatahub.io/v1alpha1", manifest["apiVersion"])
	require.Equal(t, "Monitoring", manifest["kind"])

	metadata, ok := manifest["metadata"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "default-monitoring", metadata["name"])
	// The Monitoring CR is cluster-scoped; a namespace here would make the update target nothing.
	require.NotContains(t, metadata, "namespace")

	spec, ok := manifest["spec"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, defaultInferenceMonitoringNamespace, spec["namespace"])

	usageLogs, ok := spec["usageLogs"].(map[string]any)
	require.True(t, ok)
	storage, ok := usageLogs["storage"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, lokiStorageType, storage["type"])
	require.Equal(t, lokiS3SecretName, storage["secretName"])
	require.Equal(t, lokiCredentialMode, storage["credentialMode"])
	require.Equal(t, lokiStorageClassName, storage["storageClassName"])
}

// TestMonitoringManifestMatchesDSCI guards the duplication between the two manifests.
// applyMonitoringManifest writes the whole object, so anything dsci.yaml configures and
// monitoring.yaml omits is deleted from the live CR -- compare key sets, not a fixed list, or a
// key added to dsci.yaml later goes unnoticed.
func TestMonitoringManifestMatchesDSCI(t *testing.T) {
	dsci := readInferenceManifest(t, "dsci.yaml")
	dsciSpec, ok := dsci["spec"].(map[string]any)
	require.True(t, ok)
	dsciMonitoring, ok := dsciSpec["monitoring"].(map[string]any)
	require.True(t, ok)

	monitoring := readInferenceManifest(t, "monitoring.yaml")
	monitoringSpec, ok := monitoring["spec"].(map[string]any)
	require.True(t, ok)

	require.ElementsMatch(t, slices.Collect(maps.Keys(dsciMonitoring)), slices.Collect(maps.Keys(monitoringSpec)),
		"dsci.yaml spec.monitoring and monitoring.yaml spec must configure the same keys")
	for key, want := range dsciMonitoring {
		require.Equal(t, want, monitoringSpec[key], "monitoring.yaml spec.%s must match dsci.yaml", key)
	}
}

// TestRenderManifestRewritesNamespaces covers the substitution the two namespace flags rely on.
// The risk it guards is specific: the rewrite is textual, and "opendatahub" is a substring of
// every apiVersion in the prerequisites directory, so a rewrite in the wrong direction -- or one
// that matched too loosely -- would produce manifests the API server rejects as unknown kinds.
func TestRenderManifestRewritesNamespaces(t *testing.T) {
	monitoringNamespace, applicationsNamespace := testOpts.monitoringNamespace, testOpts.applicationsNamespace
	t.Cleanup(func() {
		testOpts.monitoringNamespace, testOpts.applicationsNamespace = monitoringNamespace, applicationsNamespace
	})
	testOpts.monitoringNamespace, testOpts.applicationsNamespace = "opendatahub", "opendatahub"

	rendered, err := renderManifest(filepath.Join("prerequisites", "inference", "dsci.yaml"))
	require.NoError(t, err)

	manifest := map[string]any{}
	require.NoError(t, utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(rendered), 4096).Decode(&manifest))
	// Unchanged: the group is opendatahub.io, not a namespace.
	require.Equal(t, "dscinitialization.opendatahub.io/v2", manifest["apiVersion"])

	spec, ok := manifest["spec"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "opendatahub", spec["applicationsNamespace"])

	monitoring, ok := spec["monitoring"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "opendatahub", monitoring["namespace"])

	// The namespace is also embedded in the exporter endpoints, which is why the rewrite is
	// textual rather than a set of field writes.
	metrics, ok := monitoring["metrics"].(map[string]any)
	require.True(t, ok)
	exporters, ok := metrics["exporters"].(map[string]any)
	require.True(t, ok)
	otlp, ok := exporters["otlp/metrics"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "http://lgtm.opendatahub.svc.cluster.local:4317", otlp["endpoint"])

	require.NotContains(t, string(rendered), "redhat-ods-")
}

// TestRenderManifestDefaultsToManifestLiterals pins the no-op case: with both flags unset the
// rendered bytes must equal the file, or the committed manifests stop being what actually runs.
func TestRenderManifestDefaultsToManifestLiterals(t *testing.T) {
	monitoringNamespace, applicationsNamespace := testOpts.monitoringNamespace, testOpts.applicationsNamespace
	t.Cleanup(func() {
		testOpts.monitoringNamespace, testOpts.applicationsNamespace = monitoringNamespace, applicationsNamespace
	})
	testOpts.monitoringNamespace, testOpts.applicationsNamespace = "", ""

	for _, name := range []string{"dsci.yaml", "monitoring.yaml", "lgtm.yaml"} {
		path := filepath.Join("prerequisites", "inference", name)
		want, err := os.ReadFile(path)
		require.NoError(t, err)
		got, err := renderManifest(path)
		require.NoError(t, err)
		require.Equal(t, string(want), string(got), "%s must render unchanged by default", name)
	}
}

// maasEnvoyFilterFixture mirrors the shape of the EnvoyFilter the maas-controller creates: a
// run of HTTP_FILTER patches that carry no access_log, then the single NETWORK_FILTER patch that
// does. Only the nesting the expression walks is reproduced.
func maasEnvoyFilterFixture(resourceAttributeKeys ...string) *unstructured.Unstructured {
	values := make([]any, 0, len(resourceAttributeKeys))
	for _, key := range resourceAttributeKeys {
		values = append(values, map[string]any{
			"key":   key,
			"value": map[string]any{"string_value": "models-as-a-service"},
		})
	}

	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1alpha3",
		"kind":       "EnvoyFilter",
		"metadata": map[string]any{
			"name":      maasUsageLogEnvoyFilter,
			"namespace": maasUsageLogEnvoyFilterNamespace,
		},
		"spec": map[string]any{
			"configPatches": []any{
				map[string]any{
					"applyTo": "HTTP_FILTER",
					"patch": map[string]any{
						"value": map[string]any{
							"typed_config": map[string]any{"@type": "type.googleapis.com/envoy.extensions.filters.http.lua.v3.Lua"},
						},
					},
				},
				map[string]any{
					"applyTo": "NETWORK_FILTER",
					"patch": map[string]any{
						"value": map[string]any{
							"typed_config": map[string]any{
								"access_log": []any{
									map[string]any{
										"name": "envoy.access_loggers.open_telemetry",
										"typed_config": map[string]any{
											"resource_attributes": map[string]any{"values": values},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}}
}

// TestMaaSResourceAttributeKeysExpr pins the jq path testMaaSUsageLogEnvoyFilter asserts on.
// The path is the fragile part of that test: a typo fails loudly rather than passing vacuously,
// but it would fail minutes into a cluster run instead of here.
func TestMaaSResourceAttributeKeysExpr(t *testing.T) {
	keys, err := jq.ExtractValue[[]any](
		maasEnvoyFilterFixture("service.name", maasTenantResourceAttribute),
		maasResourceAttributeKeysExpr,
	)
	require.NoError(t, err)
	// Reaches the right depth, skips the HTTP_FILTER patch, and does not depend on the
	// NETWORK_FILTER patch's position in the list.
	require.Equal(t, []any{"service.name", maasTenantResourceAttribute}, keys)

	// The same condition testMaaSUsageLogEnvoyFilter passes to WithCondition.
	condition := jq.Match(maasResourceAttributeKeysExpr+` | any(. == "%s")`, maasTenantResourceAttribute)

	matched, err := condition.Match(maasEnvoyFilterFixture("service.name", maasTenantResourceAttribute))
	require.NoError(t, err)
	require.True(t, matched)

	// And it can fail: without the attribute, the same expression must not match.
	matched, err = condition.Match(maasEnvoyFilterFixture("service.name"))
	require.NoError(t, err)
	require.False(t, matched)
}

func TestKuadrantManifest(t *testing.T) {
	manifest := readInferenceManifest(t, "kuadrant.yaml")
	require.Equal(t, "kuadrant.io/v1beta1", manifest["apiVersion"])
	require.Equal(t, "Kuadrant", manifest["kind"])

	metadata, ok := manifest["metadata"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "kuadrant", metadata["name"])
	require.Equal(t, "kuadrant-system", metadata["namespace"])
}

func TestApplyManifestIfAbsentPreservesExistingResource(t *testing.T) {
	const manifest = `apiVersion: kuadrant.io/v1beta1
kind: Kuadrant
metadata:
  name: kuadrant
  namespace: kuadrant-system
spec: {}
`
	gvk := schema.GroupVersionKind{Group: "kuadrant.io", Version: "v1beta1", Kind: "Kuadrant"}
	existing := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kuadrant.io/v1beta1",
		"kind":       "Kuadrant",
		"metadata": map[string]any{
			"name":      "kuadrant",
			"namespace": "kuadrant-system",
		},
		"spec": map[string]any{
			"observability": map[string]any{"enable": true},
		},
	}}

	tc := &TestContext{
		client: fake.NewClientBuilder().WithObjects(existing).Build(),
		ctx:    context.Background(),
	}
	path := filepath.Join(t.TempDir(), "kuadrant.yaml")
	require.NoError(t, os.WriteFile(path, []byte(manifest), 0o600))

	require.NoError(t, applyManifestIfAbsent(tc, path))

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(gvk)
	require.NoError(t, tc.Client().Get(context.Background(), types.NamespacedName{
		Name: "kuadrant", Namespace: "kuadrant-system",
	}, got))
	enabled, found, err := unstructured.NestedBool(got.Object, "spec", "observability", "enable")
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, enabled)
}
