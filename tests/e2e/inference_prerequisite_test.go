package e2e_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
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
