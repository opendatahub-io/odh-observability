package e2e_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const otlpNamespaceLabel = "monitoring.opendatahub.io/otlp"

// TestCollectorOTLPNamespaceBoundary exercises namespace authorization separately
// from the pod-label contract, including OpenShift's Project metadata proxy.
func TestCollectorOTLPNamespaceBoundary(t *testing.T) {
	tc, err := NewTestContext(t)
	require.NoError(t, err)
	tc.MonitoringNamespace = tc.detectMonitoringNamespace(t)
	(&MonitoringTestCtx{TestContext: tc}).ValidateCollectorOTLPNamespaceBoundary(t)
}

func (tc *MonitoringTestCtx) ValidateCollectorOTLPNamespaceBoundary(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: fmt.Sprintf("collector-namespace-auth-%x", time.Now().UnixNano()),
	}}
	createOTLPAuthorizationFixture(t, tc.TestContext, namespace)
	tenant := namespace.Name
	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: tenant},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"namespaces", "namespaces/status", "namespaces/finalize"},
				ResourceNames: []string{namespace.Name}, Verbs: []string{"get", "patch", "update"}},
			{APIGroups: []string{""}, Resources: []string{"namespaces"}, Verbs: []string{"create"}},
			{APIGroups: []string{"project.openshift.io"}, Resources: []string{"projects"},
				ResourceNames: []string{namespace.Name}, Verbs: []string{"get", "patch", "update"}},
		},
	}
	createOTLPAuthorizationFixture(t, tc.TestContext, role)
	createOTLPAuthorizationFixture(t, tc.TestContext, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: tenant},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects:   []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: "User", Name: tenant}},
	})

	// Give the test tenant namespace mutation rights so a denial must come from
	// admission, rather than merely proving that ordinary namespace RBAC denies it.
	for _, test := range []struct {
		name        string
		resource    string
		subresource string
		denial      string
	}{
		{name: "namespace", resource: "namespace", denial: "Changing monitoring.opendatahub.io/otlp requires"},
		{name: "namespace status", resource: "namespace", subresource: "status", denial: "Changing monitoring.opendatahub.io/otlp requires"},
		{name: "namespace finalize", resource: "namespace", subresource: "finalize", denial: "Changing monitoring.opendatahub.io/otlp requires"},
		// OpenShift rejects Project label changes as immutable before admission.
		{name: "OpenShift project", resource: "projects.project.openshift.io", denial: "field is immutable, try updating the namespace"},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := []string{"--as=" + tenant, "patch", test.resource, namespace.Name, "--type=merge",
				"--dry-run=server", "-p", `{"metadata":{"labels":{"monitoring.opendatahub.io/otlp":"true"}}}`}
			if test.subresource != "" {
				args = append(args, "--subresource="+test.subresource)
			}
			command := exec.CommandContext(tc.Context(), "oc", args...)
			if test.subresource == "finalize" {
				updated := namespace.DeepCopy()
				require.NoError(t, tc.Client().Get(tc.Context(), client.ObjectKeyFromObject(namespace), updated))
				updated.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}
				updated.Labels[otlpNamespaceLabel] = "true"
				body, err := json.Marshal(updated)
				require.NoError(t, err)
				args = []string{"--as=" + tenant, "replace", "--raw=/api/v1/namespaces/" + namespace.Name + "/finalize?dryRun=All", "-f", "-"}
				command = exec.CommandContext(tc.Context(), "oc", args...)
				command.Stdin = bytes.NewReader(body)
			}
			output, err := command.CombinedOutput()
			require.Error(t, err, "tenant must not enroll a namespace: %s", output)
			require.Contains(t, string(output), test.denial, "denial must come from enrollment protection")
		})
	}
	//nolint:gosec // generated test identity; no shell interpolation
	create := exec.CommandContext(tc.Context(), "oc", "--as="+tenant, "create", "--dry-run=server", "-f", "-")
	create.Stdin = strings.NewReader(`{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"` + tenant + `-create","labels":{"monitoring.opendatahub.io/otlp":"true"}}}`)
	output, err := create.CombinedOutput()
	require.Error(t, err, "tenant must not enroll a namespace at creation: %s", output)
	require.Contains(t, string(output), "Changing monitoring.opendatahub.io/otlp requires")

	endpoints := []string{
		fmt.Sprintf("https://data-science-collector-collector-headless.%s.svc.cluster.local:4317/", tc.MonitoringNamespace),
		fmt.Sprintf("https://data-science-collector-collector-headless.%s.svc.cluster.local:4318/v1/traces", tc.MonitoringNamespace),
	}
	callerLabels := otlpCallerNetworkPolicyLabels("llminferenceservice-workload")
	tc.runNetworkPolicyProbe(t, "unenrolled-spoof-denied", namespace.Name, callerLabels, endpoints, false)

	// Administrator enrollment enables the legitimate path. Unrelated tenant
	// metadata changes must still work without gaining enrollment privileges.
	require.NoError(t, tc.Client().Patch(tc.Context(), namespace, client.RawPatch(types.MergePatchType,
		[]byte(`{"metadata":{"labels":{"monitoring.opendatahub.io/otlp":"true"}}}`))))
	//nolint:gosec // generated fixture identity and namespace; no shell interpolation
	output, err = exec.CommandContext(tc.Context(), "oc", "--as="+tenant, "label", "namespace", namespace.Name, "qa-metadata=allowed").CombinedOutput()
	require.NoError(t, err, "unrelated namespace metadata must remain writable: %s", output)
	tc.runNetworkPolicyProbe(t, "enrolled-inference-allowed", namespace.Name, callerLabels, endpoints, true)

	//nolint:gosec // generated fixture identity and namespace; no shell interpolation
	output, err = exec.CommandContext(tc.Context(), "oc", "--as="+tenant, "label", "namespace", namespace.Name, otlpNamespaceLabel+"-").CombinedOutput()
	require.Error(t, err, "tenant must not revoke administrator enrollment: %s", output)
	require.Contains(t, string(output), "Changing monitoring.opendatahub.io/otlp requires")
	require.NoError(t, tc.Client().Patch(tc.Context(), namespace, client.RawPatch(types.MergePatchType,
		[]byte(`{"metadata":{"labels":{"monitoring.opendatahub.io/otlp":null}}}`))))
	tc.runNetworkPolicyProbe(t, "revoked-inference-denied", namespace.Name, callerLabels, endpoints, false)
}

func createOTLPAuthorizationFixture(t *testing.T, tc *TestContext, obj client.Object) {
	t.Helper()
	require.NoError(t, tc.Client().Create(tc.Context(), obj))
	t.Cleanup(func() {
		if err := tc.Client().Delete(tc.Context(), obj); err != nil && !errors.IsNotFound(err) {
			t.Errorf("cleaning up %T %s: %v", obj, obj.GetName(), err)
		}
	})
}
