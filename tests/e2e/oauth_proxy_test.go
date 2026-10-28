package e2e_test

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"
)

type oauthProxyConfig struct {
	namespace          string
	proxyName          string
	serviceAccountName string
	bindingName        string
	tlsSecretName      string
	cookieSecretName   string
	routeHost          string
	upstream           string
}

func createOAuthProxy(t *testing.T, tc *TestContext, config oauthProxyConfig) string {
	t.Helper()

	proxyNN := types.NamespacedName{Name: config.proxyName, Namespace: config.namespace}

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.ServiceAccount, types.NamespacedName{Name: config.serviceAccountName, Namespace: config.namespace}),
		WithMutateFunc(func(serviceAccount *unstructured.Unstructured) error {
			serviceAccount.SetAnnotations(map[string]string{
				"serviceaccounts.openshift.io/oauth-redirecturi." + config.proxyName: "https://" + config.routeHost + "/oauth2/callback",
			})
			return nil
		}),
	)
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{Name: config.bindingName}),
		WithMutateFunc(func(binding *unstructured.Unstructured) error {
			binding.Object["roleRef"] = map[string]any{
				"apiGroup": "rbac.authorization.k8s.io",
				"kind":     "ClusterRole",
				"name":     "system:auth-delegator",
			}
			binding.Object["subjects"] = []any{map[string]any{
				"kind":      "ServiceAccount",
				"name":      config.serviceAccountName,
				"namespace": config.namespace,
			}}
			return nil
		}),
	)

	var sessionSecret [32]byte
	_, err := rand.Read(sessionSecret[:])
	require.NoError(t, err, "failed to generate OAuth proxy session secret")
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Secret, types.NamespacedName{Name: config.cookieSecretName, Namespace: config.namespace}),
		WithMutateFunc(func(secret *unstructured.Unstructured) error {
			secret.Object["type"] = "Opaque"
			secret.Object["stringData"] = map[string]any{
				"session_secret": base64.StdEncoding.EncodeToString(sessionSecret[:]),
			}
			return nil
		}),
	)
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Service, proxyNN),
		WithMutateFunc(func(service *unstructured.Unstructured) error {
			service.SetAnnotations(map[string]string{"service.beta.openshift.io/serving-cert-secret-name": config.tlsSecretName})
			service.Object["spec"] = map[string]any{
				"selector": map[string]any{"app": config.proxyName},
				"ports":    []any{map[string]any{"name": "https", "port": int64(8443), "targetPort": int64(8443)}},
			}
			return nil
		}),
	)

	proxyArgs := []any{
		"--provider=openshift", "--https-address=:8443", "--http-address=", "--upstream=" + config.upstream,
		"--tls-cert=/etc/proxy/tls/tls.crt", "--tls-key=/etc/proxy/tls/tls.key",
		"--cookie-secret-file=/etc/proxy/secrets/session_secret",
		"--openshift-service-account=" + config.serviceAccountName,
		"--openshift-delegate-urls={\"/\":{}}",
		"--openshift-ca=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
		"--openshift-ca=/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt",
		"--upstream-ca=/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt",
	}
	proxyContainer := map[string]any{
		"name": "oauth-proxy", "image": "quay.io/openshift/origin-oauth-proxy:4.22.0", "args": proxyArgs,
		"ports": []any{map[string]any{"name": "https", "containerPort": int64(8443)}},
		"volumeMounts": []any{
			map[string]any{"name": "tls", "mountPath": "/etc/proxy/tls", "readOnly": true},
			map[string]any{"name": "session-secret", "mountPath": "/etc/proxy/secrets", "readOnly": true},
		},
	}
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Deployment, proxyNN),
		WithMutateFunc(func(deployment *unstructured.Unstructured) error {
			deployment.Object["spec"] = map[string]any{
				"replicas": int64(1),
				"selector": map[string]any{"matchLabels": map[string]any{"app": config.proxyName}},
				"template": map[string]any{
					"metadata": map[string]any{"labels": map[string]any{"app": config.proxyName}},
					"spec": map[string]any{
						"serviceAccountName": config.serviceAccountName,
						"containers":         []any{proxyContainer},
						"volumes": []any{
							map[string]any{"name": "tls", "secret": map[string]any{"secretName": config.tlsSecretName}},
							map[string]any{"name": "session-secret", "secret": map[string]any{"secretName": config.cookieSecretName}},
						},
					},
				},
			}
			return nil
		}),
	)
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Route, types.NamespacedName{Name: config.proxyName, Namespace: config.namespace}),
		WithMutateFunc(func(route *unstructured.Unstructured) error {
			route.Object["spec"] = map[string]any{
				"host": config.routeHost,
				"to":   map[string]any{"kind": "Service", "name": config.proxyName, "weight": int64(100)},
				"port": map[string]any{"targetPort": "https"},
				"tls":  map[string]any{"termination": "reencrypt", "insecureEdgeTerminationPolicy": "Redirect"},
			}
			return nil
		}),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Secret, types.NamespacedName{Name: config.tlsSecretName, Namespace: config.namespace}),
		WithCondition(jq.Match(`.data["tls.crt"] != null and .data["tls.key"] != null`)),
	)
	tc.EnsureResourceConditionMet(gvk.Deployment, proxyNN, "Available", metav1.ConditionTrue)
	g := gomega.NewWithT(t)
	g.Eventually(func() error {
		route := &unstructured.Unstructured{}
		route.SetGroupVersionKind(gvk.Route)
		if err := tc.Client().Get(tc.Context(), types.NamespacedName{Name: config.proxyName, Namespace: config.namespace}, route); err != nil {
			return err
		}
		ingress, found, err := unstructured.NestedSlice(route.Object, "status", "ingress")
		if err != nil || !found || len(ingress) == 0 {
			return fmt.Errorf("Route %s has not been admitted yet", config.routeHost)
		}
		entry, ok := ingress[0].(map[string]any)
		if !ok {
			return fmt.Errorf("Route %s admission status is malformed", config.routeHost)
		}
		conditions, _, _ := unstructured.NestedSlice(entry, "conditions")
		for _, raw := range conditions {
			condition, ok := raw.(map[string]any)
			if ok && condition["type"] == "Admitted" && condition["status"] == "False" {
				return fmt.Errorf("Route %s admission failed: %v", config.routeHost, condition["message"])
			}
		}
		return nil
	}).Should(gomega.Succeed(), "OAuth proxy Route %s should be admitted", config.routeHost)

	return "https://" + config.routeHost
}
