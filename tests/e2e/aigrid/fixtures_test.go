package aigrid

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func fixtureTestClient(ready bool, failSubscription bool, policyMode string) client.Client { //nolint:ireturn // test client matches the fixture API
	return fake.NewClientBuilder().WithScheme(runtime.NewScheme()).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			u, ok := obj.(*unstructured.Unstructured)
			if !ok {
				return k8serr.NewBadRequest("fixture requires an unstructured MaaS object")
			}
			if failSubscription && u.GetKind() == "MaaSSubscription" {
				return k8serr.NewForbidden(schema.GroupResource{Group: "maas.opendatahub.io", Resource: "maassubscriptions"}, u.GetName(), nil)
			}
			u.SetUID(types.UID(u.GetKind() + "-original"))
			if ready {
				u.Object["status"] = map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}
				if u.GetKind() == "MaaSSubscription" {
					observed := int64(2)
					enforced := "True"
					included := u.GetNamespace() + "/" + u.GetName()
					switch policyMode {
					case "stale token policy":
						observed = 1
					case "token policy not enforced":
						enforced = "False"
					case "wrong token policy subscription":
						included = u.GetNamespace() + "/previous-run"
					}
					policy := &unstructured.Unstructured{Object: map[string]any{
						"apiVersion": "kuadrant.io/v1alpha1", "kind": "TokenRateLimitPolicy",
						"metadata": map[string]any{"name": "test-token-policy", "namespace": "models", "generation": int64(2),
							"annotations": map[string]any{"maas.opendatahub.io/subscriptions": included}},
						"status": map[string]any{"observedGeneration": observed, "conditions": []any{
							map[string]any{"type": "Accepted", "status": "True"}, map[string]any{"type": "Enforced", "status": enforced},
						}},
					}}
					if err := c.Create(ctx, policy); err != nil && !k8serr.IsAlreadyExists(err) {
						return err
					}
					if err := unstructured.SetNestedSlice(u.Object, []any{map[string]any{
						"model": "model-ref", "name": "test-token-policy", "namespace": "models", "ready": true,
					}}, "status", "tokenRateLimitStatuses"); err != nil {
						return err
					}
				}
			}
			return c.Create(ctx, obj, opts...)
		},
	}).Build()
}

func TestFreshFixtureLifecycle(t *testing.T) {
	for _, scenario := range []string{"valid lifecycle", "wrong subscription", "ambiguous mint", "readiness timeout", "partial create",
		"stale token policy", "token policy not enforced", "wrong token policy subscription"} {
		t.Run(scenario, func(t *testing.T) {
			var fixture *Fixture
			var operations []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				operations = append(operations, r.Method+" "+r.URL.Path)
				if r.URL.Path == keyPath+"/search" {
					var body struct {
						Filters map[string]any `json:"filters"`
					}
					assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
					assert.Equal(t, true, body.Filters["includeEphemeral"])
					assert.Equal(t, fixture.Name, body.Filters[subscriptionField])
					_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{
						map[string]any{"id": "owned-key", nameField: fixture.Name, subscriptionField: fixture.Name, "ephemeral": true},
						map[string]any{"id": "unrelated-key", nameField: "someone-else", subscriptionField: "other-sub", "ephemeral": true},
					}})
					return
				}
				if r.Method == http.MethodDelete {
					assert.Equal(t, keyPath+"/owned-key", r.URL.Path)
					for _, obj := range fixture.objects {
						assert.NoError(t, fixture.Client.Get(r.Context(), client.ObjectKeyFromObject(obj), obj.DeepCopy()), "revoke before deleting CRs")
					}
					w.WriteHeader(http.StatusNoContent)
					return
				}
				var body map[string]any
				assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				assert.Equal(t, fixture.Name, body[subscriptionField])
				assert.Equal(t, true, body["ephemeral"])
				assert.Equal(t, "1h", body["expiresIn"])
				if scenario == "ambiguous mint" {
					_, _ = w.Write([]byte("invalid-json-private-key"))
					return
				}
				subscription := fixture.Name
				if scenario == "wrong subscription" {
					subscription = "other-subscription"
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"id": "owned-key", "key": "private-key",
					subscriptionField: subscription, "ephemeral": true})
			}))
			defer server.Close()
			c := fixtureTestClient(scenario != "readiness timeout", scenario == "partial create", scenario)
			fixture = NewFixture(c, MaaSClient{Client: server.Client(), URL: server.URL}, validConfig().Spokes[0])
			key, err := fixture.Prepare(t.Context(), time.Millisecond, 10*time.Millisecond)
			if scenario == "valid lifecycle" {
				require.NoError(t, err)
				require.Equal(t, "private-key", key)
				require.Equal(t, fixture.Name, fixture.Spoke.Subscription)
				for _, obj := range fixture.objects {
					require.Equal(t, fixture.Name, obj.GetLabels()[fixtureLabel])
				}
				organization, _, getErr := unstructured.NestedString(fixture.objects[1].Object, "spec", "organization", "id")
				require.NoError(t, getErr)
				require.Equal(t, fixture.Spoke.OrganizationID, organization)
			} else {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "private-key")
			}
			require.NoError(t, fixture.Cleanup(t.Context()))
			for _, obj := range fixture.objects {
				require.True(t, k8serr.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(obj), obj.DeepCopy())))
			}
			if scenario == "readiness timeout" || scenario == "partial create" || strings.Contains(scenario, "token policy") {
				require.Empty(t, operations)
			} else {
				require.Equal(t, "POST "+keyPath, operations[0])
				require.Equal(t, "DELETE "+keyPath+"/owned-key", operations[len(operations)-1])
			}
		})
	}
}

func TestFixtureDoesNotAdoptExistingResources(t *testing.T) {
	c := fixtureTestClient(true, false, "")
	f := NewFixture(c, MaaSClient{}, validConfig().Spokes[0])
	other := f.object("MaaSAuthPolicy", nil)
	other.SetLabels(map[string]string{fixtureLabel: "other-run"})
	require.NoError(t, c.Create(t.Context(), other))
	_, err := f.Prepare(t.Context(), time.Millisecond, time.Second)
	require.True(t, k8serr.IsAlreadyExists(err))
	require.NoError(t, f.Cleanup(t.Context()))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(other), other))
	require.Equal(t, "other-run", other.GetLabels()[fixtureLabel])
}

func TestFixtureCleanupRejectsReplacements(t *testing.T) {
	c := fixtureTestClient(true, false, "")
	f := NewFixture(c, MaaSClient{}, validConfig().Spokes[0])
	original := f.object("MaaSSubscription", nil)
	require.NoError(t, c.Create(t.Context(), original))
	f.objects = append(f.objects, original.DeepCopy())
	require.NoError(t, c.Delete(t.Context(), original))
	replacement := original.DeepCopy()
	replacement.SetResourceVersion("")
	require.NoError(t, c.Create(t.Context(), replacement))
	replacement.SetUID("replacement-uid")
	require.NoError(t, c.Update(t.Context(), replacement))
	err := f.Cleanup(t.Context())
	require.ErrorContains(t, err, "ownership changed")
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(replacement), replacement))
}

func TestFixturesHaveDistinctRunIdentities(t *testing.T) {
	seen := map[string]bool{}
	for range 10 {
		f := NewFixture(nil, MaaSClient{}, validConfig().Spokes[0])
		require.False(t, seen[f.Name])
		require.True(t, strings.HasPrefix(f.Name, "aigrid-"))
		seen[f.Name] = true
	}
	s := validConfig().Spokes[0]
	s.Subscription = ""
	_, err := RenderCase(s, "authorized_hits", "run", 0, time.Now())
	require.ErrorContains(t, err, "fresh subscription")
}

func TestMaaSManagementUsesExplicitContextAndEndpointCA(t *testing.T) {
	var redirected bool
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirected = true
		w.WriteHeader(http.StatusOK)
	}))
	defer other.Close()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer bootstrap-token", r.Header.Get("Authorization"))
		if r.URL.Path == "/maas-api/redirect" {
			http.Redirect(w, r, other.URL, http.StatusFound)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	dir := t.TempDir()
	kubeconfig := filepath.Join(dir, "kubeconfig")
	config := clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"source": {Server: "https://kubernetes.example", CertificateAuthorityData: []byte("different Kubernetes CA")}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"spoke-user": {Token: "bootstrap-token"}, "wrong-user": {Token: "wrong-token"}},
		Contexts:       map[string]*clientcmdapi.Context{"spoke1": {Cluster: "source", AuthInfo: "spoke-user"}, "wrong": {Cluster: "source", AuthInfo: "wrong-user"}},
		CurrentContext: "wrong",
	}
	require.NoError(t, clientcmd.WriteToFile(config, kubeconfig))
	t.Setenv("KUBECONFIG", kubeconfig)
	s := validConfig().Spokes[0]
	s.MaaS.APIURL = server.URL + "/maas-api"
	m, err := NewMaaSClient(s)
	require.NoError(t, err)
	require.Error(t, m.request(t.Context(), http.MethodGet, "/health", nil, nil), "untrusted MaaS certificate must fail")
	s.CAFile = filepath.Join(dir, "maas-ca.pem")
	require.NoError(t, os.WriteFile(s.CAFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600))
	m, err = NewMaaSClient(s)
	require.NoError(t, err)
	require.NoError(t, m.request(t.Context(), http.MethodGet, "/health", nil, nil))
	require.EqualError(t, m.request(t.Context(), http.MethodGet, "/redirect", nil, nil), "HTTP status 302")
	require.False(t, redirected, "MaaS management credentials must never follow redirects")
}
