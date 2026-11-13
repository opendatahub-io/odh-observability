package main

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	configv1 "github.com/openshift/api/config/v1"
	commonTLS "github.com/openshift/controller-runtime-common/pkg/tls"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

const (
	tlsWatcherTestNamespace = "tls-watcher-ha-test"
	tlsWatcherTestLeaderID  = "tls-watcher-ha-test"
)

func TestWebhookTLSWatcherRunsOnLeaderAndFollower(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("set KUBEBUILDER_ASSETS to run the envtest HA regression")
	}

	testEnv := &envtest.Environment{
		BinaryAssetsDirectory: os.Getenv("KUBEBUILDER_ASSETS"),
		CRDs:                  []*apiextensionsv1.CustomResourceDefinition{tlsWatcherAPIServerCRD()},
	}
	cfg, err := testEnv.Start()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, testEnv.Stop())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)

	clientset, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)
	_, err = clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: tlsWatcherTestNamespace},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	scheme := newWebhookTLSScheme(t)
	apiClient, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	apiServer := &configv1.APIServer{
		ObjectMeta: metav1.ObjectMeta{Name: commonTLS.APIServerName},
		Spec: configv1.APIServerSpec{
			TLSSecurityProfile: &configv1.TLSSecurityProfile{Type: configv1.TLSProfileIntermediateType},
			TLSAdherence:       configv1.TLSAdherencePolicyNoOpinion,
		},
	}
	require.NoError(t, apiClient.Create(ctx, apiServer))

	initialProfile := *configv1.TLSProfiles[configv1.TLSProfileIntermediateType]
	managers := []*tlsWatcherTestManager{
		startTLSWatcherTestManager(t, cfg, initialProfile, configv1.TLSAdherencePolicyNoOpinion),
		startTLSWatcherTestManager(t, cfg, initialProfile, configv1.TLSAdherencePolicyNoOpinion),
	}
	for _, running := range managers {
		require.True(t, running.manager.GetCache().WaitForCacheSync(ctx))
	}
	require.Eventually(t, func() bool {
		leaders := 0
		for _, running := range managers {
			select {
			case <-running.manager.Elected():
				leaders++
			default:
			}
		}
		return leaders == 1
	}, 15*time.Second, 50*time.Millisecond)

	require.NoError(t, apiClient.Get(ctx, client.ObjectKey{Name: commonTLS.APIServerName}, apiServer))
	apiServer.Spec.TLSSecurityProfile = &configv1.TLSSecurityProfile{Type: configv1.TLSProfileModernType}
	require.NoError(t, apiClient.Update(ctx, apiServer))
	for _, running := range managers {
		require.Eventually(t, func() bool {
			observed := &configv1.APIServer{}
			if err := running.manager.GetClient().Get(ctx, client.ObjectKey{Name: commonTLS.APIServerName}, observed); err != nil {
				return false
			}
			return observed.Spec.TLSSecurityProfile != nil &&
				observed.Spec.TLSSecurityProfile.Type == configv1.TLSProfileModernType
		}, 10*time.Second, 50*time.Millisecond)
	}
	require.Never(t, func() bool {
		for _, running := range managers {
			if running.reloadRequested.Load() || running.stopped() {
				return true
			}
		}
		return false
	}, 500*time.Millisecond, 10*time.Millisecond)

	require.NoError(t, apiClient.Get(ctx, client.ObjectKey{Name: commonTLS.APIServerName}, apiServer))
	apiServer.Spec.TLSAdherence = configv1.TLSAdherencePolicyStrictAllComponents
	require.NoError(t, apiClient.Update(ctx, apiServer))
	for _, running := range managers {
		require.Eventually(t, running.stopped, 10*time.Second, 50*time.Millisecond)
		require.True(t, running.reloadRequested.Load())
		require.NoError(t, running.startErr)
	}
}

type tlsWatcherTestManager struct {
	manager         ctrl.Manager
	reloadRequested *atomic.Bool
	cancel          context.CancelFunc
	done            chan struct{}
	startErr        error
}

func startTLSWatcherTestManager(
	t *testing.T,
	cfg *rest.Config,
	profile configv1.TLSProfileSpec,
	adherence configv1.TLSAdherencePolicy,
) *tlsWatcherTestManager {
	t.Helper()

	skipNameValidation := true
	mgr, err := ctrl.NewManager(rest.CopyConfig(cfg), ctrl.Options{
		Scheme:                        newWebhookTLSScheme(t),
		Metrics:                       metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress:        "0",
		LeaderElection:                true,
		LeaderElectionID:              tlsWatcherTestLeaderID,
		LeaderElectionNamespace:       tlsWatcherTestNamespace,
		LeaderElectionReleaseOnCancel: true,
		Controller: config.Controller{
			SkipNameValidation: &skipNameValidation,
		},
	})
	require.NoError(t, err)

	parentCtx, cancel := context.WithCancel(context.Background())
	reloadRequested := &atomic.Bool{}
	managerCtx, err := registerWebhookTLSReload(parentCtx, mgr, webhookTLSResult{
		Profile:         profile,
		AdherencePolicy: adherence,
		Watchable:       true,
	}, reloadRequested, logr.Discard())
	require.NoError(t, err)

	running := &tlsWatcherTestManager{
		manager:         mgr,
		reloadRequested: reloadRequested,
		cancel:          cancel,
		done:            make(chan struct{}),
	}
	go func() {
		running.startErr = mgr.Start(managerCtx)
		close(running.done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-running.done:
			require.NoError(t, running.startErr)
		case <-time.After(10 * time.Second):
			t.Error("manager did not stop")
		}
	})
	return running
}

func (m *tlsWatcherTestManager) stopped() bool {
	select {
	case <-m.done:
		return true
	default:
		return false
	}
}

func tlsWatcherAPIServerCRD() *apiextensionsv1.CustomResourceDefinition {
	preserveUnknown := true
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "apiservers." + configv1.GroupName},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: configv1.GroupName,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   "apiservers",
				Singular: "apiserver",
				Kind:     "APIServer",
				ListKind: "APIServerList",
			},
			Scope: apiextensionsv1.ClusterScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name:    configv1.GroupVersion.Version,
				Served:  true,
				Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"spec":   {Type: "object", XPreserveUnknownFields: &preserveUnknown},
							"status": {Type: "object", XPreserveUnknownFields: &preserveUnknown},
						},
					},
				},
			}},
		},
	}
}
