/*
Copyright 2026.

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

package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	pkgtls "github.com/opendatahub-io/odh-platform-utilities/framework/tls"
	configv1 "github.com/openshift/api/config/v1"
	commonTLS "github.com/openshift/controller-runtime-common/pkg/tls"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const testOpenSSLCipher = "ECDHE-RSA-AES128-GCM-SHA256"

func TestBuildWebhookTLSOpts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		profile          configv1.TLSProfileSpec
		adherence        configv1.TLSAdherencePolicy
		wantOpts         bool
		wantError        bool
		wantErrorSub     string
		wantMin          uint16
		wantCipherSuites []uint16
	}{
		{
			name:      "legacy unset keeps Go defaults (no TLSOpts)",
			profile:   configv1.TLSProfileSpec{},
			adherence: configv1.TLSAdherencePolicyNoOpinion,
			wantOpts:  false,
		},
		{
			name:      "legacy adhering keeps Go defaults (no TLSOpts)",
			profile:   configv1.TLSProfileSpec{},
			adherence: configv1.TLSAdherencePolicyLegacyAdheringComponentsOnly,
			wantOpts:  false,
		},
		{
			name: "strict applies the profile ciphers",
			profile: configv1.TLSProfileSpec{
				MinTLSVersion: configv1.VersionTLS12,
				Ciphers:       []string{testOpenSSLCipher},
				Groups:        []configv1.TLSGroup{configv1.TLSGroupX25519},
			},
			adherence:        configv1.TLSAdherencePolicyStrictAllComponents,
			wantOpts:         true,
			wantMin:          tls.VersionTLS12,
			wantCipherSuites: []uint16{tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256},
		},
		{
			name: "unknown adherence is strict",
			profile: configv1.TLSProfileSpec{
				MinTLSVersion: configv1.VersionTLS12,
				Ciphers:       []string{testOpenSSLCipher},
			},
			adherence:        configv1.TLSAdherencePolicy("FuturePolicy"),
			wantOpts:         true,
			wantMin:          tls.VersionTLS12,
			wantCipherSuites: []uint16{tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256},
		},
		{
			name: "strict honors Old profile minimum",
			profile: configv1.TLSProfileSpec{
				MinTLSVersion: configv1.VersionTLS10,
				Ciphers:       []string{testOpenSSLCipher},
			},
			adherence:        configv1.TLSAdherencePolicyStrictAllComponents,
			wantOpts:         true,
			wantMin:          tls.VersionTLS10,
			wantCipherSuites: []uint16{tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256},
		},
		{
			name: "strict rejects unsupported ciphers",
			profile: configv1.TLSProfileSpec{
				MinTLSVersion: configv1.VersionTLS12,
				Ciphers:       []string{"NOT-A-CIPHER", "ALSO-NOT-A-CIPHER"},
			},
			adherence:    configv1.TLSAdherencePolicyStrictAllComponents,
			wantError:    true,
			wantErrorSub: "no cipher suites",
		},
		{
			name: "strict allows TLS 1.3 without cipher restriction",
			profile: configv1.TLSProfileSpec{
				MinTLSVersion: configv1.VersionTLS13,
			},
			adherence: configv1.TLSAdherencePolicyStrictAllComponents,
			wantOpts:  true,
			wantMin:   tls.VersionTLS13,
		},
		{
			name: "strict rejects empty cipher list below TLS 1.3",
			profile: configv1.TLSProfileSpec{
				MinTLSVersion: configv1.VersionTLS12,
			},
			adherence:    configv1.TLSAdherencePolicyStrictAllComponents,
			wantError:    true,
			wantErrorSub: "no cipher suites",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts, _, err := buildWebhookTLSOpts(tt.profile, tt.adherence)
			if tt.wantError {
				require.Error(t, err)
				if tt.wantErrorSub != "" {
					require.ErrorContains(t, err, tt.wantErrorSub)
				}
				return
			}
			require.NoError(t, err)
			if !tt.wantOpts {
				assert.Empty(t, opts)
				return
			}
			require.NotEmpty(t, opts)

			cfg := &tls.Config{}
			for _, opt := range opts {
				opt(cfg)
			}
			assert.Equal(t, tt.wantMin, cfg.MinVersion)
			assert.Equal(t, []string{"http/1.1"}, cfg.NextProtos,
				"Strict webhook TLS must advertise HTTP/1.1 only")
			if tt.wantCipherSuites != nil {
				assert.Equal(t, tt.wantCipherSuites, cfg.CipherSuites)
			}
		})
	}
}

func TestBuildWebhookTLSOptsConversionFailureFailsClosed(t *testing.T) {
	t.Parallel()

	profile := *configv1.TLSProfiles[configv1.TLSProfileIntermediateType]
	opts, _, err := buildWebhookTLSOptsWith(
		profile,
		configv1.TLSAdherencePolicyStrictAllComponents,
		func(configv1.TLSProfileSpec) (func(*tls.Config), []string, error) {
			return nil, nil, errors.New("forced conversion failure")
		},
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "unable to build webhook TLS config from APIServer profile")
	require.ErrorContains(t, err, "forced conversion failure")
	assert.Nil(t, opts, "must not return a nil TLSOpt that would panic at webhook startup")
}

func TestResolveWebhookTLSWithClient(t *testing.T) {
	// Not parallel: evaluateWebhookTLS mutates the process-global degraded gauge.
	t.Run("legacy leaves webhook without TLSOpts", func(t *testing.T) {
		cli := newWebhookTLSClient(t, &configv1.TLSSecurityProfile{
			Type: configv1.TLSProfileModernType,
		}, configv1.TLSAdherencePolicyNoOpinion)

		result, err := evaluateWebhookTLS(context.Background(), cli)
		require.NoError(t, err)
		assert.Empty(t, result.TLSOpts)
		assert.True(t, result.Watchable)
		assert.False(t, result.Degraded)
		assert.Equal(t, configv1.TLSAdherencePolicyNoOpinion, result.AdherencePolicy)
	})

	t.Run("strict applies modern profile", func(t *testing.T) {
		cli := newWebhookTLSClient(t, &configv1.TLSSecurityProfile{
			Type: configv1.TLSProfileModernType,
		}, configv1.TLSAdherencePolicyStrictAllComponents)

		result, err := evaluateWebhookTLS(context.Background(), cli)
		require.NoError(t, err)
		require.NotEmpty(t, result.TLSOpts)
		assert.True(t, result.Watchable)
		assert.False(t, result.Degraded)
		assert.Equal(t, configv1.TLSAdherencePolicyStrictAllComponents, result.AdherencePolicy)
		assert.InDelta(t, 0.0, testutil.ToFloat64(webhookTLSDegraded), 0.001)

		cfg := applyTLSOpts(t, result.TLSOpts)
		assert.Equal(t, uint16(tls.VersionTLS13), cfg.MinVersion)
		assert.Empty(t, cfg.CipherSuites)
	})

	t.Run("unknown adherence applies cluster profile", func(t *testing.T) {
		cli := newWebhookTLSClient(t, &configv1.TLSSecurityProfile{
			Type: configv1.TLSProfileCustomType,
			Custom: &configv1.CustomTLSProfile{TLSProfileSpec: configv1.TLSProfileSpec{
				MinTLSVersion: configv1.VersionTLS12,
				Ciphers:       []string{testOpenSSLCipher},
			}},
		}, configv1.TLSAdherencePolicy("FuturePolicy"))

		result, err := evaluateWebhookTLS(context.Background(), cli)
		require.NoError(t, err)
		require.NotEmpty(t, result.TLSOpts)
		assert.False(t, result.Degraded)

		cfg := applyTLSOpts(t, result.TLSOpts)
		assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
		assert.Equal(t, []uint16{tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256}, cfg.CipherSuites)
	})

	t.Run("denied adherence read uses Strict Intermediate until recovery", func(t *testing.T) {
		denied := k8serr.NewForbidden(
			schema.GroupResource{Group: configv1.GroupName, Resource: "apiservers"},
			commonTLS.APIServerName,
			errors.New("access denied"),
		)

		result, err := evaluateWebhookTLS(context.Background(), webhookTLSReadErrorClient{err: denied})
		require.NoError(t, err)
		assertStrictStandIn(t, result)
	})

	t.Run("unauthorized adherence read uses Strict Intermediate until recovery", func(t *testing.T) {
		denied := k8serr.NewUnauthorized("authentication required")

		result, err := evaluateWebhookTLS(context.Background(), webhookTLSReadErrorClient{err: denied})
		require.NoError(t, err)
		assertStrictStandIn(t, result)
	})

	t.Run("transient API error uses Strict Intermediate not Go defaults", func(t *testing.T) {
		transient := k8serr.NewServiceUnavailable("apiserver unavailable")

		result, err := evaluateWebhookTLS(context.Background(), webhookTLSReadErrorClient{err: transient})
		require.NoError(t, err)
		assertStrictStandIn(t, result)
	})

	t.Run("unexpected API status uses Strict Intermediate not exit", func(t *testing.T) {
		result, err := evaluateWebhookTLS(context.Background(), webhookTLSReadErrorClient{
			err: k8serr.NewInternalError(errors.New("apiserver hiccup")),
		})
		require.NoError(t, err)
		assertStrictStandIn(t, result)
	})

	t.Run("invalid custom under Strict fails with clear error", func(t *testing.T) {
		cli := newWebhookTLSClient(t, &configv1.TLSSecurityProfile{
			Type: configv1.TLSProfileCustomType,
			Custom: &configv1.CustomTLSProfile{TLSProfileSpec: configv1.TLSProfileSpec{
				MinTLSVersion: configv1.VersionTLS12,
			}},
		}, configv1.TLSAdherencePolicyStrictAllComponents)

		_, err := evaluateWebhookTLS(context.Background(), cli)
		require.Error(t, err)
		require.ErrorContains(t, err, "no cipher suites")
	})

	t.Run("custom without spec under Strict fails closed", func(t *testing.T) {
		cli := newWebhookTLSClient(t, &configv1.TLSSecurityProfile{
			Type: configv1.TLSProfileCustomType,
		}, configv1.TLSAdherencePolicyStrictAllComponents)

		_, err := evaluateWebhookTLS(context.Background(), cli)
		require.Error(t, err)
		require.ErrorContains(t, err, "custom TLS profile has no custom specification")
	})

	t.Run("missing APIServer keeps Go defaults", func(t *testing.T) {
		cli := fake.NewClientBuilder().WithScheme(newWebhookTLSScheme(t)).Build()

		result, err := evaluateWebhookTLS(context.Background(), cli)
		require.NoError(t, err)
		assert.Empty(t, result.TLSOpts)
		assert.False(t, result.Watchable)
		assert.False(t, result.Degraded)
		assert.Equal(t, configv1.TLSAdherencePolicyNoOpinion, result.AdherencePolicy)
	})

	t.Run("NoMatch keeps Go defaults", func(t *testing.T) {
		cli := webhookTLSReadErrorClient{err: &meta.NoKindMatchError{
			GroupKind: schema.GroupKind{Group: "config.openshift.io", Kind: "APIServer"},
		}}

		result, err := evaluateWebhookTLS(context.Background(), cli)
		require.NoError(t, err)
		assert.Empty(t, result.TLSOpts)
		assert.False(t, result.Watchable)
		assert.False(t, result.Degraded)
	})
}

func TestConfigureWebhookTLSReload(t *testing.T) {
	t.Parallel()

	t.Run("degraded registers only the recovery poller", func(t *testing.T) {
		t.Parallel()
		var watcherCalls, pollerCalls int
		err := configureWebhookTLSReload(webhookTLSResult{Degraded: true, Watchable: false},
			func() error { watcherCalls++; return nil },
			func() error { pollerCalls++; return nil },
		)
		require.NoError(t, err)
		assert.Zero(t, watcherCalls)
		assert.Equal(t, 1, pollerCalls)
	})

	t.Run("successful watcher registers only the watcher", func(t *testing.T) {
		t.Parallel()
		var watcherCalls, pollerCalls int
		err := configureWebhookTLSReload(webhookTLSResult{Degraded: false},
			func() error { watcherCalls++; return nil },
			func() error { pollerCalls++; return nil },
		)
		require.NoError(t, err)
		assert.Equal(t, 1, watcherCalls)
		assert.Zero(t, pollerCalls)
	})

	t.Run("watcher setup error is fatal and does not register the poller", func(t *testing.T) {
		t.Parallel()
		var watcherCalls, pollerCalls int
		err := configureWebhookTLSReload(webhookTLSResult{Degraded: false, Watchable: true},
			func() error {
				watcherCalls++
				return errors.New("cannot watch")
			},
			func() error { pollerCalls++; return nil },
		)
		require.Error(t, err)
		require.ErrorContains(t, err, "unable to register TLS security profile watcher")
		assert.Equal(t, 1, watcherCalls)
		assert.Zero(t, pollerCalls)
	})

	t.Run("degraded poller registration error is returned", func(t *testing.T) {
		t.Parallel()
		err := configureWebhookTLSReload(webhookTLSResult{Degraded: true, Watchable: false},
			func() error { return nil },
			func() error { return errors.New("cannot add poller") },
		)
		require.Error(t, err)
		require.ErrorContains(t, err, "unable to register webhook TLS recovery poller")
	})
}

func TestNeedsWebhookTLSReload(t *testing.T) {
	t.Parallel()

	assert.False(t, needsWebhookTLSReload(webhookTLSResult{}))
	assert.False(t, needsWebhookTLSReload(webhookTLSResult{Watchable: false, Degraded: false}))
	assert.True(t, needsWebhookTLSReload(webhookTLSResult{Watchable: true}))
	assert.True(t, needsWebhookTLSReload(webhookTLSResult{Degraded: true, Watchable: false}),
		"degraded stand-in must register recovery even when Watchable is false")
}

func TestMarkWebhookTLSReloadRequested(t *testing.T) {
	t.Parallel()

	var reloadRequested atomic.Bool
	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	markWebhookTLSReloadRequested(&reloadRequested, cancel)
	assert.True(t, reloadRequested.Load())
	assert.Equal(t, context.Canceled, cancelCtx.Err())
}

func TestShouldHonorClusterTLSProfileForWebhookWatcher(t *testing.T) {
	t.Parallel()

	assert.False(t, pkgtls.ShouldHonorClusterTLSProfile(configv1.TLSAdherencePolicyNoOpinion),
		"Legacy must not restart on profile-only changes")
	assert.False(t, pkgtls.ShouldHonorClusterTLSProfile(configv1.TLSAdherencePolicyLegacyAdheringComponentsOnly))
	assert.True(t, pkgtls.ShouldHonorClusterTLSProfile(configv1.TLSAdherencePolicyStrictAllComponents))
	assert.True(t, pkgtls.ShouldHonorClusterTLSProfile(configv1.TLSAdherencePolicy("FuturePolicy")),
		"unknown adherence is treated as Strict for profile watches")
}

func TestClassifyWebhookTLSStandInLeave(t *testing.T) {
	t.Parallel()

	t.Run("keep while denied", func(t *testing.T) {
		t.Parallel()
		cli := webhookTLSReadErrorClient{err: k8serr.NewForbidden(
			schema.GroupResource{Group: configv1.GroupName, Resource: "apiservers"},
			commonTLS.APIServerName,
			errors.New("denied"),
		)}
		assert.Equal(t, webhookTLSStandInKeep, classifyWebhookTLSStandInLeave(context.Background(), cli))
	})

	t.Run("keep while transient", func(t *testing.T) {
		t.Parallel()
		cli := webhookTLSReadErrorClient{err: k8serr.NewServiceUnavailable("apiserver unavailable")}
		assert.Equal(t, webhookTLSStandInKeep, classifyWebhookTLSStandInLeave(context.Background(), cli))
	})

	t.Run("fail-closed leave when Strict custom profile is invalid", func(t *testing.T) {
		t.Parallel()
		cli := newWebhookTLSClient(t, &configv1.TLSSecurityProfile{
			Type: configv1.TLSProfileCustomType,
			Custom: &configv1.CustomTLSProfile{TLSProfileSpec: configv1.TLSProfileSpec{
				MinTLSVersion: configv1.VersionTLS12,
			}},
		}, configv1.TLSAdherencePolicyStrictAllComponents)
		assert.Equal(t, webhookTLSStandInLeaveFailClosed, classifyWebhookTLSStandInLeave(context.Background(), cli),
			"must leave Intermediate stand-in so the next process exit fails closed")
	})

	t.Run("healthy leave when APIServer is readable", func(t *testing.T) {
		t.Parallel()
		cli := newWebhookTLSClient(t, &configv1.TLSSecurityProfile{
			Type: configv1.TLSProfileModernType,
		}, configv1.TLSAdherencePolicyStrictAllComponents)
		assert.Equal(t, webhookTLSStandInLeaveHealthy, classifyWebhookTLSStandInLeave(context.Background(), cli))
	})

	t.Run("keep on unexpected API status error to avoid crash-loop", func(t *testing.T) {
		t.Parallel()
		cli := webhookTLSReadErrorClient{err: k8serr.NewInternalError(errors.New("apiserver hiccup"))}
		assert.Equal(t, webhookTLSStandInKeep, classifyWebhookTLSStandInLeave(context.Background(), cli),
			"unexpected API errors must keep Intermediate stand-in, not restart into exit(1)")
	})

	t.Run("keep when context is already canceled", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		cli := newWebhookTLSClient(t, &configv1.TLSSecurityProfile{
			Type: configv1.TLSProfileModernType,
		}, configv1.TLSAdherencePolicyStrictAllComponents)
		assert.Equal(t, webhookTLSStandInKeep, classifyWebhookTLSStandInLeave(ctx, cli))
	})

	t.Run("healthy leave when API absent (vanilla Kubernetes fallback)", func(t *testing.T) {
		t.Parallel()
		cli := fake.NewClientBuilder().WithScheme(newWebhookTLSScheme(t)).Build()
		assert.Equal(t, webhookTLSStandInLeaveHealthy, classifyWebhookTLSStandInLeave(context.Background(), cli))
	})

	t.Run("keep when API Get stalls past load timeout", func(t *testing.T) {
		t.Parallel()
		start := time.Now()
		assert.Equal(t, webhookTLSStandInKeep, classifyWebhookTLSStandInLeaveWithTimeout(
			context.Background(), webhookTLSBlockingGetClient{}, 50*time.Millisecond,
		), "deadline exceeded must keep Intermediate stand-in, not hang the poller")
		assert.Less(t, time.Since(start), time.Second, "stalled Get must be bounded by the load timeout")
	})
}

func TestIsAPIServerTLSAPIStatusError(t *testing.T) {
	t.Parallel()

	assert.False(t, isAPIServerTLSAPIStatusError(nil))
	assert.False(t, isAPIServerTLSAPIStatusError(errors.New("plain")))
	assert.False(t, isAPIServerTLSAPIStatusError(pkgtls.ErrTLSProfileHasNoCipherSuites),
		"validation sentinels are not API status — fail closed")
	assert.False(t, isAPIServerTLSAPIStatusError(
		fmt.Errorf("invalid strict APIServer TLS profile: %w", pkgtls.ErrCustomTLSProfileNil),
	), "wrapped validation must still fail closed")
	assert.True(t, isAPIServerTLSAPIStatusError(k8serr.NewForbidden(
		schema.GroupResource{Group: configv1.GroupName, Resource: "apiservers"},
		commonTLS.APIServerName,
		errors.New("denied"),
	)))
	assert.True(t, isAPIServerTLSAPIStatusError(k8serr.NewUnauthorized("no")))
	assert.True(t, isAPIServerTLSAPIStatusError(k8serr.NewInternalError(errors.New("boom"))),
		"unexpected API status must stand in, not crash-loop")
}

func TestIsRetriableWebhookTLSLoadError(t *testing.T) {
	t.Parallel()

	assert.False(t, isRetriableWebhookTLSLoadError(nil))
	assert.False(t, isRetriableWebhookTLSLoadError(errors.New("plain")))
	assert.False(t, isRetriableWebhookTLSLoadError(pkgtls.ErrTLSProfileHasNoCipherSuites))
	assert.True(t, isRetriableWebhookTLSLoadError(context.DeadlineExceeded))
	assert.True(t, isRetriableWebhookTLSLoadError(fmt.Errorf("wrap: %w", context.Canceled)))
	assert.True(t, isRetriableWebhookTLSLoadError(&net.DNSError{Err: "no such host", IsNotFound: true}))
}

func TestDecideWebhookTLSLoadRetriableTransportStandsIn(t *testing.T) {
	t.Parallel()

	t.Run("net.DNSError", func(t *testing.T) {
		t.Parallel()
		decision := decideWebhookTLSLoad(context.Background(), webhookTLSReadErrorClient{
			err: &net.DNSError{Err: "no such host", IsNotFound: true, Name: "kubernetes"},
		})
		assert.True(t, decision.standIn)
		require.NoError(t, decision.err)
		assert.Equal(t, "retriable APIServer TLS transport error", decision.reason)
	})

	t.Run("wrapped url.Error still stands in", func(t *testing.T) {
		t.Parallel()
		decision := decideWebhookTLSLoad(context.Background(), webhookTLSReadErrorClient{
			err: fmt.Errorf("get APIServer: %w", &url.Error{
				Op:  "Get",
				URL: "https://kubernetes.default.svc/apis/config.openshift.io/v1/apiservers/cluster",
				Err: &net.DNSError{Err: "no such host", IsNotFound: true, Name: "kubernetes.default.svc"},
			}),
		})
		assert.True(t, decision.standIn)
		require.NoError(t, decision.err)
		assert.Equal(t, "retriable APIServer TLS transport error", decision.reason)
	})

	t.Run("invalid Strict profile still fails closed", func(t *testing.T) {
		t.Parallel()
		cli := newWebhookTLSClient(t, &configv1.TLSSecurityProfile{
			Type: configv1.TLSProfileCustomType,
			Custom: &configv1.CustomTLSProfile{TLSProfileSpec: configv1.TLSProfileSpec{
				MinTLSVersion: configv1.VersionTLS12,
			}},
		}, configv1.TLSAdherencePolicyStrictAllComponents)
		decision := decideWebhookTLSLoad(context.Background(), cli)
		assert.False(t, decision.standIn)
		require.Error(t, decision.err)
		require.ErrorContains(t, decision.err, "no cipher suites")
	})
}

type fakeAPIReaderProvider struct {
	reader client.Reader
}

//nolint:ireturn // satisfies apiReaderProvider; concrete type not exported
func (f fakeAPIReaderProvider) GetAPIReader() client.Reader {
	return f.reader
}

func TestNewWebhookTLSReadRecoveryUsesAPIReader(t *testing.T) {
	t.Parallel()
	cli := newWebhookTLSClient(t, &configv1.TLSSecurityProfile{
		Type: configv1.TLSProfileIntermediateType,
	}, configv1.TLSAdherencePolicyStrictAllComponents)

	recovery := newWebhookTLSReadRecovery(fakeAPIReaderProvider{reader: cli}, func() {}, logr.Discard())
	require.NotNil(t, recovery)
	assert.Equal(t, cli, recovery.Reader)
	assert.Equal(t, webhookTLSRecoveryInterval, recovery.Interval)
}

func TestWebhookTLSReadRecoveryCancelsOnReadableAPIServer(t *testing.T) {
	t.Parallel()

	cli := newWebhookTLSClient(t, &configv1.TLSSecurityProfile{
		Type: configv1.TLSProfileIntermediateType,
	}, configv1.TLSAdherencePolicyStrictAllComponents)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var cancelled bool
	recovery := newWebhookTLSReadRecovery(fakeAPIReaderProvider{reader: cli}, func() {
		cancelled = true
		cancel()
	}, logr.Discard())
	recovery.Interval = time.Hour // first attempt is immediate
	require.NoError(t, recovery.Start(ctx))
	assert.True(t, cancelled)
	assert.False(t, recovery.NeedLeaderElection())
}

func TestWebhookTLSReadRecoveryCancelsWhenStrictBecomesInvalid(t *testing.T) {
	t.Parallel()

	cli := newWebhookTLSClient(t, &configv1.TLSSecurityProfile{
		Type: configv1.TLSProfileCustomType,
		Custom: &configv1.CustomTLSProfile{TLSProfileSpec: configv1.TLSProfileSpec{
			MinTLSVersion: configv1.VersionTLS12,
		}},
	}, configv1.TLSAdherencePolicyStrictAllComponents)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var cancelled bool
	recovery := newWebhookTLSReadRecovery(fakeAPIReaderProvider{reader: cli}, func() {
		cancelled = true
		cancel()
	}, logr.Discard())
	recovery.Interval = time.Hour
	require.NoError(t, recovery.Start(ctx))
	assert.True(t, cancelled, "must exit stand-in so the next process can fail closed")
	assert.Equal(t, webhookTLSStandInLeaveFailClosed, classifyWebhookTLSStandInLeave(context.Background(), cli))
}

func TestWebhookTLSReadRecoveryStaysWhileDenied(t *testing.T) {
	t.Parallel()

	cli := webhookTLSReadErrorClient{err: k8serr.NewForbidden(
		schema.GroupResource{Group: configv1.GroupName, Resource: "apiservers"},
		commonTLS.APIServerName,
		errors.New("denied"),
	)}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	var cancelled bool
	recovery := newWebhookTLSReadRecovery(fakeAPIReaderProvider{reader: cli}, func() {
		cancelled = true
	}, logr.Discard())
	recovery.Interval = 10 * time.Millisecond
	require.NoError(t, recovery.Start(ctx))
	assert.False(t, cancelled, "must not reload while APIServer get remains denied")
}

func TestWebhookTLSStandInCaveatDocumentsModernRisk(t *testing.T) {
	t.Parallel()
	assert.Contains(t, webhookTLSStandInCaveat, "Modern")
	assert.Contains(t, webhookTLSStandInCaveat, "TLS 1.2")
}

func assertStrictStandIn(t *testing.T, result webhookTLSResult) {
	t.Helper()
	require.NotEmpty(t, result.TLSOpts)
	assert.False(t, result.Watchable, "stand-in must not register SecurityProfileWatcher")
	assert.True(t, result.Degraded)
	assert.True(t, needsWebhookTLSReload(result), "stand-in must still register recovery poller")
	assert.InDelta(t, 1.0, testutil.ToFloat64(webhookTLSDegraded), 0.001)
	assert.Equal(t, configv1.TLSAdherencePolicyStrictAllComponents, result.AdherencePolicy)

	intermediate := *configv1.TLSProfiles[configv1.TLSProfileIntermediateType]
	assert.Equal(t, intermediate.MinTLSVersion, result.Profile.MinTLSVersion)

	cfg := applyTLSOpts(t, result.TLSOpts)
	assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)

	expected := applyTLSOpts(t, mustBuildStrictOpts(t, intermediate))
	assert.Equal(t, expected.CipherSuites, cfg.CipherSuites)
}

func mustBuildStrictOpts(t *testing.T, profile configv1.TLSProfileSpec) []func(*tls.Config) {
	t.Helper()
	opts, _, err := buildWebhookTLSOpts(profile, configv1.TLSAdherencePolicyStrictAllComponents)
	require.NoError(t, err)
	require.NotEmpty(t, opts)
	return opts
}

func applyTLSOpts(t *testing.T, opts []func(*tls.Config)) *tls.Config {
	t.Helper()
	cfg := &tls.Config{}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

func newWebhookTLSScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	clientScheme := runtime.NewScheme()
	require.NoError(t, configv1.Install(clientScheme))
	return clientScheme
}

func newWebhookTLSClient( //nolint:ireturn // test helper; concrete type not exported by controller-runtime/fake
	t *testing.T,
	profile *configv1.TLSSecurityProfile,
	adherence configv1.TLSAdherencePolicy,
) client.Client {
	t.Helper()

	apiServer := &configv1.APIServer{
		ObjectMeta: metav1.ObjectMeta{Name: commonTLS.APIServerName},
		Spec: configv1.APIServerSpec{
			TLSSecurityProfile: profile,
			TLSAdherence:       adherence,
		},
	}
	return fake.NewClientBuilder().WithScheme(newWebhookTLSScheme(t)).WithObjects(apiServer).Build()
}

type webhookTLSReadErrorClient struct {
	client.Client

	err error
}

func (c webhookTLSReadErrorClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.err
}

// webhookTLSBlockingGetClient blocks until the request context is canceled.
// Used to prove recovery classification applies webhookTLSLoadTimeout.
type webhookTLSBlockingGetClient struct {
	client.Client
}

func (webhookTLSBlockingGetClient) Get(ctx context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	<-ctx.Done()
	return ctx.Err()
}
