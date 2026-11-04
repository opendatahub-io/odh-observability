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
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	pkgtls "github.com/opendatahub-io/odh-platform-utilities/framework/tls"
	configv1 "github.com/openshift/api/config/v1"
	commonTLS "github.com/openshift/controller-runtime-common/pkg/tls"
	"github.com/prometheus/client_golang/prometheus"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// webhookTLSLoadTimeout bounds APIServer TLS reads at startup and on each
// recovery attempt so a stalled Get cannot block the recovery poller forever.
const webhookTLSLoadTimeout = 10 * time.Second

// webhookTLSRecoveryInterval is how often a degraded stand-in retries resolving
// APIServer TLS settings for recovery.
const webhookTLSRecoveryInterval = 30 * time.Second

// webhookTLSStandInCaveat is the stand-in tradeoff when APIServer TLS settings
// are unreadable (denied, unauthorized, transient, or other Get failures).
// Intermediate allows TLS 1.2, so a Modern (TLS 1.3-only) cluster is temporarily
// weaker until settings load without stand-in. Intermediate is OpenShift's
// default and keeps Intermediate/Old clients working; Modern as stand-in would
// break those clients.
const webhookTLSStandInCaveat = "Strict Intermediate stand-in allows TLS 1.2; temporarily weaker than Modern (TLS 1.3-only) until APIServer is readable"

// webhookTLSDegraded is 1 while the admission webhook serves the Strict
// Intermediate stand-in. Scope is the webhook listener only — not metrics TLS.
var webhookTLSDegraded = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "odh_observability_webhook_tls_degraded",
	Help: "1 when the admission webhook serves Strict Intermediate stand-in until APIServer TLS settings load without degradation",
})

func init() {
	metrics.Registry.MustRegister(webhookTLSDegraded)
}

// webhookTLSResult is the TLS posture for the admission webhook listener
// (:9443 only; not metrics).
type webhookTLSResult struct {
	TLSOpts         []func(*tls.Config)
	Profile         configv1.TLSProfileSpec
	AdherencePolicy configv1.TLSAdherencePolicy
	// Watchable is true when LoadWithAdherence could read APIServer and a
	// SecurityProfileWatcher can be registered. Degraded stand-in never sets this.
	Watchable bool
	// Degraded is true when APIServer TLS settings were unreadable and the
	// webhook is serving Strict Intermediate until recovery. Covers Forbidden/
	// Unauthorized, transient OpenShift reads (UsedFallback+Watchable), and
	// other unexpected API Get failures. Invalid Strict profiles are not
	// degraded — resolve returns an error and the process must refuse to start.
	Degraded bool
}

// webhookTLSLoadDecision is the shared classify step for startup resolve and recovery.
type webhookTLSLoadDecision struct {
	standIn bool
	reason  string
	load    pkgtls.LoadResult
	err     error
}

// webhookTLSStandInLeaveReason is why degraded mode should stop serving Intermediate.
type webhookTLSStandInLeaveReason int

const (
	// webhookTLSStandInKeep: APIServer still unreadable — keep Intermediate.
	webhookTLSStandInKeep webhookTLSStandInLeaveReason = iota
	// webhookTLSStandInLeaveHealthy: settings readable — restart and apply real profile.
	webhookTLSStandInLeaveHealthy
	// webhookTLSStandInLeaveFailClosed: invalid Strict — restart so resolve exits 1.
	webhookTLSStandInLeaveFailClosed
)

// isAPIServerTLSAPIStatusError reports Kubernetes API status failures from an
// APIServer Get (Forbidden, Unauthorized, InternalError, …).
func isAPIServerTLSAPIStatusError(err error) bool {
	return err != nil && k8serr.ReasonForError(err) != metav1.StatusReasonUnknown
}

// isRetriableWebhookTLSLoadError reports non-status Load failures that should
// stand in (not crash-loop): deadlines and network errors that LoadWithAdherence
// did not already map to UsedFallback.
func isRetriableWebhookTLSLoadError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

// decideWebhookTLSLoad classifies APIServer TLS readability for webhook resolve
// and recovery.
//
//   - standIn: API status Get failures, retriable transport/deadline errors, or
//     transient OpenShift reads (UsedFallback+Watchable)
//   - err: any other non-API Load error (Strict profile validation today; unknown
//     library errors fail closed by policy) — refuse-start / recovery restart
//     into fail-closed
func decideWebhookTLSLoad(ctx context.Context, reader client.Reader) webhookTLSLoadDecision {
	result, err := pkgtls.LoadWithAdherence(ctx, reader)
	if err != nil {
		retriable := isRetriableWebhookTLSLoadError(err)
		if isAPIServerTLSAPIStatusError(err) || retriable {
			reason := "unexpected APIServer TLS API error"
			switch {
			case k8serr.IsForbidden(err) || k8serr.IsUnauthorized(err):
				reason = "denied or unauthorized APIServer TLS read"
			case retriable:
				reason = "retriable APIServer TLS transport error"
			}
			return webhookTLSLoadDecision{standIn: true, reason: reason}
		}
		return webhookTLSLoadDecision{
			err: fmt.Errorf("unable to load APIServer TLS profile and adherence for webhook: %w", err),
		}
	}
	if result.UsedFallback && result.Watchable {
		return webhookTLSLoadDecision{
			standIn: true,
			reason:  "transient API error",
			load:    result,
		}
	}
	return webhookTLSLoadDecision{load: result}
}

// resolveWebhookTLS loads the cluster TLS profile/adherence and builds webhook TLSOpts.
//
//   - Legacy / unset: no TLSOpts (Go defaults; unchanged from before this change)
//   - Strict / unknown: cluster profile min version + ciphers
//   - Denied, unauthorized, transient, or other unreadable APIServer Get: Strict Intermediate until recovery
//   - Invalid custom under Strict: error (caller must refuse to start)
//   - API absent (vanilla Kubernetes): no TLSOpts
func resolveWebhookTLS(ctx context.Context, scheme *runtime.Scheme, restCfg *rest.Config) (webhookTLSResult, error) {
	bootstrapCfg := rest.CopyConfig(restCfg)
	bootstrapCfg.Timeout = webhookTLSLoadTimeout
	bootstrapClient, err := client.New(bootstrapCfg, client.Options{Scheme: scheme})
	if err != nil {
		return webhookTLSResult{}, fmt.Errorf("unable to create bootstrap client for webhook TLS profile: %w", err)
	}
	return evaluateWebhookTLS(ctx, bootstrapClient)
}

func evaluateWebhookTLS(ctx context.Context, reader client.Reader) (webhookTLSResult, error) {
	logger := log.FromContext(ctx).WithName("webhook-tls")
	decision := decideWebhookTLSLoad(ctx, reader)
	if decision.err != nil {
		return webhookTLSResult{}, decision.err
	}
	if decision.standIn {
		return strictStandInWebhookTLS(logger, decision.reason)
	}

	result := decision.load
	if result.UsedFallback {
		logger.Info("APIServer TLS API or resource unavailable; webhook keeps Go defaults")
	}

	tlsOpts, unsupported, err := buildWebhookTLSOpts(result.Spec, result.AdherencePolicy)
	if err != nil {
		return webhookTLSResult{}, fmt.Errorf("unable to apply APIServer TLS profile to webhook: %w", err)
	}
	if len(unsupported) > 0 {
		logger.Info("some TLS settings from profile are not supported by Go", "unsupported", unsupported)
	}

	webhookTLSDegraded.Set(0)
	return webhookTLSResult{
		TLSOpts:         tlsOpts,
		Profile:         result.Spec,
		AdherencePolicy: result.AdherencePolicy,
		Watchable:       result.Watchable,
	}, nil
}

func strictStandInWebhookTLS(logger logr.Logger, reason string) (webhookTLSResult, error) {
	logger.Info("APIServer TLS adherence unreadable; using Strict Intermediate posture until recovery",
		"reason", reason,
		"caveat", webhookTLSStandInCaveat)

	intermediate := *configv1.TLSProfiles[configv1.TLSProfileIntermediateType]
	tlsOpts, unsupported, err := buildWebhookTLSOpts(intermediate, configv1.TLSAdherencePolicyStrictAllComponents)
	if err != nil {
		return webhookTLSResult{}, fmt.Errorf("unable to apply Strict Intermediate webhook TLS after %s: %w", reason, err)
	}
	if len(unsupported) > 0 {
		logger.Info("some TLS settings from Strict Intermediate posture are not supported by Go", "unsupported", unsupported)
	}
	webhookTLSDegraded.Set(1)
	return webhookTLSResult{
		TLSOpts:         tlsOpts,
		Profile:         intermediate,
		AdherencePolicy: configv1.TLSAdherencePolicyStrictAllComponents,
		Watchable:       false,
		Degraded:        true,
	}, nil
}

// profileTLSConfigurer converts a Strict-validated TLS profile into TLSOpts.
type profileTLSConfigurer func(configv1.TLSProfileSpec) (func(*tls.Config), []string, error)

// buildWebhookTLSOpts returns webhook TLS option funcs for the given adherence.
// Legacy / unset returns nil so the webhook keeps Go's default TLS settings.
func buildWebhookTLSOpts(profile configv1.TLSProfileSpec, adherence configv1.TLSAdherencePolicy) ([]func(*tls.Config), []string, error) {
	return buildWebhookTLSOptsWith(profile, adherence, pkgtls.ConfigFromProfileWithError)
}

func buildWebhookTLSOptsWith(
	profile configv1.TLSProfileSpec,
	adherence configv1.TLSAdherencePolicy,
	configure profileTLSConfigurer,
) ([]func(*tls.Config), []string, error) {
	if !pkgtls.ShouldHonorClusterTLSProfile(adherence) {
		return nil, nil, nil
	}

	if err := pkgtls.ValidateStrictTLSProfile(profile); err != nil {
		return nil, nil, fmt.Errorf("invalid APIServer TLS profile for webhook under Strict adherence: %w", err)
	}

	tlsConfigFn, unsupported, err := configure(profile)
	if err != nil {
		// Conversion build failures are not profile-content errors; fail closed
		// rather than returning a nil TLSOpt that would panic at webhook startup.
		return nil, nil, fmt.Errorf("unable to build webhook TLS config from APIServer profile: %w", err)
	}
	// HTTP/1.1-only: override controller-runtime's default NextProtos=["h2"].
	// Prevents Strict Custom TLS 1.2 cipher lists without AES-128-GCM from
	// negotiating h2 then failing Go's HTTP/2 cipher check. TLSOpts equivalent
	// of http.Server.TLSNextProto = non-nil empty map (webhook Options has no Server hook).
	return []func(*tls.Config){
		tlsConfigFn,
		commonTLS.SetNextProtos(commonTLS.HTTP1NextProtos...),
	}, unsupported, nil
}

// needsWebhookTLSReload reports whether watcher or recovery poller should run.
func needsWebhookTLSReload(tlsResult webhookTLSResult) bool {
	return tlsResult.Degraded || tlsResult.Watchable
}

// configureWebhookTLSReload decides watcher vs recovery-poller registration.
// Degraded stand-in always uses the recovery poller. Otherwise watcher setup is
// required; failures are fatal and main owns cancel/exit.
func configureWebhookTLSReload(tlsResult webhookTLSResult, setupWatcher func() error, addRecoveryPoller func() error) error {
	if tlsResult.Degraded {
		if addErr := addRecoveryPoller(); addErr != nil {
			return fmt.Errorf("unable to register webhook TLS recovery poller: %w", addErr)
		}
		return nil
	}

	if err := setupWatcher(); err != nil {
		return fmt.Errorf("unable to register TLS security profile watcher for webhook: %w", err)
	}
	return nil
}

// apiReaderProvider is satisfied by manager.Manager (GetAPIReader).
type apiReaderProvider interface {
	GetAPIReader() client.Reader
}

// newWebhookTLSReadRecovery builds the degraded-mode poller. Callers must pass
// a live API reader (mgr.GetAPIReader()), never the cache-backed client.
func newWebhookTLSReadRecovery(mgr apiReaderProvider, cancel context.CancelFunc, logger logr.Logger) *webhookTLSReadRecovery {
	return &webhookTLSReadRecovery{
		Reader:   mgr.GetAPIReader(),
		Cancel:   cancel,
		Interval: webhookTLSRecoveryInterval,
		Log:      logger,
	}
}

// webhookTLSReadRecovery polls until stand-in is no longer required. Cancel
// stops the manager so kube restarts the process with re-resolved TLS.
type webhookTLSReadRecovery struct {
	Reader   client.Reader
	Cancel   context.CancelFunc
	Interval time.Duration
	Log      logr.Logger
}

func (r *webhookTLSReadRecovery) Start(ctx context.Context) error {
	interval := r.Interval
	if interval <= 0 {
		interval = webhookTLSRecoveryInterval
	}
	logger := r.Log.WithName("webhook-tls-recovery")

	if r.requestLeaveIfReady(ctx, logger) {
		return nil
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if r.requestLeaveIfReady(ctx, logger) {
				return nil
			}
			logger.Info("webhook TLS still degraded; Strict Intermediate stand-in remains active",
				"caveat", webhookTLSStandInCaveat)
		}
	}
}

func (r *webhookTLSReadRecovery) requestLeaveIfReady(ctx context.Context, logger logr.Logger) bool {
	switch reason := classifyWebhookTLSStandInLeave(ctx, r.Reader); reason {
	case webhookTLSStandInKeep:
		return false
	case webhookTLSStandInLeaveFailClosed:
		logger.Info("APIServer TLS profile is invalid under Strict; requesting process exit so resolve can fail closed")
		r.Cancel()
		return true
	case webhookTLSStandInLeaveHealthy:
		logger.Info("APIServer TLS no longer requires stand-in; requesting process exit to reload webhook TLS")
		r.Cancel()
		return true
	default:
		logger.Info("unexpected webhook TLS stand-in leave reason; keeping Intermediate stand-in",
			"reason", int(reason))
		return false
	}
}

func (r *webhookTLSReadRecovery) NeedLeaderElection() bool {
	return false
}

// classifyWebhookTLSStandInLeave decides whether degraded Intermediate can stop.
// Each attempt uses webhookTLSLoadTimeout so a stalled API Get cannot block the
// recovery poller past one interval tick.
func classifyWebhookTLSStandInLeave(ctx context.Context, cli client.Reader) webhookTLSStandInLeaveReason {
	return classifyWebhookTLSStandInLeaveWithTimeout(ctx, cli, webhookTLSLoadTimeout)
}

func classifyWebhookTLSStandInLeaveWithTimeout(
	ctx context.Context,
	cli client.Reader,
	timeout time.Duration,
) webhookTLSStandInLeaveReason {
	if ctx.Err() != nil {
		return webhookTLSStandInKeep
	}
	loadCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	decision := decideWebhookTLSLoad(loadCtx, cli)
	if decision.standIn {
		return webhookTLSStandInKeep
	}
	if decision.err != nil {
		return webhookTLSStandInLeaveFailClosed
	}
	return webhookTLSStandInLeaveHealthy
}

// markWebhookTLSReloadRequested records that manager shutdown should exit 0 so
// kube restarts the process and re-resolves webhook TLS.
func markWebhookTLSReloadRequested(reloadRequested *atomic.Bool, cancel context.CancelFunc) {
	reloadRequested.Store(true)
	cancel()
}
