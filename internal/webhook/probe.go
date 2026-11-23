/*
Copyright 2025.

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

package webhook

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	probeAPIGroup   = "monitoring.rhobs"
	probeKind       = "PodMonitor"
	probeObjectName = "webhook-admission-probe"
)

var (
	webhookProbeLastSuccessTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "webhook_admission_probe_last_success_timestamp_seconds",
		Help: "Unix timestamp of the last successful webhook admission probe (tests that webhook is mutating PodMonitors/ServiceMonitors)",
	})

	webhookProbeLastAttemptTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "webhook_admission_probe_last_attempt_timestamp_seconds",
		Help: "Unix timestamp of the last webhook admission probe attempt (success or failure)",
	})

	webhookProbeFailuresTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "webhook_admission_probe_failures_total",
		Help: "Total number of webhook admission probe failures",
	})
)

func init() {
	metrics.Registry.MustRegister(
		webhookProbeLastSuccessTimestamp,
		webhookProbeLastAttemptTimestamp,
		webhookProbeFailuresTotal,
	)
}

// AdmissionProbe periodically tests the webhook admission path by creating/patching
// a test PodMonitor and verifying that the webhook adds the required scrape label.
// It exposes Prometheus metrics that the MonitoringWebhookAdmissionPathDown alert
// uses to detect webhook failures.
type AdmissionProbe struct {
	client       client.Client
	namespace    string
	interval     time.Duration
	retryTimeout time.Duration
	log          logr.Logger

	// startupTimeout is the write-retry budget for the first probe only. The webhook
	// server's local :9443 listener is up before this runnable starts, but the webhook
	// Service has no endpoints until the pod passes its readiness probe
	// (initialDelaySeconds + periodSeconds) and the endpoints are published. Until then,
	// with failurePolicy=Fail, the API server rejects the probe's write with
	// "no endpoints available". This budget must outlast that whole window so the first
	// probe after startup does not record a false failure; the short retryTimeout cannot.
	startupTimeout time.Duration
}

func NewAdmissionProbe(client client.Client, namespace string, interval time.Duration) *AdmissionProbe {
	return &AdmissionProbe{
		client:         client,
		namespace:      namespace,
		interval:       interval,
		retryTimeout:   10 * time.Second,
		startupTimeout: 2 * time.Minute,
		log:            ctrl.Log.WithName("webhook-probe"),
	}
}

func (p *AdmissionProbe) Start(ctx context.Context) error {
	p.log.Info("starting webhook admission probe", "interval", p.interval, "namespace", p.namespace)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	// First probe: the webhook Service endpoints are not published until this pod
	// becomes Ready, so retry a webhook-unreachable write for the longer startupTimeout
	// instead of retryTimeout, which would expire before endpoints exist.
	p.runProbe(ctx, p.startupTimeout)

	for {
		select {
		case <-ctx.Done():
			p.log.Info("stopping webhook admission probe")
			return nil
		case <-ticker.C:
			p.runProbe(ctx, p.retryTimeout)
		}
	}
}

// writeWithRetry performs a create/update and retries while the only problem is that
// the webhook is still unreachable (endpoints not yet published after the server
// started). Each attempt uses a context that expires with the budget, so an in-flight
// write cannot run past it. The parent context is only the shutdown signal: cancelling
// it returns without a probe failure, while reaching the budget returns the last error
// so the caller records one failure. Any other error — or still-unreachable after
// budget — is returned.
func (p *AdmissionProbe) writeWithRetry(ctx context.Context, budget time.Duration, write func(context.Context) error) error {
	deadline := time.Now().Add(budget)
	var err error
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A retry whose wait ran out the budget must not start another write.
		if err != nil && !time.Now().Before(deadline) {
			return err
		}
		remaining := time.Until(deadline)
		if remaining < 0 {
			remaining = 0
		}
		writeCtx, cancel := context.WithTimeout(ctx, remaining)
		err = write(writeCtx)
		cancel()
		if err == nil || !isWebhookUnavailable(err) {
			return err
		}
		wait := 500 * time.Millisecond
		if until := time.Until(deadline); until < wait {
			wait = until
		}
		if wait <= 0 {
			return err
		}
		p.log.V(1).Info("webhook not reachable yet, retrying probe write", "reason", err.Error())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

// isWebhookUnavailable reports whether err is the API server rejecting the write
// because the mutating webhook could not be reached (startup / endpoint lag),
// rather than a genuine admission or validation failure.
func isWebhookUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "no endpoints available") ||
		strings.Contains(msg, "failed calling webhook") ||
		strings.Contains(msg, "connection refused")
}

// runProbe executes one probe cycle: create/patch a test PodMonitor without the scrape label,
// wait for webhook mutation, verify the label was added. writeBudget bounds how long a
// webhook-unreachable write is retried before it counts as a failure (longer on the first
// probe to cover endpoint publication after startup).
func (p *AdmissionProbe) runProbe(ctx context.Context, writeBudget time.Duration) {
	now := time.Now()
	webhookProbeLastAttemptTimestamp.SetToCurrentTime()

	// The webhook namespaceSelector only matches namespaces labeled scrape=true,
	// so a canary in an unlabeled namespace can never be mutated.
	probeNamespace := &corev1.Namespace{}
	if err := p.client.Get(ctx, types.NamespacedName{Name: p.namespace}, probeNamespace); err != nil {
		p.log.Error(err, "failed to get probe namespace", "namespace", p.namespace)
		webhookProbeFailuresTotal.Inc()
		return
	}
	if probeNamespace.GetLabels()[labelMonitoring] != "true" {
		p.log.Error(nil, "probe namespace is not enrolled in webhook admission",
			"namespace", p.namespace, "required_label", labelMonitoring+"=true")
		webhookProbeFailuresTotal.Inc()
		return
	}

	probePodMonitor := &unstructured.Unstructured{}
	probePodMonitor.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   probeAPIGroup,
		Version: "v1",
		Kind:    probeKind,
	})
	probePodMonitor.SetNamespace(p.namespace)
	probePodMonitor.SetName(probeObjectName)

	// Set spec to a minimal valid PodMonitor (webhook only cares about labels, not spec)
	spec := map[string]any{
		"selector": map[string]any{
			"matchLabels": map[string]any{
				"app": "webhook-probe-nonexistent",
			},
		},
		"podMetricsEndpoints": []any{
			map[string]any{
				"port":     "metrics",
				"interval": "30s",
			},
		},
	}
	if err := unstructured.SetNestedMap(probePodMonitor.Object, spec, "spec"); err != nil {
		p.log.Error(err, "failed to set probe PodMonitor spec")
		webhookProbeFailuresTotal.Inc()
		return
	}

	// Important: Do NOT set the monitoring.opendatahub.io/scrape label here.
	// The webhook should add it. We verify this below.
	probePodMonitor.SetLabels(map[string]string{
		"platform.opendatahub.io/part-of": "monitoring",
		"webhook-probe":                   "true",
	})
	probePodMonitor.SetAnnotations(map[string]string{
		"description": "Webhook admission probe - periodically created to verify webhook is functioning",
	})

	// Create or update the probe PodMonitor
	key := types.NamespacedName{Namespace: p.namespace, Name: probeObjectName}
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(probePodMonitor.GroupVersionKind())

	err := p.client.Get(ctx, key, existing)
	if client.IgnoreNotFound(err) != nil {
		p.log.Error(err, "failed to get existing probe PodMonitor")
		webhookProbeFailuresTotal.Inc()
		return
	}

	if err != nil {
		if err := p.writeWithRetry(ctx, writeBudget, func(writeCtx context.Context) error {
			return p.client.Create(writeCtx, probePodMonitor)
		}); err != nil {
			if ctx.Err() != nil {
				return // shutting down, not a probe failure
			}
			p.log.Error(err, "failed to create probe PodMonitor")
			webhookProbeFailuresTotal.Inc()
			return
		}
		p.log.V(1).Info("created probe PodMonitor (webhook should mutate it)")
	} else {
		existingLabels := existing.GetLabels()
		delete(existingLabels, "monitoring.opendatahub.io/scrape")
		existing.SetLabels(existingLabels)

		existingAnnotations := existing.GetAnnotations()
		if existingAnnotations == nil {
			existingAnnotations = make(map[string]string)
		}
		existingAnnotations["probe-updated-at"] = now.Format(time.RFC3339)
		existing.SetAnnotations(existingAnnotations)

		if err := p.writeWithRetry(ctx, writeBudget, func(writeCtx context.Context) error {
			return p.client.Update(writeCtx, existing)
		}); err != nil {
			if ctx.Err() != nil {
				return // shutting down, not a probe failure
			}
			p.log.Error(err, "failed to update probe PodMonitor")
			webhookProbeFailuresTotal.Inc()
			return
		}
		p.log.V(1).Info("updated probe PodMonitor (removed scrape label, webhook should re-add it)")
	}

	// Read back and verify the webhook added the scrape label. With the shipped
	// failurePolicy=Fail, an unreachable webhook causes the write above to be rejected
	// (handled by writeWithRetry), not a silent unmutated admit. This poll still covers
	// a transient re-admission delay where the write succeeded but the mutation has not
	// yet settled, so it does not record a false failure.
	var labels map[string]string
	var lastErr error
	deadline := time.Now().Add(p.retryTimeout)
	for {
		current := &unstructured.Unstructured{}
		current.SetGroupVersionKind(probePodMonitor.GroupVersionKind())
		if err := p.client.Get(ctx, key, current); err != nil {
			lastErr = fmt.Errorf("failed to read back probe PodMonitor: %w", err)
		} else {
			labels = current.GetLabels()
			if scrapeLabel, ok := labels["monitoring.opendatahub.io/scrape"]; ok && scrapeLabel == "true" {
				lastErr = nil
				break
			}
			lastErr = errors.New("webhook did not add the scrape label")

			current.SetAnnotations(mergeAnnotation(current.GetAnnotations(), "probe-retry-at", time.Now().Format(time.RFC3339Nano)))
			if err := p.client.Update(ctx, current); err != nil && !k8serr.IsConflict(err) {
				lastErr = fmt.Errorf("failed to re-trigger webhook admission: %w", err)
			}
		}

		if time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}

	if lastErr != nil {
		p.log.Error(
			lastErr,
			"webhook admission probe failed",
			"expected_label", "monitoring.opendatahub.io/scrape=true",
			"actual_labels", labels,
		)
		webhookProbeFailuresTotal.Inc()
		return
	}

	// Success!
	webhookProbeLastSuccessTimestamp.SetToCurrentTime()
	p.log.V(1).Info("webhook admission probe succeeded", "labels", labels)
}

func mergeAnnotation(annotations map[string]string, key, value string) map[string]string {
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[key] = value
	return annotations
}
