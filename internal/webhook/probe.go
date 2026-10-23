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
	retryTimeout time.Duration // how long runProbe polls waiting for the webhook; overridable in tests
	log          logr.Logger
}

// NewAdmissionProbe creates a new webhook admission probe that runs every interval.
func NewAdmissionProbe(client client.Client, namespace string, interval time.Duration) *AdmissionProbe {
	return &AdmissionProbe{
		client:       client,
		namespace:    namespace,
		interval:     interval,
		retryTimeout: 10 * time.Second,
		log:          ctrl.Log.WithName("webhook-probe"),
	}
}

// Start implements manager.Runnable.
func (p *AdmissionProbe) Start(ctx context.Context) error {
	p.log.Info("starting webhook admission probe", "interval", p.interval, "namespace", p.namespace)

	webhookProbeLastSuccessTimestamp.SetToCurrentTime()

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	// Run one probe immediately on startup
	p.runProbe(ctx)

	for {
		select {
		case <-ctx.Done():
			p.log.Info("stopping webhook admission probe")
			return nil
		case <-ticker.C:
			p.runProbe(ctx)
		}
	}
}

// runProbe executes one probe cycle: create/patch a test PodMonitor without the scrape label,
// wait for webhook mutation, verify the label was added.
func (p *AdmissionProbe) runProbe(ctx context.Context) {
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
		if err := p.client.Create(ctx, probePodMonitor); err != nil {
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

		if err := p.client.Update(ctx, existing); err != nil {
			p.log.Error(err, "failed to update probe PodMonitor")
			webhookProbeFailuresTotal.Inc()
			return
		}
		p.log.V(1).Info("updated probe PodMonitor (removed scrape label, webhook should re-add it)")
	}

	// Read back and verify the webhook added the scrape label. The webhook server
	// (which runs in this same pod) may not be serving yet right after startup, and
	// with failurePolicy=Ignore the API server silently admits the object unmutated
	// until the webhook is reachable. Poll for a few seconds so the first probe after
	// startup — and any transient re-admission delay — does not record a false failure.
	var labels map[string]string
	var lastErr error
	deadline := time.Now().Add(p.retryTimeout)
	for {
		// Re-trigger admission on each attempt by patching the (possibly unmutated)
		// object, so a webhook that became ready after the initial write still runs.
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

			// Nudge admission again (UPDATE is covered by the webhook rules).
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

// mergeAnnotation returns the annotation map with key=value set, allocating a new
// map when the input is nil.
func mergeAnnotation(annotations map[string]string, key, value string) map[string]string {
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[key] = value
	return annotations
}
