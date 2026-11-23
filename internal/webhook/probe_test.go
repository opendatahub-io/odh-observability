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
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
)

const probeTestNamespace = "probe-ns"

var enrolledNamespaceLabels = map[string]string{labelMonitoring: "true"}

// --- helpers -----------------------------------------------------------------

// gaugeValue reads the current float64 value of a prometheus.Gauge/Counter via
// the dto.Metric Write method so we don't import the internal registry directly.
func gaugeValue(t *testing.T, g interface {
	Write(m *dto.Metric) error
}) float64 {
	t.Helper()
	m := &dto.Metric{}
	if err := g.Write(m); err != nil {
		t.Fatalf("reading metric: %v", err)
	}
	if m.GetGauge() != nil {
		return m.GetGauge().GetValue()
	}
	if m.GetCounter() != nil {
		return m.GetCounter().GetValue()
	}
	return 0
}

// newTestProbe returns an AdmissionProbe whose retryTimeout is short so failure
// tests don't take 10 seconds.
func newTestProbe(c client.Client) *AdmissionProbe {
	p := NewAdmissionProbe(c, probeTestNamespace, time.Minute)
	p.retryTimeout = 200 * time.Millisecond
	return p
}

// selectorAwareWebhookClient returns a fake client that injects the scrape label
// the way the API server dispatches to the mutating webhook configured in
// charts/odh-observability/templates/webhook.yaml: only when the object's
// namespace is labeled monitoring.opendatahub.io/scrape=true (namespaceSelector)
// and the incoming object does not already carry the label (objectSelector).
func selectorAwareWebhookClient( //nolint:ireturn // test helper; concrete type not exported by controller-runtime/fake
	t *testing.T,
	namespaceLabels map[string]string,
	objs ...client.Object,
) client.Client {
	t.Helper()

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: probeTestNamespace, Labels: namespaceLabels}}

	admit := func(ctx context.Context, c client.WithWatch, obj client.Object) error {
		if _, ok := obj.GetLabels()[labelMonitoring]; ok {
			return nil
		}
		namespace := &corev1.Namespace{}
		if err := c.Get(ctx, client.ObjectKey{Name: obj.GetNamespace()}, namespace); err != nil {
			return client.IgnoreNotFound(err)
		}
		if namespace.Labels[labelMonitoring] != "true" {
			return nil
		}
		labels := obj.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels[labelMonitoring] = "true"
		obj.SetLabels(labels)
		return nil
	}

	return fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(append([]client.Object{ns}, objs...)...).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if err := admit(ctx, c, obj); err != nil {
					return err
				}
				return c.Create(ctx, obj, opts...)
			},
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if err := admit(ctx, c, obj); err != nil {
					return err
				}
				return c.Update(ctx, obj, opts...)
			},
		}).
		Build()
}

func storedProbeLabels(t *testing.T, c client.Client) map[string]string {
	t.Helper()
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk.PodMonitor)
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: probeTestNamespace, Name: probeObjectName}, obj); err != nil {
		t.Fatalf("reading probe PodMonitor: %v", err)
	}
	return obj.GetLabels()
}

// --- mergeAnnotation ---------------------------------------------------------

func TestMergeAnnotation_NilMap(t *testing.T) {
	got := mergeAnnotation(nil, "k", "v")
	if got["k"] != "v" {
		t.Errorf("expected k=v, got %v", got)
	}
}

func TestMergeAnnotation_ExistingMap(t *testing.T) {
	ann := map[string]string{"existing": "yes"}
	got := mergeAnnotation(ann, "new-key", "new-val")
	if got["existing"] != "yes" {
		t.Errorf("existing key lost: %v", got)
	}
	if got["new-key"] != "new-val" {
		t.Errorf("expected new-key=new-val, got %v", got)
	}
}

func TestMergeAnnotation_Overwrite(t *testing.T) {
	ann := map[string]string{"key": "old"}
	got := mergeAnnotation(ann, "key", "new")
	if got["key"] != "new" {
		t.Errorf("expected key=new, got %v", got)
	}
}

// --- runProbe ----------------------------------------------------------------

func TestRunProbe_EnrolledNamespace_Create(t *testing.T) {
	c := selectorAwareWebhookClient(t, enrolledNamespaceLabels)
	p := newTestProbe(c)

	beforeFailures := gaugeValue(t, webhookProbeFailuresTotal)
	p.runProbe(context.Background(), p.retryTimeout)

	if got := gaugeValue(t, webhookProbeFailuresTotal) - beforeFailures; got != 0 {
		t.Errorf("expected no new failures, got %v", got)
	}
	if gaugeValue(t, webhookProbeLastSuccessTimestamp) == 0 {
		t.Error("expected non-zero last success timestamp after successful probe")
	}
	if got := storedProbeLabels(t, c)[labelMonitoring]; got != "true" {
		t.Errorf("expected stored canary to carry %s=true, got %q", labelMonitoring, got)
	}
}

func TestRunProbe_EnrolledNamespace_Update(t *testing.T) {
	c := selectorAwareWebhookClient(t, enrolledNamespaceLabels)
	p := newTestProbe(c)

	// First run creates the canary; the second exercises the Update branch, where
	// the probe must strip the label so the objectSelector lets the webhook run again.
	p.runProbe(context.Background(), p.retryTimeout)
	beforeFailures := gaugeValue(t, webhookProbeFailuresTotal)
	p.runProbe(context.Background(), p.retryTimeout)

	if got := gaugeValue(t, webhookProbeFailuresTotal) - beforeFailures; got != 0 {
		t.Errorf("expected no new failures on second run, got %v", got)
	}
}

func TestRunProbe_UnenrolledNamespace(t *testing.T) {
	tests := []struct {
		name            string
		namespaceLabels map[string]string
	}{
		{name: "no scrape label", namespaceLabels: nil},
		{name: "scrape label not true", namespaceLabels: map[string]string{labelMonitoring: "false"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := selectorAwareWebhookClient(t, tt.namespaceLabels)
			p := newTestProbe(c)

			beforeFailures := gaugeValue(t, webhookProbeFailuresTotal)
			p.runProbe(context.Background(), p.retryTimeout)

			if got := gaugeValue(t, webhookProbeFailuresTotal) - beforeFailures; got != 1 {
				t.Errorf("expected exactly one new failure for unenrolled namespace, got %v", got)
			}
			obj := &unstructured.Unstructured{}
			obj.SetGroupVersionKind(gvk.PodMonitor)
			err := c.Get(context.Background(), client.ObjectKey{Namespace: probeTestNamespace, Name: probeObjectName}, obj)
			if !k8serr.IsNotFound(err) {
				t.Errorf("expected no canary to be written in an unenrolled namespace, got err=%v", err)
			}
		})
	}
}

func TestRunProbe_MissingNamespace(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newTestScheme()).Build()
	p := newTestProbe(c)

	beforeFailures := gaugeValue(t, webhookProbeFailuresTotal)
	p.runProbe(context.Background(), p.retryTimeout)

	if got := gaugeValue(t, webhookProbeFailuresTotal) - beforeFailures; got != 1 {
		t.Errorf("expected exactly one new failure when the probe namespace is missing, got %v", got)
	}
}

func TestRunProbe_ExistingCanaryWithStaleLabel(t *testing.T) {
	// A canary left with scrape=false would be skipped by the objectSelector
	// forever unless the probe removes the label before re-admission.
	stale := newPodMonitor(probeTestNamespace, probeObjectName, map[string]string{labelMonitoring: "false"})
	c := selectorAwareWebhookClient(t, enrolledNamespaceLabels, stale)
	p := newTestProbe(c)

	beforeFailures := gaugeValue(t, webhookProbeFailuresTotal)
	p.runProbe(context.Background(), p.retryTimeout)

	if got := gaugeValue(t, webhookProbeFailuresTotal) - beforeFailures; got != 0 {
		t.Errorf("expected no new failures, got %v", got)
	}
	if got := storedProbeLabels(t, c)[labelMonitoring]; got != "true" {
		t.Errorf("expected stale canary to be re-labeled %s=true, got %q", labelMonitoring, got)
	}
}

// --- Start: context cancellation --------------------------------------------

func TestStart_DoesNotRecordSuccessBeforeProbe(t *testing.T) {
	webhookProbeLastSuccessTimestamp.Set(0)
	c := selectorAwareWebhookClient(t, nil)
	p := NewAdmissionProbe(c, probeTestNamespace, time.Hour)
	p.retryTimeout = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}
	if got := gaugeValue(t, webhookProbeLastSuccessTimestamp); got != 0 {
		t.Errorf("expected no success timestamp before a successful probe, got %v", got)
	}
}

func TestStart_ContextCancellation(t *testing.T) {
	c := selectorAwareWebhookClient(t, enrolledNamespaceLabels)
	p := NewAdmissionProbe(c, probeTestNamespace, 100*time.Millisecond)
	p.retryTimeout = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- p.Start(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Start returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after context cancellation")
	}
}

// --- isWebhookUnavailable ----------------------------------------------------

func TestIsWebhookUnavailable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"no endpoints", errors.New(`Internal error occurred: failed calling webhook "podmonitor-injector.opendatahub.io": no endpoints available for service "odh-observability-webhook"`), true},
		{"failed calling webhook", errors.New(`failed calling webhook "x": context deadline exceeded`), true},
		{"connection refused", errors.New("Post https://svc:9443/mutate: connection refused"), true},
		{"genuine admission denial", errors.New("admission webhook denied the request: bad spec"), false},
		{"unrelated", errors.New("some other error"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isWebhookUnavailable(tt.err); got != tt.want {
				t.Errorf("isWebhookUnavailable(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// --- writeWithRetry / startup race -------------------------------------------

// TestRunProbe_StartupRace_NoFalseFailure simulates the controller start: the
// first few writes are rejected because the webhook Service has no endpoints yet,
// then endpoints appear and the write succeeds. With the first-probe startupTimeout
// budget the probe retries and records no failure.
func TestRunProbe_StartupRace_NoFalseFailure(t *testing.T) {
	base := selectorAwareWebhookClient(t, enrolledNamespaceLabels)

	var attempts int
	c := &countingUnavailableClient{Client: base, failUntil: 2, attempts: &attempts}

	p := newTestProbe(c)
	p.startupTimeout = 2 * time.Second // allow a few 500ms retries

	beforeFailures := gaugeValue(t, webhookProbeFailuresTotal)
	p.runProbe(context.Background(), p.startupTimeout)

	if got := gaugeValue(t, webhookProbeFailuresTotal) - beforeFailures; got != 0 {
		t.Errorf("expected no failure during startup race, got %v (attempts=%d)", got, attempts)
	}
	if attempts < 2 {
		t.Errorf("expected the write to be retried, attempts=%d", attempts)
	}
}

// seedCanary returns an existing probe PodMonitor already carrying the scrape label,
// so runProbe takes the Update branch rather than Create.
func seedCanary() *unstructured.Unstructured {
	o := &unstructured.Unstructured{}
	o.SetGroupVersionKind(gvk.PodMonitor)
	o.SetNamespace(probeTestNamespace)
	o.SetName(probeObjectName)
	o.SetLabels(map[string]string{labelMonitoring: "true", "webhook-probe": "true"})
	return o
}

// TestRunProbe_StartupRace_Update_NoFalseFailure is the restart case from the ticket:
// a canary already exists, so the first probe's write is an Update. It is rejected
// until endpoints publish ("failed to update probe PodMonitor"), then succeeds within
// startupTimeout and records no failure.
func TestRunProbe_StartupRace_Update_NoFalseFailure(t *testing.T) {
	base := selectorAwareWebhookClient(t, enrolledNamespaceLabels, seedCanary())

	var attempts int
	c := &countingUnavailableClient{Client: base, failUntil: 2, attempts: &attempts}

	p := newTestProbe(c)
	p.startupTimeout = 2 * time.Second

	beforeFailures := gaugeValue(t, webhookProbeFailuresTotal)
	p.runProbe(context.Background(), p.startupTimeout)

	if got := gaugeValue(t, webhookProbeFailuresTotal) - beforeFailures; got != 0 {
		t.Errorf("expected no failure during startup Update race, got %v (attempts=%d)", got, attempts)
	}
	if attempts < 2 {
		t.Errorf("expected the Update to be retried, attempts=%d", attempts)
	}
}

// TestRunProbe_ContextCancelled_NoFailure confirms that a write aborted by context
// cancellation (shutdown) is not counted as a probe failure.
func TestRunProbe_ContextCancelled_NoFailure(t *testing.T) {
	base := selectorAwareWebhookClient(t, enrolledNamespaceLabels)

	var attempts int
	c := &countingUnavailableClient{Client: base, failUntil: 1 << 30, attempts: &attempts}

	p := newTestProbe(c)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	beforeFailures := gaugeValue(t, webhookProbeFailuresTotal)
	p.runProbe(ctx, time.Minute)

	if got := gaugeValue(t, webhookProbeFailuresTotal) - beforeFailures; got != 0 {
		t.Errorf("expected no failure when context is cancelled, got %v", got)
	}
}

// TestRunProbe_ShortBudgetFailsFast confirms the point raised in review: the short
// retryTimeout used on steady-state ticks expires before endpoints publish, so a
// startup-length outage under that budget is counted — which is why the first probe
// must use the longer startupTimeout instead.
func TestRunProbe_ShortBudgetFailsFast(t *testing.T) {
	base := selectorAwareWebhookClient(t, enrolledNamespaceLabels)

	var attempts int
	// Unavailable for ~1.5s (3 attempts at 500ms); longer than the 300ms budget below.
	c := &countingUnavailableClient{Client: base, failUntil: 3, attempts: &attempts}

	p := newTestProbe(c)
	p.retryTimeout = 300 * time.Millisecond

	beforeFailures := gaugeValue(t, webhookProbeFailuresTotal)
	p.runProbe(context.Background(), p.retryTimeout)

	if got := gaugeValue(t, webhookProbeFailuresTotal) - beforeFailures; got != 1 {
		t.Errorf("expected one failure when the short budget expires before endpoints publish, got %v", got)
	}
}

// TestRunProbe_PersistentUnavailable_CountsFailure confirms that if the webhook
// stays unreachable past the budget (the path really is down), one failure is
// recorded — the real signal the alert depends on.
func TestRunProbe_PersistentUnavailable_CountsFailure(t *testing.T) {
	base := selectorAwareWebhookClient(t, enrolledNamespaceLabels)

	var attempts int
	c := &countingUnavailableClient{Client: base, failUntil: 1 << 30, attempts: &attempts}

	p := newTestProbe(c)

	beforeFailures := gaugeValue(t, webhookProbeFailuresTotal)
	p.runProbe(context.Background(), 300*time.Millisecond)

	if got := gaugeValue(t, webhookProbeFailuresTotal) - beforeFailures; got != 1 {
		t.Errorf("expected exactly one failure when webhook stays down, got %v", got)
	}
}

// TestRunProbe_InFlightWriteStopsAtBudget is a write that does not return until its
// context is cancelled. The attempt's context must expire with the budget, so the
// probe records one failure and does not wait on the parent context.
func TestRunProbe_InFlightWriteStopsAtBudget(t *testing.T) {
	base := selectorAwareWebhookClient(t, enrolledNamespaceLabels)
	c := &blockUntilCancelClient{Client: base}
	p := newTestProbe(c)

	const budget = 200 * time.Millisecond
	start := time.Now()
	beforeFailures := gaugeValue(t, webhookProbeFailuresTotal)
	p.runProbe(context.Background(), budget)
	elapsed := time.Since(start)

	if got := gaugeValue(t, webhookProbeFailuresTotal) - beforeFailures; got != 1 {
		t.Errorf("expected one failure when the in-flight write hits the budget, got %v", got)
	}
	if elapsed > budget+300*time.Millisecond {
		t.Errorf("in-flight write ran past the budget: elapsed %s, budget %s", elapsed, budget)
	}
	if !c.sawDeadline {
		t.Error("expected the write context to carry the budget deadline")
	}
}

// TestRunProbe_NoRetryAfterBudget is a webhook rejection that returns immediately.
// The retry wait must not start another attempt once the budget has expired, and
// that expiry still counts as one failure.
func TestRunProbe_NoRetryAfterBudget(t *testing.T) {
	base := selectorAwareWebhookClient(t, enrolledNamespaceLabels)

	var attempts int
	c := &countingUnavailableClient{Client: base, failUntil: 1 << 30, attempts: &attempts}
	p := newTestProbe(c)

	const budget = 100 * time.Millisecond
	start := time.Now()
	beforeFailures := gaugeValue(t, webhookProbeFailuresTotal)
	p.runProbe(context.Background(), budget)
	elapsed := time.Since(start)

	if attempts != 1 {
		t.Errorf("expected no write after the budget expired, attempts=%d", attempts)
	}
	if got := gaugeValue(t, webhookProbeFailuresTotal) - beforeFailures; got != 1 {
		t.Errorf("expected one failure when the budget expires before a retry, got %v", got)
	}
	if elapsed >= 500*time.Millisecond {
		t.Errorf("retry wait ran past the budget: elapsed %s", elapsed)
	}
}

// countingUnavailableClient fails Create/Update with a webhook-unavailable error
// for the first failUntil attempts, then delegates to the wrapped client.
type countingUnavailableClient struct {
	client.Client
	failUntil int
	attempts  *int
}

var errNoEndpoints = errors.New(`Internal error occurred: failed calling webhook "podmonitor-injector.opendatahub.io": no endpoints available for service "odh-observability-webhook"`)

func (c *countingUnavailableClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	*c.attempts++
	if *c.attempts <= c.failUntil {
		return errNoEndpoints
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c *countingUnavailableClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	*c.attempts++
	if *c.attempts <= c.failUntil {
		return errNoEndpoints
	}
	return c.Client.Update(ctx, obj, opts...)
}

// blockUntilCancelClient blocks Create until the write context is done, then
// returns that context's error.
type blockUntilCancelClient struct {
	client.Client
	sawDeadline bool
}

func (c *blockUntilCancelClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := ctx.Deadline(); ok {
		c.sawDeadline = true
	}
	<-ctx.Done()
	return ctx.Err()
}
