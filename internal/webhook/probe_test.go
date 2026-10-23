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
	p.runProbe(context.Background())

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
	p.runProbe(context.Background())
	beforeFailures := gaugeValue(t, webhookProbeFailuresTotal)
	p.runProbe(context.Background())

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
			p.runProbe(context.Background())

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
	p.runProbe(context.Background())

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
	p.runProbe(context.Background())

	if got := gaugeValue(t, webhookProbeFailuresTotal) - beforeFailures; got != 0 {
		t.Errorf("expected no new failures, got %v", got)
	}
	if got := storedProbeLabels(t, c)[labelMonitoring]; got != "true" {
		t.Errorf("expected stale canary to be re-labeled %s=true, got %q", labelMonitoring, got)
	}
}

// --- Start: context cancellation --------------------------------------------

func TestStart_SeedsSuccessTimestamp(t *testing.T) {
	// An unenrolled namespace makes the first probe fail, so only the seed in
	// Start can move the gauge off 0.
	webhookProbeLastSuccessTimestamp.Set(0)
	c := selectorAwareWebhookClient(t, nil)
	p := NewAdmissionProbe(c, probeTestNamespace, time.Hour)
	p.retryTimeout = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	floor := float64(time.Now().Add(-time.Second).Unix())
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}
	if got := gaugeValue(t, webhookProbeLastSuccessTimestamp); got < floor {
		t.Errorf("expected success timestamp seeded at start, got %v", got)
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
