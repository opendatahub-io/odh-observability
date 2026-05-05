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

// Package webhook implements the mutating admission webhook that injects
// the opendatahub.io/monitoring=true label on ServiceMonitor and PodMonitor
// resources created in namespaces that opt in to monitoring.
package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
)

const (
	// labelMonitoring is the opt-in label on namespaces and the injected label on monitors.
	labelMonitoring = "opendatahub.io/monitoring"
	// webhookPath is the path registered with the webhook server.
	webhookPath = "/mutate-prometheus-monitors"
)

//+kubebuilder:webhook:path=/mutate-prometheus-monitors,mutating=true,failurePolicy=fail,groups=monitoring.coreos.com,resources=podmonitors,verbs=create;update,versions=v1,name=podmonitor-injector.opendatahub.io,sideEffects=None,admissionReviewVersions=v1
//+kubebuilder:webhook:path=/mutate-prometheus-monitors,mutating=true,failurePolicy=fail,groups=monitoring.coreos.com,resources=servicemonitors,verbs=create;update,versions=v1,name=servicemonitor-injector.opendatahub.io,sideEffects=None,admissionReviewVersions=v1

// Injector is a mutating admission webhook that injects the monitoring label
// into ServiceMonitor and PodMonitor resources in opted-in namespaces.
type Injector struct {
	Client  client.Reader
	Decoder admission.Decoder
}

var _ admission.Handler = &Injector{}

// SetupWithManager registers the webhook with the manager's webhook server.
func (i *Injector) SetupWithManager(mgr ctrl.Manager) error {
	mgr.GetWebhookServer().Register(webhookPath, &webhook.Admission{Handler: i})
	return nil
}

// Handle processes admission requests for ServiceMonitor and PodMonitor resources.
func (i *Injector) Handle(ctx context.Context, req admission.Request) admission.Response {
	log := logf.FromContext(ctx)

	if i.Decoder == nil {
		log.Error(nil, "Decoder is nil — webhook not properly initialized")
		return admission.Errored(http.StatusInternalServerError, errors.New("webhook decoder not initialized"))
	}
	if i.Client == nil {
		log.Error(nil, "Client is nil — webhook not properly initialized")
		return admission.Errored(http.StatusInternalServerError, errors.New("webhook client not initialized"))
	}

	if !isExpectedKind(req.Kind) {
		err := fmt.Errorf("unexpected kind: %s", req.Kind.Kind)
		log.Error(err, "Received unexpected resource kind")
		return admission.Errored(http.StatusBadRequest, err)
	}

	obj := &unstructured.Unstructured{}
	if err := i.Decoder.DecodeRaw(req.Object, obj); err != nil {
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("failed to decode object: %w", err))
	}

	if !obj.GetDeletionTimestamp().IsZero() {
		return admission.Allowed("object marked for deletion, skipping monitoring injection")
	}

	switch req.Operation {
	case admissionv1.Create, admissionv1.Update:
		return i.inject(ctx, &req, obj)
	default:
		return admission.Allowed(fmt.Sprintf("operation %s on %s allowed", req.Operation, req.Kind.Kind))
	}
}

// inject checks the namespace label and injects the monitoring label if appropriate.
func (i *Injector) inject(ctx context.Context, req *admission.Request, obj *unstructured.Unstructured) admission.Response {
	log := logf.FromContext(ctx)

	ns := &corev1.Namespace{}
	if err := i.Client.Get(ctx, types.NamespacedName{Name: obj.GetNamespace()}, ns); err != nil {
		if k8serr.IsNotFound(err) {
			return admission.Errored(http.StatusBadRequest, fmt.Errorf("namespace '%s' not found", obj.GetNamespace()))
		}
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("failed to get namespace '%s': %w", obj.GetNamespace(), err))
	}

	if ns.GetLabels()[labelMonitoring] != "true" {
		log.V(1).Info("Namespace not labeled for monitoring", "namespace", obj.GetNamespace())
		return admission.Allowed("namespace not configured for monitoring")
	}

	// Verify Monitoring CR exists (confirms monitoring is actually enabled).
	monitoringCR := &unstructured.Unstructured{}
	monitoringCR.SetGroupVersionKind(gvk.Monitoring)
	if err := i.Client.Get(ctx, types.NamespacedName{Name: v1alpha1.MonitoringInstanceName}, monitoringCR); err != nil {
		if k8serr.IsNotFound(err) {
			log.V(1).Info("Monitoring CR not found — monitoring disabled, skipping injection")
			return admission.Allowed("monitoring is disabled — CR does not exist")
		}
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("failed to verify monitoring state: %w", err))
	}

	lbls := obj.GetLabels()
	if lbls == nil {
		lbls = make(map[string]string)
	}
	if _, exists := lbls[labelMonitoring]; !exists {
		lbls[labelMonitoring] = "true"
		obj.SetLabels(lbls)
	}

	marshaled, err := json.Marshal(obj)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("failed to marshal mutated object: %w", err))
	}

	return admission.PatchResponseFromRaw(req.Object.Raw, marshaled)
}

func isExpectedKind(kind metav1.GroupVersionKind) bool {
	supported := []schema.GroupVersionKind{
		gvk.CoreosServiceMonitor,
		gvk.CoreosPodMonitor,
	}
	return slices.Contains(supported, schema.GroupVersionKind{
		Group:   kind.Group,
		Version: kind.Version,
		Kind:    kind.Kind,
	})
}
