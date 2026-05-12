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
	"encoding/json"
	"net/http"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
)

func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(v1alpha1.AddToScheme(s))

	// CoreOS monitor GVKs are external types the webhook intercepts; register as
	// unstructured so the fake client can handle them.
	s.AddKnownTypeWithName(gvk.CoreosServiceMonitor, &unstructured.Unstructured{})
	listSM := schema.GroupVersionKind{Group: gvk.CoreosServiceMonitor.Group, Version: gvk.CoreosServiceMonitor.Version, Kind: "ServiceMonitorList"}
	s.AddKnownTypeWithName(listSM, &unstructured.UnstructuredList{})

	s.AddKnownTypeWithName(gvk.CoreosPodMonitor, &unstructured.Unstructured{})
	listPM := schema.GroupVersionKind{Group: gvk.CoreosPodMonitor.Group, Version: gvk.CoreosPodMonitor.Version, Kind: "PodMonitorList"}
	s.AddKnownTypeWithName(listPM, &unstructured.UnstructuredList{})

	return s
}

func newServiceMonitor(namespace, name string, labels map[string]string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk.CoreosServiceMonitor)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	if labels != nil {
		obj.SetLabels(labels)
	}
	return obj
}

func newPodMonitor(namespace, name string, labels map[string]string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk.CoreosPodMonitor)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	if labels != nil {
		obj.SetLabels(labels)
	}
	return obj
}

func monitoringCR() *v1alpha1.Monitoring {
	return &v1alpha1.Monitoring{
		ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.MonitoringInstanceName},
	}
}

func makeAdmissionRequest(op admissionv1.Operation, kind metav1.GroupVersionKind, obj *unstructured.Unstructured) admission.Request {
	raw, _ := json.Marshal(obj)
	return admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: op,
			Kind:      kind,
			Object:    runtime.RawExtension{Raw: raw},
		},
	}
}

func TestHandle_NilDecoder(t *testing.T) {
	injector := &Injector{
		Client:  fake.NewClientBuilder().WithScheme(newTestScheme()).Build(),
		Decoder: nil,
	}

	req := makeAdmissionRequest(admissionv1.Create, metav1.GroupVersionKind{
		Group: "monitoring.coreos.com", Version: "v1", Kind: "ServiceMonitor",
	}, newServiceMonitor("test-ns", "sm", nil))

	resp := injector.Handle(context.Background(), req)
	if resp.Result == nil || resp.Result.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for nil decoder, got %v", resp.Result)
	}
}

func TestHandle_NilClient(t *testing.T) {
	injector := &Injector{
		Client:  nil,
		Decoder: admission.NewDecoder(newTestScheme()),
	}

	req := makeAdmissionRequest(admissionv1.Create, metav1.GroupVersionKind{
		Group: "monitoring.coreos.com", Version: "v1", Kind: "ServiceMonitor",
	}, newServiceMonitor("test-ns", "sm", nil))

	resp := injector.Handle(context.Background(), req)
	if resp.Result == nil || resp.Result.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for nil client, got %v", resp.Result)
	}
}

func TestHandle_UnexpectedKind(t *testing.T) {
	scheme := newTestScheme()
	injector := &Injector{
		Client:  fake.NewClientBuilder().WithScheme(scheme).Build(),
		Decoder: admission.NewDecoder(scheme),
	}

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk.CoreosServiceMonitor)
	obj.SetNamespace("test-ns")
	obj.SetName("sm")
	raw, _ := json.Marshal(obj)

	req := admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Kind:      metav1.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
			Object:    runtime.RawExtension{Raw: raw},
		},
	}

	resp := injector.Handle(context.Background(), req)
	if resp.Result == nil || resp.Result.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for unexpected kind, got %v", resp.Result)
	}
}

func TestHandle_NamespaceNotLabeled(t *testing.T) {
	scheme := newTestScheme()
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "unlabeled-ns"},
	}
	monCR := monitoringCR()

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns).WithObjects(monCR).Build()
	injector := &Injector{
		Client:  cli,
		Decoder: admission.NewDecoder(scheme),
	}

	sm := newServiceMonitor("unlabeled-ns", "my-sm", nil)
	req := makeAdmissionRequest(admissionv1.Create, metav1.GroupVersionKind{
		Group: "monitoring.coreos.com", Version: "v1", Kind: "ServiceMonitor",
	}, sm)

	resp := injector.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Errorf("expected allowed for unlabeled namespace, got denied: %v", resp.Result)
	}
}

func TestHandle_InjectsLabelOnServiceMonitor(t *testing.T) {
	scheme := newTestScheme()
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "monitored-ns",
			Labels: map[string]string{labelMonitoring: "true"},
		},
	}
	monCR := monitoringCR()

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns).WithObjects(monCR).Build()
	injector := &Injector{
		Client:  cli,
		Decoder: admission.NewDecoder(scheme),
	}

	sm := newServiceMonitor("monitored-ns", "my-sm", nil)
	req := makeAdmissionRequest(admissionv1.Create, metav1.GroupVersionKind{
		Group: "monitoring.coreos.com", Version: "v1", Kind: "ServiceMonitor",
	}, sm)

	resp := injector.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected allowed, got denied: %v", resp.Result)
	}
	if len(resp.Patches) == 0 {
		t.Fatal("expected patches for label injection, got none")
	}

	hasLabelPatch := false
	for _, p := range resp.Patches {
		if p.Path == "/metadata/labels" || p.Path == "/metadata/labels/opendatahub.io~1monitoring" {
			hasLabelPatch = true
			break
		}
	}
	if !hasLabelPatch {
		t.Errorf("expected label patch, got patches: %v", resp.Patches)
	}
}

func TestHandle_InjectsLabelOnPodMonitor(t *testing.T) {
	scheme := newTestScheme()
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "monitored-ns",
			Labels: map[string]string{labelMonitoring: "true"},
		},
	}
	monCR := monitoringCR()

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns).WithObjects(monCR).Build()
	injector := &Injector{
		Client:  cli,
		Decoder: admission.NewDecoder(scheme),
	}

	pm := newPodMonitor("monitored-ns", "my-pm", nil)
	req := makeAdmissionRequest(admissionv1.Create, metav1.GroupVersionKind{
		Group: "monitoring.coreos.com", Version: "v1", Kind: "PodMonitor",
	}, pm)

	resp := injector.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected allowed, got denied: %v", resp.Result)
	}
	if len(resp.Patches) == 0 {
		t.Fatal("expected patches for label injection on PodMonitor")
	}
}

func TestHandle_PreservesExistingLabel(t *testing.T) {
	scheme := newTestScheme()
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "monitored-ns",
			Labels: map[string]string{labelMonitoring: "true"},
		},
	}
	monCR := monitoringCR()

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns).WithObjects(monCR).Build()
	injector := &Injector{
		Client:  cli,
		Decoder: admission.NewDecoder(scheme),
	}

	sm := newServiceMonitor("monitored-ns", "already-labeled", map[string]string{
		labelMonitoring: "true",
		"other":         "label",
	})
	req := makeAdmissionRequest(admissionv1.Create, metav1.GroupVersionKind{
		Group: "monitoring.coreos.com", Version: "v1", Kind: "ServiceMonitor",
	}, sm)

	resp := injector.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected allowed, got denied: %v", resp.Result)
	}
}

func TestHandle_MonitoringCRMissing(t *testing.T) {
	scheme := newTestScheme()
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "monitored-ns",
			Labels: map[string]string{labelMonitoring: "true"},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns).Build()
	injector := &Injector{
		Client:  cli,
		Decoder: admission.NewDecoder(scheme),
	}

	sm := newServiceMonitor("monitored-ns", "my-sm", nil)
	req := makeAdmissionRequest(admissionv1.Create, metav1.GroupVersionKind{
		Group: "monitoring.coreos.com", Version: "v1", Kind: "ServiceMonitor",
	}, sm)

	resp := injector.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Errorf("expected allowed (monitoring disabled), got denied: %v", resp.Result)
	}
}

func TestHandle_DeleteOperation(t *testing.T) {
	scheme := newTestScheme()
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	injector := &Injector{
		Client:  cli,
		Decoder: admission.NewDecoder(scheme),
	}

	sm := newServiceMonitor("test-ns", "sm", nil)
	req := makeAdmissionRequest(admissionv1.Delete, metav1.GroupVersionKind{
		Group: "monitoring.coreos.com", Version: "v1", Kind: "ServiceMonitor",
	}, sm)

	resp := injector.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Errorf("expected allowed for delete, got denied: %v", resp.Result)
	}
}

func TestIsExpectedKind(t *testing.T) {
	tests := []struct {
		name string
		kind metav1.GroupVersionKind
		want bool
	}{
		{
			name: "coreos ServiceMonitor",
			kind: metav1.GroupVersionKind{Group: "monitoring.coreos.com", Version: "v1", Kind: "ServiceMonitor"},
			want: true,
		},
		{
			name: "coreos PodMonitor",
			kind: metav1.GroupVersionKind{Group: "monitoring.coreos.com", Version: "v1", Kind: "PodMonitor"},
			want: true,
		},
		{
			name: "rhobs ServiceMonitor (not expected)",
			kind: metav1.GroupVersionKind{Group: "monitoring.rhobs", Version: "v1", Kind: "ServiceMonitor"},
			want: false,
		},
		{
			name: "random kind",
			kind: metav1.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isExpectedKind(tc.kind); got != tc.want {
				t.Errorf("isExpectedKind(%v): want %v, got %v", tc.kind, tc.want, got)
			}
		})
	}
}
