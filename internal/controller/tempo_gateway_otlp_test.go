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

package controller

import (
	"errors"
	"strings"
	"testing"
	"time"

	platformcommon "github.com/opendatahub-io/odh-platform-utilities/api/common"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
)

func tempoStackMonitoring(ns string, backend string) *v1alpha1.Monitoring {
	return &v1alpha1.Monitoring{
		ObjectMeta: metav1.ObjectMeta{Name: "default-monitoring"},
		Spec: v1alpha1.MonitoringSpec{
			Namespace: ns,
			Traces: &v1alpha1.Traces{
				Storage: &v1alpha1.TracesStorage{Backend: backend, Secret: "secret"},
			},
		},
	}
}

func gatewayService(ns string, ports []corev1.ServicePort) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tempoStackGatewayServiceName,
			Namespace: ns,
			Labels: map[string]string{
				"app.kubernetes.io/component":  tempoGatewayComponentLabelValue,
				"app.kubernetes.io/managed-by": tempoOperatorManagedByLabelValue,
				"app.kubernetes.io/instance":   tempoStackInstanceLabelValue,
			},
		},
		Spec: corev1.ServiceSpec{Ports: ports},
	}
}

func TestSyncTempoStackGatewayOTLPPort_AddsMissingPort(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	ns := "redhat-ods-monitoring"
	svc := gatewayService(ns, []corev1.ServicePort{
		{Name: "grpc-public", Port: 8090, TargetPort: intstr.FromString("grpc-public")},
		{Name: "public", Port: 8080, TargetPort: intstr.FromString("public")},
	})
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(svc).Build()

	if err := syncTempoStackGatewayOTLPPort(t.Context(), c, tempoStackMonitoring(ns, v1alpha1.StorageBackendS3)); err != nil {
		t.Fatalf("sync: %v", err)
	}

	got := &corev1.Service{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(svc), got); err != nil {
		t.Fatalf("get service: %v", err)
	}
	if !tempoStackGatewayHasOTLPPort(got) {
		t.Fatalf("expected otlp-grpc :4317 → grpc-public, got %#v", got.Spec.Ports)
	}
}

func TestSyncTempoStackGatewayOTLPPort_FixesWrongTargetPort(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	ns := "ns"
	svc := gatewayService(ns, []corev1.ServicePort{
		{Name: "otlp-grpc", Port: 4317, TargetPort: intstr.FromInt32(4317)},
		{Name: "grpc-public", Port: 8090, TargetPort: intstr.FromString("grpc-public")},
	})
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(svc).Build()

	if err := syncTempoStackGatewayOTLPPort(t.Context(), c, tempoStackMonitoring(ns, v1alpha1.StorageBackendGCS)); err != nil {
		t.Fatalf("sync: %v", err)
	}

	got := &corev1.Service{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(svc), got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if !tempoStackGatewayHasOTLPPort(got) {
		t.Fatalf("expected corrected targetPort grpc-public, got %#v", got.Spec.Ports)
	}
	if len(got.Spec.Ports) != 2 {
		t.Fatalf("should replace wrong 4317 mapping in place, got %#v", got.Spec.Ports)
	}
}

func TestSyncTempoStackGatewayOTLPPort_Idempotent(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	ns := "ns"
	svc := gatewayService(ns, []corev1.ServicePort{
		{Name: "otlp-grpc", Port: 4317, TargetPort: intstr.FromString("grpc-public")},
		{Name: "grpc-public", Port: 8090, TargetPort: intstr.FromString("grpc-public")},
	})
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(svc).Build()

	if err := syncTempoStackGatewayOTLPPort(t.Context(), c, tempoStackMonitoring(ns, v1alpha1.StorageBackendGCS)); err != nil {
		t.Fatalf("sync: %v", err)
	}
	got := &corev1.Service{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(svc), got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(got.Spec.Ports) != 2 {
		t.Fatalf("should not duplicate ports, got %#v", got.Spec.Ports)
	}
}

func TestSyncTempoStackGatewayOTLPPort_SkipsPVBackend(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	// No Service in the fake client: PV must not attempt Get.
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	monitoring := tempoStackMonitoring("ns", v1alpha1.StorageBackendPV)
	if err := syncTempoStackGatewayOTLPPort(t.Context(), c, monitoring); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

func TestSyncTempoStackGatewayOTLPPort_RequeuesWhenServiceMissing(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	err := syncTempoStackGatewayOTLPPort(t.Context(), c, tempoStackMonitoring("ns", v1alpha1.StorageBackendS3))
	if !errors.Is(err, errTempoStackGatewayServiceNotFound) {
		t.Fatalf("want errTempoStackGatewayServiceNotFound, got %v", err)
	}
}

func TestIsTempoStackGatewayService(t *testing.T) {
	// No t.Parallel: predicate reads MONITORING_NAMESPACE like sibling watch tests.
	t.Setenv("MONITORING_NAMESPACE", "monitoring-test")

	good := gatewayService("monitoring-test", nil)
	if !isTempoStackGatewayService(good) {
		t.Fatal("expected TempoStack gateway Service to match")
	}

	wrongName := good.DeepCopy()
	wrongName.Name = "other"
	if isTempoStackGatewayService(wrongName) {
		t.Fatal("name-only match must not pass")
	}

	wrongNS := good.DeepCopy()
	wrongNS.Namespace = "other-ns"
	if isTempoStackGatewayService(wrongNS) {
		t.Fatal("wrong namespace must not match")
	}

	missingLabels := good.DeepCopy()
	missingLabels.Labels = map[string]string{"app.kubernetes.io/component": tempoGatewayComponentLabelValue}
	if isTempoStackGatewayService(missingLabels) {
		t.Fatal("incomplete Tempo labels must not match")
	}
}

func TestTracesUseTempoStack_MatchesDeployTracingStack(t *testing.T) {
	t.Parallel()

	if tracesUseTempoStack(tempoStackMonitoring("ns", v1alpha1.StorageBackendPV)) {
		t.Fatal("PV must use TempoMonolithic, not TempoStack sync")
	}
	if !tracesUseTempoStack(tempoStackMonitoring("ns", v1alpha1.StorageBackendS3)) {
		t.Fatal("S3 must use TempoStack sync")
	}
	if !tracesUseTempoStack(tempoStackMonitoring("ns", v1alpha1.StorageBackendGCS)) {
		t.Fatal("GCS must use TempoStack sync")
	}
	if tracesUseTempoStack(&v1alpha1.Monitoring{}) {
		t.Fatal("nil traces must skip TempoStack sync")
	}
}

func TestSyncPostDeployResources_OTLPPendingIsNotHardError(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	monitoring := tempoStackMonitoring("ns", v1alpha1.StorageBackendS3)
	cm := conditions.NewConditionsManager(monitoring, monitoring.Generation)
	r := &MonitoringReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}

	pending, err := r.syncPostDeployResources(t.Context(), monitoring, cm)
	if err != nil {
		t.Fatalf("missing gateway must be pending, not hard error: %v", err)
	}
	if !pending {
		t.Fatal("expected otlpGatewayPending when TempoStack gateway Service is missing")
	}
}

func TestRequeueForTempoStackGatewayOTLP(t *testing.T) {
	t.Parallel()

	monitoring := tempoStackMonitoring("ns", v1alpha1.StorageBackendS3)
	cm := conditions.NewConditionsManager(monitoring, monitoring.Generation)
	cm.MarkTrue(string(platformcommon.ConditionTypeReady))
	cm.MarkTrue(string(platformcommon.ConditionTypeProvisioningSucceeded))
	cm.MarkTrue(string(platformcommon.ConditionTypeDegraded))

	if _, wait := requeueForTempoStackGatewayOTLP(cm, false); wait {
		t.Fatal("ready gateway must not request requeue")
	}

	result, wait := requeueForTempoStackGatewayOTLP(cm, true)
	if !wait {
		t.Fatal("pending gateway must request requeue")
	}
	if result.RequeueAfter != 15*time.Second {
		t.Fatalf("RequeueAfter: want 15s, got %v", result.RequeueAfter)
	}
	if cond := findCondition(monitoring, string(platformcommon.ConditionTypeReady)); cond == nil ||
		cond.Status != metav1.ConditionUnknown || cond.Reason != tempoStackGatewayServiceNotFoundReason {
		t.Fatalf("Ready: want Unknown/%s, got %#v", tempoStackGatewayServiceNotFoundReason, cond)
	}
	if cond := findCondition(monitoring, string(platformcommon.ConditionTypeProvisioningSucceeded)); cond == nil ||
		cond.Reason != tempoStackGatewayServiceNotFoundReason {
		t.Fatalf("ProvisioningSucceeded reason: want %s, got %#v", tempoStackGatewayServiceNotFoundReason, cond)
	}
	if cond := findCondition(monitoring, string(platformcommon.ConditionTypeDegraded)); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("must preserve existing Degraded=True, got %#v", cond)
	}
	if cond := findCondition(monitoring, conditions.ConditionTempoAvailable); cond != nil {
		t.Fatalf("must not touch TempoAvailable, got %#v", cond)
	}
}

func TestMarkTempoStackGatewayServiceNotFound_SurfacesBesideCA(t *testing.T) {
	t.Parallel()

	monitoring := tempoStackMonitoring("ns", v1alpha1.StorageBackendS3)
	cm := conditions.NewConditionsManager(monitoring, monitoring.Generation)
	markTempoStackGatewayServiceNotFound(cm)
	// CA wait overwrites ProvisioningSucceeded but must leave Ready progressing
	// so dual-wait still surfaces the missing gateway Service.
	cm.MarkFalse(string(platformcommon.ConditionTypeProvisioningSucceeded),
		"PrometheusCAUnavailable",
		"Waiting for Prometheus CA Secret before applying MonitoringStack; "+errTempoStackGatewayServiceNotFound.Error())

	if cond := findCondition(monitoring, string(platformcommon.ConditionTypeReady)); cond == nil ||
		cond.Status != metav1.ConditionUnknown || cond.Reason != tempoStackGatewayServiceNotFoundReason {
		t.Fatalf("Ready must stay Unknown/%s under dual wait, got %#v", tempoStackGatewayServiceNotFoundReason, cond)
	}
	cond := findCondition(monitoring, string(platformcommon.ConditionTypeProvisioningSucceeded))
	if cond == nil || cond.Reason != "PrometheusCAUnavailable" ||
		!strings.Contains(cond.Message, "Prometheus CA") ||
		!strings.Contains(cond.Message, tempoStackGatewayServiceName) {
		t.Fatalf("ProvisioningSucceeded must keep CA reason and mention gateway, got %#v", cond)
	}
}

func TestSyncPostDeployResources_OTLPReadyNotPending(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	ns := "ns"
	svc := gatewayService(ns, []corev1.ServicePort{
		{Name: "otlp-grpc", Port: 4317, TargetPort: intstr.FromString("grpc-public")},
	})
	monitoring := tempoStackMonitoring(ns, v1alpha1.StorageBackendGCS)
	cm := conditions.NewConditionsManager(monitoring, monitoring.Generation)
	r := &MonitoringReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(svc).Build(),
	}

	pending, err := r.syncPostDeployResources(t.Context(), monitoring, cm)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if pending {
		t.Fatal("gateway with :4317 → grpc-public must not be pending")
	}
}
