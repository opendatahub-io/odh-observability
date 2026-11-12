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
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
)

const (
	// tempoStackGatewayServiceName is the Tempo Operator-managed gateway Service
	// for Monitoring's data-science-tempostack instance.
	tempoStackGatewayServiceName     = "tempo-data-science-tempostack-gateway"
	tempoStackInstanceLabelValue     = "data-science-tempostack"
	tempoGatewayComponentLabelValue  = "gateway"
	tempoOperatorManagedByLabelValue = "tempo-operator"
	tempoGatewayOTLPGRPCPortName     = "otlp-grpc"
	tempoGatewayOTLPGRPCPort         = int32(4317)
	tempoGatewayGRPCPublicTargetPort = "grpc-public"
)

// errTempoStackGatewayServiceNotFound is returned only when TempoStack traces
// are configured and the gateway Service is missing (NotFound). A Service that
// exists without :4317 is patched in-band; patch failures are hard errors.
var errTempoStackGatewayServiceNotFound = fmt.Errorf(
	"TempoStack gateway Service %q not found yet", tempoStackGatewayServiceName)

// syncTempoStackGatewayOTLPPort ensures the Tempo Operator-managed TempoStack
// gateway Service exposes standard OTLP gRPC :4317 mapped to grpc-public.
// TempoMonolithic already publishes this alias; TempoStack does not.
//
// Deliberate ownership fight: Tempo Operator owns the Service and may strip the
// alias on its reconcile. We re-patch on Monitoring reconcile and on Service
// watch events. Tempo Operator's gateway NetworkPolicy already admits pod
// :8090 (grpc-public), so the Service alias alone is sufficient for collector
// traffic — no ODH NetworkPolicy is required.
func syncTempoStackGatewayOTLPPort(ctx context.Context, c client.Client, monitoring *v1alpha1.Monitoring) error {
	if !tracesUseTempoStack(monitoring) {
		return nil
	}

	svc := &corev1.Service{}
	key := client.ObjectKey{
		Namespace: monitoringNamespace(monitoring),
		Name:      tempoStackGatewayServiceName,
	}
	if err := c.Get(ctx, key, svc); err != nil {
		if errors.IsNotFound(err) {
			return errTempoStackGatewayServiceNotFound
		}
		return fmt.Errorf("get TempoStack gateway Service: %w", err)
	}

	if tempoStackGatewayHasOTLPPort(svc) {
		return nil
	}

	patched := svc.DeepCopy()
	ensureTempoStackGatewayOTLPPort(patched)
	if err := c.Patch(ctx, patched, client.MergeFrom(svc)); err != nil {
		return fmt.Errorf("ensure OTLP gRPC port %d→%s on TempoStack gateway Service: %w",
			tempoGatewayOTLPGRPCPort, tempoGatewayGRPCPublicTargetPort, err)
	}
	return nil
}

// tracesUseTempoStack mirrors deployTracingStack: any non-PV traces backend
// deploys TempoStack (today S3/GCS); PV deploys TempoMonolithic.
func tracesUseTempoStack(monitoring *v1alpha1.Monitoring) bool {
	if monitoring.Spec.Traces == nil || monitoring.Spec.Traces.Storage == nil {
		return false
	}
	return monitoring.Spec.Traces.Storage.Backend != v1alpha1.StorageBackendPV
}

func tempoStackGatewayHasOTLPPort(svc *corev1.Service) bool {
	for _, port := range svc.Spec.Ports {
		if port.Port == tempoGatewayOTLPGRPCPort &&
			port.TargetPort == intstr.FromString(tempoGatewayGRPCPublicTargetPort) {
			return true
		}
	}
	return false
}

// ensureTempoStackGatewayOTLPPort mutates svc so :4317 targets grpc-public.
// Replaces a wrong 4317 mapping if present; otherwise appends the alias.
func ensureTempoStackGatewayOTLPPort(svc *corev1.Service) {
	desired := corev1.ServicePort{
		Name:       tempoGatewayOTLPGRPCPortName,
		Protocol:   corev1.ProtocolTCP,
		Port:       tempoGatewayOTLPGRPCPort,
		TargetPort: intstr.FromString(tempoGatewayGRPCPublicTargetPort),
	}
	for i := range svc.Spec.Ports {
		if svc.Spec.Ports[i].Port == tempoGatewayOTLPGRPCPort {
			svc.Spec.Ports[i] = desired
			return
		}
	}
	svc.Spec.Ports = append(svc.Spec.Ports, desired)
}

func isTempoStackGatewayService(obj client.Object) bool {
	if obj.GetName() != tempoStackGatewayServiceName {
		return false
	}
	if obj.GetNamespace() != getEnvOrDefault("MONITORING_NAMESPACE", defaultMonitoringNamespace) {
		return false
	}
	labels := obj.GetLabels()
	return labels["app.kubernetes.io/component"] == tempoGatewayComponentLabelValue &&
		labels["app.kubernetes.io/managed-by"] == tempoOperatorManagedByLabelValue &&
		labels["app.kubernetes.io/instance"] == tempoStackInstanceLabelValue
}
