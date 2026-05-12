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

package v1alpha1

import (
	"testing"

	platformcommon "github.com/opendatahub-io/odh-platform-utilities/api/common"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMonitoringConstants(t *testing.T) {
	if MonitoringServiceName != "monitoring" {
		t.Errorf("MonitoringServiceName: want %q, got %q", "monitoring", MonitoringServiceName)
	}
	if MonitoringInstanceName != "default-monitoring" {
		t.Errorf("MonitoringInstanceName: want %q, got %q", "default-monitoring", MonitoringInstanceName)
	}
	if MonitoringKind != "Monitoring" {
		t.Errorf("MonitoringKind: want %q, got %q", "Monitoring", MonitoringKind)
	}
}

func TestMonitoring_GetStatus(t *testing.T) {
	m := &Monitoring{
		Status: MonitoringStatus{
			Status: platformcommon.Status{
				Phase:              platformcommon.PhaseReady,
				ObservedGeneration: 5,
			},
		},
	}

	status := m.GetStatus()
	if status == nil {
		t.Fatal("GetStatus returned nil")
	}
	if status.Phase != platformcommon.PhaseReady {
		t.Errorf("Phase: want %q, got %q", platformcommon.PhaseReady, status.Phase)
	}
	if status.ObservedGeneration != 5 {
		t.Errorf("ObservedGeneration: want 5, got %d", status.ObservedGeneration)
	}
}

func TestMonitoring_GetSetConditions(t *testing.T) {
	m := &Monitoring{}

	if got := m.GetConditions(); got != nil {
		t.Errorf("initial GetConditions: want nil, got %v", got)
	}

	conds := []platformcommon.Condition{
		{
			Type:   string(platformcommon.ConditionTypeReady),
			Status: metav1.ConditionTrue,
			Reason: "Available",
		},
		{
			Type:   string(platformcommon.ConditionTypeDegraded),
			Status: metav1.ConditionFalse,
			Reason: "NotDegraded",
		},
	}
	m.SetConditions(conds)

	got := m.GetConditions()
	if len(got) != 2 {
		t.Fatalf("GetConditions length: want 2, got %d", len(got))
	}
	if got[0].Type != string(platformcommon.ConditionTypeReady) {
		t.Errorf("condition[0].Type: want %q, got %q", platformcommon.ConditionTypeReady, got[0].Type)
	}
	if got[1].Status != metav1.ConditionFalse {
		t.Errorf("condition[1].Status: want False, got %s", got[1].Status)
	}
}

func TestMonitoring_GetSetReleaseStatus(t *testing.T) {
	m := &Monitoring{}

	rs := m.GetReleaseStatus()
	if rs == nil {
		t.Fatal("GetReleaseStatus returned nil")
	}
	if len(rs.Releases) != 0 {
		t.Errorf("initial releases: want empty, got %v", rs.Releases)
	}

	m.SetReleaseStatus(platformcommon.ComponentReleaseStatus{
		Releases: []platformcommon.ComponentRelease{
			{Name: "monitoring", RepoURL: "https://github.com/opendatahub-io/odh-observability", Version: "1.0.0"},
		},
	})

	rs = m.GetReleaseStatus()
	if len(rs.Releases) != 1 {
		t.Fatalf("releases length: want 1, got %d", len(rs.Releases))
	}
	if rs.Releases[0].Name != "monitoring" {
		t.Errorf("releases[0].Name: want %q, got %q", "monitoring", rs.Releases[0].Name)
	}
	if rs.Releases[0].Version != "1.0.0" {
		t.Errorf("releases[0].Version: want %q, got %q", "1.0.0", rs.Releases[0].Version)
	}
}

func TestMonitoring_StatusMutationThroughPointer(t *testing.T) {
	m := &Monitoring{}

	status := m.GetStatus()
	status.ObservedGeneration = 42
	status.Phase = platformcommon.PhaseNotReady

	if m.Status.Status.ObservedGeneration != 42 {
		t.Error("mutating GetStatus() pointer should affect the original object")
	}
	if m.Status.Status.Phase != platformcommon.PhaseNotReady {
		t.Error("mutating GetStatus() pointer should affect the original Phase")
	}
}

func TestMonitoring_DeepCopy(t *testing.T) {
	m := &Monitoring{
		ObjectMeta: metav1.ObjectMeta{
			Name:       MonitoringInstanceName,
			Generation: 3,
		},
		Spec: MonitoringSpec{
			ManagementSpec: platformcommon.ManagementSpec{
				ManagementState: platformcommon.Managed,
			},
			Namespace: "opendatahub",
			Metrics:   &Metrics{Replicas: 2},
		},
		Status: MonitoringStatus{
			Status: platformcommon.Status{
				ObservedGeneration: 3,
				Phase:              platformcommon.PhaseReady,
				Conditions: []platformcommon.Condition{
					{Type: string(platformcommon.ConditionTypeReady), Status: metav1.ConditionTrue},
				},
			},
			URL: "https://thanos.example.com",
		},
	}

	copy := m.DeepCopy()

	if copy.Name != m.Name {
		t.Errorf("DeepCopy Name mismatch: want %q, got %q", m.Name, copy.Name)
	}
	if copy.Spec.Namespace != m.Spec.Namespace {
		t.Errorf("DeepCopy Namespace mismatch")
	}
	if copy.Spec.Metrics == nil {
		t.Fatal("DeepCopy Metrics should not be nil")
	}
	if copy.Status.URL != m.Status.URL {
		t.Errorf("DeepCopy URL mismatch")
	}

	copy.Spec.Metrics.Replicas = 99
	if m.Spec.Metrics.Replicas == 99 {
		t.Error("DeepCopy should produce independent Metrics pointer")
	}

	copy.Status.Status.Conditions[0].Status = metav1.ConditionFalse
	if m.Status.Status.Conditions[0].Status == metav1.ConditionFalse {
		t.Error("DeepCopy should produce independent conditions slice")
	}
}

func TestMonitoring_DeepCopyObject(t *testing.T) {
	m := &Monitoring{
		ObjectMeta: metav1.ObjectMeta{Name: MonitoringInstanceName},
	}

	obj := m.DeepCopyObject()
	if obj == nil {
		t.Fatal("DeepCopyObject returned nil")
	}

	copied, ok := obj.(*Monitoring)
	if !ok {
		t.Fatalf("DeepCopyObject returned %T, want *Monitoring", obj)
	}
	if copied.Name != MonitoringInstanceName {
		t.Errorf("DeepCopyObject Name: want %q, got %q", MonitoringInstanceName, copied.Name)
	}
}

func TestMonitoringList_DeepCopy(t *testing.T) {
	list := &MonitoringList{
		Items: []Monitoring{
			{ObjectMeta: metav1.ObjectMeta{Name: "a"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "b"}},
		},
	}

	copy := list.DeepCopy()
	if len(copy.Items) != 2 {
		t.Fatalf("DeepCopy Items length: want 2, got %d", len(copy.Items))
	}

	copy.Items[0].Name = "modified"
	if list.Items[0].Name == "modified" {
		t.Error("DeepCopy should produce independent Items slice")
	}
}

func TestStorageBackendConstants(t *testing.T) {
	if StorageBackendPV != "pv" {
		t.Errorf("StorageBackendPV: want %q, got %q", "pv", StorageBackendPV)
	}
	if StorageBackendS3 != "s3" {
		t.Errorf("StorageBackendS3: want %q, got %q", "s3", StorageBackendS3)
	}
	if StorageBackendGCS != "gcs" {
		t.Errorf("StorageBackendGCS: want %q, got %q", "gcs", StorageBackendGCS)
	}
}
