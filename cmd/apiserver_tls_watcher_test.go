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
	"testing"

	configv1 "github.com/openshift/api/config/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestAPIServerTLSProfileChanged(t *testing.T) {
	oldAPIServer := &configv1.APIServer{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
		Spec: configv1.APIServerSpec{
			TLSSecurityProfile: &configv1.TLSSecurityProfile{Type: configv1.TLSProfileIntermediateType},
			TLSAdherence:       configv1.TLSAdherencePolicyStrictAllComponents,
		},
	}

	tests := []struct {
		name string
		new  *configv1.APIServer
		want bool
	}{
		{name: "profile change", new: oldAPIServer.DeepCopy(), want: true},
		{name: "adherence change", new: oldAPIServer.DeepCopy(), want: true},
		{name: "other object change", new: oldAPIServer.DeepCopy(), want: false},
	}
	tests[0].new.Spec.TLSSecurityProfile.Type = configv1.TLSProfileModernType
	tests[1].new.Spec.TLSAdherence = configv1.TLSAdherencePolicyLegacyAdheringComponentsOnly
	tests[2].new.Name = "other"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := apiServerTLSProfileChanged(oldAPIServer, tt.new); got != tt.want {
				t.Fatalf("apiServerTLSProfileChanged() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestAPIServerTLSChangeWatcherRequestsReload(t *testing.T) {
	requested := false
	watcher := &apiServerTLSChangeWatcher{onChange: func() { requested = true }}
	if _, err := watcher.Reconcile(context.Background(), reconcile.Request{}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !requested {
		t.Fatal("Reconcile() did not request a TLS reload")
	}
}
