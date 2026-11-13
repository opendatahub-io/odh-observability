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
	"reflect"
	"sync/atomic"

	"github.com/go-logr/logr"
	configv1 "github.com/openshift/api/config/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type apiServerTLSChangeWatcher struct {
	onChange func()
}

func (w *apiServerTLSChangeWatcher) Reconcile(context.Context, reconcile.Request) (reconcile.Result, error) {
	if w.onChange != nil {
		w.onChange()
	}
	return reconcile.Result{}, nil
}

func (w *apiServerTLSChangeWatcher) SetupWithManager(mgr ctrl.Manager) error {
	needLeaderElection := false
	return ctrl.NewControllerManagedBy(mgr).
		Named("apiserver-tls-change-watcher").
		WithOptions(controller.Options{NeedLeaderElection: &needLeaderElection}).
		For(&configv1.APIServer{}, builder.WithPredicates(predicate.Funcs{
			CreateFunc:  func(event.CreateEvent) bool { return false },
			UpdateFunc:  func(e event.UpdateEvent) bool { return apiServerTLSProfileChanged(e.ObjectOld, e.ObjectNew) },
			DeleteFunc:  func(event.DeleteEvent) bool { return false },
			GenericFunc: func(event.GenericEvent) bool { return false },
		})).
		Complete(w)
}

func apiServerTLSProfileChanged(oldObject, newObject client.Object) bool {
	oldAPIServer, oldOK := oldObject.(*configv1.APIServer)
	newAPIServer, newOK := newObject.(*configv1.APIServer)
	return oldOK && newOK && oldAPIServer.Name == "cluster" && newAPIServer.Name == "cluster" &&
		(!reflect.DeepEqual(oldAPIServer.Spec.TLSSecurityProfile, newAPIServer.Spec.TLSSecurityProfile) ||
			oldAPIServer.Spec.TLSAdherence != newAPIServer.Spec.TLSAdherence)
}

func registerAPIServerTLSReload(
	ctx context.Context,
	mgr ctrl.Manager,
	reloadRequested *atomic.Bool,
	logger logr.Logger,
) (context.Context, error) {
	mgrCtx, cancel := context.WithCancel(ctx)
	watcher := &apiServerTLSChangeWatcher{onChange: func() {
		logger.Info("APIServer TLS profile changed, initiating graceful shutdown to reload TLS")
		markTLSReloadRequested(reloadRequested, cancel)
	}}
	if err := watcher.SetupWithManager(mgr); err != nil {
		cancel()
		return ctx, err
	}
	return mgrCtx, nil
}
