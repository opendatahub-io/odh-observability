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

package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/opendatahub-io/odh-platform-utilities/pkg/deploy"
	configv1 "github.com/openshift/api/config/v1"
	routev1 "github.com/openshift/api/route/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
	moncontroller "github.com/opendatahub-io/odh-observability/internal/controller"
	monwebhook "github.com/opendatahub-io/odh-observability/internal/webhook"
)

var scheme = runtime.NewScheme()

const (
	prometheusInstantQueryPath = "/api/v1/query"
	formContentType            = "application/x-www-form-urlencoded"
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	utilruntime.Must(configv1.Install(scheme))
	utilruntime.Must(routev1.Install(scheme))
	utilruntime.Must(extv1.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
	utilruntime.Must(batchv1.AddToScheme(scheme))
	utilruntime.Must(networkingv1.AddToScheme(scheme))
	utilruntime.Must(rbacv1.AddToScheme(scheme))
}

func main() {
	// Set a logger before the method-gate path returns. ctrl.Log discards
	// records until SetLogger runs, so a failure there would exit silently.
	// The controller path replaces this with the flag-configured logger below.
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	// The "method-gate" subcommand runs a minimal read-only reverse proxy
	// (see runMethodGate). It reuses this operator image as
	// a sidecar, so no extra container image has to be shipped for disconnected
	// installs. It must be handled before the controller-manager flag setup below.
	if len(os.Args) > 1 && os.Args[1] == "method-gate" {
		if err := runMethodGate(os.Args[2:]); err != nil {
			ctrl.Log.WithName("method-gate").Error(err, "method gate failed")
			os.Exit(1)
		}
		return
	}

	var (
		metricsAddr    string
		probeAddr      string
		leaderElect    bool
		webhookCertDir string
		enableWebhook  bool
	)

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8443", "The address the metrics endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&leaderElect, "leader-elect", false, "Enable leader election for controller manager.")
	flag.StringVar(&webhookCertDir, "webhook-cert-dir", "/tmp/k8s-webhook-server/serving-certs", "Directory containing the webhook TLS certificate.")
	flag.BoolVar(&enableWebhook, "enable-webhook", false, "Enable the mutating admission webhook (requires TLS certs to be mounted).")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")
	setupLog.Info("odh-observability", "version", os.Getenv("OPERATOR_VERSION"))

	cfg := ctrl.GetConfigOrDie()

	mgrOpts := ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress:    metricsAddr,
			SecureServing:  true,
			CertDir:        "/etc/metrics-tls",
			CertName:       "tls.crt",
			KeyName:        "tls.key",
			FilterProvider: filters.WithAuthenticationAndAuthorization,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "odh-observability-leader",
	}

	if enableWebhook {
		mgrOpts.WebhookServer = webhook.NewServer(webhook.Options{
			Port:    9443,
			CertDir: webhookCertDir,
		})
	}

	mgr, err := ctrl.NewManager(cfg, mgrOpts)
	if err != nil {
		setupLog.Error(err, "Unable to create manager")
		os.Exit(1)
	}

	deployer := deploy.NewDeployer(
		deploy.WithFieldOwner("monitoring"),
		deploy.WithMode(deploy.ModeSSA),
		deploy.WithCache(),
		deploy.WithApplyOrder(),
	)

	dynamicClient := dynamic.NewForConfigOrDie(cfg)
	discoveryClient := discovery.NewDiscoveryClientForConfigOrDie(cfg)

	if err := (&moncontroller.MonitoringReconciler{
		Client:          mgr.GetClient(),
		Scheme:          mgr.GetScheme(),
		Deployer:        deployer,
		DynamicClient:   dynamicClient,
		DiscoveryClient: discoveryClient,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Unable to create Monitoring controller")
		os.Exit(1)
	}

	if enableWebhook {
		injector := &monwebhook.Injector{
			Client:  mgr.GetClient(),
			Decoder: admission.NewDecoder(mgr.GetScheme()),
		}
		if err := injector.SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "Unable to register monitoring webhook")
			os.Exit(1)
		}

		// The probe namespace must carry monitoring.opendatahub.io/scrape=true for the
		// webhook namespaceSelector to match the canary; the Helm chart provisions it.
		if probeNamespace := os.Getenv("WEBHOOK_PROBE_NAMESPACE"); probeNamespace != "" {
			probe := monwebhook.NewAdmissionProbe(mgr.GetClient(), probeNamespace, 5*time.Minute)
			if err := mgr.Add(probe); err != nil {
				setupLog.Error(err, "Unable to register webhook admission probe")
				os.Exit(1)
			}
		} else {
			setupLog.Info("Webhook admission probe disabled: WEBHOOK_PROBE_NAMESPACE not set")
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Problem running manager")
		os.Exit(1)
	}
}

// methodGateHandler restricts methods independently of upstream authorization.
// When enabled for the Thanos proxy, form POSTs are restricted to read-only
// query APIs and cannot contain namespace, which is authorized only in the URL.
func methodGateHandler(upstream *url.URL, allowFormPost bool) http.Handler {
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			proxy.ServeHTTP(w, r)
			return
		}
		if !allowFormPost || r.Method != http.MethodPost || !isReadOnlyPrometheusPostPath(r.URL.Path) {
			message := "method not allowed"
			if !allowFormPost {
				message = "only GET and HEAD are allowed"
			}
			http.Error(w, message, http.StatusForbidden)
			return
		}
		if !validateQueryForm(w, r) {
			return
		}
		proxy.ServeHTTP(w, r)
	})
}

func isReadOnlyPrometheusPostPath(path string) bool {
	switch path {
	case prometheusInstantQueryPath, "/api/v1/query_range", "/api/v1/query_exemplars", "/api/v1/series", "/api/v1/labels":
		return true
	default:
		return false
	}
}

// validateQueryForm leaves the original body intact after checking it with the
// same URL-form parser used by prom-label-proxy. Fail closed on ambiguous or
// malformed input so no body namespace can bypass the URL-only SAR.
func validateQueryForm(w http.ResponseWriter, r *http.Request) bool {
	defer r.Body.Close()
	contentTypes := r.Header.Values("Content-Type")
	if len(contentTypes) != 1 || len(r.Header.Values("Content-Encoding")) != 0 {
		http.Error(w, "expected an unencoded form", http.StatusUnsupportedMediaType)
		return false
	}
	mediaType, _, err := mime.ParseMediaType(contentTypes[0])
	if err != nil || mediaType != formContentType {
		http.Error(w, "expected an unencoded form", http.StatusUnsupportedMediaType)
		return false
	}
	const maxFormBytes = 10 << 20 // Match net/http's default ParseForm limit.
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxFormBytes))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "form too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "could not read form", http.StatusBadRequest)
		}
		return false
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return false
	}
	if _, exists := form["namespace"]; exists {
		http.Error(w, "namespace must be in the URL", http.StatusForbidden)
		return false
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	return true
}

// runMethodGate runs the read-only method gate described in methodGateHandler.
// It is wired as a sidecar next to prom-label-proxy and shares the operator
// image, so no additional container image has to be shipped or mirrored for
// disconnected installs. It returns an error so the caller can decide on the
// process exit code without defeating deferred cleanup.
func runMethodGate(argv []string) error {
	setupLog := ctrl.Log.WithName("method-gate")

	fs := flag.NewFlagSet("method-gate", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:9092", "address the method gate listens on")
	upstream := fs.String("upstream", "http://127.0.0.1:9091", "upstream to forward GET/HEAD requests to")
	allowFormPost := fs.Bool("allow-form-post", false, "allow read-only form POSTs without a body namespace")
	if err := fs.Parse(argv); err != nil {
		return err
	}

	upstreamURL, err := url.Parse(*upstream)
	if err != nil {
		return fmt.Errorf("invalid --upstream URL %q: %w", *upstream, err)
	}
	if upstreamURL.Scheme == "" || upstreamURL.Host == "" {
		return fmt.Errorf("invalid --upstream URL %q: missing scheme or host", *upstream)
	}

	srv := &http.Server{
		Addr:              *listen,
		Handler:           methodGateHandler(upstreamURL, *allowFormPost),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ListenAndServe returns as soon as Shutdown closes the listener.
	// Wait for this goroutine so in-flight connections finish draining
	// before the process exits.
	var drained sync.WaitGroup
	drained.Go(func() {
		<-ctx.Done()
		// WithoutCancel keeps the parent's values while dropping its (already
		// fired) cancellation, so the shutdown gets its own fresh deadline.
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			setupLog.Error(err, "method gate shutdown failed")
		}
	})

	setupLog.Info("starting method gate", "listen", *listen, "upstream", *upstream)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("method gate exited: %w", err)
	}
	drained.Wait()
	return nil
}
