package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// ReconcileDuration measures reconcile loop duration in seconds.
//
//nolint:gochecknoglobals
var ReconcileDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "module_reconcile_duration_seconds",
		Help:    "Duration in seconds of a module operator reconcile loop.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"module", "result"},
)

// ReconcileTotal counts total reconcile invocations.
//
//nolint:gochecknoglobals
var ReconcileTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "module_reconcile_total",
		Help: "Total number of reconcile invocations per module operator.",
	},
	[]string{"module", "result"},
)

//nolint:gochecknoinits
func init() {
	ctrlmetrics.Registry.MustRegister(ReconcileDuration, ReconcileTotal)
}
