package metrics

import "time"

func ReconcileTimer(module string, errPtr *error) func() {
	start := time.Now()

	return func() {
		result := "success"
		if errPtr != nil && *errPtr != nil {
			result = "error"
		}

		duration := time.Since(start).Seconds()
		ReconcileDuration.WithLabelValues(module, result).Observe(duration)
		ReconcileTotal.WithLabelValues(module, result).Inc()
	}
}
