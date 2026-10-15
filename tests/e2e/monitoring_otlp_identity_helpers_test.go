package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildOTLPMetricsPayload(t *testing.T) {
	timestamp := time.Unix(1_800_000_000, 123)
	resourceAttributes := map[string]string{
		"k8s.namespace.name": "tenant-ns",
		"k8s.pod.name":       "victim-pod",
		"k8s.pod.uid":        "victim-uid",
		"k8s.pod.ip":         "192.0.2.10",
	}
	datapointAttributes := map[string]string{
		"exported_namespace": "tenant-ns",
		"exported_pod":       "victim-pod",
		"k8s_pod_name":       "victim-pod",
		"namespace":          "tenant-ns",
		"pod":                "victim-pod",
		"probe_attempt":      "7",
	}

	payload, err := buildOTLPMetricsPayload("odh_identity_probe_7", timestamp, resourceAttributes, datapointAttributes)
	require.NoError(t, err)

	var decoded otlpExportMetricsRequest
	require.NoError(t, json.Unmarshal(payload, &decoded))
	require.Len(t, decoded.ResourceMetrics, 1)
	resourceMetrics := decoded.ResourceMetrics[0]
	require.Equal(t, resourceAttributes, otlpAttributesMap(resourceMetrics.Resource.Attributes))
	require.Len(t, resourceMetrics.ScopeMetrics, 1)
	require.Len(t, resourceMetrics.ScopeMetrics[0].Metrics, 1)
	metric := resourceMetrics.ScopeMetrics[0].Metrics[0]
	require.Equal(t, "odh_identity_probe_7", metric.Name)
	require.Len(t, metric.Gauge.DataPoints, 1)
	datapoint := metric.Gauge.DataPoints[0]
	require.Equal(t, "1800000000000000123", datapoint.TimeUnixNano)
	require.Equal(t, datapointAttributes, otlpAttributesMap(datapoint.Attributes))
}

func TestOTLPSeriesIdentityChecks(t *testing.T) {
	sender := otlpPodIdentity{Namespace: "redhat-ods-monitoring", Name: "otlp-sender", UID: "sender-uid", IP: "10.0.0.10"}
	valid := map[string]string{
		"namespace":          sender.Namespace,
		"pod":                sender.Name,
		"k8s_namespace_name": sender.Namespace,
		"k8s_pod_name":       sender.Name,
		"k8s_pod_uid":        string(sender.UID),
		"k8s_pod_ip":         sender.IP,
	}

	tests := []struct {
		name                  string
		labels                map[string]string
		wantAttributionError  bool
		wantSanitizationError bool
	}{
		{name: "sender identity accepted", labels: valid},
		{
			name: "collector target fallback fails attribution",
			labels: map[string]string{
				"namespace": sender.Namespace,
				"pod":       "data-science-collector-collector-0",
			},
			wantAttributionError:  true,
			wantSanitizationError: true,
		},
		{
			name: "victim identity and exported labels rejected",
			labels: map[string]string{
				"namespace":          "tenant-ns",
				"pod":                "victim-pod",
				"k8s_namespace_name": "tenant-ns",
				"k8s_pod_name":       "victim-pod",
				"k8s_pod_uid":        "victim-uid",
				"k8s_pod_ip":         "192.0.2.10",
				"exported_namespace": "tenant-ns",
				"exported_pod":       "victim-pod",
			},
			wantAttributionError:  true,
			wantSanitizationError: true,
		},
		{
			name: "association hints cannot leak victim identity",
			labels: map[string]string{
				"namespace":          sender.Namespace,
				"pod":                sender.Name,
				"k8s_namespace_name": sender.Namespace,
				"k8s_pod_name":       sender.Name,
				"k8s_pod_uid":        "victim-uid",
				"k8s_pod_ip":         "192.0.2.10",
			},
			wantSanitizationError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			attributionErr := otlpSeriesAttributionError(test.labels, sender)
			sanitizationErr := otlpSeriesSanitizationError(test.labels, sender)
			if test.wantAttributionError {
				require.Error(t, attributionErr)
			} else {
				require.NoError(t, attributionErr)
			}
			if test.wantSanitizationError {
				require.Error(t, sanitizationErr)
			} else {
				require.NoError(t, sanitizationErr)
			}
		})
	}
}

func TestOTLPDeliveryRequiresAcceptedPushAndMonitoringSeries(t *testing.T) {
	snapshot := otlpIdentitySnapshot{
		monitoring: otlpQueryObservation{
			status: http.StatusOK,
			series: []map[string]string{{"__name__": "odh_identity_probe"}},
		},
	}

	t.Run("accepted and observed", func(t *testing.T) {
		evidence := &otlpIdentityEvidence{acceptedPushes: 1, snapshots: []otlpIdentitySnapshot{snapshot}}
		require.NoError(t, otlpDeliveryError(evidence, "odh_identity_probe"))
	})
	t.Run("query alone is not delivery", func(t *testing.T) {
		evidence := &otlpIdentityEvidence{snapshots: []otlpIdentitySnapshot{snapshot}}
		require.Error(t, otlpDeliveryError(evidence, "odh_identity_probe"))
	})
	t.Run("accepted push without observed series is not delivery", func(t *testing.T) {
		evidence := &otlpIdentityEvidence{acceptedPushes: 1}
		require.Error(t, otlpDeliveryError(evidence, "odh_identity_probe"))
	})
	t.Run("non-success query is not delivery", func(t *testing.T) {
		failed := otlpIdentitySnapshot{monitoring: otlpQueryObservation{status: http.StatusForbidden}}
		evidence := &otlpIdentityEvidence{acceptedPushes: 1, snapshots: []otlpIdentitySnapshot{failed}}
		require.Error(t, otlpDeliveryError(evidence, "odh_identity_probe"))
	})
}

func TestOTLPTenantIsolationRequiresSuccessfulEmptyQueries(t *testing.T) {
	start := time.Unix(1_800_000_000, 0)
	minSpan := 30 * time.Second
	emptySnapshots := []otlpIdentitySnapshot{
		{at: start, tenant: otlpQueryObservation{status: http.StatusOK}},
		{at: start.Add(10 * time.Second), tenant: otlpQueryObservation{status: http.StatusOK}},
		{at: start.Add(20 * time.Second), tenant: otlpQueryObservation{status: http.StatusOK}},
		{at: start.Add(minSpan), tenant: otlpQueryObservation{status: http.StatusOK}},
	}

	t.Run("four empty results span three intervals", func(t *testing.T) {
		require.NoError(t, otlpTenantIsolationError(emptySnapshots, minSpan))
	})
	t.Run("injected series fails immediately", func(t *testing.T) {
		injected := append([]otlpIdentitySnapshot(nil), emptySnapshots...)
		injected[1].tenant.series = []map[string]string{{"namespace": "tenant-ns", "pod": "victim-pod"}}
		require.Error(t, otlpTenantIsolationError(injected, minSpan))
	})
	t.Run("query error is not an empty success", func(t *testing.T) {
		observations := append([]otlpIdentitySnapshot(nil), emptySnapshots[:3]...)
		observations = append(observations, otlpIdentitySnapshot{
			at: start.Add(minSpan), tenant: otlpQueryObservation{status: http.StatusOK, err: errors.New("query failed")},
		})
		require.Error(t, otlpTenantIsolationError(observations, minSpan))
	})
	t.Run("non-200 response is not an empty success", func(t *testing.T) {
		observations := append([]otlpIdentitySnapshot(nil), emptySnapshots[:3]...)
		observations = append(observations, otlpIdentitySnapshot{
			at: start.Add(minSpan), tenant: otlpQueryObservation{status: http.StatusForbidden},
		})
		require.Error(t, otlpTenantIsolationError(observations, minSpan))
	})
	t.Run("empty results must span the full observation window", func(t *testing.T) {
		observations := append([]otlpIdentitySnapshot(nil), emptySnapshots...)
		require.Error(t, otlpTenantIsolationError(observations, minSpan+time.Second))
	})
}

func TestQueryOTLPMetricRejectsPrometheusErrorStatus(t *testing.T) {
	server, rootCAs := startQueriedTLSServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"error","errorType":"bad_data","error":"query failed"}`))
	})
	routeHost := strings.TrimPrefix(server.URL, "https://")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	observation := queryOTLPMetric(ctx, rootCAs, routeHost, "token", "tenant-ns", "odh_identity_probe")
	require.Equal(t, http.StatusOK, observation.status)
	require.Error(t, observation.err)
	require.False(t, observation.successful(), "Prometheus API errors must not count as empty successful queries")
}

func TestParseOTLPHTTPResponse(t *testing.T) {
	tests := []struct {
		name       string
		output     string
		wantStatus int
		wantBody   string
		wantError  bool
	}{
		{name: "success with body", output: "{}\n200", wantStatus: http.StatusOK, wantBody: "{}"},
		{name: "success without body", output: "\n204", wantStatus: http.StatusNoContent},
		{name: "rejected request", output: "denied\n403", wantStatus: http.StatusForbidden, wantBody: "denied", wantError: true},
		{name: "missing status", output: "no response", wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status, body, err := parseOTLPHTTPResponse(test.output)
			require.Equal(t, test.wantStatus, status)
			require.Equal(t, test.wantBody, body)
			if test.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestJSONInt64(t *testing.T) {
	tests := []struct {
		name  string
		value json.RawMessage
		want  int64
	}{
		{name: "protobuf JSON string", value: json.RawMessage(`"12"`), want: 12},
		{name: "JSON number", value: json.RawMessage(`12`), want: 12},
		{name: "omitted", value: nil},
		{name: "null", value: json.RawMessage(`null`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := jsonInt64(test.value)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func otlpAttributesMap(values []otlpKeyValue) map[string]string {
	result := make(map[string]string, len(values))
	for _, value := range values {
		result[value.Key] = value.Value.StringValue
	}
	return result
}
