package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	odhLabels "github.com/opendatahub-io/odh-platform-utilities/pkg/metadata/labels"
	rendertemplate "github.com/opendatahub-io/odh-platform-utilities/pkg/render/template"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
)

// The GPU panels read accelerator_* through the cluster-wide Data Science lane:
// those series carry no namespace label, so the scoped datasource cannot serve
// them, and OpenShift Thanos never sees the Data Science recording rules.
const clusterPrometheusDatasourceName = "data-science-cluster-prometheus-datasource"

// noMatchForPerses models a cluster whose API server does not serve the Perses
// group at all, the shape hasCRD treats as "CRD absent" rather than an error.
func noMatchForPerses() interceptor.Funcs {
	return interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			gvk := list.GetObjectKind().GroupVersionKind()
			if gvk.Group == "perses.dev" {
				return &meta.NoKindMatchError{GroupKind: gvk.GroupKind()}
			}
			return c.List(ctx, list, opts...)
		},
	}
}

type gpuDashboardQuery struct {
	panel      string
	query      string
	datasource string
	kind       string
}

func gpuDashboardPanels(t *testing.T, dashboard unstructured.Unstructured, configPath ...string) map[string]any {
	t.Helper()

	panels, found, err := unstructured.NestedMap(dashboard.Object,
		append(slices.Clone(configPath), "panels")...)
	if err != nil || !found {
		t.Fatalf("dashboard has no panels: found=%t error=%v", found, err)
	}
	return panels
}

func gpuDashboardQueries(t *testing.T, dashboard unstructured.Unstructured, configPath ...string) []gpuDashboardQuery {
	t.Helper()

	var out []gpuDashboardQuery
	for name, raw := range gpuDashboardPanels(t, dashboard, configPath...) {
		panel, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("panel %q has unexpected type %T", name, raw)
		}
		queries, found, err := unstructured.NestedSlice(panel, "spec", "queries")
		if err != nil || !found {
			t.Fatalf("panel %q has no queries: found=%t error=%v", name, found, err)
		}
		for _, rawQuery := range queries {
			query, ok := rawQuery.(map[string]any)
			if !ok {
				t.Fatalf("panel %q query has unexpected type %T", name, rawQuery)
			}
			spec, _, _ := unstructured.NestedString(query, "spec", "plugin", "spec", "query")
			datasource, _, _ := unstructured.NestedString(query, "spec", "plugin", "spec", "datasource", "name")
			kind, _, _ := unstructured.NestedString(query, "spec", "plugin", "spec", "datasource", "kind")
			out = append(out, gpuDashboardQuery{panel: name, query: spec, datasource: datasource, kind: kind})
		}
	}
	return out
}

func TestPersesGPUHealthDashboardTemplateContract(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		apiVersion string
		configPath []string
	}{
		{
			name:       "v1alpha1",
			path:       PersesGPUHealthDashboardV1Alpha1Template,
			apiVersion: "perses.dev/v1alpha1",
			configPath: []string{"spec"},
		},
		{
			name:       "v1alpha2",
			path:       PersesGPUHealthDashboardV1Alpha2Template,
			apiVersion: "perses.dev/v1alpha2",
			configPath: []string{"spec", "config"},
		},
	}

	// Every panel reads the accelerator recording rules, which this dashboard
	// can only reach through the cluster-scoped OpenShift datasource: the
	// namespace proxy injects namespace= and the collector strips Kubernetes
	// identity from accelerator series, so a scoped datasource never matches.
	wantQueries := []string{
		"count(count by (Hostname, gpu) (accelerator_gpu_utilization))",
		`sum(kube_node_status_capacity{resource="nvidia_com_gpu"})`,
		"avg(accelerator_gpu_utilization)",
		"count(accelerator_temperature_celsius > 80)",
		"accelerator_gpu_utilization",
		"accelerator_memory_used_bytes",
		"accelerator_temperature_celsius",
		"vector(83)",
		"accelerator_power_usage_watts",
		`sum by (exported_namespace) (accelerator_gpu_utilization{exported_namespace!="nvidia-gpu-operator"})`,
	}
	wantPanels := []string{
		"gpusReporting", "gpusAdvertised", "meanUtilization", "hotGpus",
		"gpuUtilization", "gpuFramebuffer", "gpuTemperature", "gpuPower", "gpuByProject",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resources := renderObservabilityTemplate(t, tt.path, map[string]any{
				"Namespace": testMonitoringNamespace,
			})
			if len(resources) != 1 {
				t.Fatalf("expected one resource, got %d", len(resources))
			}
			dashboard := resources[0]
			if dashboard.GetAPIVersion() != tt.apiVersion || dashboard.GetKind() != "PersesDashboard" {
				t.Fatalf("unexpected resource identity: apiVersion=%q kind=%q",
					dashboard.GetAPIVersion(), dashboard.GetKind())
			}
			if dashboard.GetName() != "data-science-gpu-health" || dashboard.GetNamespace() != testMonitoringNamespace {
				t.Fatalf("unexpected dashboard metadata: name=%q namespace=%q",
					dashboard.GetName(), dashboard.GetNamespace())
			}
			// The part-of label is what lets the garbage collector own the
			// dashboard: without it a dashboard whose gate closes would linger
			// and reappear against a Perses that has no GPU datasource.
			if labels := dashboard.GetLabels(); labels[odhLabels.PlatformPartOf] != monitoringPartOf {
				t.Fatalf("dashboard must carry the part-of label, got %#v", labels)
			}

			name, _, _ := unstructured.NestedString(dashboard.Object,
				append(slices.Clone(tt.configPath), "display", "name")...)
			if name != "RHOAI GPU Health" {
				t.Errorf("display name: want %q, got %q", "RHOAI GPU Health", name)
			}

			panels := gpuDashboardPanels(t, dashboard, tt.configPath...)
			if len(panels) != len(wantPanels) {
				t.Errorf("panel count: want %d, got %d", len(wantPanels), len(panels))
			}
			for _, panel := range wantPanels {
				if _, ok := panels[panel]; !ok {
					t.Errorf("missing panel %q", panel)
				}
			}

			queries := gpuDashboardQueries(t, dashboard, tt.configPath...)
			gotQueries := make([]string, 0, len(queries))
			for _, q := range queries {
				if q.datasource != clusterPrometheusDatasourceName {
					t.Errorf("panel %q must pin %s, got %q", q.panel, clusterPrometheusDatasourceName, q.datasource)
				}
				if q.kind != "PrometheusDatasource" {
					t.Errorf("panel %q must use PrometheusDatasource, got %q", q.panel, q.kind)
				}
				if strings.Contains(q.query, "data-science-prometheus-datasource") ||
					strings.Contains(q.query, "cluster-prometheus-tenancy-datasource") {
					t.Errorf("panel %q reaches a scoped datasource: %q", q.panel, q.query)
				}
				gotQueries = append(gotQueries, q.query)
			}
			for _, want := range wantQueries {
				if !slices.Contains(gotQueries, want) {
					t.Errorf("missing query %q in %v", want, gotQueries)
				}
			}

			// Series names are the one place the dashboard needs Prometheus'
			// {{label}} syntax, which the template engine would otherwise eat.
			utilization, ok := panels["gpuUtilization"].(map[string]any)
			if !ok {
				t.Fatalf("gpuUtilization has unexpected type %T", panels["gpuUtilization"])
			}
			rawQueries, _, _ := unstructured.NestedSlice(utilization, "spec", "queries")
			first, ok := rawQueries[0].(map[string]any)
			if !ok {
				t.Fatalf("gpuUtilization query has unexpected type %T", rawQueries[0])
			}
			format, _, _ := unstructured.NestedString(first, "spec", "plugin", "spec", "seriesNameFormat")
			if format != "GPU {{gpu}}" {
				t.Errorf("series name format: want %q, got %q", "GPU {{gpu}}", format)
			}

			// The correlation panel is a deliberate empty state, so its
			// description has to say why it carries no data.
			correlation, ok := panels["gpuByProject"].(map[string]any)
			if !ok {
				t.Fatalf("gpuByProject has unexpected type %T", panels["gpuByProject"])
			}
			description, _, _ := unstructured.NestedString(correlation, "spec", "display", "description")
			if !strings.Contains(description, "pod-level") {
				t.Errorf("correlation panel must explain the missing pod attribution, got %q", description)
			}
		})
	}
}

func TestDeployPersesGPUHealthDashboard(t *testing.T) {
	metricsStorage := func() *v1alpha1.Metrics {
		return &v1alpha1.Metrics{Storage: &v1alpha1.MetricsStorage{}}
	}

	tests := []struct {
		name          string
		metrics       *v1alpha1.Metrics
		persesFound   bool
		dashboardCRD  bool
		persesVersion string
		wantTemplate  string
	}{
		{
			name:          "metrics not configured",
			metrics:       nil,
			persesFound:   true,
			dashboardCRD:  true,
			persesVersion: persesV1Alpha2,
		},
		{
			name:          "exporters-only has no storage, so no rule or datasource",
			metrics:       &v1alpha1.Metrics{},
			persesFound:   true,
			dashboardCRD:  true,
			persesVersion: persesV1Alpha2,
		},
		{
			name:          "Perses not deployed",
			metrics:       metricsStorage(),
			persesFound:   false,
			dashboardCRD:  true,
			persesVersion: persesV1Alpha2,
		},
		{
			name:          "PersesDashboard CRD missing",
			metrics:       metricsStorage(),
			persesFound:   true,
			dashboardCRD:  false,
			persesVersion: persesV1Alpha2,
		},
		{
			name:          "v1alpha2 cluster",
			metrics:       metricsStorage(),
			persesFound:   true,
			dashboardCRD:  true,
			persesVersion: persesV1Alpha2,
			wantTemplate:  PersesGPUHealthDashboardV1Alpha2Template,
		},
		{
			name:          "v1alpha1 cluster",
			metrics:       metricsStorage(),
			persesFound:   true,
			dashboardCRD:  true,
			persesVersion: "v1alpha1",
			wantTemplate:  PersesGPUHealthDashboardV1Alpha1Template,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newActionsTestScheme(t)
			_, _, dashboardGVK := persesGVKs(tt.persesVersion)
			if tt.dashboardCRD {
				registerCRDs(s, dashboardGVK)
			}

			m := newMonitoring(v1alpha1.MonitoringInstanceName)
			m.Spec.Metrics = tt.metrics
			cm := conditions.NewConditionsManager(m, m.Generation)

			builder := fake.NewClientBuilder().WithScheme(s)
			if !tt.dashboardCRD {
				builder = builder.WithInterceptorFuncs(noMatchForPerses())
			}

			var sources []rendertemplate.TemplateSource
			err := deployPersesGPUHealthDashboard(context.Background(),
				builder.Build(),
				m, cm, &sources, tt.persesVersion, tt.persesFound)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.wantTemplate == "" {
				if len(sources) != 0 {
					t.Fatalf("expected no dashboard, got %d sources", len(sources))
				}
				return
			}
			if len(sources) != 1 {
				t.Fatalf("expected one dashboard source, got %d", len(sources))
			}
			if sources[0].Path != tt.wantTemplate {
				t.Errorf("template: want %q, got %q", tt.wantTemplate, sources[0].Path)
			}
		})
	}
}

func TestDeployPersesGPUHealthDashboard_CRDCheckFailurePropagates(t *testing.T) {
	s := newActionsTestScheme(t)
	_, _, dashboardGVK := persesGVKs(persesV1Alpha2)
	registerCRDs(s, dashboardGVK)

	m := newMonitoring(v1alpha1.MonitoringInstanceName)
	m.Spec.Metrics = &v1alpha1.Metrics{Storage: &v1alpha1.MetricsStorage{}}
	cm := conditions.NewConditionsManager(m, m.Generation)

	cli := fake.NewClientBuilder().WithScheme(s).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if list.GetObjectKind().GroupVersionKind().Group == "perses.dev" {
					return errors.New("apiserver unavailable")
				}
				return c.List(ctx, list, opts...)
			},
		}).Build()

	var sources []rendertemplate.TemplateSource
	if err := deployPersesGPUHealthDashboard(context.Background(), cli, m, cm, &sources,
		persesV1Alpha2, true); err == nil {
		t.Fatal("expected a CRD lookup failure to surface")
	}
}
