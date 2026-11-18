package controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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
	"sigs.k8s.io/yaml"

	v1alpha1 "github.com/opendatahub-io/odh-observability/api/v1alpha1"
	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
)

// The GPU panels read accelerator_* through the cluster-wide Data Science lane:
// the default namespace label identifies the collector target rather than the
// GPU workload, so the namespace-scoped datasource cannot serve them, and
// OpenShift Thanos never sees the Data Science recording rules.
const clusterPrometheusDatasourceName = "data-science-cluster-prometheus-datasource"
const dataSciencePrometheusDatasourceName = "data-science-prometheus-datasource"

// The capacity panel is the one query the Data Science stack cannot answer:
// kube-state-metrics is scraped by OpenShift Monitoring, so that panel pins the
// OCP admin lane instead. Both lanes are caller-token and cluster-wide, so the
// dashboard never needs a namespace-scoped datasource.
const ocpPrometheusDatasourceName = "cluster-prometheus-datasource"

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

	// Fleet panels need the cluster-wide lane for unattributed accelerator
	// series. The by-project panel uses the namespace-scoped lane because #162
	// promotes verified built-in DCGM workload identity to namespace/pod.
	ocpLaneQueries := []string{`sum(kube_node_status_capacity{resource="nvidia_com_gpu"}) or vector(-1)`}
	namespaceScopedQueries := []string{
		`sum by (namespace) (max by (namespace, node, gpu) (accelerator_gpu_utilization{odh_dcgm_source="verified", namespace=~".+"}))`,
	}
	wantQueries := []string{
		"count(count by (node, gpu) (accelerator_gpu_utilization)) or vector(0)",
		`sum(kube_node_status_capacity{resource="nvidia_com_gpu"}) or vector(-1)`,
		"avg(accelerator_gpu_utilization)",
		"count(accelerator_temperature_celsius > 80) or on() (count(accelerator_temperature_celsius) * 0)",
		"accelerator_gpu_utilization",
		"accelerator_memory_used_bytes",
		"accelerator_temperature_celsius",
		"vector(83)",
		"accelerator_power_usage_watts",
		`sum by (namespace) (max by (namespace, node, gpu) (accelerator_gpu_utilization{odh_dcgm_source="verified", namespace=~".+"}))`,
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
				wantDatasource := clusterPrometheusDatasourceName
				if slices.Contains(ocpLaneQueries, q.query) {
					wantDatasource = ocpPrometheusDatasourceName
				} else if slices.Contains(namespaceScopedQueries, q.query) {
					wantDatasource = dataSciencePrometheusDatasourceName
				}
				if q.datasource != wantDatasource {
					t.Errorf("panel %q must pin %s, got %q", q.panel, wantDatasource, q.datasource)
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

			assertGPUDashboardCapacityUnavailableMapping(t, panels)
			validateGPUDashboardFormatSchema(t, dashboard.Object)

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
			wantFormat := "{{node}} / GPU {{gpu}}"
			if format != wantFormat {
				t.Errorf("series name format: want %q, got %q", wantFormat, format)
			}

			// The by-project panel must use only the verified built-in DCGM
			// identity promoted by the namespace-scoped Data Science lane.
			correlation, ok := panels["gpuByProject"].(map[string]any)
			if !ok {
				t.Fatalf("gpuByProject has unexpected type %T", panels["gpuByProject"])
			}
			description, _, _ := unstructured.NestedString(correlation, "spec", "display", "description")
			if !strings.Contains(description, "not additive") {
				t.Errorf("by-project panel must explain shared-device attribution, got %q", description)
			}
			if !strings.Contains(description, "verified") || !strings.Contains(description, "selected project") {
				t.Errorf("by-project panel must explain verified namespace-scoped attribution, got %q", description)
			}
		})
	}
}

func assertGPUDashboardCapacityUnavailableMapping(t *testing.T, panels map[string]any) {
	t.Helper()

	// An empty capacity result cannot distinguish zero GPUs from
	// kube-state-metrics being unavailable. The sentinel and supported
	// StatChart mapping make that distinction explicit in the UI.
	advertised, ok := panels["gpusAdvertised"].(map[string]any)
	if !ok {
		t.Fatalf("gpusAdvertised has unexpected type %T", panels["gpusAdvertised"])
	}
	mappings, found, err := unstructured.NestedSlice(advertised, "spec", "plugin", "spec", "mappings")
	if err != nil || !found || len(mappings) != 1 {
		t.Fatalf("gpusAdvertised must define an unavailable mapping: found=%t error=%v", found, err)
	}
	mapping, ok := mappings[0].(map[string]any)
	if !ok {
		t.Fatalf("gpusAdvertised mapping has unexpected type %T", mappings[0])
	}
	mappingKind, _, _ := unstructured.NestedString(mapping, "kind")
	mappingValue, _, _ := unstructured.NestedString(mapping, "spec", "value")
	mappingResult, _, _ := unstructured.NestedString(mapping, "spec", "result", "value")
	if mappingKind != "Value" || mappingValue != "-1" || mappingResult != "Unavailable" {
		t.Errorf("gpusAdvertised unavailable mapping: got kind=%q value=%q result=%q",
			mappingKind, mappingValue, mappingResult)
	}
}

func validateGPUDashboardFormatSchema(t *testing.T, node any) {
	t.Helper()

	switch value := node.(type) {
	case map[string]any:
		if format, ok := value["format"].(map[string]any); ok {
			unit, _, _ := unstructured.NestedString(format, "unit")
			// Both target API versions support this common format set. In
			// particular, v1alpha1 predates celsius and Perses never supported
			// watts, so those units must not enter either dashboard template.
			switch unit {
			case "decimal", "percent", "bytes":
			default:
				t.Errorf("unsupported Perses format unit %q", unit)
			}
		}
		for _, child := range value {
			validateGPUDashboardFormatSchema(t, child)
		}
	case []any:
		for _, child := range value {
			validateGPUDashboardFormatSchema(t, child)
		}
	}
}

func TestPersesGPUHealthDashboardPromQL(t *testing.T) {
	promtool, err := exec.LookPath("promtool")
	if err != nil {
		t.Skip("promtool is not installed")
	}

	for _, path := range []string{
		PersesGPUHealthDashboardV1Alpha1Template,
		PersesGPUHealthDashboardV1Alpha2Template,
	} {
		t.Run(path, func(t *testing.T) {
			resources := renderObservabilityTemplate(t, path, map[string]any{
				"Namespace": testMonitoringNamespace,
			})
			if len(resources) != 1 {
				t.Fatalf("expected one dashboard, got %d", len(resources))
			}
			configPath := []string{"spec"}
			if path == PersesGPUHealthDashboardV1Alpha2Template {
				configPath = []string{"spec", "config"}
			}

			rules := make([]map[string]any, 0)
			for index, query := range gpuDashboardQueries(t, resources[0], configPath...) {
				rules = append(rules, map[string]any{
					"record": fmt.Sprintf("gpu_dashboard_query_%d", index),
					"expr":   query.query,
				})
			}
			document, err := yaml.Marshal(map[string]any{
				"groups": []any{map[string]any{"name": "gpu-dashboard", "rules": rules}},
			})
			if err != nil {
				t.Fatal(err)
			}

			rulesPath := filepath.Join(t.TempDir(), "rules.yaml")
			if err := os.WriteFile(rulesPath, document, 0o600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(t.Context(), promtool, "check", "rules", "--lint=none", rulesPath)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("promtool check rules: %v\n%s", err, output)
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
