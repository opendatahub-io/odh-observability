package e2e_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/opendatahub-io/odh-observability/tests/e2e/aigrid"
)

// TestAIGridObservability uses preinstalled controllers, MaaS gateways and models.
// It owns temporary Grafana, MaaS policies/subscriptions/keys and inference traffic.
func TestAIGridObservability(t *testing.T) {
	if !testOpts.aiGridEnabled {
		t.Skip("AI Grid observability is opt-in; pass -ai-grid-enabled=true or E2E_TEST_AI_GRID_ENABLED=true")
	}
	cfg, err := aigrid.Load(testOpts.aiGridConfig)
	require.NoError(t, err, "AI Grid input configuration")

	hub, hubID := aiGridClient(t, cfg.HubContext)
	seenIDs := map[string]bool{hubID: true}
	spokeClients := make(map[string]client.Client, len(cfg.Spokes))
	for _, s := range cfg.Spokes {
		spoke, id := aiGridClient(t, s.Context)
		require.Equal(t, s.ClusterID, id, "[%s] kube context must resolve to the configured source cluster", s.Context)
		require.False(t, seenIDs[id], "[%s] hub and spokes must be three distinct clusters", s.Context)
		seenIDs[id] = true
		aiGridPreflight(t, hub, spoke, s)
		spokeClients[s.Context] = spoke
	}

	// Validate all credential and CA files before creating any cluster resources.
	credentials := make([]string, 0, 5)
	for _, filename := range []string{cfg.Metrics.TokenFile, cfg.Logs.TokenFile} {
		value, readErr := aigrid.ReadCredential(filename)
		require.NoError(t, readErr)
		credentials = append(credentials, value)
	}
	for _, filename := range []string{cfg.Grafana.RouteCAFile, cfg.Metrics.CAFile, cfg.Logs.CAFile, cfg.Spokes[0].CAFile, cfg.Spokes[1].CAFile} {
		_, caErr := aigrid.HTTPClient(filename)
		require.NoError(t, caErr)
	}
	projectRoot, err := findProjectRoot()
	require.NoError(t, err)
	oats := ensureOatsBinary(t, projectRoot)
	_, gcxArgs, err := gcxTooling(t.Context())
	require.NoError(t, err, "pinned GCX tooling")

	grafana := aiGridGrafana(t, hub, cfg)
	credentials = append(credentials, grafana.Password)
	for _, ds := range []struct {
		uid, name, kind string
		backend         aigrid.Backend
	}{
		{aigrid.MetricsUID, "ACM Thanos", "prometheus", cfg.Metrics},
		{aigrid.LogsUID, "Hub Loki", "loki", cfg.Logs},
	} {
		require.NoError(t, grafana.AddDatasource(t.Context(), ds.uid, ds.name, ds.kind, ds.backend), "provision datasource %s", ds.name)
		err := wait.PollUntilContextTimeout(t.Context(), cfg.PollInterval, cfg.Timeout, true, func(ctx context.Context) (bool, error) {
			var health struct {
				Status string `json:"status"`
			}
			queryErr := grafana.Request(ctx, http.MethodGet, "/api/datasources/uid/"+ds.uid+"/health", nil, &health)
			return queryErr == nil && health.Status == "OK", nil
		})
		require.NoError(t, err, "datasource %s health: check its read credential, TLS CA and hub connectivity", ds.name)
	}

	privateDir := t.TempDir()
	gcxFile := filepath.Join(privateDir, "gcx.yaml")
	gcxConfig, err := aigrid.GCXConfig(grafana.URL, grafana.Password, cfg.Grafana.RouteCAFile)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(gcxFile, gcxConfig, 0o600))
	artifactsBase, err := compatibilityReportBaseDir()
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(artifactsBase, 0o750))
	artifacts, err := os.MkdirTemp(artifactsBase, "ai-grid-") //nolint:usetesting // retain redacted artifacts after the test finishes
	require.NoError(t, err)
	t.Logf("AI Grid cases and redacted OATS output: %s", artifacts)

	for i, s := range cfg.Spokes {
		t.Run(s.Context, func(t *testing.T) {
			var lastErr error
			err := wait.PollUntilContextTimeout(t.Context(), cfg.PollInterval, cfg.Timeout, true, func(ctx context.Context) (bool, error) {
				value, queryErr := grafana.Number(ctx, aigrid.CollectionQuery(s), false)
				lastErr = queryErr
				return queryErr == nil && value == 1, nil
			})
			require.NoError(t, err, "[%s] ACM Thanos collection preflight: all discovered collectors must be UP; last query error: %v", s.Context, lastErr)
			api, err := aigrid.NewMaaSClient(s)
			require.NoError(t, err, "[%s] MaaS management client", s.Context)
			fixture := aigrid.NewFixture(spokeClients[s.Context], api, s)
			t.Logf("[%s] Fresh MaaS policy/subscription/key: %s (revoked/deleted on success or failure)", s.Context, fixture.Name)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
				defer cancel()
				if err := fixture.Cleanup(ctx); err != nil {
					t.Errorf("[%s] cleanup MaaS fixture %s: %v", s.Context, fixture.Name, err)
				}
			})
			apiKey, err := fixture.Prepare(t.Context(), cfg.PollInterval, cfg.Timeout)
			require.NoError(t, err, "[%s] prepare isolated MaaS traffic", s.Context)
			credentials = append(credentials, apiKey)
			s = fixture.Spoke
			baseline, err := grafana.Number(t.Context(), aigrid.CounterQuery(s, cfg.MetricNameRE), true)
			require.NoError(t, err, "[%s] ACM Thanos organization counter baseline", s.Context)
			require.Zero(t, baseline, "[%s] a fresh subscription must have no preexisting inference traffic", s.Context)
			runID := strings.TrimPrefix(fixture.Name, "aigrid-")
			started := time.Now()
			inferenceClient, err := aigrid.HTTPClient(s.CAFile)
			require.NoError(t, err)
			require.NoError(t, aigrid.Infer(t.Context(), inferenceClient, s, apiKey, runID),
				"[%s] inference: check the dedicated subscription, API key, full chat-completions URL and model", s.Context)

			// Numeric index avoids treating a caller-controlled context as a file path.
			caseDir := filepath.Join(artifacts, fmt.Sprintf("spoke-%d", i+1))
			require.NoError(t, os.Mkdir(caseDir, 0o750))
			caseYAML, err := aigrid.RenderCase(s, cfg.MetricNameRE, runID, baseline, started)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(caseDir, "oats-case.yaml"), caseYAML, 0o600))
			require.NoError(t, os.WriteFile(filepath.Join(caseDir, "oats-config.yaml"),
				[]byte("meta:\n  version: 3\ncases:\n  - oats-case.yaml\n"), 0o600))
			args := make([]string, 0, 9+len(gcxArgs))
			args = append(args, "--config", filepath.Join(caseDir, "oats-config.yaml"), "--no-cache",
				"--timeout", cfg.Timeout.String(), "--interval", cfg.PollInterval.String(), "--gcx-context", aigrid.GCXContext)
			args = append(args, gcxArgs...)
			ctx, cancel := context.WithTimeout(t.Context(), 2*cfg.Timeout+time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, oats, args...)
			cmd.Dir = privateDir
			cmd.Env = aigrid.OATSEnv(os.Environ(), gcxFile)
			output, runErr := cmd.CombinedOutput()
			redacted := aigrid.Redact(string(output), credentials...)
			require.NoError(t, os.WriteFile(filepath.Join(caseDir, "oats.log"), []byte(redacted), 0o600))
			require.NoError(t, runErr,
				"[%s] OATS: ACM Thanos must show a new organization counter sample; Hub Loki must show this fresh subscription's usage log with token fields.\n%s",
				s.Context, redacted)
		})
	}
}

//nolint:ireturn // controller-runtime client interface is shared by the E2E helpers
func aiGridClient(t *testing.T, contextName string) (client.Client, string) {
	t.Helper()
	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{CurrentContext: contextName})
	cfg, err := loader.ClientConfig()
	require.NoError(t, err, "load explicit kube context %s", contextName)
	scheme := k8sruntime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	nodes := &corev1.NodeList{}
	require.NoError(t, c.List(t.Context(), nodes), "[%s] read node readiness", contextName)
	readyNodes := 0
	for _, node := range nodes.Items {
		for _, condition := range node.Status.Conditions {
			if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
				readyNodes++
			}
		}
	}
	require.Positive(t, readyNodes,
		"[%s] no Ready nodes among %d nodes; API availability and stale Pod readiness do not establish that the cluster is running",
		contextName, len(nodes.Items))
	version := aiGridObject("config.openshift.io", "v1", "ClusterVersion")
	require.NoError(t, c.Get(t.Context(), types.NamespacedName{Name: "version"}, version), "[%s] authenticate and read cluster identity", contextName)
	id, _, err := unstructured.NestedString(version.Object, "spec", "clusterID")
	require.NoError(t, err)
	require.NotEmpty(t, id)
	return c, id
}

func aiGridPreflight(t *testing.T, hub, spoke client.Client, s aigrid.Spoke) {
	t.Helper()
	managed := aiGridObject("cluster.open-cluster-management.io", "v1", "ManagedCluster")
	require.NoError(t, hub.Get(t.Context(), types.NamespacedName{Name: s.ClusterName}, managed), "[%s] ACM source-cluster membership", s.Context)
	require.True(t, aiGridCondition(managed, "ManagedClusterConditionAvailable"), "[%s] ACM ManagedCluster available", s.Context)
	claims, _, err := unstructured.NestedSlice(managed.Object, "status", "clusterClaims")
	require.NoError(t, err)
	var claimedID string
	for _, raw := range claims {
		claim, ok := raw.(map[string]any)
		if ok && claim["name"] == "id.k8s.io" {
			claimedID, _ = claim["value"].(string)
		}
	}
	require.Equal(t, s.ClusterID, claimedID, "[%s] ACM claim matches source identity", s.Context)
	deployment := &appsv1.Deployment{}
	require.NoError(t, spoke.Get(t.Context(), types.NamespacedName{Namespace: s.ControllerNamespace, Name: "odh-observability"}, deployment))
	require.Positive(t, deployment.Status.AvailableReplicas, "[%s] odh-observability ready", s.Context)
	require.GreaterOrEqual(t, deployment.Status.ObservedGeneration, deployment.Generation, "[%s] controller rollout observed", s.Context)
	desired := ptr.Deref(deployment.Spec.Replicas, int32(1))
	require.Equal(t, desired, deployment.Status.Replicas, "[%s] controller rollout converged", s.Context)
	require.Equal(t, desired, deployment.Status.UpdatedReplicas, "[%s] controller replicas updated", s.Context)
	require.Equal(t, desired, deployment.Status.ReadyReplicas, "[%s] controller replicas ready", s.Context)
	var image string
	for _, c := range deployment.Spec.Template.Spec.Containers {
		if c.Name == "manager" {
			image = c.Image
		}
	}
	require.Equal(t, s.ExpectedControllerImage, image, "[%s] deploy the intended checkout image before running the suite", s.Context)
	monitoring := aiGridObject("services.platform.opendatahub.io", "v1alpha1", "Monitoring")
	require.NoError(t, spoke.Get(t.Context(), types.NamespacedName{Name: "default-monitoring"}, monitoring))
	require.True(t, aiGridCondition(monitoring, "Ready"), "[%s] Monitoring Ready", s.Context)
	job := aiGridObject("monitoring.rhobs", "v1alpha1", "ScrapeConfig")
	key := types.NamespacedName{Namespace: "open-cluster-management-agent-addon", Name: "rhai-metrics"}
	require.NoError(t, spoke.Get(t.Context(), key, job), "[%s] ACM must deliver the rhai-metrics ScrapeConfig", s.Context)
	agent := aiGridObject("monitoring.rhobs", "v1alpha1", "PrometheusAgent")
	key.Name = "user-workload-metrics-collector"
	require.NoError(t, spoke.Get(t.Context(), key, agent))
	require.True(t, aiGridCondition(agent, "Available"), "[%s] workload metrics collector Available", s.Context)
	conditions, _, err := unstructured.NestedSlice(agent.Object, "status", "conditions")
	require.NoError(t, err)
	for _, raw := range conditions {
		condition, ok := raw.(map[string]any)
		require.False(t, ok && condition["reason"] == "NoSelectedResources", "[%s] ACM workload collector selects no resources; check addon registration/delivery", s.Context)
	}
}

func aiGridObject(group, version, kind string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{Group: group, Version: version, Kind: kind})
	return u
}

func aiGridCondition(u *unstructured.Unstructured, name string) bool {
	conditions, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	for _, raw := range conditions {
		condition, ok := raw.(map[string]any)
		if ok && condition["type"] == name && condition["status"] == "True" {
			return true
		}
	}
	return false
}

func aiGridGrafana(t *testing.T, hub client.Client, cfg aigrid.Config) aigrid.Grafana {
	t.Helper()
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		GenerateName: "ai-grid-oats-", Labels: map[string]string{"app.kubernetes.io/managed-by": "ai-grid-oats"},
	}}
	require.NoError(t, hub.Create(t.Context(), namespace))
	t.Logf("Temporary hub Grafana namespace: %s (deleted on success or failure)", namespace.Name)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := hub.Delete(ctx, namespace); err != nil && !k8serr.IsNotFound(err) {
			t.Errorf("delete temporary Grafana namespace %s: %v", namespace.Name, err)
			return
		}
		err := wait.PollUntilContextCancel(ctx, 2*time.Second, true, func(ctx context.Context) (bool, error) {
			err := hub.Get(ctx, types.NamespacedName{Name: namespace.Name}, &corev1.Namespace{})
			if k8serr.IsNotFound(err) {
				return true, nil
			}
			return false, err
		})
		if err != nil {
			t.Errorf("temporary Grafana namespace %s did not finish deleting: %v", namespace.Name, err)
		}
	})
	password := uuid.NewString() + uuid.NewString()
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "grafana-admin", Namespace: namespace.Name},
		Data: map[string][]byte{"password": []byte(password)}}
	require.NoError(t, hub.Create(t.Context(), secret))
	labels := map[string]string{"app": "ai-grid-oats-grafana"}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "grafana", Namespace: namespace.Name, Labels: labels}, Spec: corev1.PodSpec{
		AutomountServiceAccountToken: new(false),
		SecurityContext:              &corev1.PodSecurityContext{RunAsNonRoot: new(true), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
		Containers: []corev1.Container{{Name: "grafana", Image: cfg.Grafana.Image,
			Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 3000}},
			SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: new(false),
				Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
			Env: []corev1.EnvVar{
				{Name: "GF_SECURITY_ADMIN_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: secret.Name}, Key: "password"}}},
				{Name: "GF_AUTH_ANONYMOUS_ENABLED", Value: "false"}, {Name: "GF_USERS_ALLOW_SIGN_UP", Value: "false"},
				{Name: "GF_ANALYTICS_REPORTING_ENABLED", Value: "false"}, {Name: "GF_ANALYTICS_CHECK_FOR_UPDATES", Value: "false"},
			},
			VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/var/lib/grafana"}, {Name: "tmp", MountPath: "/tmp"}},
			ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
				Path: "/api/health", Port: intstr.FromString("http")}}, PeriodSeconds: 5},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("256Mi")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("512Mi")},
			},
		}},
		Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
	}}
	require.NoError(t, hub.Create(t.Context(), pod))
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "grafana", Namespace: namespace.Name}, Spec: corev1.ServiceSpec{
		Selector: labels, Ports: []corev1.ServicePort{{Name: "http", Port: 3000, TargetPort: intstr.FromString("http")}},
	}}
	require.NoError(t, hub.Create(t.Context(), service))
	route := aiGridObject("route.openshift.io", "v1", "Route")
	route.SetName("grafana")
	route.SetNamespace(namespace.Name)
	route.Object["spec"] = map[string]any{
		"to": map[string]any{"kind": "Service", "name": service.Name}, "port": map[string]any{"targetPort": "http"},
		"tls": map[string]any{"termination": "edge", "insecureEdgeTerminationPolicy": "Redirect"},
	}
	require.NoError(t, hub.Create(t.Context(), route))
	httpClient, err := aigrid.HTTPClient(cfg.Grafana.RouteCAFile)
	require.NoError(t, err)
	grafana := aigrid.Grafana{Client: httpClient, Password: password}
	err = wait.PollUntilContextTimeout(t.Context(), cfg.PollInterval, cfg.Timeout, true, func(ctx context.Context) (bool, error) {
		if err := hub.Get(ctx, client.ObjectKeyFromObject(route), route); err != nil {
			return false, err
		}
		ingresses, _, _ := unstructured.NestedSlice(route.Object, "status", "ingress")
		for _, raw := range ingresses {
			ingress, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			conditions, _, _ := unstructured.NestedSlice(ingress, "conditions")
			for _, rawCondition := range conditions {
				condition, ok := rawCondition.(map[string]any)
				if ok && condition["type"] == "Admitted" && condition["status"] == "True" {
					host, _ := ingress["host"].(string)
					if host != "" {
						grafana.URL = "https://" + strings.TrimSpace(host)
					}
				}
			}
		}
		if grafana.URL == "" {
			return false, nil
		}
		var health struct {
			Database string `json:"database"`
		}
		return grafana.Request(ctx, http.MethodGet, "/api/health", nil, &health) == nil && health.Database == "ok", nil
	})
	require.NoError(t, err, "temporary Grafana ready via admitted HTTPS Route; inspect pod scheduling/image/SCC and route CA")
	return grafana
}
