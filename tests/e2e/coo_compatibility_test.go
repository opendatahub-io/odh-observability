package e2e_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcfg "sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega
)

const (
	persesServicePort                 = 8080
	prometheusNamespaceProxyService   = "data-science-prometheus-namespace-proxy"
	prometheusNamespaceProxyPort      = 8443
	prometheusDashboardRouteName      = "data-science-prometheus-route"
	cooCompatibilityPromQLQuery       = "up"
	maxCompatibilityHTTPResponseBytes = 1 << 20 // 1 MiB
	compatibilityHTTPRequestTimeout   = 2 * time.Minute
)

// rhoai-operator-dev is the olminstall dev-catalog subscription name (package rhods-operator).
var rhoaiOperatorSubscriptionNames = []string{"rhods-operator", "rhoai-operator-dev", "odh-observability-operator"}

var (
	clusterVersionGVK = schema.GroupVersionKind{Group: "config.openshift.io", Version: "v1", Kind: "ClusterVersion"}
	uiPluginGVK       = schema.GroupVersionKind{Group: "observability.openshift.io", Version: "v1alpha1", Kind: "UIPlugin"}
)

// cooCompatibilityReport captures cluster and operator versions for release-gate diagnostics.
type cooCompatibilityReport struct {
	OpenShiftVersion        string            `json:"openShiftVersion,omitempty"`
	ClusterObservabilityCSV string            `json:"clusterObservabilityCSV,omitempty"`
	ClusterObservabilityVer string            `json:"clusterObservabilityVersion,omitempty"`
	OdhObservabilityCSV     string            `json:"odhObservabilityCSV,omitempty"`
	OdhObservabilityVer     string            `json:"odhObservabilityVersion,omitempty"`
	PersesImage             string            `json:"persesImage,omitempty"`
	PersesContainerArgs     []string          `json:"persesContainerArgs,omitempty"`
	MonitoringConditions    map[string]string `json:"monitoringConditions,omitempty"`
	DashboardProbe          string            `json:"dashboardProbe,omitempty"`
	PersesProbe             string            `json:"persesProbe,omitempty"`
	PersesProjectsProbe     string            `json:"persesProjectsProbe,omitempty"`
	PrometheusQueryProbe    string            `json:"prometheusQueryProbe,omitempty"`
	PrometheusRouteHost     string            `json:"prometheusRouteHost,omitempty"`
	PersesLogDiagnostic     string            `json:"persesLogDiagnostic,omitempty"`
	CompatibilityNotes      []string          `json:"compatibilityNotes,omitempty"`
	FailedAssertion         string            `json:"failedAssertion,omitempty"`
}

func failCompatibilityGate(t *testing.T, report *cooCompatibilityReport, assertion string, err error) {
	t.Helper()
	if err != nil {
		report.FailedAssertion = fmt.Sprintf("%s: %v", assertion, err)
		t.Fatalf("%s: %v", assertion, err)
	}
	report.FailedAssertion = assertion
	t.Fatal(assertion)
}

func cooVersionCompatibilitySuite(t *testing.T) {
	t.Helper()

	tc, err := NewTestContext(t)
	require.NoError(t, err)

	mctx := MonitoringTestCtx{
		TestContext:             tc,
		expectedDefaultReplicas: detectExpectedReplicas(t, tc),
	}
	report := &cooCompatibilityReport{}

	// defer (not t.Cleanup) so diagnostics are captured before registerMonitoringRestore's
	// t.Cleanup runs in LIFO order and reverts Monitoring/DSCI.
	defer func() {
		if t.Failed() {
			mctx.enrichCompatibilityReport(t, report)
			payload, err := json.MarshalIndent(report, "", "  ")
			if err != nil {
				t.Logf("failed to marshal compatibility report: %v", err)
				return
			}
			t.Logf("COO compatibility diagnostics:\n%s", string(payload))
			if path := testOpts.compatibilityReport; path != "" {
				resolved, err := resolveCompatibilityReportPath(path)
				if err != nil {
					t.Logf("failed to resolve compatibility report path: %v", err)
					return
				}
				if writeErr := writeCompatibilityReportFile(resolved, payload); writeErr != nil {
					t.Logf("failed to write compatibility report to %s: %v", resolved, writeErr)
				}
			}
		}
	}()

	mctx.ensurePrerequisites(t)
	mctx.setupMetrics(t)

	t.Run("COO version matches release gate", func(t *testing.T) {
		mctx.validateCOOVersionInstalled(t, report)
	})
	t.Run("Monitoring and Perses reconcile", func(t *testing.T) {
		mctx.validateMonitoringPersesReady(t, report)
	})
	t.Run("Perses operand health", func(t *testing.T) {
		mctx.validatePersesOperandHealth(t, report)
	})
	t.Run("Dashboard proxy APIs", func(t *testing.T) {
		mctx.validateDashboardProxyAPIs(t, report)
	})
}

func (tc *MonitoringTestCtx) validateCOOVersionInstalled(t *testing.T, report *cooCompatibilityReport) {
	t.Helper()
	tc = tc.WithT(t)

	csvName, version, err := tc.clusterObservabilityCSV()
	report.ClusterObservabilityCSV = csvName
	report.ClusterObservabilityVer = version
	if err != nil {
		failCompatibilityGate(t, report, "failed to read Cluster Observability Operator CSV", err)
	}

	if !releaseGateCOOCSVMatches(csvName) {
		report.FailedAssertion = fmt.Sprintf("COO CSV %q does not match required prefix %q", csvName, releaseGateContract.COOCSVPrefix)
		t.Fatalf("%s", report.FailedAssertion)
	}

	openshiftVersion, err := tc.openShiftVersion()
	if err != nil {
		failCompatibilityGate(t, report, "failed to read OpenShift cluster version", err)
	}
	report.OpenShiftVersion = openshiftVersion

	odhCSV, odhVersion, err := tc.odhObservabilityOperatorCSV()
	report.OdhObservabilityCSV = odhCSV
	report.OdhObservabilityVer = odhVersion
	if err != nil {
		failCompatibilityGate(t, report, "failed to read installed RHOAI/odh-observability operator CSV", err)
	}

	if !releaseGateRHOAICSVMatches(odhCSV) {
		report.FailedAssertion = fmt.Sprintf("RHOAI operator CSV %q does not match required prefix %q", odhCSV, releaseGateContract.RHOAICSVPrefix)
		t.Fatalf("%s", report.FailedAssertion)
	}

	t.Logf("COO compatibility baseline: OpenShift=%s COO=%s (%s) RHOAI=%s (%s)",
		report.OpenShiftVersion, csvName, version, odhCSV, odhVersion)
}

func (tc *MonitoringTestCtx) validateMonitoringPersesReady(t *testing.T, report *cooCompatibilityReport) {
	t.Helper()
	tc = tc.WithT(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.phase == "%s"`, common.PhaseReady),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionPersesAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionMonitoringStackAvailable, metav1.ConditionTrue),
		)),
		WithEventuallyTimeout(15*time.Minute),
		WithCustomErrorMsg("Monitoring should be Ready with MonitoringStack and Perses available for COO compatibility gate"),
	)

	report.MonitoringConditions = tc.monitoringConditionMap()
}

func (tc *MonitoringTestCtx) validatePersesOperandHealth(t *testing.T, report *cooCompatibilityReport) {
	t.Helper()
	tc = tc.WithT(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.StatefulSet, types.NamespacedName{Name: PersesName, Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.status.readyReplicas >= 1`),
			jq.Match(`.spec.replicas == .status.readyReplicas`),
		)),
		WithCustomErrorMsg("Perses StatefulSet should be ready"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Pod, types.NamespacedName{Name: PersesName + "-0", Namespace: tc.MonitoringNamespace}),
		WithCondition(And(
			jq.Match(`.status.phase == "Running"`),
			jq.Match(`.status.conditions[] | select(.type == "Ready") | .status == "True"`),
		)),
		WithCustomErrorMsg("Perses pod should be running"),
	)

	sts := tc.FetchResource(WithMinimalObject(gvk.StatefulSet, types.NamespacedName{Name: PersesName, Namespace: tc.MonitoringNamespace}))
	containers, _, _ := unstructured.NestedSlice(sts.Object, "spec", "template", "spec", "containers")
	persesContainer, image, args, found := persesContainerFromPodSpec(containers)
	if !found {
		report.FailedAssertion = "Perses StatefulSet has no Perses container to validate"
		t.Fatalf("%s", report.FailedAssertion)
	}
	if image != "" {
		report.PersesImage = image
	}
	if err := persesImageMatchesReleaseGate(image); err != nil {
		report.FailedAssertion = err.Error()
		t.Fatalf("%s", report.FailedAssertion)
	}
	report.PersesContainerArgs = sanitizeContainerArgs(args)
	for _, arg := range args {
		if isUnsupportedPersesTLSArg(arg) {
			report.FailedAssertion = fmt.Sprintf("Perses container has unsupported TLS argument %q (image %s)", arg, image)
			t.Fatalf("%s", report.FailedAssertion)
		}
	}

	logSnippet, err := tc.podLogTail(
		types.NamespacedName{Name: PersesName + "-0", Namespace: tc.MonitoringNamespace},
		persesContainer,
		80,
	)
	if err != nil {
		report.PersesLogDiagnostic = err.Error()
		report.FailedAssertion = "failed to read Perses pod logs for startup validation"
		t.Fatalf("%s: %v", report.FailedAssertion, err)
	} else if containsPersesStartupFailure(logSnippet) {
		report.FailedAssertion = "Perses pod logs indicate startup failure (unknown flag or crash)"
		t.Logf("Perses pod log excerpt: %s", truncateForLog(sanitizeLogExcerpt(logSnippet), 512))
		t.Fatalf("%s", report.FailedAssertion)
	}
}

func (tc *MonitoringTestCtx) validateDashboardProxyAPIs(t *testing.T, report *cooCompatibilityReport) {
	t.Helper()
	tc = tc.WithT(t)

	tc.validatePrometheusNamespaceProxyResourcesCommon(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Route, types.NamespacedName{Name: prometheusDashboardRouteName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`((.status.ingress[0].host // .spec.host) // "") != ""`)),
		WithCustomErrorMsg("Prometheus dashboard Route should expose a user-visible host"),
	)
	route := tc.FetchResource(WithMinimalObject(gvk.Route, types.NamespacedName{Name: prometheusDashboardRouteName, Namespace: tc.MonitoringNamespace}))
	report.PrometheusRouteHost = prometheusDashboardRouteHost(route)

	persesBody, persesStatus, err := tc.kubernetesServiceGET(tc.MonitoringNamespace, PersesName, persesServicePort, "/api/v1/health", false)
	if err != nil {
		failCompatibilityGate(t, report, "Perses health request failed", err)
	}
	report.PersesProbe = fmt.Sprintf("HTTP %d", persesStatus)
	if persesStatus < 200 || persesStatus >= 300 {
		report.FailedAssertion = fmt.Sprintf("Perses health returned HTTP %d", persesStatus)
		t.Fatalf("%s", report.FailedAssertion)
	}
	if looksLikeHTMLResponse(persesBody) {
		report.FailedAssertion = "Perses health returned HTML instead of an API payload"
		t.Fatalf("%s", report.FailedAssertion)
	}
	if err := validatePersesHealthResponse(persesBody); err != nil {
		report.FailedAssertion = "Perses health did not return a valid Perses API response"
		t.Fatalf("%s: %v", report.FailedAssertion, err)
	}

	projectsBody, projectsStatus, err := tc.kubernetesServiceGET(tc.MonitoringNamespace, PersesName, persesServicePort, "/api/v1/projects", false)
	if err != nil {
		failCompatibilityGate(t, report, "Perses projects API request failed", err)
	}
	report.PersesProjectsProbe = fmt.Sprintf("HTTP %d /api/v1/projects", projectsStatus)
	if projectsStatus < 200 || projectsStatus >= 300 {
		report.FailedAssertion = fmt.Sprintf("Perses projects API returned HTTP %d", projectsStatus)
		t.Fatalf("%s", report.FailedAssertion)
	}
	if err := validatePersesProjectsResponse(projectsBody); err != nil {
		report.FailedAssertion = "Perses projects API did not return a valid Perses API response"
		t.Fatalf("%s: %v", report.FailedAssertion, err)
	}

	// Namespace proxy kube-rbac-proxy authorizes metrics.k8s.io/pods; requests must include namespace=.
	// Use the dashboard Route (not the API server service proxy): HTTPS backends do not receive the
	// caller bearer token through /services/proxy, so in-cluster SAR checks would always fail.
	promQueryPath := prometheusNamespaceProxyQueryPath(cooCompatibilityPromQLQuery, tc.MonitoringNamespace)
	queryBody, queryStatus, err := tc.prometheusDashboardRouteGET(t, report.PrometheusRouteHost, promQueryPath)
	if err != nil {
		failCompatibilityGate(t, report, "Prometheus dashboard proxy PromQL request failed", err)
	}
	report.DashboardProbe = fmt.Sprintf(
		"route %s %s HTTP %d",
		report.PrometheusRouteHost, promQueryPath, queryStatus,
	)
	report.PrometheusQueryProbe = report.DashboardProbe
	if queryStatus < 200 || queryStatus >= 300 {
		report.FailedAssertion = fmt.Sprintf("Prometheus PromQL query returned HTTP %d", queryStatus)
		t.Fatalf("%s", report.FailedAssertion)
	}
	if looksLikeHTMLResponse(queryBody) {
		report.FailedAssertion = "Prometheus PromQL query returned HTML instead of an API payload"
		t.Fatalf("%s", report.FailedAssertion)
	}
	if err := validatePrometheusInstantQueryResponse(queryBody); err != nil {
		report.FailedAssertion = "Prometheus dashboard proxy did not return a valid PromQL API response"
		t.Fatalf("%s: %v", report.FailedAssertion, err)
	}

	uiPluginCRDAvailable, err := tc.optionalCRDAvailable(uiPluginGVK)
	if err != nil {
		failCompatibilityGate(t, report, "failed to determine whether Dashboards UIPlugin CRD is available", err)
	}
	if !uiPluginCRDAvailable {
		report.CompatibilityNotes = append(report.CompatibilityNotes, "Dashboards UIPlugin CRD is not installed on this cluster")
		return
	}
	_, err = tc.fetchResource(tc.t, uiPluginGVK, types.NamespacedName{Name: "dashboards"})
	if err != nil {
		if k8serr.IsNotFound(err) {
			report.CompatibilityNotes = append(report.CompatibilityNotes,
				"Dashboards UIPlugin CRD is installed but dashboards UIPlugin instance is not present on this cluster")
			return
		}
		failCompatibilityGate(t, report, "failed to read Dashboards UIPlugin", err)
	}
	tc.EnsureResourceExists(
		WithMinimalObject(uiPluginGVK, types.NamespacedName{Name: "dashboards"}),
		WithCondition(jq.Match(`[.status.conditions[]? | select(.type == "Ready" or .type == "Available") | .status] | any(. == "True")`)),
		WithEventuallyTimeout(10*time.Minute),
		WithCustomErrorMsg("Dashboards UIPlugin should report Ready when installed"),
	)
}

func (tc *MonitoringTestCtx) enrichCompatibilityReport(t *testing.T, report *cooCompatibilityReport) {
	t.Helper()

	if report.ClusterObservabilityCSV == "" {
		if csvName, version, err := tc.clusterObservabilityCSV(); err == nil {
			report.ClusterObservabilityCSV = csvName
			report.ClusterObservabilityVer = version
		} else {
			report.CompatibilityNotes = append(report.CompatibilityNotes, "COO CSV: "+err.Error())
		}
	}
	if report.OpenShiftVersion == "" {
		if version, err := tc.openShiftVersion(); err == nil {
			report.OpenShiftVersion = version
		} else {
			report.CompatibilityNotes = append(report.CompatibilityNotes, "OpenShift version: "+err.Error())
		}
	}
	if report.OdhObservabilityCSV == "" {
		if csvName, version, err := tc.odhObservabilityOperatorCSV(); err == nil {
			report.OdhObservabilityCSV = csvName
			report.OdhObservabilityVer = version
		} else {
			report.CompatibilityNotes = append(report.CompatibilityNotes, "RHOAI operator CSV: "+err.Error())
		}
	}
	if report.MonitoringConditions == nil {
		report.MonitoringConditions = tc.monitoringConditionMap()
	}
	tc.enrichPersesOperandSnapshot(t, report)
	tc.enrichDashboardProbeSnapshot(t, report)
}

func (tc *MonitoringTestCtx) enrichPersesOperandSnapshot(t *testing.T, report *cooCompatibilityReport) {
	t.Helper()
	if tc.MonitoringNamespace == "" {
		return
	}
	var persesContainer string
	if report.PersesImage == "" || len(report.PersesContainerArgs) == 0 || report.PersesLogDiagnostic == "" {
		sts, err := tc.fetchResource(t, gvk.StatefulSet, types.NamespacedName{Name: PersesName, Namespace: tc.MonitoringNamespace})
		if err != nil {
			report.CompatibilityNotes = append(report.CompatibilityNotes, "Perses StatefulSet: "+err.Error())
		} else {
			containers, _, _ := unstructured.NestedSlice(sts.Object, "spec", "template", "spec", "containers")
			name, image, args, found := persesContainerFromPodSpec(containers)
			if found {
				persesContainer = name
				if report.PersesImage == "" && image != "" {
					report.PersesImage = image
				}
				if len(report.PersesContainerArgs) == 0 {
					report.PersesContainerArgs = sanitizeContainerArgs(args)
				}
			}
		}
	}
	if report.PersesLogDiagnostic == "" && persesContainer != "" {
		logSnippet, err := tc.podLogTail(
			types.NamespacedName{Name: PersesName + "-0", Namespace: tc.MonitoringNamespace},
			persesContainer,
			80,
		)
		if err != nil {
			report.PersesLogDiagnostic = err.Error()
		} else if containsPersesStartupFailure(logSnippet) {
			report.PersesLogDiagnostic = "startup failure markers present in pod log excerpt"
		}
	}
}

func (tc *MonitoringTestCtx) enrichDashboardProbeSnapshot(t *testing.T, report *cooCompatibilityReport) {
	t.Helper()
	if report.PersesProbe == "" {
		_, status, err := tc.kubernetesServiceGET(tc.MonitoringNamespace, PersesName, persesServicePort, "/api/v1/health", false)
		if err != nil {
			report.CompatibilityNotes = append(report.CompatibilityNotes, "Perses probe: "+err.Error())
		} else {
			report.PersesProbe = fmt.Sprintf("HTTP %d", status)
		}
	}
	if report.DashboardProbe == "" && report.PrometheusRouteHost != "" {
		path := prometheusNamespaceProxyQueryPath(cooCompatibilityPromQLQuery, tc.MonitoringNamespace)
		_, status, err := tc.prometheusDashboardRouteGET(t, report.PrometheusRouteHost, path)
		if err != nil {
			report.CompatibilityNotes = append(report.CompatibilityNotes, "Prometheus probe: "+err.Error())
		} else {
			report.DashboardProbe = fmt.Sprintf("route %s %s HTTP %d", report.PrometheusRouteHost, path, status)
		}
	}
}

func (tc *MonitoringTestCtx) clusterObservabilityCSV() (string, string, error) {
	sub, err := tc.fetchResource(tc.t, gvk.Subscription, types.NamespacedName{
		Name:      observabilityOpName,
		Namespace: observabilityOpNamespace,
	})
	if err != nil {
		return "", "", err
	}
	csvName, _, _ := unstructured.NestedString(sub.Object, "status", "installedCSV")
	if csvName == "" {
		return "", "", fmt.Errorf("subscription %s/%s has no installed CSV", observabilityOpNamespace, observabilityOpName)
	}

	csv, err := tc.fetchResource(tc.t, gvk.ClusterServiceVersion, types.NamespacedName{
		Name:      csvName,
		Namespace: observabilityOpNamespace,
	})
	if err != nil {
		return csvName, "", err
	}
	phase, _, _ := unstructured.NestedString(csv.Object, "status", "phase")
	if phase != "Succeeded" {
		return csvName, "", fmt.Errorf("COO CSV %s phase is %q, expected Succeeded", csvName, phase)
	}
	version, _, _ := unstructured.NestedString(csv.Object, "spec", "version")
	if version == "" {
		return csvName, "", fmt.Errorf("COO CSV %s has empty spec.version", csvName)
	}
	return csvName, version, nil
}

type rhoaiOperatorCSV struct {
	subscriptionName string
	csvName          string
	version          string
}

func (tc *MonitoringTestCtx) odhObservabilityOperatorCSV() (string, string, error) {
	subs := &unstructured.UnstructuredList{}
	subs.SetGroupVersionKind(gvk.Subscription)
	if err := tc.Client().List(tc.Context(), subs); err != nil {
		return "", "", err
	}

	candidates := make([]rhoaiOperatorCSV, 0, len(rhoaiOperatorSubscriptionNames))
	for _, subscriptionName := range rhoaiOperatorSubscriptionNames {
		for _, sub := range subs.Items {
			if sub.GetName() != subscriptionName {
				continue
			}
			csvName, _, _ := unstructured.NestedString(sub.Object, "status", "installedCSV")
			if csvName == "" {
				return "", "", fmt.Errorf("subscription %s has no installed CSV", sub.GetName())
			}
			csv, err := tc.fetchResource(tc.t, gvk.ClusterServiceVersion, types.NamespacedName{
				Name:      csvName,
				Namespace: sub.GetNamespace(),
			})
			if err != nil {
				return csvName, "", err
			}
			phase, _, _ := unstructured.NestedString(csv.Object, "status", "phase")
			if phase != "Succeeded" {
				return csvName, "", fmt.Errorf("RHOAI operator CSV %s phase is %q, expected Succeeded", csvName, phase)
			}
			version, _, _ := unstructured.NestedString(csv.Object, "spec", "version")
			if version == "" {
				return csvName, "", fmt.Errorf("RHOAI operator CSV %s has empty spec.version", csvName)
			}
			candidates = append(candidates, rhoaiOperatorCSV{
				subscriptionName: subscriptionName,
				csvName:          csvName,
				version:          version,
			})
		}
	}
	if len(candidates) == 0 {
		return "", "", errors.New("RHOAI operator subscription not found (expected OLM subscription for rhods-operator, e.g. rhods-operator or rhoai-operator-dev)")
	}
	if len(candidates) == 1 {
		return candidates[0].csvName, candidates[0].version, nil
	}

	matching := slices.DeleteFunc(slices.Clone(candidates), func(candidate rhoaiOperatorCSV) bool {
		return !releaseGateRHOAICSVMatches(candidate.csvName)
	})
	switch len(matching) {
	case 1:
		return matching[0].csvName, matching[0].version, nil
	case 0:
		names := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			names = append(names, fmt.Sprintf("%s (%s)", candidate.subscriptionName, candidate.csvName))
		}
		return "", "", fmt.Errorf(
			"multiple RHOAI operator subscriptions are installed (%s) and none match CSV prefix %q",
			strings.Join(names, ", "), releaseGateContract.RHOAICSVPrefix,
		)
	default:
		names := make([]string, 0, len(matching))
		for _, candidate := range matching {
			names = append(names, fmt.Sprintf("%s (%s)", candidate.subscriptionName, candidate.csvName))
		}
		return "", "", fmt.Errorf(
			"ambiguous RHOAI operator subscriptions match CSV prefix %q: %s",
			releaseGateContract.RHOAICSVPrefix, strings.Join(names, ", "),
		)
	}
}

func (tc *MonitoringTestCtx) openShiftVersion() (string, error) {
	cv, err := tc.fetchResource(tc.t, clusterVersionGVK, types.NamespacedName{Name: "version"})
	if err != nil {
		return "", err
	}
	if err := validateClusterVersionReady(cv); err != nil {
		return "", err
	}
	return runningOpenShiftVersion(cv)
}

func validateClusterVersionReady(cv *unstructured.Unstructured) error {
	available, found := clusterVersionConditionStatus(cv, "Available")
	if !found || available != string(metav1.ConditionTrue) {
		return fmt.Errorf("ClusterVersion Available=%q", available)
	}
	progressing, found := clusterVersionConditionStatus(cv, "Progressing")
	if found && progressing == string(metav1.ConditionTrue) {
		return errors.New("ClusterVersion is still progressing")
	}
	degraded, found := clusterVersionConditionStatus(cv, "Degraded")
	if found && degraded == string(metav1.ConditionTrue) {
		return errors.New("ClusterVersion is degraded")
	}
	return nil
}

func clusterVersionConditionStatus(cv *unstructured.Unstructured, conditionType string) (string, bool) {
	conditions, _, _ := unstructured.NestedSlice(cv.Object, "status", "conditions")
	for _, raw := range conditions {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		typ, _, _ := unstructured.NestedString(entry, "type")
		if typ != conditionType {
			continue
		}
		status, _, _ := unstructured.NestedString(entry, "status")
		return status, true
	}
	return "", false
}

func runningOpenShiftVersion(cv *unstructured.Unstructured) (string, error) {
	history, _, _ := unstructured.NestedSlice(cv.Object, "status", "history")
	for _, raw := range history {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		state, _, _ := unstructured.NestedString(entry, "state")
		version, _, _ := unstructured.NestedString(entry, "version")
		if state == "Completed" && version != "" {
			return version, nil
		}
	}
	versions, _, _ := unstructured.NestedSlice(cv.Object, "status", "versions")
	for _, raw := range versions {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		state, _, _ := unstructured.NestedString(entry, "state")
		version, _, _ := unstructured.NestedString(entry, "version")
		if state == "Completed" && version != "" {
			return version, nil
		}
	}
	return "", errors.New("no completed OpenShift version found in ClusterVersion status")
}

type prometheusBuildInfoResponse struct {
	Status string `json:"status"`
	Data   struct {
		Version string `json:"version"`
	} `json:"data"`
}

func validatePrometheusBuildInfoResponse(body string) error {
	var response prometheusBuildInfoResponse
	if err := json.Unmarshal([]byte(body), &response); err != nil {
		return err
	}
	if response.Status != "success" {
		return fmt.Errorf("status is %q", response.Status)
	}
	if response.Data.Version == "" {
		return errors.New("data.version is empty")
	}
	return nil
}

type prometheusInstantQueryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
	} `json:"data"`
}

func validatePrometheusInstantQueryResponse(body string) error {
	var response prometheusInstantQueryResponse
	if err := json.Unmarshal([]byte(body), &response); err != nil {
		return err
	}
	if response.Status != "success" {
		return fmt.Errorf("status is %q", response.Status)
	}
	if response.Data.ResultType == "" {
		return errors.New("data.resultType is empty")
	}
	return nil
}

type persesHealthResponse struct {
	BuildTime string `json:"buildTime"`
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	Database  bool   `json:"database"`
}

func validatePersesHealthResponse(body string) error {
	if strings.TrimSpace(body) == "" {
		return errors.New("empty response body")
	}
	var response persesHealthResponse
	if err := json.Unmarshal([]byte(body), &response); err != nil {
		return err
	}
	if response.Version != "" {
		return nil
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		return err
	}
	if persesHealthVersionFromMap(raw) != "" {
		return nil
	}
	if db, ok := raw["database"].(bool); ok && db {
		return nil
	}
	return errors.New("version is empty")
}

func persesHealthVersionFromMap(raw map[string]any) string {
	if v, ok := raw["version"].(string); ok && v != "" {
		return v
	}
	meta, ok := raw["metadata"].(map[string]any)
	if !ok {
		return ""
	}
	if v, ok := meta["version"].(string); ok {
		return v
	}
	return ""
}

func validatePersesProjectsResponse(body string) error {
	if strings.TrimSpace(body) == "" {
		return errors.New("empty response body")
	}
	var decoded any
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		return err
	}
	switch decoded.(type) {
	case []any, map[string]any:
		return nil
	default:
		return errors.New("unexpected Perses projects payload type")
	}
}

func persesContainerFromPodSpec(containers []any) (string, string, []string, bool) {
	for _, raw := range containers {
		container, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		containerName, _, _ := unstructured.NestedString(container, "name")
		containerImage, _, _ := unstructured.NestedString(container, "image")
		if !strings.Contains(strings.ToLower(containerName), "perses") && !strings.Contains(strings.ToLower(containerImage), "perses") {
			continue
		}
		containerArgs, _, _ := unstructured.NestedStringSlice(container, "args")
		return containerName, containerImage, containerArgs, true
	}
	return "", "", nil, false
}

func (tc *MonitoringTestCtx) monitoringConditionMap() map[string]string {
	out := map[string]string{}
	monitoring, err := tc.fetchResource(tc.t, gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName})
	if err != nil {
		return out
	}
	conditions, _, _ := unstructured.NestedSlice(monitoring.Object, "status", "conditions")
	for _, raw := range conditions {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		typ, _, _ := unstructured.NestedString(entry, "type")
		status, _, _ := unstructured.NestedString(entry, "status")
		if typ != "" {
			out[typ] = status
		}
	}
	return out
}

func (tc *MonitoringTestCtx) optionalCRDAvailable(g schema.GroupVersionKind) (bool, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(g)
	err := tc.Client().List(tc.Context(), list, client.Limit(1))
	if err == nil {
		return true, nil
	}
	if meta.IsNoMatchError(err) {
		return false, nil
	}
	return false, err
}

func (tc *MonitoringTestCtx) kubernetesServiceGET(namespace, service string, port int, path string, tls bool) (string, int, error) {
	if err := validateKubernetesNamespace(namespace); err != nil {
		return "", 0, err
	}
	if err := validateKubernetesServiceName(service); err != nil {
		return "", 0, err
	}
	if err := validateKubernetesProxyPath(path); err != nil {
		return "", 0, err
	}

	cfg, err := ctrlcfg.GetConfig()
	if err != nil {
		return "", 0, err
	}
	httpClient, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return "", 0, err
	}
	requestURL, err := kubernetesServiceProxyURL(cfg.Host, namespace, service, port, path, tls)
	if err != nil {
		return "", 0, err
	}
	ctx, cancel := context.WithTimeout(tc.Context(), compatibilityHTTPRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return "", 0, err
	}
	if token := tc.AuthToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	body, err := readLimitedHTTPBody(resp.Body)
	if err != nil {
		return "", resp.StatusCode, err
	}
	return string(body), resp.StatusCode, nil
}

func (tc *MonitoringTestCtx) podLogTail(nn types.NamespacedName, container string, lines int64) (string, error) {
	if container == "" {
		return "", errors.New("container name is required")
	}
	cfg, err := ctrlcfg.GetConfig()
	if err != nil {
		return "", err
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(tc.Context(), compatibilityHTTPRequestTimeout)
	defer cancel()
	stream, err := clientset.CoreV1().Pods(nn.Namespace).GetLogs(nn.Name, &corev1.PodLogOptions{
		Container: container,
		TailLines: &lines,
	}).Stream(ctx)
	if err != nil {
		return "", err
	}
	defer stream.Close()
	raw, err := readLimitedHTTPBody(stream)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func isUnsupportedPersesTLSArg(arg string) bool {
	lower := strings.ToLower(strings.TrimSpace(arg))
	unsupported := []string{
		"--web.tls.cert",
		"--web.tls.key",
		"--web.tls.ca",
		"--tls.server",
		"--web.enable-tls",
	}
	for _, prefix := range unsupported {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	if lower == "--web.tls" || strings.HasPrefix(lower, "--web.tls=") {
		return true
	}
	return false
}

func containsPersesStartupFailure(logs string) bool {
	lower := strings.ToLower(logs)
	markers := []string{
		"unknown flag",
		"flag provided but not defined",
		"panic:",
		"crashloopbackoff",
	}
	for _, marker := range markers {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func looksLikeHTMLResponse(body string) bool {
	trimmed := strings.TrimSpace(body)
	return strings.HasPrefix(trimmed, "<") && strings.Contains(strings.ToLower(trimmed), "<html")
}

func truncateForLog(body string, limit int) string {
	if len(body) <= limit {
		return body
	}
	return body[:limit] + "..."
}

func prometheusNamespaceProxyQueryPath(promQL, namespace string) string {
	return "/api/v1/query?query=" + url.QueryEscape(promQL) + "&namespace=" + url.QueryEscape(namespace)
}

func prometheusDashboardRouteHost(route *unstructured.Unstructured) string {
	host, _, _ := unstructured.NestedString(route.Object, "status", "ingress", "0", "host")
	if host != "" {
		return host
	}
	host, _, _ = unstructured.NestedString(route.Object, "spec", "host")
	return host
}

func (tc *MonitoringTestCtx) prometheusDashboardRouteGET(t *testing.T, routeHost, path string) (string, int, error) {
	t.Helper()
	if routeHost == "" {
		return "", 0, errors.New("prometheus dashboard route host is empty")
	}
	if err := validateKubernetesProxyPath(path); err != nil {
		return "", 0, err
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	requestURL := "https://" + routeHost + path
	token, err := tc.kubernetesBearerToken()
	if err != nil {
		return "", 0, err
	}
	rootCAs := clusterIngressCAPool(t, tc)
	httpClient := &http.Client{
		Timeout: compatibilityHTTPRequestTimeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
				RootCAs:    rootCAs,
			},
		},
	}
	ctx, cancel := context.WithTimeout(tc.Context(), compatibilityHTTPRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	body, err := readLimitedHTTPBody(resp.Body)
	if err != nil {
		return "", resp.StatusCode, err
	}
	return string(body), resp.StatusCode, nil
}

func (tc *MonitoringTestCtx) kubernetesBearerToken() (string, error) {
	if token := strings.TrimSpace(tc.AuthToken()); token != "" {
		return token, nil
	}
	cfg, err := ctrlcfg.GetConfig()
	if err != nil {
		return "", err
	}
	if token := strings.TrimSpace(cfg.BearerToken); token != "" {
		return token, nil
	}
	out, err := exec.CommandContext(tc.Context(), "oc", "whoami", "-t").Output()
	if err != nil {
		return "", fmt.Errorf("no Kubernetes bearer token in kubeconfig and oc whoami -t failed: %w", err)
	}
	if token := strings.TrimSpace(string(out)); token != "" {
		return token, nil
	}
	return "", errors.New("no Kubernetes bearer token in test context, kubeconfig, or oc whoami -t")
}

func cooCSVMatchesPrefix(csvName, prefix string) bool {
	if !strings.HasPrefix(csvName, prefix) {
		return false
	}
	remainder := csvName[len(prefix):]
	return remainder == "" || remainder[0] == '.'
}

var logRedactionReplacements = []struct {
	pattern     *regexp.Regexp
	replacement string
}{
	{regexp.MustCompile(`(?i)(bearer\s+)[a-z0-9._~+/=-]+`), "${1}<redacted>"},
	{regexp.MustCompile(`(?i)(authorization:\s*(?:basic|bearer)\s+)[^\s]+`), "${1}<redacted>"},
	{
		regexp.MustCompile(`(?i)(\b(?:api[_-]?key|apikey|client[_-]?secret|access[_-]?token|refresh[_-]?token|password|passwd|secret|credential)\b)(\s*[=:]\s*)[^\s&]+`),
		"${1}${2}<redacted>",
	},
	{regexp.MustCompile(`(?i)(cookie:\s*)[^\n]+`), "${1}<redacted>"},
	{
		regexp.MustCompile(`(?i)([?&](?:token|api[_-]?key|apikey|access[_-]?token|secret|password)=)[^&\s]+`),
		"${1}<redacted>",
	},
	{regexp.MustCompile(`//[^:@\s/?#]+:[^@\s/?#]+@`), "//<redacted>@"},
}

func sanitizeContainerArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		out = append(out, sanitizeContainerArg(arg))
		if containerArgConsumesNextValue(arg) && i+1 < len(args) {
			out = append(out, "<redacted>")
			i++
		}
	}
	return out
}

func containerArgConsumesNextValue(arg string) bool {
	if strings.Contains(arg, "=") {
		return false
	}
	lower := strings.ToLower(strings.TrimSpace(arg))
	return slices.Contains([]string{
		"--token", "--secret", "--password", "--client-secret", "--cookie-secret",
		"--client-id", "--api-key",
	}, lower)
}

func sanitizeContainerArg(arg string) string {
	if containerArgConsumesNextValue(arg) {
		return arg
	}
	lower := strings.ToLower(arg)
	for _, marker := range []string{"token", "password", "secret", "key", "credential"} {
		if strings.Contains(lower, marker) {
			if idx := strings.Index(arg, "="); idx >= 0 {
				return arg[:idx+1] + "<redacted>"
			}
			return "<redacted>"
		}
	}
	if idx := strings.Index(arg, "="); idx >= 0 {
		return arg[:idx+1] + "<value>"
	}
	return arg
}

func sanitizeLogExcerpt(logs string) string {
	out := logs
	for _, entry := range logRedactionReplacements {
		out = entry.pattern.ReplaceAllString(out, entry.replacement)
	}
	return out
}

func validateKubernetesNamespace(namespace string) error {
	if errs := validation.IsDNS1123Subdomain(namespace); len(errs) > 0 {
		return fmt.Errorf("invalid namespace %q: %s", namespace, strings.Join(errs, "; "))
	}
	return nil
}

func validateKubernetesServiceName(service string) error {
	if errs := validation.IsDNS1123Subdomain(service); len(errs) > 0 {
		return fmt.Errorf("invalid service name %q: %s", service, strings.Join(errs, "; "))
	}
	return nil
}

func validateKubernetesProxyPath(path string) error {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if strings.Contains(path, "..") {
		return fmt.Errorf("invalid proxy path %q: path traversal is not allowed", path)
	}
	return nil
}

func kubernetesServiceProxyURL(apiHost, namespace, service string, port int, path string, tls bool) (string, error) {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	resourcePath, rawQuery, _ := strings.Cut(path, "?")
	if err := validateKubernetesProxyPath(resourcePath); err != nil {
		return "", err
	}
	scheme := "http"
	if tls {
		scheme = "https"
	}
	serviceRef := fmt.Sprintf("%s:%s:%d", scheme, service, port)
	segments := strings.Split(strings.Trim(resourcePath, "/"), "/")
	for i, segment := range segments {
		segments[i] = url.PathEscape(segment)
	}
	proxyPath := fmt.Sprintf(
		"/api/v1/namespaces/%s/services/%s/proxy/%s",
		namespace,
		serviceRef,
		strings.Join(segments, "/"),
	)
	if rawQuery != "" {
		proxyPath += "?" + rawQuery
	}
	endpoint, err := url.Parse(apiHost)
	if err != nil {
		return "", fmt.Errorf("invalid Kubernetes API host: %w", err)
	}
	endpoint.Path = proxyPath
	return endpoint.String(), nil
}

func readLimitedHTTPBody(r io.Reader) ([]byte, error) {
	limited := io.LimitReader(r, maxCompatibilityHTTPResponseBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(body) > maxCompatibilityHTTPResponseBytes {
		return nil, fmt.Errorf("response body exceeds %d bytes", maxCompatibilityHTTPResponseBytes)
	}
	return body, nil
}
