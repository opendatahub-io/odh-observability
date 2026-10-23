package e2e_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega
)

// olmUpgradeSurvivabilityReport captures baseline and post-upgrade evidence for upgrade diagnostics.
type olmUpgradeSurvivabilityReport struct {
	OpenShiftVersion      string                      `json:"openShiftVersion,omitempty"`
	OperatorsRequested    []string                    `json:"operatorsRequested,omitempty"`
	Baseline              *olmUpgradeHealthSnapshot   `json:"baseline,omitempty"`
	PostUpgrade           *olmUpgradeHealthSnapshot   `json:"postUpgrade,omitempty"`
	OperatorTransitions   []operatorUpgradeTransition `json:"operatorTransitions,omitempty"`
	FailedAssertion       string                      `json:"failedAssertion,omitempty"`
	FirstLifecycleFailure string                      `json:"firstLifecycleFailure,omitempty"`
}

type olmUpgradeHealthSnapshot struct {
	ClusterObservabilityCSV string                `json:"clusterObservabilityCSV,omitempty"`
	ClusterObservabilityVer string                `json:"clusterObservabilityVersion,omitempty"`
	TempoOperatorCSV        string                `json:"tempoOperatorCSV,omitempty"`
	TempoOperatorVer        string                `json:"tempoOperatorVersion,omitempty"`
	MonitoringConditions    map[string]string     `json:"monitoringConditions,omitempty"`
	OLM                     []operatorOLMSnapshot `json:"olm,omitempty"`
}

type operatorUpgradeTransition struct {
	Operator      string `json:"operator,omitempty"`
	TargetChannel string `json:"targetChannel,omitempty"`
	SourceCSV     string `json:"sourceCSV,omitempty"`
	TargetCSV     string `json:"targetCSV,omitempty"`
	Skipped       bool   `json:"skipped,omitempty"`
	SkipReason    string `json:"skipReason,omitempty"`
}

func olmUpgradeSurvivabilitySuite(t *testing.T) {
	t.Helper()
	if !testOpts.upgradeEnabled {
		t.Skip("OLM upgrade survivability is opt-in; pass -upgrade-enabled=true or E2E_TEST_UPGRADE_ENABLED=true")
	}

	operators, err := parseUpgradeOperators(testOpts.upgradeOperators)
	if err != nil {
		t.Fatalf("invalid upgrade-operators: %v", err)
	}

	tc, err := NewTestContext(t)
	if err != nil {
		t.Fatalf("failed to create test context: %v", err)
	}

	mctx := MonitoringTestCtx{
		TestContext:             tc,
		expectedDefaultReplicas: detectExpectedReplicas(t, tc),
	}
	report := &olmUpgradeSurvivabilityReport{OperatorsRequested: operators}

	defer func() {
		if t.Failed() {
			mctx.writeUpgradeSurvivabilityReport(t, report)
		}
	}()

	mctx.ensurePrerequisites(t)
	mctx.setupMetrics(t)

	t.Run("Record pre-upgrade baseline", func(t *testing.T) {
		report.Baseline = mctx.captureUpgradeHealthSnapshot(t, report)
		mctx.assertOLMSnapshotsHealthy(t, report, report.Baseline.OLM)
	})
	if t.Failed() {
		return
	}

	t.Run("Exercise OLM upgrades", func(t *testing.T) {
		mctx.runConfiguredOperatorUpgrades(t, report, operators)
		if !upgradeTransitionsIncludePerformedUpgrade(report.OperatorTransitions) {
			mctx.failUpgradeAssertion(t, report, "no operator upgrade was performed; target channels must differ from the current subscription channel")
		}
	})
	if t.Failed() {
		return
	}

	t.Run("Post-upgrade monitoring health contract", func(t *testing.T) {
		report.PostUpgrade = mctx.captureUpgradeHealthSnapshot(t, report)
		mctx.assertOLMSnapshotsHealthy(t, report, report.PostUpgrade.OLM)
		mctx.assertMonitoringUpgradeHealthContract(t, report)
	})

	if !t.Failed() {
		mctx.writeUpgradeSurvivabilityReport(t, report)
	}
}

func (tc *MonitoringTestCtx) failUpgradeAssertion(t *testing.T, report *olmUpgradeSurvivabilityReport, assertion string) {
	t.Helper()
	report.FailedAssertion = assertion
	t.Fatal(assertion)
}

func (tc *MonitoringTestCtx) captureUpgradeHealthSnapshot(t *testing.T, report *olmUpgradeSurvivabilityReport) *olmUpgradeHealthSnapshot {
	t.Helper()
	tc = tc.WithT(t)

	snap := &olmUpgradeHealthSnapshot{
		MonitoringConditions: tc.monitoringConditionMap(),
		OLM:                  tc.collectUpgradeOperatorOLMSnapshots(t, report),
	}

	if csvName, version, err := tc.clusterObservabilityCSV(); err != nil {
		tc.failUpgradeAssertion(t, report, fmt.Sprintf("failed to read Cluster Observability Operator CSV: %v", err))
	} else {
		snap.ClusterObservabilityCSV = csvName
		snap.ClusterObservabilityVer = version
	}
	if csvName, version, err := tc.tempoOperatorCSV(); err != nil {
		tc.failUpgradeAssertion(t, report, fmt.Sprintf("failed to read Tempo Operator CSV: %v", err))
	} else {
		snap.TempoOperatorCSV = csvName
		snap.TempoOperatorVer = version
	}
	return snap
}

func (tc *MonitoringTestCtx) tempoOperatorCSV() (string, string, error) {
	sub, err := tc.fetchResource(tc.t, gvk.Subscription, types.NamespacedName{
		Name:      tempoOpName,
		Namespace: tempoOpNamespace,
	})
	if err != nil {
		return "", "", err
	}
	csvName := subscriptionInstalledCSV(sub)
	if csvName == "" {
		return "", "", fmt.Errorf("subscription %s/%s has no installed CSV", tempoOpNamespace, tempoOpName)
	}
	csv, err := tc.fetchResource(tc.t, gvk.ClusterServiceVersion, types.NamespacedName{
		Name:      csvName,
		Namespace: tempoOpNamespace,
	})
	if err != nil {
		return csvName, "", err
	}
	phase, _, _ := unstructured.NestedString(csv.Object, "status", "phase")
	if phase != "Succeeded" {
		return csvName, "", fmt.Errorf("Tempo Operator CSV %s phase is %q, expected Succeeded", csvName, phase)
	}
	version, _, _ := unstructured.NestedString(csv.Object, "spec", "version")
	if version == "" {
		return csvName, "", fmt.Errorf("Tempo Operator CSV %s has empty spec.version", csvName)
	}
	return csvName, version, nil
}

func (tc *MonitoringTestCtx) collectUpgradeOperatorOLMSnapshots(t *testing.T, report *olmUpgradeSurvivabilityReport) []operatorOLMSnapshot {
	t.Helper()
	operators, err := parseUpgradeOperators(testOpts.upgradeOperators)
	if err != nil {
		tc.failUpgradeAssertion(t, report, fmt.Sprintf("invalid upgrade-operators: %v", err))
	}
	snapshots := make([]operatorOLMSnapshot, 0, len(operators))
	for _, operator := range operators {
		ns, name, err := upgradeOperatorSubscription(operator)
		if err != nil {
			tc.failUpgradeAssertion(t, report, err.Error())
		}
		snap, err := tc.operatorOLMSnapshot(ns, name)
		if err != nil {
			tc.failUpgradeAssertion(t, report, fmt.Sprintf("failed to snapshot OLM state for %s/%s: %v", ns, name, err))
		}
		snapshots = append(snapshots, snap)
	}
	return snapshots
}

func (tc *MonitoringTestCtx) operatorOLMSnapshot(namespace, name string) (operatorOLMSnapshot, error) {
	sub, err := tc.fetchResource(tc.t, gvk.Subscription, types.NamespacedName{Name: name, Namespace: namespace})
	if err != nil {
		return operatorOLMSnapshot{}, err
	}
	snap := operatorOLMSnapshot{
		SubscriptionNamespace: namespace,
		SubscriptionName:      name,
	}
	snap.Channel, _, _ = unstructured.NestedString(sub.Object, "spec", "channel")
	snap.StartingCSV, _, _ = unstructured.NestedString(sub.Object, "spec", "startingCSV")
	snap.InstalledCSV = subscriptionInstalledCSV(sub)
	snap.CurrentCSV, _, _ = unstructured.NestedString(sub.Object, "status", "currentCSV")
	installPlanName, _, _ := unstructured.NestedString(sub.Object, "status", "installPlanRef", "name")
	snap.InstallPlanRef = installPlanName

	if snap.InstalledCSV == "" {
		return snap, nil
	}
	csv, err := tc.fetchResource(tc.t, gvk.ClusterServiceVersion, types.NamespacedName{
		Name:      snap.InstalledCSV,
		Namespace: namespace,
	})
	if err != nil {
		return snap, err
	}
	snap.CSVPhase, _, _ = unstructured.NestedString(csv.Object, "status", "phase")
	snap.CSVVersion, _, _ = unstructured.NestedString(csv.Object, "spec", "version")
	return snap, nil
}

func (tc *MonitoringTestCtx) assertOLMSnapshotsHealthy(t *testing.T, report *olmUpgradeSurvivabilityReport, snapshots []operatorOLMSnapshot) {
	t.Helper()
	if failure := firstOLMLifecycleFailure(snapshots); failure != "" {
		tc.failUpgradeAssertion(t, report, "OLM lifecycle failure: "+failure)
	}
}

func (tc *MonitoringTestCtx) runConfiguredOperatorUpgrades(t *testing.T, report *olmUpgradeSurvivabilityReport, operators []string) {
	t.Helper()
	tc = tc.WithT(t)

	for _, operator := range operators {
		transition := operatorUpgradeTransition{Operator: operator}
		targetChannel := upgradeTargetChannelForOperator(operator)
		if targetChannel == "" {
			transition.Skipped = true
			transition.SkipReason = "no target channel configured for operator"
			report.OperatorTransitions = append(report.OperatorTransitions, transition)
			t.Logf("skipping %s upgrade: %s", operator, transition.SkipReason)
			continue
		}
		transition.TargetChannel = targetChannel

		ns, name, err := upgradeOperatorSubscription(operator)
		if err != nil {
			tc.failUpgradeAssertion(t, report, err.Error())
		}
		before, err := tc.operatorOLMSnapshot(ns, name)
		if err != nil {
			tc.failUpgradeAssertion(t, report, fmt.Sprintf("failed to read pre-upgrade OLM state for %s: %v", operator, err))
		}
		transition.SourceCSV = before.InstalledCSV

		if stringsEqualFoldTrim(before.Channel, targetChannel) && before.InstalledCSV != "" {
			transition.TargetCSV = before.InstalledCSV
			transition.Skipped = true
			transition.SkipReason = "subscription already on target channel with installed CSV"
			report.OperatorTransitions = append(report.OperatorTransitions, transition)
			t.Logf("skipping %s upgrade: already on channel %s with CSV %s", operator, targetChannel, before.InstalledCSV)
			continue
		}

		tc.patchSubscriptionChannel(t, types.NamespacedName{Namespace: ns, Name: name}, targetChannel)
		tc.waitForOperatorUpgrade(t, ns, name, before.InstalledCSV)
		after, err := tc.operatorOLMSnapshot(ns, name)
		if err != nil {
			tc.failUpgradeAssertion(t, report, fmt.Sprintf("failed to read post-upgrade OLM state for %s: %v", operator, err))
		}
		transition.TargetCSV = after.InstalledCSV
		report.OperatorTransitions = append(report.OperatorTransitions, transition)

		tc.assertOLMSnapshotsHealthy(t, report, []operatorOLMSnapshot{after})
		tc.assertMonitoringOperandsWithoutBlockingFinalizers(t, report)
	}
}

func stringsEqualFoldTrim(a, b string) bool {
	return strings.TrimSpace(strings.ToLower(a)) == strings.TrimSpace(strings.ToLower(b))
}

func (tc *MonitoringTestCtx) patchSubscriptionChannel(t *testing.T, nn types.NamespacedName, channel string) {
	t.Helper()
	patch, err := json.Marshal(map[string]any{"spec": map[string]any{"channel": channel}})
	if err != nil {
		t.Fatalf("failed to build subscription patch: %v", err)
	}
	tc.g.Eventually(func() error {
		sub := &unstructured.Unstructured{}
		sub.SetGroupVersionKind(gvk.Subscription)
		if err := tc.client.Get(tc.ctx, nn, sub); err != nil {
			return err
		}
		return tc.client.Patch(tc.ctx, sub, client.RawPatch(types.MergePatchType, patch))
	}).WithTimeout(2*time.Minute).WithPolling(2*time.Second).Should(Succeed(),
		"failed to patch Subscription %s channel to %s", nn, channel)
	t.Logf("patched Subscription %s/%s to channel %s", nn.Namespace, nn.Name, channel)
}

func (tc *MonitoringTestCtx) waitForOperatorUpgrade(t *testing.T, namespace, name, previousCSV string) {
	t.Helper()
	timeout := tc.Timeouts.olmOperationTimeout
	tc.g.Eventually(func(g Gomega) {
		snap, err := tc.operatorOLMSnapshot(namespace, name)
		g.Expect(err).NotTo(HaveOccurred(), "failed to read subscription %s/%s during upgrade", namespace, name)
		g.Expect(snap.InstalledCSV).NotTo(BeEmpty(), "subscription %s/%s should have installed CSV", namespace, name)
		if previousCSV != "" {
			g.Expect(snap.InstalledCSV).NotTo(Equal(previousCSV),
				"subscription %s/%s should advance from CSV %s", namespace, name, previousCSV)
		}
		g.Expect(csvPhaseIndicatesStuckLifecycle(snap.CSVPhase)).To(BeFalse(),
			"CSV %s should not be stuck in phase %q", snap.InstalledCSV, snap.CSVPhase)
		g.Expect(snap.CSVPhase).To(Equal("Succeeded"), "CSV %s phase", snap.InstalledCSV)
	}).WithTimeout(timeout).WithPolling(15*time.Second).Should(Succeed(),
		"operator %s/%s did not converge after OLM upgrade within %s", namespace, name, timeout)
}

func (tc *MonitoringTestCtx) assertMonitoringOperandsWithoutBlockingFinalizers(t *testing.T, report *olmUpgradeSurvivabilityReport) {
	t.Helper()
	tc = tc.WithT(t)

	// COO operands can retain finalizers while stuck in Replacing/Deleting.
	resources := []struct {
		gvk       schema.GroupVersionKind
		name      string
		namespace string
	}{
		{gvk.ThanosQuerier, ThanosQuerierName, tc.MonitoringNamespace},
		{gvk.MonitoringStack, MonitoringStackName, tc.MonitoringNamespace},
		{gvk.StatefulSet, PersesName, tc.MonitoringNamespace},
	}
	cleanupTimeout := 10 * time.Minute
	for _, resource := range resources {
		nn := types.NamespacedName{Name: resource.name, Namespace: resource.namespace}
		tc.g.Eventually(func(g Gomega) {
			obj := tc.FetchResource(WithMinimalObject(resource.gvk, nn))
			if obj.GetDeletionTimestamp() == nil {
				return
			}
			g.Expect(obj.GetFinalizers()).To(BeEmpty(),
				"%s %s is still deleting with finalizers after OLM upgrade", resource.gvk.Kind, resource.name)
		}).WithTimeout(cleanupTimeout).WithPolling(15*time.Second).Should(Succeed(),
			"%s %s did not finish deletion cleanup within %s after OLM upgrade", resource.gvk.Kind, resource.name, cleanupTimeout)
	}
}

func (tc *MonitoringTestCtx) assertMonitoringUpgradeHealthContract(t *testing.T, report *olmUpgradeSurvivabilityReport) {
	t.Helper()
	tc = tc.WithT(t)

	compat := &cooCompatibilityReport{}
	defer func() {
		if compat.FailedAssertion != "" {
			report.FailedAssertion = compat.FailedAssertion
		}
	}()

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.phase == "%s"`, common.PhaseReady),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionPersesAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionMonitoringStackAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionThanosQuerierAvailable, metav1.ConditionTrue),
		)),
		WithEventuallyTimeout(20*time.Minute),
		WithCustomErrorMsg("Monitoring should be Ready with metrics stack, Thanos, and Perses available after OLM upgrade"),
	)

	tc.validatePersesOperandHealth(t, compat)
	tc.validateDashboardProxyAPIs(t, compat)
}

func (tc *MonitoringTestCtx) writeUpgradeSurvivabilityReport(t *testing.T, report *olmUpgradeSurvivabilityReport) {
	t.Helper()
	if report.OpenShiftVersion == "" {
		if version, err := tc.openShiftVersion(); err == nil {
			report.OpenShiftVersion = version
		}
	}
	if report.FirstLifecycleFailure == "" {
		if report.PostUpgrade != nil {
			report.FirstLifecycleFailure = firstOLMLifecycleFailure(report.PostUpgrade.OLM)
		} else if report.Baseline != nil {
			report.FirstLifecycleFailure = firstOLMLifecycleFailure(report.Baseline.OLM)
		}
	}
	payload, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Logf("failed to marshal upgrade survivability report: %v", err)
		return
	}
	t.Logf("OLM upgrade survivability report:\n%s", string(payload))
	if path := testOpts.upgradeReport; path != "" {
		resolved, err := resolveCompatibilityReportPath(path)
		if err != nil {
			t.Logf("failed to resolve upgrade report path: %v", err)
			return
		}
		if writeErr := writeCompatibilityReportFile(resolved, payload); writeErr != nil {
			t.Logf("failed to write upgrade report to %s: %v", resolved, writeErr)
		}
	}
}
