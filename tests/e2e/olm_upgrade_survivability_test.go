package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
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

	mctx.ensurePrerequisitesPreservingMonitoring(t)

	// Keep the baseline, upgrade, and post-upgrade assertions in one test body.
	// Separate subtests can be independently selected with -run and would allow
	// the post-upgrade contract to pass without executing an upgrade.
	t.Log("recording and validating pre-upgrade baseline")
	report.Baseline = mctx.captureUpgradeHealthSnapshot(t, report)
	mctx.assertOLMSnapshotsHealthy(t, report, report.Baseline.OLM)
	mctx.assertMonitoringHealthContract(t, report, "before OLM upgrade")
	if slices.Contains(operators, upgradeOperatorTempo) {
		mctx.assertTempoTraceHealth(t, report, "before OLM upgrade")
	}

	t.Log("executing configured OLM upgrades")
	mctx.runConfiguredOperatorUpgrades(t, report, operators)
	if !upgradeTransitionsIncludePerformedUpgrade(report.OperatorTransitions) {
		mctx.failUpgradeAssertion(t, report, "no operator upgrade was performed; target channels must differ from the current subscription channel")
	}

	t.Log("capturing and validating post-upgrade state")
	report.PostUpgrade = mctx.captureUpgradeHealthSnapshot(t, report)
	mctx.assertOLMSnapshotsHealthy(t, report, report.PostUpgrade.OLM)
	mctx.assertMonitoringHealthContract(t, report, "after OLM upgrade")
	if slices.Contains(operators, upgradeOperatorTempo) {
		mctx.assertTempoTraceHealth(t, report, "after OLM upgrade")
	}

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
	snap.InstallPlanNamespace, _, _ = unstructured.NestedString(sub.Object, "status", "installPlanRef", "namespace")
	if snap.InstallPlanNamespace == "" {
		snap.InstallPlanNamespace = namespace
	}
	if installPlanName != "" {
		ip, installPlanErr := tc.fetchResource(tc.t, gvk.InstallPlan, types.NamespacedName{
			Name: installPlanName, Namespace: snap.InstallPlanNamespace,
		})
		if installPlanErr != nil {
			snap.InstallPlanReadError = installPlanErr.Error()
		} else {
			snap.InstallPlanPhase, _, _ = unstructured.NestedString(ip.Object, "status", "phase")
			snap.InstallPlanApproved, _, _ = unstructured.NestedBool(ip.Object, "spec", "approved")
			snap.InstallPlanTargetCSVs, _, _ = unstructured.NestedStringSlice(ip.Object, "spec", "clusterServiceVersionNames")
			conditions, _, _ := unstructured.NestedSlice(ip.Object, "status", "conditions")
			for _, rawCondition := range conditions {
				condition, ok := rawCondition.(map[string]any)
				if !ok {
					continue
				}
				snap.InstallPlanConditions = append(snap.InstallPlanConditions, operatorInstallPlanCondition{
					Type: installPlanConditionString(condition, "type"), Message: installPlanConditionString(condition, "message"),
					Reason: installPlanConditionString(condition, "reason"), Status: installPlanConditionString(condition, "status"),
				})
			}
		}
	}

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

	snap.OperatorCSVs, err = tc.listOperatorCSVPhases(namespace, name)
	if err != nil {
		return snap, err
	}
	return snap, nil
}

func installPlanConditionString(condition map[string]any, key string) string {
	value, _ := condition[key].(string)
	return value
}

func (tc *MonitoringTestCtx) listOperatorCSVPhases(namespace, subscriptionName string) ([]operatorCSVPhase, error) {
	prefix := operatorCSVNamePrefix(subscriptionName)
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   gvk.ClusterServiceVersion.Group,
		Version: gvk.ClusterServiceVersion.Version,
		Kind:    gvk.ClusterServiceVersion.Kind + "List",
	})
	if err := tc.client.List(tc.ctx, list, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list ClusterServiceVersions in %s: %w", namespace, err)
	}
	phases := make([]operatorCSVPhase, 0)
	for _, item := range list.Items {
		if !strings.HasPrefix(item.GetName(), prefix) {
			continue
		}
		phase, _, _ := unstructured.NestedString(item.Object, "status", "phase")
		phases = append(phases, operatorCSVPhase{Name: item.GetName(), Phase: phase})
	}
	slices.SortFunc(phases, func(a, b operatorCSVPhase) int {
		return strings.Compare(a.Name, b.Name)
	})
	return phases, nil
}

func (tc *MonitoringTestCtx) recordFailedOLMUpgradeState(report *olmUpgradeSurvivabilityReport, snap operatorOLMSnapshot) {
	if report.PostUpgrade == nil {
		report.PostUpgrade = &olmUpgradeHealthSnapshot{}
	}
	report.PostUpgrade.OLM = []operatorOLMSnapshot{snap}
	if msg := firstOLMLifecycleFailure([]operatorOLMSnapshot{snap}); msg != "" {
		report.FirstLifecycleFailure = msg
		return
	}
	if snap.InstalledCSV != "" {
		report.FirstLifecycleFailure = fmt.Sprintf(
			"OLM upgrade timed out for subscription %s/%s with installed CSV %s phase %q (installPlanRef=%s)",
			snap.SubscriptionNamespace, snap.SubscriptionName, snap.InstalledCSV, snap.CSVPhase, snap.InstallPlanRef,
		)
	}
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
		expectedTargetCSV, err := tc.subscriptionChannelCurrentCSV(ns, name, targetChannel)
		if err != nil {
			tc.failUpgradeAssertion(t, report, fmt.Sprintf(
				"failed to resolve target CSV for %s channel %s: %v", operator, targetChannel, err))
		}
		transition.TargetCSV = expectedTargetCSV
		if before.InstalledCSV == expectedTargetCSV {
			if !stringsEqualFoldTrim(before.Channel, targetChannel) {
				tc.failUpgradeAssertion(t, report, fmt.Sprintf(
					"cannot test %s upgrade: target channel %s resolves to already-installed CSV %s",
					operator, targetChannel, expectedTargetCSV))
			}
			transition.Skipped = true
			transition.SkipReason = "operator already uses target-channel CSV"
			report.OperatorTransitions = append(report.OperatorTransitions, transition)
			t.Logf("skipping %s upgrade: already using target CSV %s", operator, expectedTargetCSV)
			continue
		}
		requireNewInstallPlan := !stringsEqualFoldTrim(before.Channel, targetChannel)

		if err := tc.patchSubscriptionChannel(types.NamespacedName{Namespace: ns, Name: name}, targetChannel); err != nil {
			tc.failUpgradeAssertion(t, report, fmt.Sprintf("failed to patch %s subscription to target channel %s: %v", operator, targetChannel, err))
		}
		t.Logf("patched Subscription %s/%s to channel %s", ns, name, targetChannel)
		tc.waitForOperatorUpgrade(
			t, report, ns, name, targetChannel, expectedTargetCSV,
			before.InstalledCSV, before.InstallPlanRef, requireNewInstallPlan,
		)
		after, err := tc.operatorOLMSnapshot(ns, name)
		if err != nil {
			tc.failUpgradeAssertion(t, report, fmt.Sprintf("failed to read post-upgrade OLM state for %s: %v", operator, err))
		}
		report.OperatorTransitions = append(report.OperatorTransitions, transition)

		tc.assertOLMSnapshotsHealthy(t, report, []operatorOLMSnapshot{after})
		tc.assertMonitoringOperandsWithoutBlockingFinalizers(t, report)
	}
}

func (tc *MonitoringTestCtx) subscriptionChannelCurrentCSV(namespace, name, targetChannel string) (string, error) {
	sub, err := tc.fetchResource(tc.t, gvk.Subscription, types.NamespacedName{Namespace: namespace, Name: name})
	if err != nil {
		return "", fmt.Errorf("read subscription: %w", err)
	}
	packageName, _, _ := unstructured.NestedString(sub.Object, "spec", "name")
	source, _, _ := unstructured.NestedString(sub.Object, "spec", "source")
	sourceNamespace, _, _ := unstructured.NestedString(sub.Object, "spec", "sourceNamespace")
	if packageName == "" || source == "" || sourceNamespace == "" {
		return "", errors.New("subscription is missing package, catalog source, or source namespace")
	}

	manifests := &unstructured.UnstructuredList{}
	manifests.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "packages.operators.coreos.com", Version: "v1", Kind: "PackageManifestList",
	})
	if err := tc.client.List(tc.ctx, manifests,
		client.InNamespace(sourceNamespace), client.MatchingLabels{"catalog": source}); err != nil {
		return "", fmt.Errorf("list PackageManifests for catalog %s/%s: %w", sourceNamespace, source, err)
	}
	for _, manifest := range manifests.Items {
		if manifest.GetName() != packageName {
			continue
		}
		return packageManifestChannelCurrentCSV(&manifest, targetChannel)
	}
	return "", fmt.Errorf("PackageManifest %s not found for catalog %s/%s", packageName, sourceNamespace, source)
}

func packageManifestChannelCurrentCSV(manifest *unstructured.Unstructured, targetChannel string) (string, error) {
	channels, _, _ := unstructured.NestedSlice(manifest.Object, "status", "channels")
	for _, raw := range channels {
		channel, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		channelName, _, _ := unstructured.NestedString(channel, "name")
		if channelName != targetChannel {
			continue
		}
		currentCSV, _, _ := unstructured.NestedString(channel, "currentCSV")
		if currentCSV == "" {
			return "", fmt.Errorf("PackageManifest %s channel %s has no currentCSV", manifest.GetName(), targetChannel)
		}
		return currentCSV, nil
	}
	return "", fmt.Errorf("PackageManifest %s has no channel %s", manifest.GetName(), targetChannel)
}

func stringsEqualFoldTrim(a, b string) bool {
	return strings.TrimSpace(strings.ToLower(a)) == strings.TrimSpace(strings.ToLower(b))
}

func (tc *MonitoringTestCtx) patchSubscriptionChannel(nn types.NamespacedName, channel string) error {
	patch, err := json.Marshal(map[string]any{"spec": map[string]any{"channel": channel}})
	if err != nil {
		return fmt.Errorf("build subscription patch: %w", err)
	}
	var lastErr error
	err = wait.PollUntilContextTimeout(tc.ctx, 2*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		sub := &unstructured.Unstructured{}
		sub.SetGroupVersionKind(gvk.Subscription)
		if err := tc.client.Get(ctx, nn, sub); err != nil {
			lastErr = err
			return false, nil //nolint:nilerr // retry transient API failures until the bounded timeout
		}
		if err := tc.client.Patch(ctx, sub, client.RawPatch(types.MergePatchType, patch)); err != nil {
			lastErr = err
			return false, nil //nolint:nilerr // retry conflicts and transient API failures
		}
		return true, nil
	})
	if err != nil {
		if lastErr != nil {
			return fmt.Errorf("patch Subscription %s/%s channel to %s: %w", nn.Namespace, nn.Name, channel, lastErr)
		}
		return fmt.Errorf("patch Subscription %s/%s channel to %s: %w", nn.Namespace, nn.Name, channel, err)
	}
	return nil
}

func (tc *MonitoringTestCtx) approvePendingInstallPlanForSubscription(
	t *testing.T, subscriptionNamespace, subscriptionName, targetChannel, expectedTargetCSV, previousInstallPlan string,
	requireNewInstallPlan bool,
) error {
	t.Helper()
	sub, err := tc.fetchResource(tc.t, gvk.Subscription, types.NamespacedName{
		Namespace: subscriptionNamespace,
		Name:      subscriptionName,
	})
	if err != nil {
		return fmt.Errorf("read subscription %s/%s: %w", subscriptionNamespace, subscriptionName, err)
	}
	if !subscriptionInstallPlanApprovalIsManual(sub) {
		return nil
	}
	channel, _, _ := unstructured.NestedString(sub.Object, "spec", "channel")
	if !stringsEqualFoldTrim(channel, targetChannel) {
		return fmt.Errorf("subscription channel is %q, waiting for target channel %q", channel, targetChannel)
	}
	installPlanName, found, _ := unstructured.NestedString(sub.Object, "status", "installPlanRef", "name")
	if !found || installPlanName == "" {
		return fmt.Errorf("subscription has no InstallPlan resolved for target channel %q", targetChannel)
	}
	if requireNewInstallPlan && previousInstallPlan != "" && installPlanName == previousInstallPlan {
		return fmt.Errorf("subscription still references pre-upgrade InstallPlan %s", previousInstallPlan)
	}
	installPlanNamespace, _, _ := unstructured.NestedString(sub.Object, "status", "installPlanRef", "namespace")
	if installPlanNamespace == "" {
		installPlanNamespace = subscriptionNamespace
	}
	ipNN := types.NamespacedName{Name: installPlanName, Namespace: installPlanNamespace}
	ip, err := tc.fetchResource(tc.t, gvk.InstallPlan, ipNN)
	if err != nil {
		return fmt.Errorf("read InstallPlan %s: %w", ipNN, err)
	}
	targetCSVs, _, _ := unstructured.NestedStringSlice(ip.Object, "spec", "clusterServiceVersionNames")
	installedCSV := subscriptionInstalledCSV(sub)
	planSnap := operatorOLMSnapshot{SubscriptionName: subscriptionName, InstallPlanTargetCSVs: targetCSVs}
	if !installPlanTargetsExpectedOrIntermediate(planSnap, installedCSV, expectedTargetCSV) {
		return fmt.Errorf("InstallPlan %s targets %v, waiting for an operator upgrade from %s toward target-channel CSV %s",
			ipNN, targetCSVs, installedCSV, expectedTargetCSV)
	}
	approved, found, _ := unstructured.NestedBool(ip.Object, "spec", "approved")
	if found && approved {
		return nil
	}
	patch, err := json.Marshal(map[string]any{"spec": map[string]any{"approved": true}})
	if err != nil {
		return fmt.Errorf("build InstallPlan approval patch: %w", err)
	}
	if err := tc.client.Patch(tc.ctx, ip, client.RawPatch(types.MergePatchType, patch)); err != nil {
		return fmt.Errorf("approve InstallPlan %s: %w", ipNN, err)
	}
	t.Logf("approved InstallPlan %s/%s for Manual subscription %s/%s",
		installPlanNamespace, installPlanName, subscriptionNamespace, subscriptionName)
	return nil
}

func (tc *MonitoringTestCtx) waitForOperatorUpgrade(
	t *testing.T, report *olmUpgradeSurvivabilityReport,
	namespace, name, targetChannel, expectedTargetCSV, previousCSV, previousInstallPlan string,
	requireNewInstallPlan bool,
) {
	t.Helper()
	timeout := tc.Timeouts.olmOperationTimeout
	failureMsg := fmt.Sprintf("operator %s/%s did not converge after OLM upgrade within %s", namespace, name, timeout)
	var lastSnap operatorOLMSnapshot
	defer func() {
		if !t.Failed() {
			return
		}
		if lastSnap.SubscriptionName != "" {
			tc.recordFailedOLMUpgradeState(report, lastSnap)
		}
		if report.FailedAssertion == "" {
			report.FailedAssertion = failureMsg
		}
	}()
	tc.g.Eventually(func(g Gomega) {
		snap, err := tc.operatorOLMSnapshot(namespace, name)
		g.Expect(err).NotTo(HaveOccurred(), "failed to read subscription %s/%s during upgrade", namespace, name)
		lastSnap = snap
		g.Expect(snap.Channel).To(Equal(targetChannel),
			"subscription %s/%s should remain on target channel", namespace, name)
		g.Expect(snap.InstallPlanRef).NotTo(BeEmpty(),
			"subscription %s/%s should resolve an InstallPlan for the target channel", namespace, name)
		if requireNewInstallPlan && previousInstallPlan != "" {
			g.Expect(snap.InstallPlanRef).NotTo(Equal(previousInstallPlan),
				"subscription %s/%s should stop referencing pre-upgrade InstallPlan %s", namespace, name, previousInstallPlan)
		}
		g.Expect(installPlanTargetsExpectedOrIntermediate(snap, snap.InstalledCSV, expectedTargetCSV)).To(BeTrue(),
			"subscription %s/%s should reference an InstallPlan advancing toward target-channel CSV %s", namespace, name, expectedTargetCSV)
		g.Expect(tc.approvePendingInstallPlanForSubscription(
			t, namespace, name, targetChannel, expectedTargetCSV, previousInstallPlan, requireNewInstallPlan,
		)).To(Succeed(),
			"approve target InstallPlan for subscription %s/%s", namespace, name)
		if msg := staleOperatorCSVFailure(snap); msg != "" {
			g.Expect(msg).To(BeEmpty(), "%s", msg)
		}
		g.Expect(snap.InstalledCSV).NotTo(BeEmpty(), "subscription %s/%s should have installed CSV", namespace, name)
		if previousCSV != "" {
			g.Expect(snap.InstalledCSV).NotTo(Equal(previousCSV),
				"subscription %s/%s should advance from CSV %s", namespace, name, previousCSV)
		}
		g.Expect(snap.InstalledCSV).To(Equal(expectedTargetCSV),
			"subscription %s/%s should converge on target-channel CSV %s", namespace, name, expectedTargetCSV)
		g.Expect(csvPhaseIndicatesStuckLifecycle(snap.CSVPhase)).To(BeFalse(),
			"CSV %s should not be stuck in phase %q", snap.InstalledCSV, snap.CSVPhase)
		g.Expect(snap.CSVPhase).To(Equal("Succeeded"), "CSV %s phase", snap.InstalledCSV)
	}).WithTimeout(timeout).WithPolling(15*time.Second).Should(Succeed(), failureMsg)
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
		tc.waitForOperandDeletionCleanupAfterUpgrade(t, report, resource.gvk, resource.name, resource.namespace, cleanupTimeout)
	}
}

func (tc *MonitoringTestCtx) waitForOperandDeletionCleanupAfterUpgrade(
	t *testing.T,
	report *olmUpgradeSurvivabilityReport,
	resourceGVK schema.GroupVersionKind,
	name, namespace string,
	cleanupTimeout time.Duration,
) {
	t.Helper()
	tc = tc.WithT(t)
	nn := types.NamespacedName{Name: name, Namespace: namespace}
	failureMsg := fmt.Sprintf("%s %s did not finish deletion cleanup within %s after OLM upgrade",
		resourceGVK.Kind, name, cleanupTimeout)
	defer func() {
		if t.Failed() && report.FailedAssertion == "" {
			report.FailedAssertion = failureMsg
		}
	}()
	tc.g.Eventually(func() error {
		obj, err := tc.fetchResource(t, resourceGVK, nn)
		if err != nil {
			if k8serr.IsNotFound(err) {
				return nil
			}
			return err
		}
		if obj.GetDeletionTimestamp() == nil {
			return nil
		}
		if finalizers := obj.GetFinalizers(); len(finalizers) > 0 {
			return fmt.Errorf("%s %s is still deleting with finalizers %v", resourceGVK.Kind, name, finalizers)
		}
		return nil
	}).WithTimeout(cleanupTimeout).WithPolling(15*time.Second).Should(Succeed(), failureMsg)
}

func (tc *MonitoringTestCtx) assertMonitoringHealthContract(
	t *testing.T, report *olmUpgradeSurvivabilityReport, stage string,
) {
	t.Helper()
	tc = tc.WithT(t)

	monitoringFailureMsg := fmt.Sprintf(
		"Monitoring should be Ready with metrics stack, Thanos, and Perses available %s", stage,
	)
	compat := &cooCompatibilityReport{}
	defer func() {
		if compat.FailedAssertion != "" {
			report.FailedAssertion = compat.FailedAssertion
			return
		}
		if t.Failed() && report.FailedAssertion == "" {
			report.FailedAssertion = monitoringFailureMsg
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
		WithCustomErrorMsg(monitoringFailureMsg),
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
