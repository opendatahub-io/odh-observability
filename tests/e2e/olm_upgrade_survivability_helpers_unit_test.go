package e2e_test

import (
	"encoding/json"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestParseUpgradeOperators(t *testing.T) {
	got, err := parseUpgradeOperators("coo, tempo")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 || got[0] != upgradeOperatorCOO || got[1] != upgradeOperatorTempo {
		t.Fatalf("unexpected operators: %v", got)
	}

	if _, err := parseUpgradeOperators("coo,coo"); err != nil {
		t.Fatalf("duplicate coo should be deduped, got error: %v", err)
	}

	if _, err := parseUpgradeOperators("loki"); err == nil {
		t.Fatal("expected unsupported operator to fail")
	}
}

func TestFirstOLMLifecycleFailure(t *testing.T) {
	if msg := firstOLMLifecycleFailure([]operatorOLMSnapshot{
		{SubscriptionNamespace: "ns", SubscriptionName: "sub", InstalledCSV: "csv", CSVPhase: "Succeeded"},
	}); msg != "" {
		t.Fatalf("expected healthy snapshot, got %q", msg)
	}

	if msg := firstOLMLifecycleFailure([]operatorOLMSnapshot{
		{SubscriptionNamespace: "ns", SubscriptionName: "sub", CSVPhase: "Replacing", InstalledCSV: "csv"},
	}); msg == "" {
		t.Fatal("expected stuck CSV to be reported")
	}

	if msg := firstOLMLifecycleFailure([]operatorOLMSnapshot{
		{SubscriptionNamespace: "ns", SubscriptionName: "sub", InstalledCSV: "csv", CSVPhase: "InstallReady"},
	}); msg == "" {
		t.Fatal("expected non-Succeeded CSV phase to be reported")
	}
}

func TestUpgradeTransitionsIncludePerformedUpgrade(t *testing.T) {
	if upgradeTransitionsIncludePerformedUpgrade([]operatorUpgradeTransition{
		{Skipped: true},
	}) {
		t.Fatal("expected all-skipped transitions to be rejected")
	}
	if !upgradeTransitionsIncludePerformedUpgrade([]operatorUpgradeTransition{
		{Skipped: false},
	}) {
		t.Fatal("expected performed upgrade when Skipped is false")
	}
}

func TestSubscriptionInstallPlanApprovalIsManual(t *testing.T) {
	if subscriptionInstallPlanApprovalIsManual(nil) {
		t.Fatal("nil subscription should not be Manual")
	}
	if subscriptionInstallPlanApprovalIsManual(&unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"installPlanApproval": "Automatic"},
	}}) {
		t.Fatal("Automatic should not be Manual")
	}
	if !subscriptionInstallPlanApprovalIsManual(&unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"installPlanApproval": "Manual"},
	}}) {
		t.Fatal("expected Manual approval policy")
	}
}

func TestOperatorUpgradeTargetCSV(t *testing.T) {
	snap := operatorOLMSnapshot{
		SubscriptionName: tempoOpName,
		InstallPlanTargetCSVs: []string{
			"dependency.v1.0.0",
			"tempo-operator.v0.22.0-4",
		},
	}
	if got := operatorUpgradeTargetCSV(snap, "tempo-operator.v0.22.0-3"); got != "tempo-operator.v0.22.0-4" {
		t.Fatalf("unexpected target CSV %q", got)
	}
	if !installPlanTargetsOperatorUpgrade(snap, "tempo-operator.v0.22.0-3") {
		t.Fatal("expected InstallPlan to target a Tempo upgrade")
	}
	if got := operatorUpgradeTargetCSV(snap, "tempo-operator.v0.22.0-4"); got != "" {
		t.Fatalf("current CSV must not be treated as an upgrade target, got %q", got)
	}
}

func TestInstallPlanTargetsExpectedOrIntermediate(t *testing.T) {
	snap := operatorOLMSnapshot{
		SubscriptionName:      tempoOpName,
		InstallPlanTargetCSVs: []string{"tempo-operator.v0.22.0-3"},
	}
	if !installPlanTargetsExpectedOrIntermediate(snap, "tempo-operator.v0.22.0-2", "tempo-operator.v0.22.0-4") {
		t.Fatal("expected an intermediate Tempo CSV to be accepted")
	}
	if installPlanTargetsExpectedOrIntermediate(snap, "tempo-operator.v0.22.0-3", "tempo-operator.v0.22.0-4") {
		t.Fatal("the currently installed CSV must not be accepted as progress")
	}
	snap.InstallPlanTargetCSVs = []string{"tempo-operator.v0.22.0-4"}
	if !installPlanTargetsExpectedOrIntermediate(snap, "tempo-operator.v0.22.0-3", "tempo-operator.v0.22.0-4") {
		t.Fatal("expected the final target CSV to be accepted")
	}
}

func TestPackageManifestChannelCurrentCSV(t *testing.T) {
	manifest := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "tempo-product"},
		"status": map[string]any{"channels": []any{
			map[string]any{"name": "stable", "currentCSV": "tempo-operator.v0.22.0-4"},
		}},
	}}
	got, err := packageManifestChannelCurrentCSV(manifest, "stable")
	if err != nil || got != "tempo-operator.v0.22.0-4" {
		t.Fatalf("unexpected target CSV %q, err=%v", got, err)
	}
	if _, err := packageManifestChannelCurrentCSV(manifest, "missing"); err == nil {
		t.Fatal("expected missing channel to fail")
	}
}

func TestTempoTracePayload(t *testing.T) {
	const traceID = "0123456789abcdef0123456789abcdef"
	const spanID = "0123456789abcdef"
	payload := tempoTracePayload(traceID, spanID, 100, 200)
	if !json.Valid([]byte(payload)) {
		t.Fatalf("Tempo trace payload is not valid JSON: %s", payload)
	}
	for _, expected := range []string{traceID, spanID, `"startTimeUnixNano":"100"`, `"endTimeUnixNano":"200"`} {
		if !strings.Contains(payload, expected) {
			t.Fatalf("Tempo trace payload does not contain %q: %s", expected, payload)
		}
	}
}

func TestUpgradeConfigRejectsOperatorInstallation(t *testing.T) {
	c := TestContextConfig{
		apiMode:           APIModeDSC,
		installOperators:  true,
		upgradeEnabled:    true,
		upgradeOperators:  upgradeOperatorCOO,
		upgradeCOOChannel: "stable",
	}
	if err := c.validate(); err == nil || !strings.Contains(err.Error(), "requires -install-operators=false") {
		t.Fatalf("expected upgrade configuration to reject operator installation, got %v", err)
	}
}

func TestOperatorCSVNamePrefix(t *testing.T) {
	if got := operatorCSVNamePrefix(tempoOpName); got != "tempo-operator." {
		t.Fatalf("tempo subscription prefix: got %q", got)
	}
	if got := operatorCSVNamePrefix(observabilityOpName); got != observabilityOpName+"." {
		t.Fatalf("COO subscription prefix: got %q", got)
	}
}

func TestStaleOperatorCSVFailure(t *testing.T) {
	snap := operatorOLMSnapshot{
		SubscriptionNamespace: "openshift-tempo-operator",
		SubscriptionName:      tempoOpName,
		InstalledCSV:          "tempo-operator.v1.2.3",
		CSVPhase:              "Succeeded",
		OperatorCSVs: []operatorCSVPhase{
			{Name: "tempo-operator.v1.2.3", Phase: "Succeeded"},
			{Name: "tempo-operator.v1.2.2", Phase: "Replacing"},
		},
	}
	if msg := staleOperatorCSVFailure(snap); msg == "" {
		t.Fatal("expected stale Replacing CSV to be reported")
	}

	healthy := snap
	healthy.OperatorCSVs = []operatorCSVPhase{{Name: healthy.InstalledCSV, Phase: "Succeeded"}}
	if msg := staleOperatorCSVFailure(healthy); msg != "" {
		t.Fatalf("expected no stale failure, got %q", msg)
	}
}

func TestSubscriptionInstalledCSV(t *testing.T) {
	sub := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{"installedCSV": "a"},
	}}
	if subscriptionInstalledCSV(sub) != "a" {
		t.Fatal("expected installedCSV")
	}
	sub = &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{"currentCSV": "b"},
	}}
	if subscriptionInstalledCSV(sub) != "b" {
		t.Fatal("expected currentCSV fallback")
	}
}
