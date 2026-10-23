package e2e_test

import (
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
