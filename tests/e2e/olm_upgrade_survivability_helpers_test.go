package e2e_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	upgradeOperatorCOO   = "coo"
	upgradeOperatorTempo = "tempo"
)

var csvPhasesStuck = []string{
	"Replacing",
	"Deleting",
	"Pending",
	"Failed",
	"Unknown",
}

// operatorOLMSnapshot captures subscription and CSV state for upgrade diagnostics.
type operatorOLMSnapshot struct {
	SubscriptionNamespace string `json:"subscriptionNamespace,omitempty"`
	SubscriptionName      string `json:"subscriptionName,omitempty"`
	Channel               string `json:"channel,omitempty"`
	StartingCSV           string `json:"startingCSV,omitempty"`
	InstallPlanRef        string `json:"installPlanRef,omitempty"`
	InstalledCSV          string `json:"installedCSV,omitempty"`
	CurrentCSV            string `json:"currentCSV,omitempty"`
	CSVPhase              string `json:"csvPhase,omitempty"`
	CSVVersion            string `json:"csvVersion,omitempty"`
}

func parseUpgradeOperators(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("upgrade-operators is empty")
	}
	parts := strings.Split(raw, ",")
	operators := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(strings.ToLower(part))
		if part == "" {
			continue
		}
		switch part {
		case upgradeOperatorCOO, upgradeOperatorTempo:
			if slices.Contains(operators, part) {
				continue
			}
			operators = append(operators, part)
		default:
			return nil, fmt.Errorf("unsupported operator %q (allowed: coo, tempo)", part)
		}
	}
	if len(operators) == 0 {
		return nil, errors.New("no operators parsed from upgrade-operators")
	}
	return operators, nil
}

func subscriptionInstalledCSV(sub *unstructured.Unstructured) string {
	if sub == nil {
		return ""
	}
	csvName, _, _ := unstructured.NestedString(sub.Object, "status", "installedCSV")
	if csvName != "" {
		return csvName
	}
	csvName, _, _ = unstructured.NestedString(sub.Object, "status", "currentCSV")
	return csvName
}

func csvPhaseIndicatesStuckLifecycle(phase string) bool {
	phase = strings.TrimSpace(phase)
	if phase == "" {
		return true
	}
	return slices.Contains(csvPhasesStuck, phase)
}

func firstOLMLifecycleFailure(snapshots []operatorOLMSnapshot) string {
	for _, snap := range snapshots {
		if snap.InstalledCSV == "" {
			return fmt.Sprintf(
				"subscription %s/%s has no installed CSV (installPlanRef=%s)",
				snap.SubscriptionNamespace, snap.SubscriptionName, snap.InstallPlanRef,
			)
		}
		if csvPhaseIndicatesStuckLifecycle(snap.CSVPhase) {
			return fmt.Sprintf(
				"CSV %s/%s phase is %q (subscription %s/%s channel=%s startingCSV=%s)",
				snap.SubscriptionNamespace, snap.InstalledCSV, snap.CSVPhase,
				snap.SubscriptionNamespace, snap.SubscriptionName, snap.Channel, snap.StartingCSV,
			)
		}
	}
	return ""
}

func upgradeTargetChannelForOperator(operator string) string {
	switch operator {
	case upgradeOperatorCOO:
		return strings.TrimSpace(testOpts.upgradeCOOChannel)
	case upgradeOperatorTempo:
		return strings.TrimSpace(testOpts.upgradeTempoChannel)
	default:
		return ""
	}
}

func upgradeOperatorSubscription(operator string) (namespace, name string, err error) {
	switch operator {
	case upgradeOperatorCOO:
		return observabilityOpNamespace, observabilityOpName, nil
	case upgradeOperatorTempo:
		return tempoOpNamespace, tempoOpName, nil
	default:
		return "", "", fmt.Errorf("unsupported upgrade operator %q", operator)
	}
}
