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
	SubscriptionNamespace string                         `json:"subscriptionNamespace,omitempty"`
	SubscriptionName      string                         `json:"subscriptionName,omitempty"`
	Channel               string                         `json:"channel,omitempty"`
	StartingCSV           string                         `json:"startingCSV,omitempty"`
	InstallPlanRef        string                         `json:"installPlanRef,omitempty"`
	InstallPlanNamespace  string                         `json:"installPlanNamespace,omitempty"`
	InstallPlanPhase      string                         `json:"installPlanPhase,omitempty"`
	InstallPlanApproved   bool                           `json:"installPlanApproved,omitempty"`
	InstallPlanTargetCSVs []string                       `json:"installPlanTargetCSVs,omitempty"`
	InstallPlanConditions []operatorInstallPlanCondition `json:"installPlanConditions,omitempty"`
	InstallPlanReadError  string                         `json:"installPlanReadError,omitempty"`
	InstalledCSV          string                         `json:"installedCSV,omitempty"`
	CurrentCSV            string                         `json:"currentCSV,omitempty"`
	CSVPhase              string                         `json:"csvPhase,omitempty"`
	CSVVersion            string                         `json:"csvVersion,omitempty"`
	OperatorCSVs          []operatorCSVPhase             `json:"operatorCSVs,omitempty"`
}

type operatorInstallPlanCondition struct {
	Type    string `json:"type,omitempty"`
	Status  string `json:"status,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

type operatorCSVPhase struct {
	Name  string `json:"name,omitempty"`
	Phase string `json:"phase,omitempty"`
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

func csvPhaseIsHealthy(phase string) bool {
	return strings.TrimSpace(phase) == "Succeeded"
}

func csvPhaseIndicatesStuckLifecycle(phase string) bool {
	phase = strings.TrimSpace(phase)
	if phase == "" {
		return true
	}
	if csvPhaseIsHealthy(phase) {
		return false
	}
	return slices.Contains(csvPhasesStuck, phase)
}

func firstOLMLifecycleFailure(snapshots []operatorOLMSnapshot) string {
	for _, snap := range snapshots {
		if msg := staleOperatorCSVFailure(snap); msg != "" {
			return msg
		}
		if snap.InstalledCSV == "" {
			return fmt.Sprintf(
				"subscription %s/%s has no installed CSV (installPlanRef=%s)",
				snap.SubscriptionNamespace, snap.SubscriptionName, snap.InstallPlanRef,
			)
		}
		if !csvPhaseIsHealthy(snap.CSVPhase) {
			return fmt.Sprintf(
				"CSV %s/%s phase is %q (subscription %s/%s channel=%s startingCSV=%s)",
				snap.SubscriptionNamespace, snap.InstalledCSV, snap.CSVPhase,
				snap.SubscriptionNamespace, snap.SubscriptionName, snap.Channel, snap.StartingCSV,
			)
		}
	}
	return ""
}

func operatorCSVNamePrefix(subscriptionName string) string {
	switch strings.TrimSpace(subscriptionName) {
	case tempoOpName:
		return "tempo-operator."
	default:
		return strings.TrimSpace(subscriptionName) + "."
	}
}

func staleOperatorCSVFailure(snap operatorOLMSnapshot) string {
	for _, csv := range snap.OperatorCSVs {
		if csv.Name == snap.InstalledCSV && csvPhaseIsHealthy(csv.Phase) {
			continue
		}
		if csvPhaseIndicatesStuckLifecycle(csv.Phase) {
			return fmt.Sprintf(
				"stale operator CSV %s/%s phase is %q (subscription %s/%s installedCSV=%s)",
				snap.SubscriptionNamespace, csv.Name, csv.Phase,
				snap.SubscriptionNamespace, snap.SubscriptionName, snap.InstalledCSV,
			)
		}
		if !csvPhaseIsHealthy(csv.Phase) {
			return fmt.Sprintf(
				"operator CSV %s/%s phase is %q (subscription %s/%s installedCSV=%s)",
				snap.SubscriptionNamespace, csv.Name, csv.Phase,
				snap.SubscriptionNamespace, snap.SubscriptionName, snap.InstalledCSV,
			)
		}
	}
	return ""
}

func upgradeTransitionsIncludePerformedUpgrade(transitions []operatorUpgradeTransition) bool {
	for _, transition := range transitions {
		if !transition.Skipped {
			return true
		}
	}
	return false
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

func upgradeOperatorSubscription(operator string) (string, string, error) {
	switch operator {
	case upgradeOperatorCOO:
		return observabilityOpNamespace, observabilityOpName, nil
	case upgradeOperatorTempo:
		return tempoOpNamespace, tempoOpName, nil
	default:
		return "", "", fmt.Errorf("unsupported upgrade operator %q", operator)
	}
}

func subscriptionInstallPlanApprovalIsManual(sub *unstructured.Unstructured) bool {
	if sub == nil {
		return false
	}
	approval, _, _ := unstructured.NestedString(sub.Object, "spec", "installPlanApproval")
	return strings.EqualFold(strings.TrimSpace(approval), "Manual")
}

func installPlanTargetsOperatorUpgrade(snap operatorOLMSnapshot, previousCSV string) bool {
	return operatorUpgradeTargetCSV(snap, previousCSV) != ""
}

func installPlanTargetsExpectedOrIntermediate(
	snap operatorOLMSnapshot, installedCSV, expectedTargetCSV string,
) bool {
	if slices.Contains(snap.InstallPlanTargetCSVs, expectedTargetCSV) {
		return true
	}
	return installPlanTargetsOperatorUpgrade(snap, installedCSV)
}

func operatorUpgradeTargetCSV(snap operatorOLMSnapshot, previousCSV string) string {
	prefix := operatorCSVNamePrefix(snap.SubscriptionName)
	for _, csvName := range snap.InstallPlanTargetCSVs {
		if strings.HasPrefix(csvName, prefix) && csvName != previousCSV {
			return csvName
		}
	}
	return ""
}
