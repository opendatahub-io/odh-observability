package e2e_test

import (
	"flag"
	"fmt"
	"os"
	"testing"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/opendatahub-io/odh-observability/tests/e2e/runner"
)

var testOpts = TestContextConfig{}

func TestMain(m *testing.M) {
	testOpts.registerFlags()
	args := append(runner.EnvTestFlags(), os.Args[1:]...)
	if err := flag.CommandLine.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "invalid test flags: %v\n", err)
		os.Exit(1)
	}

	if err := testOpts.validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid test configuration: %v\n", err)
		os.Exit(1)
	}

	testOpts.applyDefaults()

	logf.SetLogger(zap.New(zap.UseDevMode(true)))

	m.Run()
}

func TestMonitoring(t *testing.T) {
	monitoringTestSuite(t)
}

func TestCOOVersionCompatibility(t *testing.T) {
	cooVersionCompatibilitySuite(t)
}

func TestOLMUpgradeSurvivability(t *testing.T) {
	olmUpgradeSurvivabilitySuite(t)
}
