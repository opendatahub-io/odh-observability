package e2e_test

import (
	"flag"
	"fmt"
	"os"
	"testing"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var testOpts = TestContextConfig{}

func TestMain(m *testing.M) {
	testOpts.registerFlags()
	flag.Parse()

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
