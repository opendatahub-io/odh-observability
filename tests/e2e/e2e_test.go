package e2e_test

import (
	"flag"
	"testing"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var testOpts = TestContextConfig{}

func TestMain(m *testing.M) {
	testOpts.registerFlags()
	flag.Parse()
	testOpts.applyDefaults()

	logf.SetLogger(zap.New(zap.UseDevMode(true)))

	m.Run()
}

func TestMonitoring(t *testing.T) {
	monitoringTestSuite(t)
}
