package e2e_test

import (
	"flag"
	"testing"
)

var testOpts = TestContextConfig{}

func TestMain(m *testing.M) {
	testOpts.registerFlags()
	flag.Parse()
	testOpts.applyDefaults()

	m.Run()
}

func TestMonitoring(t *testing.T) {
	monitoringTestSuite(t)
}
