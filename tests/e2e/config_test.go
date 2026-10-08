package e2e_test

import (
	"flag"
	"fmt"
	"time"
)

// API mode constants for test configuration.
const (
	APIModeModule = "module"
	APIModeDSC    = "dsc"
)

type TestTimeouts struct {
	defaultEventuallyTimeout        time.Duration
	defaultEventuallyPollInterval   time.Duration
	defaultConsistentlyTimeout      time.Duration
	defaultConsistentlyPollInterval time.Duration
	olmOperationTimeout             time.Duration
}

type TestContextConfig struct {
	monitoringNamespace     string
	applicationsNamespace   string
	monitoringCRName        string
	installOperators        bool
	applyMonitoringManifest bool
	apiMode                 string
	dsciCRName              string
	Timeouts                TestTimeouts
}

// registerFlags registers test binary flags.
// These flags are mapped from env vars by runner.envToFlags (tests/e2e/runner/runner.go).
func (c *TestContextConfig) registerFlags() {
	flag.StringVar(&c.monitoringNamespace, "monitoring-namespace", "", "namespace for monitoring operands (auto-detected if omitted; inference test uses redhat-ods-monitoring)")
	flag.StringVar(&c.applicationsNamespace, "applications-namespace", "", "inference test only: DSCI spec.applicationsNamespace (defaults to redhat-ods-applications)")
	flag.StringVar(&c.monitoringCRName, "monitoring-cr-name", "", "name of the Monitoring CR")
	flag.BoolVar(&c.installOperators, "install-operators", true, "install dependent OLM operators before running tests")
	flag.BoolVar(&c.applyMonitoringManifest, "apply-monitoring-manifest", true, "inference test only: overwrite the Monitoring CR spec from prerequisites/inference/monitoring.yaml")
	flag.StringVar(&c.apiMode, "api-mode", "module", "API mode: 'module' for standalone module CR, 'dsc' for DSC/DSCI integration")
	flag.StringVar(&c.dsciCRName, "dsci-cr-name", "default-dsci", "name of the DSCInitialization CR (DSC mode only)")

	flag.DurationVar(&c.Timeouts.defaultEventuallyTimeout, "eventually-timeout", 0, "default eventually timeout")
	flag.DurationVar(&c.Timeouts.defaultEventuallyPollInterval, "eventually-poll-interval", 0, "default eventually poll interval")
	flag.DurationVar(&c.Timeouts.defaultConsistentlyTimeout, "consistently-timeout", 0, "default consistently timeout")
	flag.DurationVar(&c.Timeouts.defaultConsistentlyPollInterval, "consistently-poll-interval", 0, "default consistently poll interval")
	flag.DurationVar(&c.Timeouts.olmOperationTimeout, "olm-timeout", 0, "timeout for OLM operator installation")
}

func (c *TestContextConfig) validate() error {
	switch c.apiMode {
	case APIModeModule, APIModeDSC:
	default:
		return fmt.Errorf("invalid api-mode %q: must be %q or %q", c.apiMode, APIModeModule, APIModeDSC)
	}
	return nil
}

func (c *TestContextConfig) applyDefaults() {
	if c.monitoringCRName == "" {
		c.monitoringCRName = "default-monitoring"
	}
	if c.Timeouts.defaultEventuallyTimeout <= 0 {
		c.Timeouts.defaultEventuallyTimeout = 5 * time.Minute
	}
	if c.Timeouts.defaultEventuallyPollInterval <= 0 {
		c.Timeouts.defaultEventuallyPollInterval = 2 * time.Second
	}
	if c.Timeouts.defaultConsistentlyTimeout <= 0 {
		c.Timeouts.defaultConsistentlyTimeout = 30 * time.Second
	}
	if c.Timeouts.defaultConsistentlyPollInterval <= 0 {
		c.Timeouts.defaultConsistentlyPollInterval = 2 * time.Second
	}
	if c.Timeouts.olmOperationTimeout <= 0 {
		c.Timeouts.olmOperationTimeout = 5 * time.Minute
	}
}
