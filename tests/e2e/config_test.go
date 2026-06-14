package e2e_test

import (
	"flag"
	"time"
)

type TestTimeouts struct {
	defaultEventuallyTimeout      time.Duration
	defaultEventuallyPollInterval time.Duration
	defaultConsistentlyTimeout    time.Duration
	defaultConsistentlyPollInterval time.Duration
}

type TestContextConfig struct {
	monitoringNamespace string
	monitoringCRName    string
	Timeouts            TestTimeouts
}

func (c *TestContextConfig) registerFlags() {
	flag.StringVar(&c.monitoringNamespace, "monitoring-namespace", "", "namespace where monitoring operands are deployed")
	flag.StringVar(&c.monitoringCRName, "monitoring-cr-name", "", "name of the Monitoring CR")

	flag.DurationVar(&c.Timeouts.defaultEventuallyTimeout, "eventually-timeout", 0, "default eventually timeout")
	flag.DurationVar(&c.Timeouts.defaultEventuallyPollInterval, "eventually-poll-interval", 0, "default eventually poll interval")
	flag.DurationVar(&c.Timeouts.defaultConsistentlyTimeout, "consistently-timeout", 0, "default consistently timeout")
	flag.DurationVar(&c.Timeouts.defaultConsistentlyPollInterval, "consistently-poll-interval", 0, "default consistently poll interval")
}

func (c *TestContextConfig) applyDefaults() {
	if c.monitoringNamespace == "" {
		c.monitoringNamespace = "redhat-ods-monitoring"
	}
	if c.monitoringCRName == "" {
		c.monitoringCRName = "default-monitoring"
	}
	if c.Timeouts.defaultEventuallyTimeout == 0 {
		c.Timeouts.defaultEventuallyTimeout = 5 * time.Minute
	}
	if c.Timeouts.defaultEventuallyPollInterval == 0 {
		c.Timeouts.defaultEventuallyPollInterval = 2 * time.Second
	}
	if c.Timeouts.defaultConsistentlyTimeout == 0 {
		c.Timeouts.defaultConsistentlyTimeout = 30 * time.Second
	}
	if c.Timeouts.defaultConsistentlyPollInterval == 0 {
		c.Timeouts.defaultConsistentlyPollInterval = 2 * time.Second
	}
}
