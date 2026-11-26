package e2e_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/opendatahub-io/odh-observability/tests/e2e/runner"
)

func TestAIGridOptInConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled bool
		config  string
		wantErr bool
	}{
		{name: "disabled by default"},
		{name: "configuration alone does not enable", config: "/private/grid.yaml"},
		{name: "enabled requires configuration", enabled: true, wantErr: true},
		{name: "blank configuration rejected", enabled: true, config: " \t", wantErr: true},
		{name: "enabled with configuration", enabled: true, config: "/private/grid.yaml"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := TestContextConfig{apiMode: APIModeModule, aiGridEnabled: tc.enabled, aiGridConfig: tc.config}
			err := cfg.validate()
			if tc.wantErr {
				require.ErrorContains(t, err, "ai-grid-enabled requires -ai-grid-config or E2E_TEST_AI_GRID_CONFIG")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAIGridOptInEnvironmentFlags(t *testing.T) {
	for _, value := range []string{"", "false", "true"} {
		t.Run("enabled="+value, func(t *testing.T) {
			t.Setenv("E2E_TEST_AI_GRID_ENABLED", value)
			t.Setenv("E2E_TEST_AI_GRID_CONFIG", "/private/grid.yaml")
			flags := runner.EnvTestFlags()
			require.Contains(t, flags, "-ai-grid-config=/private/grid.yaml")
			if value == "" {
				for _, flag := range flags {
					require.False(t, strings.HasPrefix(flag, "-ai-grid-enabled="), "a config path must not add an enable flag")
				}
			} else {
				require.Contains(t, flags, "-ai-grid-enabled="+value)
			}
		})
	}
}
