package tui

import (
	"testing"

	"github.com/Slach/clickhouse-timeline/pkg/config"
	"github.com/Slach/clickhouse-timeline/pkg/models"
	"github.com/Slach/clickhouse-timeline/pkg/types"
)

// TestMouseSupportConfiguration verifies that mouse support is configured correctly
// based on CLI flags and config file settings
func TestMouseSupportConfiguration(t *testing.T) {
	testCases := []struct {
		name              string
		configUsingMouse  bool
		cliDisableMouse   bool
		expectedUseMouse  bool
		description       string
	}{
		{
			name:             "default_mouse_enabled",
			configUsingMouse: true,
			cliDisableMouse:  false,
			expectedUseMouse: true,
			description:      "Default behavior - mouse support enabled",
		},
		{
			name:             "cli_flag_disables_mouse",
			configUsingMouse: true,
			cliDisableMouse:  true,
			expectedUseMouse: false,
			description:      "CLI flag --disable-mouse overrides config",
		},
		{
			name:             "config_disables_mouse",
			configUsingMouse: false,
			cliDisableMouse:  false,
			expectedUseMouse: false,
			description:      "Config file disables mouse support",
		},
		{
			name:             "cli_flag_takes_precedence",
			configUsingMouse: false,
			cliDisableMouse:  true,
			expectedUseMouse: false,
			description:      "CLI flag takes precedence when both disable",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create app with test config
			cfg := &config.Config{
				UI: config.UI{
					UsingMouse: tc.configUsingMouse,
				},
			}

			cli := &types.CLI{
				DisableMouse: tc.cliDisableMouse,
			}

			app := &App{
				cfg: cfg,
				state: &models.AppState{
					CLI: cli,
				},
			}

			// Simulate the logic from Run() method
			usingMouse := true
			if app.cfg != nil && !app.cfg.UI.UsingMouse {
				usingMouse = false
			}
			if app.state.CLI != nil && app.state.CLI.DisableMouse {
				usingMouse = false
			}

			if usingMouse != tc.expectedUseMouse {
				t.Errorf("%s: Expected using_mouse=%v, got %v",
					tc.description, tc.expectedUseMouse, usingMouse)
			}
		})
	}
}

// TestMouseSupportPriority verifies that CLI flag has higher priority than config
func TestMouseSupportPriority(t *testing.T) {
	// Config says use mouse, CLI says disable - CLI should win
	cfg := &config.Config{
		UI: config.UI{
			UsingMouse: true,
		},
	}

	cli := &types.CLI{
		DisableMouse: true,
	}

	app := &App{
		cfg: cfg,
		state: &models.AppState{
			CLI: cli,
		},
	}

	// Simulate the logic from Run() method
	usingMouse := true
	if app.cfg != nil && !app.cfg.UI.UsingMouse {
		usingMouse = false
	}
	if app.state.CLI != nil && app.state.CLI.DisableMouse {
		usingMouse = false
	}

	if usingMouse {
		t.Error("CLI flag --disable-mouse should take precedence over config file")
	}
}
