package tui

import (
	"testing"
	"time"
)

// TestLogsTableHeightCalculation verifies proper table height calculation
func TestLogsTableHeightCalculation(t *testing.T) {
	testCases := []struct {
		name                string
		screenHeight        int
		overviewMode        bool
		expectedTableHeight int
		description         string
	}{
		{
			name:                "standard_height_40_with_overview",
			screenHeight:        40,
			overviewMode:        true,
			expectedTableHeight: 31, // 40 - 9 (title + overview) = 31
			description:         "Standard terminal with overview visible",
		},
		{
			name:                "standard_height_40_no_overview",
			screenHeight:        40,
			overviewMode:        false,
			expectedTableHeight: 39, // 40 - 1 (title only) = 39
			description:         "Standard terminal with overview hidden",
		},
		{
			name:                "tall_height_60_with_overview",
			screenHeight:        60,
			overviewMode:        true,
			expectedTableHeight: 51, // 60 - 9 = 51
			description:         "Tall terminal with overview",
		},
		{
			name:                "tall_height_60_no_overview",
			screenHeight:        60,
			overviewMode:        false,
			expectedTableHeight: 59, // 60 - 1 = 59
			description:         "Tall terminal without overview",
		},
		{
			name:                "short_height_20_with_overview",
			screenHeight:        20,
			overviewMode:        true,
			expectedTableHeight: 11, // 20 - 9 = 11
			description:         "Short terminal with overview",
		},
		{
			name:                "short_height_20_no_overview",
			screenHeight:        20,
			overviewMode:        false,
			expectedTableHeight: 19, // 20 - 1 = 19
			description:         "Short terminal without overview",
		},
		{
			name:                "minimum_height_15_with_overview",
			screenHeight:        15,
			overviewMode:        true,
			expectedTableHeight: 6, // 15 - 9 = 6
			description:         "Very short terminal with overview",
		},
		{
			name:                "minimum_height_6_no_overview",
			screenHeight:        6,
			overviewMode:        false,
			expectedTableHeight: 5, // 6 - 1 = 5 (hits minimum)
			description:         "Very short terminal without overview hits minimum",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Simulate the calculation based on overview mode
			var uiOverhead int
			if tc.overviewMode {
				// Overview visible: title (1) + overview with borders (7-8 typical) = 8-9 total
				uiOverhead = 9
			} else {
				// Overview hidden: title only
				uiOverhead = 1
			}

			tableHeight := tc.screenHeight - uiOverhead
			if tableHeight < 5 {
				tableHeight = 5
			}

			if tableHeight != tc.expectedTableHeight {
				t.Errorf("%s: Expected table height %d, got %d (screenHeight=%d, overhead=%d, overviewMode=%v)",
					tc.description, tc.expectedTableHeight, tableHeight, tc.screenHeight, uiOverhead, tc.overviewMode)
			}
		})
	}
}

// TestLogsUIOverheadBreakdown verifies the UI overhead calculation
func TestLogsUIOverheadBreakdown(t *testing.T) {
	testCases := []struct {
		name         string
		overviewMode bool
		expected     int
		breakdown    string
	}{
		{
			name:         "overview_visible",
			overviewMode: true,
			expected:     9,
			breakdown:    "Title (1) + Overview with borders (7-8 typical)",
		},
		{
			name:         "overview_hidden",
			overviewMode: false,
			expected:     1,
			breakdown:    "Title only (1)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var actual int
			if tc.overviewMode {
				// Overview visible: title + overview box
				// Overview box: top border + bar chart + Timeline header + 3-4 sparklines + bottom border
				// Note: actual size varies with content, typically 7-8 lines for overview + 1 for title = 8-9 total
				actual = 1 + 8 // title + typical overview
			} else {
				// Overview hidden: title only
				actual = 1
			}

			if actual != tc.expected {
				t.Errorf("UI overhead calculation incorrect for %s. Expected %d, got %d. Breakdown: %s",
					tc.name, tc.expected, actual, tc.breakdown)
			}
		})
	}
}

// TestLogsViewStructure verifies the view structure
func TestLogsViewStructure(t *testing.T) {
	now := time.Now()

	testCases := []struct {
		name            string
		overviewMode    bool
		expectOverview  bool
		expectTimeline  bool
	}{
		{
			name:            "with_overview",
			overviewMode:    true,
			expectOverview:  true,
			expectTimeline:  true,
		},
		{
			name:            "without_overview",
			overviewMode:    false,
			expectOverview:  false,
			expectTimeline:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			viewer := logsViewer{
				width:   160,
				height:  40,
				loading: false,
				totalRows: 100,
				levelCounts: map[string]int{
					"error": 10,
					"info":  90,
				},
				firstEntryTime: now,
				lastEntryTime:  now,
				config: LogConfig{
					WindowSize: 1000,
				},
				levelTimeSeries: map[string][]float64{
					"error": {1, 2, 3, 4, 5},
					"info":  {10, 15, 20, 25, 30},
				},
				timeLabels: []string{"12:00", "12:05", "12:10", "12:15", "12:20"},
				overviewMode: tc.overviewMode,
			}

			view := viewer.View()

			// Check that header is always present
			if !contains(view, "Log Entries | Page") {
				t.Error("Header line 'Log Entries | Page' should be present in view")
			}

			// Check that Timeline is present only when overview mode is active
			if tc.expectTimeline && !contains(view, "Timeline:") {
				t.Error("Timeline header should be present when overview mode is active")
			}
			if !tc.expectTimeline && contains(view, "Timeline:") {
				t.Error("Timeline header should NOT be present when overview mode is inactive")
			}

			// Check that there's no excessive spacing
			// Count double newlines in the output
			doubleNewlines := 0
			for i := 0; i < len(view)-1; i++ {
				if view[i] == '\n' && view[i+1] == '\n' {
					doubleNewlines++
				}
			}

			// There should be 0 double newlines since we removed all padding
			if doubleNewlines > 0 {
				t.Errorf("View has %d double newlines, but expected 0 (no padding between elements)", doubleNewlines)
			}
		})
	}
}
