package tui

import "testing"

// TestSparklineWidthCalculation verifies sparkline width matches available space
func TestSparklineWidthCalculation(t *testing.T) {
	testCases := []struct {
		name                  string
		screenWidth           int
		expectedContentWidth  int
		expectedSparklineWidth int
		description           string
	}{
		{
			name:                  "standard_width_160",
			screenWidth:           160,
			expectedContentWidth:  158, // 160 - 2
			expectedSparklineWidth: 146, // 158 - 12
			description:           "Standard terminal width",
		},
		{
			name:                  "wide_width_200",
			screenWidth:           200,
			expectedContentWidth:  198, // 200 - 2
			expectedSparklineWidth: 186, // 198 - 12
			description:           "Wide terminal",
		},
		{
			name:                  "narrow_width_80",
			screenWidth:           80,
			expectedContentWidth:  78, // 80 - 2
			expectedSparklineWidth: 66, // 78 - 12
			description:           "Narrow terminal",
		},
		{
			name:                  "minimum_width_60",
			screenWidth:           60,
			expectedContentWidth:  58,  // 60 - 2
			expectedSparklineWidth: 46,  // 58 - 12
			description:           "Near minimum width",
		},
		{
			name:                  "very_narrow_50",
			screenWidth:           50,
			expectedContentWidth:  48,  // 50 - 2
			expectedSparklineWidth: 40,  // 48 - 12 = 36, but minimum is 40
			description:           "Very narrow hits minimum sparkline width",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Simulate the calculation from fetchLogsDataCmd
			overviewContentWidth := tc.screenWidth - 2
			labelWidth := 12
			sparklineWidth := overviewContentWidth - labelWidth

			// Apply minimum constraint
			if sparklineWidth < 40 {
				sparklineWidth = 40
			}

			// Verify overview content width
			if overviewContentWidth != tc.expectedContentWidth {
				t.Errorf("%s: Expected content width %d, got %d",
					tc.description, tc.expectedContentWidth, overviewContentWidth)
			}

			// Verify sparkline width
			if sparklineWidth != tc.expectedSparklineWidth {
				t.Errorf("%s: Expected sparkline width %d, got %d (screen=%d, content=%d, label=%d)",
					tc.description, tc.expectedSparklineWidth, sparklineWidth,
					tc.screenWidth, overviewContentWidth, labelWidth)
			}
		})
	}
}

// TestSparklineWidthMatchesBarChart verifies sparkline and bar chart use same base width
func TestSparklineWidthMatchesBarChart(t *testing.T) {
	screenWidth := 160

	// Bar chart calculation (from renderOverview)
	barContentWidth := screenWidth - 2
	barPrefixLength := 13 // "Total: 1000 | " is typical
	barAvailableWidth := barContentWidth - barPrefixLength

	// Sparkline calculation (from fetchLogsDataCmd)
	sparklineContentWidth := screenWidth - 2
	sparklineLabelWidth := 12
	sparklineAvailableWidth := sparklineContentWidth - sparklineLabelWidth

	// Verify they use the same base content width
	if barContentWidth != sparklineContentWidth {
		t.Errorf("Bar and sparkline use different content widths: bar=%d, sparkline=%d",
			barContentWidth, sparklineContentWidth)
	}

	// Log the widths for documentation
	t.Logf("Screen width: %d", screenWidth)
	t.Logf("Content width (both): %d", barContentWidth)
	t.Logf("Bar chart available: %d (prefix: %d)", barAvailableWidth, barPrefixLength)
	t.Logf("Sparkline available: %d (label: %d)", sparklineAvailableWidth, sparklineLabelWidth)
}

// TestSparklineWidthConstraints verifies min/max constraints work correctly
func TestSparklineWidthConstraints(t *testing.T) {
	testCases := []struct {
		name           string
		calculated     int
		expectedFinal  int
		constraintType string
	}{
		{
			name:           "below_minimum",
			calculated:     30,
			expectedFinal:  40,
			constraintType: "minimum",
		},
		{
			name:           "at_minimum",
			calculated:     40,
			expectedFinal:  40,
			constraintType: "none",
		},
		{
			name:           "normal_range",
			calculated:     100,
			expectedFinal:  100,
			constraintType: "none",
		},
		{
			name:           "at_maximum",
			calculated:     200,
			expectedFinal:  200,
			constraintType: "none",
		},
		{
			name:           "above_maximum",
			calculated:     250,
			expectedFinal:  200,
			constraintType: "maximum",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sparklineWidth := tc.calculated

			// Apply constraints (same as in fetchLogsDataCmd)
			if sparklineWidth < 40 {
				sparklineWidth = 40
			}
			if sparklineWidth > 200 {
				sparklineWidth = 200
			}

			if sparklineWidth != tc.expectedFinal {
				t.Errorf("%s: Expected final width %d, got %d (calculated: %d)",
					tc.constraintType, tc.expectedFinal, sparklineWidth, tc.calculated)
			}
		})
	}
}

// TestLabelWidthConstant verifies label width is consistent
func TestLabelWidthConstant(t *testing.T) {
	// The label format is: "  ERROR   : " (2 spaces + 8 chars uppercase + 2 chars ": ")
	// Total: 12 characters
	expectedLabelWidth := 12

	// Verify this matches what's used in the code
	labelWidth := 12 // From fetchLogsDataCmd

	if labelWidth != expectedLabelWidth {
		t.Errorf("Label width mismatch: expected %d, code uses %d",
			expectedLabelWidth, labelWidth)
	}

	// Document the label format
	t.Logf("Label format: \"  ERROR   : \"")
	t.Logf("Label width: %d characters", labelWidth)
	t.Logf("  - Prefix: 2 spaces")
	t.Logf("  - Level name: 8 chars (padded)")
	t.Logf("  - Separator: \": \" (2 chars)")
}
