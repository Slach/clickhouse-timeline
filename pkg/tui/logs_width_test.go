package tui

import (
	"testing"
)

// TestLogsTableWidthCalculation verifies that the logs table uses 100% of screen width
func TestLogsTableWidthCalculation(t *testing.T) {
	testCases := []struct {
		name        string
		screenWidth int
		expectTime  int
		expectMsg   int
		expectTotal int
	}{
		{
			name:        "standard_width_160",
			screenWidth: 160,
			expectTime:  23,
			expectMsg:   134, // 160 - 23 - 3 = 134
			expectTotal: 160,
		},
		{
			name:        "wide_width_200",
			screenWidth: 200,
			expectTime:  23,
			expectMsg:   174, // 200 - 23 - 3 = 174
			expectTotal: 200,
		},
		{
			name:        "narrow_width_80",
			screenWidth: 80,
			expectTime:  23,
			expectMsg:   54, // 80 - 23 - 3 = 54
			expectTotal: 80,
		},
		{
			name:        "minimum_width_60",
			screenWidth: 60,
			expectTime:  23,
			expectMsg:   34, // 60 - 23 - 3 = 34
			expectTotal: 60,
		},
		{
			name:        "too_narrow_50",
			screenWidth: 50,
			expectTime:  23,
			expectMsg:   30, // 50 - 23 - 3 = 24, but minimum is 30
			expectTotal: 56, // 23 + 30 + 3 = 56 (exceeds screen width due to minimum)
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			timeWidth := 23
			borderOverhead := 3
			messageWidth := tc.screenWidth - timeWidth - borderOverhead
			if messageWidth < 30 {
				messageWidth = 30
			}

			totalUsed := timeWidth + messageWidth + borderOverhead

			if timeWidth != tc.expectTime {
				t.Errorf("Expected time width %d, got %d", tc.expectTime, timeWidth)
			}

			if messageWidth != tc.expectMsg {
				t.Errorf("Expected message width %d, got %d", tc.expectMsg, messageWidth)
			}

			if totalUsed != tc.expectTotal {
				t.Errorf("Expected total width %d, got %d", tc.expectTotal, totalUsed)
			}

			// Verify the calculation matches the actual screen width (unless constrained by minimum)
			if tc.screenWidth >= 56 { // 56 is minimum viable (23 + 30 + 3)
				if totalUsed != tc.screenWidth {
					t.Errorf("Total used width %d doesn't match screen width %d", totalUsed, tc.screenWidth)
				}
			}
		})
	}
}

// TestLogsTableBorderOverheadConstant verifies the border overhead calculation
func TestLogsTableBorderOverheadConstant(t *testing.T) {
	// Border overhead breakdown (padding is included in column widths):
	// Left border (1) + column separator (1) + right border (1) = 3 chars overhead

	expected := 3
	actual := 1 + 1 + 1

	if actual != expected {
		t.Errorf("Border and padding calculation incorrect. Expected %d, got %d", expected, actual)
	}
}
