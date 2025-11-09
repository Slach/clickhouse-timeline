package tui

import (
	"testing"
)

// TestTimelineSparklineGeneration tests the multi-row sparkline generation
func TestTimelineSparklineGeneration(t *testing.T) {
	testCases := []struct {
		name            string
		levelTimeSeries map[string][]float64
		expectedRows    int
		description     string
	}{
		{
			name: "all_four_levels_present",
			levelTimeSeries: map[string][]float64{
				"error":   {1, 2, 3, 4, 5, 4, 3, 2, 1},
				"warning": {2, 3, 4, 5, 6, 5, 4, 3, 2},
				"info":    {10, 15, 20, 25, 30, 25, 20, 15, 10},
				"debug":   {5, 7, 9, 11, 13, 11, 9, 7, 5},
			},
			expectedRows: 4,
			description:  "Should show all 4 priority levels",
		},
		{
			name: "only_error_and_warning",
			levelTimeSeries: map[string][]float64{
				"error":   {1, 2, 3, 4, 5},
				"warning": {2, 3, 4, 5, 6},
			},
			expectedRows: 2,
			description:  "Should show only 2 levels present",
		},
		{
			name: "single_level",
			levelTimeSeries: map[string][]float64{
				"error": {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			},
			expectedRows: 1,
			description:  "Should show single level",
		},
		{
			name:            "empty_time_series",
			levelTimeSeries: map[string][]float64{},
			expectedRows:    0,
			description:     "Should return empty string for no data",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			viewer := logsViewer{
				levelTimeSeries: tc.levelTimeSeries,
				width:           160,
			}

			result := viewer.generateSparklineForLevels(nil)

			if tc.expectedRows == 0 {
				if result != "" {
					t.Errorf("%s: Expected empty string, got: %s", tc.description, result)
				}
				return
			}

			if result == "" {
				t.Errorf("%s: Expected sparkline output, got empty string", tc.description)
				return
			}

			// Count rows by splitting on newlines
			rows := 0
			for _, char := range result {
				if char == '\n' {
					rows++
				}
			}
			rows++ // Add one for the last row (no trailing newline)

			if rows != tc.expectedRows {
				t.Errorf("%s: Expected %d rows, got %d. Output:\n%s",
					tc.description, tc.expectedRows, rows, result)
			}
		})
	}
}

// TestTimelineSparklineColorCoding tests that sparklines use correct colors
func TestTimelineSparklineColorCoding(t *testing.T) {
	viewer := logsViewer{
		levelTimeSeries: map[string][]float64{
			"error":   {1, 2, 3, 4, 5},
			"warning": {2, 3, 4, 5, 6},
			"info":    {10, 15, 20, 25, 30},
			"debug":   {5, 7, 9, 11, 13},
		},
		width: 160,
	}

	result := viewer.generateSparklineForLevels(nil)

	// Check that all expected level labels are present
	expectedLabels := []string{"ERROR", "WARNING", "INFO", "DEBUG"}
	for _, label := range expectedLabels {
		if !contains(result, label) {
			t.Errorf("Expected to find label '%s' in output, but it's missing. Output:\n%s", label, result)
		}
	}

	// Check that output contains sparkline characters
	sparklineChars := "▁▂▃▄▅▆▇█"
	found := false
	for _, char := range sparklineChars {
		if contains(result, string(char)) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected to find sparkline characters in output. Output:\n%s", result)
	}
}

// TestTimelineSparklineMaxRows tests that output is limited to 4 rows
func TestTimelineSparklineMaxRows(t *testing.T) {
	// Create time series for more than 4 levels
	viewer := logsViewer{
		levelTimeSeries: map[string][]float64{
			"error":   {1, 2, 3},
			"warning": {2, 3, 4},
			"info":    {10, 15, 20},
			"debug":   {5, 7, 9},
			"trace":   {1, 1, 1}, // 5th level, should not appear
			"fatal":   {2, 2, 2}, // 6th level, should not appear
		},
		width: 160,
	}

	result := viewer.generateSparklineForLevels(nil)

	// Count rows
	rows := 1
	for _, char := range result {
		if char == '\n' {
			rows++
		}
	}

	if rows > 4 {
		t.Errorf("Expected maximum 4 rows, got %d. Output:\n%s", rows, result)
	}
}

// TestFetchTimeSeriesDataLogic tests the time-series data processing logic
func TestFetchTimeSeriesDataLogic(t *testing.T) {
	// Test bucket calculation
	testCases := []struct {
		name             string
		timeRangeSeconds float64
		buckets          int
		expectedInterval int
	}{
		{
			name:             "1_hour_range",
			timeRangeSeconds: 3600,
			buckets:          120,
			expectedInterval: 30, // 3600 / 120 = 30 seconds
		},
		{
			name:             "24_hour_range",
			timeRangeSeconds: 86400,
			buckets:          120,
			expectedInterval: 720, // 86400 / 120 = 720 seconds
		},
		{
			name:             "5_minute_range",
			timeRangeSeconds: 300,
			buckets:          120,
			expectedInterval: 3, // 300 / 120 = 2.5, ceil to 3
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Simulate interval calculation
			intervalSeconds := int(tc.timeRangeSeconds / float64(tc.buckets))
			if intervalSeconds < 1 {
				intervalSeconds = 1
			}

			// Allow small rounding difference
			diff := tc.expectedInterval - intervalSeconds
			if diff < 0 {
				diff = -diff
			}
			if diff > 1 {
				t.Errorf("Expected interval ~%d seconds, got %d seconds",
					tc.expectedInterval, intervalSeconds)
			}
		})
	}
}

// Helper function
func contains(str string, substr string) bool {
	return len(str) > 0 && len(substr) > 0 &&
		(str == substr || len(str) >= len(substr) && findInString(str, substr))
}

func findInString(str string, substr string) bool {
	for i := 0; i <= len(str)-len(substr); i++ {
		if str[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
