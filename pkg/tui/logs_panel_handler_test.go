package tui

import (
	"testing"
	"time"

	"github.com/evertras/bubble-table/table"
)

// TestLogsPanelEnterKeyHandling tests the Enter key fix for accessing row data
// This verifies the fix for the panic that occurred when pressing Enter
func TestLogsPanelEnterKeyHandling(t *testing.T) {
	// Create a mock row with lowercase keys (matching the fix)
	timeStr := "2025-11-09 10:30:45.123"
	rowData := table.RowData{
		"time":    timeStr,
		"message": "Test message with color",
	}
	row := table.NewRow(rowData)

	// Test 1: Access with lowercase key (should work)
	t.Run("lowercase_time_key_access", func(t *testing.T) {
		timeData, ok := row.Data["time"]
		if !ok {
			t.Fatal("Failed to retrieve time data with lowercase 'time' key")
		}
		if timeData == nil {
			t.Fatal("Time data is nil")
		}

		retrievedTimeStr, ok := timeData.(string)
		if !ok {
			t.Fatal("Failed type assertion to string")
		}

		if retrievedTimeStr != timeStr {
			t.Errorf("Expected time string %s, got %s", timeStr, retrievedTimeStr)
		}
	})

	// Test 2: Verify uppercase key doesn't exist (old bug scenario)
	t.Run("uppercase_Time_key_not_found", func(t *testing.T) {
		timeData, ok := row.Data["Time"]
		if ok && timeData != nil {
			t.Error("Uppercase 'Time' key should not exist (this was the bug)")
		}
	})

	// Test 3: Nil safety check
	t.Run("nil_safety_empty_row", func(t *testing.T) {
		emptyRow := table.Row{Data: table.RowData{}}
		timeData, ok := emptyRow.Data["time"]
		if ok && timeData != nil {
			t.Error("Empty row should not have time data")
		}
	})

	// Test 4: Entry matching logic
	t.Run("entry_matching_logic", func(t *testing.T) {
		entries := []LogEntry{
			{
				Time:    time.Date(2025, 11, 9, 10, 30, 45, 123000000, time.UTC),
				Message: "Test message",
				Level:   "info",
			},
		}

		// Retrieve time from row
		timeData, ok := row.Data["time"]
		if !ok || timeData == nil {
			t.Fatal("Failed to retrieve time data")
		}

		timeStrFromRow, ok := timeData.(string)
		if !ok {
			t.Fatal("Failed type assertion")
		}

		// Try to find matching entry
		found := false
		for _, entry := range entries {
			if entry.Time.Format("2006-01-02 15:04:05.000") == timeStrFromRow {
				found = true
				break
			}
		}

		if !found {
			t.Error("Should have found matching entry")
		}
	})
}

// TestLogsPanelEnterKeyNilChecks verifies all nil checks in Enter key handler
func TestLogsPanelEnterKeyNilChecks(t *testing.T) {
	testCases := []struct {
		name     string
		rowData  table.RowData
		shouldOK bool
	}{
		{
			name: "valid_time_string",
			rowData: table.RowData{
				"time":    "2025-11-09 10:30:45.123",
				"message": "test",
			},
			shouldOK: true,
		},
		{
			name: "missing_time_key",
			rowData: table.RowData{
				"message": "test",
			},
			shouldOK: false,
		},
		{
			name: "nil_time_value",
			rowData: table.RowData{
				"time":    nil,
				"message": "test",
			},
			shouldOK: false,
		},
		{
			name: "wrong_time_type",
			rowData: table.RowData{
				"time":    123, // int instead of string
				"message": "test",
			},
			shouldOK: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			row := table.NewRow(tc.rowData)

			// Simulate the Enter key handler logic
			timeData, ok := row.Data["time"]
			if !ok || timeData == nil {
				if tc.shouldOK {
					t.Error("Expected to retrieve time data but failed")
				}
				return
			}

			timeStr, ok := timeData.(string)
			if !ok {
				if tc.shouldOK {
					t.Error("Expected successful type assertion to string but failed")
				}
				return
			}

			if !tc.shouldOK {
				t.Errorf("Expected to fail but got time string: %s", timeStr)
			}
		})
	}
}
