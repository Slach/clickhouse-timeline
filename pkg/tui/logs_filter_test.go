package tui

import (
	"testing"
)

// TestBuildWhereClause tests the WHERE clause building function
func TestBuildWhereClause(t *testing.T) {
	tests := []struct {
		name             string
		filters          []LogFilter
		expectedClause   string
		expectedArgsLen  int
		expectedFirstArg interface{}
	}{
		{
			name:            "no filters",
			filters:         []LogFilter{},
			expectedClause:  "",
			expectedArgsLen: 0,
		},
		{
			name: "single equals filter",
			filters: []LogFilter{
				{Field: "level", Operator: "=", Value: "Error"},
			},
			expectedClause:   "`level` = ?",
			expectedArgsLen:  1,
			expectedFirstArg: "Error",
		},
		{
			name: "single LIKE filter",
			filters: []LogFilter{
				{Field: "message", Operator: "LIKE", Value: "timeout"},
			},
			expectedClause:   "`message` LIKE ?",
			expectedArgsLen:  1,
			expectedFirstArg: "%timeout%",
		},
		{
			name: "single NOT LIKE filter",
			filters: []LogFilter{
				{Field: "message", Operator: "NOT LIKE", Value: "success"},
			},
			expectedClause:   "`message` NOT LIKE ?",
			expectedArgsLen:  1,
			expectedFirstArg: "%success%",
		},
		{
			name: "single greater than filter",
			filters: []LogFilter{
				{Field: "response_time", Operator: ">", Value: "1000"},
			},
			expectedClause:   "`response_time` > ?",
			expectedArgsLen:  1,
			expectedFirstArg: "1000",
		},
		{
			name: "multiple filters with AND",
			filters: []LogFilter{
				{Field: "level", Operator: "=", Value: "Error"},
				{Field: "message", Operator: "LIKE", Value: "timeout"},
			},
			expectedClause:   "`level` = ? AND `message` LIKE ?",
			expectedArgsLen:  2,
			expectedFirstArg: "Error",
		},
		{
			name: "filter with field name containing backticks",
			filters: []LogFilter{
				{Field: "field`with`ticks", Operator: "=", Value: "value"},
			},
			expectedClause:   "`field``with``ticks` = ?",
			expectedArgsLen:  1,
			expectedFirstArg: "value",
		},
		{
			name: "multiple operators",
			filters: []LogFilter{
				{Field: "status_code", Operator: ">=", Value: "400"},
				{Field: "status_code", Operator: "<", Value: "500"},
				{Field: "path", Operator: "!=", Value: "/health"},
			},
			expectedClause:   "`status_code` >= ? AND `status_code` < ? AND `path` != ?",
			expectedArgsLen:  3,
			expectedFirstArg: "400",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause, args := buildWhereClause(tt.filters)

			if clause != tt.expectedClause {
				t.Errorf("buildWhereClause() clause = %v, want %v", clause, tt.expectedClause)
			}

			if len(args) != tt.expectedArgsLen {
				t.Errorf("buildWhereClause() args length = %d, want %d", len(args), tt.expectedArgsLen)
			}

			if tt.expectedArgsLen > 0 && args[0] != tt.expectedFirstArg {
				t.Errorf("buildWhereClause() first arg = %v, want %v", args[0], tt.expectedFirstArg)
			}
		})
	}
}

// TestDropdownFilterOptions tests the dropdown filtering logic
func TestDropdownFilterOptions(t *testing.T) {
	dd := dropdown{
		options: []string{"event_time", "event_date", "level", "message", "thread_id", "query_id"},
	}

	tests := []struct {
		name           string
		filter         string
		expectedCount  int
		expectedFirst  string
	}{
		{
			name:          "empty filter returns all",
			filter:        "",
			expectedCount: 6,
			expectedFirst: "event_time",
		},
		{
			name:          "filter 'event' returns matching",
			filter:        "event",
			expectedCount: 2,
			expectedFirst: "event_time",
		},
		{
			name:          "filter 'id' returns matching",
			filter:        "id",
			expectedCount: 2,
			expectedFirst: "thread_id",
		},
		{
			name:          "filter 'TIME' case insensitive",
			filter:        "TIME",
			expectedCount: 1,
			expectedFirst: "event_time",
		},
		{
			name:          "filter 'xyz' returns none",
			filter:        "xyz",
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dd.filterOptions(tt.filter)

			if len(dd.filtered) != tt.expectedCount {
				t.Errorf("filterOptions() count = %d, want %d", len(dd.filtered), tt.expectedCount)
			}

			if tt.expectedCount > 0 && dd.filtered[0] != tt.expectedFirst {
				t.Errorf("filterOptions() first = %s, want %s", dd.filtered[0], tt.expectedFirst)
			}
		})
	}
}

// TestDropdownSetValue tests setting dropdown value
func TestDropdownSetValue(t *testing.T) {
	dd := dropdown{
		options: []string{"level", "message", "thread_id"},
	}
	dd.filterOptions("") // Initialize filtered

	dd.SetValue("message")

	if dd.value != "message" {
		t.Errorf("SetValue() value = %s, want message", dd.value)
	}

	if dd.selected != 1 {
		t.Errorf("SetValue() selected = %d, want 1", dd.selected)
	}
}

// TestLogFilterValidation tests filter validation logic
func TestLogFilterValidation(t *testing.T) {
	tests := []struct {
		name   string
		field  string
		op     string
		value  string
		valid  bool
	}{
		{
			name:  "all fields present",
			field: "level",
			op:    "=",
			value: "Error",
			valid: true,
		},
		{
			name:  "missing field",
			field: "",
			op:    "=",
			value: "Error",
			valid: false,
		},
		{
			name:  "missing operator",
			field: "level",
			op:    "",
			value: "Error",
			valid: false,
		},
		{
			name:  "missing value",
			field: "level",
			op:    "=",
			value: "",
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the validation logic from handleFilterFormKey
			valid := tt.field != "" && tt.op != "" && tt.value != ""

			if valid != tt.valid {
				t.Errorf("filter validation = %v, want %v", valid, tt.valid)
			}
		})
	}
}

// TestLogFilterRemoval tests filter removal logic
func TestLogFilterRemoval(t *testing.T) {
	filters := []LogFilter{
		{Field: "level", Operator: "=", Value: "Error"},
		{Field: "message", Operator: "LIKE", Value: "timeout"},
		{Field: "thread_id", Operator: ">", Value: "100"},
	}

	// Remove middle filter (index 1)
	removeIdx := 1
	filters = append(filters[:removeIdx], filters[removeIdx+1:]...)

	if len(filters) != 2 {
		t.Errorf("after removal, length = %d, want 2", len(filters))
	}

	if filters[0].Field != "level" {
		t.Errorf("first filter field = %s, want level", filters[0].Field)
	}

	if filters[1].Field != "thread_id" {
		t.Errorf("second filter field = %s, want thread_id", filters[1].Field)
	}
}

// TestBuildWhereClauseEscaping tests SQL injection prevention
func TestBuildWhereClauseEscaping(t *testing.T) {
	tests := []struct {
		name          string
		field         string
		expectedField string
	}{
		{
			name:          "normal field",
			field:         "level",
			expectedField: "`level`",
		},
		{
			name:          "field with backticks",
			field:         "field`test",
			expectedField: "`field``test`",
		},
		{
			name:          "field with multiple backticks",
			field:         "a`b`c",
			expectedField: "`a``b``c`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filters := []LogFilter{
				{Field: tt.field, Operator: "=", Value: "test"},
			}

			clause, _ := buildWhereClause(filters)

			if clause != tt.expectedField+" = ?" {
				t.Errorf("buildWhereClause() clause = %v, want %v = ?", clause, tt.expectedField)
			}
		})
	}
}

// TestDropdownNavigation tests dropdown up/down navigation
func TestDropdownNavigation(t *testing.T) {
	dd := dropdown{
		options:     []string{"opt1", "opt2", "opt3", "opt4"},
		selected:    0,
		showOptions: true,
	}
	dd.filterOptions("") // Initialize filtered

	// Navigate down
	dd.selected++
	if dd.selected != 1 {
		t.Errorf("after down, selected = %d, want 1", dd.selected)
	}

	// Navigate down again
	dd.selected++
	if dd.selected != 2 {
		t.Errorf("after down, selected = %d, want 2", dd.selected)
	}

	// Navigate up
	dd.selected--
	if dd.selected != 1 {
		t.Errorf("after up, selected = %d, want 1", dd.selected)
	}

	// Try to navigate down past end (should be clamped by UI logic)
	dd.selected = len(dd.filtered) - 1
	// UI prevents going beyond len(filtered)-1
	nextSelected := dd.selected + 1
	if nextSelected < len(dd.filtered) {
		dd.selected = nextSelected
	}
	if dd.selected != len(dd.filtered)-1 {
		t.Errorf("at end, selected = %d, want %d", dd.selected, len(dd.filtered)-1)
	}
}
