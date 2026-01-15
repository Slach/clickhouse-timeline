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

// ==================== FilterNode Tree Tests ====================

// TestNewFilterGroup tests creating a new filter group
func TestNewFilterGroup(t *testing.T) {
	group := NewFilterGroup("AND")

	if !group.IsGroup {
		t.Errorf("NewFilterGroup() IsGroup = false, want true")
	}
	if group.Logic != "AND" {
		t.Errorf("NewFilterGroup() Logic = %s, want AND", group.Logic)
	}
	if len(group.Children) != 0 {
		t.Errorf("NewFilterGroup() Children length = %d, want 0", len(group.Children))
	}
}

// TestNewFilterCondition tests creating a new filter condition
func TestNewFilterCondition(t *testing.T) {
	cond := NewFilterCondition("level", "=", "Error")

	if cond.IsGroup {
		t.Errorf("NewFilterCondition() IsGroup = true, want false")
	}
	if cond.Field != "level" {
		t.Errorf("NewFilterCondition() Field = %s, want level", cond.Field)
	}
	if cond.Operator != "=" {
		t.Errorf("NewFilterCondition() Operator = %s, want =", cond.Operator)
	}
	if cond.Value != "Error" {
		t.Errorf("NewFilterCondition() Value = %s, want Error", cond.Value)
	}
}

// TestFilterNodeAddChild tests adding children to a group
func TestFilterNodeAddChild(t *testing.T) {
	group := NewFilterGroup("OR")
	cond1 := NewFilterCondition("level", "=", "Error")
	cond2 := NewFilterCondition("level", "=", "Warning")

	group.AddChild(cond1)
	group.AddChild(cond2)

	if len(group.Children) != 2 {
		t.Errorf("AddChild() Children length = %d, want 2", len(group.Children))
	}
	if group.Children[0].Field != "level" {
		t.Errorf("AddChild() first child Field = %s, want level", group.Children[0].Field)
	}
	if group.Children[0].Value != "Error" {
		t.Errorf("AddChild() first child Value = %s, want Error", group.Children[0].Value)
	}
}

// TestPathEqual tests path comparison function
func TestPathEqual(t *testing.T) {
	tests := []struct {
		name     string
		pathA    []int
		pathB    []int
		expected bool
	}{
		{
			name:     "empty paths equal",
			pathA:    []int{},
			pathB:    []int{},
			expected: true,
		},
		{
			name:     "single element equal",
			pathA:    []int{0},
			pathB:    []int{0},
			expected: true,
		},
		{
			name:     "multiple elements equal",
			pathA:    []int{0, 1, 2},
			pathB:    []int{0, 1, 2},
			expected: true,
		},
		{
			name:     "different length",
			pathA:    []int{0, 1},
			pathB:    []int{0, 1, 2},
			expected: false,
		},
		{
			name:     "different values",
			pathA:    []int{0, 1},
			pathB:    []int{0, 2},
			expected: false,
		},
		{
			name:     "nil vs empty",
			pathA:    nil,
			pathB:    []int{},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pathEqual(tt.pathA, tt.pathB)
			if result != tt.expected {
				t.Errorf("pathEqual(%v, %v) = %v, want %v", tt.pathA, tt.pathB, result, tt.expected)
			}
		})
	}
}

// TestGetParentPath tests parent path computation
func TestGetParentPath(t *testing.T) {
	tests := []struct {
		name     string
		path     []int
		expected []int
	}{
		{
			name:     "empty path",
			path:     []int{},
			expected: nil,
		},
		{
			name:     "single element",
			path:     []int{0},
			expected: []int{},
		},
		{
			name:     "multiple elements",
			path:     []int{0, 1, 2},
			expected: []int{0, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getParentPath(tt.path)
			if !pathEqual(result, tt.expected) {
				t.Errorf("getParentPath(%v) = %v, want %v", tt.path, result, tt.expected)
			}
		})
	}
}

// TestFlattenTree tests tree flattening for navigation
func TestFlattenTree(t *testing.T) {
	// Build a tree:
	// root (AND)
	//   ├── condition1 (level = Error)
	//   ├── subgroup (OR)
	//   │   ├── condition2 (host LIKE prod)
	//   │   └── condition3 (host LIKE stage)
	//   └── condition4 (message LIKE SELECT)

	root := NewFilterGroup("AND")
	cond1 := NewFilterCondition("level", "=", "Error")
	subgroup := NewFilterGroup("OR")
	cond2 := NewFilterCondition("host", "LIKE", "prod")
	cond3 := NewFilterCondition("host", "LIKE", "stage")
	cond4 := NewFilterCondition("message", "LIKE", "SELECT")

	subgroup.AddChild(cond2)
	subgroup.AddChild(cond3)

	root.AddChild(cond1)
	root.AddChild(subgroup)
	root.AddChild(cond4)

	flat := flattenTree(root, []int{})

	// Expected order (depth-first):
	// 0: root []
	// 1: cond1 [0]
	// 2: subgroup [1]
	// 3: cond2 [1, 0]
	// 4: cond3 [1, 1]
	// 5: cond4 [2]

	if len(flat) != 6 {
		t.Errorf("flattenTree() returned %d nodes, want 6", len(flat))
	}

	expectedPaths := [][]int{
		{},
		{0},
		{1},
		{1, 0},
		{1, 1},
		{2},
	}

	for i, expected := range expectedPaths {
		if i >= len(flat) {
			break
		}
		if !pathEqual(flat[i].Path, expected) {
			t.Errorf("flattenTree()[%d].Path = %v, want %v", i, flat[i].Path, expected)
		}
	}
}

// TestBuildWhereFromTree_SimpleAnd tests simple AND group SQL generation
func TestBuildWhereFromTree_SimpleAnd(t *testing.T) {
	root := NewFilterGroup("AND")
	root.AddChild(NewFilterCondition("level", "=", "Error"))
	root.AddChild(NewFilterCondition("message", "LIKE", "timeout"))

	sql, args := buildWhereFromTree(root)

	expectedSQL := "(`level` = ? AND `message` LIKE ?)"
	if sql != expectedSQL {
		t.Errorf("buildWhereFromTree() SQL = %s, want %s", sql, expectedSQL)
	}
	if len(args) != 2 {
		t.Errorf("buildWhereFromTree() args length = %d, want 2", len(args))
	}
}

// TestBuildWhereFromTree_SimpleOr tests simple OR group SQL generation
func TestBuildWhereFromTree_SimpleOr(t *testing.T) {
	root := NewFilterGroup("OR")
	root.AddChild(NewFilterCondition("level", "=", "Error"))
	root.AddChild(NewFilterCondition("level", "=", "Warning"))

	sql, args := buildWhereFromTree(root)

	expectedSQL := "(`level` = ? OR `level` = ?)"
	if sql != expectedSQL {
		t.Errorf("buildWhereFromTree() SQL = %s, want %s", sql, expectedSQL)
	}
	if len(args) != 2 {
		t.Errorf("buildWhereFromTree() args length = %d, want 2", len(args))
	}
}

// TestBuildWhereFromTree_NotAnd tests NOT AND group SQL generation
func TestBuildWhereFromTree_NotAnd(t *testing.T) {
	root := NewFilterGroup("NOT AND")
	root.AddChild(NewFilterCondition("level", "=", "Debug"))
	root.AddChild(NewFilterCondition("level", "=", "Trace"))

	sql, args := buildWhereFromTree(root)

	expectedSQL := "(NOT (`level` = ?) AND NOT (`level` = ?))"
	if sql != expectedSQL {
		t.Errorf("buildWhereFromTree() SQL = %s, want %s", sql, expectedSQL)
	}
	if len(args) != 2 {
		t.Errorf("buildWhereFromTree() args length = %d, want 2", len(args))
	}
}

// TestBuildWhereFromTree_NotOr tests NOT OR group SQL generation
func TestBuildWhereFromTree_NotOr(t *testing.T) {
	root := NewFilterGroup("NOT OR")
	root.AddChild(NewFilterCondition("host", "LIKE", "prod"))
	root.AddChild(NewFilterCondition("host", "LIKE", "stage"))

	sql, args := buildWhereFromTree(root)

	expectedSQL := "(NOT (`host` LIKE ?) OR NOT (`host` LIKE ?))"
	if sql != expectedSQL {
		t.Errorf("buildWhereFromTree() SQL = %s, want %s", sql, expectedSQL)
	}
	if len(args) != 2 {
		t.Errorf("buildWhereFromTree() args length = %d, want 2", len(args))
	}
}

// TestBuildWhereFromTree_NestedGroups tests nested group SQL generation
func TestBuildWhereFromTree_NestedGroups(t *testing.T) {
	// (level = 'Error' AND (host LIKE 'prod' OR host LIKE 'stage'))
	root := NewFilterGroup("AND")
	root.AddChild(NewFilterCondition("level", "=", "Error"))

	subgroup := NewFilterGroup("OR")
	subgroup.AddChild(NewFilterCondition("host", "LIKE", "prod"))
	subgroup.AddChild(NewFilterCondition("host", "LIKE", "stage"))
	root.AddChild(subgroup)

	sql, args := buildWhereFromTree(root)

	expectedSQL := "(`level` = ? AND (`host` LIKE ? OR `host` LIKE ?))"
	if sql != expectedSQL {
		t.Errorf("buildWhereFromTree() SQL = %s, want %s", sql, expectedSQL)
	}
	if len(args) != 3 {
		t.Errorf("buildWhereFromTree() args length = %d, want 3", len(args))
	}
}

// TestBuildWhereFromTree_EmptyGroup tests empty group returns empty result
func TestBuildWhereFromTree_EmptyGroup(t *testing.T) {
	root := NewFilterGroup("AND")

	sql, args := buildWhereFromTree(root)

	if sql != "" {
		t.Errorf("buildWhereFromTree() SQL = %s, want empty string", sql)
	}
	if len(args) != 0 {
		t.Errorf("buildWhereFromTree() args length = %d, want 0", len(args))
	}
}

// TestBuildWhereFromTree_SingleCondition tests single condition in group
func TestBuildWhereFromTree_SingleCondition(t *testing.T) {
	root := NewFilterGroup("AND")
	root.AddChild(NewFilterCondition("level", "=", "Error"))

	sql, args := buildWhereFromTree(root)

	expectedSQL := "`level` = ?"
	if sql != expectedSQL {
		t.Errorf("buildWhereFromTree() SQL = %s, want %s", sql, expectedSQL)
	}
	if len(args) != 1 {
		t.Errorf("buildWhereFromTree() args length = %d, want 1", len(args))
	}
}

// TestBuildWhereFromTree_NilRoot tests nil root returns empty result
func TestBuildWhereFromTree_NilRoot(t *testing.T) {
	sql, args := buildWhereFromTree(nil)

	if sql != "" {
		t.Errorf("buildWhereFromTree(nil) SQL = %s, want empty string", sql)
	}
	if len(args) != 0 {
		t.Errorf("buildWhereFromTree(nil) args length = %d, want 0", len(args))
	}
}

// TestBuildWhereFromTree_IsNullOperator tests IS NULL operator
func TestBuildWhereFromTree_IsNullOperator(t *testing.T) {
	root := NewFilterGroup("AND")
	root.AddChild(NewFilterCondition("error_code", "IS NULL", ""))

	sql, args := buildWhereFromTree(root)

	expectedSQL := "`error_code` IS NULL"
	if sql != expectedSQL {
		t.Errorf("buildWhereFromTree() SQL = %s, want %s", sql, expectedSQL)
	}
	if len(args) != 0 {
		t.Errorf("buildWhereFromTree() args length = %d, want 0", len(args))
	}
}

// TestBuildWhereFromTree_IsNotNullOperator tests IS NOT NULL operator
func TestBuildWhereFromTree_IsNotNullOperator(t *testing.T) {
	root := NewFilterGroup("AND")
	root.AddChild(NewFilterCondition("error_code", "IS NOT NULL", ""))

	sql, args := buildWhereFromTree(root)

	expectedSQL := "`error_code` IS NOT NULL"
	if sql != expectedSQL {
		t.Errorf("buildWhereFromTree() SQL = %s, want %s", sql, expectedSQL)
	}
	if len(args) != 0 {
		t.Errorf("buildWhereFromTree() args length = %d, want 0", len(args))
	}
}

// TestFilterNode_CycleLogic tests cycling through logic options
func TestFilterNode_CycleLogic(t *testing.T) {
	group := NewFilterGroup("AND")

	// AND -> OR
	group.CycleLogic()
	if group.Logic != "OR" {
		t.Errorf("CycleLogic() from AND = %s, want OR", group.Logic)
	}

	// OR -> NOT AND
	group.CycleLogic()
	if group.Logic != "NOT AND" {
		t.Errorf("CycleLogic() from OR = %s, want NOT AND", group.Logic)
	}

	// NOT AND -> NOT OR
	group.CycleLogic()
	if group.Logic != "NOT OR" {
		t.Errorf("CycleLogic() from NOT AND = %s, want NOT OR", group.Logic)
	}

	// NOT OR -> AND
	group.CycleLogic()
	if group.Logic != "AND" {
		t.Errorf("CycleLogic() from NOT OR = %s, want AND", group.Logic)
	}
}
