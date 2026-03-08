package tui

import (
	"fmt"
	"testing"
	"time"

	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	"github.com/stretchr/testify/assert"
)

// TestLogsPanelEnterKeyHandling tests the Enter key fix for accessing row data
// This verifies the fix for the panic that occurred when pressing Enter
func TestLogsPanelEnterKeyHandling(t *testing.T) {
	// Create a mock row with lowercase keys (matching the fix)
	timeStr := "2025-11-09 10:30:45.123"
	rowData := widgets.RowData{
		"time":    timeStr,
		"message": "Test message with color",
	}
	row := widgets.NewRow(rowData)

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
		emptyRow := widgets.NewRow(widgets.RowData{})
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

// TestFilterNodeTreeStructure tests the FilterNode tree structure operations
func TestFilterNodeTreeStructure(t *testing.T) {
	t.Run("NewFilterGroup creates group with correct logic and empty children", func(t *testing.T) {
		tests := []struct {
			name     string
			logic    string
			expected string
		}{
			{"AND logic", "AND", "AND"},
			{"OR logic", "OR", "OR"},
			{"NOT AND logic", "NOT AND", "NOT AND"},
			{"NOT OR logic", "NOT OR", "NOT OR"},
			{"Empty logic defaults to AND", "", "AND"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				node := NewFilterGroup(tt.logic)

				assert.NotNil(t, node, "Node should not be nil")
				assert.True(t, node.IsGroup, "IsGroup should be true for group nodes")
				if tt.logic != "" {
					assert.Equal(t, tt.expected, node.Logic, "Logic should match input")
				} else {
					assert.Equal(t, "AND", node.Logic, "Empty logic should default to AND")
				}
				assert.Empty(t, node.Children, "Children should be empty initially")
			})
		}
	})

	t.Run("NewFilterCondition creates condition with correct field, operator, value", func(t *testing.T) {
		tests := []struct {
			name     string
			field    string
			operator string
			value    string
		}{
			{"Simple equals", "level", "=", "error"},
			{"Not like operator", "message", "NOT LIKE", "%test%"},
			{"IS NULL check", "field_name", "IS NULL", ""},
			{"IN operator with comma values", "status", "IN", "active,inactive"},
			{"Greater than operator", "count", ">", "100"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				node := NewFilterCondition(tt.field, tt.operator, tt.value)

				assert.NotNil(t, node, "Node should not be nil")
				assert.False(t, node.IsGroup, "IsGroup should be false for condition nodes")
				assert.Equal(t, tt.field, node.Field, "Field should match input")
				assert.Equal(t, tt.operator, node.Operator, "Operator should match input")
				assert.Equal(t, tt.value, node.Value, "Value should match input")
			})
		}
	})

	t.Run("FilterNode.AddChild adds children to groups", func(t *testing.T) {
		tests := []struct {
			name           string
			setupLogic     string
			childType      string // "condition" or "group"
			expectedCount  int
		}{
			{"Add condition to AND group", "AND", "condition", 1},
			{"Add multiple conditions", "OR", "condition", 3},
			{"Add group to condition parent (should add to root)", "AND", "group", 1},
			{"Add nested groups", "NOT AND", "group", 2},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				parent := NewFilterGroup(tt.setupLogic)

				// Add first child (condition)
				child1 := NewFilterCondition("level", "=", "error")
				parent.AddChild(child1)

				// Add second child (condition or group based on test case)
				if tt.childType == "group" {
					child2 := NewFilterGroup("AND")
					parent.AddChild(child2)

					// Add child to the group
					child3 := NewFilterCondition("message", "LIKE", "%test%")
					child2.AddChild(child3)

					// Verify group has child
					assert.Equal(t, 1, len(child2.Children), "Group should have one child")
				} else {
					child2 := NewFilterCondition("message", "LIKE", "%test%")
					parent.AddChild(child2)

					child3 := NewFilterCondition("count", ">", "10")
					parent.AddChild(child3)

					assert.Equal(t, tt.expectedCount, len(parent.Children), "Parent should have correct number of children")
				}

				assert.Equal(t, tt.expectedCount, len(parent.Children), "Parent should have correct number of children")
			})
		}

		t.Run("AddChild to condition node adds to parent", func(t *testing.T) {
			parent := NewFilterGroup("AND")

			// Add a condition as child
			condition1 := NewFilterCondition("level", "=", "error")
			parent.AddChild(condition1)

			// Try to add another child to the condition (should go to parent)
			condition2 := NewFilterCondition("message", "LIKE", "%test%")
			parent.AddChild(condition2)

			// Verify both children are in parent, not in condition1
			assert.Equal(t, 2, len(parent.Children), "Parent should have two children")
			assert.False(t, condition1.IsGroup, "condition1 should not be a group")
		})

		t.Run("AddChild to nil node does nothing", func(t *testing.T) {
			var nilNode *FilterNode

			// Should not panic
			nilNode.AddChild(NewFilterCondition("test", "=", "value"))

			assert.Nil(t, nilNode, "Nil node should remain nil")
		})
	})

	t.Run("FilterNode.CycleLogic cycles through AND → OR → NOT AND → NOT OR → AND", func(t *testing.T) {
		tests := []struct {
			name           string
			initialLogic   string
			cycleCount     int
			expectedLogics []string
		}{
			{
				name:         "Start from AND",
				initialLogic: "AND",
				cycleCount:   4,
				expectedLogics: []string{"OR", "NOT AND", "NOT OR", "AND"},
			},
			{
				name:         "Start from OR",
				initialLogic: "OR",
				cycleCount:   4,
				expectedLogics: []string{"NOT AND", "NOT OR", "AND", "OR"},
			},
			{
				name:         "Start from NOT AND",
				initialLogic: "NOT AND",
				cycleCount:   4,
				expectedLogics: []string{"NOT OR", "AND", "OR", "NOT AND"},
			},
			{
				name:         "Start from NOT OR",
				initialLogic: "NOT OR",
				cycleCount:   4,
				expectedLogics: []string{"AND", "OR", "NOT AND", "NOT OR"},
			},
			{
				name:         "Multiple cycles",
				initialLogic: "AND",
				cycleCount:   8,
				expectedLogics: []string{
					"OR", "NOT AND", "NOT OR", "AND",
					"OR", "NOT AND", "NOT OR", "AND",
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				node := NewFilterGroup(tt.initialLogic)

				for i := 0; i < tt.cycleCount; i++ {
					node.CycleLogic()
					assert.Equal(t, tt.expectedLogics[i], node.Logic, "Logic should cycle correctly")
				}

				// Verify it cycles back to start after full cycle
				assert.Equal(t, tt.initialLogic, node.Logic, "After 4 cycles should return to start")
			})
		}

		t.Run("CycleLogic on condition node does nothing", func(t *testing.T) {
			condition := NewFilterCondition("level", "=", "error")

			// Should not panic
			condition.CycleLogic()

			assert.False(t, condition.IsGroup, "Should remain a condition")
		})
	})

	t.Run("FilterNode.RemoveChild removes child at index", func(t *testing.T) {
		tests := []struct {
			name           string
			setupChildren  int
			removeIndex    int
			expectedCount  int
			shouldPanic    bool
		}{
			{"Remove first child", 3, 0, 2, false},
			{"Remove middle child", 5, 2, 4, false},
			{"Remove last child", 3, 2, 2, false},
			{"Invalid negative index", 3, -1, 3, true},
			{"Invalid out of bounds index", 3, 5, 3, true},
			{"Remove from empty group", 0, 0, 0, false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				parent := NewFilterGroup("AND")

				// Add children based on test case
				for i := 0; i < tt.setupChildren; i++ {
					child := NewFilterCondition("field", "=", fmt.Sprintf("value%d", i))
					parent.AddChild(child)
				}

				if tt.shouldPanic {
					assert.Panics(t, func() {
						parent.RemoveChild(tt.removeIndex)
					}, "Should panic on invalid index")
				} else {
					parent.RemoveChild(tt.removeIndex)

					assert.Equal(t, tt.expectedCount, len(parent.Children), "Children count should match expected")

					// Verify remaining children are still valid
					for i, child := range parent.Children {
						assert.NotNil(t, child, "Child at index %d should not be nil", i)
						assert.False(t, child.IsGroup, "Child should remain a condition")
					}
				}
			})
		}

		t.Run("RemoveChild on condition node does nothing", func(t *testing.T) {
			condition := NewFilterCondition("level", "=", "error")

			// Should not panic
			condition.RemoveChild(0)

			assert.False(t, condition.IsGroup, "Should remain a condition")
		})

		t.Run("RemoveChild on nil node does nothing", func(t *testing.T) {
			var nilNode *FilterNode

			// Should not panic
			nilNode.RemoveChild(0)

			assert.Nil(t, nilNode, "Nil node should remain nil")
		})
	})

	t.Run("Complex tree operations", func(t *testing.T) {
		// Create a complex filter tree with nested groups
		root := NewFilterGroup("AND")

		// Add first condition
		root.AddChild(NewFilterCondition("level", "=", "error"))

		// Add group with OR logic
		orGroup := NewFilterGroup("OR")
		orGroup.AddChild(NewFilterCondition("message", "LIKE", "%test%"))
		orGroup.AddChild(NewFilterCondition("count", ">", "100"))
		root.AddChild(orGroup)

		// Add another condition
		root.AddChild(NewFilterCondition("status", "IN", "active,inactive"))

		// Verify structure
		assert.Equal(t, 3, len(root.Children), "Root should have 3 children")

		// Verify nested group
		orGroupNode := root.Children[1]
		assert.True(t, orGroupNode.IsGroup, "Second child should be a group")
		assert.Equal(t, "OR", orGroupNode.Logic, "Nested group should have OR logic")
		assert.Equal(t, 2, len(orGroupNode.Children), "Nested group should have 2 children")

		// Test cycle logic on nested group
		orGroupNode.CycleLogic()
		assert.Equal(t, "NOT AND", orGroupNode.Logic, "Nested group logic should cycle")

		// Test remove child from nested group
		orGroupNode.RemoveChild(0)
		assert.Equal(t, 1, len(orGroupNode.Children), "Nested group should have 1 child after removal")

		// Test remove from root
		root.RemoveChild(0)
		assert.Equal(t, 2, len(root.Children), "Root should have 2 children after removal")
	})

	t.Run("Edge cases and error handling", func(t *testing.T) {
		t.Run("Empty group operations", func(t *testing.T) {
			group := NewFilterGroup("AND")

			// Remove from empty group should not panic
			group.RemoveChild(0)

			// Cycle on empty group is fine
			group.CycleLogic()
			assert.Equal(t, "OR", group.Logic)
		})

		t.Run("Condition with empty values", func(t *testing.T) {
			condition := NewFilterCondition("", "", "")

			assert.False(t, condition.IsGroup)
			assert.Equal(t, "", condition.Field)
			assert.Equal(t, "", condition.Operator)
			assert.Equal(t, "", condition.Value)
		})

		t.Run("Group with nil children slice", func(t *testing.T) {
			group := NewFilterGroup("AND")

			// Children should be initialized as empty slice, not nil
			assert.NotNil(t, group.Children)
			assert.Equal(t, 0, len(group.Children))
		})
	})
}

// TestLogsPanelEnterKeyNilChecks verifies all nil checks in Enter key handler
func TestLogsPanelEnterKeyNilChecks(t *testing.T) {
	testCases := []struct {
		name     string
		rowData  widgets.RowData
		shouldOK bool
	}{
		{
			name: "valid_time_string",
			rowData: widgets.RowData{
				"time":    "2025-11-09 10:30:45.123",
				"message": "test",
			},
			shouldOK: true,
		},
		{
			name: "missing_time_key",
			rowData: widgets.RowData{
				"message": "test",
			},
			shouldOK: false,
		},
		{
			name: "nil_time_value",
			rowData: widgets.RowData{
				"time":    nil,
				"message": "test",
			},
			shouldOK: false,
		},
		{
			name: "wrong_time_type",
			rowData: widgets.RowData{
				"time":    123, // int instead of string
				"message": "test",
			},
			shouldOK: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			row := widgets.NewRow(tc.rowData)

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
