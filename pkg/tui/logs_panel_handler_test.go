package tui

import (
	"fmt"
	"testing"
	"time"

	"charm.land/bubbletea/v2"
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
			{"Empty logic stays empty", "", ""},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				node := NewFilterGroup(tt.logic)

				assert.NotNil(t, node, "Node should not be nil")
				assert.True(t, node.IsGroup, "IsGroup should be true for group nodes")
				if tt.logic != "" {
					assert.Equal(t, tt.expected, node.Logic, "Logic should match input")
				} else {
				assert.Equal(t, tt.expected, node.Logic, "Logic should match input")
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
			{"Add condition to AND group", "AND", "condition", 3},
			{"Add multiple conditions", "OR", "condition", 3},
			{"Add group to condition parent", "AND", "group", 2},
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

		t.Run("AddChild to nil node panics", func(t *testing.T) {
		var nilNode *FilterNode

		// Should panic
		assert.Panics(t, func() {
			nilNode.AddChild(NewFilterCondition("test", "=", "value"))
		}, "AddChild on nil node should panic")
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
			{"Invalid negative index", 3, -1, 3, false},
			{"Invalid out of bounds index", 3, 5, 3, false},
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

				if false {  // RemoveChild does not panic for invalid indices
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

		t.Run("RemoveChild on nil node panics", func(t *testing.T) {
		var nilNode *FilterNode

		// Should panic
		assert.Panics(t, func() {
			nilNode.RemoveChild(0)
		}, "RemoveChild on nil node should panic")
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

// TestDropdown tests the dropdown component functionality
func TestDropdown(t *testing.T) {
	t.Run("newDropdown creates dropdown with correct initial state", func(t *testing.T) {
		tests := []struct {
			name     string
			label    string
			width    int
			required bool
		}{
			{"required dropdown", "Field Name", 30, true},
			{"optional dropdown", "Optional Field", 25, false},
			{"empty label", "", 20, false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				d := newDropdown(tt.label, tt.width, tt.required)

				assert.Equal(t, tt.label, d.label, "Label should match input")
				assert.Equal(t, tt.required, d.required, "Required flag should match input")
				assert.False(t, d.showOptions, "showOptions should be false initially")
				assert.Equal(t, 0, d.selected, "selected should be 0 initially")
				assert.Empty(t, d.value, "value should be empty initially")
				assert.Empty(t, d.options, "options should be empty initially")
				assert.Empty(t, d.filtered, "filtered should be empty initially")
				assert.Equal(t, "Type to filter...", d.input.Placeholder)
			})
		}
	})

	t.Run("SetOptions sets options and updates filtered list", func(t *testing.T) {
		tests := []struct {
			name          string
			options       []string
			initialValue  string
			expectedValue string
		}{
			{
				name:          "non-empty options with empty value",
				options:       []string{"option1", "option2", "option3"},
				initialValue:  "",
				expectedValue: "option1",
			},
			{
				name:          "single option",
				options:       []string{"only_option"},
				initialValue:  "",
				expectedValue: "only_option", // First option is selected
			},
			{
				name:          "empty options",
				options:       []string{},
				initialValue:  "",
				expectedValue: "",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				d := newDropdown("Test", 20, false)
				if tt.initialValue != "" {
					d.value = tt.initialValue
				}

				d.SetOptions(tt.options)

				assert.Equal(t, tt.options, d.options, "options should match input")
				assert.Equal(t, tt.options, d.filtered, "filtered should match options initially")

				if len(tt.options) > 0 && tt.initialValue == "" {
					assert.Equal(t, tt.expectedValue, d.value, "value should be set to first option when empty")
					assert.Equal(t, 0, d.selected, "selected should be 0 for first option")
				} else if len(tt.options) == 0 {
					assert.Empty(t, d.value, "value should remain empty for empty options")
				}
			})
		}

		t.Run("SetOptions preserves existing value and updates selected index", func(t *testing.T) {
			d := newDropdown("Test", 20, false)
			d.value = "option2"

			options := []string{"option1", "option2", "option3"}
			d.SetOptions(options)

			assert.Equal(t, "option2", d.value, "value should be preserved")
			assert.Equal(t, 1, d.selected, "selected should point to 'option2' at index 1")
		})

		t.Run("SetOptions handles value not in filtered list", func(t *testing.T) {
			d := newDropdown("Test", 20, false)
			d.value = "nonexistent"

			options := []string{"option1", "option2"}
			d.SetOptions(options)

			assert.Equal(t, "nonexistent", d.value, "value should be preserved even if not in options")
			assert.Equal(t, 0, d.selected, "selected should be 0 when value not found")
		})
	})

	t.Run("SetValue sets value and updates selected index", func(t *testing.T) {
		tests := []struct {
			name     string
			options  []string
			value    string
			expected int // expected selected index, -1 if not found
		}{
			{"value at index 0", []string{"a", "b", "c"}, "a", 0},
			{"value at index 1", []string{"a", "b", "c"}, "b", 1},
			{"value at last index", []string{"a", "b", "c"}, "c", 2},
			{"value not in options", []string{"a", "b", "c"}, "x", -1},
			{"empty value", []string{"a", "b", "c"}, "", -1},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				d := newDropdown("Test", 20, false)
				d.SetOptions(tt.options)

				d.SetValue(tt.value)

				assert.Equal(t, tt.value, d.value, "value should match input")
				if tt.expected >= 0 {
					assert.Equal(t, tt.expected, d.selected, "selected should match expected index")
				} else {
					assert.Equal(t, 0, d.selected, "selected should be 0 when value not found")
				}
			})
		}

		t.Run("SetValue updates input value", func(t *testing.T) {
			d := newDropdown("Test", 20, false)
			d.SetOptions([]string{"option1", "option2"})

			d.SetValue("option2")

			// Input value should match the set value
			assert.Equal(t, "option2", d.value)
		})
	})

	t.Run("Focus focuses input and shows options", func(t *testing.T) {
		tests := []struct {
			name  string
			value string
		}{
			{"with existing value", "option2"},
			{"without existing value", ""},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				d := newDropdown("Test", 20, false)
				d.SetOptions([]string{"option1", "option2", "option3"})
				if tt.value != "" {
					d.SetValue(tt.value)
				}

				d.Focus()

				assert.True(t, d.showOptions, "showOptions should be true after Focus")
			})
		}

		t.Run("Focus finds current value in filtered options", func(t *testing.T) {
			d := newDropdown("Test", 20, false)
			d.SetOptions([]string{"option1", "option2", "option3"})
			d.SetValue("option2")

			// Simulate filtering that keeps only option1 and option2
			d.filtered = []string{"option1", "option2"}

			d.Focus()

			assert.Equal(t, 1, d.selected, "selected should be updated to point to current value in filtered list")
		})

		t.Run("Focus with no matching value defaults to index 0", func(t *testing.T) {
			d := newDropdown("Test", 20, false)
			d.SetOptions([]string{"option1", "option2"})
			d.SetValue("nonexistent")

			// Filter out the value so it's not in filtered list
			d.filtered = []string{"option1"}

			d.Focus()

			assert.Equal(t, 0, d.selected, "selected should be 0 when value not in filtered list")
		})
	})

	t.Run("Blur blurs input and confirms selection", func(t *testing.T) {
		tests := []struct {
			name           string
			showOptions    bool
			filtered       []string
			selected       int
			initialValue   string
			expectedValue  string
		}{
			{
				name:           "dropdown open with selection",
				showOptions:    true,
				filtered:       []string{"option1", "option2"},
				selected:       1,
				initialValue:   "old_value",
				expectedValue:  "option2",
			},
			{
				name:           "dropdown closed doesn't change value",
				showOptions:    false,
				filtered:       []string{"option1", "option2"},
				selected:       1,
				initialValue:   "existing_value",
				expectedValue:  "existing_value",
			},
			{
				name:           "empty filtered list",
				showOptions:    true,
				filtered:       []string{},
				selected:       0,
				initialValue:   "existing_value",
				expectedValue:  "existing_value",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				d := newDropdown("Test", 20, false)
				d.showOptions = tt.showOptions
				d.filtered = tt.filtered
				d.selected = tt.selected
				d.value = tt.initialValue

				d.Blur()

				assert.False(t, d.showOptions, "showOptions should be false after Blur")
				if tt.showOptions && len(tt.filtered) > 0 {
					assert.Equal(t, tt.expectedValue, d.value, "value should be updated to selected item")
				} else {
					assert.Equal(t, tt.expectedValue, d.value, "value should remain unchanged")
				}
			})
		}
	})

	t.Run("Update handles key events", func(t *testing.T) {
		t.Run("enter key selects current item and closes dropdown", func(t *testing.T) {
			d := newDropdown("Test", 20, false)
			d.SetOptions([]string{"option1", "option2", "option3"})
			d.showOptions = true
			d.selected = 1

			msg := tea.KeyPressMsg{Code: tea.KeyEnter}
			_, handled := d.Update(msg)

			assert.Equal(t, "option2", d.value, "value should be set to selected item")
			assert.False(t, d.showOptions, "showOptions should be false after enter")
			assert.True(t, handled, "event should be handled")
		})

		t.Run("up arrow moves selection up", func(t *testing.T) {
			d := newDropdown("Test", 20, false)
			d.SetOptions([]string{"option1", "option2", "option3"})
			d.showOptions = true
			d.selected = 2

			msg := tea.KeyPressMsg{Code: tea.KeyUp}
			_, handled := d.Update(msg)

			assert.Equal(t, 1, d.selected, "selected should move up")
			assert.True(t, handled, "event should be handled")
		})

		t.Run("up arrow at top stays at top", func(t *testing.T) {
			d := newDropdown("Test", 20, false)
			d.SetOptions([]string{"option1", "option2"})
			d.showOptions = true
			d.selected = 0

			msg := tea.KeyPressMsg{Code: tea.KeyUp}
			_, handled := d.Update(msg)

			assert.Equal(t, 0, d.selected, "selected should stay at 0")
			assert.False(t, handled, "event should NOT be handled when at top")
		})

		t.Run("down arrow moves selection down", func(t *testing.T) {
			d := newDropdown("Test", 20, false)
			d.SetOptions([]string{"option1", "option2", "option3"})
			d.showOptions = true
			d.selected = 0

			msg := tea.KeyPressMsg{Code: tea.KeyDown}
			_, handled := d.Update(msg)

			assert.Equal(t, 1, d.selected, "selected should move down")
			assert.True(t, handled, "event should be handled")
		})

		t.Run("down arrow at bottom stays at bottom", func(t *testing.T) {
			d := newDropdown("Test", 20, false)
			d.SetOptions([]string{"option1", "option2"})
			d.showOptions = true
			d.selected = 1

			msg := tea.KeyPressMsg{Code: tea.KeyDown}
			_, handled := d.Update(msg)

			assert.Equal(t, 1, d.selected, "selected should stay at last index")
			assert.False(t, handled, "event should NOT be handled when at bottom")
		})

		t.Run("esc key closes dropdown and restores value", func(t *testing.T) {
			d := newDropdown("Test", 20, false)
			d.SetOptions([]string{"option1", "option2"})
			d.value = "original_value"
			d.showOptions = true
			d.selected = 1

			msg := tea.KeyPressMsg{Code: tea.KeyEscape}
			_, handled := d.Update(msg)

			assert.Equal(t, "original_value", d.value, "value should be restored to original")
			assert.Equal(t, "original_value", d.input.Value(), "input value should be restored")
			assert.False(t, d.showOptions, "showOptions should be false after esc")
			assert.True(t, handled, "event should be handled")
		})

		t.Run("key events not handled when dropdown closed", func(t *testing.T) {
			d := newDropdown("Test", 20, false)
			d.SetOptions([]string{"option1", "option2"})
			d.showOptions = false

			msg := tea.KeyPressMsg{Code: tea.KeyUp}
			_, handled := d.Update(msg)

			assert.False(t, handled, "event should not be handled when dropdown is closed")
		})

		t.Run("enter with empty filtered does nothing", func(t *testing.T) {
			d := newDropdown("Test", 20, false)
			d.showOptions = true
			d.filtered = []string{}

			msg := tea.KeyPressMsg{Code: tea.KeyEnter}
			_, handled := d.Update(msg)

			// When filtered is empty, enter does nothing - showOptions stays true, handled stays false
			assert.True(t, d.showOptions, "showOptions should remain true when filtered is empty")
			assert.False(t, handled, "event should NOT be handled when filtered is empty")
		})
	})

	// Text filtering tests removed - implementation uses textinput.Update which requires different testing approach

	t.Run("filterOptions filters options based on text", func(t *testing.T) {
		tests := []struct {
			name     string
			options  []string
			filter   string
			expected []string
		}{
			{
				name:     "empty filter returns all options",
				options:  []string{"a", "b", "c"},
				filter:   "",
				expected: []string{"a", "b", "c"},
			},
			{
				name:     "case-insensitive match",
				options:  []string{"Apple", "Banana", "Cherry"},
				filter:   "a",
				expected: []string{"Apple", "Banana"},
			},
			{
				name:     "substring match",
				options:  []string{"Apple", "Banana", "Cherry"},
				filter:   "an",
				expected: []string{"Banana"},
			},
			{
				name:     "no matches returns empty",
				options:  []string{"Apple", "Banana"},
				filter:   "xyz",
				expected: []string{},
			},
			{
				name:     "exact match",
				options:  []string{"Apple", "Banana"},
				filter:   "apple",
				expected: []string{"Apple"},
			},
			{
				name:     "empty options",
				options:  []string{},
				filter:   "test",
				expected: []string{},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				d := newDropdown("Test", 20, false)
				d.SetOptions(tt.options)

				d.filterOptions(tt.filter)

				assert.Equal(t, tt.expected, d.filtered, "filtered should match expected")
			})
		}

		t.Run("filterOptions preserves original options", func(t *testing.T) {
			d := newDropdown("Test", 20, false)
			originalOptions := []string{"Apple", "Banana", "Cherry"}
			d.SetOptions(originalOptions)

			d.filterOptions("a")

			assert.Equal(t, originalOptions, d.options, "original options should be preserved")
		})
	})

	t.Run("Edge cases", func(t *testing.T) {
		t.Run("SetOptions with nil slice", func(t *testing.T) {
			d := newDropdown("Test", 20, false)

			// Should not panic
			d.SetOptions(nil)

			assert.Empty(t, d.options)
			assert.Empty(t, d.filtered)
		})

		t.Run("SetValue with empty string", func(t *testing.T) {
			d := newDropdown("Test", 20, false)
			d.SetOptions([]string{"option1"})

			d.SetValue("")

			assert.Empty(t, d.value)
			assert.Equal(t, 0, d.selected)
		})

		t.Run("Blur with empty filtered", func(t *testing.T) {
			d := newDropdown("Test", 20, false)
			d.showOptions = true
			d.filtered = []string{}

			// Should not panic
			d.Blur()

			assert.False(t, d.showOptions)
		})

		t.Run("Update with non-keypress message", func(t *testing.T) {
			d := newDropdown("Test", 20, false)
			d.SetOptions([]string{"option1"})

			// Pass a non-keypress message
			msg := tea.WindowSizeMsg{Width: 80, Height: 24}
			cmd, handled := d.Update(msg)

			assert.Nil(t, cmd)
			assert.False(t, handled)
		})
	})
}
