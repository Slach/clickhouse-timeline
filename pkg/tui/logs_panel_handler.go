package tui

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/evertras/bubble-table/table"
	"github.com/rs/zerolog/log"
)

// LogEntry represents a single log entry
type LogEntry struct {
	Time      time.Time
	Message   string
	Level     string
	AllFields map[string]interface{}
}

// LogFilter represents a filter condition (kept for buildFilterCondition compatibility)
type LogFilter struct {
	Field    string
	Operator string
	Value    string // empty for IS NULL / IS NOT NULL
}

// FilterNode represents either a condition or a group (recursive tree structure)
// This is the new DevExpress-style filter builder structure
type FilterNode struct {
	IsGroup bool // true = group, false = condition

	// For conditions (IsGroup = false):
	Field    string
	Operator string
	Value    string

	// For groups (IsGroup = true):
	Logic    string        // "AND", "OR", "NOT AND", "NOT OR"
	Children []*FilterNode // Nested nodes (conditions or groups)
}

// NewFilterGroup creates a new group node with specified logic
func NewFilterGroup(logic string) *FilterNode {
	return &FilterNode{
		IsGroup:  true,
		Logic:    logic,
		Children: []*FilterNode{},
	}
}

// NewFilterCondition creates a new condition node
func NewFilterCondition(field, operator, value string) *FilterNode {
	return &FilterNode{
		IsGroup:  false,
		Field:    field,
		Operator: operator,
		Value:    value,
	}
}

// AddChild adds a child node to a group
func (n *FilterNode) AddChild(child *FilterNode) {
	if n.IsGroup {
		n.Children = append(n.Children, child)
	}
}

// CycleLogic cycles through logic options: AND -> OR -> NOT AND -> NOT OR -> AND
func (n *FilterNode) CycleLogic() {
	if !n.IsGroup {
		return
	}
	switch n.Logic {
	case "AND":
		n.Logic = "OR"
	case "OR":
		n.Logic = "NOT AND"
	case "NOT AND":
		n.Logic = "NOT OR"
	case "NOT OR":
		n.Logic = "AND"
	default:
		n.Logic = "AND"
	}
}

// RemoveChild removes a child at the given index
func (n *FilterNode) RemoveChild(index int) {
	if n.IsGroup && index >= 0 && index < len(n.Children) {
		n.Children = append(n.Children[:index], n.Children[index+1:]...)
	}
}

// FilterGroup represents a group of filters combined with AND or OR (legacy, to be removed)
type FilterGroup struct {
	Filters    []LogFilter
	Combinator string // "AND" or "OR" - how filters within this group are combined
}

// LogFilterGroups represents multiple filter groups (legacy, to be removed)
type LogFilterGroups struct {
	Groups          []FilterGroup
	GroupCombinator string // "AND" or "OR" - how groups are combined with each other
}

// logLevelCount is used for sorting and displaying level statistics
type logLevelCount struct {
	level string
	count int
}

// dropdown field with text input and filtering
type dropdown struct {
	label       string
	input       textinput.Model
	options     []string
	filtered    []string
	selected    int
	value       string
	showOptions bool
	required    bool
}

func newDropdown(label string, width int, required bool) dropdown {
	input := textinput.New()
	input.Width = width
	input.Placeholder = "Type to filter..."

	return dropdown{
		label:       label,
		input:       input,
		required:    required,
		showOptions: false,
		selected:    0,
	}
}

func (d *dropdown) SetOptions(options []string) {
	d.options = options
	d.filtered = options
	if len(options) > 0 && d.value == "" {
		d.value = options[0]
	}
	// Update selected index to match current value
	d.selected = 0
	if d.value != "" {
		for i, opt := range d.filtered {
			if opt == d.value {
				d.selected = i
				break
			}
		}
	}
}

func (d *dropdown) SetValue(value string) {
	d.value = value
	d.input.SetValue(value)
	// Update selected index to match the value in filtered list
	d.selected = 0
	for i, opt := range d.filtered {
		if opt == value {
			d.selected = i
			break
		}
	}
}

func (d *dropdown) Focus() {
	d.input.Focus()
	d.showOptions = true
	// Find current value in filtered options and select it
	if d.value != "" {
		for i, opt := range d.filtered {
			if opt == d.value {
				d.selected = i
				break
			}
		}
	}
}

func (d *dropdown) Blur() {
	d.input.Blur()
	// When losing focus, confirm the current selection if dropdown was open
	if d.showOptions && len(d.filtered) > 0 {
		d.value = d.filtered[d.selected]
		d.input.SetValue(d.value)
	}
	d.showOptions = false
}

func (d *dropdown) Update(msg tea.Msg) (tea.Cmd, bool) {
	var cmd tea.Cmd
	handled := false

	switch msg := msg.(type) {
	case tea.KeyMsg:
		if !d.showOptions {
			return nil, false
		}

		switch msg.String() {
		case "enter":
			if len(d.filtered) > 0 {
				d.value = d.filtered[d.selected]
				d.input.SetValue(d.value)
				d.showOptions = false
				handled = true
			}
			return nil, handled

		case "up":
			if d.selected > 0 {
				d.selected--
				handled = true
			}
			return nil, handled

		case "down":
			if d.selected < len(d.filtered)-1 {
				d.selected++
				handled = true
			}
			return nil, handled

		case "esc":
			d.showOptions = false
			d.input.SetValue(d.value)
			handled = true
			return nil, handled
		}
	}

	oldValue := d.input.Value()
	d.input, cmd = d.input.Update(msg)
	newValue := d.input.Value()

	// Filter options when text changes
	if oldValue != newValue && d.showOptions {
		d.filterOptions(newValue)
		d.selected = 0
	}

	return cmd, false
}

func (d *dropdown) filterOptions(filter string) {
	if filter == "" {
		d.filtered = d.options
		return
	}

	filter = strings.ToLower(filter)
	d.filtered = make([]string, 0)

	for _, opt := range d.options {
		if strings.Contains(strings.ToLower(opt), filter) {
			d.filtered = append(d.filtered, opt)
		}
	}
}

func (d *dropdown) View(focused bool) string {
	labelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("12"))
	if d.required {
		labelStyle = labelStyle.Bold(true)
	}

	label := labelStyle.Render(d.label + ":")

	inputStyle := lipgloss.NewStyle().
		BorderStyle(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("240"))

	if focused {
		inputStyle = inputStyle.BorderForeground(lipgloss.Color("6"))
	}

	var content string
	if d.showOptions && focused {
		// Show filtered dropdown on new line with scrolling
		var options strings.Builder
		maxShow := 5

		// Calculate scroll offset to keep selected item visible
		scrollOffset := 0
		if d.selected >= maxShow {
			// If selected is beyond visible window, scroll to center it
			scrollOffset = d.selected - maxShow/2
			if scrollOffset < 0 {
				scrollOffset = 0
			}
			// Don't scroll past the end
			if scrollOffset+maxShow > len(d.filtered) {
				scrollOffset = len(d.filtered) - maxShow
				if scrollOffset < 0 {
					scrollOffset = 0
				}
			}
		}

		// Show scroll indicator if there are items before
		if scrollOffset > 0 {
			options.WriteString(fmt.Sprintf("  ↑ %d more above\n", scrollOffset))
		}

		// Show visible window
		endIdx := scrollOffset + maxShow
		if endIdx > len(d.filtered) {
			endIdx = len(d.filtered)
		}

		for i := scrollOffset; i < endIdx; i++ {
			opt := d.filtered[i]
			if i == d.selected {
				options.WriteString(lipgloss.NewStyle().
					Foreground(lipgloss.Color("0")).
					Background(lipgloss.Color("6")).
					Render(fmt.Sprintf("▶ %s", opt)))
			} else {
				options.WriteString(fmt.Sprintf("  %s", opt))
			}
			options.WriteString("\n")
		}

		// Show scroll indicator if there are items after
		if endIdx < len(d.filtered) {
			options.WriteString(fmt.Sprintf("  ↓ %d more below\n", len(d.filtered)-endIdx))
		}

		content = lipgloss.JoinVertical(lipgloss.Left,
			label,
			inputStyle.Render(d.input.View()),
			"",
			options.String(),
		)
	} else {
		// Show compact view with current value on new line
		value := d.value
		if value == "" {
			value = lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render("<not set>")
		}
		content = lipgloss.JoinVertical(lipgloss.Left,
			label,
			inputStyle.Render(value),
		)
	}

	return content
}

// logsConfigForm is the logs configuration form
type logsConfigForm struct {
	app *App

	// Available options
	databases    []string
	tables       []string
	allFields    []string
	timeFields   []string
	timeMsFields []string
	dateFields   []string
	textFields   []string

	// Current configuration
	config LogConfig

	// Dropdowns
	dbDropdown     dropdown
	tableDropdown  dropdown
	msgDropdown    dropdown
	timeDropdown   dropdown
	timeMsDropdown dropdown
	dateDropdown   dropdown
	levelDropdown  dropdown
	windowInput    textinput.Model

	// UI state
	focusIndex    int // 0-7 for fields, 8=Show Logs, 9=Cancel
	width         int
	height        int
	loading       bool
	loadingWhat   string
	err           error
	autoSubmit    bool
	restoringData bool // True when restoring from saved config (don't autoload cascades)
}

func newLogsConfigForm(app *App, width, height int, lastConfig *LogConfig) *logsConfigForm {
	windowInput := textinput.New()
	windowInput.Placeholder = "1000"
	windowInput.SetValue("1000")
	windowInput.Width = 15

	form := &logsConfigForm{
		app:            app,
		dbDropdown:     newDropdown("Database", 30, true),
		tableDropdown:  newDropdown("Table", 30, true),
		msgDropdown:    newDropdown("Message Field", 30, true),
		timeDropdown:   newDropdown("Time Field", 30, true),
		timeMsDropdown: newDropdown("TimeMs Field", 30, false),
		dateDropdown:   newDropdown("Date Field", 30, false),
		levelDropdown:  newDropdown("Level Field", 30, false),
		windowInput:    windowInput,
		width:          width,
		height:         height,
		loading:        true,
		loadingWhat:    "databases",
		config: LogConfig{
			WindowSize: 1000,
		},
	}

	// Apply last config if available (remembers previous choices)
	if lastConfig != nil {
		form.config = *lastConfig
		form.restoringData = true // Mark that we're restoring, not making new selections
		// Set window input immediately (doesn't depend on loading data)
		if lastConfig.WindowSize > 0 {
			form.windowInput.SetValue(fmt.Sprint(lastConfig.WindowSize))
		}
		log.Debug().
			Str("database", lastConfig.Database).
			Str("table", lastConfig.Table).
			Str("message", lastConfig.MessageField).
			Str("time", lastConfig.TimeField).
			Msg("Restoring logs config from lastConfig")
		// Note: We set dropdown values after options are loaded in the message handlers
	}

	// Apply CLI parameters if available (CLI params override saved config)
	if app.state.CLI != nil {
		params := app.state.CLI.LogsParams
		if params.Database != "" {
			form.config.Database = params.Database
			form.dbDropdown.SetValue(params.Database)
		}
		if params.Table != "" {
			form.config.Table = params.Table
			form.tableDropdown.SetValue(params.Table)
		}
		if params.Message != "" {
			form.config.MessageField = params.Message
			form.msgDropdown.SetValue(params.Message)
		}
		if params.Time != "" {
			form.config.TimeField = params.Time
			form.timeDropdown.SetValue(params.Time)
		}
		if params.TimeMs != "" {
			form.config.TimeMsField = params.TimeMs
			form.timeMsDropdown.SetValue(params.TimeMs)
		}
		if params.Date != "" {
			form.config.DateField = params.Date
			form.dateDropdown.SetValue(params.Date)
		}
		if params.Level != "" {
			form.config.LevelField = params.Level
			form.levelDropdown.SetValue(params.Level)
		}
		if params.Window > 0 {
			form.config.WindowSize = params.Window
			form.windowInput.SetValue(fmt.Sprint(params.Window))
		}

		// Check if we can auto-submit
		if params.Database != "" && params.Table != "" &&
			params.Message != "" && params.Time != "" {
			form.autoSubmit = true
		}
	}

	return form
}

func (m *logsConfigForm) Init() tea.Cmd {
	m.dbDropdown.Focus()
	return tea.Batch(
		textinput.Blink,
		m.loadDatabases(),
	)
}

func (m *logsConfigForm) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height

	case DatabasesLoadedMsg:
		m.loading = false
		if msg.Err != nil {
			m.err = msg.Err
			return m, nil
		}
		m.databases = msg.Databases

		// Restore database value BEFORE setting options so SetOptions can find and select it
		if m.config.Database != "" {
			log.Debug().
				Str("database", m.config.Database).
				Bool("restoring", m.restoringData).
				Msg("Setting database dropdown value from config")
			m.dbDropdown.value = m.config.Database
			m.dbDropdown.input.SetValue(m.config.Database)
		}

		// Now set options - this will update the selected index to match the value
		m.dbDropdown.SetOptions(m.databases)

		// If we restored a database, load tables
		if m.config.Database != "" {
			// If restoring, load tables in background
			if m.restoringData {
				m.loading = true
				m.loadingWhat = "tables"
				return m, m.loadTables()
			} else {
				// User explicitly selected - auto-load tables
				return m, m.loadTables()
			}
		}
		return m, nil

	case TablesLoadedMsg:
		m.loading = false
		if msg.Err != nil {
			m.err = msg.Err
			return m, nil
		}
		m.tables = msg.Tables

		// Restore table value BEFORE setting options
		if m.config.Table != "" {
			m.tableDropdown.value = m.config.Table
			m.tableDropdown.input.SetValue(m.config.Table)
		}

		// Now set options - this will update the selected index
		m.tableDropdown.SetOptions(m.tables)

		// If we restored a table, load columns
		if m.config.Table != "" {
			// If restoring, load columns in background
			if m.restoringData {
				m.loading = true
				m.loadingWhat = "columns"
				return m, m.loadColumns()
			} else {
				// User explicitly selected - auto-load columns
				return m, m.loadColumns()
			}
		}
		return m, nil

	case ColumnsLoadedMsg:
		m.loading = false
		if msg.Err != nil {
			m.err = msg.Err
			return m, nil
		}

		m.allFields = msg.AllFields
		m.timeFields = msg.TimeFields
		m.timeMsFields = msg.TimeMsFields
		m.dateFields = msg.DateFields
		m.textFields = msg.TextFields

		// Smart field type detection for new configurations
		// Only auto-select if user hasn't already configured these fields
		if m.config.MessageField == "" && !m.restoringData {
			// Preferred field names for message (in priority order)
			preferredMessageNames := []string{"message", "text", "query", "log_message", "msg"}

			// First try to find a preferred field name with String type
			for _, prefName := range preferredMessageNames {
				for _, field := range m.textFields {
					if strings.EqualFold(field, prefName) {
						if fieldType, ok := msg.FieldTypes[field]; ok {
							if strings.Contains(fieldType, "String") {
								m.config.MessageField = field
								log.Debug().
									Str("field", field).
									Str("type", fieldType).
									Msg("Auto-selected message field based on preferred name and String type")
								goto messageSelected
							}
						}
					}
				}
			}

			// Fallback: Find first String type field for message
			for _, field := range m.textFields {
				if fieldType, ok := msg.FieldTypes[field]; ok {
					if strings.Contains(fieldType, "String") {
						m.config.MessageField = field
						log.Debug().
							Str("field", field).
							Str("type", fieldType).
							Msg("Auto-selected message field based on String type (fallback)")
						break
					}
				}
			}
		messageSelected:
		}

		// Auto-select time field if not set
		if m.config.TimeField == "" && !m.restoringData {
			// Preferred field names for time (in priority order)
			preferredTimeNames := []string{"event_time", "time", "timestamp", "event_date_time", "log_time"}

			// First try to find a preferred field name with DateTime type
			for _, prefName := range preferredTimeNames {
				for _, field := range m.timeFields {
					if strings.EqualFold(field, prefName) {
						m.config.TimeField = field
						log.Debug().
							Str("field", field).
							Msg("Auto-selected time field based on preferred name")
						goto timeSelected
					}
				}
			}

			// Fallback: Use first DateTime field
			if len(m.timeFields) > 0 {
				m.config.TimeField = m.timeFields[0]
				log.Debug().
					Str("field", m.timeFields[0]).
					Msg("Auto-selected time field (first DateTime field)")
			}
		timeSelected:
		}

		if m.config.LevelField == "" && !m.restoringData {
			// Preferred field names for level (in priority order)
			preferredLevelNames := []string{"level", "severity", "log_level", "priority"}
			foundEnum := false // Declare before any goto

			// First try to find a preferred field name with Enum or String type
			for _, prefName := range preferredLevelNames {
				for _, field := range m.textFields {
					if strings.EqualFold(field, prefName) {
						m.config.LevelField = field
						if fieldType, ok := msg.FieldTypes[field]; ok {
							log.Debug().
								Str("field", field).
								Str("type", fieldType).
								Msg("Auto-selected level field based on preferred name")
						}
						goto levelSelected
					}
				}
			}

			// Fallback: Find first Enum type field for level
			for _, field := range m.textFields {
				if fieldType, ok := msg.FieldTypes[field]; ok {
					if strings.Contains(fieldType, "Enum") {
						m.config.LevelField = field
						foundEnum = true
						log.Debug().
							Str("field", field).
							Str("type", fieldType).
							Msg("Auto-selected level field based on Enum type")
						break
					}
				}
			}
			// Fallback to String type if no Enum found
			if !foundEnum {
				for _, field := range m.textFields {
					if fieldType, ok := msg.FieldTypes[field]; ok {
						if strings.Contains(fieldType, "String") {
							m.config.LevelField = field
							log.Debug().
								Str("field", field).
								Str("type", fieldType).
								Msg("Auto-selected level field based on String type (fallback)")
							break
						}
					}
				}
			}
		levelSelected:
		}

		// Restore field values BEFORE setting options
		if m.config.MessageField != "" {
			m.msgDropdown.value = m.config.MessageField
			m.msgDropdown.input.SetValue(m.config.MessageField)
		}
		if m.config.TimeField != "" {
			m.timeDropdown.value = m.config.TimeField
			m.timeDropdown.input.SetValue(m.config.TimeField)
		}
		if m.config.TimeMsField != "" {
			m.timeMsDropdown.value = m.config.TimeMsField
			m.timeMsDropdown.input.SetValue(m.config.TimeMsField)
		}
		if m.config.DateField != "" {
			m.dateDropdown.value = m.config.DateField
			m.dateDropdown.input.SetValue(m.config.DateField)
		}
		if m.config.LevelField != "" {
			m.levelDropdown.value = m.config.LevelField
			m.levelDropdown.input.SetValue(m.config.LevelField)
		}

		// Now set options - this will update selected indices
		m.msgDropdown.SetOptions(m.textFields)
		m.timeDropdown.SetOptions(m.timeFields)

		timeMsOpts := append([]string{""}, m.timeMsFields...)
		m.timeMsDropdown.SetOptions(timeMsOpts)

		dateOpts := append([]string{""}, m.dateFields...)
		m.dateDropdown.SetOptions(dateOpts)

		levelOpts := append([]string{""}, m.textFields...)
		m.levelDropdown.SetOptions(levelOpts)

		// Mark restoration as complete
		m.restoringData = false

		// Auto-submit if all required fields are set (CLI mode)
		if m.autoSubmit {
			m.autoSubmit = false
			return m, m.submit()
		}

		return m, nil

	case tea.KeyMsg:
		// Check for Esc first, before dropdown processes it
		if msg.String() == "esc" {
			// Check if any dropdown has options showing
			dropdownOpen := m.dbDropdown.showOptions ||
				m.tableDropdown.showOptions ||
				m.msgDropdown.showOptions ||
				m.timeDropdown.showOptions ||
				m.timeMsDropdown.showOptions ||
				m.dateDropdown.showOptions ||
				m.levelDropdown.showOptions

			// If no dropdown is open, exit the form
			if !dropdownOpen {
				m.saveCurrentConfig()
				m.app.SwitchToMainPage("Returned from :logs")
				return m, nil
			}
			// Otherwise, let dropdown handle closing (fall through to focused component)
		}

		switch msg.String() {
		case "ctrl+c":
			m.saveCurrentConfig()
			m.app.SwitchToMainPage("Returned from :logs")
			return m, nil

		case "tab":
			oldFocusIndex := m.focusIndex
			m.focusIndex = (m.focusIndex + 1) % 10
			m.updateFocus()
			// Check if we need to trigger cascading loads after tab
			return m, m.checkCascadingLoads(oldFocusIndex)

		case "shift+tab":
			oldFocusIndex := m.focusIndex
			m.focusIndex = (m.focusIndex + 9) % 10
			m.updateFocus()
			// Check if we need to trigger cascading loads after shift+tab
			return m, m.checkCascadingLoads(oldFocusIndex)

		case "enter":
			// Handle button presses
			if m.focusIndex == 8 {
				// Show Logs button
				return m, m.submit()
			} else if m.focusIndex == 9 {
				// Cancel button
				m.saveCurrentConfig()
				m.app.SwitchToMainPage("Returned from :logs")
				return m, nil
			}
			// Otherwise, let dropdown handle enter
		}
	}

	// Update focused component
	var cmd tea.Cmd
	var handled bool

	switch m.focusIndex {
	case 0:
		wasOpen := m.dbDropdown.showOptions
		cmd, handled = m.dbDropdown.Update(msg)
		if !handled {
			// Allow arrow navigation between fields when dropdown is closed
			if keyMsg, ok := msg.(tea.KeyMsg); ok {
				switch keyMsg.String() {
				case "down":
					oldFocusIndex := m.focusIndex
					m.focusIndex = 1
					m.updateFocus()
					return m, m.checkCascadingLoads(oldFocusIndex)
				case "up":
					oldFocusIndex := m.focusIndex
					m.focusIndex = 9
					m.updateFocus()
					return m, m.checkCascadingLoads(oldFocusIndex)
				}
			}
		}

		// Check if dropdown was just closed (selection made)
		nowClosed := !m.dbDropdown.showOptions
		if wasOpen && nowClosed {
			// Dropdown was closed, move to next field
			m.focusIndex = 1
			m.updateFocus()
		}

		if m.dbDropdown.value != m.config.Database {
			m.config.Database = m.dbDropdown.value
			if !m.dbDropdown.showOptions {
				// User explicitly changed selection - clear restoration flag
				m.restoringData = false
				// Load tables when selection is confirmed
				m.loading = true
				m.loadingWhat = "tables"
				return m, tea.Batch(cmd, m.loadTables())
			}
		}
	case 1:
		wasOpen := m.tableDropdown.showOptions
		cmd, handled = m.tableDropdown.Update(msg)
		if !handled {
			if keyMsg, ok := msg.(tea.KeyMsg); ok {
				switch keyMsg.String() {
				case "down":
					oldFocusIndex := m.focusIndex
					m.focusIndex = 2
					m.updateFocus()
					return m, m.checkCascadingLoads(oldFocusIndex)
				case "up":
					oldFocusIndex := m.focusIndex
					m.focusIndex = 0
					m.updateFocus()
					return m, m.checkCascadingLoads(oldFocusIndex)
				}
			}
		}

		// Check if dropdown was just closed (selection made)
		nowClosed := !m.tableDropdown.showOptions
		if wasOpen && nowClosed {
			// Dropdown was closed, move to next field
			m.focusIndex = 2
			m.updateFocus()
		}

		if m.tableDropdown.value != m.config.Table {
			m.config.Table = m.tableDropdown.value
			if !m.tableDropdown.showOptions {
				// User explicitly changed selection - clear restoration flag
				m.restoringData = false
				// Load columns when selection is confirmed
				m.loading = true
				m.loadingWhat = "columns"
				return m, tea.Batch(cmd, m.loadColumns())
			}
		}
	case 2:
		wasOpen := m.msgDropdown.showOptions
		cmd, handled = m.msgDropdown.Update(msg)
		if !handled {
			if keyMsg, ok := msg.(tea.KeyMsg); ok {
				switch keyMsg.String() {
				case "down":
					m.focusIndex = 3
					m.updateFocus()
					return m, nil
				case "up":
					m.focusIndex = 1
					m.updateFocus()
					return m, nil
				}
			}
		}

		// Check if dropdown was just closed (selection made)
		nowClosed := !m.msgDropdown.showOptions
		if wasOpen && nowClosed {
			// Dropdown was closed, move to next field
			m.focusIndex = 3
			m.updateFocus()
		}

		m.config.MessageField = m.msgDropdown.value
	case 3:
		wasOpen := m.timeDropdown.showOptions
		cmd, handled = m.timeDropdown.Update(msg)
		if !handled {
			if keyMsg, ok := msg.(tea.KeyMsg); ok {
				switch keyMsg.String() {
				case "down":
					m.focusIndex = 4
					m.updateFocus()
					return m, nil
				case "up":
					m.focusIndex = 2
					m.updateFocus()
					return m, nil
				}
			}
		}

		// Check if dropdown was just closed (selection made)
		nowClosed := !m.timeDropdown.showOptions
		if wasOpen && nowClosed {
			// Dropdown was closed, move to next field
			m.focusIndex = 4
			m.updateFocus()
		}

		m.config.TimeField = m.timeDropdown.value
	case 4:
		wasOpen := m.timeMsDropdown.showOptions
		cmd, handled = m.timeMsDropdown.Update(msg)
		if !handled {
			if keyMsg, ok := msg.(tea.KeyMsg); ok {
				switch keyMsg.String() {
				case "down":
					m.focusIndex = 5
					m.updateFocus()
					return m, nil
				case "up":
					m.focusIndex = 3
					m.updateFocus()
					return m, nil
				}
			}
		}

		// Check if dropdown was just closed (selection made)
		nowClosed := !m.timeMsDropdown.showOptions
		if wasOpen && nowClosed {
			// Dropdown was closed, move to next field
			m.focusIndex = 5
			m.updateFocus()
		}

		m.config.TimeMsField = m.timeMsDropdown.value
	case 5:
		wasOpen := m.dateDropdown.showOptions
		cmd, handled = m.dateDropdown.Update(msg)
		if !handled {
			if keyMsg, ok := msg.(tea.KeyMsg); ok {
				switch keyMsg.String() {
				case "down":
					m.focusIndex = 6
					m.updateFocus()
					return m, nil
				case "up":
					m.focusIndex = 4
					m.updateFocus()
					return m, nil
				}
			}
		}

		// Check if dropdown was just closed (selection made)
		nowClosed := !m.dateDropdown.showOptions
		if wasOpen && nowClosed {
			// Dropdown was closed, move to next field
			m.focusIndex = 6
			m.updateFocus()
		}

		m.config.DateField = m.dateDropdown.value
	case 6:
		wasOpen := m.levelDropdown.showOptions
		cmd, handled = m.levelDropdown.Update(msg)
		if !handled {
			if keyMsg, ok := msg.(tea.KeyMsg); ok {
				switch keyMsg.String() {
				case "down":
					m.focusIndex = 7
					m.updateFocus()
					return m, nil
				case "up":
					m.focusIndex = 5
					m.updateFocus()
					return m, nil
				}
			}
		}

		// Check if dropdown was just closed (selection made)
		nowClosed := !m.levelDropdown.showOptions
		if wasOpen && nowClosed {
			// Dropdown was closed, move to next field
			m.focusIndex = 7
			m.updateFocus()
		}

		m.config.LevelField = m.levelDropdown.value
	case 7:
		if keyMsg, ok := msg.(tea.KeyMsg); ok {
			switch keyMsg.String() {
			case "down":
				m.focusIndex = 8
				m.updateFocus()
				return m, nil
			case "up":
				m.focusIndex = 6
				m.updateFocus()
				return m, nil
			}
		}
		m.windowInput, cmd = m.windowInput.Update(msg)
	case 8:
		// Show Logs button
		if keyMsg, ok := msg.(tea.KeyMsg); ok {
			switch keyMsg.String() {
			case "down":
				m.focusIndex = 9
				m.updateFocus()
				return m, nil
			case "up":
				m.focusIndex = 7
				m.updateFocus()
				return m, nil
			case "right":
				m.focusIndex = 9
				m.updateFocus()
				return m, nil
			}
		}
	case 9:
		// Cancel button
		if keyMsg, ok := msg.(tea.KeyMsg); ok {
			switch keyMsg.String() {
			case "down":
				m.focusIndex = 0
				m.updateFocus()
				return m, nil
			case "up":
				m.focusIndex = 8
				m.updateFocus()
				return m, nil
			case "left":
				m.focusIndex = 8
				m.updateFocus()
				return m, nil
			}
		}
	}

	cmds = append(cmds, cmd)
	return m, tea.Batch(cmds...)
}

func (m *logsConfigForm) updateFocus() {
	m.dbDropdown.Blur()
	m.tableDropdown.Blur()
	m.msgDropdown.Blur()
	m.timeDropdown.Blur()
	m.timeMsDropdown.Blur()
	m.dateDropdown.Blur()
	m.levelDropdown.Blur()
	m.windowInput.Blur()

	switch m.focusIndex {
	case 0:
		m.dbDropdown.Focus()
	case 1:
		m.tableDropdown.Focus()
	case 2:
		m.msgDropdown.Focus()
	case 3:
		m.timeDropdown.Focus()
	case 4:
		m.timeMsDropdown.Focus()
	case 5:
		m.dateDropdown.Focus()
	case 6:
		m.levelDropdown.Focus()
	case 7:
		m.windowInput.Focus()
	}
}

// checkCascadingLoads checks if database or table values changed and triggers cascading loads
func (m *logsConfigForm) checkCascadingLoads(oldFocusIndex int) tea.Cmd {
	// Check if we just left the database dropdown (index 0)
	if oldFocusIndex == 0 && m.dbDropdown.value != m.config.Database {
		m.config.Database = m.dbDropdown.value
		// User changed database via tab - clear restoration flag and load tables
		m.restoringData = false
		m.loading = true
		m.loadingWhat = "tables"
		return m.loadTables()
	}

	// Check if we just left the table dropdown (index 1)
	if oldFocusIndex == 1 && m.tableDropdown.value != m.config.Table {
		m.config.Table = m.tableDropdown.value
		// User changed table via tab - clear restoration flag and load columns
		m.restoringData = false
		m.loading = true
		m.loadingWhat = "columns"
		return m.loadColumns()
	}

	return nil
}

func (m *logsConfigForm) saveCurrentConfig() {
	// Save current form state for next time (even on cancel)
	// This preserves user choices across sessions
	m.config.Database = m.dbDropdown.value
	m.config.Table = m.tableDropdown.value
	m.config.MessageField = m.msgDropdown.value
	m.config.TimeField = m.timeDropdown.value
	m.config.TimeMsField = m.timeMsDropdown.value
	m.config.DateField = m.dateDropdown.value
	m.config.LevelField = m.levelDropdown.value

	if windowStr := m.windowInput.Value(); windowStr != "" {
		if w, err := strconv.Atoi(windowStr); err == nil && w > 0 {
			m.config.WindowSize = w
		}
	}

	configCopy := m.config
	m.app.lastLogsConfig = &configCopy

	log.Debug().
		Str("database", configCopy.Database).
		Str("table", configCopy.Table).
		Str("message", configCopy.MessageField).
		Str("time", configCopy.TimeField).
		Msg("Saved logs config")
}

func (m *logsConfigForm) View() string {
	if m.err != nil {
		return lipgloss.NewStyle().
			Foreground(lipgloss.Color("9")).
			Render(fmt.Sprintf("Error: %v\n\nPress ESC to return", m.err))
	}

	// Title with optional loading indicator
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	titleText := "Log Explorer Configuration"
	if m.loading {
		loadingStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("11"))
		titleText += " " + loadingStyle.Render(fmt.Sprintf("(loading %s...)", m.loadingWhat))
	}
	title := titleStyle.Render(titleText)

	// Form fields in two columns
	leftCol := lipgloss.JoinVertical(lipgloss.Left,
		m.dbDropdown.View(m.focusIndex == 0),
		"",
		m.tableDropdown.View(m.focusIndex == 1),
		"",
		m.msgDropdown.View(m.focusIndex == 2),
		"",
		m.timeDropdown.View(m.focusIndex == 3),
	)

	rightCol := lipgloss.JoinVertical(lipgloss.Left,
		m.timeMsDropdown.View(m.focusIndex == 4),
		"",
		m.dateDropdown.View(m.focusIndex == 5),
		"",
		m.levelDropdown.View(m.focusIndex == 6),
		"",
		lipgloss.JoinVertical(lipgloss.Left,
			lipgloss.NewStyle().Foreground(lipgloss.Color("12")).Render("Window Size:"),
			lipgloss.NewStyle().
				BorderStyle(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("240")).
				Render(m.windowInput.View()),
		),
	)

	form := lipgloss.JoinHorizontal(lipgloss.Top, leftCol, "    ", rightCol)

	// Buttons at bottom left
	buttonStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("0")).
		Background(lipgloss.Color("6")).
		Padding(0, 2)

	buttonStyleInactive := lipgloss.NewStyle().
		Foreground(lipgloss.Color("15")).
		Background(lipgloss.Color("240")).
		Padding(0, 2)

	var showLogsBtn, cancelBtn string
	if m.focusIndex == 8 {
		showLogsBtn = buttonStyle.Render("Show Logs")
	} else {
		showLogsBtn = buttonStyleInactive.Render("Show Logs")
	}

	if m.focusIndex == 9 {
		cancelBtn = buttonStyle.Render("Cancel")
	} else {
		cancelBtn = buttonStyleInactive.Render("Cancel")
	}

	buttons := lipgloss.JoinHorizontal(lipgloss.Left, showLogsBtn, "  ", cancelBtn)

	// Help
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	help := helpStyle.Render("Tab/↑↓: Navigate | Enter: Select/Confirm | ←→: Switch buttons | Type: Filter | Esc: Cancel")

	content := lipgloss.JoinVertical(lipgloss.Left,
		title,
		"",
		form,
		"",
		buttons,
		"",
		help,
	)

	return content
}

func (m *logsConfigForm) submit() tea.Cmd {
	// Sync dropdown values to config before validation
	m.config.Database = m.dbDropdown.value
	m.config.Table = m.tableDropdown.value
	m.config.MessageField = m.msgDropdown.value
	m.config.TimeField = m.timeDropdown.value
	m.config.TimeMsField = m.timeMsDropdown.value
	m.config.DateField = m.dateDropdown.value
	m.config.LevelField = m.levelDropdown.value

	log.Debug().
		Str("database", m.config.Database).
		Str("table", m.config.Table).
		Str("message", m.config.MessageField).
		Str("time", m.config.TimeField).
		Msg("Submitting logs config")

	// Validate required fields
	if m.config.Database == "" || m.config.Table == "" ||
		m.config.MessageField == "" || m.config.TimeField == "" {
		m.err = fmt.Errorf("database, table, message field and time field are required")
		return nil
	}

	// Parse window size
	if windowStr := m.windowInput.Value(); windowStr != "" {
		if w, err := strconv.Atoi(windowStr); err == nil && w > 0 {
			m.config.WindowSize = w
		}
	}

	// Store all fields in config
	m.config.AllFields = m.allFields

	// Save config for next time (memory persistence)
	configCopy := m.config
	m.app.lastLogsConfig = &configCopy

	return func() tea.Msg {
		return LogsConfigMsg{Config: m.config}
	}
}

func (m *logsConfigForm) loadDatabases() tea.Cmd {
	return func() tea.Msg {
		if m.app.state.ClickHouse == nil {
			return DatabasesLoadedMsg{Err: fmt.Errorf("not connected to ClickHouse")}
		}

		rows, err := m.app.state.ClickHouse.Query("SELECT name FROM system.databases ORDER BY name")
		if err != nil {
			return DatabasesLoadedMsg{Err: err}
		}
		defer rows.Close()

		var databases []string
		for rows.Next() {
			var db string
			if err := rows.Scan(&db); err != nil {
				log.Error().Err(err).Msg("error scanning database name")
				continue
			}
			databases = append(databases, db)
		}

		return DatabasesLoadedMsg{Databases: databases}
	}
}

func (m *logsConfigForm) loadTables() tea.Cmd {
	return func() tea.Msg {
		if m.app.state.ClickHouse == nil {
			return TablesLoadedMsg{Err: fmt.Errorf("not connected to ClickHouse")}
		}

		query := fmt.Sprintf("SHOW TABLES FROM `%s`", m.config.Database)
		rows, err := m.app.state.ClickHouse.Query(query)
		if err != nil {
			return TablesLoadedMsg{Err: err}
		}
		defer rows.Close()

		var tables []string
		for rows.Next() {
			var tbl string
			if err := rows.Scan(&tbl); err != nil {
				log.Error().Err(err).Msg("error scanning table name")
				continue
			}
			tables = append(tables, tbl)
		}

		return TablesLoadedMsg{Tables: tables}
	}
}

func (m *logsConfigForm) loadColumns() tea.Cmd {
	return func() tea.Msg {
		if m.app.state.ClickHouse == nil {
			return ColumnsLoadedMsg{Err: fmt.Errorf("not connected to ClickHouse")}
		}

		query := fmt.Sprintf(
			"SELECT name, type FROM system.columns WHERE database='%s' AND table='%s' ORDER BY name",
			m.config.Database, m.config.Table,
		)
		rows, err := m.app.state.ClickHouse.Query(query)
		if err != nil {
			return ColumnsLoadedMsg{Err: err}
		}
		defer rows.Close()

		var allFields, timeFields, timeMsFields, dateFields, textFields []string
		fieldTypes := make(map[string]string)

		for rows.Next() {
			var fieldName, fieldType string
			if err := rows.Scan(&fieldName, &fieldType); err != nil {
				log.Error().Err(err).Msg("error scanning column info")
				continue
			}

			allFields = append(allFields, fieldName)
			fieldTypes[fieldName] = fieldType

			// Categorize fields by type
			if strings.Contains(fieldType, "DateTime64") {
				timeMsFields = append(timeMsFields, fieldName)
			} else if strings.Contains(fieldType, "DateTime") {
				timeFields = append(timeFields, fieldName)
			} else if strings.Contains(fieldType, "Date") {
				dateFields = append(dateFields, fieldName)
			}

			// Text fields (for message and level)
			if !strings.Contains(fieldType, "Array") &&
				!strings.Contains(fieldType, "Tuple") &&
				!strings.Contains(fieldType, "Map") {
				textFields = append(textFields, fieldName)
			}
		}

		return ColumnsLoadedMsg{
			AllFields:    allFields,
			TimeFields:   timeFields,
			TimeMsFields: timeMsFields,
			DateFields:   dateFields,
			TextFields:   textFields,
			FieldTypes:   fieldTypes,
		}
	}
}

// Message types for async operations
type DatabasesLoadedMsg struct {
	Databases []string
	Err       error
}

type TablesLoadedMsg struct {
	Tables []string
	Err    error
}

type ColumnsLoadedMsg struct {
	AllFields    []string
	TimeFields   []string
	TimeMsFields []string
	DateFields   []string
	TextFields   []string
	FieldTypes   map[string]string // Map of field name to type
	Err          error
}

type LogsConfigMsg struct {
	Config LogConfig
}

// LogConfig holds configuration for log viewing
type LogConfig struct {
	Database     string
	Table        string
	MessageField string
	TimeField    string
	TimeMsField  string
	DateField    string
	LevelField   string
	WindowSize   int
	AllFields    []string // All fields available in the table (for filtering)
}

// handleLogsCommand shows the logs configuration form
func (a *App) handleLogsCommand() tea.Cmd {
	if a.state.ClickHouse == nil {
		a.SwitchToMainPage("Error: Please connect to a ClickHouse instance first using :connect command")
		return nil
	}

	// Show configuration form with last used config if available
	form := newLogsConfigForm(a, a.width, a.height, a.lastLogsConfig)
	a.logsHandler = form
	a.currentPage = pageLogs

	return form.Init()
}

// LogsDataMsg is sent when log data is loaded
type LogsDataMsg struct {
	Entries         []LogEntry
	FirstEntryTime  time.Time
	LastEntryTime   time.Time
	TotalRows       int
	LevelCounts     map[string]int
	LevelTimeSeries map[string][]float64 // Time-bucketed counts per level for sparkline
	TimeLabels      []string             // Time labels for buckets
	Err             error
}

// OverviewDataMsg is sent when overview sparkline data is loaded via LTTB
type OverviewDataMsg struct {
	LevelTimeSeries map[string][]float64 // LTTB-downsampled counts per level
	TimeLabels      []string             // Time labels for LTTB points
	Err             error
}

// timeRange represents a time range for zoom functionality
type timeRange struct {
	from time.Time
	to   time.Time
}

// logsViewer is the main log viewer
type logsViewer struct {
	config          LogConfig
	table           widgets.FilteredTable
	entries         []LogEntry
	firstEntryTime  time.Time
	lastEntryTime   time.Time
	totalRows       int
	levelCounts     map[string]int
	levelTimeSeries map[string][]float64 // Time-bucketed counts per level for sparkline
	timeLabels      []string             // Time labels for buckets
	bucketInterval  int                  // Seconds per bucket
	loading         bool
	err             error
	width                   int
	height                  int
	tableHeight             int // Current table height
	measuredWidgetOverhead  int // Measured overhead: actual rendered lines - allocated height
	showDetails             bool
	selectedEntry           LogEntry
	detailsSelectedIdx      int      // Selected field index in details view (-1 = none, 0-2 = fixed fields, 3+ = AllFields)
	detailsFieldNames       []string // Cached sorted field names for details view
	offset                  int  // Current offset for pagination
	app                     *App // Reference to app for triggering data loads

	// Interactive sparkline navigation
	overviewMode   bool        // true = overview visible, false = hidden
	focusOverview  bool        // true = overview has focus, false = table has focus
	selectedLevel  int         // 0-3 (error, warning, info, debug)
	selectedBucket int         // 0 to len(timeLabels)-1
	zoomStack      []timeRange // History of time ranges for zoom out
	originalRange  timeRange   // Original time range before any zoom

	// Zoom menu
	showZoomMenu bool // Show zoom menu
	zoomMenuIdx  int  // Selected menu item (0-3)

	// Filter form (new tree-based structure)
	showFilterForm    bool            // Whether filter form is visible
	rootFilter        *FilterNode     // Root group (always a group with Logic="AND")
	selectedPath      []int           // Path to selected node [childIdx, childIdx, ...]
	filterMode        string          // "tree", "edit", "logicDropdown"
	editFieldFocus    int             // In edit mode: 0=field, 1=operator, 2=value
	groupFocusPos     int             // Position within group: 0=logic, 1=add condition, 2=add group
	showLogicDropdown bool            // Show logic selection dropdown (AND/OR/NOT AND/NOT OR)
	logicDropdownIdx  int             // Selected index in logic dropdown (0-3)
	filtersChanged    bool            // True when filters changed, need Apply
	applyFocused      bool            // True when Apply button has focus (separate from tree)
	allFields         []string        // All available fields from table
	filterFieldDD     dropdown        // Filter field dropdown (for editing)
	filterOperatorDD  dropdown        // Filter operator dropdown (for editing)
	filterValueInput  textinput.Model // Filter value input (for editing)
}

func newLogsViewer(config LogConfig, width, height int) logsViewer {
	// Calculate column widths to use 100% screen width
	// Time: 23 chars for "2006-01-02 15:04:05.000"
	// Message: rest of available width (no separate Level column)
	timeWidth := 23

	// Account for table borders only (padding is included in column widths):
	// Left border (1) + column separator (1) + right border (1) = 3 chars
	borderOverhead := 3
	messageWidth := width - timeWidth - borderOverhead
	if messageWidth < 30 {
		messageWidth = 30
	}

	headers := []string{"time", "message"}
	widths := []int{timeWidth, messageWidth}

	// Calculate initial table height
	// This is just a starting estimate - will be recalculated after first data load
	// when we can measure the actual title and overview rendering
	initialTableHeight := height - 10 // Conservative estimate
	if initialTableHeight < 5 {
		initialTableHeight = 5
	}

	log.Debug().
		Int("screen_width", width).
		Int("screen_height", height).
		Int("time_width", timeWidth).
		Int("message_width", messageWidth).
		Int("border_overhead", borderOverhead).
		Int("initial_table_height", initialTableHeight).
		Int("total_used", timeWidth+messageWidth+borderOverhead).
		Msg(">>> Logs table dimensions calculation")

	tableModel := widgets.NewFilteredTableBubbleWithWidths(
		"",
		headers,
		widths,
		width,
		initialTableHeight,
	)
	// Hide the FilteredTable's built-in help footer since we have our own main help line
	tableModel.SetShowHelp(false)

	// Initialize filter form components (for editing conditions)
	filterFieldDD := newDropdown("Field", 20, true)
	filterOperatorDD := newDropdown("Operator", 15, true)
	filterOperatorDD.SetOptions([]string{"=", "!=", ">", "<", ">=", "<=", "LIKE", "NOT LIKE", "IN", "NOT IN", "IS NULL", "IS NOT NULL"})
	filterOperatorDD.SetValue("=")

	filterValueInput := textinput.New()
	filterValueInput.Placeholder = "Filter value..."
	filterValueInput.Width = 30

	// Initialize with one empty root group (AND logic)
	rootFilter := NewFilterGroup("AND")

	return logsViewer{
		config:                 config,
		table:                  tableModel,
		loading:                true,
		width:                  width,
		height:                 height,
		tableHeight:            initialTableHeight,
		measuredWidgetOverhead: 2, // Initial guess, will be measured after first render
		overviewMode:           true, // Show overview by default
		showFilterForm:         false,
		rootFilter:             rootFilter,
		selectedPath:           []int{}, // Root is selected by default
		filterMode:             "tree",
		allFields:              []string{},
		filterFieldDD:          filterFieldDD,
		filterOperatorDD:       filterOperatorDD,
		filterValueInput:       filterValueInput,
	}
}

func (m logsViewer) Init() tea.Cmd {
	return nil
}

// pathEqual compares two paths for equality
func pathEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// getNodeAtPath returns the node at the given path in the filter tree
func (m *logsViewer) getNodeAtPath(path []int) *FilterNode {
	if m.rootFilter == nil {
		return nil
	}
	node := m.rootFilter
	for _, idx := range path {
		if !node.IsGroup || idx < 0 || idx >= len(node.Children) {
			return nil
		}
		node = node.Children[idx]
	}
	return node
}

// getParentPath returns the parent path (removes last element)
func getParentPath(path []int) []int {
	if len(path) == 0 {
		return nil
	}
	return path[:len(path)-1]
}

// nodeWithPath represents a node and its path in the tree
type nodeWithPath struct {
	Node *FilterNode
	Path []int
}

// flattenTree returns all nodes in depth-first order with their paths
func flattenTree(node *FilterNode, path []int) []nodeWithPath {
	result := []nodeWithPath{{Node: node, Path: path}}
	if node.IsGroup {
		for i, child := range node.Children {
			childPath := make([]int, len(path)+1)
			copy(childPath, path)
			childPath[len(path)] = i
			result = append(result, flattenTree(child, childPath)...)
		}
	}
	return result
}

// getSelectedNode returns the currently selected node
func (m *logsViewer) getSelectedNode() *FilterNode {
	return m.getNodeAtPath(m.selectedPath)
}

// getParentNode returns the parent of the selected node
func (m *logsViewer) getParentNode() *FilterNode {
	if len(m.selectedPath) == 0 {
		return nil
	}
	return m.getNodeAtPath(m.selectedPath[:len(m.selectedPath)-1])
}

// getTotalFilterCount returns total number of conditions in the tree
func (m *logsViewer) getTotalFilterCount() int {
	if m.rootFilter == nil {
		return 0
	}
	return countConditions(m.rootFilter)
}

// countConditions recursively counts all conditions in a node
func countConditions(node *FilterNode) int {
	if !node.IsGroup {
		return 1
	}
	count := 0
	for _, child := range node.Children {
		count += countConditions(child)
	}
	return count
}

// addConditionToSelected adds a new condition to the selected group
func (m *logsViewer) addConditionToSelected(field, operator, value string) {
	node := m.getSelectedNode()
	if node == nil {
		node = m.rootFilter
	}
	// If selected is a condition, add to its parent
	if !node.IsGroup {
		parentPath := getParentPath(m.selectedPath)
		node = m.getNodeAtPath(parentPath)
		if node == nil {
			node = m.rootFilter
		}
	}
	if node.IsGroup {
		node.AddChild(NewFilterCondition(field, operator, value))
		m.filtersChanged = true
	}
}

// addGroupToSelected adds a new group to the selected group
func (m *logsViewer) addGroupToSelected(logic string) {
	node := m.getSelectedNode()
	if node == nil {
		node = m.rootFilter
	}
	// If selected is a condition, add to its parent
	if !node.IsGroup {
		parentPath := getParentPath(m.selectedPath)
		node = m.getNodeAtPath(parentPath)
		if node == nil {
			node = m.rootFilter
		}
	}
	if node.IsGroup {
		node.AddChild(NewFilterGroup(logic))
		m.filtersChanged = true
	}
}

// deleteSelectedNode deletes the currently selected node
func (m *logsViewer) deleteSelectedNode() {
	if len(m.selectedPath) == 0 {
		// Can't delete root
		return
	}
	parentPath := m.selectedPath[:len(m.selectedPath)-1]
	parent := m.getNodeAtPath(parentPath)
	if parent == nil || !parent.IsGroup {
		return
	}
	childIdx := m.selectedPath[len(m.selectedPath)-1]
	parent.RemoveChild(childIdx)
	m.filtersChanged = true

	// Adjust selection to previous sibling or parent
	if childIdx > 0 {
		m.selectedPath[len(m.selectedPath)-1] = childIdx - 1
	} else if len(parent.Children) > 0 {
		// Stay at index 0
	} else {
		// No children left, select parent
		m.selectedPath = parentPath
	}
}

// cycleGroupLogic cycles the logic of the selected group (AND → OR → NOT AND → NOT OR)
func (m *logsViewer) cycleGroupLogic() {
	node := m.getSelectedNode()
	if node == nil || !node.IsGroup {
		return
	}
	node.CycleLogic()
	m.filtersChanged = true
}

// recalculateTableHeight recalculates and updates table height based on current overview mode
func (m *logsViewer) recalculateTableHeight() {
	// Calculate actual title lines (title might wrap on narrow terminals)
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	pageNum := (m.offset / m.config.WindowSize) + 1
	title := fmt.Sprintf("Log Entries | Page %d (offset: %d, window: %d) | From: %s To: %s",
		pageNum,
		m.offset,
		m.config.WindowSize,
		m.firstEntryTime.Format("2006-01-02 15:04:05.000 MST"),
		m.lastEntryTime.Format("2006-01-02 15:04:05.000 MST"))
	renderedTitle := titleStyle.Render(title)
	titleLines := strings.Count(renderedTitle, "\n") + 1

	var actualOverhead int

	// Account for filter form if visible
	var filterFormLines int
	if m.showFilterForm {
		filterForm := m.renderFilterForm()
		borderStyle := lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("240"))
		renderedFilterForm := borderStyle.Render(filterForm)
		filterFormLines = strings.Count(renderedFilterForm, "\n") + 1
	}

	// Main help line is always present at the bottom
	mainHelpLines := 1

	if m.overviewMode && m.totalRows > 0 {
		// Overview is visible and we have data - calculate actual overhead
		overview := m.renderOverview()
		borderStyle := lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("240"))
		renderedOverview := borderStyle.Render(overview)

		overviewLines := strings.Count(renderedOverview, "\n") + 1
		actualOverhead = titleLines + filterFormLines + overviewLines + mainHelpLines
	} else if m.overviewMode && m.totalRows == 0 {
		// Overview will show but no data yet - use estimate
		estimatedOverviewLines := 8
		actualOverhead = titleLines + filterFormLines + estimatedOverviewLines + mainHelpLines
	} else {
		// Overview is hidden - only title line (and filter form if shown)
		actualOverhead = titleLines + filterFormLines + mainHelpLines
	}

	// Calculate table height based on actual measurements
	// Available space = screen height - title - overview
	availableForTable := m.height - actualOverhead

	// Use measured widget overhead from previous renders
	// Widget overhead = how many extra lines the widget renders beyond what we allocate
	newTableHeight := availableForTable - m.measuredWidgetOverhead

	if newTableHeight < 5 {
		newTableHeight = 5
	}

	log.Debug().
		Int("screen_height", m.height).
		Int("title_lines", titleLines).
		Int("actual_overhead", actualOverhead).
		Int("available_for_table", availableForTable).
		Int("measured_widget_overhead", m.measuredWidgetOverhead).
		Int("old_table_height", m.tableHeight).
		Int("new_table_height", newTableHeight).
		Bool("overview_mode", m.overviewMode).
		Msg(">>> Recalculating table height")

	// Update table height if changed
	if newTableHeight != m.tableHeight {
		m.table.SetSize(m.width, newTableHeight)
		m.tableHeight = newTableHeight
	}
}

// getLogLevelColor returns the appropriate color for a log level
func getLogLevelColor(level string) lipgloss.Color {
	levelLower := strings.ToLower(level)
	switch levelLower {
	case "fatal", "critical", "error", "exception":
		return lipgloss.Color("9") // Red
	case "warning", "warn":
		return lipgloss.Color("11") // Yellow
	case "debug", "trace":
		return lipgloss.Color("14") // Cyan
	case "info", "information", "notice":
		return lipgloss.Color("10") // Green
	default:
		return lipgloss.Color("7") // White/default
	}
}

func (m logsViewer) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height

		// Recalculate column widths to maintain 100% screen width
		timeWidth := 23
		borderOverhead := 3
		messageWidth := msg.Width - timeWidth - borderOverhead
		if messageWidth < 30 {
			messageWidth = 30
		}

		// Dynamically calculate overhead based on overview visibility
		var actualOverhead int
		titleLines := 1

		if m.overviewMode && m.totalRows > 0 {
			// Overview is visible and we have data - calculate actual overhead
			overview := m.renderOverview()
			borderStyle := lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("240"))
			renderedOverview := borderStyle.Render(overview)

			overviewLines := strings.Count(renderedOverview, "\n") + 1
			actualOverhead = titleLines + overviewLines
		} else if m.overviewMode && m.totalRows == 0 {
			// Overview will be visible but no data yet - use estimate
			estimatedOverviewLines := 8 // Typical: bar + 4 sparklines + borders + info
			actualOverhead = titleLines + estimatedOverviewLines
		} else {
			// Overview is hidden - only title line
			actualOverhead = titleLines
		}

		tableHeight := msg.Height - actualOverhead
		if tableHeight < 5 {
			tableHeight = 5
		}

		log.Debug().
			Int("new_width", msg.Width).
			Int("new_height", msg.Height).
			Int("time_width", timeWidth).
			Int("message_width", messageWidth).
			Int("border_overhead", borderOverhead).
			Int("actual_overhead", actualOverhead).
			Int("table_height", tableHeight).
			Bool("overview_mode", m.overviewMode).
			Msg(">>> Logs table resized")

		// Update table size
		m.table.SetSize(msg.Width, tableHeight)
		m.tableHeight = tableHeight

		// If we don't have data yet and aren't loading, trigger initial fetch
		// This handles the case where ShowLogsViewer was called before WindowSizeMsg
		if m.totalRows == 0 && !m.loading && m.app != nil {
			m.loading = true
			return m, m.app.fetchLogsDataCmd(m.config, 0, m.rootFilter, m.width)
		}

		return m, nil

	case LogsDataMsg:
		m.loading = false
		if msg.Err != nil {
			m.err = msg.Err
			return m, nil
		}

		m.entries = msg.Entries
		m.firstEntryTime = msg.FirstEntryTime
		m.lastEntryTime = msg.LastEntryTime
		m.totalRows = msg.TotalRows
		m.levelCounts = msg.LevelCounts
		// LevelTimeSeries and TimeLabels will be populated by OverviewDataMsg

		// Recalculate table height based on current state (filter form, overview, etc.)
		m.recalculateTableHeight()

		// Convert to table rows
		var rows []table.Row
		for _, entry := range msg.Entries {
			timeStr := entry.Time.Format("2006-01-02 15:04:05.000")

			// Apply color to message based on log level
			messageColor := getLogLevelColor(entry.Level)
			messageStyled := lipgloss.NewStyle().Foreground(messageColor).Render(entry.Message)

			rowData := table.RowData{
				"time":         timeStr,
				"message":      messageStyled,
				"_plain_msg":   entry.Message, // Store plain message for matching
				"_level":       entry.Level,   // Store level for matching
			}

			rows = append(rows, table.NewRow(rowData))
		}
		m.table.SetRows(rows)

		// Trigger overview data fetch if overview is visible and we have time range
		if m.overviewMode && m.app != nil && !m.app.state.FromTime.IsZero() && !m.app.state.ToTime.IsZero() {
			// Calculate sparkline width for bucket count
			contentWidth := m.width - 2
			sparklineRowLabelWidth := 14 // "  ERROR     : "
			sparklineWidth := contentWidth - sparklineRowLabelWidth
			if sparklineWidth < 40 {
				sparklineWidth = 40
			}
			if sparklineWidth > 200 {
				sparklineWidth = 200
			}

			log.Debug().
				Int("sparkline_width", sparklineWidth).
				Time("from_time", m.app.state.FromTime).
				Time("to_time", m.app.state.ToTime).
				Msg(">>> Triggering overview fetch")

			return m, m.app.fetchOverviewDataCmd(m.config, m.app.state.FromTime, m.app.state.ToTime, sparklineWidth, m.rootFilter)
		}
		return m, nil

	case OverviewDataMsg:
		if msg.Err != nil {
			log.Error().Err(msg.Err).Msg("Error fetching overview data")
			// Don't show error to user, just log it - overview is optional
			return m, nil
		}

		m.levelTimeSeries = msg.LevelTimeSeries
		m.timeLabels = msg.TimeLabels

		// Calculate bucket interval from time range and number of buckets
		if m.app != nil && !m.app.state.FromTime.IsZero() && !m.app.state.ToTime.IsZero() && len(m.timeLabels) > 0 {
			timeRange := m.app.state.ToTime.Sub(m.app.state.FromTime).Seconds()
			m.bucketInterval = int(timeRange / float64(len(m.timeLabels)))
			if m.bucketInterval < 1 {
				m.bucketInterval = 1
			}
		}

		// Log keys received
		keys := make([]string, 0, len(m.levelTimeSeries))
		for k := range m.levelTimeSeries {
			keys = append(keys, k)
		}
		log.Debug().
			Int("levelTimeSeries_count", len(m.levelTimeSeries)).
			Strs("levelTimeSeries_keys", keys).
			Int("timeLabels_count", len(m.timeLabels)).
			Int("bucketInterval", m.bucketInterval).
			Msg(">>> OverviewDataMsg received - data assigned")

		// Recalculate table height because overview size changes when sparklines are added
		m.recalculateTableHeight()

		return m, nil

	case tea.KeyMsg:
		// Handle zoom menu first (highest priority)
		if m.showZoomMenu {
			return m.handleZoomMenuKey(msg)
		}

		// Handle filter form (second highest priority)
		if m.showFilterForm {
			return m.handleFilterFormKey(msg)
		}

		// Check for Ctrl+F to toggle filter form
		if msg.String() == "ctrl+f" && !m.table.IsFiltering() {
			m.showFilterForm = !m.showFilterForm
			if m.showFilterForm {
				// Initialize field options if not already done
				if len(m.filterFieldDD.options) == 0 && len(m.allFields) > 0 {
					m.filterFieldDD.SetOptions(m.allFields)
				}
				// Initialize tree navigation mode
				m.filterMode = "tree"
				m.selectedPath = []int{} // Select root
				m.groupFocusPos = 0      // Focus on logic
				m.showLogicDropdown = false
			}
			m.recalculateTableHeight()
			return m, nil
		}

		// Handle Tab/Shift+Tab keys for switching focus (when overview mode is active)
		// BUT: Don't intercept if table is in filter mode - let the filter input handle it
		if m.overviewMode && (msg.String() == "tab" || msg.String() == "shift+tab") && !m.table.IsFiltering() {
			m.focusOverview = !m.focusOverview
			return m, nil
		}

		// Handle overview navigation when overview has focus
		if m.overviewMode && m.focusOverview {
			return m.handleOverviewKey(msg)
		}

		// Handle details view
		if m.showDetails {
			// Calculate total selectable fields:
			// 0 = Time, 1 = Level (if present), 2 = Message, 3+ = AllFields
			hasLevel := m.selectedEntry.Level != ""
			fixedFieldCount := 2 // Time, Message
			if hasLevel {
				fixedFieldCount = 3 // Time, Level, Message
			}
			totalFields := fixedFieldCount + len(m.detailsFieldNames)

			switch msg.String() {
			case "esc", "q":
				m.showDetails = false
				return m, nil
			case "up", "k":
				if m.detailsSelectedIdx > 0 {
					m.detailsSelectedIdx--
				}
				return m, nil
			case "down", "j":
				if m.detailsSelectedIdx < totalFields-1 {
					m.detailsSelectedIdx++
				}
				return m, nil
			case "enter":
				// Add selected field as filter (field = value)
				var fieldName, fieldValue string
				if hasLevel {
					switch m.detailsSelectedIdx {
					case 0: // Time
						fieldName = m.config.TimeField
						fieldValue = m.selectedEntry.Time.Format("2006-01-02 15:04:05")
					case 1: // Level
						fieldName = m.config.LevelField
						fieldValue = m.selectedEntry.Level
					case 2: // Message
						fieldName = m.config.MessageField
						fieldValue = m.selectedEntry.Message
					default: // AllFields
						idx := m.detailsSelectedIdx - 3
						if idx >= 0 && idx < len(m.detailsFieldNames) {
							fieldName = m.detailsFieldNames[idx]
							value := m.selectedEntry.AllFields[fieldName]
							fieldValue = fmt.Sprintf("%v", value)
						}
					}
				} else {
					switch m.detailsSelectedIdx {
					case 0: // Time
						fieldName = m.config.TimeField
						fieldValue = m.selectedEntry.Time.Format("2006-01-02 15:04:05")
					case 1: // Message
						fieldName = m.config.MessageField
						fieldValue = m.selectedEntry.Message
					default: // AllFields
						idx := m.detailsSelectedIdx - 2
						if idx >= 0 && idx < len(m.detailsFieldNames) {
							fieldName = m.detailsFieldNames[idx]
							value := m.selectedEntry.AllFields[fieldName]
							fieldValue = fmt.Sprintf("%v", value)
						}
					}
				}

				if fieldName != "" {
					// Add filter to root group using tree-based structure
					m.addConditionToSelected(fieldName, "=", fieldValue)
					m.showDetails = false
					m.showFilterForm = true // Show filter form so user can see the added filter
					m.recalculateTableHeight()
				}
				return m, nil
			}
			return m, nil
		}

		// Normal table mode keys (or overview mode with table focus)
		switch msg.String() {
		case "esc", "q":
			// If overview is hidden, show it
			if !m.overviewMode {
				m.overviewMode = true
				m.focusOverview = true
				m.selectedLevel = 0
				m.selectedBucket = 0
				m.recalculateTableHeight()
				return m, nil
			}
			// If overview is already visible, exit to main menu
			return m, func() tea.Msg {
				return tea.KeyMsg{Type: tea.KeyEsc}
			}
		case "0":
			// Toggle overview mode (works even without data)
			// Note: Ctrl+0 is not a valid terminal sequence, so we use just "0"
			if !m.overviewMode {
				m.overviewMode = true
				m.focusOverview = true
				m.selectedLevel = 0
				m.selectedBucket = 0
				m.recalculateTableHeight()

				// Trigger overview fetch if we have time range and no data yet
				if m.app != nil && !m.app.state.FromTime.IsZero() && !m.app.state.ToTime.IsZero() && len(m.levelTimeSeries) == 0 {
					contentWidth := m.width - 2
					sparklineRowLabelWidth := 14
					sparklineWidth := contentWidth - sparklineRowLabelWidth
					if sparklineWidth < 40 {
						sparklineWidth = 40
					}
					if sparklineWidth > 200 {
						sparklineWidth = 200
					}
					return m, m.app.fetchOverviewDataCmd(m.config, m.app.state.FromTime, m.app.state.ToTime, sparklineWidth, m.rootFilter)
				}
				return m, nil
			}
			// Toggle overview visibility off
			m.overviewMode = false
			m.focusOverview = false
			m.recalculateTableHeight()
			return m, nil
		case "enter":
			// Don't handle Enter if table is in filter mode - let table widget handle it
			if m.table.IsFiltering() {
				// Pass through to table widget to exit filter mode
				break
			}
			// Show details for selected row
			selected := m.table.HighlightedRow()
			if selected.Data != nil && len(m.entries) > 0 {
				// Find corresponding entry by matching time, message, and level
				// This ensures we find the exact entry even if multiple entries have the same timestamp
				timeData, ok := selected.Data["time"]
				if !ok || timeData == nil {
					return m, nil
				}
				timeStr, ok := timeData.(string)
				if !ok {
					return m, nil
				}

				// Get plain message and level for unique matching
				plainMsgData, _ := selected.Data["_plain_msg"]
				plainMsg, _ := plainMsgData.(string)
				levelData, _ := selected.Data["_level"]
				level, _ := levelData.(string)

				// Search for matching entry
				for _, entry := range m.entries {
					if entry.Time.Format("2006-01-02 15:04:05.000") == timeStr &&
						entry.Message == plainMsg &&
						entry.Level == level {
						m.selectedEntry = entry
						m.showDetails = true
						// Initialize details navigation
						m.detailsSelectedIdx = 0 // Start with Time field selected
						// Cache sorted field names for navigation
						m.detailsFieldNames = make([]string, 0, len(entry.AllFields))
						for fieldName := range entry.AllFields {
							m.detailsFieldNames = append(m.detailsFieldNames, fieldName)
						}
						// Sort field names
						for i := 0; i < len(m.detailsFieldNames)-1; i++ {
							for j := i + 1; j < len(m.detailsFieldNames); j++ {
								if m.detailsFieldNames[i] > m.detailsFieldNames[j] {
									m.detailsFieldNames[i], m.detailsFieldNames[j] = m.detailsFieldNames[j], m.detailsFieldNames[i]
								}
							}
						}
						break
					}
				}
			}
			return m, nil
		case "ctrl+n":
			// Load next window (older records)
			if !m.loading && m.app != nil {
				m.loading = true
				newOffset := m.offset + m.config.WindowSize
				m.offset = newOffset
				log.Debug().
					Int("new_offset", newOffset).
					Msg("Loading next window (Ctrl+N)")
				return m, m.app.fetchLogsDataCmd(m.config, newOffset, m.rootFilter, m.width)
			}
			return m, nil
		case "ctrl+p":
			// Load previous window (newer records)
			if !m.loading && m.app != nil && m.offset > 0 {
				m.loading = true
				newOffset := m.offset - m.config.WindowSize
				if newOffset < 0 {
					newOffset = 0
				}
				m.offset = newOffset
				log.Debug().
					Int("new_offset", newOffset).
					Msg("Loading previous window (Ctrl+P)")
				return m, m.app.fetchLogsDataCmd(m.config, newOffset, m.rootFilter, m.width)
			}
			return m, nil
		}
	}

	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

// handleFilterFormKey handles key events when filter form is active
func (m logsViewer) handleFilterFormKey(msg tea.Msg) (tea.Model, tea.Cmd) {
	keyMsg, ok := msg.(tea.KeyMsg)
	if !ok {
		return m, nil
	}

	// Handle logic dropdown mode
	if m.showLogicDropdown {
		return m.handleLogicDropdownKey(keyMsg)
	}

	// Handle edit mode
	if m.filterMode == "edit" {
		return m.handleEditModeKey(keyMsg)
	}

	// Tree navigation mode
	switch keyMsg.String() {
	case "ctrl+f", "esc":
		// Close filter form
		m.showFilterForm = false
		m.filterFieldDD.Blur()
		m.filterOperatorDD.Blur()
		m.filterValueInput.Blur()
		m.filterMode = "tree"
		m.showLogicDropdown = false
		m.applyFocused = false
		m.recalculateTableHeight()
		return m, nil

	case "up":
		flat := flattenTree(m.rootFilter, []int{})
		if m.applyFocused {
			// From Apply, go to last tree node
			m.applyFocused = false
			if len(flat) > 0 {
				m.selectedPath = flat[len(flat)-1].Path
				m.groupFocusPos = 0 // Reset to logic position
			}
		} else {
			// Move to previous node in depth-first order
			for i, item := range flat {
				if pathEqual(item.Path, m.selectedPath) {
					if i > 0 {
						m.selectedPath = flat[i-1].Path
						m.groupFocusPos = 0 // Reset to logic position
					}
					// If already at first node (i==0), stay there
					break
				}
			}
		}
		return m, nil

	case "down":
		flat := flattenTree(m.rootFilter, []int{})
		if m.applyFocused {
			// From Apply, wrap to root (first node)
			m.applyFocused = false
			if len(flat) > 0 {
				m.selectedPath = flat[0].Path
				m.groupFocusPos = 0 // Reset to logic position
			}
		} else {
			// Find current position
			for i, item := range flat {
				if pathEqual(item.Path, m.selectedPath) {
					if i < len(flat)-1 {
						// Move to next node
						m.selectedPath = flat[i+1].Path
						m.groupFocusPos = 0 // Reset to logic position
					} else {
						// At last node, go to Apply button
						m.applyFocused = true
					}
					break
				}
			}
		}
		return m, nil

	case "left":
		// Navigate within group or go to parent
		if !m.applyFocused {
			node := m.getSelectedNode()
			if node != nil && node.IsGroup && m.groupFocusPos > 0 {
				// Move left within group elements
				m.groupFocusPos--
			} else if len(m.selectedPath) > 0 {
				// Go to parent group
				m.selectedPath = getParentPath(m.selectedPath)
				m.groupFocusPos = 0 // Reset to logic position
			}
		}
		return m, nil

	case "right":
		// Navigate within group or go to first child
		if !m.applyFocused {
			node := m.getSelectedNode()
			if node != nil && node.IsGroup {
				if m.groupFocusPos < 2 {
					// Move right within group elements
					m.groupFocusPos++
				} else if len(node.Children) > 0 {
					// At rightmost element, go to first child
					m.selectedPath = append(m.selectedPath, 0)
					m.groupFocusPos = 0 // Reset to logic position for child
				}
			}
		}
		return m, nil

	case "enter":
		if m.applyFocused {
			// Apply filters
			if m.filtersChanged && m.app != nil {
				m.loading = true
				m.offset = 0
				m.filtersChanged = false
				// Reset sparklines to force refresh with new filters
				m.levelTimeSeries = nil
				m.timeLabels = nil
				sparklineWidth := m.width - 20
				if sparklineWidth < 40 {
					sparklineWidth = 40
				}
				return m, tea.Batch(
					m.app.fetchLogsDataCmd(m.config, 0, m.rootFilter, m.width),
					m.app.fetchOverviewDataCmd(m.config, m.app.state.FromTime, m.app.state.ToTime, sparklineWidth, m.rootFilter),
				)
			}
			return m, nil
		}
		// Handle Enter based on selected element
		node := m.getSelectedNode()
		if node == nil {
			node = m.rootFilter
		}
		if node.IsGroup {
			// Action depends on groupFocusPos
			switch m.groupFocusPos {
			case 0: // Logic dropdown
				m.showLogicDropdown = true
				m.filterMode = "logicDropdown"
				// Set current logic as selected
				logic := node.Logic
				if logic == "" {
					logic = "AND"
				}
				switch logic {
				case "AND":
					m.logicDropdownIdx = 0
				case "OR":
					m.logicDropdownIdx = 1
				case "NOT AND":
					m.logicDropdownIdx = 2
				case "NOT OR":
					m.logicDropdownIdx = 3
				default:
					m.logicDropdownIdx = 0
				}
			case 1: // Add condition
				m.filterMode = "edit"
				m.editFieldFocus = 0
				m.filterFieldDD.SetValue("")
				m.filterOperatorDD.SetValue("=")
				m.filterValueInput.SetValue("")
				m.filterFieldDD.Focus()
			case 2: // Add group
				m.addGroupToSelected("AND")
			}
		} else {
			// Enter edit mode for condition
			m.filterMode = "edit"
			m.editFieldFocus = 0
			m.filterFieldDD.SetValue(node.Field)
			m.filterOperatorDD.SetValue(node.Operator)
			m.filterValueInput.SetValue(node.Value)
			m.filterFieldDD.Focus()
		}
		return m, nil

	case "ctrl+a":
		// Add condition to current group directly
		node := m.getSelectedNode()
		if node == nil {
			node = m.rootFilter
		}
		// If selected is a condition, add to its parent
		if !node.IsGroup {
			parentPath := getParentPath(m.selectedPath)
			node = m.getNodeAtPath(parentPath)
			if node == nil {
				node = m.rootFilter
			}
		}
		if node.IsGroup {
			// Enter edit mode for new condition
			m.filterMode = "edit"
			m.editFieldFocus = 0
			m.filterFieldDD.SetValue("")
			m.filterOperatorDD.SetValue("=")
			m.filterValueInput.SetValue("")
			m.filterFieldDD.Focus()
		}
		return m, nil

	case "ctrl+g":
		// Add group inside current group directly
		m.addGroupToSelected("AND")
		return m, nil

	case "delete", "d":
		// Delete selected node
		if len(m.selectedPath) > 0 { // Can't delete root
			m.deleteSelectedNode()
		}
		return m, nil

	case "e":
		// Edit selected condition
		node := m.getSelectedNode()
		if node != nil && !node.IsGroup {
			m.filterMode = "edit"
			m.editFieldFocus = 0
			m.filterFieldDD.SetValue(node.Field)
			m.filterOperatorDD.SetValue(node.Operator)
			m.filterValueInput.SetValue(node.Value)
			m.filterFieldDD.Focus()
		}
		return m, nil

	case "tab":
		// Move focus to Apply button
		m.applyFocused = true
		return m, nil

	case "ctrl+enter":
		// Apply filters if there are changes
		if m.filtersChanged && m.app != nil {
			m.loading = true
			m.offset = 0 // Reset to first page when filter changes
			m.filtersChanged = false
			// Reset sparklines to force refresh with new filters
			m.levelTimeSeries = nil
			m.timeLabels = nil
			// Calculate sparkline width for overview refresh
			sparklineWidth := m.width - 20 // Approximate width available for sparklines
			if sparklineWidth < 40 {
				sparklineWidth = 40
			}
			// Fetch both logs data and overview data with filters
			return m, tea.Batch(
				m.app.fetchLogsDataCmd(m.config, 0, m.rootFilter, m.width),
				m.app.fetchOverviewDataCmd(m.config, m.app.state.FromTime, m.app.state.ToTime, sparklineWidth, m.rootFilter),
			)
		}
		return m, nil
	}

	return m, nil
}

// handleLogicDropdownKey handles key events when logic dropdown is shown
func (m logsViewer) handleLogicDropdownKey(keyMsg tea.KeyMsg) (tea.Model, tea.Cmd) {
	logicOptions := []string{"AND", "OR", "NOT AND", "NOT OR"}

	switch keyMsg.String() {
	case "up":
		if m.logicDropdownIdx > 0 {
			m.logicDropdownIdx--
		}
		return m, nil
	case "down":
		if m.logicDropdownIdx < len(logicOptions)-1 {
			m.logicDropdownIdx++
		}
		return m, nil
	case "1":
		m.logicDropdownIdx = 0
		return m, nil
	case "2":
		m.logicDropdownIdx = 1
		return m, nil
	case "3":
		m.logicDropdownIdx = 2
		return m, nil
	case "4":
		m.logicDropdownIdx = 3
		return m, nil
	case "enter":
		// Apply selected logic to current group
		node := m.getSelectedNode()
		if node == nil {
			node = m.rootFilter
		}
		if node.IsGroup {
			node.Logic = logicOptions[m.logicDropdownIdx]
			m.filtersChanged = true
		}
		m.showLogicDropdown = false
		m.filterMode = "tree"
		return m, nil
	case "esc":
		m.showLogicDropdown = false
		m.filterMode = "tree"
		return m, nil
	}
	return m, nil
}

// handleEditModeKey handles key events when editing a condition
func (m logsViewer) handleEditModeKey(keyMsg tea.KeyMsg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch keyMsg.String() {
	case "tab":
		// Move to next field: Field → Operator → Value → Save
		m.confirmCurrentFieldAndMoveNext()
		return m, nil

	case "shift+tab":
		// Move to previous field
		m.blurCurrentFilterField()
		if m.editFieldFocus == 0 {
			m.editFieldFocus = 2 // Wrap to Value
		} else {
			m.editFieldFocus--
		}
		m.focusCurrentFilterField()
		return m, nil

	case "enter":
		// On dropdowns: select item and move to next field
		// On value input or save button: save the condition
		switch m.editFieldFocus {
		case 0: // Field dropdown - select and move to operator
			m.confirmDropdownSelection(&m.filterFieldDD)
			m.filterFieldDD.Blur()
			m.editFieldFocus = 1
			m.filterOperatorDD.Focus()
			return m, nil
		case 1: // Operator dropdown - select and move to value
			m.confirmDropdownSelection(&m.filterOperatorDD)
			m.filterOperatorDD.Blur()
			m.editFieldFocus = 2
			m.filterValueInput.Focus()
			return m, nil
		case 2, 3: // Value input or Save button - save condition
			m.saveEditedCondition()
			m.filterMode = "tree"
			m.editFieldFocus = 0
			return m, nil
		}

	case "esc":
		// Cancel and return to tree mode
		m.filterMode = "tree"
		m.editFieldFocus = 0
		m.filterFieldDD.Blur()
		m.filterOperatorDD.Blur()
		m.filterValueInput.Blur()
		return m, nil
	}

	// Delegate to active field for other keys (up/down navigation, typing)
	switch m.editFieldFocus {
	case 0: // Field dropdown
		cmd, handled := m.filterFieldDD.Update(keyMsg)
		if handled {
			return m, cmd
		}
	case 1: // Operator dropdown
		cmd, handled := m.filterOperatorDD.Update(keyMsg)
		if handled {
			return m, cmd
		}
	case 2: // Value input
		m.filterValueInput, cmd = m.filterValueInput.Update(keyMsg)
		return m, cmd
	}

	return m, nil
}

// confirmDropdownSelection confirms the currently selected dropdown item
func (m *logsViewer) confirmDropdownSelection(d *dropdown) {
	if d.showOptions && len(d.filtered) > 0 {
		d.value = d.filtered[d.selected]
		d.input.SetValue(d.value)
	}
	d.showOptions = false
}

// confirmCurrentFieldAndMoveNext confirms current field and moves to next
func (m *logsViewer) confirmCurrentFieldAndMoveNext() {
	// Confirm current field
	switch m.editFieldFocus {
	case 0:
		m.confirmDropdownSelection(&m.filterFieldDD)
		m.filterFieldDD.Blur()
	case 1:
		m.confirmDropdownSelection(&m.filterOperatorDD)
		m.filterOperatorDD.Blur()
	case 2:
		m.filterValueInput.Blur()
	}

	// Move to next field
	m.editFieldFocus++
	if m.editFieldFocus > 3 {
		// Done - save and return to tree mode
		m.saveEditedCondition()
		m.filterMode = "tree"
		m.editFieldFocus = 0
		return
	}

	// Focus new field
	m.focusCurrentFilterField()
}

// saveEditedCondition saves the edited condition to the filter tree
func (m *logsViewer) saveEditedCondition() {
	field := m.filterFieldDD.value
	operator := m.filterOperatorDD.value
	value := m.filterValueInput.Value()

	// IS NULL and IS NOT NULL don't need a value
	isNullOperator := operator == "IS NULL" || operator == "IS NOT NULL"
	if field == "" || operator == "" || (value == "" && !isNullOperator) {
		return // Invalid condition
	}

	node := m.getSelectedNode()
	if node != nil && !node.IsGroup {
		// Update existing condition
		node.Field = field
		node.Operator = operator
		node.Value = value
		m.filtersChanged = true
	} else {
		// Add new condition to selected group
		m.addConditionToSelected(field, operator, value)
	}

	// Clear inputs for next time
	m.filterFieldDD.SetValue("")
	m.filterValueInput.SetValue("")
}

// blurCurrentFilterField removes focus from currently focused filter field
func (m *logsViewer) blurCurrentFilterField() {
	switch m.editFieldFocus {
	case 0:
		m.filterFieldDD.Blur()
	case 1:
		m.filterOperatorDD.Blur()
	case 2:
		m.filterValueInput.Blur()
	}
}

// focusCurrentFilterField sets focus to currently selected filter field
func (m *logsViewer) focusCurrentFilterField() {
	switch m.editFieldFocus {
	case 0:
		m.filterFieldDD.Focus()
	case 1:
		m.filterOperatorDD.Focus()
	case 2:
		m.filterValueInput.Focus()
	}
}

// handleOverviewKey handles key events when in overview/sparkline navigation mode
func (m logsViewer) handleOverviewKey(msg tea.Msg) (tea.Model, tea.Cmd) {
	keyMsg, ok := msg.(tea.KeyMsg)
	if !ok {
		return m, nil
	}

	// Get available levels for navigation
	priorityLevels := []string{"error", "warning", "info", "debug"}
	availableLevels := []string{}
	for _, level := range priorityLevels {
		if values, exists := m.levelTimeSeries[level]; exists && len(values) > 0 {
			availableLevels = append(availableLevels, level)
		}
	}

	if len(availableLevels) == 0 || len(m.timeLabels) == 0 {
		m.overviewMode = false
		return m, nil
	}

	// Get the current level name and its data length for bounds checking
	var currentLevelDataLen int
	if m.selectedLevel < len(availableLevels) {
		currentLevelName := availableLevels[m.selectedLevel]
		currentLevelDataLen = len(m.levelTimeSeries[currentLevelName])
	} else {
		currentLevelDataLen = len(m.timeLabels)
	}

	switch keyMsg.String() {
	case "up", "k":
		// Navigate to previous level
		if m.selectedLevel > 0 {
			m.selectedLevel--
			// Adjust bucket if it's beyond the new level's data length
			newLevelName := availableLevels[m.selectedLevel]
			newLevelDataLen := len(m.levelTimeSeries[newLevelName])
			if m.selectedBucket >= newLevelDataLen {
				m.selectedBucket = newLevelDataLen - 1
			}
		}

	case "down", "j":
		// Navigate to next level
		if m.selectedLevel < len(availableLevels)-1 {
			m.selectedLevel++
			// Adjust bucket if it's beyond the new level's data length
			newLevelName := availableLevels[m.selectedLevel]
			newLevelDataLen := len(m.levelTimeSeries[newLevelName])
			if m.selectedBucket >= newLevelDataLen {
				m.selectedBucket = newLevelDataLen - 1
			}
		}

	case "left", "h":
		// Navigate to previous bucket
		if m.selectedBucket > 0 {
			m.selectedBucket--
		}

	case "right", "l":
		// Navigate to next bucket (use current level's data length)
		if m.selectedBucket < currentLevelDataLen-1 {
			m.selectedBucket++
		}

	case "home":
		// Jump to first bucket
		m.selectedBucket = 0

	case "end":
		// Jump to last bucket (use current level's data length)
		m.selectedBucket = currentLevelDataLen - 1

	case "enter":
		// Show zoom menu
		m.showZoomMenu = true
		m.zoomMenuIdx = 0
		return m, nil
	}

	return m, nil
}

// handleZoomMenuKey handles key events when zoom menu is shown
func (m logsViewer) handleZoomMenuKey(msg tea.Msg) (tea.Model, tea.Cmd) {
	keyMsg, ok := msg.(tea.KeyMsg)
	if !ok {
		return m, nil
	}

	menuOptions := 4 // Zoom in, Zoom out, Reset, Cancel

	switch keyMsg.String() {
	case "esc", "q":
		// Close menu
		m.showZoomMenu = false
		return m, nil

	case "up", "k":
		// Navigate up in menu
		if m.zoomMenuIdx > 0 {
			m.zoomMenuIdx--
		}

	case "down", "j":
		// Navigate down in menu
		if m.zoomMenuIdx < menuOptions-1 {
			m.zoomMenuIdx++
		}

	case "enter":
		// Execute selected action
		return m.executeZoomAction()
	}

	return m, nil
}

// executeZoomAction performs the selected zoom menu action
func (m logsViewer) executeZoomAction() (tea.Model, tea.Cmd) {
	m.showZoomMenu = false

	switch m.zoomMenuIdx {
	case 0: // Zoom to this time bucket
		return m.zoomToBucket()
	case 1: // Zoom out (restore previous)
		return m.zoomOut()
	case 2: // Reset to original range
		return m.resetZoom()
	case 3: // Cancel
		return m, nil
	}

	return m, nil
}

// zoomToBucket zooms into the selected time bucket
func (m logsViewer) zoomToBucket() (tea.Model, tea.Cmd) {
	if m.app == nil || m.bucketInterval == 0 || m.selectedBucket < 0 {
		return m, nil
	}

	// Save current range to zoom stack for zoom out
	currentRange := timeRange{
		from: m.app.state.FromTime,
		to:   m.app.state.ToTime,
	}

	// Save original range if this is the first zoom
	if len(m.zoomStack) == 0 {
		m.originalRange = currentRange
	}
	m.zoomStack = append(m.zoomStack, currentRange)

	// Calculate new time range for selected bucket
	bucketStart := m.app.state.FromTime.Add(time.Duration(m.selectedBucket*m.bucketInterval) * time.Second)
	bucketEnd := bucketStart.Add(time.Duration(m.bucketInterval) * time.Second)

	log.Debug().
		Int("bucket_index", m.selectedBucket).
		Int("bucket_interval", m.bucketInterval).
		Time("bucket_start", bucketStart).
		Time("bucket_end", bucketEnd).
		Msg(">>> Zooming to bucket")

	// Update app state with new time range
	m.app.state.FromTime = bucketStart
	m.app.state.ToTime = bucketEnd

	// Reset pagination and clear current data
	m.offset = 0
	m.entries = nil
	m.levelTimeSeries = nil
	m.timeLabels = nil
	m.selectedBucket = 0
	m.loading = true

	// Fetch new data for the zoomed time range
	return m, m.app.fetchLogsDataCmd(m.config, 0, m.rootFilter, m.width)
}

// zoomOut restores the previous time range from zoom stack
func (m logsViewer) zoomOut() (tea.Model, tea.Cmd) {
	if m.app == nil || len(m.zoomStack) == 0 {
		return m, nil
	}

	// Pop the last range from the stack
	prevRange := m.zoomStack[len(m.zoomStack)-1]
	m.zoomStack = m.zoomStack[:len(m.zoomStack)-1]

	log.Debug().
		Time("from", prevRange.from).
		Time("to", prevRange.to).
		Int("stack_depth", len(m.zoomStack)).
		Msg(">>> Zooming out to previous range")

	// Update app state with previous time range
	m.app.state.FromTime = prevRange.from
	m.app.state.ToTime = prevRange.to

	// Reset pagination and clear current data
	m.offset = 0
	m.entries = nil
	m.levelTimeSeries = nil
	m.timeLabels = nil
	m.selectedBucket = 0
	m.loading = true

	// Fetch new data for the restored time range
	return m, m.app.fetchLogsDataCmd(m.config, 0, m.rootFilter, m.width)
}

// resetZoom resets to the original time range
func (m logsViewer) resetZoom() (tea.Model, tea.Cmd) {
	if m.app == nil || m.originalRange.from.IsZero() {
		return m, nil
	}

	log.Debug().
		Time("from", m.originalRange.from).
		Time("to", m.originalRange.to).
		Msg(">>> Resetting to original range")

	// Update app state with original time range
	m.app.state.FromTime = m.originalRange.from
	m.app.state.ToTime = m.originalRange.to

	// Clear zoom stack
	m.zoomStack = nil

	// Reset pagination and clear current data
	m.offset = 0
	m.entries = nil
	m.levelTimeSeries = nil
	m.timeLabels = nil
	m.selectedBucket = 0
	m.loading = true

	// Fetch new data for the original time range
	return m, m.app.fetchLogsDataCmd(m.config, 0, m.rootFilter, m.width)
}

func (m logsViewer) View() string {
	if m.loading {
		return "Loading logs, please wait..."
	}
	if m.err != nil {
		return fmt.Sprintf("Error loading logs: %v\n\nPress ESC to return", m.err)
	}

	if m.showDetails {
		return m.renderDetails()
	}

	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))

	// Set overview box to use full available width
	// Width is content width (screen minus borders: left + right = 2)
	overviewContentWidth := m.width - 2
	if overviewContentWidth < 40 {
		overviewContentWidth = 40
	}

	// Change border color based on focus state
	borderColor := lipgloss.Color("240") // Default: gray when blurred
	if m.overviewMode && m.focusOverview {
		borderColor = lipgloss.Color("15") // White when focused
	}

	borderStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(borderColor).
		Width(overviewContentWidth)

	// Title with time range and pagination info
	pageNum := (m.offset / m.config.WindowSize) + 1
	title := fmt.Sprintf("Log Entries | Page %d (offset: %d, window: %d) | From: %s To: %s",
		pageNum,
		m.offset,
		m.config.WindowSize,
		m.firstEntryTime.Format("2006-01-02 15:04:05.000 MST"),
		m.lastEntryTime.Format("2006-01-02 15:04:05.000 MST"))

	// Set table border color based on focus state
	tableBorderColor := lipgloss.Color("15") // White (focused)
	if m.overviewMode && m.focusOverview {
		tableBorderColor = lipgloss.Color("240") // Gray (blurred)
	}
	m.table.SetBorderColor(tableBorderColor)

	// Render title and count actual lines (title might wrap on narrow terminals)
	renderedTitle := titleStyle.Render(title)
	titleLines := strings.Count(renderedTitle, "\n") + 1

	// Render table view once to avoid multiple calls
	tableView := m.table.View()
	tableViewLines := strings.Count(tableView, "\n") + 1

	// Build overview with timeline bar (only if overview mode is active)
	var content string
	var overviewRenderedLines int
	var filterFormLines int

	// Build components list
	components := []string{renderedTitle}

	// Always show compact filter summary when filters are active (filter form is now a modal overlay)
	if m.getTotalFilterCount() > 0 {
		filterSummary := m.renderFilterSummary()
		filterFormLines = 1
		components = append(components, filterSummary)
	}

	// Add overview if visible
	if m.overviewMode {
		overview := m.renderOverview()
		overviewRendered := borderStyle.Render(overview)
		overviewRenderedLines = strings.Count(overviewRendered, "\n") + 1
		components = append(components, overviewRendered)
	} else {
		overviewRenderedLines = 0
	}

	// Add table
	components = append(components, tableView)

	// Add main help line at the bottom
	mainHelpLine := m.renderMainHelpLine()
	components = append(components, mainHelpLine)

	// Join all parts using lipgloss to avoid extra newlines
	// lipgloss.JoinVertical adds exactly one newline between components
	content = lipgloss.JoinVertical(lipgloss.Left, components...)

	// Count actual rendered lines
	totalContentLines := strings.Count(content, "\n") + 1

	// Detect and log overflow
	overflowAmount := totalContentLines - m.height
	hasOverflow := overflowAmount > 0

	// Debug logging with detailed breakdown
	mainHelpLines := 1 // Main help line is always 1 line
	log.Debug().
		Int("screen_height", m.height).
		Int("title_lines", titleLines).
		Int("filter_form_lines", filterFormLines).
		Int("overview_rendered_lines", overviewRenderedLines).
		Int("table_height_allocated", m.tableHeight).
		Int("table_view_actual_lines", tableViewLines).
		Int("main_help_lines", mainHelpLines).
		Int("total_content_lines", totalContentLines).
		Int("expected_total", titleLines+filterFormLines+overviewRenderedLines+tableViewLines+mainHelpLines).
		Int("overflow_amount", overflowAmount).
		Int("missing_lines", (titleLines+filterFormLines+overviewRenderedLines+tableViewLines+mainHelpLines)-totalContentLines).
		Bool("overflow", hasOverflow).
		Bool("overview_mode", m.overviewMode).
		Bool("focus_overview", m.focusOverview).
		Bool("show_filter_form", m.showFilterForm).
		Str("calculation", fmt.Sprintf("%d(title) + %d(filter) + %d(overview) + %d(table) + %d(help) = %d(expected) vs %d(actual)",
			titleLines, filterFormLines, overviewRenderedLines, tableViewLines, mainHelpLines,
			titleLines+filterFormLines+overviewRenderedLines+tableViewLines+mainHelpLines, totalContentLines)).
		Str("title_text", title).
		Int("title_text_length", len(title)).
		Int("table_widget_overhead_actual", tableViewLines-m.tableHeight).
		Msg(">>> Logs layout line counts")

	// Measure actual widget overhead for next calculation
	// actual overhead = how many lines it rendered - how many we allocated
	actualWidgetOverhead := tableViewLines - m.tableHeight

	// Update measured overhead if it changed (for next render)
	if actualWidgetOverhead != m.measuredWidgetOverhead {
		log.Debug().
			Int("old_measured_overhead", m.measuredWidgetOverhead).
			Int("new_measured_overhead", actualWidgetOverhead).
			Msg("Widget overhead measurement updated")
		// Note: We can't update m here because View() has value receiver
		// This will be updated on next Update() cycle
	}

	// If overflow detected, log warning
	if hasOverflow {
		log.Warn().
			Int("overflow_lines", overflowAmount).
			Int("current_widget_overhead", m.measuredWidgetOverhead).
			Int("actual_widget_overhead", actualWidgetOverhead).
			Msg("Layout overflow detected - will adjust on next render")
	}

	// Show zoom menu instead of normal content when active
	if m.showZoomMenu {
		// Render base content dimmed in background
		dimmedContent := lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render(content)
		// Overlay menu on top
		return dimmedContent + "\n" + m.renderZoomMenu()
	}

	// Show filter form modal when active
	if m.showFilterForm {
		// Render base content dimmed in background
		dimmedContent := lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render(content)
		// Overlay filter modal on top
		return dimmedContent + "\n" + m.renderFilterFormModal()
	}

	// Debug: log the first 3 lines of content to verify title is included
	lines := strings.Split(content, "\n")
	var preview string
	if len(lines) > 3 {
		preview = strings.Join(lines[:3], " | ")
	} else {
		preview = strings.Join(lines, " | ")
	}
	log.Debug().
		Str("first_3_lines", preview).
		Int("total_lines", len(lines)).
		Msg(">>> Logs View() output preview")

	return content
}

func (m logsViewer) renderFilterForm() string {
	if !m.showFilterForm {
		return ""
	}

	var builder strings.Builder

	// Title
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("14")) // Cyan
	builder.WriteString(titleStyle.Render("Filters"))
	builder.WriteString("\n\n")

	// Render the filter tree
	if m.rootFilter != nil {
		builder.WriteString(m.renderFilterNode(m.rootFilter, 0, []int{}))
	}

	// Show edit form when in edit mode
	if m.filterMode == "edit" {
		builder.WriteString("\n")
		builder.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("14")).Bold(true).Render("Edit Condition:"))
		builder.WriteString("\n")

		// Field labels
		fieldLabel := lipgloss.NewStyle().Foreground(lipgloss.Color("yellow")).Render("Field: ")
		operatorLabel := lipgloss.NewStyle().Foreground(lipgloss.Color("yellow")).Render("Operator: ")
		valueLabel := lipgloss.NewStyle().Foreground(lipgloss.Color("yellow")).Render("Value: ")

		// Field input with focus highlight
		fieldStyle := lipgloss.NewStyle()
		if m.editFieldFocus == 0 {
			fieldStyle = fieldStyle.Background(lipgloss.Color("6")).Foreground(lipgloss.Color("0"))
		}
		fieldView := fieldStyle.Render(m.filterFieldDD.input.View())

		// Operator input with focus highlight
		operatorStyle := lipgloss.NewStyle()
		if m.editFieldFocus == 1 {
			operatorStyle = operatorStyle.Background(lipgloss.Color("6")).Foreground(lipgloss.Color("0"))
		}
		operatorView := operatorStyle.Render(m.filterOperatorDD.input.View())

		// Value input with focus highlight
		valueStyle := lipgloss.NewStyle()
		if m.editFieldFocus == 2 {
			valueStyle = valueStyle.Background(lipgloss.Color("6")).Foreground(lipgloss.Color("0"))
		}
		valueView := valueStyle.Render(m.filterValueInput.View())

		// Save button with focus highlight
		saveStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("10")) // Green
		if m.editFieldFocus == 3 {
			saveStyle = saveStyle.Background(lipgloss.Color("10")).Foreground(lipgloss.Color("0")).Bold(true)
		}
		saveButton := saveStyle.Render(" [Save] ")

		builder.WriteString(fieldLabel + fieldView + "  " + operatorLabel + operatorView + "  " + valueLabel + valueView + "  " + saveButton)
		builder.WriteString("\n")

		// Show field dropdown options
		if m.editFieldFocus == 0 && m.filterFieldDD.showOptions && len(m.filterFieldDD.filtered) > 0 {
			builder.WriteString(m.renderFieldDropdown())
		}

		// Show operator dropdown options
		if m.editFieldFocus == 1 && m.filterOperatorDD.showOptions && len(m.filterOperatorDD.filtered) > 0 {
			builder.WriteString(m.renderOperatorDropdown())
		}
	}

	// Show logic dropdown when in logicDropdown mode
	if m.showLogicDropdown {
		builder.WriteString("\n")
		menuStyle := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("6")).Padding(0, 1)

		var menuContent strings.Builder
		menuContent.WriteString(lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("14")).Render("Select logic:"))
		menuContent.WriteString("\n")

		logicOptions := []string{"AND", "OR", "NOT AND", "NOT OR"}
		for i, opt := range logicOptions {
			optStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("6"))
			if m.logicDropdownIdx == i {
				optStyle = optStyle.Background(lipgloss.Color("6")).Foreground(lipgloss.Color("0"))
			}
			menuContent.WriteString(optStyle.Render(fmt.Sprintf(" %d. %s ", i+1, opt)))
			menuContent.WriteString("\n")
		}

		builder.WriteString(menuStyle.Render(menuContent.String()))
		builder.WriteString("\n")
	}

	// Apply button
	builder.WriteString("\n")
	applyStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("10")) // Green
	if m.filterMode == "tree" && m.applyFocused {
		// Apply button is focused
		applyStyle = applyStyle.Background(lipgloss.Color("10")).Foreground(lipgloss.Color("0"))
	}
	if m.filtersChanged {
		applyStyle = applyStyle.Bold(true)
		builder.WriteString(applyStyle.Render(" [Apply*] "))
	} else {
		builder.WriteString(applyStyle.Render(" [Apply] "))
	}
	builder.WriteString("\n")

	// Help text - context-sensitive
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	var helpText string
	switch m.filterMode {
	case "edit":
		helpText = "Enter: Next field (on Value/Save: save) | Tab: Next field | ↑/↓: Select option | Esc: Cancel"
	case "logicDropdown":
		helpText = "↑/↓ or 1-4: Select logic | Enter: Confirm | Esc: Cancel"
	default: // tree mode
		if m.applyFocused {
			helpText = "↑: Back to filters | Enter: Apply filters | Esc: Close"
		} else {
			helpText = "←/→: Switch element | ↑/↓: Navigate | Enter: Action | d: Delete | Ctrl+Enter: Apply | Esc: Close"
		}
	}
	builder.WriteString(helpStyle.Render(helpText))

	return builder.String()
}

// renderFilterFormModal renders the filter form as a centered modal dialog
func (m logsViewer) renderFilterFormModal() string {
	if !m.showFilterForm {
		return ""
	}

	// Get the filter form content
	filterContent := m.renderFilterForm()

	// Calculate modal width - use 80% of screen width, min 60, max 100
	modalWidth := int(float64(m.width) * 0.8)
	if modalWidth < 60 {
		modalWidth = 60
	}
	if modalWidth > 100 {
		modalWidth = 100
	}

	// Wrap in modal border
	modalStyle := lipgloss.NewStyle().
		Border(lipgloss.DoubleBorder()).
		BorderForeground(lipgloss.Color("6")). // Cyan border
		Padding(1, 2).
		Width(modalWidth)

	modalBox := modalStyle.Render(filterContent)

	// Center the modal on screen
	return lipgloss.Place(
		m.width,
		m.height,
		lipgloss.Center,
		lipgloss.Center,
		modalBox,
	)
}

// renderFilterNode renders a single node in the filter tree recursively
func (m logsViewer) renderFilterNode(node *FilterNode, depth int, path []int) string {
	indent := strings.Repeat("    ", depth)
	var result strings.Builder
	isSelected := pathEqual(path, m.selectedPath) && m.filterMode == "tree"

	if node.IsGroup {
		// Delete button (not for root)
		if depth > 0 {
			deleteStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("9")) // Red
			result.WriteString(indent)
			result.WriteString(deleteStyle.Render("[×]"))
			result.WriteString(" ")
		} else {
			result.WriteString(indent)
		}

		// Group indicator and logic - focusable element 0
		logic := node.Logic
		if logic == "" {
			logic = "AND"
		}
		logicStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("6")).Bold(true) // Cyan
		if isSelected && m.groupFocusPos == 0 {
			logicStyle = logicStyle.Background(lipgloss.Color("6")).Foreground(lipgloss.Color("0"))
		}
		result.WriteString(logicStyle.Render(fmt.Sprintf("▼ [%s]", logic)))

		// Separator line
		result.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Render(" ── "))

		// Add condition button - focusable element 1
		addCondStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("10")) // Green
		if isSelected && m.groupFocusPos == 1 {
			addCondStyle = addCondStyle.Background(lipgloss.Color("10")).Foreground(lipgloss.Color("0"))
		}
		result.WriteString(addCondStyle.Render("[Add condition]"))
		result.WriteString(" ")

		// Add group button - focusable element 2
		addGroupStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("10")) // Green
		if isSelected && m.groupFocusPos == 2 {
			addGroupStyle = addGroupStyle.Background(lipgloss.Color("10")).Foreground(lipgloss.Color("0"))
		}
		result.WriteString(addGroupStyle.Render("[Add group]"))
		result.WriteString("\n")

		// Render children recursively
		for i, child := range node.Children {
			childPath := make([]int, len(path)+1)
			copy(childPath, path)
			childPath[len(path)] = i
			result.WriteString(m.renderFilterNode(child, depth+1, childPath))
		}

		// Show empty placeholder if no children
		if len(node.Children) == 0 {
			emptyStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Italic(true)
			result.WriteString(indent + "    ")
			result.WriteString(emptyStyle.Render("(empty - press Enter or Ctrl+A to add condition)"))
			result.WriteString("\n")
		}
	} else {
		// Condition: delete button + field operator value
		deleteStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
		condStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("11")) // Yellow
		if isSelected {
			condStyle = condStyle.Background(lipgloss.Color("11")).Foreground(lipgloss.Color("0"))
		}

		result.WriteString(indent)
		result.WriteString(deleteStyle.Render("[×]"))
		result.WriteString(" ")

		// Format condition text
		var condText string
		if node.Operator == "IS NULL" || node.Operator == "IS NOT NULL" {
			condText = fmt.Sprintf("%s %s", node.Field, node.Operator)
		} else {
			condText = fmt.Sprintf("%s %s %s", node.Field, node.Operator, node.Value)
		}
		result.WriteString(condStyle.Render(condText))
		result.WriteString("\n")
	}

	return result.String()
}

// renderFieldDropdown renders the field dropdown options
func (m logsViewer) renderFieldDropdown() string {
	var builder strings.Builder
	maxVisible := 10
	total := len(m.filterFieldDD.filtered)
	selected := m.filterFieldDD.selected

	// Calculate scroll offset
	scrollOffset := 0
	if selected >= maxVisible {
		scrollOffset = selected - maxVisible/2
		if scrollOffset < 0 {
			scrollOffset = 0
		}
		if scrollOffset+maxVisible > total {
			scrollOffset = total - maxVisible
			if scrollOffset < 0 {
				scrollOffset = 0
			}
		}
	}

	builder.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render(
		fmt.Sprintf("Fields (%d total, ↑↓ to navigate):", total)))
	builder.WriteString("\n")

	if scrollOffset > 0 {
		builder.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Render(
			fmt.Sprintf("  ↑ %d more above", scrollOffset)))
		builder.WriteString("\n")
	}

	endIdx := scrollOffset + maxVisible
	if endIdx > total {
		endIdx = total
	}

	for i := scrollOffset; i < endIdx; i++ {
		opt := m.filterFieldDD.filtered[i]
		if i == selected {
			builder.WriteString(lipgloss.NewStyle().
				Background(lipgloss.Color("6")).
				Foreground(lipgloss.Color("0")).
				Render(fmt.Sprintf("▶ %s", opt)))
		} else {
			builder.WriteString(fmt.Sprintf("  %s", opt))
		}
		builder.WriteString("\n")
	}

	if endIdx < total {
		builder.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Render(
			fmt.Sprintf("  ↓ %d more below", total-endIdx)))
		builder.WriteString("\n")
	}

	return builder.String()
}

// renderOperatorDropdown renders the operator dropdown options
func (m logsViewer) renderOperatorDropdown() string {
	var builder strings.Builder
	builder.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render("Operators: "))
	for i, opt := range m.filterOperatorDD.filtered {
		if i == m.filterOperatorDD.selected {
			builder.WriteString(lipgloss.NewStyle().Background(lipgloss.Color("240")).Foreground(lipgloss.Color("15")).Render(opt))
		} else {
			builder.WriteString(opt)
		}
		if i < len(m.filterOperatorDD.filtered)-1 {
			builder.WriteString(" | ")
		}
	}
	builder.WriteString("\n")
	return builder.String()
}

// renderFilterSummary renders a compact one-line summary of active filters
func (m logsViewer) renderFilterSummary() string {
	totalFilters := m.getTotalFilterCount()
	if totalFilters == 0 {
		return ""
	}

	var builder strings.Builder

	// Title with filter count
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("14")) // Cyan
	builder.WriteString(titleStyle.Render(fmt.Sprintf("Active Filters (%d)", totalFilters)))
	builder.WriteString(": ")

	// Build compact filter representation from tree
	if m.rootFilter != nil {
		builder.WriteString(m.renderNodeSummary(m.rootFilter))
	}

	// Add hint to expand
	hintStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	builder.WriteString(hintStyle.Render("  [Ctrl+F to edit]"))

	return builder.String()
}

// renderNodeSummary renders a compact summary of a filter node
func (m logsViewer) renderNodeSummary(node *FilterNode) string {
	filterStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("11"))    // Yellow
	combinatorStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("6")) // Cyan

	if !node.IsGroup {
		// Render single condition
		var filterText string
		if node.Operator == "IS NULL" || node.Operator == "IS NOT NULL" {
			filterText = fmt.Sprintf("%s %s", node.Field, node.Operator)
		} else {
			value := node.Value
			if len(value) > 15 {
				value = value[:12] + "..."
			}
			filterText = fmt.Sprintf("%s %s %s", node.Field, node.Operator, value)
		}
		return filterStyle.Render(filterText)
	}

	// Render group
	if len(node.Children) == 0 {
		return ""
	}

	var parts []string
	for _, child := range node.Children {
		part := m.renderNodeSummary(child)
		if part != "" {
			parts = append(parts, part)
		}
	}

	if len(parts) == 0 {
		return ""
	}

	logic := node.Logic
	if logic == "" {
		logic = "AND"
	}

	// Handle NOT logic
	isNot := strings.HasPrefix(logic, "NOT ")
	combinator := strings.TrimPrefix(logic, "NOT ")

	var result string
	if len(parts) == 1 {
		if isNot {
			result = "NOT (" + parts[0] + ")"
		} else {
			result = parts[0]
		}
	} else {
		joined := strings.Join(parts, combinatorStyle.Render(fmt.Sprintf(" %s ", combinator)))
		if isNot {
			// For NOT AND/NOT OR, each part is negated
			var notParts []string
			for _, p := range parts {
				notParts = append(notParts, "NOT "+p)
			}
			result = "(" + strings.Join(notParts, combinatorStyle.Render(fmt.Sprintf(" %s ", combinator))) + ")"
		} else {
			result = "(" + joined + ")"
		}
	}

	return result
}

func (m logsViewer) renderOverview() string {
	if m.totalRows == 0 {
		return "No log entries to display"
	}

	if len(m.levelCounts) == 0 {
		return fmt.Sprintf("Total log entries: %d (no level field selected for breakdown)", m.totalRows)
	}

	// Sort levels by count (descending)
	var sortedLevels []logLevelCount
	for level, count := range m.levelCounts {
		sortedLevels = append(sortedLevels, logLevelCount{level, count})
	}

	// Simple bubble sort by count descending
	for i := 0; i < len(sortedLevels)-1; i++ {
		for j := i + 1; j < len(sortedLevels); j++ {
			if sortedLevels[j].count > sortedLevels[i].count {
				sortedLevels[i], sortedLevels[j] = sortedLevels[j], sortedLevels[i]
			}
		}
	}

	// Color mapping for log levels (using background colors like old tview version)
	levelBgColors := map[string]lipgloss.Color{
		"error":       lipgloss.Color("9"),  // Red
		"exception":   lipgloss.Color("9"),  // Red
		"fatal":       lipgloss.Color("9"),  // Red
		"critical":    lipgloss.Color("9"),  // Red
		"warning":     lipgloss.Color("11"), // Yellow
		"warn":        lipgloss.Color("11"), // Yellow
		"debug":       lipgloss.Color("11"), // Yellow
		"trace":       lipgloss.Color("11"), // Yellow
		"info":        lipgloss.Color("10"), // Green
		"information": lipgloss.Color("10"), // Green
		"unknown":     lipgloss.Color("8"),  // Gray
	}

	// Build timeline bar - calculate width dynamically
	// Content width matches the border's inner width (screen minus left + right borders)
	contentWidth := m.width - 2
	if contentWidth < 40 {
		contentWidth = 40
	}

	var builder strings.Builder
	// Use fixed-width prefix to match sparkline label width (14 chars)
	// Format: "Total: 1234 | " with right-aligned number (exactly 14 chars)
	prefixText := fmt.Sprintf("Total:%5d | ", m.totalRows)
	builder.WriteString(prefixText)

	// Calculate available width for bar segments (content minus prefix)
	// This should now always be 14 chars, matching sparklineRowLabelWidth
	availableWidth := contentWidth - len(prefixText)
	if availableWidth < 20 {
		availableWidth = 20
	}

	log.Debug().
		Int("screen_width", m.width).
		Int("content_width", contentWidth).
		Int("prefix_length", len(prefixText)).
		Int("bar_available_width", availableWidth).
		Msg(">>> Overview bar width calculation")

	// Create visual bar with background colors
	for _, lc := range sortedLevels {
		if lc.count == 0 {
			continue
		}

		proportion := float64(lc.count) / float64(m.totalRows)
		segmentWidth := int(proportion * float64(availableWidth))
		if segmentWidth == 0 && lc.count > 0 {
			segmentWidth = 1 // At least 1 char for visible levels
		}

		// Get background color for this level
		levelLower := strings.ToLower(lc.level)
		bgColor := levelBgColors[levelLower]
		if bgColor == "" {
			bgColor = lipgloss.Color("6") // Default cyan
		}

		// Create label text for this segment
		labelText := fmt.Sprintf("%s:%d", lc.level, lc.count)

		// If segment is wide enough to fit the label, embed it
		var segment string
		if segmentWidth >= len(labelText) {
			// Calculate padding to center the label
			padding := (segmentWidth - len(labelText)) / 2
			leftPad := strings.Repeat(" ", padding)
			rightPad := strings.Repeat(" ", segmentWidth-padding-len(labelText))
			segment = leftPad + labelText + rightPad
		} else {
			// Segment too small for label, just fill with spaces
			segment = strings.Repeat(" ", segmentWidth)
		}

		// Apply background color styling
		segmentStyle := lipgloss.NewStyle().
			Background(bgColor).
			Foreground(lipgloss.Color("0")) // Black text on colored background
		builder.WriteString(segmentStyle.Render(segment))

	}

	// Add sparkline visualization below the bar
	sparklineData := m.generateSparklineForLevels(sortedLevels)
	if sparklineData != "" {
		builder.WriteString("\n")
		builder.WriteString("Timeline:\n")
		builder.WriteString(sparklineData)
	}

	// Add selection info when overview has focus
	if m.overviewMode {
		helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8")) // Gray

		if m.focusOverview && m.selectedBucket >= 0 && m.selectedBucket < len(m.timeLabels) {
			// Get available levels
			priorityLevels := []string{"error", "warning", "info", "debug"}
			availableLevels := []string{}
			for _, level := range priorityLevels {
				if values, exists := m.levelTimeSeries[level]; exists && len(values) > 0 {
					availableLevels = append(availableLevels, level)
				}
			}

			if m.selectedLevel < len(availableLevels) {
				levelName := availableLevels[m.selectedLevel]
				timeLabel := m.timeLabels[m.selectedBucket]

				// Add bounds check before accessing levelTimeSeries array
				if m.selectedBucket < len(m.levelTimeSeries[levelName]) {
					value := m.levelTimeSeries[levelName][m.selectedBucket]

					infoStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("11")) // Yellow

					infoLine := infoStyle.Render(fmt.Sprintf("[%s @ %s] Value: %.0f events",
						strings.ToUpper(levelName), timeLabel, value))
					helpLine := helpStyle.Render(" | ↑↓←→: Navigate | Enter: Zoom | Tab/Shift+Tab: Switch to Table | Esc: Exit")

					builder.WriteString("\n")
					builder.WriteString(infoLine + helpLine)
				}
			}
		} else if !m.focusOverview {
			// Overview visible but table has focus
			builder.WriteString("\n")
			builder.WriteString(helpStyle.Render("[Table Focus] Tab/Shift+Tab: Switch to Overview | 0: Hide Overview | Esc: Exit"))
		}
	}

	return builder.String()
}

// generateSparklineForLevels creates a multi-row sparkline showing level distribution over time
func (m logsViewer) generateSparklineForLevels(sortedLevels []logLevelCount) string {
	if len(m.levelTimeSeries) == 0 {
		return ""
	}

	// Debug: Show what keys are actually in levelTimeSeries
	keys := make([]string, 0, len(m.levelTimeSeries))
	for k := range m.levelTimeSeries {
		keys = append(keys, k)
	}
	log.Debug().
		Int("levelTimeSeries_count", len(m.levelTimeSeries)).
		Strs("levelTimeSeries_keys", keys).
		Msg(">>> generateSparklineForLevels - checking levelTimeSeries keys")

	// Show top 3-4 priority levels
	priorityLevels := []string{"error", "warning", "info", "debug"}

	// Validate that all levels have the same number of data points (for width consistency)
	var expectedWidth int
	for _, level := range priorityLevels {
		if values, exists := m.levelTimeSeries[level]; exists && len(values) > 0 {
			if expectedWidth == 0 {
				expectedWidth = len(values)
			} else if len(values) != expectedWidth {
				log.Warn().
					Str("level", level).
					Int("expected_width", expectedWidth).
					Int("actual_width", len(values)).
					Msg("Sparkline width mismatch detected")
			}
		}
	}

	var lines []string
	levelIdx := 0
	for _, level := range priorityLevels {
		values, exists := m.levelTimeSeries[level]
		if !exists || len(values) == 0 {
			log.Debug().
				Str("level", level).
				Bool("exists", exists).
				Int("len", func() int { if values != nil { return len(values) } else { return -1 } }()).
				Msg(">>> Skipping level - no data")
			continue
		}

		// Debug: Check if values are actually non-zero
		nonZeroCount := 0
		var sampleValues []float64
		for i, v := range values {
			if v > 0 {
				nonZeroCount++
				if len(sampleValues) < 5 {
					sampleValues = append(sampleValues, v)
				}
			}
			if i < 5 && v > 0 {
				sampleValues = append(sampleValues, v)
			}
		}

		log.Debug().
			Str("level", level).
			Int("values_count", len(values)).
			Int("non_zero_count", nonZeroCount).
			Interface("first_5_non_zero", sampleValues).
			Msg(">>> Values before sparkline generation")

		// Generate sparkline characters
		sparklineChars := m.generateSparklineChars(values)

		log.Debug().
			Str("level", level).
			Int("sparkline_chars_len", len(sparklineChars)).
			Str("sparkline_preview", func() string {
				s := string(sparklineChars)
				if len(s) > 20 {
					return s[:20] + "..."
				}
				return s
			}()).
			Msg(">>> Generated sparkline for level")

		// Apply color based on level
		color := getLogLevelColor(level)

		// Build sparkline with individual character styling for cursor
		var styledSparkline strings.Builder
		for i, char := range sparklineChars {
			// Check if this bucket is selected (cursor position)
			// Only show selection when overview has focus
			isSelected := m.overviewMode && m.focusOverview && m.selectedLevel == levelIdx && m.selectedBucket == i

			if isSelected {
				// Highlight selected bucket with inverted colors
				// Swap foreground and background for clean inline cursor
				highlightStyle := lipgloss.NewStyle().
					Foreground(lipgloss.Color("0")).      // Black foreground
					Background(color).                     // Use level color as background
					Bold(true)
				styledSparkline.WriteString(highlightStyle.Render(string(char)))
			} else {
				// Normal style
				normalStyle := lipgloss.NewStyle().Foreground(color)
				styledSparkline.WriteString(normalStyle.Render(string(char)))
			}
		}

		// Add label (uppercase, padded to 10 chars for 14-char total to match bar prefix)
		label := fmt.Sprintf("  %-10s: ", strings.ToUpper(level))
		lines = append(lines, label+styledSparkline.String())

		levelIdx++

		// Limit to 4 rows
		if len(lines) >= 4 {
			break
		}
	}

	if len(lines) == 0 {
		return ""
	}

	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

// generateSparklineChars converts values to sparkline characters
// Zero values are rendered as spaces to show sparse data gaps
func (m logsViewer) generateSparklineChars(values []float64) []rune {
	if len(values) == 0 {
		return []rune{}
	}

	// Find min/max among non-zero values for proper scaling
	var minVal, maxVal float64
	hasNonZero := false
	for _, v := range values {
		if v > 0 {
			if !hasNonZero {
				minVal = v
				maxVal = v
				hasNonZero = true
			} else {
				if v < minVal {
					minVal = v
				}
				if v > maxVal {
					maxVal = v
				}
			}
		}
	}

	// If all values are zero, return all spaces
	if !hasNonZero {
		chars := make([]rune, len(values))
		for i := range chars {
			chars[i] = ' '
		}
		return chars
	}

	rangeVal := maxVal - minVal
	if rangeVal == 0 {
		rangeVal = 1
	}

	// Sparkline character set (8 levels)
	sparks := []rune("▁▂▃▄▅▆▇█")
	chars := make([]rune, len(values))

	for i, v := range values {
		if v == 0 {
			// Show space for empty buckets (sparse data)
			chars[i] = ' '
		} else {
			// Scale non-zero value to sparkline character
			pos := int(((v - minVal) / rangeVal) * float64(len(sparks)-1))
			if pos < 0 {
				pos = 0
			}
			if pos >= len(sparks) {
				pos = len(sparks) - 1
			}
			chars[i] = sparks[pos]
		}
	}

	return chars
}

func (m logsViewer) renderDetails() string {
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	labelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("11"))
	selectedLabelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("0")).Background(lipgloss.Color("11"))
	selectedValueStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("0")).Background(lipgloss.Color("6"))
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))

	hasLevel := m.selectedEntry.Level != ""
	currentIdx := 0

	var sb strings.Builder
	sb.WriteString(titleStyle.Render("Log Entry Details"))
	sb.WriteString("\n\n")

	// Time field (index 0)
	if m.detailsSelectedIdx == currentIdx {
		sb.WriteString(selectedLabelStyle.Render("▶ Time: "))
		sb.WriteString(selectedValueStyle.Render(m.selectedEntry.Time.Format("2006-01-02 15:04:05.000 MST")))
	} else {
		sb.WriteString(labelStyle.Render("  Time: "))
		sb.WriteString(m.selectedEntry.Time.Format("2006-01-02 15:04:05.000 MST"))
	}
	sb.WriteString("\n\n")
	currentIdx++

	// Level field (index 1 if present)
	if hasLevel {
		if m.detailsSelectedIdx == currentIdx {
			sb.WriteString(selectedLabelStyle.Render("▶ Level: "))
			sb.WriteString(selectedValueStyle.Render(m.selectedEntry.Level))
		} else {
			sb.WriteString(labelStyle.Render("  Level: "))
			sb.WriteString(m.selectedEntry.Level)
		}
		sb.WriteString("\n\n")
		currentIdx++
	}

	// Message field (index 1 or 2)
	if m.detailsSelectedIdx == currentIdx {
		sb.WriteString(selectedLabelStyle.Render("▶ Message:"))
		sb.WriteString("\n")
		sb.WriteString(selectedValueStyle.Render(m.selectedEntry.Message))
	} else {
		sb.WriteString(labelStyle.Render("  Message:"))
		sb.WriteString("\n")
		sb.WriteString(m.selectedEntry.Message)
	}
	sb.WriteString("\n\n")
	currentIdx++

	// Show all fields if available (use cached sorted field names)
	if len(m.detailsFieldNames) > 0 {
		sb.WriteString(titleStyle.Render("All Fields"))
		sb.WriteString("\n\n")

		// Display sorted fields using cached field names
		for _, fieldName := range m.detailsFieldNames {
			value := m.selectedEntry.AllFields[fieldName]

			// Format value based on type
			var valueStr string
			switch v := value.(type) {
			case []byte:
				valueStr = string(v)
			case time.Time:
				valueStr = v.Format("2006-01-02 15:04:05.000 MST")
			case nil:
				valueStr = "NULL"
			default:
				valueStr = fmt.Sprintf("%v", v)
			}

			// Truncate very long values
			if len(valueStr) > 200 {
				valueStr = valueStr[:197] + "..."
			}

			if m.detailsSelectedIdx == currentIdx {
				sb.WriteString(selectedLabelStyle.Render("▶ " + fieldName + ": "))
				sb.WriteString(selectedValueStyle.Render(valueStr))
			} else {
				sb.WriteString(labelStyle.Render("  " + fieldName + ": "))
				sb.WriteString(valueStr)
			}
			sb.WriteString("\n")
			currentIdx++
		}
		sb.WriteString("\n")
	}

	sb.WriteString(helpStyle.Render("↑↓: Navigate | Enter: Add as filter (field = value) | ESC: Close"))

	borderStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("6")).
		Padding(1, 2)

	return borderStyle.Render(sb.String())
}

// IsTableFiltering returns true if the table is currently in filter mode
func (m logsViewer) IsTableFiltering() bool {
	return m.table.IsFiltering()
}

// renderMainHelpLine renders the bottom help line with context-aware shortcuts
func (m logsViewer) renderMainHelpLine() string {
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8")) // Gray

	if m.showFilterForm {
		// When filter form is visible, show filter-related help
		return helpStyle.Render("Tab/Shift+Tab: Navigate filter fields | Enter: Add/Remove filter | Esc/Ctrl+F: Hide filters")
	}

	// Normal mode: show all available shortcuts
	return helpStyle.Render("↑↓/PgUp/PgDn: Scroll | Ctrl+P/N: Prev/Next window | /: Filter | Ctrl+F: Filters | Tab: Focus | Enter: Details | Esc: Back")
}

// renderZoomMenu renders the zoom action menu
func (m logsViewer) renderZoomMenu() string {
	if !m.showZoomMenu {
		return ""
	}

	// Get bucket time info for display
	var bucketTimeInfo string
	if m.app != nil && m.selectedBucket >= 0 && m.bucketInterval > 0 {
		bucketStart := m.app.state.FromTime.Add(time.Duration(m.selectedBucket*m.bucketInterval) * time.Second)
		bucketEnd := bucketStart.Add(time.Duration(m.bucketInterval) * time.Second)
		bucketTimeInfo = fmt.Sprintf("From: %s\nTo:   %s\nDuration: %s",
			bucketStart.Format("2006-01-02 15:04:05"),
			bucketEnd.Format("2006-01-02 15:04:05"),
			formatDuration(time.Duration(m.bucketInterval)*time.Second))
	}

	// Menu options
	options := []string{
		"Zoom to this time bucket",
		"Zoom out (restore previous)",
		"Reset to original range",
		"Cancel",
	}

	// Disable options if not applicable
	canZoomOut := len(m.zoomStack) > 0
	canReset := len(m.zoomStack) > 0

	// Build menu content
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	selectedStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("11")).Bold(true) // Yellow
	normalStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("7"))               // White
	disabledStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))             // Gray
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))

	var content strings.Builder
	content.WriteString(titleStyle.Render("Zoom to Time Bucket"))
	content.WriteString("\n")
	content.WriteString(bucketTimeInfo)
	content.WriteString("\n\n")

	for i, option := range options {
		var line string
		isDisabled := (i == 1 && !canZoomOut) || (i == 2 && !canReset)

		if i == m.zoomMenuIdx {
			line = "> " + selectedStyle.Render(option)
		} else if isDisabled {
			line = "  " + disabledStyle.Render(option)
		} else {
			line = "  " + normalStyle.Render(option)
		}
		content.WriteString(line)
		content.WriteString("\n")
	}

	content.WriteString("\n")
	content.WriteString(helpStyle.Render("↑↓: Navigate | Enter: Select | Esc: Cancel"))

	// Wrap in border
	menuStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("6")).
		Padding(1, 2).
		Width(45)

	menuBox := menuStyle.Render(content.String())

	// Center the menu on screen
	return lipgloss.Place(
		m.width,
		m.height,
		lipgloss.Center,
		lipgloss.Center,
		menuBox,
	)
}

// formatDuration formats a duration in a human-readable format
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		minutes := int(d.Minutes())
		seconds := int(d.Seconds()) % 60
		if seconds > 0 {
			return fmt.Sprintf("%dm %ds", minutes, seconds)
		}
		return fmt.Sprintf("%dm", minutes)
	}
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	if minutes > 0 {
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	return fmt.Sprintf("%dh", hours)
}

// buildFilterCondition builds a single filter condition
func buildFilterCondition(filter LogFilter) (string, []interface{}) {
	// Escape field name by wrapping in backticks
	field := "`" + strings.ReplaceAll(filter.Field, "`", "``") + "`"

	switch filter.Operator {
	case "IS NULL", "IS NOT NULL":
		// No value needed for NULL checks
		return fmt.Sprintf("%s %s", field, filter.Operator), nil
	case "LIKE", "NOT LIKE":
		return fmt.Sprintf("%s %s ?", field, filter.Operator), []interface{}{"%" + filter.Value + "%"}
	case "IN", "NOT IN":
		// Value is comma-separated list: "val1,val2,val3" -> IN ('val1','val2','val3')
		values := strings.Split(filter.Value, ",")
		placeholders := make([]string, len(values))
		args := make([]interface{}, len(values))
		for i, v := range values {
			placeholders[i] = "?"
			args[i] = strings.TrimSpace(v)
		}
		return fmt.Sprintf("%s %s (%s)", field, filter.Operator, strings.Join(placeholders, ", ")), args
	default:
		return fmt.Sprintf("%s %s ?", field, filter.Operator), []interface{}{filter.Value}
	}
}

// buildWhereClause builds SQL WHERE clause from log filters (simple AND combination)
func buildWhereClause(filters []LogFilter) (string, []interface{}) {
	if len(filters) == 0 {
		return "", nil
	}

	var conditions []string
	var args []interface{}

	for _, filter := range filters {
		cond, condArgs := buildFilterCondition(filter)
		conditions = append(conditions, cond)
		args = append(args, condArgs...)
	}

	whereClause := strings.Join(conditions, " AND ")
	return whereClause, args
}

// buildWhereClauseFromGroups builds SQL WHERE clause from filter groups (legacy, kept for backward compatibility)
func buildWhereClauseFromGroups(filterGroups LogFilterGroups) (string, []interface{}) {
	if len(filterGroups.Groups) == 0 {
		return "", nil
	}

	var groupClauses []string
	var allArgs []interface{}

	for _, group := range filterGroups.Groups {
		if len(group.Filters) == 0 {
			continue
		}

		var conditions []string
		for _, filter := range group.Filters {
			cond, condArgs := buildFilterCondition(filter)
			conditions = append(conditions, cond)
			allArgs = append(allArgs, condArgs...)
		}

		// Combine filters within group
		combinator := group.Combinator
		if combinator == "" {
			combinator = "AND"
		}
		groupClause := strings.Join(conditions, " "+combinator+" ")

		// Wrap in parentheses if more than one filter
		if len(conditions) > 1 {
			groupClause = "(" + groupClause + ")"
		}
		groupClauses = append(groupClauses, groupClause)
	}

	if len(groupClauses) == 0 {
		return "", nil
	}

	// Combine groups
	groupCombinator := filterGroups.GroupCombinator
	if groupCombinator == "" {
		groupCombinator = "AND"
	}

	whereClause := strings.Join(groupClauses, " "+groupCombinator+" ")
	return whereClause, allArgs
}

// buildWhereFromTree builds SQL WHERE clause from a filter tree (new recursive structure)
func buildWhereFromTree(node *FilterNode) (string, []interface{}) {
	if node == nil {
		return "", nil
	}

	if !node.IsGroup {
		// Single condition - use existing buildFilterCondition
		return buildFilterCondition(LogFilter{
			Field:    node.Field,
			Operator: node.Operator,
			Value:    node.Value,
		})
	}

	// Group - recursively build children
	var parts []string
	var allArgs []interface{}

	// Parse logic: "AND", "OR", "NOT AND", "NOT OR"
	logic := node.Logic
	if logic == "" {
		logic = "AND"
	}
	isNot := strings.HasPrefix(logic, "NOT ")
	combinator := strings.TrimPrefix(logic, "NOT ")
	if combinator == "" {
		combinator = "AND"
	}

	for _, child := range node.Children {
		sql, args := buildWhereFromTree(child)
		if sql == "" {
			continue // Skip empty conditions
		}
		if isNot {
			// NOT AND: (NOT cond1 AND NOT cond2)
			// NOT OR:  (NOT cond1 OR NOT cond2)
			sql = "NOT (" + sql + ")"
		}
		parts = append(parts, sql)
		allArgs = append(allArgs, args...)
	}

	if len(parts) == 0 {
		return "", nil // Empty group
	}
	if len(parts) == 1 {
		return parts[0], allArgs // Single child, no extra parentheses
	}

	// Multiple children: wrap in parentheses
	return "(" + strings.Join(parts, " "+combinator+" ") + ")", allArgs
}

// fetchAllTableFields fetches all column names from a table
func (a *App) fetchAllTableFields(database, table string) []string {
	query := fmt.Sprintf("DESCRIBE TABLE `%s`.`%s`", database, table)
	rows, err := a.state.ClickHouse.Query(query)
	if err != nil {
		log.Error().Err(err).Str("database", database).Str("table", table).Msg("Error describing table")
		return nil
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("Error closing describe table query")
		}
	}()

	var fields []string
	for rows.Next() {
		var name, typeStr, defaultType, defaultExpr, comment, codecExpr, ttlExpr string
		if err := rows.Scan(&name, &typeStr, &defaultType, &defaultExpr, &comment, &codecExpr, &ttlExpr); err != nil {
			log.Error().Err(err).Msg("Error scanning table field")
			continue
		}
		fields = append(fields, name)
	}

	return fields
}

// ShowLogsViewer shows the log viewer with the given configuration
func (a *App) ShowLogsViewer(config LogConfig) tea.Cmd {
	viewer := newLogsViewer(config, a.width, a.height)
	viewer.app = a    // Set app reference for pagination
	viewer.offset = 0 // Start at offset 0

	// Fetch all table fields for filter dropdown
	viewer.allFields = a.fetchAllTableFields(config.Database, config.Table)
	if len(viewer.allFields) > 0 {
		viewer.filterFieldDD.SetOptions(viewer.allFields)
	}

	a.logsHandler = viewer
	a.currentPage = pageLogs

	// Start async data fetch only if we have valid dimensions
	// If width is 0, we haven't received WindowSizeMsg yet - data will be fetched
	// when LogsDataMsg is received after window size is set
	if a.width > 0 {
		// Use the viewer's root filter (empty by default)
		return a.fetchLogsDataCmd(config, 0, viewer.rootFilter, a.width)
	}
	return nil
}

// fetchLogsDataCmd fetches log data from ClickHouse
func (a *App) fetchLogsDataCmd(config LogConfig, offset int, rootFilter *FilterNode, viewerWidth int) tea.Cmd {
	return func() tea.Msg {
		// Build query - select all fields (*)
		queryBuilder := fmt.Sprintf(
			"SELECT * FROM `%s`.`%s`",
			config.Database,
			config.Table,
		)

		// Build time range conditions (always required)
		var timeConditions []string
		fromTime := a.state.FromTime
		toTime := a.state.ToTime

		// TimeField condition (required)
		if config.TimeField != "" {
			timeConditions = append(timeConditions,
				fmt.Sprintf("%s >= toDateTime('%s') AND %s <= toDateTime('%s')",
					config.TimeField, fromTime.Format("2006-01-02 15:04:05"),
					config.TimeField, toTime.Format("2006-01-02 15:04:05")))
		}

		// DateField condition (optional, for partition pruning)
		if config.DateField != "" {
			timeConditions = append(timeConditions,
				fmt.Sprintf("%s >= toDate('%s') AND %s <= toDate('%s')",
					config.DateField, fromTime.Format("2006-01-02"),
					config.DateField, toTime.Format("2006-01-02")))
		}

		// TimeMsField condition (optional, for millisecond precision)
		if config.TimeMsField != "" {
			timeConditions = append(timeConditions,
				fmt.Sprintf("%s >= toDateTime64('%s', 3) AND %s <= toDateTime64('%s', 3)",
					config.TimeMsField, fromTime.Format("2006-01-02 15:04:05.000"),
					config.TimeMsField, toTime.Format("2006-01-02 15:04:05.999")))
		}

		// Build WHERE clause from user filter tree
		whereClause, whereArgs := buildWhereFromTree(rootFilter)
		var args []interface{}

		// Combine time conditions with user filters
		var allConditions []string
		allConditions = append(allConditions, timeConditions...)
		if whereClause != "" {
			allConditions = append(allConditions, whereClause)
			args = append(args, whereArgs...)
		}

		if len(allConditions) > 0 {
			queryBuilder += " WHERE " + strings.Join(allConditions, " AND ")
		}

		// Add ORDER BY (use TimeMsField for millisecond precision if available, otherwise TimeField)
		orderByField := config.TimeField
		if config.TimeMsField != "" {
			orderByField = config.TimeMsField
		}
		queryBuilder += fmt.Sprintf(" ORDER BY %s", orderByField)

		// Add LIMIT and OFFSET
		queryBuilder += fmt.Sprintf(" LIMIT %d OFFSET %d", config.WindowSize, offset)

		log.Debug().
			Str("query", queryBuilder).
			Interface("args", args).
			Msg("Executing logs query with filters")

		rows, err := a.state.ClickHouse.Query(queryBuilder, args...)
		if err != nil {
			return LogsDataMsg{Err: fmt.Errorf("error executing query: %v", err)}
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close logs query")
			}
		}()

		// Get column types
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			return LogsDataMsg{Err: fmt.Errorf("error getting column types: %v", err)}
		}

		var entries []LogEntry
		levelCounts := make(map[string]int)
		var firstTime, lastTime time.Time

		for rows.Next() {
			entry := LogEntry{
				AllFields: make(map[string]interface{}),
			}

			// Prepare scan destinations for all columns
			scanArgs := make([]interface{}, len(colTypes))
			fieldValues := make([]interface{}, len(colTypes))

			for i, col := range colTypes {
				fieldName := col.Name()

				// Assign to struct fields for known columns
				switch fieldName {
				case config.TimeField:
					scanArgs[i] = &entry.Time
				case config.MessageField:
					scanArgs[i] = &entry.Message
				case config.LevelField:
					scanArgs[i] = &entry.Level
				default:
					// Store other fields in AllFields map
					fieldValues[i] = new(interface{})
					scanArgs[i] = fieldValues[i]
				}
			}

			if err := rows.Scan(scanArgs...); err != nil {
				return LogsDataMsg{Err: fmt.Errorf("error scanning row: %v", err)}
			}

			// Populate AllFields map with non-primary fields
			for i, col := range colTypes {
				fieldName := col.Name()

				// Skip primary fields (already in struct)
				switch fieldName {
				case config.TimeField, config.MessageField, config.LevelField:
					// Skip - already in struct
				default:
					if fieldValues[i] != nil {
						val := *fieldValues[i].(*interface{})
						entry.AllFields[fieldName] = val
					}
				}
			}

			entries = append(entries, entry)

			if firstTime.IsZero() || entry.Time.Before(firstTime) {
				firstTime = entry.Time
			}
			if lastTime.IsZero() || entry.Time.After(lastTime) {
				lastTime = entry.Time
			}

			if entry.Level != "" {
				levelCounts[entry.Level]++
			}
		}

		if err := rows.Err(); err != nil {
			return LogsDataMsg{Err: fmt.Errorf("error reading rows: %v", err)}
		}

		// Overview/sparkline data will be fetched separately via fetchOverviewDataCmd
		// using LTTB (Largest Triangle Three Buckets) for optimal downsampling
		return LogsDataMsg{
			Entries:        entries,
			FirstEntryTime: firstTime,
			LastEntryTime:  lastTime,
			TotalRows:      len(entries),
			LevelCounts:    levelCounts,
			// LevelTimeSeries and TimeLabels will be populated by OverviewDataMsg
		}
	}
}

// fetchOverviewDataCmd fetches overview sparkline data using time-based bucketing
func (a *App) fetchOverviewDataCmd(config LogConfig, startTime, endTime time.Time, sparklineWidth int, rootFilter *FilterNode) tea.Cmd {
	return func() tea.Msg {
		if startTime.IsZero() || endTime.IsZero() || config.LevelField == "" {
			return OverviewDataMsg{Err: nil} // No data to display
		}

		// Ensure sparklineWidth is within bounds
		buckets := sparklineWidth
		if buckets < 40 {
			buckets = 40
		}
		if buckets > 200 {
			buckets = 200
		}

		// Calculate interval in seconds for time bucketing
		startUnix := float64(startTime.Unix())
		endUnix := float64(endTime.Unix())
		timeRange := endUnix - startUnix
		if timeRange <= 0 {
			return OverviewDataMsg{Err: nil}
		}
		intervalSeconds := int(timeRange / float64(buckets))
		if intervalSeconds < 1 {
			intervalSeconds = 1
		}

		// Use TimeMsField for better precision if available, otherwise TimeField
		timeFieldForQuery := config.TimeField
		if config.TimeMsField != "" {
			timeFieldForQuery = config.TimeMsField
		}

		// Build WHERE clause with time range using Unix timestamps to avoid timezone issues
		startUnixInt := int64(startUnix)
		endUnixInt := int64(endUnix)
		whereClause := fmt.Sprintf("toUnixTimestamp(%s) BETWEEN %d AND %d",
			config.TimeField,
			startUnixInt,
			endUnixInt)

		// Create args slice for parameterized query
		var args []interface{}

		// Add user filter tree if present
		filterWhere, filterArgs := buildWhereFromTree(rootFilter)
		if filterWhere != "" {
			whereClause = whereClause + " AND (" + filterWhere + ")"
			args = append(args, filterArgs...)
		}

		// Time-bucketed query using Unix timestamps to avoid timezone issues
		// Calculate bucket index directly: (unix_ts - start_unix) / interval_seconds
		// Level normalization done in SQL using multiIf for consistent mapping
		// Use toString() to handle Enum types (like in system.text_log)
		// IMPORTANT: Use __level__ alias for normalized level to avoid conflict with original level field in WHERE clause
		query := fmt.Sprintf("SELECT "+
			"multiIf("+
			"lower(toString(%s)) IN ('information', 'notice'), 'info', "+
			"lower(toString(%s)) IN ('warn'), 'warning', "+
			"lower(toString(%s)) IN ('exception', 'critical', 'fatal'), 'error', "+
			"lower(toString(%s)) IN ('trace'), 'debug', "+
			"lower(toString(%s))"+
			") AS __level__, "+
			"intDiv(toUnixTimestamp(%s) - %d, %d) AS bucket_idx, "+
			"count() AS cnt "+
			"FROM `%s`.`%s` "+
			"WHERE %s "+
			"GROUP BY __level__, bucket_idx "+
			"ORDER BY __level__, bucket_idx",
			config.LevelField, config.LevelField, config.LevelField, config.LevelField, config.LevelField,
			timeFieldForQuery, startUnixInt, intervalSeconds,
			config.Database, config.Table,
			whereClause,
		)

		log.Debug().
			Str("query", query).
			Int("buckets", buckets).
			Int("intervalSeconds", intervalSeconds).
			Msg("Fetching overview data with time bucketing")

		rows, err := a.state.ClickHouse.Query(query, args...)
		if err != nil {
			log.Error().Err(err).Msg("Error querying overview data")
			return OverviewDataMsg{Err: fmt.Errorf("error executing overview query: %v", err)}
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close overview query")
			}
		}()

		// Parse results: each row is (__level__, bucket_idx, cnt)
		// Build map of normalized level -> bucket index -> count
		levelBuckets := make(map[string]map[int]float64)

		rowCount := 0
		for rows.Next() {
			rowCount++
			var level string
			var bucketIdx int64
			var cnt uint64

			if err := rows.Scan(&level, &bucketIdx, &cnt); err != nil {
				log.Error().Err(err).Msg("Error scanning overview row")
				continue
			}

			// Clamp bucket index to valid range
			idx := int(bucketIdx)
			if idx < 0 {
				idx = 0
			}
			if idx >= buckets {
				idx = buckets - 1
			}

			if levelBuckets[level] == nil {
				levelBuckets[level] = make(map[int]float64)
			}
			levelBuckets[level][idx] += float64(cnt)
		}

		log.Debug().
			Int("rows_processed", rowCount).
			Int("levels_found", len(levelBuckets)).
			Msg("Overview query completed")

		if err := rows.Err(); err != nil {
			log.Error().Err(err).Msg("Error reading overview rows")
			return OverviewDataMsg{Err: fmt.Errorf("error reading overview rows: %v", err)}
		}

		// Convert bucket maps to fixed-size arrays
		levelTimeSeries := make(map[string][]float64)
		for level, bucketMap := range levelBuckets {
			values := make([]float64, buckets)
			for idx, cnt := range bucketMap {
				values[idx] = cnt
			}
			levelTimeSeries[level] = values
		}

		// Generate time labels for fixed buckets
		timeLabels := make([]string, buckets)
		for i := 0; i < buckets; i++ {
			ts := startUnix + float64(i)*float64(intervalSeconds)
			t := time.Unix(int64(ts), 0)
			timeLabels[i] = t.Format("15:04:05")
		}

		log.Debug().
			Int("levels_count", len(levelTimeSeries)).
			Int("time_labels_count", len(timeLabels)).
			Int("buckets", buckets).
			Msg("Overview data ready")

		return OverviewDataMsg{
			LevelTimeSeries: levelTimeSeries,
			TimeLabels:      timeLabels,
		}
	}
}

// generateSparklineFromEntries generates sparkline data directly from fetched log entries
// This avoids making a separate SQL query - we bucket the data we already have
func generateSparklineFromEntries(entries []LogEntry, levelField string, startTime, endTime time.Time, sparklineWidth int) (map[string][]float64, []string) {
	if len(entries) == 0 || startTime.IsZero() || endTime.IsZero() {
		return nil, nil
	}

	// Use sparklineWidth as bucket count - ensures sparkline fills available width
	buckets := sparklineWidth
	if buckets < 20 {
		buckets = 20
	}
	if buckets > 200 {
		buckets = 200
	}

	timeRange := endTime.Sub(startTime).Seconds()
	if timeRange <= 0 {
		return nil, nil
	}
	intervalSeconds := timeRange / float64(buckets)
	if intervalSeconds < 1 {
		intervalSeconds = 1
	}

	// Generate fixed-width bucket timestamps from startTime to endTime
	// This ensures the sparkline always has exactly `buckets` characters
	startUnix := startTime.Unix()
	fixedBuckets := make([]int64, buckets)
	for i := 0; i < buckets; i++ {
		fixedBuckets[i] = startUnix + int64(float64(i)*intervalSeconds)
	}

	// Bucket entries by level and time interval
	levelBucketCounts := make(map[string]map[int]float64) // map[level]map[bucketIndex]count

	for _, entry := range entries {
		// Normalize level name (same logic as fetchTimeSeriesData)
		levelLower := strings.ToLower(entry.Level)
		switch levelLower {
		case "information", "notice":
			levelLower = "info"
		case "warn":
			levelLower = "warning"
		case "exception", "critical", "fatal":
			levelLower = "error"
		case "trace":
			levelLower = "debug"
		}

		// Calculate which bucket index this entry belongs to
		entryUnix := entry.Time.Unix()
		bucketIndex := int(float64(entryUnix-startUnix) / intervalSeconds)
		if bucketIndex < 0 {
			bucketIndex = 0
		}
		if bucketIndex >= buckets {
			bucketIndex = buckets - 1
		}

		if levelBucketCounts[levelLower] == nil {
			levelBucketCounts[levelLower] = make(map[int]float64)
		}
		levelBucketCounts[levelLower][bucketIndex]++
	}

	log.Debug().
		Int("entries_count", len(entries)).
		Int("fixed_buckets_count", len(fixedBuckets)).
		Int("requested_buckets", buckets).
		Float64("interval_seconds", intervalSeconds).
		Msg(">>> Generated sparkline from entries (fixed-width buckets)")

	// Build aligned time series with fixed width
	levelTimeSeries := make(map[string][]float64)
	for level, bucketCounts := range levelBucketCounts {
		values := make([]float64, buckets)
		for idx, count := range bucketCounts {
			if idx >= 0 && idx < buckets {
				values[idx] = count
			}
		}
		levelTimeSeries[level] = values

		log.Debug().
			Str("level", level).
			Int("data_points", len(values)).
			Int("bucketCounts_size", len(bucketCounts)).
			Msg(">>> Built sparkline for level from entries")
	}

	// Generate time labels for fixed buckets
	timeLabels := make([]string, buckets)
	for i, ts := range fixedBuckets {
		t := time.Unix(ts, 0)
		timeLabels[i] = t.Format("15:04:05")
	}

	return levelTimeSeries, timeLabels
}

// fetchTimeSeriesData queries time-bucketed log counts for sparkline visualization
func (a *App) fetchTimeSeriesData(config LogConfig, startTime, endTime time.Time, sparklineWidth int, filters []LogFilter) (map[string][]float64, []string) {
	if startTime.IsZero() || endTime.IsZero() || config.LevelField == "" {
		return nil, nil
	}

	// Use sparklineWidth as bucket count - one character per bucket
	buckets := sparklineWidth
	if buckets < 20 {
		buckets = 20
	}
	if buckets > 200 {
		buckets = 200
	}

	// Calculate interval in seconds
	timeRange := endTime.Sub(startTime).Seconds()
	if timeRange <= 0 {
		return nil, nil
	}
	intervalSeconds := int(math.Ceil(timeRange / float64(buckets)))
	if intervalSeconds < 1 {
		intervalSeconds = 1
	}

	// Build WHERE clause with time range and filters
	whereClause := fmt.Sprintf("%s BETWEEN '%s' AND '%s'",
		config.TimeField,
		startTime.Format("2006-01-02 15:04:05"),
		endTime.Format("2006-01-02 15:04:05"))

	// Create args slice for parameterized query
	var args []interface{}

	// Add user filters if present
	if len(filters) > 0 {
		filterWhere, filterArgs := buildWhereClause(filters)
		if filterWhere != "" {
			whereClause = whereClause + " AND (" + filterWhere + ")"
			args = append(args, filterArgs...)
		}
	}

	// Build query using fixed time buckets to ensure all levels have same timestamps and width
	query := fmt.Sprintf(`
		SELECT
			%s as level,
			toUnixTimestamp(toStartOfInterval(%s, INTERVAL %d SECOND)) as bucket_ts,
			count() as cnt
		FROM %s.%s
		WHERE %s
		GROUP BY level, bucket_ts
		ORDER BY level, bucket_ts
	`,
		config.LevelField,
		config.TimeField,
		intervalSeconds,
		config.Database,
		config.Table,
		whereClause,
	)

	log.Debug().
		Str("query", query).
		Int("buckets", buckets).
		Int("interval_seconds", intervalSeconds).
		Int("filter_args_count", len(args)).
		Msg("Fetching time-series data for sparkline")

	rows, err := a.state.ClickHouse.Query(query, args...)
	if err != nil {
		log.Error().Err(err).Msg("Error querying time-series data")
		return nil, nil
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("can't close time-series query")
		}
	}()

	// Parse query results into map[level]map[timestamp]count
	// Also collect all unique timestamps from the query (these are the ACTUAL buckets from ClickHouse)
	levelBucketCounts := make(map[string]map[int64]float64)
	allBucketTimestamps := make(map[int64]bool)

	for rows.Next() {
		var level string
		var bucketTs int64
		var count float64

		if err := rows.Scan(&level, &bucketTs, &count); err != nil {
			log.Error().Err(err).Msg("Error scanning time-series row")
			continue
		}

		levelLower := strings.ToLower(level)
		originalLevel := levelLower

		// Normalize level aliases to canonical names for consistent sparkline display
		switch levelLower {
		case "information", "notice":
			levelLower = "info"
		case "warn":
			levelLower = "warning"
		case "exception", "critical", "fatal":
			levelLower = "error"
		case "trace":
			levelLower = "debug"
		}

		// Debug: Show level normalization
		if originalLevel != levelLower {
			log.Debug().
				Str("original_level", level).
				Str("lowercased", originalLevel).
				Str("normalized_level", levelLower).
				Msg(">>> Sparkline level normalization applied")
		}

		if levelBucketCounts[levelLower] == nil {
			levelBucketCounts[levelLower] = make(map[int64]float64)
		}
		levelBucketCounts[levelLower][bucketTs] = count
		allBucketTimestamps[bucketTs] = true  // Track all unique timestamps
	}

	// Convert unique timestamps to sorted slice - these are the ACTUAL buckets from the query
	actualBuckets := make([]int64, 0, len(allBucketTimestamps))
	for ts := range allBucketTimestamps {
		actualBuckets = append(actualBuckets, ts)
	}
	// Sort timestamps
	sort.Slice(actualBuckets, func(i, j int) bool {
		return actualBuckets[i] < actualBuckets[j]
	})

	log.Debug().
		Int("actual_buckets_count", len(actualBuckets)).
		Int("requested_buckets", buckets).
		Msg(">>> Using actual bucket timestamps from query")

	// Build aligned time series for each level using ACTUAL bucket timestamps
	levelTimeSeries := make(map[string][]float64)
	for level, bucketCounts := range levelBucketCounts {
		values := make([]float64, len(actualBuckets))
		for i, ts := range actualBuckets {
			if count, exists := bucketCounts[ts]; exists {
				values[i] = count
			}
			// else: values[i] = 0 (default for float64)
		}
		levelTimeSeries[level] = values

		log.Debug().
			Str("level", level).
			Int("data_points", len(values)).
			Int("bucketCounts_size", len(bucketCounts)).
			Msg(">>> Built aligned time series for level")
	}

	if err := rows.Err(); err != nil {
		log.Error().Err(err).Msg("Error reading time-series rows")
		return nil, nil
	}

	// Generate time labels from actual bucket timestamps
	timeLabels := make([]string, len(actualBuckets))
	for i, ts := range actualBuckets {
		t := time.Unix(ts, 0)
		timeLabels[i] = t.Format("15:04:05")
	}

	// Debug: Check data before returning
	debugSample := make(map[string]interface{})
	for level, values := range levelTimeSeries {
		nonZero := 0
		for _, v := range values {
			if v > 0 {
				nonZero++
			}
		}
		debugSample[level] = map[string]int{
			"total":    len(values),
			"non_zero": nonZero,
		}
	}

	log.Debug().
		Int("levels_count", len(levelTimeSeries)).
		Int("bucket_count", len(actualBuckets)).
		Int("interval_seconds", intervalSeconds).
		Interface("data_before_return", debugSample).
		Msg(">>> Time-series data BEFORE RETURN from fetchTimeSeriesData")

	return levelTimeSeries, timeLabels
}
