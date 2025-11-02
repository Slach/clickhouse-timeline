package tui

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // Import tzdata to embed timezone database

	"github.com/Slach/clickhouse-timeline/pkg/timezone"
	"github.com/charmbracelet/bubbles/list"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"golang.org/x/text/message"
)

// Predefined ranges similar to Grafana
var predefinedRanges = []string{
	"Last 5 minutes",
	"Last 15 minutes",
	"Last 30 minutes",
	"Last 1 hour",
	"Last 3 hours",
	"Last 6 hours",
	"Last 12 hours",
	"Last 24 hours",
	"Last 2 days",
	"Last 7 days",
	"Last 30 days",
	"Last 90 days",
	"Last 6 months",
	"Last 1 year",
	"Last 2 years",
	"Last 5 years",
	"Today",
	"Yesterday",
	"This week",
	"This month",
	"This year",
}

// DateSelectedMsg is sent when a date is selected from the date picker
type DateSelectedMsg struct {
	Time     time.Time
	IsFrom   bool // true if setting "from" time, false if setting "to" time
	Canceled bool
}

// RangeSelectedMsg is sent when a range is selected from the range picker
type RangeSelectedMsg struct {
	FromTime time.Time
	ToTime   time.Time
	Canceled bool
}

// datePickerMode represents the current input focus
type datePickerMode int

const (
	modeCalendar datePickerMode = iota
	modeTimeInput
	modeTimezoneInput
	modeButtons
)

// datePicker is a bubbletea model for date/time selection with calendar
type datePicker struct {
	title       string
	initialTime time.Time
	isFrom      bool
	width       int
	height      int

	// Calendar state
	selectedYear  int
	selectedMonth time.Month
	selectedDay   int
	calendarRows  int
	calendarCols  int
	cursorRow     int
	cursorCol     int

	// Time input
	timeInput textinput.Model

	// Timezone input
	timezoneInput     textinput.Model
	selectedTimeZone  string
	tzMatches         []string
	tzDisplayText     string
	showTzSuggestions bool

	// UI state
	mode           datePickerMode
	buttonIndex    int // 0=Now, 1=Save, 2=Cancel
	firstDayOfWeek time.Weekday
	err            error
}

func newDatePicker(title string, initialTime time.Time, isFrom bool, width, height int) datePicker {
	// Create time input
	timeInput := textinput.New()
	timeInput.Placeholder = "HH:MM:SS"
	timeInput.SetValue(fmt.Sprintf("%02d:%02d:%02d", initialTime.Hour(), initialTime.Minute(), initialTime.Second()))
	timeInput.CharLimit = 8

	// Create timezone input
	timezoneInput := textinput.New()
	timezoneInput.Placeholder = "Type to search..."
	timezoneInput.CharLimit = 50

	// Get timezone information
	tzName, tzOffset := initialTime.Zone()
	tzOffset = tzOffset / 60 // Convert to minutes

	// Try to find the timezone in our list
	selectedTimeZone := tzName
	tzDisplayText := ""
	tzFound := false

	for _, zone := range timezone.TimeZones {
		if zone.Name == tzName {
			selectedTimeZone = zone.Name
			tzDisplayText = zone.DisplayText
			tzFound = true
			break
		}
	}

	// If not found, try current timezone
	if !tzFound {
		if currentTZ, err := timezone.GetCurrentTimeZone(); err == nil {
			selectedTimeZone = currentTZ.Name
			tzDisplayText = currentTZ.DisplayText
			tzFound = true
		}
	}

	// If still not found, try by offset
	if !tzFound {
		for _, zone := range timezone.TimeZones {
			if zone.Offset == tzOffset {
				selectedTimeZone = zone.Name
				tzDisplayText = zone.DisplayText
				break
			}
		}
	}

	timezoneInput.SetValue(tzDisplayText)

	// Determine first day of week based on locale
	firstDayOfWeek := time.Monday
	tag := message.MatchLanguage("")
	tagStr := tag.String()
	if tagStr == "en-US" || tagStr == "en-CA" ||
		strings.HasPrefix(tagStr, "ar") || strings.HasPrefix(tagStr, "fa") {
		firstDayOfWeek = time.Sunday
	}

	return datePicker{
		title:            title,
		initialTime:      initialTime,
		isFrom:           isFrom,
		width:            width,
		height:           height,
		selectedYear:     initialTime.Year(),
		selectedMonth:    initialTime.Month(),
		selectedDay:      initialTime.Day(),
		calendarRows:     6,
		calendarCols:     7,
		cursorRow:        0,
		cursorCol:        0,
		timeInput:        timeInput,
		timezoneInput:    timezoneInput,
		selectedTimeZone: selectedTimeZone,
		tzDisplayText:    tzDisplayText,
		mode:             modeCalendar,
		firstDayOfWeek:   firstDayOfWeek,
	}
}

func (m datePicker) Init() tea.Cmd {
	return nil
}

func (m datePicker) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc":
			// Cancel
			return m, func() tea.Msg {
				return DateSelectedMsg{Canceled: true}
			}

		case "tab":
			// Cycle through modes
			m.mode = (m.mode + 1) % 4
			switch m.mode {
			case modeTimeInput:
				m.timeInput.Focus()
			case modeTimezoneInput:
				m.timezoneInput.Focus()
			default:
				m.timeInput.Blur()
				m.timezoneInput.Blur()
			}
			return m, nil

		case "shift+tab":
			// Cycle backwards
			m.mode = (m.mode + 3) % 4
			switch m.mode {
			case modeTimeInput:
				m.timeInput.Focus()
			case modeTimezoneInput:
				m.timezoneInput.Focus()
			default:
				m.timeInput.Blur()
				m.timezoneInput.Blur()
			}
			return m, nil

		case "enter":
			switch m.mode {
			case modeCalendar:
				// Select day and move to time input
				m.mode = modeTimeInput
				m.timeInput.Focus()
				return m, nil

			case modeTimeInput:
				// Move to timezone input
				m.mode = modeTimezoneInput
				m.timeInput.Blur()
				m.timezoneInput.Focus()
				return m, nil

			case modeTimezoneInput:
				// Move to buttons
				m.mode = modeButtons
				m.timezoneInput.Blur()
				m.buttonIndex = 1 // Save
				return m, nil

			case modeButtons:
				// Execute button action
				switch m.buttonIndex {
				case 0: // Now
					now := time.Now()
					m.selectedYear = now.Year()
					m.selectedMonth = now.Month()
					m.selectedDay = now.Day()
					m.timeInput.SetValue(fmt.Sprintf("%02d:%02d:%02d", now.Hour(), now.Minute(), now.Second()))
					return m, nil

				case 1: // Save
					// Parse time
					timeStr := m.timeInput.Value()
					var hour, minute, sec int
					if _, err := fmt.Sscanf(timeStr, "%d:%d:%d", &hour, &minute, &sec); err != nil {
						hour, minute, sec = m.initialTime.Clock()
					}

					// Get timezone
					location := time.Local
					if m.selectedTimeZone != "" {
						if loc, err := time.LoadLocation(m.selectedTimeZone); err == nil {
							location = loc
						}
					}

					// Create final time
					selectedTime := time.Date(m.selectedYear, m.selectedMonth, m.selectedDay, hour, minute, sec, 0, location)
					return m, func() tea.Msg {
						return DateSelectedMsg{
							Time:   selectedTime,
							IsFrom: m.isFrom,
						}
					}

				case 2: // Cancel
					return m, func() tea.Msg {
						return DateSelectedMsg{Canceled: true}
					}
				}
			}
		}

		// Mode-specific key handling
		switch m.mode {
		case modeCalendar:
			return m.handleCalendarKeys(msg)
		case modeTimeInput:
			m.timeInput, cmd = m.timeInput.Update(msg)
			return m, cmd
		case modeTimezoneInput:
			oldValue := m.timezoneInput.Value()
			m.timezoneInput, cmd = m.timezoneInput.Update(msg)
			newValue := m.timezoneInput.Value()

			// Update timezone matches if value changed
			if oldValue != newValue {
				m.updateTimezoneMatches(newValue)
			}
			return m, cmd
		case modeButtons:
			switch msg.String() {
			case "left", "h":
				m.buttonIndex = (m.buttonIndex + 2) % 3
			case "right", "l":
				m.buttonIndex = (m.buttonIndex + 1) % 3
			}
			return m, nil
		}
	}

	return m, nil
}

func (m datePicker) handleCalendarKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "up", "k":
		m.cursorRow--
		if m.cursorRow < 0 {
			m.cursorRow = 0
		}
	case "down", "j":
		m.cursorRow++
		if m.cursorRow >= m.calendarRows {
			m.cursorRow = m.calendarRows - 1
		}
	case "left", "h":
		m.cursorCol--
		if m.cursorCol < 0 {
			m.cursorCol = 0
		}
	case "right", "l":
		m.cursorCol++
		if m.cursorCol >= m.calendarCols {
			m.cursorCol = m.calendarCols - 1
		}
	case "ctrl+p", "ctrl+left":
		// Previous month
		if m.selectedMonth == 1 {
			m.selectedMonth = 12
			m.selectedYear--
		} else {
			m.selectedMonth--
		}
	case "ctrl+n", "ctrl+right":
		// Next month
		if m.selectedMonth == 12 {
			m.selectedMonth = 1
			m.selectedYear++
		} else {
			m.selectedMonth++
		}
	}
	return m, nil
}

func (m *datePicker) updateTimezoneMatches(search string) {
	m.tzMatches = []string{}
	if search == "" {
		m.showTzSuggestions = false
		return
	}

	lowerSearch := strings.ToLower(search)
	for _, tz := range timezone.TimeZones {
		if strings.Contains(strings.ToLower(tz.DisplayText), lowerSearch) {
			m.tzMatches = append(m.tzMatches, tz.DisplayText)
			if len(m.tzMatches) >= 5 {
				break
			}
		}
	}
	m.showTzSuggestions = len(m.tzMatches) > 0

	// Update selected timezone if exact match
	for _, tz := range timezone.TimeZones {
		if tz.DisplayText == search {
			m.selectedTimeZone = tz.Name
			break
		}
	}
}

func (m datePicker) View() string {
	var sb strings.Builder

	// Title
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	sb.WriteString(titleStyle.Render(m.title))
	sb.WriteString("\n\n")

	// Status line
	selectedDate := time.Date(m.selectedYear, m.selectedMonth, m.selectedDay,
		m.initialTime.Hour(), m.initialTime.Minute(), m.initialTime.Second(), 0, time.Local)
	statusStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	sb.WriteString("Selected: ")
	sb.WriteString(statusStyle.Render(selectedDate.Format("2006-01-02 15:04:05 -07:00")))
	sb.WriteString("\n\n")

	// Month/Year header
	headerStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("3"))
	sb.WriteString(headerStyle.Render(fmt.Sprintf("%s %d", m.selectedMonth.String(), m.selectedYear)))
	sb.WriteString("\n")

	// Render calendar
	sb.WriteString(m.renderCalendar())
	sb.WriteString("\n")

	// Month navigation hint
	navHintStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	sb.WriteString(navHintStyle.Render("  ◀ Ctrl+P: Prev Month  |  Ctrl+N: Next Month ▶"))
	sb.WriteString("\n\n")

	// Time input
	timeLabel := "Time (HH:MM:SS): "
	if m.mode == modeTimeInput {
		timeLabel = "> " + timeLabel
	} else {
		timeLabel = "  " + timeLabel
	}
	sb.WriteString(timeLabel)
	sb.WriteString(m.timeInput.View())
	sb.WriteString("\n")

	// Timezone input
	tzLabel := "Time Zone: "
	if m.mode == modeTimezoneInput {
		tzLabel = "> " + tzLabel
	} else {
		tzLabel = "  " + tzLabel
	}
	sb.WriteString(tzLabel)
	sb.WriteString(m.timezoneInput.View())
	sb.WriteString("\n")

	// Show timezone suggestions
	if m.showTzSuggestions && m.mode == modeTimezoneInput {
		suggStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
		for i, match := range m.tzMatches {
			if i < 5 {
				sb.WriteString("    ")
				sb.WriteString(suggStyle.Render(match))
				sb.WriteString("\n")
			}
		}
	}
	sb.WriteString("\n")

	// Buttons
	sb.WriteString(m.renderButtons())
	sb.WriteString("\n\n")

	// Help
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	sb.WriteString(helpStyle.Render("Tab: Next field  |  Enter: Select  |  Esc: Cancel"))

	// Wrap in border
	borderStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("6")).
		Padding(1, 2)

	return borderStyle.Render(sb.String())
}

func (m datePicker) renderCalendar() string {
	var sb strings.Builder

	// Day headers
	dayHeaderStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("6")).Bold(true)
	var days []string
	if m.firstDayOfWeek == time.Monday {
		days = []string{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}
	} else {
		days = []string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"}
	}

	sb.WriteString("  ")
	for _, day := range days {
		sb.WriteString(dayHeaderStyle.Render(fmt.Sprintf("%-4s", day)))
	}
	sb.WriteString("\n")

	// Get first day of month
	firstDay := time.Date(m.selectedYear, m.selectedMonth, 1, 0, 0, 0, 0, time.Local)
	lastDay := time.Date(m.selectedYear, m.selectedMonth+1, 0, 0, 0, 0, 0, time.Local).Day()

	// Calculate starting position
	startPos := int(firstDay.Weekday())
	if m.firstDayOfWeek == time.Monday {
		startPos = (startPos + 6) % 7
	}

	// Render calendar grid
	normalStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("15"))
	selectedStyle := lipgloss.NewStyle().Background(lipgloss.Color("2")).Foreground(lipgloss.Color("0")).Bold(true)
	cursorStyle := lipgloss.NewStyle().Background(lipgloss.Color("8")).Foreground(lipgloss.Color("15"))

	row, col := 0, startPos
	sb.WriteString("  ")

	// Empty cells before first day
	for i := 0; i < startPos; i++ {
		sb.WriteString("    ")
	}

	// Days of month
	for day := 1; day <= lastDay; day++ {
		dayStr := fmt.Sprintf("%-4d", day)

		isSelected := day == m.selectedDay &&
			m.selectedMonth == m.initialTime.Month() &&
			m.selectedYear == m.initialTime.Year()

		isCursor := (m.mode == modeCalendar && row == m.cursorRow && col == m.cursorCol)

		if isSelected {
			sb.WriteString(selectedStyle.Render(dayStr))
		} else if isCursor {
			sb.WriteString(cursorStyle.Render(dayStr))
			// Update selected day based on cursor position
			if m.mode == modeCalendar {
				m.selectedDay = day
			}
		} else {
			sb.WriteString(normalStyle.Render(dayStr))
		}

		col++
		if col >= 7 {
			col = 0
			row++
			sb.WriteString("\n  ")
		}
	}

	return sb.String()
}

func (m datePicker) renderButtons() string {
	normalStyle := lipgloss.NewStyle().
		Padding(0, 2).
		Foreground(lipgloss.Color("15"))

	selectedStyle := lipgloss.NewStyle().
		Padding(0, 2).
		Background(lipgloss.Color("6")).
		Foreground(lipgloss.Color("0")).
		Bold(true)

	buttons := []string{"Set to Now", "Save", "Cancel"}
	var rendered []string

	for i, btn := range buttons {
		if m.mode == modeButtons && i == m.buttonIndex {
			rendered = append(rendered, selectedStyle.Render(btn))
		} else {
			rendered = append(rendered, normalStyle.Render(btn))
		}
	}

	return "  " + strings.Join(rendered, "  ")
}

// rangePicker is a bubbletea model for selecting time ranges
type rangePicker struct {
	width  int
	height int

	// Predefined range list
	predefinedList list.Model

	// Custom range input
	customInput textinput.Model

	// UI state
	mode        int // 0=predefined list, 1=custom input, 2=buttons
	buttonIndex int // 0=Apply Predefined, 1=Apply Custom, 2=Cancel

	// Current range for display
	currentFrom time.Time
	currentTo   time.Time

	err error
}

func newRangePicker(currentFrom, currentTo time.Time, width, height int) rangePicker {
	// Create list items
	items := make([]list.Item, len(predefinedRanges))
	for i, r := range predefinedRanges {
		items[i] = rangeItem{title: r}
	}

	// Create list
	delegate := list.NewDefaultDelegate()
	predefinedList := list.New(items, delegate, width-4, height/2)
	predefinedList.Title = "Predefined Ranges"
	predefinedList.SetShowHelp(false)

	// Create custom input
	customInput := textinput.New()
	customInput.Placeholder = "e.g., now-1h or now-7d"
	customInput.CharLimit = 50
	customInput.Width = 40

	return rangePicker{
		width:          width,
		height:         height,
		predefinedList: predefinedList,
		customInput:    customInput,
		mode:           0,
		currentFrom:    currentFrom,
		currentTo:      currentTo,
	}
}

// rangeItem implements list.Item
type rangeItem struct {
	title string
}

func (i rangeItem) FilterValue() string { return i.title }
func (i rangeItem) Title() string       { return i.title }
func (i rangeItem) Description() string { return "" }

func (m rangePicker) Init() tea.Cmd {
	return nil
}

func (m rangePicker) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc":
			return m, func() tea.Msg {
				return RangeSelectedMsg{Canceled: true}
			}

		case "tab":
			m.mode = (m.mode + 1) % 3
			if m.mode == 1 {
				m.customInput.Focus()
			} else {
				m.customInput.Blur()
			}
			return m, nil

		case "shift+tab":
			m.mode = (m.mode + 2) % 3
			if m.mode == 1 {
				m.customInput.Focus()
			} else {
				m.customInput.Blur()
			}
			return m, nil

		case "enter":
			switch m.mode {
			case 0: // Apply predefined
				if item, ok := m.predefinedList.SelectedItem().(rangeItem); ok {
					from, to := applyPredefinedRange(item.title)
					return m, func() tea.Msg {
						return RangeSelectedMsg{FromTime: from, ToTime: to}
					}
				}

			case 1: // Apply custom
				expr := m.customInput.Value()
				if expr != "" {
					from, to, ok := applyCustomRange(expr)
					if ok {
						return m, func() tea.Msg {
							return RangeSelectedMsg{FromTime: from, ToTime: to}
						}
					}
					m.err = fmt.Errorf("invalid range expression")
				}

			case 2: // Buttons
				switch m.buttonIndex {
				case 0: // Apply Predefined
					if item, ok := m.predefinedList.SelectedItem().(rangeItem); ok {
						from, to := applyPredefinedRange(item.title)
						return m, func() tea.Msg {
							return RangeSelectedMsg{FromTime: from, ToTime: to}
						}
					}
				case 1: // Apply Custom
					expr := m.customInput.Value()
					if expr != "" {
						from, to, ok := applyCustomRange(expr)
						if ok {
							return m, func() tea.Msg {
								return RangeSelectedMsg{FromTime: from, ToTime: to}
							}
						}
						m.err = fmt.Errorf("invalid range expression")
					}
				case 2: // Cancel
					return m, func() tea.Msg {
						return RangeSelectedMsg{Canceled: true}
					}
				}
			}
		}

		// Mode-specific handling
		switch m.mode {
		case 0:
			m.predefinedList, cmd = m.predefinedList.Update(msg)
			return m, cmd
		case 1:
			m.customInput, cmd = m.customInput.Update(msg)
			return m, cmd
		case 2:
			switch msg.String() {
			case "left", "h":
				m.buttonIndex = (m.buttonIndex + 2) % 3
			case "right", "l":
				m.buttonIndex = (m.buttonIndex + 1) % 3
			}
			return m, nil
		}
	}

	return m, nil
}

func (m rangePicker) View() string {
	var sb strings.Builder

	// Title
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	sb.WriteString(titleStyle.Render("Set Time Range"))
	sb.WriteString("\n\n")

	// Current range
	statusStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	sb.WriteString(statusStyle.Render(fmt.Sprintf("Current: %s to %s",
		m.currentFrom.Format("2006-01-02 15:04:05"),
		m.currentTo.Format("2006-01-02 15:04:05"))))
	sb.WriteString("\n\n")

	// Predefined list
	listLabel := "Predefined Ranges:"
	if m.mode == 0 {
		listLabel = "> " + listLabel
	} else {
		listLabel = "  " + listLabel
	}
	sb.WriteString(listLabel)
	sb.WriteString("\n")
	sb.WriteString(m.predefinedList.View())
	sb.WriteString("\n\n")

	// Custom input
	customLabel := "Custom Range: "
	if m.mode == 1 {
		customLabel = "> " + customLabel
	} else {
		customLabel = "  " + customLabel
	}
	sb.WriteString(customLabel)
	sb.WriteString(m.customInput.View())
	sb.WriteString("\n")

	// Error message
	if m.err != nil {
		errStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
		sb.WriteString("\n")
		sb.WriteString(errStyle.Render(fmt.Sprintf("Error: %v", m.err)))
	}
	sb.WriteString("\n\n")

	// Buttons
	sb.WriteString(m.renderRangeButtons())
	sb.WriteString("\n\n")

	// Help
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	sb.WriteString(helpStyle.Render("Tab: Next field  |  Enter: Apply  |  Esc: Cancel"))

	// Wrap in border
	borderStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("6")).
		Padding(1, 2)

	return borderStyle.Render(sb.String())
}

func (m rangePicker) renderRangeButtons() string {
	normalStyle := lipgloss.NewStyle().
		Padding(0, 2).
		Foreground(lipgloss.Color("15"))

	selectedStyle := lipgloss.NewStyle().
		Padding(0, 2).
		Background(lipgloss.Color("6")).
		Foreground(lipgloss.Color("0")).
		Bold(true)

	buttons := []string{"Apply Predefined", "Apply Custom", "Cancel"}
	var rendered []string

	for i, btn := range buttons {
		if m.mode == 2 && i == m.buttonIndex {
			rendered = append(rendered, selectedStyle.Render(btn))
		} else {
			rendered = append(rendered, normalStyle.Render(btn))
		}
	}

	return "  " + strings.Join(rendered, "  ")
}

// Helper functions for range calculation

func applyPredefinedRange(option string) (time.Time, time.Time) {
	now := time.Now()
	toTime := now
	var fromTime time.Time

	switch option {
	case "Last 5 minutes":
		fromTime = now.Add(-5 * time.Minute)
	case "Last 15 minutes":
		fromTime = now.Add(-15 * time.Minute)
	case "Last 30 minutes":
		fromTime = now.Add(-30 * time.Minute)
	case "Last 1 hour":
		fromTime = now.Add(-1 * time.Hour)
	case "Last 3 hours":
		fromTime = now.Add(-3 * time.Hour)
	case "Last 6 hours":
		fromTime = now.Add(-6 * time.Hour)
	case "Last 12 hours":
		fromTime = now.Add(-12 * time.Hour)
	case "Last 24 hours":
		fromTime = now.Add(-24 * time.Hour)
	case "Last 2 days":
		fromTime = now.Add(-48 * time.Hour)
	case "Last 7 days":
		fromTime = now.Add(-7 * 24 * time.Hour)
	case "Last 30 days":
		fromTime = now.Add(-30 * 24 * time.Hour)
	case "Last 90 days":
		fromTime = now.Add(-90 * 24 * time.Hour)
	case "Last 6 months":
		fromTime = now.Add(-180 * 24 * time.Hour)
	case "Last 1 year":
		fromTime = now.Add(-365 * 24 * time.Hour)
	case "Last 2 years":
		fromTime = now.Add(-2 * 365 * 24 * time.Hour)
	case "Last 5 years":
		fromTime = now.Add(-5 * 365 * 24 * time.Hour)
	case "Today":
		y, m, d := now.Date()
		fromTime = time.Date(y, m, d, 0, 0, 0, 0, now.Location())
	case "Yesterday":
		yesterday := now.Add(-24 * time.Hour)
		y, m, d := yesterday.Date()
		fromTime = time.Date(y, m, d, 0, 0, 0, 0, now.Location())
		toTime = time.Date(y, m, d, 23, 59, 59, 999999999, now.Location())
	case "This week":
		daysFromMonday := int(now.Weekday())
		if daysFromMonday == 0 {
			daysFromMonday = 6
		} else {
			daysFromMonday--
		}
		y, m, d := now.Date()
		fromTime = time.Date(y, m, d-daysFromMonday, 0, 0, 0, 0, now.Location())
	case "This month":
		y, m, _ := now.Date()
		fromTime = time.Date(y, m, 1, 0, 0, 0, 0, now.Location())
	case "This year":
		y, _, _ := now.Date()
		fromTime = time.Date(y, 1, 1, 0, 0, 0, 0, now.Location())
	default:
		fromTime = now.Add(-1 * time.Hour)
	}

	return fromTime, toTime
}

func applyCustomRange(expr string) (time.Time, time.Time, bool) {
	toTime := time.Now()

	// Parse expressions like "now-1h", "now-7d", etc.
	if strings.HasPrefix(expr, "now") {
		re := regexp.MustCompile(`now-(\d+)([smhdwMyY])`)
		matches := re.FindStringSubmatch(expr)

		if len(matches) == 3 {
			value, err := strconv.Atoi(matches[1])
			if err != nil {
				return time.Time{}, time.Time{}, false
			}

			unit := matches[2]
			var duration time.Duration

			switch unit {
			case "s", "SS":
				duration = time.Duration(value) * time.Second
			case "m", "mm":
				duration = time.Duration(value) * time.Minute
			case "h", "H", "HH":
				duration = time.Duration(value) * time.Hour
			case "d", "D":
				duration = time.Duration(value*24) * time.Hour
			case "w", "W":
				duration = time.Duration(value*7*24) * time.Hour
			case "M", "MM":
				duration = time.Duration(value*30*24) * time.Hour
			case "y", "Y":
				duration = time.Duration(value*365*24) * time.Hour
			default:
				return time.Time{}, time.Time{}, false
			}

			fromTime := toTime.Add(-duration)
			return fromTime, toTime, true
		}
	}

	return time.Time{}, time.Time{}, false
}

// App methods for showing date and range pickers

func (a *App) showFromDatePicker() {
	picker := newDatePicker("From Date/Time", a.state.FromTime, true, a.width, a.height)
	a.datePickerHandler = picker
	a.currentPage = pageDatePicker
}

func (a *App) showToDatePicker() {
	picker := newDatePicker("To Date/Time", a.state.ToTime, false, a.width, a.height)
	a.datePickerHandler = picker
	a.currentPage = pageDatePicker
}

func (a *App) showRangePicker() {
	picker := newRangePicker(a.state.FromTime, a.state.ToTime, a.width, a.height)
	a.rangePickerHandler = picker
	a.currentPage = pageRangePicker
}
