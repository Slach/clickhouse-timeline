package tui

import (
	"fmt"
	"github.com/Slach/clickhouse-timeline/pkg/timezone"
	"golang.org/x/text/message"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // Import tzdata to embed timezone database

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
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

// showFromDatePicker displays a date picker for the "from" time
func (a *App) showFromDatePicker() {
	a.showDatePicker("From Date/Time", a.fromTime, func(t time.Time) {
		a.fromTime = t
		a.mainView.SetText(fmt.Sprintf("From time set to: %s", t.Format(time.RFC3339)))
	})
}

// showToDatePicker displays a date picker for the "to" time
func (a *App) showToDatePicker() {
	a.showDatePicker("To Date/Time", a.toTime, func(t time.Time) {
		a.toTime = t
		a.mainView.SetText(fmt.Sprintf("To time set to: %s", t.Format(time.RFC3339)))
	})
}

// showDatePicker displays a date and time picker with a calendar widget
func (a *App) showDatePicker(title string, initialTime time.Time, onSelect func(time.Time)) {
	// Create a calendar widget
	calendar := tview.NewTable().
		SetBorders(false).
		SetSelectable(true, true)

	// Time input field in hh:mm:ss format
	timeField := tview.NewInputField().
		SetLabel("Time (hh:mm:ss): ").
		SetText(fmt.Sprintf("%02d:%02d:%02d", initialTime.Hour(), initialTime.Minute(), initialTime.Second())).
		SetFieldWidth(10).
		SetAcceptanceFunc(func(textToCheck string, lastChar rune) bool {
			// Validate time format
			matched, _ := regexp.MatchString(`^([0-1]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$`, textToCheck)
			return matched || textToCheck == "" || lastChar == ':' || (lastChar >= '0' && lastChar <= '9')
		})

	// Get timezone information from initialTime first
	tzName, tzOffset := initialTime.Zone()
	tzOffset = tzOffset / 60 // Convert to minutes

	// Time zone input field with autocomplete
	timeZoneInput := tview.NewInputField().
		SetLabel("Time Zone: ").
		SetFieldWidth(40).
		SetPlaceholder("Type to search...")

	// Store the currently selected timezone
	var selectedTimeZone = tzName
	var tzDisplayText string

	// Try to find the timezone in our list first
	tzFound := false
	for _, zone := range timezone.TimeZones {
		if zone.Name == tzName || runtime.GOOS == "windows" && zone.WindowsName == tzName {
			selectedTimeZone = zone.Name
			tzDisplayText = zone.DisplayText
			tzFound = true
			break
		}
	}

	// If still not found, try to get current timezone info
	if !tzFound {
		currentTZ, err := timezone.GetCurrentTimeZone()
		if err == nil {
			selectedTimeZone = currentTZ.Name
			tzDisplayText = currentTZ.DisplayText
			tzFound = true
		}
	}

	// If not found by name, try by offset
	if !tzFound {
		for _, zone := range timezone.TimeZones {
			if zone.Offset == tzOffset {
				selectedTimeZone = zone.Name
				tzDisplayText = zone.DisplayText
				tzFound = true
				break
			}
		}
	}

	// Set the timezone display text
	timeZoneInput.SetText(tzDisplayText)

	// Set up autocomplete function
	timeZoneInput.SetAutocompleteFunc(func(currentText string) []string {
		// If empty, show some common timezones or return empty list
		if currentText == "" {
			return nil
		}

		// Filter time zones based on the search text
		var matches []string
		lowerText := strings.ToLower(currentText)

		for _, tz := range timezone.TimeZones {
			lowerTz := strings.ToLower(tz.DisplayText)
			if strings.Contains(lowerTz, lowerText) {
				matches = append(matches, tz.DisplayText)
				// Limit results to avoid overwhelming the UI
				if len(matches) >= 15 {
					break
				}
			}
		}

		return matches
	})

	var saveButton, cancelButton *tview.Button
	// Handle selection from autocomplete
	timeZoneInput.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEnter {
			// Find the matching timezone
			currentText := timeZoneInput.GetText()
			for _, tz := range timezone.TimeZones {
				if tz.DisplayText == currentText {
					selectedTimeZone = tz.Name
					break
				}
			}
			a.tviewApp.SetFocus(saveButton)
		} else if key == tcell.KeyEscape {
			a.tviewApp.SetFocus(cancelButton)
		}
	})

	// Status text to show selected date
	statusText := tview.NewTextView().
		SetDynamicColors(true).
		SetText(fmt.Sprintf("Selected: [green]%s[white]", initialTime.Format("2006-01-02 15:04:05 -07:00")))

	// Variables to track current view state
	selectedYear := initialTime.Year()
	selectedMonth := initialTime.Month()
	selectedDay := initialTime.Day()

	// Create a header for month and year
	headerText := tview.NewTextView().
		SetTextAlign(tview.AlignLeft).
		SetDynamicColors(true)

	// Determine first day of week based on locale
	// Default to Monday (1) as first day of week for most locales
	firstDayOfWeek := time.Monday

	// Get system locale and check if it's US-like (Sunday first)
	tag := message.MatchLanguage("")
	tagStr := tag.String()
	if tagStr == "en-US" || tagStr == "en-CA" ||
		strings.HasPrefix(tagStr, "ar") || strings.HasPrefix(tagStr, "fa") {
		firstDayOfWeek = time.Sunday
	}

	// Function to update the calendar display
	updateCalendar := func() {
		calendar.Clear()

		// Update header with month and year
		monthYearHeader := fmt.Sprintf("[yellow]%s %d[white]", selectedMonth.String(), selectedYear)
		headerText.SetText(monthYearHeader)

		// Set day headers based on first day of week
		var days []string
		if firstDayOfWeek == time.Monday {
			days = []string{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}
		} else {
			days = []string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"}
		}

		for i, day := range days {
			calendar.SetCell(0, i, tview.NewTableCell(day).
				SetTextColor(tcell.ColorAqua).
				SetAlign(tview.AlignCenter).
				SetSelectable(false))
		}

		// Get the first day of the month
		firstDay := time.Date(selectedYear, selectedMonth, 1, 0, 0, 0, 0, time.Local)
		lastDay := time.Date(selectedYear, selectedMonth+1, 0, 0, 0, 0, 0, time.Local).Day()

		// Calculate the starting position based on the first day of week
		startPos := int(firstDay.Weekday())
		if firstDayOfWeek == time.Monday {
			// Convert from Sunday=0 to Monday=0 based system
			startPos = (startPos + 6) % 7
		}

		// Fill in the calendar
		row, col := 1, startPos
		for day := 1; day <= lastDay; day++ {
			// Highlight the selected day
			cell := tview.NewTableCell(fmt.Sprintf("%2d", day)).
				SetAlign(tview.AlignCenter).
				SetSelectable(true)

			// Highlight current day
			if day == selectedDay &&
				selectedMonth == initialTime.Month() &&
				selectedYear == initialTime.Year() {
				cell.SetBackgroundColor(tcell.ColorGreen).SetTextColor(tcell.ColorBlack)
			}

			calendar.SetCell(row, col, cell)

			// Move to the next position
			col++
			if col > 6 {
				col = 0
				row++
			}
		}

		// Update status text
		selectedDate := time.Date(selectedYear, selectedMonth, selectedDay,
			initialTime.Hour(), initialTime.Minute(), initialTime.Second(), 0, time.Local)
		statusText.SetText(fmt.Sprintf("Selected: [green]%s[white]", selectedDate.Format("2006-01-02 15:04:05 -07:00")))
	}

	// Navigation buttons for the calendar
	prevMonthBtn := tview.NewButton("◀ Prev Month").SetSelectedFunc(func() {
		if selectedMonth == 1 {
			selectedMonth = 12
			selectedYear--
		} else {
			selectedMonth--
		}
		updateCalendar()
	})

	nextMonthBtn := tview.NewButton("Next Month ▶").SetSelectedFunc(func() {
		if selectedMonth == 12 {
			selectedMonth = 1
			selectedYear++
		} else {
			selectedMonth++
		}
		updateCalendar()
	})

	// Add navigation buttons to a flex container with margins
	navFlex := tview.NewFlex().
		AddItem(nil, 1, 0, false).         // Left margin
		AddItem(prevMonthBtn, 0, 2, true). // Button with weight 2
		AddItem(nil, 1, 0, false).         // Margin between buttons
		AddItem(nextMonthBtn, 0, 2, true). // Button with weight 2
		AddItem(nil, 1, 0, false)          // Right margin

	// Create buttons
	nowButton := tview.NewButton("Set to Now").SetSelectedFunc(func() {
		now := time.Now()
		selectedYear = now.Year()
		selectedMonth = now.Month()
		selectedDay = now.Day()
		selectedTimeZone, _ = now.Zone()
		timeField.SetText(fmt.Sprintf("%02d:%02d:%02d", now.Hour(), now.Minute(), now.Second()))
		updateCalendar()
	})

	saveButton = tview.NewButton("Save").SetSelectedFunc(func() {
		// Parse time from the input field
		timeStr := timeField.GetText()
		var hour, minute, sec int

		// Default to current time if parsing fails
		if _, err := fmt.Sscanf(timeStr, "%d:%d:%d", &hour, &minute, &sec); err != nil {
			hour, minute, sec = initialTime.Clock()
		}

		// Get the selected timezone
		location := time.Local
		if selectedTimeZone != "" {
			loc, err := time.LoadLocation(selectedTimeZone)
			if err == nil {
				location = loc
			}
		}

		// Create the final time with the selected time zone
		selectedTime := time.Date(selectedYear, selectedMonth, selectedDay, hour, minute, sec, 0, location)
		onSelect(selectedTime)
		a.pages.RemovePage("datepicker")
		a.pages.SwitchToPage("main")
	})

	cancelButton = tview.NewButton("Cancel").SetSelectedFunc(func() {
		a.pages.RemovePage("datepicker")
		a.pages.SwitchToPage("main")
	})

	// Create button flex with margins
	buttonFlex := tview.NewFlex().
		AddItem(nil, 1, 0, false).         // Left margin
		AddItem(nowButton, 0, 2, true).    // Button with weight 2
		AddItem(nil, 1, 0, false).         // Margin between buttons
		AddItem(saveButton, 0, 2, true).   // Button with weight 2
		AddItem(nil, 1, 0, false).         // Margin between buttons
		AddItem(cancelButton, 0, 2, true). // Button with weight 2
		AddItem(nil, 1, 0, false)          // Right margin

	// Set up calendar selection handler
	calendar.SetSelectedFunc(func(row, col int) {
		// Only process clicks on actual days (row >= 1)
		cell := calendar.GetCell(row, col)
		if cell != nil && cell.Text != "" {
			day, err := strconv.Atoi(strings.TrimSpace(cell.Text))
			if err == nil && day > 0 {
				selectedDay = day

				// Update the calendar immediately
				a.tviewApp.QueueUpdateDraw(func() {
					updateCalendar()
					timeField.SetText(fmt.Sprintf("%02d:%02d:%02d", initialTime.Hour(), initialTime.Minute(), initialTime.Second()))
					a.tviewApp.SetFocus(timeField)
				})
			}
		}
	})

	// Set up calendar keyboard navigation
	calendar.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		// Add month navigation with various key combinations
		if (event.Key() == tcell.KeyLeft && (event.Modifiers()&tcell.ModAlt != 0)) ||
			(event.Key() == tcell.KeyLeft && (event.Modifiers()&tcell.ModCtrl != 0)) ||
			(event.Rune() == 16) { // Ctrl+P
			if selectedMonth == 1 {
				selectedMonth = 12
				selectedYear--
			} else {
				selectedMonth--
			}
			updateCalendar()
			return nil
		} else if (event.Key() == tcell.KeyRight && (event.Modifiers()&tcell.ModAlt != 0)) ||
			(event.Key() == tcell.KeyRight && (event.Modifiers()&tcell.ModCtrl != 0)) ||
			(event.Rune() == 14) { // Ctrl+N
			if selectedMonth == 12 {
				selectedMonth = 1
				selectedYear++
			} else {
				selectedMonth++
			}
			updateCalendar()
			return nil
		}

		// Prevent arrow keys from causing focus loss
		if event.Key() == tcell.KeyUp || event.Key() == tcell.KeyDown ||
			event.Key() == tcell.KeyLeft || event.Key() == tcell.KeyRight {
			// Let the table handle these keys internally
			return event
		}

		return event
	})

	// Make calendar selectable again when clicked
	calendar.SetMouseCapture(func(action tview.MouseAction, event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse) {
		if action == tview.MouseLeftClick {
			a.tviewApp.SetFocus(calendar)
		}
		return action, event
	})

	// Set up time field keyboard navigation
	timeField.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEnter {
			a.tviewApp.SetFocus(saveButton)
		} else if key == tcell.KeyEscape {
			a.pages.RemovePage("datepicker")
			a.pages.SwitchToPage("main")
		}
	})

	// Prevent ':' from triggering command mode while editing time
	timeField.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Rune() == ':' {
			return event // Allow ':' character in the time field
		}
		return event
	})

	// Create a flex for time input and timezone input field (horizontal layout)
	timeInputFlex := tview.NewFlex().
		SetDirection(tview.FlexColumn).
		AddItem(timeField, 0, 1, false).
		AddItem(nil, 1, 0, false).          // Add a small space between fields
		AddItem(timeZoneInput, 0, 2, false) // Give timezone input more space

	// Create layout with padding
	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(statusText, 1, 0, false).
		AddItem(headerText, 1, 0, false).
		AddItem(calendar, 0, 1, true).
		AddItem(nil, 1, 0, false). // Add padding
		AddItem(timeInputFlex, 2, 0, false).
		AddItem(nil, 1, 0, false). // Add padding
		AddItem(navFlex, 1, 0, false).
		AddItem(nil, 1, 0, false). // Add padding
		AddItem(buttonFlex, 1, 0, false)

	// Set border and title
	flex.SetBorder(true).SetTitle(title)

	// Initial calendar update
	updateCalendar()

	// Add page and show
	a.pages.AddPage("datepicker", flex, true, true)
	a.pages.SwitchToPage("datepicker")
	a.tviewApp.SetFocus(calendar)

	// Make all components in the flex capture mouse events to restore focus
	flex.SetMouseCapture(func(action tview.MouseAction, event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse) {
		// On any click, check if we need to restore focus to a component
		if action == tview.MouseLeftClick {
			x, y := event.Position()
			if calendar.InRect(x, y) {
				a.tviewApp.SetFocus(calendar)
			} else if prevMonthBtn.InRect(x, y) {
				a.tviewApp.SetFocus(prevMonthBtn)
			} else if nextMonthBtn.InRect(x, y) {
				a.tviewApp.SetFocus(nextMonthBtn)
			} else if timeField.InRect(x, y) {
				a.tviewApp.SetFocus(timeField)
			} else if timeZoneInput.InRect(x, y) {
				a.tviewApp.SetFocus(timeZoneInput)
			} else if nowButton.InRect(x, y) {
				a.tviewApp.SetFocus(nowButton)
			} else if saveButton.InRect(x, y) {
				a.tviewApp.SetFocus(saveButton)
			} else if cancelButton.InRect(x, y) {
				a.tviewApp.SetFocus(cancelButton)
			}
		}
		return action, event
	})

	// Handle keyboard events
	flex.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape {
			a.pages.RemovePage("datepicker")
			a.pages.SwitchToPage("main")
			return nil
		}

		// Handle Enter key when calendar is focused
		if event.Key() == tcell.KeyEnter && a.tviewApp.GetFocus() == calendar {
			row, col := calendar.GetSelection()
			cell := calendar.GetCell(row, col)
			if cell != nil && cell.Text != "" {
				day, err := strconv.Atoi(strings.TrimSpace(cell.Text))
				if err == nil && day > 0 {
					selectedDay = day
					updateCalendar()
					a.tviewApp.SetFocus(timeField)
					return nil
				}
			}
		}

		// Tab key navigation
		if event.Key() == tcell.KeyTab {
			currentFocus := a.tviewApp.GetFocus()
			if currentFocus == calendar {
				a.tviewApp.SetFocus(timeZoneInput)
			} else if currentFocus == timeField {
				a.tviewApp.SetFocus(timeZoneInput)
			} else if currentFocus == timeZoneInput {
				a.tviewApp.SetFocus(prevMonthBtn)
			} else if currentFocus == prevMonthBtn {
				a.tviewApp.SetFocus(nextMonthBtn)
			} else if currentFocus == nextMonthBtn {
				a.tviewApp.SetFocus(saveButton)
			} else if currentFocus == nowButton {
				a.tviewApp.SetFocus(saveButton)
			} else if currentFocus == saveButton {
				a.tviewApp.SetFocus(cancelButton)
			} else if currentFocus == cancelButton {
				a.tviewApp.SetFocus(calendar)
			} else {
				// If focus is lost or on an unknown element, return it to calendar
				a.tviewApp.SetFocus(calendar)
			}
			return nil
		}

		// Ctrl+C to return focus to calendar
		if event.Key() == tcell.KeyCtrlC {
			a.tviewApp.SetFocus(calendar)
			return nil
		}

		// Arrow key navigation - only for non-calendar components
		currentFocus := a.tviewApp.GetFocus()

		// Skip arrow key navigation when calendar has focus
		if currentFocus == calendar {
			return event
		}

		if event.Key() == tcell.KeyDown {
			if currentFocus == prevMonthBtn || currentFocus == nextMonthBtn {
				a.tviewApp.SetFocus(saveButton)
				return nil
			} else if currentFocus == timeField {
				a.tviewApp.SetFocus(timeZoneInput)
				return nil
			} else if currentFocus == timeZoneInput {
				a.tviewApp.SetFocus(nowButton)
				return nil
			}
		} else if event.Key() == tcell.KeyUp {
			if currentFocus == nowButton || currentFocus == saveButton || currentFocus == cancelButton {
				a.tviewApp.SetFocus(nextMonthBtn)
				return nil
			} else if currentFocus == timeZoneInput {
				a.tviewApp.SetFocus(timeField)
				return nil
			} else if currentFocus == timeField {
				a.tviewApp.SetFocus(calendar)
				return nil
			} else if currentFocus == prevMonthBtn || currentFocus == nextMonthBtn {
				a.tviewApp.SetFocus(timeZoneInput)
				return nil
			}
		} else if event.Key() == tcell.KeyRight {
			if currentFocus == prevMonthBtn {
				a.tviewApp.SetFocus(nextMonthBtn)
				return nil
			} else if currentFocus == nowButton {
				a.tviewApp.SetFocus(saveButton)
				return nil
			} else if currentFocus == saveButton {
				a.tviewApp.SetFocus(cancelButton)
				return nil
			}
		} else if event.Key() == tcell.KeyLeft {
			if currentFocus == nextMonthBtn {
				a.tviewApp.SetFocus(prevMonthBtn)
				return nil
			} else if currentFocus == cancelButton {
				a.tviewApp.SetFocus(saveButton)
				return nil
			} else if currentFocus == saveButton {
				a.tviewApp.SetFocus(nowButton)
				return nil
			}
		}

		return event
	})
}

// showRangePicker displays a form to set a time range with Grafana-like expressions
func (a *App) showRangePicker() {
	// Dropdown for predefined ranges
	rangeDropdown := tview.NewDropDown().
		SetLabel("Predefined Range").
		SetFieldWidth(30)

	for _, r := range predefinedRanges {
		rangeDropdown.AddOption(r, nil)
	}

	// Custom range input
	customRangeInput := tview.NewInputField().
		SetLabel("Custom Range").
		SetFieldWidth(30).
		SetPlaceholder("e.g., now-1h or now-7d")

	// Status text
	statusText := tview.NewTextView().
		SetDynamicColors(true).
		SetText("Current range: " + a.formatTimeRange())

	// Create buttons
	applyPredefinedButton := tview.NewButton("Apply Predefined").SetSelectedFunc(func() {
		_, option := rangeDropdown.GetCurrentOption()
		if option != "" {
			a.applyPredefinedRange(option)
			a.mainView.SetText(fmt.Sprintf("Time range set to: %s", a.formatTimeRange()))
			a.pages.RemovePage("rangepicker")
			a.pages.SwitchToPage("main")
		}
	})

	applyCustomButton := tview.NewButton("Apply Custom").SetSelectedFunc(func() {
		expr := customRangeInput.GetText()
		if expr != "" {
			if a.applyCustomRange(expr) {
				a.mainView.SetText(fmt.Sprintf("Time range set to: %s", a.formatTimeRange()))
				a.pages.RemovePage("rangepicker")
				a.pages.SwitchToPage("main")
			} else {
				statusText.SetText("[red]Invalid range expression[white]\nFormat: now-1h, now-7d, etc.")
			}
		}
	})

	cancelButton := tview.NewButton("Cancel").SetSelectedFunc(func() {
		a.pages.RemovePage("rangepicker")
		a.pages.SwitchToPage("main")
	})

	// Create button flex with margins
	buttonFlex := tview.NewFlex().
		AddItem(nil, 1, 0, false).                  // Left margin
		AddItem(applyPredefinedButton, 0, 2, true). // Button with weight 2
		AddItem(nil, 1, 0, false).                  // Margin between buttons
		AddItem(applyCustomButton, 0, 2, true).     // Button with weight 2
		AddItem(nil, 1, 0, false).                  // Margin between buttons
		AddItem(cancelButton, 0, 2, true).          // Button with weight 2
		AddItem(nil, 1, 0, false)                   // Right margin

	// Set up input field keyboard navigation
	customRangeInput.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEnter {
			a.tviewApp.SetFocus(applyCustomButton)
		} else if key == tcell.KeyEscape {
			a.pages.RemovePage("rangepicker")
			a.pages.SwitchToPage("main")
		}
	})

	// Create a flex layout for the entire form with padding
	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(statusText, 3, 0, false).
		AddItem(rangeDropdown, 1, 0, true).
		AddItem(nil, 1, 0, false). // Add padding
		AddItem(customRangeInput, 1, 0, false).
		AddItem(nil, 1, 0, false). // Add padding
		AddItem(buttonFlex, 1, 0, false)

	// Set border and title
	flex.SetBorder(true).SetTitle("Set Time Range")

	// Add page and show
	a.pages.AddPage("rangepicker", flex, true, true)
	a.pages.SwitchToPage("rangepicker")
	a.tviewApp.SetFocus(rangeDropdown)

	// Handle keyboard events
	flex.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		// Handle command mode with ':'
		if event.Rune() == ':' {
			a.pages.RemovePage("rangepicker")
			a.pages.SwitchToPage("main")
			a.mainFlex.ResizeItem(a.commandInput, 1, 0) // Show command input
			a.commandInput.SetText("")
			a.tviewApp.SetFocus(a.commandInput)
			return nil
		}

		if event.Key() == tcell.KeyEscape {
			a.pages.RemovePage("rangepicker")
			a.pages.SwitchToPage("main")
			return nil
		}

		// Tab key navigation
		if event.Key() == tcell.KeyTab {
			currentFocus := a.tviewApp.GetFocus()
			if currentFocus == rangeDropdown {
				a.tviewApp.SetFocus(customRangeInput)
			} else if currentFocus == customRangeInput {
				a.tviewApp.SetFocus(applyPredefinedButton)
			} else if currentFocus == applyPredefinedButton {
				a.tviewApp.SetFocus(applyCustomButton)
			} else if currentFocus == applyCustomButton {
				a.tviewApp.SetFocus(cancelButton)
			} else if currentFocus == cancelButton {
				a.tviewApp.SetFocus(rangeDropdown)
			}
			return nil
		}

		// Arrow key navigation
		if event.Key() == tcell.KeyDown {
			currentFocus := a.tviewApp.GetFocus()
			if currentFocus == rangeDropdown {
				a.tviewApp.SetFocus(customRangeInput)
				return nil
			} else if currentFocus == customRangeInput {
				a.tviewApp.SetFocus(applyPredefinedButton)
				return nil
			}
		} else if event.Key() == tcell.KeyUp {
			currentFocus := a.tviewApp.GetFocus()
			if currentFocus == applyPredefinedButton || currentFocus == applyCustomButton || currentFocus == cancelButton {
				a.tviewApp.SetFocus(customRangeInput)
				return nil
			} else if currentFocus == customRangeInput {
				a.tviewApp.SetFocus(rangeDropdown)
				return nil
			}
		} else if event.Key() == tcell.KeyRight {
			currentFocus := a.tviewApp.GetFocus()
			if currentFocus == applyPredefinedButton {
				a.tviewApp.SetFocus(applyCustomButton)
				return nil
			} else if currentFocus == applyCustomButton {
				a.tviewApp.SetFocus(cancelButton)
				return nil
			}
		} else if event.Key() == tcell.KeyLeft {
			currentFocus := a.tviewApp.GetFocus()
			if currentFocus == cancelButton {
				a.tviewApp.SetFocus(applyCustomButton)
				return nil
			} else if currentFocus == applyCustomButton {
				a.tviewApp.SetFocus(applyPredefinedButton)
				return nil
			}
		}

		return event
	})
}

// formatTimeRange returns a formatted string of the current time range
func (a *App) formatTimeRange() string {
	return fmt.Sprintf("From: %s\nTo: %s",
		a.fromTime.Format("2006-01-02 15:04:05 -07:00"),
		a.toTime.Format("2006-01-02 15:04:05 -07:00"))
}

// applyPredefinedRange sets the time range based on a predefined option
func (a *App) applyPredefinedRange(option string) {
	now := time.Now()
	a.toTime = now

	switch option {
	case "Last 5 minutes":
		a.fromTime = now.Add(-5 * time.Minute)
	case "Last 15 minutes":
		a.fromTime = now.Add(-15 * time.Minute)
	case "Last 30 minutes":
		a.fromTime = now.Add(-30 * time.Minute)
	case "Last 1 hour":
		a.fromTime = now.Add(-1 * time.Hour)
	case "Last 3 hours":
		a.fromTime = now.Add(-3 * time.Hour)
	case "Last 6 hours":
		a.fromTime = now.Add(-6 * time.Hour)
	case "Last 12 hours":
		a.fromTime = now.Add(-12 * time.Hour)
	case "Last 24 hours":
		a.fromTime = now.Add(-24 * time.Hour)
	case "Last 2 days":
		a.fromTime = now.Add(-48 * time.Hour)
	case "Last 7 days":
		a.fromTime = now.Add(-7 * 24 * time.Hour)
	case "Last 30 days":
		a.fromTime = now.Add(-30 * 24 * time.Hour)
	case "Last 90 days":
		a.fromTime = now.Add(-90 * 24 * time.Hour)
	case "Last 6 months":
		a.fromTime = now.Add(-180 * 24 * time.Hour)
	case "Last 1 year":
		a.fromTime = now.Add(-365 * 24 * time.Hour)
	case "Last 2 years":
		a.fromTime = now.Add(-2 * 365 * 24 * time.Hour)
	case "Last 5 years":
		a.fromTime = now.Add(-5 * 365 * 24 * time.Hour)
	case "Today":
		y, m, d := now.Date()
		a.fromTime = time.Date(y, m, d, 0, 0, 0, 0, now.Location())
	case "Yesterday":
		yesterday := now.Add(-24 * time.Hour)
		y, m, d := yesterday.Date()
		a.fromTime = time.Date(y, m, d, 0, 0, 0, 0, now.Location())
		a.toTime = time.Date(y, m, d, 23, 59, 59, 999999999, now.Location())
	case "This week":
		// Calculate beginning of week (Monday)
		daysFromMonday := int(now.Weekday())
		if daysFromMonday == 0 { // Sunday
			daysFromMonday = 6
		} else {
			daysFromMonday--
		}
		y, m, d := now.Date()
		a.fromTime = time.Date(y, m, d-daysFromMonday, 0, 0, 0, 0, now.Location())
	case "This month":
		y, m, _ := now.Date()
		a.fromTime = time.Date(y, m, 1, 0, 0, 0, 0, now.Location())
	case "This year":
		y, _, _ := now.Date()
		a.fromTime = time.Date(y, 1, 1, 0, 0, 0, 0, now.Location())
	}
}

// applyCustomRange parses and applies a custom range expression like "now-1h"
func (a *App) applyCustomRange(expr string) bool {
	// Set the "to" time to now
	a.toTime = time.Now()

	// Parse expressions like "now-1h", "now-7d", etc.
	if strings.HasPrefix(expr, "now") {
		// Match patterns like "now-1h", "now-30m", etc.
		re := regexp.MustCompile(`now-(\d+)([smhdwMyY])`)
		matches := re.FindStringSubmatch(expr)

		if len(matches) == 3 {
			value, err := strconv.Atoi(matches[1])
			if err != nil {
				return false
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
				return false
			}

			a.fromTime = a.toTime.Add(-duration)
			return true
		}
	}

	return false
}
