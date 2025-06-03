package tui

import (
	"database/sql"
	"fmt"
	"github.com/gdamore/tcell/v2"
	"github.com/rs/zerolog/log"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/rivo/tview"
)

type LogPanel struct {
	app            *App
	database       string
	table          string
	messageField   string
	timeField      string
	timeMsField    string
	dateField      string
	levelField     string
	windowSize     int
	filters        []LogFilter
	firstEntryTime time.Time
	lastEntryTime  time.Time
	totalRows      int
	logDetails     *tview.Table
	overview       *tview.TextView
	filterPanel    *tview.Flex
	mainFlex       *tview.Flex // Reference to the main flex container for resizing
	databases      []string
	tables         []string
	allFields      []string // Stores all field names from current table
}

type LogFilter struct {
	Field    string
	Operator string
	Value    string
}

type LogEntry struct {
	Time      time.Time
	TimeMs    time.Time
	Date      string
	Level     string
	Message   string
	AllFields map[string]interface{} // Stores all fields not in the main display
}

func (lp *LogPanel) Show() {
	if lp.app.clickHouse == nil {
		lp.app.SwitchToMainPage("Error: Please connect to ClickHouse first")
		return
	}

	// Apply CLI params if available
	if lp.app.CLI != nil {
		if lp.app.CLI.LogsParams.Database != "" {
			lp.database = lp.app.CLI.LogsParams.Database
		}
		if lp.app.CLI.LogsParams.Table != "" {
			lp.table = lp.app.CLI.LogsParams.Table
		}
		if lp.app.CLI.LogsParams.Message != "" {
			lp.messageField = lp.app.CLI.LogsParams.Message
		}
		if lp.app.CLI.LogsParams.Time != "" {
			lp.timeField = lp.app.CLI.LogsParams.Time
		}
		if lp.app.CLI.LogsParams.TimeMs != "" {
			lp.timeMsField = lp.app.CLI.LogsParams.TimeMs
		}
		if lp.app.CLI.LogsParams.Date != "" {
			lp.dateField = lp.app.CLI.LogsParams.Date
		}
		if lp.app.CLI.LogsParams.Level != "" {
			lp.levelField = lp.app.CLI.LogsParams.Level
		}
		if lp.app.CLI.LogsParams.Window > 0 {
			lp.windowSize = lp.app.CLI.LogsParams.Window
		}
	}

	// Query ClickHouse for available databases
	rows, err := lp.app.clickHouse.Query("SELECT name FROM system.databases")
	if err != nil {
		lp.app.SwitchToMainPage(fmt.Sprintf("Error getting databases: %v", err))
		return
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msgf("can't close databases query rows")
		}
	}()

	lp.databases = []string{lp.database}
	for rows.Next() {
		var db string
		if scanErr := rows.Scan(&db); scanErr != nil {
			log.Error().Err(scanErr).Msg("can't scan database name")
			continue
		}
		lp.databases = append(lp.databases, db)
	}

	// Create form with all fields
	form := lp.createForm()

	// Main flex layout
	logsFlex := tview.NewFlex().SetDirection(tview.FlexRow)
	logsFlex.AddItem(form, 0, 1, true)

	lp.app.pages.AddPage("logs", logsFlex, true, true)
	lp.app.pages.SwitchToPage("logs")

	// If all required fields are set via CLI, auto-submit the form
	if lp.database != "" && lp.table != "" && lp.messageField != "" && lp.timeField != "" {
		go func() {
			time.Sleep(500 * time.Millisecond) // Small delay to let UI render
			lp.app.tviewApp.QueueUpdateDraw(func() {
				lp.showLogExplorer()
			})
		}()
	}
}

func (lp *LogPanel) createForm() *tview.Form {
	form := tview.NewForm()
	form.SetBorder(true).SetTitle("Log Explorer")

	// Database dropdown - preselect if CLI param exists
	dbIndex := 0
	if lp.database != "" {
		for i, db := range lp.databases {
			if db == lp.database {
				dbIndex = i
				break
			}
		}
	}
	form.AddDropDown("Database", lp.databases, dbIndex, func(db string, index int) {
		lp.database = db
		lp.updateTableDropdown(form)
	})

	// Table dropdown - preselect if CLI param exists
	form.AddDropDown("Table", []string{lp.table}, 0, func(table string, index int) {
		lp.table = table
		lp.updateFieldDropdowns(form)
	})

	// Field dropdowns - preselect if CLI params exist
	// Create dropdowns for each log field type
	form.AddDropDown("Message Field: ", []string{lp.messageField}, 0, func(field string, index int) {
		lp.messageField = field
	})

	form.AddDropDown("Time Field: ", []string{lp.timeField}, 0, func(field string, index int) {
		lp.timeField = field
	})

	form.AddDropDown("TimeMs Field (optional): ", []string{lp.timeMsField}, 0, func(field string, index int) {
		lp.timeMsField = field
	})

	form.AddDropDown("Date Field (optional): ", []string{lp.dateField}, 0, func(field string, index int) {
		lp.dateField = field
	})

	form.AddDropDown("Level Field (optional): ", []string{lp.levelField}, 0, func(field string, index int) {
		lp.levelField = field
	})

	// Window size input - use CLI param if available
	form.AddInputField("Window Size (rows)", fmt.Sprint(lp.windowSize), 10,
		func(text string, lastRune rune) bool { return unicode.IsDigit(lastRune) },
		func(text string) { lp.windowSize, _ = strconv.Atoi(text) })

	// Buttons
	form.AddButton("Explore Logs", func() { lp.showLogExplorer() })
	form.AddButton("Cancel", func() { lp.app.SwitchToMainPage("Returned from :logs") })

	return form
}

func (lp *LogPanel) updateTableDropdown(form *tview.Form) {
	if lp.database == "" {
		return
	}

	// Query ClickHouse for tables in selected database
	query := fmt.Sprintf("SHOW TABLES FROM `%s`", lp.database)
	rows, err := lp.app.clickHouse.Query(query)
	if err != nil {
		lp.app.SwitchToMainPage(fmt.Sprintf("Error getting tables: %v", err))
		return
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msgf("can't close updateTableDropdown rows")
		}
	}()

	lp.tables = []string{""}
	for rows.Next() {
		var table string
		if scanErr := rows.Scan(&table); scanErr != nil {
			log.Error().Err(scanErr).Msg("can't scan tables in updateTableDropdown")
			continue
		}
		lp.tables = append(lp.tables, table)
	}

	// Update the table dropdown if exists
	if tableItem := form.GetFormItemByLabel("Table"); tableItem != nil {
		if tableDropdown, ok := tableItem.(*tview.DropDown); ok {
			tableDropdown.SetOptions(lp.tables, func(table string, index int) {
				lp.table = table
				lp.updateFieldDropdowns(form)
			})
		}
	}
}

func (lp *LogPanel) updateFieldDropdowns(form *tview.Form) {
	if lp.database == "" || lp.table == "" {
		return
	}

	// Query ClickHouse for columns in selected table
	query := fmt.Sprintf("SELECT name,type FROM system.columns WHERE database='%s' AND table='%s'", lp.database, lp.table)
	rows, err := lp.app.clickHouse.Query(query)
	if err != nil {
		lp.app.SwitchToMainPage(fmt.Sprintf("Error getting columns: %v", err))
		return
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msgf("can't close updateFieldDropdown rows")
		}
	}()

	var columns, timeMsColumns, timeColumns, dateColumns []string
	lp.allFields = []string{} // Reset stored fields
	for rows.Next() {
		var fieldName, fieldType string
		if scanErr := rows.Scan(&fieldName, &fieldType); scanErr != nil {
			log.Error().Err(scanErr).Msg("can't scan columns in updateFieldDropdowns")
			continue
		}
		// Store all field names
		lp.allFields = append(lp.allFields, fieldName)

		if !strings.Contains(fieldType, "Date") && !strings.Contains(fieldType, "Array") && !strings.Contains(fieldType, "Tuple") && !strings.Contains(fieldType, "Map") {
			columns = append(columns, fieldName)
		}
		if fieldType == "Date" || fieldType == "Date32" || strings.HasPrefix(fieldType, "Date(") || strings.HasPrefix(fieldType, "Date32(") || strings.HasPrefix(fieldType, "Nullable(Date)") || strings.HasPrefix(fieldType, "Nullable(Date(") || strings.HasPrefix(fieldType, "Nullable(Date32") {
			dateColumns = append(dateColumns, fieldName)
		}
		if fieldType == "DateTime" || fieldType == "Nullable(DateTime)" || strings.HasPrefix(fieldType, "DateTime(") || strings.HasPrefix(fieldType, "Nullable(DateTime(") {
			timeColumns = append(timeColumns, fieldName)
		}
		if fieldType == "DateTime64" || strings.HasPrefix(fieldType, "DateTime64(") || strings.HasPrefix(fieldType, "Nullable(DateTime64") {
			timeMsColumns = append(timeMsColumns, fieldName)
		}
	}

	lp.updateDropdownOptions(form, "Message Field", columns, func(field string, index int) {
		lp.messageField = field
	})
	lp.updateDropdownOptions(form, "Time Field", timeColumns, func(field string, index int) {
		lp.timeField = field
	})
	lp.updateDropdownOptions(form, "TimeMs Field (optional)", timeMsColumns, func(field string, index int) {
		lp.timeMsField = field
	})
	lp.updateDropdownOptions(form, "Date Field (optional)", dateColumns, func(field string, index int) {
		lp.dateField = field
	})
	lp.updateDropdownOptions(form, "Level Field (optional)", columns, func(field string, index int) {
		lp.levelField = field
	})
}

// updateDropdownOptions updates a dropdown's options and selects the current value if set
func (lp *LogPanel) updateDropdownOptions(form *tview.Form, label string, options []string, selectedFunc func(option string, optionIndex int)) {
	for i := 0; i < form.GetFormItemCount(); i++ {
		item := form.GetFormItem(i)
		if dropdown, ok := item.(*tview.DropDown); ok && dropdown.GetLabel() == label+": " {
			dropdown.SetOptions(options, selectedFunc)

			// Get current value for this field type
			var currentValue string
			switch label {
			case "Message Field":
				currentValue = lp.messageField
			case "Time Field":
				currentValue = lp.timeField
			case "TimeMs Field (optional)":
				currentValue = lp.timeMsField
			case "Date Field (optional)":
				currentValue = lp.dateField
			case "Level Field (optional)":
				currentValue = lp.levelField
			}

			// Select matching option if current value exists
			for idx, opt := range options {
				if opt == currentValue {
					dropdown.SetCurrentOption(idx)
					break
				}
			}
			break
		}
	}
}

func (lp *LogPanel) showLogExplorer() {
	// Create main layout with 3 panels
	// Store mainFlex in LogPanel struct for later access (e.g., resizing children)
	lp.mainFlex = tview.NewFlex().SetDirection(tview.FlexRow)

	// 1. AdHoc Filter Panel (1 line height)
	lp.filterPanel = tview.NewFlex().SetDirection(tview.FlexRow)
	lp.filterPanel.SetBorder(true).SetTitle("Filters").SetTitleAlign(tview.AlignLeft)

	// Filter input components
	filterField := tview.NewDropDown().
		SetLabel("Field: ").
		SetOptions(lp.getAvailableFilterFields(), nil)
	filterOp := tview.NewDropDown().
		SetLabel("Operator: ").
		SetOptions([]string{"=", "!=", ">", "<", ">=", "<=", "LIKE", "NOT LIKE"}, nil)
	filterValue := tview.NewInputField().
		SetLabel("Value: ")

	addFilterBtn := tview.NewButton("Add Filter").
		SetSelectedFunc(func() {
			_, field := filterField.GetCurrentOption()
			_, op := filterOp.GetCurrentOption()
			value := filterValue.GetText()

			if field != "" && op != "" && value != "" {
				lp.filters = append(lp.filters, LogFilter{
					Field:    field,
					Operator: op,
					Value:    value,
				})
				lp.updateFilterDisplay(lp.filterPanel)
				go lp.loadLogs()
			}
		})

	filterFlex := tview.NewFlex().
		AddItem(filterField, 0, 1, false).
		AddItem(filterOp, 0, 1, false).
		AddItem(filterValue, 0, 1, false).
		AddItem(addFilterBtn, 10, 1, false)

	// Ensure filterFlex (input row) is 1 row high, and does not take proportional space.
	lp.filterPanel.AddItem(filterFlex, 1, 0, false)

	// Add filterPanel to mainFlex. Height = 1 (input row) + num_filters + 2 (panel border).
	// Proportion is 1, consistent with other elements.
	initialFilterPanelHeight := 1 + len(lp.filters) + 2
	lp.mainFlex.AddItem(lp.filterPanel, initialFilterPanelHeight, 1, false)

	// 2. Overview Panel (20% height)
	lp.overview = tview.NewTextView().SetDynamicColors(true)
	lp.overview.SetBorder(true).SetTitle("Overview").SetTitleAlign(tview.AlignLeft)
	lp.mainFlex.AddItem(lp.overview, 3, 1, false)

	// 3. Log Details Panel (60% height)
	lp.logDetails = tview.NewTable().
		SetBorders(false).
		SetSelectable(true, false).
		SetFixed(1, 0)
	lp.logDetails.SetBorder(true).SetTitleAlign(tview.AlignLeft).
		SetTitle(fmt.Sprintf("Log Entries [yellow](Ctrl+PageUp/Ctlr+PageDown to load more)[-] | From: %s To: %s",
			lp.firstEntryTime.Format("2006-01-02 15:04:05.000 MST"),
			lp.lastEntryTime.Format("2006-01-02 15:04:05.000 MST")))

	// Add column headers
	lp.logDetails.SetCell(0, 0, tview.NewTableCell("Time").SetTextColor(tcell.ColorYellow))
	lp.logDetails.SetCell(0, 1, tview.NewTableCell("Message").SetTextColor(tcell.ColorYellow))

	// Handle keyboard navigation
	lp.logDetails.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		rowNumber, _ := lp.logDetails.GetSelection()
		if event.Key() == tcell.KeyEnter {
			if rowNumber > 0 && rowNumber <= lp.totalRows {
				lp.showLogDetailsModal(rowNumber)
			}
		} else if event.Key() == tcell.KeyPgUp && event.Modifiers()&tcell.ModCtrl != 0 {
			go lp.loadMoreLogs(false) // Load older logs
		} else if event.Key() == tcell.KeyPgDn && event.Modifiers()&tcell.ModCtrl != 0 {
			go lp.loadMoreLogs(true) // Load newer logs
		}
		return event
	})

	lp.mainFlex.AddItem(lp.logDetails, 0, 1, false)

	// Set up tab navigation between all components
	lp.setupTabNavigation(filterField, filterOp, filterValue, addFilterBtn)

	lp.app.pages.AddPage("logExplorer", lp.mainFlex, true, true)
	lp.app.pages.SwitchToPage("logExplorer")

	// Execute initial query
	go lp.loadLogs()

}

func (lp *LogPanel) loadLogs() {
	if lp.database == "" || lp.table == "" || lp.messageField == "" || lp.timeField == "" {
		return
	}

	lp.overview.SetText(fmt.Sprintf("Loading %d log rows from `%s`.`%s`...", lp.windowSize, lp.database, lp.table))

	// Build WHERE clause with filters
	timeCondition := fmt.Sprintf("%s >= ?", lp.timeField)
	whereClause, queryArgs := lp.buildWhereClause(timeCondition, []interface{}{lp.app.fromTime})

	// Build query
	logsQuery := lp.buildQuery(whereClause, lp.timeField)
	queryArgs = append(queryArgs, lp.windowSize)

	rows, err := lp.app.clickHouse.Query(logsQuery, queryArgs...)
	if err != nil {
		lp.app.SwitchToMainPage(fmt.Sprintf("loadLogs Query failed: %v", err))
		return
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msgf("can't close loadLogs rows")
		}
	}()

	// Stream directly to table while storing full entry data
	lp.streamRowsToTable(rows, true)
}

func (lp *LogPanel) getAvailableFilterFields() []string {
	// Return stored fields if we have them
	if len(lp.allFields) > 0 {
		return lp.allFields
	}
	// Fallback to basic fields if no fields stored yet
	return []string{lp.messageField, lp.timeField}
}

func (lp *LogPanel) updateFilterDisplay(panel *tview.Flex) {
	// Clear existing filter displays (buttons), keeping the first item (filterFlex)
	// The first item (index 0) is filterFlex, which should not be removed.
	for panel.GetItemCount() > 1 {
		panel.RemoveItem(panel.GetItem(1)) // Repeatedly remove the item at index 1
	}

	// Add current filters
	for _, filter := range lp.filters {
		filterText := fmt.Sprintf("%s %s %s", filter.Field, filter.Operator, filter.Value)
		// Capture the filter value to avoid closure issues
		currentFilter := filter
		filterBtn := tview.NewButton(filterText).
			SetSelectedFunc(func() {
				// Remove this specific filter by value comparison
				for i, f := range lp.filters {
					if f.Field == currentFilter.Field && f.Operator == currentFilter.Operator && f.Value == currentFilter.Value {
						lp.filters = append(lp.filters[:i], lp.filters[i+1:]...)
						break
					}
				}
				lp.updateFilterDisplay(panel)
				go lp.loadLogs()
			}).
			SetStyle(tcell.StyleDefault.Background(tcell.ColorDarkBlue)).
			SetActivatedStyle(tcell.StyleDefault.Background(tcell.ColorRed))

		// Add tab navigation for filter buttons
		filterBtn.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
			if event.Key() == tcell.KeyTab {
				lp.app.tviewApp.SetFocus(lp.logDetails)
				return nil
			} else if event.Key() == tcell.KeyBacktab {
				// Focus back to the filter input area
				if filterFlex := panel.GetItem(0); filterFlex != nil {
					if flex, ok := filterFlex.(*tview.Flex); ok && flex.GetItemCount() > 0 {
						lp.app.tviewApp.SetFocus(flex.GetItem(0))
					}
				}
				return nil
			}
			return event
		})

		// Ensure filterBtn is 1 row high, and does not take proportional space.
		panel.AddItem(filterBtn, 1, 0, false)
		lp.app.tviewApp.SetFocus(filterBtn)
	}

	// Dynamically adjust the height of the filterPanel in mainFlex
	// Height = 1 (for filterFlex row) + number of filters + 2 (for filterPanel's border)
	newHeight := 1 + len(lp.filters) + 2
	if lp.mainFlex != nil && lp.filterPanel != nil {
		// Use proportion 1, consistent with how it was added initially and with other items.
		lp.mainFlex.ResizeItem(lp.filterPanel, newHeight, 1)
	}
}

func (lp *LogPanel) updateOverviewWithStats(levelCounts map[string]int, totalItems int) {
	if totalItems == 0 {
		lp.overview.SetText("No log entries to display")
		return
	}

	if lp.levelField == "" {
		lp.overview.SetText(fmt.Sprintf("Total log entries: %d (no level field selected for breakdown)", totalItems))
		return
	}

	// Get all actual levels found in data, sorted by count (descending)
	type levelCount struct {
		level string
		count int
	}
	var sortedLevels []levelCount
	for level, count := range levelCounts {
		sortedLevels = append(sortedLevels, levelCount{level, count})
	}

	// Sort by count descending
	for i := 0; i < len(sortedLevels)-1; i++ {
		for j := i + 1; j < len(sortedLevels); j++ {
			if sortedLevels[j].count > sortedLevels[i].count {
				sortedLevels[i], sortedLevels[j] = sortedLevels[j], sortedLevels[i]
			}
		}
	}

	// Get available width from the overview TextView
	_, _, viewWidth, _ := lp.overview.GetInnerRect()
	if viewWidth <= 0 {
		viewWidth = 80 // fallback width
	}

	// Reserve space for "Total: XXXX | " prefix and some padding
	prefixText := fmt.Sprintf("Total: %d | ", totalItems)
	availableWidth := viewWidth - len(prefixText) - 5 // 5 chars padding

	if availableWidth < 20 {
		availableWidth = 20 // minimum bar width
	}

	// Build the bar with embedded legend
	var builder strings.Builder
	builder.WriteString(prefixText)

	// Create segments with embedded labels
	currentPos := 0
	for _, lc := range sortedLevels {
		if lc.count == 0 {
			continue
		}

		proportion := float64(lc.count) / float64(totalItems)
		segmentWidth := int(proportion * float64(availableWidth))
		if segmentWidth == 0 && lc.count > 0 {
			segmentWidth = 1 // Ensure at least 1 character for non-zero counts
		}

		var bgColor string
		switch lc.level {
		case "error", "exception", "fatal", "critical":
			bgColor = "red"
		case "warning", "warn", "debug", "trace":
			bgColor = "yellow"
		case "info", "information":
			bgColor = "green"
		case "unknown":
			bgColor = "gray"
		default:
			bgColor = "cyan" // For any other levels
		}

		// Create label text for this segment
		labelText := fmt.Sprintf("%s:%d", lc.level, lc.count)

		// If segment is wide enough to fit the label, embed it
		if segmentWidth >= len(labelText) {
			// Calculate padding to center the label
			padding := (segmentWidth - len(labelText)) / 2
			leftPad := strings.Repeat(" ", padding)
			rightPad := strings.Repeat(" ", segmentWidth-padding-len(labelText))
			builder.WriteString(fmt.Sprintf("[black:%s]%s%s%s[-]", bgColor, leftPad, labelText, rightPad))
		} else {
			// Segment too small for label, just fill with blocks
			builder.WriteString(fmt.Sprintf("[black:%s]%s[-]", bgColor, strings.Repeat(" ", segmentWidth)))
		}

		currentPos += segmentWidth
	}

	// Fill remaining space if any
	if currentPos < availableWidth {
		builder.WriteString(strings.Repeat(" ", availableWidth-currentPos))
	}

	lp.overview.SetText(builder.String())
}

func (lp *LogPanel) loadMoreLogs(newer bool) {
	if lp.totalRows == 0 {
		lp.app.tviewApp.QueueUpdateDraw(func() {
			lp.overview.SetText("No logs loaded yet")
		})
		return
	}
	lp.app.tviewApp.QueueUpdateDraw(func() {
		lp.overview.SetText(fmt.Sprintf(ternary(newer, "Loading next %d rows...", "Loading previous %d rows..."), lp.windowSize))
	})

	var timeConditionStr, whereClause string
	var queryArgs []interface{}

	if !newer {
		// Use window function to find the exact timestamp for the previous batch
		timeQuery := fmt.Sprintf(`
			SELECT timestamp FROM (
				SELECT
					%s,
					FIRST_VALUE(%s) OVER (ORDER BY %s ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS timestamp
				FROM `+"`%s`.`%s`"+`
				ORDER BY %s
			) WHERE %s = ?`,
			lp.timeField, lp.timeField, lp.timeField, lp.windowSize,
			lp.database, lp.table,
			lp.timeField,
			lp.timeField)

		var prevBatchTime time.Time
		err := lp.app.clickHouse.QueryRow(timeQuery, lp.firstEntryTime).Scan(&prevBatchTime)
		if err != nil {
			lp.app.tviewApp.QueueUpdateDraw(func() {
				lp.overview.SetText(fmt.Sprintf("Error finding previous batch time: %v", err))
			})
			return
		}

		timeConditionStr = fmt.Sprintf("%s BETWEEN ? AND ?", lp.timeField)
		builtWhereClause, args := lp.buildWhereClause(timeConditionStr, []interface{}{prevBatchTime, lp.firstEntryTime})
		queryArgs = args
		whereClause = builtWhereClause
	} else {
		timeConditionStr = fmt.Sprintf("%s BETWEEN ? AND ?", lp.timeField)
		builtWhereClause, args := lp.buildWhereClause(timeConditionStr, []interface{}{lp.lastEntryTime, lp.app.toTime})
		queryArgs = args
		whereClause = builtWhereClause
	}

	// Build query with appropriate time range
	query := lp.buildQuery(whereClause, lp.timeField)
	queryArgs = append(queryArgs, lp.windowSize)

	rows, err := lp.app.clickHouse.Query(query, queryArgs...)
	if err != nil {
		lp.app.tviewApp.QueueUpdateDraw(func() {
			lp.overview.SetText(fmt.Sprintf("Error loading more logs: %v", err))
		})
		return
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msgf("can't close loadMoreLogs rows")
		}
	}()

	// Stream additional rows (don't clear table)
	lp.streamRowsToTable(rows, false)
}

func (lp *LogPanel) showLogDetailsModal(rowIndex int) {
	if rowIndex <= 0 || rowIndex > lp.totalRows {
		return
	}

	// Get the stored LogEntry from the row's reference
	cell := lp.logDetails.GetCell(rowIndex, 0)
	if cell == nil || cell.Reference == nil {
		return
	}

	if entry, ok := cell.Reference.(LogEntry); ok {
		lp.showLogDetailsModalWithEntry(entry)
	}
}

func (lp *LogPanel) showLogDetailsModalWithEntry(entry LogEntry) {
	// Create a flex layout for the details window
	detailsFlex := tview.NewFlex().SetDirection(tview.FlexRow)

	// Header section with time and level info
	headerText := tview.NewTextView().
		SetDynamicColors(true).
		SetWordWrap(true)
	headerText.SetBorder(true).SetTitle("Log Entry Info")

	// Build header content with standard fields
	var headerBuilder strings.Builder
	if !entry.Time.IsZero() {
		headerBuilder.WriteString(fmt.Sprintf("[yellow]%s:[-] %s\n", lp.timeField, entry.Time.Format("2006-01-02 15:04:05 MST")))
	}
	if !entry.TimeMs.IsZero() {
		headerBuilder.WriteString(fmt.Sprintf("[yellow]%s:[-] %s\n", lp.timeMsField, entry.TimeMs))
	}
	if entry.Date != "" {
		headerBuilder.WriteString(fmt.Sprintf("[yellow]%s:[-] %s\n", lp.dateField, entry.Date))
	}
	if entry.Level != "" {
		levelColor := "[white]"
		switch strings.ToLower(entry.Level) {
		case "error", "exception":
			levelColor = "[red]"
		case "warning", "debug", "trace":
			levelColor = "[yellow]"
		case "info":
			levelColor = "[green]"
		}
		headerBuilder.WriteString(fmt.Sprintf("[yellow]%s:[-] %s%s[-]\n", lp.levelField, levelColor, entry.Level))
	}
	headerText.SetText(headerBuilder.String())

	// Create a form for additional fields
	fieldsForm := tview.NewForm()
	fieldsForm.SetBorder(true).
		SetTitle("Additional Fields (press Enter to filter)")
	formPrimitive := fieldsForm // Store as primitive for navigation

	// Add all additional fields as buttons
	if len(entry.AllFields) > 0 {
		// Sort field names for consistent display
		fields := make([]string, 0, len(entry.AllFields))
		for field := range entry.AllFields {
			fields = append(fields, field)
		}
		sort.Strings(fields)

		for _, field := range fields {
			value := entry.AllFields[field]
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

			// Capture current field and value for the closure
			fieldName := field
			fieldValue := valueStr

			fieldsForm.AddButton(fmt.Sprintf("[yellow]%s:[-] %s", fieldName, fieldValue), func() {
				// Add this field/value pair as a filter
				lp.filters = append(lp.filters, LogFilter{
					Field:    fieldName,
					Operator: "=",
					Value:    fieldValue,
				})
				lp.updateFilterDisplay(lp.filterPanel)
				lp.app.pages.RemovePage("logDetails")
				go lp.loadLogs()
			})
		}
	}

	// Message section with scrolling
	messageText := tview.NewTextView().
		SetDynamicColors(true).
		SetWordWrap(true).
		SetScrollable(true)
	messageText.SetBorder(true).SetTitle(fmt.Sprintf("Message (%s)", lp.messageField))
	messageText.SetText(entry.Message)

	// Instructions
	instructionsText := tview.NewTextView().
		SetDynamicColors(true).
		SetText("[yellow]Navigation:[-] Tab/Shift+Tab to move, Enter to filter, Esc to close")
	instructionsText.SetTextAlign(tview.AlignCenter)

	// Add components to flex layout
	detailsFlex.AddItem(headerText, 0, 1, false)       // Header takes minimum space needed
	detailsFlex.AddItem(fieldsForm, 0, 1, false)       // Fields form
	detailsFlex.AddItem(messageText, 0, 3, true)       // Message takes most space
	detailsFlex.AddItem(instructionsText, 1, 0, false) // Instructions take 1 line

	// Setup tab navigation between form and message

	// Setup tab navigation between form and message
	formPrimitive.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyTab {
			lp.app.tviewApp.SetFocus(messageText)
			return nil
		} else if event.Key() == tcell.KeyEscape {
			lp.app.pages.RemovePage("logDetails")
			return nil
		}
		return event
	})

	messageText.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyTab || event.Key() == tcell.KeyBacktab {
			lp.app.tviewApp.SetFocus(formPrimitive)
			return nil
		} else if event.Key() == tcell.KeyEscape {
			lp.app.pages.RemovePage("logDetails")
			return nil
		}
		return event
	})

	// Set initial focus to fields form
	lp.app.tviewApp.SetFocus(fieldsForm)

	lp.app.pages.AddPage("logDetails", detailsFlex, true, true)
	lp.app.pages.SwitchToPage("logDetails")
}

func (lp *LogPanel) getSelectedFields() []string {
	fields := []string{lp.messageField, lp.timeField}
	if lp.timeMsField != "" {
		fields = append(fields, lp.timeMsField)
	}
	if lp.dateField != "" {
		fields = append(fields, lp.dateField)
	}
	if lp.levelField != "" {
		fields = append(fields, lp.levelField)
	}
	return fields
}

func (lp *LogPanel) buildWhereClause(timeCondition string, args []interface{}) (string, []interface{}) {
	whereConditions := make([]string, 0)
	queryArgs := args

	// Optimize time filtering by using dateField if available
	if len(args) > 0 && lp.dateField != "" {
		if len(args) >= 1 {
			if t, ok := args[0].(time.Time); ok {
				whereConditions = append(whereConditions, fmt.Sprintf("%s >= '%s'", lp.dateField, t.Format("2006-01-02")))
			}
		}
		if len(args) >= 2 {
			if t, ok := args[1].(time.Time); ok {
				whereConditions = append(whereConditions, fmt.Sprintf("%s <= '%s'", lp.dateField, t.Format("2006-01-02")))
			}
		}
	}

	// Add the main time condition
	whereConditions = append(whereConditions, timeCondition)

	// Add filter conditions
	for _, filter := range lp.filters {
		whereConditions = append(whereConditions, fmt.Sprintf("%s %s ?", filter.Field, filter.Operator))
		queryArgs = append(queryArgs, filter.Value)
	}

	return strings.Join(whereConditions, " AND "), queryArgs
}

func (lp *LogPanel) buildQuery(whereClause, orderBy string) string {
	return fmt.Sprintf(`
		SELECT *
		FROM `+"`%s`.`%s`"+`
		WHERE %s
		ORDER BY %s
		LIMIT ?`,
		lp.database,
		lp.table,
		whereClause,
		orderBy)
}

func (lp *LogPanel) streamRowsToTable(rows *sql.Rows, clearFirst bool) {
	// Calculate dynamic batch size based on windowSize
	batchSize := lp.windowSize / 10
	if batchSize < 100 {
		batchSize = 100 // Minimum batch size
	}

	lp.app.tviewApp.QueueUpdateDraw(func() {
		if clearFirst {
			lp.logDetails.Clear()
			// Re-add headers
			lp.logDetails.SetCell(0, 0, tview.NewTableCell("Time").SetTextColor(tcell.ColorYellow))
			lp.logDetails.SetCell(0, 1, tview.NewTableCell("Message").SetTextColor(tcell.ColorYellow))
			lp.totalRows = 0
		}
	})

	colTypes, _ := rows.ColumnTypes()
	scanArgs := make([]interface{}, len(colTypes))

	// For overview statistics
	levelCounts := make(map[string]int)
	rowIndex := lp.totalRows
	var batch []LogEntry

	for rows.Next() {
		var entry LogEntry

		// Initialize scan args and field storage
		entry.AllFields = make(map[string]interface{})
		fieldValues := make([]interface{}, len(colTypes))
		for i, col := range colTypes {
			fieldName := col.Name()
			switch fieldName {
			case lp.timeField:
				scanArgs[i] = &entry.Time
			case lp.timeMsField:
				scanArgs[i] = &entry.TimeMs
			case lp.dateField:
				scanArgs[i] = &entry.Date
			case lp.messageField:
				scanArgs[i] = &entry.Message
			case lp.levelField:
				scanArgs[i] = &entry.Level
			default:
				// Store all other fields in AllFields map
				var val interface{}
				fieldValues[i] = &val
				scanArgs[i] = fieldValues[i]
			}
		}

		if err := rows.Scan(scanArgs...); err != nil {
			log.Error().Err(err).Send()
			continue
		}

		// Store all additional fields
		for i, col := range colTypes {
			fieldName := col.Name()
			switch fieldName {
			case lp.timeField, lp.timeMsField, lp.dateField, lp.messageField, lp.levelField:
				// Skip fields we already handle specially
			default:
				if fieldValues[i] != nil {
					val := *fieldValues[i].(*interface{})
					entry.AllFields[fieldName] = val
				}
			}
		}

		// Track time bounds for pagination
		if rowIndex == 0 || (!entry.Time.IsZero() && entry.Time.Before(lp.firstEntryTime)) {
			lp.firstEntryTime = entry.Time
		}
		if rowIndex == 0 || (!entry.Time.IsZero() && entry.Time.After(lp.lastEntryTime)) {
			lp.lastEntryTime = entry.Time
		}

		// Update level counts for overview
		if lp.levelField != "" {
			if entry.Level != "" {
				levelCounts[strings.ToLower(entry.Level)]++
			} else {
				levelCounts["unknown"]++
			}
		}

		// Add to batch
		batch = append(batch, entry)
		rowIndex++

		// Process batch when full
		if len(batch) >= batchSize {
			lp.processBatch(batch, rowIndex-len(batch))
			batch = batch[:0] // Clear batch while keeping capacity

			// Update title with current time range
			lp.app.tviewApp.QueueUpdateDraw(func() {
				lp.logDetails.SetTitle(fmt.Sprintf("Log Entries [yellow](Ctrl+PageUp/Ctlr+PageDown to load more)[-] | From: %s To: %s",
					lp.firstEntryTime.Format("2006-01-02 15:04:05.000 MST"),
					lp.lastEntryTime.Format("2006-01-02 15:04:05.000 MST")))
			})
		}
	}

	// Process any remaining entries in the batch
	if len(batch) > 0 {
		lp.processBatch(batch, rowIndex-len(batch))

		// Update title with final time range
		lp.app.tviewApp.QueueUpdateDraw(func() {
			lp.logDetails.SetTitle(fmt.Sprintf("Log Entries [yellow](Ctrl+PageUp/Ctlr+PageDown to load more)[-] | From: %s To: %s",
				lp.firstEntryTime.Format("2006-01-02 15:04:05.000 MST"),
				lp.lastEntryTime.Format("2006-01-02 15:04:05.000 MST")))
		})
	}

	lp.totalRows = rowIndex

	// Update overview with collected statistics
	lp.app.tviewApp.QueueUpdateDraw(func() {
		lp.updateOverviewWithStats(levelCounts, lp.totalRows)
		// Set focus to logDetails table after logs are loaded
		lp.app.tviewApp.SetFocus(lp.logDetails)
	})
}

func (lp *LogPanel) processBatch(batch []LogEntry, startRow int) {
	lp.app.tviewApp.QueueUpdateDraw(func() {
		for i, entry := range batch {
			row := startRow + i + 1 // +1 for header row
			timeStr := lp.formatTimeForDisplay(entry)

			// Store full entry in first cell's reference
			timeCell := tview.NewTableCell(timeStr)
			timeCell.SetReference(entry)
			lp.logDetails.SetCell(row, 0, timeCell)

			messageCell := tview.NewTableCell(entry.Message)

			// Color by level
			if entry.Level != "" {
				messageCell.SetTextColor(lp.getColorForLevel(entry.Level))
			}
			lp.logDetails.SetCell(row, 1, messageCell)
		}
	})
}

func (lp *LogPanel) formatTimeForDisplay(entry LogEntry) string {
	if !entry.TimeMs.IsZero() {
		return entry.TimeMs.Format("2006-01-02 15:04:05.000 MST")
	} else if !entry.Time.IsZero() {
		return entry.Time.Format("2006-01-02 15:04:05 MST")
	} else if entry.Date != "" {
		return entry.Date
	}
	return ""
}

func (lp *LogPanel) getColorForLevel(level string) tcell.Color {
	switch strings.ToLower(level) {
	case "error", "exception":
		return tcell.ColorRed
	case "warning", "debug", "trace":
		return tcell.ColorYellow
	case "info":
		return tcell.ColorGreen
	default:
		return tcell.ColorWhite
	}
}

func (lp *LogPanel) setupTabNavigation(filterField *tview.DropDown, filterOp *tview.DropDown, filterValue *tview.InputField, addFilterBtn *tview.Button) {
	// Create a list of all focusableItems components in order
	focusableItems := []tview.Primitive{
		filterField,
		filterOp,
		filterValue,
		addFilterBtn,
		lp.logDetails,
	}

	// Helper function to create tab navigation handler
	createTabHandler := func(currentIndex int) func(event *tcell.EventKey) *tcell.EventKey {
		return func(event *tcell.EventKey) *tcell.EventKey {
			if event.Key() == tcell.KeyTab {
				// Move to next component
				nextIndex := (currentIndex + 1) % len(focusableItems)
				lp.app.tviewApp.SetFocus(focusableItems[nextIndex])
				return nil
			} else if event.Key() == tcell.KeyBacktab {
				// Move to previous component
				prevIndex := (currentIndex - 1 + len(focusableItems)) % len(focusableItems)
				lp.app.tviewApp.SetFocus(focusableItems[prevIndex])
				return nil
			}
			return event
		}
	}

	// Set up tab navigation for each component type
	filterField.SetInputCapture(createTabHandler(0))
	filterOp.SetInputCapture(createTabHandler(1))
	filterValue.SetInputCapture(createTabHandler(2))
	addFilterBtn.SetInputCapture(createTabHandler(3))

	// For logDetails, we need to preserve existing input capture and add tab navigation
	existingHandler := lp.logDetails.GetInputCapture()
	lp.logDetails.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		// Handle tab navigation first
		if event.Key() == tcell.KeyTab {
			lp.app.tviewApp.SetFocus(focusableItems[0]) // Go back to first component
			return nil
		} else if event.Key() == tcell.KeyBacktab {
			// If we have filters, focus the last one added
			if len(lp.filters) > 0 {
				if filterFlex := lp.filterPanel.GetItem(0); filterFlex != nil {
					if _, ok := filterFlex.(*tview.Flex); ok {
						// The last filter button is at index len(lp.filters) in filterPanel
						lastFilterIndex := len(lp.filters)
						if lastFilterIndex < lp.filterPanel.GetItemCount() {
							lp.app.tviewApp.SetFocus(lp.filterPanel.GetItem(lastFilterIndex))
						}
					}
				}
			} else {
				// No filters, focus the filter field
				lp.app.tviewApp.SetFocus(filterField)
			}
			return nil
		}
		// Pass to existing handler if not tab navigation
		if existingHandler != nil {
			return existingHandler(event)
		}
		return event
	})

	// Set initial focus to the first filter field
	lp.app.tviewApp.SetFocus(filterField)
}

func ternary(condition bool, trueVal, falseVal string) string {
	if condition {
		return trueVal
	}
	return falseVal
}
