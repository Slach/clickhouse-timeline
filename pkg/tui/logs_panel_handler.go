package tui

import (
	"database/sql"
	"fmt"
	"github.com/gdamore/tcell/v2"
	"github.com/rs/zerolog/log"
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
	databases      []string
	tables         []string
}

type LogFilter struct {
	Field    string
	Operator string
	Value    string
}

type LogEntry struct {
	Time    time.Time
	TimeMs  time.Time
	Date    string
	Level   string
	Message string
}

func (a *App) ShowLogsPanel() {
	if a.clickHouse == nil {
		a.SwitchToMainPage("Error: Please connect to ClickHouse first")
		return
	}
	lp := &LogPanel{
		app:        a,
		windowSize: 1000,
	}

	// Query ClickHouse for available databases
	rows, err := a.clickHouse.Query("SELECT name FROM system.databases")
	if err != nil {
		a.SwitchToMainPage(fmt.Sprintf("Error getting databases: %v", err))
		return
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msgf("can't close databases query rows")
		}
	}()

	lp.databases = []string{""}
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

	a.pages.AddPage("logs", logsFlex, true, true)
	a.pages.SwitchToPage("logs")
}

func (lp *LogPanel) createForm() *tview.Form {
	form := tview.NewForm()
	form.SetBorder(true).SetTitle("Log Explorer")

	// Database dropdown
	form.AddDropDown("Database", lp.databases, 0, func(db string, index int) {
		lp.database = db
		lp.updateTableDropdown(form)
	})

	// Table dropdown
	form.AddDropDown("Table", []string{}, 0, func(table string, index int) {
		lp.table = table
		lp.updateFieldDropdowns(form)
	})

	// Field dropdowns
	form.AddDropDown("Message Field", []string{}, 0, func(field string, index int) {
		lp.messageField = field
	})
	form.AddDropDown("Time Field", []string{}, 0, func(field string, index int) {
		lp.timeField = field
	})
	form.AddDropDown("TimeMs Field (optional)", []string{}, 0, func(field string, index int) {
		lp.timeMsField = field
	})
	form.AddDropDown("Date Field (optional)", []string{}, 0, func(field string, index int) {
		lp.dateField = field
	})
	form.AddDropDown("Level Field (optional)", []string{}, 0, func(field string, index int) {
		lp.levelField = field
	})

	// Window size input
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

	// Update the table dropdown
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
	for rows.Next() {
		var fieldName, fieldType string
		if scanErr := rows.Scan(&fieldName, &fieldType); scanErr != nil {
			log.Error().Err(scanErr).Msg("can't scan columns in updateFieldDropdowns")
			continue
		}
		if !strings.Contains(fieldType, "Date") && !strings.Contains(fieldType, "Array") && !strings.Contains(fieldType, "Tuple") && !strings.Contains(fieldType, "Map") {
			columns = append(columns, fieldName)
		}
		if fieldType == "Date" || fieldType == "Date32" || strings.HasPrefix(fieldType, "Date(") || strings.HasPrefix(fieldType, "Date32(") || strings.HasPrefix(fieldType, "Nullable(Date)") || strings.HasPrefix(fieldType, "Nullable(Date(") || strings.HasPrefix(fieldType, "Nullable(Date32") {
			dateColumns = append(dateColumns, fieldName)
		}
		if fieldType == "DateTime" || fieldType == "Nullable(DateTime)" || strings.HasPrefix(fieldType, "DateTime(") || strings.HasPrefix(fieldType, "Nullable(DateTime(") {
			timeColumns = append(timeColumns, fieldName)
		}
		if fieldType == "DateTime64" || strings.HasPrefix(fieldType, "DateTime64(") {
			timeMsColumns = append(timeMsColumns, fieldName)
		}
	}

	// Update field dropdowns
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

func (lp *LogPanel) updateDropdownOptions(form *tview.Form, label string, options []string, selectedFunc func(option string, optionIndex int)) {
	if item := form.GetFormItemByLabel(label); item != nil {
		if dropdown, ok := item.(*tview.DropDown); ok {
			dropdown.SetOptions(options, selectedFunc)
		}
	}
}

func (lp *LogPanel) showLogExplorer() {
	// Create main layout with 3 panels
	mainFlex := tview.NewFlex().SetDirection(tview.FlexRow)

	// 1. AdHoc Filter Panel (20% height)
	filterPanel := tview.NewFlex().SetDirection(tview.FlexRow)
	filterPanel.SetBorder(true).SetTitle("Filters")

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
				lp.updateFilterDisplay(filterPanel)
				go lp.loadLogs()
			}
		})

	filterFlex := tview.NewFlex().
		AddItem(filterField, 0, 1, false).
		AddItem(filterOp, 0, 1, false).
		AddItem(filterValue, 0, 1, false).
		AddItem(addFilterBtn, 10, 1, false)

	filterPanel.AddItem(filterFlex, 1, 1, false)
	lp.updateFilterDisplay(filterPanel)
	mainFlex.AddItem(filterPanel, 3, 1, false)

	// 2. Overview Panel (20% height)
	lp.overview = tview.NewTextView().SetDynamicColors(true)
	lp.overview.SetBorder(true).SetTitle("Overview")
	mainFlex.AddItem(lp.overview, 3, 1, false)

	// 3. Log Details Panel (60% height)
	lp.logDetails = tview.NewTable().
		SetBorders(false).
		SetSelectable(true, false).
		SetFixed(1, 0)
	lp.logDetails.SetBorder(true).SetTitle("Log Entries")

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

	mainFlex.AddItem(lp.logDetails, 0, 1, true)

	// Execute initial query
	go lp.loadLogs()

	lp.app.pages.AddPage("logExplorer", mainFlex, true, true)
	lp.app.pages.SwitchToPage("logExplorer")
}

func (lp *LogPanel) loadLogs() {
	if lp.database == "" || lp.table == "" || lp.messageField == "" || lp.timeField == "" {
		return
	}

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

	// Stream directly to table
	lp.streamRowsToTable(rows, true)
}

func (lp *LogPanel) getAvailableFilterFields() []string {
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

func (lp *LogPanel) updateFilterDisplay(panel *tview.Flex) {
	// Clear existing filter displays
	for i := 1; i < panel.GetItemCount(); i++ {
		panel.RemoveItem(panel.GetItem(i))
	}

	// Add current filters
	for _, filter := range lp.filters {
		filterText := fmt.Sprintf("%s %s %s", filter.Field, filter.Operator, filter.Value)
		filterBtn := tview.NewButton(filterText).
			SetSelectedFunc(func() {
				// Remove this filter
				for i, f := range lp.filters {
					if f == filter {
						lp.filters = append(lp.filters[:i], lp.filters[i+1:]...)
						break
					}
				}
				lp.updateFilterDisplay(panel)
				go lp.loadLogs()
			})
		panel.AddItem(filterBtn, len(filterText)+4, 1, false)
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

	// Build query with appropriate ordering
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

	// Get data from table cells
	timeCell := lp.logDetails.GetCell(rowIndex, 0)
	messageCell := lp.logDetails.GetCell(rowIndex, 1)

	if timeCell == nil || messageCell == nil {
		return
	}

	// Create entry from table data
	entry := LogEntry{
		Message: messageCell.Text,
	}

	lp.showLogDetailsModalWithEntry(entry)
}

func (lp *LogPanel) showLogDetailsModalWithEntry(entry LogEntry) {
	// Create a flex layout for the details window
	detailsFlex := tview.NewFlex().SetDirection(tview.FlexRow)

	// Header section with time and level info
	headerText := tview.NewTextView().
		SetDynamicColors(true).
		SetWordWrap(true)
	headerText.SetBorder(true).SetTitle("Log Entry Info")

	// Build header content
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
		SetText("[yellow]Navigation:[-] ↑/↓ or j/k to scroll, [yellow]Esc[-] to close")
	instructionsText.SetTextAlign(tview.AlignCenter)

	// Add components to flex layout
	detailsFlex.AddItem(headerText, 0, 1, false)       // Header takes minimum space needed
	detailsFlex.AddItem(messageText, 0, 3, true)       // Message takes most space and is focusable
	detailsFlex.AddItem(instructionsText, 1, 0, false) // Instructions take 1 line

	// Handle keyboard input
	detailsFlex.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape {
			lp.app.pages.RemovePage("logDetails")
			return nil
		}
		if event.Key() == tcell.KeyRune {
			switch event.Rune() {
			case 'q', 'Q':
				lp.app.pages.RemovePage("logDetails")
				return nil
			}
		}
		return event
	})

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
	whereConditions := []string{}
	queryArgs := args

	// Optimize time filtering by using dateField if available
	if len(args) > 0 && lp.dateField != "" {
		if len(args) >= 1 {
			dateStr := args[0].(time.Time).Format("2006-01-02")
			whereConditions = append(whereConditions, fmt.Sprintf("%s >= '%s'", lp.dateField, dateStr))
		}
		if len(args) >= 2 {
			dateStr := args[1].(time.Time).Format("2006-01-02")
			whereConditions = append(whereConditions, fmt.Sprintf("%s <= '%s'", lp.dateField, dateStr))
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
		SELECT %s
		FROM `+"`%s`.`%s`"+`
		WHERE %s
		ORDER BY %s
		LIMIT ?`,
		strings.Join(lp.getSelectedFields(), ", "),
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

		// Initialize scan args
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
				var dummy interface{}
				scanArgs[i] = &dummy
			}
		}

		if err := rows.Scan(scanArgs...); err != nil {
			log.Error().Err(err).Send()
			continue
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
		}
	}

	// Process any remaining entries in the batch
	if len(batch) > 0 {
		lp.processBatch(batch, rowIndex-len(batch))
	}

	lp.totalRows = rowIndex

	// Update overview with collected statistics
	lp.app.tviewApp.QueueUpdateDraw(func() {
		lp.updateOverviewWithStats(levelCounts, lp.totalRows)
	})
}

func (lp *LogPanel) processBatch(batch []LogEntry, startRow int) {
	lp.app.tviewApp.QueueUpdateDraw(func() {
		for i, entry := range batch {
			row := startRow + i + 1 // +1 for header row
			timeStr := lp.formatTimeForDisplay(entry)

			lp.logDetails.SetCell(row, 0, tview.NewTableCell(timeStr))
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

func ternary(condition bool, trueVal, falseVal string) string {
	if condition {
		return trueVal
	}
	return falseVal
}
