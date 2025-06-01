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
	currentResults []LogEntry
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
	TimeMs  int64
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
	lp.updateDropdownOptions(form, "Message Field", columns)
	lp.updateDropdownOptions(form, "Time Field", timeColumns)
	lp.updateDropdownOptions(form, "TimeMs Field (optional)", timeMsColumns)
	lp.updateDropdownOptions(form, "Date Field (optional)", dateColumns)
	lp.updateDropdownOptions(form, "Level Field (optional)", columns)
}

func (lp *LogPanel) updateDropdownOptions(form *tview.Form, label string, options []string) {
	if item := form.GetFormItemByLabel(label); item != nil {
		if dropdown, ok := item.(*tview.DropDown); ok {
			dropdown.SetOptions(options, nil)
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
			if rowNumber > 0 && rowNumber-1 < len(lp.currentResults) {
				lp.showLogDetailsModal(lp.currentResults[rowNumber-1])
			}
		} else if event.Key() == tcell.KeyPgDn {
			lp.loadMoreLogs(false) // Load older logs
		} else if event.Key() == tcell.KeyPgUp {
			lp.loadMoreLogs(true) // Load newer logs
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

	// Process results
	entries, err := lp.processRows(rows)
	if err != nil {
		lp.app.SwitchToMainPage(fmt.Sprintf("Error processing rows: %v", err))
		return
	}

	lp.currentResults = entries
	lp.updateLogTable(entries)
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

func (lp *LogPanel) updateOverview(view *tview.TextView) {
	totalItems := len(lp.currentResults)
	if totalItems == 0 {
		view.SetText("No log entries to display")
		return
	}

	if lp.levelField == "" {
		view.SetText(fmt.Sprintf("Total log entries: %d (no level field selected for breakdown)", totalItems))
		return
	}

	levelCounts := make(map[string]int)
	for _, entry := range lp.currentResults {
		if entry.Level != "" {
			levelCounts[strings.ToLower(entry.Level)]++
		} else {
			levelCounts["unknown"]++
		}
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

	// Get available width from the view
	_, _, viewWidth, _ := view.GetInnerRect()
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

	view.SetText(builder.String())
}

func (lp *LogPanel) loadMoreLogs(newer bool) {
	if len(lp.currentResults) == 0 {
		return
	}

	var timeCondition time.Time
	if newer {
		// For newer logs, use the earliest time in current results minus window size
		timeCondition = lp.currentResults[0].Time.Add(-time.Duration(lp.windowSize) * time.Millisecond)
	} else {
		// For older logs, use the latest time in current results
		timeCondition = lp.currentResults[len(lp.currentResults)-1].Time
	}

	// Build WHERE clause with filters
	timeConditionStr := fmt.Sprintf("%s BETWEEN ? AND ?", lp.timeField)
	whereClause, queryArgs := lp.buildWhereClause(timeConditionStr, []interface{}{timeCondition, lp.app.toTime})

	// Build query with appropriate ordering
	orderBy := fmt.Sprintf("%s %s", lp.timeField, ternary(newer, "ASC", "DESC"))
	query := lp.buildQuery(whereClause, orderBy)
	queryArgs = append(queryArgs, lp.windowSize)

	rows, err := lp.app.clickHouse.Query(query, queryArgs...)
	if err != nil {
		return
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msgf("can't close loadMoreLogs rows")
		}
	}()

	// Process results
	newEntries, err := lp.processRows(rows)
	if err != nil {
		return
	}

	// Merge results
	if newer {
		lp.currentResults = append(newEntries, lp.currentResults...)
	} else {
		lp.currentResults = append(lp.currentResults, newEntries...)
	}

	lp.updateLogTable(lp.currentResults)
}

func (lp *LogPanel) showLogDetailsModal(entry LogEntry) {
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
	if entry.TimeMs > 0 {
		headerBuilder.WriteString(fmt.Sprintf("[yellow]%s:[-] %d\n", lp.timeMsField, entry.TimeMs))
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
	whereConditions := []string{timeCondition}
	queryArgs := args

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

func (lp *LogPanel) processRows(rows *sql.Rows) ([]LogEntry, error) {
	var entries []LogEntry
	colTypes, _ := rows.ColumnTypes()
	scanArgs := make([]interface{}, len(colTypes))

	for rows.Next() {
		var entry LogEntry
		// Initialize scan args based on column types
		for i, col := range colTypes {
			fieldName := col.Name()
			fieldType := col.DatabaseTypeName()

			// Check if this is a time-related field
			if fieldName == lp.timeField && (fieldType == "DateTime" || fieldType == "Nullable(DateTime)" || strings.HasPrefix(fieldType, "DateTime(") || strings.HasPrefix(fieldType, "Nullable(DateTime(")) {
				scanArgs[i] = &entry.Time
			} else if fieldName == lp.timeMsField && (fieldType == "DateTime64" || strings.HasPrefix(fieldType, "DateTime64(")) {
				scanArgs[i] = &entry.TimeMs
			} else if fieldName == lp.dateField && (fieldType == "Date" || fieldType == "Date32" || strings.HasPrefix(fieldType, "Date(") || strings.HasPrefix(fieldType, "Date32(") || strings.HasPrefix(fieldType, "Nullable(Date)") || strings.HasPrefix(fieldType, "Nullable(Date(") || strings.HasPrefix(fieldType, "Nullable(Date32")) {
				scanArgs[i] = &entry.Date
			} else if fieldName == lp.messageField {
				scanArgs[i] = &entry.Message
			} else if fieldName == lp.levelField {
				scanArgs[i] = &entry.Level
			} else {
				// For any other field, use a dummy variable
				var dummy interface{}
				scanArgs[i] = &dummy
			}
		}

		if err := rows.Scan(scanArgs...); err != nil {
			continue
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (lp *LogPanel) updateLogTable(entries []LogEntry) {
	lp.app.tviewApp.QueueUpdateDraw(func() {
		// Update log details table
		lp.logDetails.Clear()

		// Re-add headers
		lp.logDetails.SetCell(0, 0, tview.NewTableCell("Time").SetTextColor(tcell.ColorYellow))
		lp.logDetails.SetCell(0, 1, tview.NewTableCell("Message").SetTextColor(tcell.ColorYellow))
		// Add log entries
		for i, entry := range entries {
			timeStr := ""
			if !entry.Time.IsZero() {
				timeStr = entry.Time.Format("2006-01-02 15:04:05")
			} else if entry.TimeMs > 0 {
				timeStr = time.Unix(0, entry.TimeMs*int64(time.Millisecond)).Format("2006-01-02 15:04:05")
			} else if entry.Date != "" {
				timeStr = entry.Date
			}

			lp.logDetails.SetCell(i+1, 0, tview.NewTableCell(timeStr))
			lp.logDetails.SetCell(i+1, 1, tview.NewTableCell(entry.Message))

			// Color by level if available
			if entry.Level != "" {
				color := tcell.ColorWhite
				switch strings.ToLower(entry.Level) {
				case "error", "exception":
					color = tcell.ColorRed
				case "warning", "debug", "trace":
					color = tcell.ColorYellow
				case "info":
					color = tcell.ColorGreen
				}
				lp.logDetails.GetCell(i+1, 1).SetTextColor(color)
			}
		}

		// Update overview panel
		lp.updateOverview(lp.overview)
	})
}

func ternary(condition bool, trueVal, falseVal string) string {
	if condition {
		return trueVal
	}
	return falseVal
}
