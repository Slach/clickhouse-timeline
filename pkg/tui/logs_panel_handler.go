package tui

import (
	"fmt"
	"github.com/gdamore/tcell/v2"
	"github.com/rs/zerolog/log"
	"slices"
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

	// Main flex layout
	logsFlex := tview.NewFlex().SetDirection(tview.FlexRow)

	// Database and table selection form
	form := tview.NewForm()
	form.SetBorder(true).SetTitle("Log Explorer")

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

	lp.databases = []string{}
	for rows.Next() {
		var db string
		if scanErr := rows.Scan(&db); scanErr != nil {
			log.Error().Err(scanErr).Msg("can't scan database name")
			continue
		}
		lp.databases = append(lp.databases, db)
	}

	// Database dropdown
	form.AddDropDown("Database", lp.databases, 0,
		func(db string, index int) {
			lp.database = db
			lp.updateTableDropdown(form)
		})

	// Table dropdown (will be populated after database selection)
	form.AddDropDown("Table", []string{}, 0,
		func(table string, index int) {
			lp.table = table
			lp.updateFieldDropdowns(form)
		})

	// Add buttons
	form.AddButton("Explore Logs", func() {
		lp.showLogExplorer()
	})
	form.AddButton("Cancel", func() {
		a.SwitchToMainPage("Returned from :logs")
	})

	logsFlex.AddItem(form, 0, 1, true)
	a.pages.AddPage("logs", logsFlex, true, true)
	a.pages.SwitchToPage("logs")
	form.GetFormItemByLabel("Database")
}

func (lp *LogPanel) updateTableDropdown(form *tview.Form) {
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

	lp.tables = []string{}
	for rows.Next() {
		var table string
		if scanErr := rows.Scan(&table); scanErr != nil {
			log.Error().Err(scanErr).Msg("can't scan tables in updateTableDropdown")
		}
		lp.tables = append(lp.tables, table)
	}

	// Safely update the table dropdown
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

	// Clear form but keep current selections
	currentDB := lp.database
	currentTable := lp.table
	currentMsgField := lp.messageField
	currentTimeField := lp.timeField
	currentTimeMsField := lp.timeMsField
	currentDateField := lp.dateField
	currentLevelField := lp.levelField

	form.Clear(true)

	// Add dropdowns with current selections
	dbIdx := slices.Index(lp.databases, currentDB)
	if dbIdx == -1 {
		dbIdx = 0
	}
	form.AddDropDown("Database", lp.databases, dbIdx,
		func(db string, index int) {
			if db != lp.database { // Only update if changed
				lp.database = db
				lp.updateTableDropdown(form)
			}
		})

	tableIdx := slices.Index(lp.tables, currentTable)
	if tableIdx == -1 {
		tableIdx = 0
	}
	form.AddDropDown("Table", lp.tables, tableIdx,
		func(table string, index int) {
			if table != lp.table { // Only update if changed
				lp.table = table
				lp.updateFieldDropdowns(form)
			}
		})

	msgIdx := slices.Index(columns, currentMsgField)
	if msgIdx == -1 {
		msgIdx = 0
	}
	form.AddDropDown("Message Field", columns, msgIdx,
		func(field string, index int) { lp.messageField = field })

	timeIdx := slices.Index(timeColumns, currentTimeField)
	if timeIdx == -1 {
		timeIdx = 0
	}
	form.AddDropDown("Time Field", timeColumns, timeIdx,
		func(field string, index int) { lp.timeField = field })

	timeMsIdx := slices.Index(append([]string{""}, timeMsColumns...), currentTimeMsField)
	if timeMsIdx == -1 {
		timeMsIdx = 0
	}
	form.AddDropDown("TimeMs Field (optional)", append([]string{""}, timeMsColumns...), timeMsIdx,
		func(field string, index int) { lp.timeMsField = field })

	dateIdx := slices.Index(append([]string{""}, dateColumns...), currentDateField)
	if dateIdx == -1 {
		dateIdx = 0
	}
	form.AddDropDown("Date Field (optional)", append([]string{""}, dateColumns...), dateIdx,
		func(field string, index int) { lp.dateField = field })

	levelIdx := slices.Index(append([]string{""}, columns...), currentLevelField)
	if levelIdx == -1 {
		levelIdx = 0
	}
	form.AddDropDown("Level Field (optional)", append([]string{""}, columns...), levelIdx,
		func(field string, index int) { lp.levelField = field })

	form.AddInputField("Window Size (rows)", fmt.Sprint(lp.windowSize), 10,
		func(text string, lastRune rune) bool { return unicode.IsDigit(lastRune) },
		func(text string) { lp.windowSize, _ = strconv.Atoi(text) })

	form.AddButton("Explore Logs", func() { lp.showLogExplorer() })
	form.AddButton("Cancel", func() { lp.app.SwitchToMainPage("Returned from :logs") })
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

	// Build logsQuery with sliding window
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

	// Build WHERE clause with filters
	whereConditions := []string{fmt.Sprintf("%s >= ?", lp.timeField)}
	queryArgs := []interface{}{lp.app.fromTime}

	// Add filter conditions
	for _, filter := range lp.filters {
		whereConditions = append(whereConditions, fmt.Sprintf("%s %s ?", filter.Field, filter.Operator))
		queryArgs = append(queryArgs, filter.Value)
	}

	logsQuery := fmt.Sprintf(`
		SELECT %s
		FROM `+"`%s`.`%s`"+`
		WHERE %s
		ORDER BY %s
		LIMIT ?`,
		strings.Join(fields, ", "),
		lp.database,
		lp.table,
		strings.Join(whereConditions, " AND "),
		lp.timeField)

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
	var entries []LogEntry
	colTypes, _ := rows.ColumnTypes()
	scanArgs := make([]interface{}, len(colTypes))
	for rows.Next() {
		var entry LogEntry
		// Initialize scan args based on column types
		for i, col := range colTypes {
			switch col.DatabaseTypeName() {
			case "DateTime", "DateTime64":
				scanArgs[i] = &entry.Time
			case "UInt64", "Int64":
				scanArgs[i] = &entry.TimeMs
			case "String", "Enum":
				switch col.Name() {
				case lp.messageField:
					scanArgs[i] = &entry.Message
				case lp.levelField:
					scanArgs[i] = &entry.Level
				case lp.dateField:
					scanArgs[i] = &entry.Date
				default:
					var dummy string
					scanArgs[i] = &dummy
				}
			default:
				var dummy interface{}
				scanArgs[i] = &dummy
			}
		}

		if err := rows.Scan(scanArgs...); err != nil {
			continue
		}
		entries = append(entries, entry)
	}

	lp.currentResults = entries
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
	if lp.levelField == "" {
		view.SetText("No level field selected for overview")
		return
	}

	levelCounts := make(map[string]int)
	for _, entry := range lp.currentResults {
		if entry.Level != "" {
			levelCounts[entry.Level]++
		}
	}

	var builder strings.Builder
	for level, count := range levelCounts {
		bar := strings.Repeat("█", int(float64(count)/float64(len(lp.currentResults))*50))
		color := "[white]"
		switch strings.ToLower(level) {
		case "error", "exception":
			color = "[red]"
		case "warning", "debug", "trace":
			color = "[yellow]"
		case "info":
			color = "[green]"
		}
		builder.WriteString(fmt.Sprintf("%s%-10s %s %d\n", color, level, bar, count))
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
	whereConditions := []string{fmt.Sprintf("%s BETWEEN ? AND ?", lp.timeField)}
	queryArgs := []interface{}{timeCondition, lp.app.toTime}

	// Add filter conditions
	for _, filter := range lp.filters {
		whereConditions = append(whereConditions, fmt.Sprintf("%s %s ?", filter.Field, filter.Operator))
		queryArgs = append(queryArgs, filter.Value)
	}

	query := fmt.Sprintf(`
		SELECT %s
		FROM %s.%s
		WHERE %s
		ORDER BY %s %s
		LIMIT ?`,
		strings.Join(lp.getSelectedFields(), ", "),
		lp.database,
		lp.table,
		strings.Join(whereConditions, " AND "),
		lp.timeField,
		ternary(newer, "ASC", "DESC"),
	)

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

	var newEntries []LogEntry
	colTypes, _ := rows.ColumnTypes()
	scanArgs := make([]interface{}, len(colTypes))

	for rows.Next() {
		var entry LogEntry
		// Initialize scan args based on column types
		for i, col := range colTypes {
			switch col.DatabaseTypeName() {
			case "DateTime", "DateTime64":
				scanArgs[i] = &entry.Time
			case "UInt64", "Int64":
				scanArgs[i] = &entry.TimeMs
			case "String":
				switch col.Name() {
				case lp.messageField:
					scanArgs[i] = &entry.Message
				case lp.levelField:
					scanArgs[i] = &entry.Level
				case lp.dateField:
					scanArgs[i] = &entry.Date
				default:
					var dummy string
					scanArgs[i] = &dummy
				}
			default:
				var dummy interface{}
				scanArgs[i] = &dummy
			}
		}

		if err := rows.Scan(scanArgs...); err != nil {
			continue
		}
		newEntries = append(newEntries, entry)
	}

	if newer {
		lp.currentResults = append(newEntries, lp.currentResults...)
	} else {
		lp.currentResults = append(lp.currentResults, newEntries...)
	}

	lp.app.tviewApp.QueueUpdateDraw(func() {
		// Update log details table
		lp.logDetails.Clear()

		// Re-add headers
		lp.logDetails.SetCell(0, 0, tview.NewTableCell("Time").SetTextColor(tcell.ColorYellow))
		lp.logDetails.SetCell(0, 1, tview.NewTableCell("Message").SetTextColor(tcell.ColorYellow))

		// Add log entries
		for i, entry := range lp.currentResults {
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

func (lp *LogPanel) showLogDetailsModal(entry LogEntry) {
	// Create modal dialog
	modal := tview.NewModal().
		SetText(lp.formatLogDetails(entry)).
		AddButtons([]string{"OK"}).
		SetDoneFunc(func(buttonIndex int, buttonLabel string) {
			lp.app.pages.RemovePage("logDetails")
		})

	modal.SetTitle("Log Entry Details").
		SetBorder(true).
		SetBackgroundColor(tcell.ColorDefault)

	lp.app.pages.AddPage("logDetails", modal, true, true)
	lp.app.pages.SwitchToPage("logDetails")
}

func (lp *LogPanel) formatLogDetails(entry LogEntry) string {
	var builder strings.Builder

	// Always show time fields if available
	if !entry.Time.IsZero() {
		builder.WriteString(fmt.Sprintf("[yellow]%s[-]: %s\n", lp.timeField, entry.Time.Format(time.RFC3339)))
	}
	if entry.TimeMs > 0 {
		builder.WriteString(fmt.Sprintf("[yellow]%s[-]: %d\n", lp.timeMsField, entry.TimeMs))
	}
	if entry.Date != "" {
		builder.WriteString(fmt.Sprintf("[yellow]%s[-]: %s\n", lp.dateField, entry.Date))
	}

	// Show level if available
	if entry.Level != "" {
		builder.WriteString(fmt.Sprintf("[yellow]%s[-]: %s\n", lp.levelField, entry.Level))
	}

	// Show message
	builder.WriteString(fmt.Sprintf("[yellow]%s[-]:\n", lp.messageField))
	messageLines := wordWrap(entry.Message, 80) // Wrap at 80 chars
	for _, line := range messageLines {
		builder.WriteString(fmt.Sprintf("  %s\n", line))
	}

	return builder.String()
}

func wordWrap(text string, lineWidth int) []string {
	words := strings.Fields(text)
	if len(words) == 0 {
		return []string{""}
	}

	var lines []string
	currentLine := words[0]

	for _, word := range words[1:] {
		if len(currentLine)+1+len(word) <= lineWidth {
			currentLine += " " + word
		} else {
			lines = append(lines, currentLine)
			currentLine = word
		}
	}
	lines = append(lines, currentLine)

	return lines
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

func ternary(condition bool, trueVal, falseVal string) string {
	if condition {
		return trueVal
	}
	return falseVal
}
