package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
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
	mainFlex := tview.NewFlex().SetDirection(tview.FlexRow)

	// Database and table selection form
	form := tview.NewForm()
	form.SetBorder(true).SetTitle("Log Explorer")

	// Database dropdown
	form.AddDropDown("Database", []string{"system", "default"}, 0,
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

	mainFlex.AddItem(form, 0, 1, true)
	a.pages.AddPage("logs", mainFlex, true, true)
	a.pages.SwitchToPage("logs")
}

func (lp *LogPanel) updateTableDropdown(form *tview.Form) {
	// Query ClickHouse for tables in selected database
	query := fmt.Sprintf("SHOW TABLES FROM %s", lp.database)
	rows, err := lp.app.clickHouse.Query(context.Background(), query)
	if err != nil {
		lp.app.mainView.SetText(fmt.Sprintf("Error getting tables: %v", err))
		return
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			continue
		}
		tables = append(tables, table)
	}

	// Update the table dropdown
	form.GetFormItemByLabel("Table").(*tview.DropDown).
		SetOptions(tables, nil)
}

func (lp *LogPanel) updateFieldDropdowns(form *tview.Form) {
	if lp.database == "" || lp.table == "" {
		return
	}

	// Query ClickHouse for columns in selected table
	query := fmt.Sprintf("DESCRIBE %s.%s", lp.database, lp.table)
	rows, err := lp.app.clickHouse.Query(context.Background(), query)
	if err != nil {
		lp.app.mainView.SetText(fmt.Sprintf("Error getting columns: %v", err))
		return
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var name, typ, defaultType, defaultExpr, comment, codec string
		if err := rows.Scan(&name, &typ, &defaultType, &defaultExpr, &comment, &codec); err != nil {
			continue
		}
		columns = append(columns, name)
	}

	// Add field selection dropdowns
	form.Clear(true)
	form.AddDropDown("Database", []string{"system", "default"}, 0,
		func(db string, index int) { lp.database = db; lp.updateTableDropdown(form) })
	form.AddDropDown("Table", []string{}, 0,
		func(table string, index int) { lp.table = table; lp.updateFieldDropdowns(form) })
	form.AddDropDown("Message Field", columns, 0,
		func(field string, index int) { lp.messageField = field })
	form.AddDropDown("Time Field", columns, 0,
		func(field string, index int) { lp.timeField = field })
	form.AddDropDown("TimeMs Field (optional)", append([]string{""}, columns...), 0,
		func(field string, index int) { lp.timeMsField = field })
	form.AddDropDown("Date Field (optional)", append([]string{""}, columns...), 0,
		func(field string, index int) { lp.dateField = field })
	form.AddDropDown("Level Field (optional)", append([]string{""}, columns...), 0,
		func(field string, index int) { lp.levelField = field })
	form.AddInputField("Window Size (ms)", "1000", 10,
		func(text string) { lp.windowSize, _ = strconv.Atoi(text) })

	form.AddButton("Explore Logs", func() { lp.showLogExplorer() })
	form.AddButton("Cancel", func() { lp.app.SwitchToMainPage("Returned from :logs") })
}

func (lp *LogPanel) showLogExplorer() {
	// Create main layout with 3 panels
	mainFlex := tview.NewFlex().SetDirection(tview.FlexRow)

	// 1. AdHoc Filter Panel (20% height)
	filterPanel := tview.NewFlex().SetDirection(tview.FlexColumn)
	// TODO: Implement adhoc filter UI
	mainFlex.AddItem(filterPanel, 3, 1, false)

	// 2. Overview Panel (20% height)
	overviewPanel := tview.NewTextView().SetDynamicColors(true)
	// TODO: Implement overview bar chart
	mainFlex.AddItem(overviewPanel, 3, 1, false)

	// 3. Log Details Panel (60% height)
	logDetails := tview.NewTable().
		SetBorders(false).
		SetSelectable(true, false)
	// TODO: Implement log details table
	mainFlex.AddItem(logDetails, 0, 1, true)

	// Execute initial query
	go lp.loadLogs()

	lp.app.pages.AddPage("logExplorer", mainFlex, true, true)
	lp.app.pages.SwitchToPage("logExplorer")
}

func (lp *LogPanel) loadLogs() {
	if lp.database == "" || lp.table == "" || lp.messageField == "" || lp.timeField == "" {
		return
	}

	// Build query with sliding window
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

	query := fmt.Sprintf(`
		SELECT %s
		FROM %s.%s
		WHERE %s >= ?
		ORDER BY %s DESC
		LIMIT ?`,
		strings.Join(fields, ", "),
		lp.database,
		lp.table,
		lp.timeField,
		lp.timeField)

	windowStart := time.Now().Add(-time.Duration(lp.windowSize) * time.Millisecond)
	rows, err := lp.app.clickHouse.Query(context.Background(), query, windowStart, lp.windowSize)
	if err != nil {
		lp.app.mainView.SetText(fmt.Sprintf("Query failed: %v", err))
		return
	}
	defer rows.Close()

	// Process results
	var entries []LogEntry
	for rows.Next() {
		var entry LogEntry
		// TODO: Scan fields based on selected columns
		entries = append(entries, entry)
	}

	lp.currentResults = entries
	// TODO: Update UI with results
}

// TODO: Implement additional methods for:
// - Adhoc filter management
// - Overview bar chart rendering
// - Log details table rendering
// - Pagination/loading more logs
