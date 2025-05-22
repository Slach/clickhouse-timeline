package tui

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/rivo/tview"
	"github.com/spf13/cobra"
)

// LogPanelHandler manages both TUI and CLI log interfaces
type LogPanelHandler struct {
	app            *tview.Application
	mainFlex       *tview.Flex
	clickhouseConn clickhouse.Conn
	targetTable    string
	selectedFields []string
	windowSize     int
	lastQueryTime  time.Time
}

// NewLogPanelHandler creates a new log handler instance
func NewLogPanelHandler(app *tview.Application, conn clickhouse.Conn) *LogPanelHandler {
	return &LogPanelHandler{
		app:            app,
		clickhouseConn: conn,
		windowSize:     1000, // default window size
	}
}

// HandleCLI implements the CLI "logs" command
func (h *LogPanelHandler) HandleCLI(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: logs <table>")
	}

	h.targetTable = args[0]

	// Get fields from cobra flag
	if fields, _ := cmd.Flags().GetString("fields"); fields != "" {
		h.selectedFields = strings.Split(fields, ",")
	}

	// Get window size from cobra flag
	if window, _ := cmd.Flags().GetInt("window"); window > 0 {
		h.windowSize = window
	}

	// Ensure required fields are included
	if !contains(h.selectedFields, "message") {
		h.selectedFields = append(h.selectedFields, "message")
	}
	if !contains(h.selectedFields, "time") && !contains(h.selectedFields, "timeMs") && !contains(h.selectedFields, "date") {
		h.selectedFields = append(h.selectedFields, "time")
	}

	return h.executeQuery()
}

func (h *LogPanelHandler) executeQuery() error {
	if h.targetTable == "" {
		return fmt.Errorf("target table not specified")
	}

	// Build fields list
	fields := strings.Join(h.selectedFields, ", ")
	if fields == "" {
		fields = "*"
	}

	// Build sliding window query
	query := fmt.Sprintf(`
		SELECT %s
		FROM %s
		WHERE event_time >= ?
		ORDER BY event_time DESC
		LIMIT ?`,
		fields, h.targetTable)

	// Execute query with sliding window parameters
	windowStart := time.Now().Add(-time.Duration(h.windowSize) * time.Millisecond)
	rows, err := h.clickhouseConn.Query(context.Background(), query, windowStart, h.windowSize)
	if err != nil {
		return fmt.Errorf("query failed: %v", err)
	}
	defer rows.Close()

	// TODO: Process and display results
	h.lastQueryTime = time.Now()
	return nil
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// HandleTUI implements the TUI ":logs" command
func (h *LogPanelHandler) HandleTUI() {
	// Initialize main flex layout
	h.mainFlex = tview.NewFlex().SetDirection(tview.FlexRow)

	// Build form components
	form := tview.NewForm()
	form.AddDropDown("Target Table", []string{"system.query_log", "system.query_thread_log"}, 0,
		func(option string, index int) {
			h.targetTable = option
		})

	// Field selection
	form.AddCheckbox("message", true, nil) // Required
	form.AddCheckbox("time", true, nil)    // Required
	form.AddCheckbox("timeMs", false, nil)
	form.AddCheckbox("date", false, nil)
	form.AddCheckbox("level", false, nil)

	// Window size
	form.AddInputField("Window Size", "1000", 10,
		func(textToCheck string, lastChar rune) bool {
			_, err := strconv.Atoi(textToCheck)
			return err == nil
		},
		func(text string) {
			size, _ := strconv.Atoi(text)
			h.windowSize = size
		})

	// TODO: Add other form fields and buttons

	h.mainFlex.AddItem(form, 0, 1, true)
	h.app.SetRoot(h.mainFlex, true)
}

// Initial form model for target selection
type logFormModel struct {
	// TODO: Implement form fields
}

// AdHocPanel handles Grafana-style filtering
type AdHocPanel struct {
	// TODO: Implement filter panel
}

// OverViewPanel shows ASCII bar chart
type OverViewPanel struct {
	// TODO: Implement overview panel
}

// LogDetailsPanel shows detailed log entries
type LogDetailsPanel struct {
	// TODO: Implement details panel
}
