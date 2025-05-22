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

// ShowLogsPanel displays the logs interface for both CLI and TUI
func (a *App) ShowLogsPanel() {
	if a.clickHouse == nil {
		a.SwitchToMainPage("Error: Please connect to a ClickHouse instance first using :connect command")
		return
	}

	// Initialize main flex layout
	mainFlex := tview.NewFlex().SetDirection(tview.FlexRow)

	// Build form components
	form := tview.NewForm()
	targetTable := "system.query_log"
	selectedFields := []string{"message", "time"}
	windowSize := 1000

	form.AddDropDown("Target Table", []string{"system.query_log", "system.query_thread_log"}, 0,
		func(option string, index int) {
			targetTable = option
		})

	// Field selection
	form.AddCheckbox("message", true, func(checked bool) {
		if checked {
			selectedFields = appendIfMissing(selectedFields, "message")
		} else {
			selectedFields = removeFromSlice(selectedFields, "message")
		}
	})
	form.AddCheckbox("time", true, func(checked bool) {
		if checked {
			selectedFields = appendIfMissing(selectedFields, "time")
		} else {
			selectedFields = removeFromSlice(selectedFields, "time")
		}
	})
	form.AddCheckbox("timeMs", false, func(checked bool) {
		if checked {
			selectedFields = appendIfMissing(selectedFields, "timeMs")
		} else {
			selectedFields = removeFromSlice(selectedFields, "timeMs")
		}
	})
	form.AddCheckbox("date", false, func(checked bool) {
		if checked {
			selectedFields = appendIfMissing(selectedFields, "date")
		} else {
			selectedFields = removeFromSlice(selectedFields, "date")
		}
	})
	form.AddCheckbox("level", false, func(checked bool) {
		if checked {
			selectedFields = appendIfMissing(selectedFields, "level")
		} else {
			selectedFields = removeFromSlice(selectedFields, "level")
		}
	})

	// Window size
	form.AddInputField("Window Size", "1000", 10,
		func(textToCheck string, lastChar rune) bool {
			_, err := strconv.Atoi(textToCheck)
			return err == nil
		},
		func(text string) {
			size, _ := strconv.Atoi(text)
			windowSize = size
		})

	// Add execute button
	form.AddButton("Execute", func() {
		a.executeLogsQuery(targetTable, selectedFields, windowSize)
	})
	form.AddButton("Cancel", func() {
		a.SwitchToMainPage("Logs panel closed")
	})

	mainFlex.AddItem(form, 0, 1, true)
	a.pages.AddPage("logs", mainFlex, true, true)
	a.pages.SwitchToPage("logs")
}

func (a *App) executeLogsQuery(targetTable string, selectedFields []string, windowSize int) {
	if targetTable == "" {
		a.SwitchToMainPage("Error: Target table not specified")
		return
	}

	// Build fields list
	fields := strings.Join(selectedFields, ", ")
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
		fields, targetTable)

	// Execute query with sliding window parameters
	windowStart := time.Now().Add(-time.Duration(windowSize) * time.Millisecond)
	rows, err := a.clickHouse.Query(context.Background(), query, windowStart, windowSize)
	if err != nil {
		a.SwitchToMainPage(fmt.Sprintf("Query failed: %v", err))
		return
	}
	defer rows.Close()

	// TODO: Process and display results
	a.SwitchToMainPage("Log query executed successfully")
}

func appendIfMissing(slice []string, item string) []string {
	for _, s := range slice {
		if s == item {
			return slice
		}
	}
	return append(slice, item)
}

func removeFromSlice(slice []string, item string) []string {
	for i, s := range slice {
		if s == item {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}
