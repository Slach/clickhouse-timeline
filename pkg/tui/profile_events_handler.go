package tui

import (
	"fmt"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"time"
)

const profileEventsQueryTemplate = `
SELECT
    key AS EventName,
    count(),
    quantile(0.5)(value) AS p50,
    quantile(0.9)(value) AS p90,
    quantile(0.99)(value) AS p99,
    formatReadableQuantity(p50) AS p50_s,
    formatReadableQuantity(p90) AS p90_s,
    formatReadableQuantity(p99) AS p99_s
FROM clusterAllReplicas('%s', merge(system,'^query_log'))
ARRAY JOIN mapKeys(ProfileEvents) AS key, mapValues(ProfileEvents) AS value
WHERE
    event_date >= toDate(parseDateTimeBestEffort('%s')) AND event_date <= toDate(parseDateTimeBestEffort('%s')) AND
    event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s')
    AND type != 'QueryStart'
    %s
GROUP BY key
ORDER BY key
`

func (a *App) filterProfileEventsTable(table *tview.Table, originalRows [][]string, filter string) {
	// Clear existing rows (keep headers)
	for r := table.GetRowCount() - 1; r > 0; r-- {
		table.RemoveRow(r)
	}

	filter = strings.ToLower(filter)
	for _, row := range originalRows {
		// Check if any cell in row matches filter (case insensitive)
		match := false
		for _, cell := range row {
			if strings.Contains(strings.ToLower(cell), filter) {
				match = true
				break
			}
		}

		if match || filter == "" {
			r := table.GetRowCount()
			for c, val := range row {
				table.SetCell(r, c, tview.NewTableCell(val).
					SetTextColor(table.GetCell(1, c).GetTextColor()).
					SetAlign(tview.AlignLeft)
			}
		}
	}
}

func (a *App) ShowProfileEvents(categoryType CategoryType, categoryValue string, fromTime, toTime time.Time, cluster string) {
	if a.clickHouse == nil {
		a.mainView.SetText("Error: Please connect to a ClickHouse instance first")
		return
	}

	a.mainView.SetText("Loading profile events, please wait...")

	go func() {
		// Format dates for the query
		fromStr := fromTime.Format("2006-01-02 15:04:05 -07:00")
		toStr := toTime.Format("2006-01-02 15:04:05 -07:00")

		// Build category filter if categoryValue is provided
		var categoryFilter string
		if categoryValue != "" {
			switch categoryType {
			case CategoryQueryHash:
				categoryFilter = fmt.Sprintf("AND normalized_query_hash = '%s'", categoryValue)
			case CategoryTable:
				categoryFilter = fmt.Sprintf("AND has(tables, ['%s'])", categoryValue)
			case CategoryHost:
				categoryFilter = fmt.Sprintf("AND hostName() = '%s'", categoryValue)
			}
		}

		query := fmt.Sprintf(
			profileEventsQueryTemplate,
			cluster,
			fromStr, toStr, fromStr, toStr,
			categoryFilter,
		)

		rows, err := a.clickHouse.Query(query)
		if err != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Error executing query: %v\n%s", err, query))
			})
			return
		}
		defer rows.Close()

		// Create table to display results
		a.tviewApp.QueueUpdateDraw(func() {
			table := tview.NewTable().
				SetBorders(false).
				SetSelectable(true, true).
				SetFixed(1, 1)

			// Set headers
			headers := []string{"Event", "Count", "p50", "p90", "p99"}
			for col, header := range headers {
				table.SetCell(0, col,
					tview.NewTableCell(header).SetTextColor(tcell.ColorYellow).SetAlign(tview.AlignCenter),
				)
			}

			// Process rows
			row := 1
			for rows.Next() {
				var (
					event string
					count int
					p50   float64
					p90   float64
					p99   float64
					p50s  string
					p90s  string
					p99s  string
				)

				if err := rows.Scan(&event, &count, &p50, &p90, &p99, &p50s, &p90s, &p99s); err != nil {
					a.mainView.SetText(fmt.Sprintf("Error scanning row: %v", err))
					return
				}

				// Determine cell colors based on percentile differences
				color := tcell.ColorWhite
				if p90 > 2*p50 || p99 > 2*p90 {
					color = tcell.ColorYellow
				}
				if p90 > 4*p50 || p99 > 6*p50 {
					color = tcell.ColorRed
				}

				// Add row to table
				table.SetCell(row, 0, tview.NewTableCell(event).
					SetTextColor(color).
					SetAlign(tview.AlignLeft))
				table.SetCell(row, 1, tview.NewTableCell(fmt.Sprintf("%d", count)).
					SetTextColor(color).
					SetAlign(tview.AlignRight))
				table.SetCell(row, 2, tview.NewTableCell(p50s).
					SetTextColor(color).
					SetAlign(tview.AlignRight))
				table.SetCell(row, 3, tview.NewTableCell(p90s).
					SetTextColor(color).
					SetAlign(tview.AlignRight))
				table.SetCell(row, 4, tview.NewTableCell(p99s).
					SetTextColor(color).
					SetAlign(tview.AlignRight))

				row++
			}

			if err := rows.Err(); err != nil {
				a.mainView.SetText(fmt.Sprintf("Error reading rows: %v", err))
				return
			}

			// Set title
			title := fmt.Sprintf("Profile Events: %s (%s to %s)",
				categoryValue,
				fromTime.Format("2006-01-02 15:04:05"),
				toTime.Format("2006-01-02 15:04:05"))
			table.SetTitle(title).SetBorder(true)

			// Store original rows data for filtering
			originalRows := make([][]string, table.GetRowCount()-1)
			for r := 1; r < table.GetRowCount(); r++ {
				originalRows[r-1] = []string{
					table.GetCell(r, 0).Text,
					table.GetCell(r, 1).Text,
					table.GetCell(r, 2).Text,
					table.GetCell(r, 3).Text,
					table.GetCell(r, 4).Text,
				}
			}

			// Add key handler for filtering table content
			table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
				if event.Key() == tcell.KeyEscape {
					a.pages.SwitchToPage("heatmap")
					return nil
				}
				if event.Rune() == '/' {
					// Show filter input for table content
					filterInput := tview.NewInputField().
						SetLabel("/").
						SetFieldWidth(30).
						SetChangedFunc(func(text string) {
							a.filterProfileEventsTable(table, originalRows, text)
						})
					
					filterInput.SetDoneFunc(func(key tcell.Key) {
						if key == tcell.KeyEscape || key == tcell.KeyEnter {
							a.pages.RemovePage("profile_filter")
							a.tviewApp.SetFocus(table)
						}
					})

					filterModal := tview.NewFlex().
						SetDirection(tview.FlexRow).
						AddItem(filterInput, 1, 0, true).
						AddItem(table, 0, 1, false)

					a.pages.AddPage("profile_filter", filterModal, true, true)
					a.tviewApp.SetFocus(filterInput)
					return nil
				}
				if event.Key() == tcell.KeyEnter {
					row, _ := table.GetSelection()
					eventName := table.GetCell(row, 0).Text
					
					// Query event description
					go func() {
						query := fmt.Sprintf("SELECT description FROM system.events WHERE name = '%s'", eventName)
						rows, err := a.clickHouse.Query(query)
						if err != nil {
							a.tviewApp.QueueUpdateDraw(func() {
								a.mainView.SetText(fmt.Sprintf("Error getting event description: %v", err))
							})
							return
						}
						defer rows.Close()

						var description string
						if rows.Next() {
							if err := rows.Scan(&description); err != nil {
								a.tviewApp.QueueUpdateDraw(func() {
									a.mainView.SetText(fmt.Sprintf("Error scanning description: %v", err))
								})
								return
							}
						}

						// Show description in modal
						a.tviewApp.QueueUpdateDraw(func() {
							modal := tview.NewModal().
								SetText(fmt.Sprintf("[yellow]%s[-]\n\n%s", eventName, description)).
								AddButtons([]string{"OK"}).
								SetDoneFunc(func(buttonIndex int, buttonLabel string) {
									a.pages.HidePage("event_desc")
								})

							a.pages.AddPage("event_desc", modal, true, true)
						})
					}()
					return nil
				}
				return event
			})

			a.pages.AddPage("profile_events", table, true, true)
			a.pages.SwitchToPage("profile_events")
		})
	}()
}
