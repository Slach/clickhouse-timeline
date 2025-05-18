package tui

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const profileEventsQueryTemplate = `
SELECT
    key AS EventName,
    count(),
    quantile(0.5)(value) AS p50,
    quantile(0.9)(value) AS p90,
    quantile(0.99)(value) AS p99
FROM clusterAllReplicas('%s', merge(system,'^query_log'))
ARRAY JOIN mapKeys(ProfileEvents) AS key, mapValues(ProfileEvents) AS value
WHERE
    event_date >= toDate(parseDateTimeBestEffort('%s')) AND event_date <= toDate(parseDateTimeBestEffort('%s')) AND
    event_time >= parseDateTimeBestEffort('%s')) AND event_time <= parseDateTimeBestEffort('%s'))
    AND type != 'QueryStart'
    %s
GROUP BY key
ORDER BY key
`

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

		// Build category filter
		var categoryFilter string
		switch categoryType {
		case CategoryQueryHash:
			categoryFilter = fmt.Sprintf("AND normalized_query_hash = '%s'", categoryValue)
		case CategoryTable:
			categoryFilter = fmt.Sprintf("AND hasAll(tables, ['%s'])", categoryValue)
		case CategoryHost:
			categoryFilter = fmt.Sprintf("AND hostName() = '%s'", categoryValue)
		default:
			categoryFilter = ""
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
				table.SetCell(0, col, tview.NewTableCell(header).
					SetTextColor(tcell.ColorYellow).
					SetAlign(tview.AlignCenter)
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
				)

				if err := rows.Scan(&event, &count, &p50, &p90, &p99); err != nil {
					a.mainView.SetText(fmt.Sprintf("Error scanning row: %v", err))
					return
				}

				// Add row to table
				table.SetCell(row, 0, tview.NewTableCell(event).
					SetTextColor(tcell.ColorWhite).
					SetAlign(tview.AlignLeft))
				table.SetCell(row, 1, tview.NewTableCell(fmt.Sprintf("%d", count)).
					SetTextColor(tcell.ColorWhite).
					SetAlign(tview.AlignRight))
				table.SetCell(row, 2, tview.NewTableCell(fmt.Sprintf("%.2f", p50)).
					SetTextColor(tcell.ColorWhite).
					SetAlign(tview.AlignRight))
				table.SetCell(row, 3, tview.NewTableCell(fmt.Sprintf("%.2f", p90)).
					SetTextColor(tcell.ColorWhite).
					SetAlign(tview.AlignRight))
				table.SetCell(row, 4, tview.NewTableCell(fmt.Sprintf("%.2f", p99)).
					SetTextColor(tcell.ColorWhite).
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

			// Add key handler
			table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
				if event.Key() == tcell.KeyEscape {
					a.pages.SwitchToPage("heatmap")
					return nil
				}
				return event
			})

			a.pages.AddPage("profile_events", table, true, true)
			a.pages.SwitchToPage("profile_events")
		})
	}()
}
