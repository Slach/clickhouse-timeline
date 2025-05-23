package tui

import (
	"fmt"
	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"strings"
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
    formatReadableQuantity(p99) AS p99_s,
    any(normalizeQueryKeepNames(query)) AS normalized_query
FROM clusterAllReplicas('%s', merge(system,'^query_log'))
LEFT ARRAY JOIN mapKeys(ProfileEvents) AS key, mapValues(ProfileEvents) AS value
WHERE
    event_date >= toDate(parseDateTimeBestEffort('%s')) AND event_date <= toDate(parseDateTimeBestEffort('%s')) AND
    event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s')
    AND type != 'QueryStart'
    %s
GROUP BY key
ORDER BY key
`

func (a *App) filterProfileEventsTable(headers []string, table *tview.Table, originalRows [][]*tview.TableCell, filter string) {
	// Clear existing rows (keep headers)
	for r := table.GetRowCount() - 1; r > 0; r-- {
		table.RemoveRow(r)
	}

	filter = strings.ToLower(filter)
	for _, row := range originalRows {
		// Check if any cell in row matches filter (case-insensitive)
		match := false
		for _, cell := range row {
			if strings.Contains(strings.ToLower(cell.Text), filter) {
				match = true
				break
			}
		}

		if match || filter == "" {
			r := table.GetRowCount()
			for c, cell := range row {
				// clone only first columns which exists in headers
				if c < len(headers) {
					// Clone the original cell to preserve all attributes
					newCell := tview.NewTableCell(cell.Text).
						SetStyle(cell.Style).
						SetSelectedStyle(cell.SelectedStyle).
						SetAlign(cell.Align)
					table.SetCell(r, c, newCell)
				}
			}
		}
	}
}

func (a *App) ShowProfileEvents(categoryType CategoryType, categoryValue string, fromTime, toTime time.Time, cluster string) {
	if a.clickHouse == nil {
		a.mainView.SetText("Error: Please connect to a ClickHouse instance first")
		a.pages.SwitchToPage("main")
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
			case CategoryError:
				parts := strings.Split(categoryValue, ":")
				categoryFilter = fmt.Sprintf("AND errorCodeToName(exception_code)='%s' AND normalized_query_hash = %s", parts[0], parts[1])
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

			// Create flex layout with table on left and query view on right
			flex := tview.NewFlex().
				SetDirection(tview.FlexColumn)

			// Set headers
			headers := []string{"Event", "Count", "p50", "p90", "p99"}
			for col, header := range headers {
				table.SetCell(0, col,
					tview.NewTableCell(header).SetTextColor(tcell.ColorYellow).SetAlign(tview.AlignCenter),
				)
			}
			// Store original cells data for filtering (preserving colors and formatting)
			originalRows := make([][]*tview.TableCell, 0)

			// Process rows
			row := 1
			for rows.Next() {
				var (
					event           string
					count           int
					p50             float64
					p90             float64
					p99             float64
					p50s            string
					p90s            string
					p99s            string
					normalizedQuery string
				)

				if err := rows.Scan(&event, &count, &p50, &p90, &p99, &p50s, &p90s, &p99s, &normalizedQuery); err != nil {
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
				//add normalizedQuery as hidden column in originalRows
				originalRows = append(originalRows, []*tview.TableCell{
					table.GetCell(row, 0),
					table.GetCell(row, 1),
					table.GetCell(row, 2),
					table.GetCell(row, 3),
					table.GetCell(row, 4),
					tview.NewTableCell(normalizedQuery).SetSelectable(false),
				})

				row++
			}

			if err := rows.Err(); err != nil {
				a.mainView.SetText(fmt.Sprintf("Error reading rows: %v", err))
				return
			}

			// Set title
			title := fmt.Sprintf("Profile Events: %s (%s to %s)",
				categoryValue,
				fromStr,
				toStr)
			table.SetTitle(title).SetBorder(true)

			// Add key handler for filtering table content
			table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
				if event.Key() == tcell.KeyEscape {
					if a.pages.HasPage("heatmap") {
						a.pages.SwitchToPage("heatmap")
					} else {
						a.pages.SwitchToPage("main")
					}
					return nil
				}
				if event.Rune() == '/' {
					// Show filter input for table content
					filterInput := tview.NewInputField().
						SetLabel("/").
						SetFieldWidth(30).
						SetChangedFunc(func(text string) {
							a.filterProfileEventsTable(headers, table, originalRows, text)
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

			// Create query view widget
			queryView := widgets.NewQueryView()

			// Update query view when selection changes
			table.SetSelectionChangedFunc(func(row, column int) {
				if row > 0 && row <= len(originalRows) {
					if normalizedQuery := originalRows[row-1][5].Text; normalizedQuery != "" {
						queryView.SetSQL(normalizedQuery)
					}
				}
			})

			// Add components to flex
			flex.AddItem(table, 0, 2, true).
				AddItem(queryView, 0, 1, false)

			a.pages.AddPage("profile_events", flex, true, true)
			a.pages.SwitchToPage("profile_events")
		})
	}()
}
