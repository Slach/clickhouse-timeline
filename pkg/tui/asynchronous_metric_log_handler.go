package tui

import (
	"fmt"
	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/rs/zerolog/log"
	"time"
)

const (
	asyncMetricLogColumnsQuery = `
SELECT DISTINCT name 
FROM system.columns 
WHERE database='system' 
  AND table LIKE 'asynchronous_metric_log%'`
)

func (a *App) ShowAsynchronousMetricLog(fromTime, toTime time.Time, cluster string) {
	if a.clickHouse == nil {
		a.mainView.SetText("Error: Please connect to a ClickHouse instance first using :connect command")
		a.pages.SwitchToPage("main")
		return
	}
	if cluster == "" {
		a.mainView.SetText("Error: Please select a cluster first using :cluster command")
		a.pages.SwitchToPage("main")
		return
	}

	a.mainView.SetText("Loading asynchronous_metric_log data, please wait...")

	// Format time strings for queries
	fromStr := fromTime.Format("2006-01-02 15:04:05 -07:00")
	toStr := toTime.Format("2006-01-02 15:04:05 -07:00")

	// Create filtered table widget
	filteredTable := widgets.NewFilteredTable()
	filteredTable.SetupHeaders([]string{"Metric", "Min", "Spark line", "Max"})

	go func() {
		// Get available metric columns
		columnNameRows, columnNameErr := a.clickHouse.Query(asyncMetricLogColumnsQuery)
		if columnNameErr != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Error getting async metric columns: %v", columnNameErr))
			})
			return
		}
		defer func() {
			if closeErr := columnNameRows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close columnNameRows")
			}
		}()

		var columns []string
		maxNameLen := 0
		for columnNameRows.Next() {
			var name string
			if err := columnNameRows.Scan(&name); err != nil {
				a.tviewApp.QueueUpdateDraw(func() {
					a.mainView.SetText(fmt.Sprintf("Error scanning column name: %v", err))
				})
				return
			}
			columns = append(columns, name)
			if len(name) > maxNameLen {
				maxNameLen = len(name)
			}
		}

		if rowErr := columnNameRows.Err(); rowErr != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Error reading columns: %v", rowErr))
			})
			return
		}

		// Calculate buckets based on full screen width
		_, _, width, _ := a.mainFlex.GetRect()
		buckets := width - 15 - maxNameLen
		if buckets < 10 {
			buckets = 10
		}

		// Execute query for each metric
		for _, metric := range columns {
			query := fmt.Sprintf(`
SELECT name, lttb(%d)(event_time,value) 
FROM cluster('%s', merge(system,'asynchronous_metric_log'))
WHERE event_date >= toDate(parseDateTimeBestEffort('%s')) 
  AND event_date <= toDate(parseDateTimeBestEffort('%s'))
  AND event_time >= parseDateTimeBestEffort('%s') 
  AND event_time <= parseDateTimeBestEffort('%s')
GROUP BY name`,
				buckets, cluster, fromStr, toStr, fromStr, toStr)

			rows, err := a.clickHouse.Query(query)
			if err != nil {
				a.tviewApp.QueueUpdateDraw(func() {
					a.mainView.SetText(fmt.Sprintf("Error executing query for %s: %v", metric, err))
				})
				continue
			}

			// Process results
			var (
				name      string
				timeValue [][]interface{}
			)
			if rows.Next() {
				if err := rows.Scan(&name, &timeValue); err != nil {
					rows.Close()
					continue
				}

				// Extract values
				var values []float64
				for _, tv := range timeValue {
					if len(tv) >= 2 {
						if val, ok := tv[1].(float64); ok {
							values = append(values, val)
						}
					}
				}

				if len(values) > 0 {
					minVal := values[0]
					maxVal := values[0]
					for _, v := range values {
						if v < minVal {
							minVal = v
						}
						if v > maxVal {
							maxVal = v
						}
					}

					sparkline := widgets.GenerateSparkline(values)
					color := tcell.ColorWhite
					if maxVal > 2*minVal {
						color = tcell.ColorYellow
					}
					if maxVal > 4*minVal {
						color = tcell.ColorRed
					}

					filteredTable.AddRow([]*tview.TableCell{
						tview.NewTableCell(name).
							SetTextColor(color).
							SetAlign(tview.AlignLeft),
						tview.NewTableCell(fmt.Sprintf("%.1f", minVal)).
							SetTextColor(color).
							SetAlign(tview.AlignRight),
						tview.NewTableCell(sparkline).
							SetTextColor(color).
							SetAlign(tview.AlignLeft),
						tview.NewTableCell(fmt.Sprintf("%.1f", maxVal)).
							SetTextColor(color).
							SetAlign(tview.AlignLeft),
					})
				}
			}
			rows.Close()
		}

		// Create table to display results
		a.tviewApp.QueueUpdateDraw(func() {
			filteredTable.Table.SetTitle(fmt.Sprintf("Asynchronous Metric Log: %s to %s", fromStr, toStr)).
				SetBorder(true)

			filteredTable.Table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
				if event.Key() == tcell.KeyEscape {
					a.pages.SwitchToPage("main")
					return nil
				}
				if filterHandler := filteredTable.GetInputCapture(a.tviewApp, a.pages); filterHandler(event) == nil {
					return nil
				}
				if event.Key() == tcell.KeyEnter {
					row, _ := filteredTable.Table.GetSelection()
					if row > 0 { // Skip header row
						metricName := filteredTable.Table.GetCell(row, 0).Text
						go a.showAsyncMetricDescription(metricName)
					}
					return nil
				}
				return event
			})

			a.pages.AddPage("async_metric_log", filteredTable.Table, true, true)
			a.pages.SwitchToPage("async_metric_log")
		})
	}()
}

func (a *App) showAsyncMetricDescription(metricName string) {
	query := fmt.Sprintf("SELECT description FROM system.asynchronous_metrics WHERE name='%s'", metricName)

	rows, err := a.clickHouse.Query(query)
	if err != nil {
		a.tviewApp.QueueUpdateDraw(func() {
			a.mainView.SetText(fmt.Sprintf("Error getting metric description: %v", err))
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

	widgets.ShowDescription(a.tviewApp, a.pages, metricName, description)
}
