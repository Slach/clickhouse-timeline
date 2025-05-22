package tui

import (
	"fmt"
	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	"github.com/gdamore/tcell/v2"
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
		// Get available metric asyncMetricFields
		columnNameRows, columnNameErr := a.clickHouse.Query(asyncMetricLogColumnsQuery)
		if columnNameErr != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Error getting async metric asyncMetricFields: %v", columnNameErr))
			})
			return
		}
		defer func() {
			if closeErr := columnNameRows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close columnNameRows")
			}
		}()

		var asyncMetricFields []string
		maxNameLen := 0
		for columnNameRows.Next() {
			var name string
			if err := columnNameRows.Scan(&name); err != nil {
				a.tviewApp.QueueUpdateDraw(func() {
					a.mainView.SetText(fmt.Sprintf("Error scanning column name: %v", err))
				})
				return
			}
			asyncMetricFields = append(asyncMetricFields, name)
			if len(name) > maxNameLen {
				maxNameLen = len(name)
			}
		}

		if rowErr := columnNameRows.Err(); rowErr != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Error reading asyncMetricFields: %v", rowErr))
			})
			return
		}

		// Calculate buckets based on full screen width
		_, _, width, _ := a.mainFlex.GetRect()
		buckets := width - 15 - maxNameLen
		if buckets < 10 {
			buckets = 10
		}

		// Execute single query for all metrics
		query := fmt.Sprintf(`
SELECT name, lttb(%d)(event_time,value) 
FROM cluster('%s', merge(system,'asynchronous_metric_log'))
WHERE event_date >= toDate(parseDateTimeBestEffort('%s')) 
  AND event_date <= toDate(parseDateTimeBestEffort('%s'))
  AND event_time >= parseDateTimeBestEffort('%s') 
  AND event_time <= parseDateTimeBestEffort('%s')
GROUP BY name`,
			buckets, cluster, fromStr, toStr, fromStr, toStr)

		row := 1
		if err := a.ExecuteAndProcessSparklineQuery(query, "AsynchronousMetric", asyncMetricFields, filteredTable, &row); err != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(err.Error())
			})
			return
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
						go a.showMetricDescription(metricName)
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
