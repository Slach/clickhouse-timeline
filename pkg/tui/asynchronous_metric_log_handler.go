package tui

import (
	"fmt"
	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	"github.com/gdamore/tcell/v2"
	"github.com/rs/zerolog/log"
	"time"
)

const (
	asyncMetricLogColumnsQuery = `SELECT DISTINCT metric FROM clusterAllReplicas('%s',merge(system, '^asynchronous_metrics'))`
)

func (a *App) ShowAsynchronousMetricLog(fromTime, toTime time.Time, cluster string) {
	if a.clickHouse == nil {
		a.SwitchToMainPage("Error: Please connect to a ClickHouse instance first using :connect command")
		return
	}
	if cluster == "" {
		a.SwitchToMainPage("Error: Please select a cluster first using :cluster command")
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
		columnNameRows, columnNameErr := a.clickHouse.Query(fmt.Sprintf(asyncMetricLogColumnsQuery, cluster))
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
SELECT metric, lttb(%d)(event_time,value) 
FROM clusterAllReplicas('%s', merge(system,'asynchronous_metric_log'))
WHERE event_date >= toDate(parseDateTimeBestEffort('%s')) 
  AND event_date <= toDate(parseDateTimeBestEffort('%s'))
  AND event_time >= parseDateTimeBestEffort('%s') 
  AND event_time <= parseDateTimeBestEffort('%s')
GROUP BY metric`,
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
					a.SwitchToMainPage("returned from :asynchronous_metric_log")
					return nil
				}
				if filterHandler := filteredTable.GetInputCapture(a.tviewApp, a.pages); filterHandler(event) == nil {
					return nil
				}
				if event.Key() == tcell.KeyEnter {
					currentRow, _ := filteredTable.Table.GetSelection()
					if currentRow > 0 {
						metricName := filteredTable.Table.GetCell(currentRow, 0).Text
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
