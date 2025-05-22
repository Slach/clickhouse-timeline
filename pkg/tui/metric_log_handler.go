package tui

import (
	"fmt"
	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	"github.com/rs/zerolog/log"
	"math"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
)

const (
	metricLogColumnsQuery = `
SELECT DISTINCT name 
FROM system.columns 
WHERE database='system' 
  AND table LIKE 'metric_log%' 
  AND type NOT LIKE 'Date%'`
)

func (a *App) ShowMetricLog(fromTime, toTime time.Time, cluster string) {
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

	a.mainView.SetText("Loading metric_log data, please wait...")

	// Format time strings for queries
	fromStr := fromTime.Format("2006-01-02 15:04:05 -07:00")
	toStr := toTime.Format("2006-01-02 15:04:05 -07:00")

	// Create filtered table widget
	filteredTable := widgets.NewFilteredTable()
	filteredTable.SetupHeaders([]string{"Metric", "Min", "Spark line", "Max"})

	go func() {
		// Get available metric columns
		columnNameRows, columnNameErr := a.clickHouse.Query(metricLogColumnsQuery)
		if columnNameErr != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Error getting metric columns: %v", columnNameErr))
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
			name = strings.TrimPrefix(strings.TrimPrefix(name, "CurrentMetric_"), "ProfileEvent_")
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

		// Calculate time interval in seconds
		interval := int(math.Ceil(toTime.Sub(fromTime).Seconds() / float64(buckets)))

		// Build field lists
		var currentFields []string
		var profileFields []string
		for _, col := range columns {
			if strings.HasPrefix(col, "Current") {
				currentFields = append(currentFields, col)
			} else if strings.HasPrefix(col, "Profile") {
				profileFields = append(profileFields, col)
			}
		}
		row := 1
		// Execute CurrentMetric query
		if len(currentFields) > 0 {
			var selectParts []string
			for _, field := range currentFields {
				alias := strings.TrimPrefix(field, "CurrentMetric_")
				selectParts = append(selectParts,
					fmt.Sprintf("lttb(%d)(event_time, %s) AS %s", buckets, field, alias))
			}

			query := fmt.Sprintf(`
SELECT %s 
FROM clusterAllReplicas('%s', merge(system,'^metric_log'))
WHERE event_date >= toDate(parseDateTimeBestEffort('%s')) 
  AND event_date <= toDate(parseDateTimeBestEffort('%s'))
  AND event_time >= parseDateTimeBestEffort('%s') 
  AND event_time <= parseDateTimeBestEffort('%s')`,
				strings.Join(selectParts, ", "),
				cluster,
				fromStr, toStr, fromStr, toStr)

			err := a.ExecuteAndProcessSparklineQuery(query, "CurrentMetric", currentFields, filteredTable, &row)
			if err != nil {
				a.tviewApp.QueueUpdateDraw(func() {
					a.mainView.SetText(err.Error())
				})
				return
			}
		}

		// Execute ProfileEvent query
		if len(profileFields) > 0 {
			var selectParts []string
			for _, field := range profileFields {
				alias := strings.TrimPrefix(field, "ProfileEvent_")
				selectParts = append(selectParts, fmt.Sprintf("sum(%s) AS %s", field, strings.ToLower(alias)))
			}

			query := fmt.Sprintf(`
SELECT 
    toStartOfInterval(event_time, INTERVAL %d SECOND) AS bucket_time,
    %s
FROM clusterAllReplicas('%s', merge(system,'^metric_log'))
WHERE event_date >= toDate(parseDateTimeBestEffort('%s')) 
  AND event_date <= toDate(parseDateTimeBestEffort('%s'))
  AND event_time >= parseDateTimeBestEffort('%s') 
  AND event_time <= parseDateTimeBestEffort('%s')
GROUP BY bucket_time
ORDER BY bucket_time`,
				interval,
				strings.Join(selectParts, ", "),
				cluster,
				fromStr, toStr, fromStr, toStr)

			err := a.ExecuteAndProcessSparklineQuery(query, "ProfileEvent", profileFields, filteredTable, &row)
			if err != nil {
				a.tviewApp.QueueUpdateDraw(func() {
					a.mainView.SetText(err.Error())
				})
				return
			}
		}

		// Create table to display results
		a.tviewApp.QueueUpdateDraw(func() {
			filteredTable.Table.SetTitle(fmt.Sprintf("Metric Log: %s to %s", fromStr, toStr)).
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
					currentRow, currentCol := filteredTable.Table.GetSelection()
					if currentRow > 0 && currentCol == 0 {
						metricName := filteredTable.Table.GetCell(row, 0).Text
						go a.showMetricDescription(metricName)
					}
					return nil
				}
				return event
			})

			a.pages.AddPage("metric_log", filteredTable.Table, true, true)
			a.pages.SwitchToPage("metric_log")
		})
	}()
}
