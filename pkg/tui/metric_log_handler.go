package tui

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const (
	metricLogColumnsQuery = `
SELECT DISTINCT name 
FROM system.columns 
WHERE database='system' 
  AND table LIKE 'metric_log%%' 
  AND (name LIKE 'Current%%' OR name LIKE 'Profile%%')`
)

func (a *App) executeAndProcessQuery(query string, fields []string, prefix string, buckets int, table *tview.Table, row *int) error {
	rows, err := a.clickHouse.Query(query)
	if err != nil {
		return fmt.Errorf("error executing %s query: %v", prefix, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			a.mainView.SetText(fmt.Sprintf("can't close metric_log.%s rows", prefix))
		}
	}()

	// Store results for display
	results := make(map[string][]float64)
	for rows.Next() {
		if prefix == "CurrentMetrics" {
			// Handle CurrentMetrics which returns array(tuple(time,value))
			values := make([][]interface{}, len(fields))
			valuePtrs := make([]interface{}, len(fields))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				return fmt.Errorf("error scanning %s row: %v", prefix, err)
			}

			for i, field := range fields {
				alias := strings.TrimPrefix(field, prefix+"_")
				for _, tuple := range values[i] {
					if tupleSlice, ok := tuple.([]interface{}); ok && len(tupleSlice) >= 2 {
						if val, ok := tupleSlice[1].(float64); ok {
							results[alias] = append(results[alias], val)
						}
					}
				}
			}
		} else {
			// Handle ProfileEvents which returns direct values
			values := make([]float64, len(fields))
			valuePtrs := make([]interface{}, len(fields))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				return fmt.Errorf("error scanning %s row: %v", prefix, err)
			}

			for i, field := range fields {
				alias := strings.TrimPrefix(field, prefix+"_")
				results[alias] = append(results[alias], values[i])
			}
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error reading %s rows: %v", prefix, err)
	}

	// Add results to display table
	for field, values := range results {
		if len(values) == 0 {
			continue
		}

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

		sparkline := generateSparkline(values)
		color := tcell.ColorWhite
		if maxVal > 2*minVal {
			color = tcell.ColorYellow
		}
		if maxVal > 4*minVal {
			color = tcell.ColorRed
		}

		table.SetCell(*row, 0, tview.NewTableCell(field).
			SetTextColor(color).
			SetAlign(tview.AlignLeft))
		table.SetCell(*row, 1, tview.NewTableCell(fmt.Sprintf("%.1f", minVal)).
			SetTextColor(color).
			SetAlign(tview.AlignRight))
		table.SetCell(*row, 2, tview.NewTableCell(sparkline).
			SetTextColor(color).
			SetAlign(tview.AlignLeft))
		table.SetCell(*row, 3, tview.NewTableCell(fmt.Sprintf("%.1f", maxVal)).
			SetTextColor(color).
			SetAlign(tview.AlignRight))

		*row++
	}
	return nil
}

func (a *App) ShowMetricLog(fromTime, toTime time.Time, cluster string) {
	if a.clickHouse == nil {
		a.mainView.SetText("Error: Please connect to a ClickHouse instance first")
		return
	}

	if cluster == "" {
		a.mainView.SetText("Error: Please select a cluster first using :cluster command")
		return
	}

	a.mainView.SetText("Loading metric_log data, please wait...")

	// Format time strings for queries
	fromStr := fromTime.Format("2006-01-02 15:04:05 -07:00")
	toStr := toTime.Format("2006-01-02 15:04:05 -07:00")

	table := tview.NewTable().
		SetBorders(false).
		SetSelectable(true, true)

	// Set headers
	headers := []string{"Metric", "Min", "Trend", "Max"}
	for col, header := range headers {
		table.SetCell(0, col,
			tview.NewTableCell(header).
				SetTextColor(tcell.ColorYellow).
				SetAlign(tview.AlignCenter),
		)
	}

	go func() {
		// Get available metric columns
		columnNameRows, columnNameErr := a.clickHouse.Query(metricLogColumnsQuery)
		if columnNameErr != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Error getting metric columns: %v", columnNameErr))
			})
			return
		}
		defer columnNameRows.Close()

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

		// Calculate buckets based on screen width
		_, _, width, _ := a.mainView.GetRect()
		buckets := width - 15 - maxNameLen
		if buckets < 10 {
			buckets = 10
		}

		// Calculate time interval in seconds
		interval := int(math.Ceil(float64(toTime.Sub(fromTime).Seconds()) / float64(buckets)))

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
		// Execute CurrentMetrics query
		if len(currentFields) > 0 {
			var selectParts []string
			for _, field := range currentFields {
				alias := strings.TrimPrefix(field, "CurrentMetrics_")
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

			err := a.executeAndProcessQuery(query, currentFields, "CurrentMetrics", buckets, table, &row)
			if err != nil {
				a.tviewApp.QueueUpdateDraw(func() {
					a.mainView.SetText(err.Error())
				})
				return
			}
		}

		// Execute ProfileEvents query
		if len(profileFields) > 0 {
			var selectParts []string
			for _, field := range profileFields {
				alias := strings.TrimPrefix(field, "ProfileEvents_")
				selectParts = append(selectParts, fmt.Sprintf("sum(%s) AS %s", field, strings.ToLower(alias)))
			}

			query := fmt.Sprintf(`
SELECT 
    toStartOfInterval(event_time, INTERVAL %d SECOND) AS bucket_time,
    %s
FROM clusterAllReplicas('%s', merge(system,'^metric_log'))
WHERE event_date >= toDate(parseDateTimeBestEffort('%s')) 
  AND event_date <= toDate(parseDateTimeBestEffort('%s'))
  AND event_time >= parseDateTimeBestEffort('%s')) 
  AND event_time <= parseDateTimeBestEffort('%s'))
GROUP BY bucket_time
ORDER BY bucket_time`,
				interval,
				strings.Join(selectParts, ", "),
				cluster,
				fromStr, toStr, fromStr, toStr)

			err := a.executeAndProcessQuery(query, profileFields, "ProfileEvents", buckets, table, &row)
			if err != nil {
				a.tviewApp.QueueUpdateDraw(func() {
					a.mainView.SetText(err.Error())
				})
				return
			}
		}

		// Create table to display results
		a.tviewApp.QueueUpdateDraw(func() {
			// Add data columnNameRows with sparklines
			row := 1
			for _, field := range currentFields {
				// Get min/max values and generate sparkline
				values := []float64{1, 2, 3, 4, 5} // TODO: Replace with actual query results
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

				sparkline := generateSparkline(values)
				alias := strings.TrimPrefix(field, "CurrentMetrics_")

				// Set cell colors based on value ranges
				color := tcell.ColorWhite
				if maxVal > 2*minVal {
					color = tcell.ColorYellow
				}
				if maxVal > 4*minVal {
					color = tcell.ColorRed
				}

				// Add row to table
				table.SetCell(row, 0, tview.NewTableCell(alias).
					SetTextColor(color).
					SetAlign(tview.AlignLeft))
				table.SetCell(row, 1, tview.NewTableCell(fmt.Sprintf("%.1f", minVal)).
					SetTextColor(color).
					SetAlign(tview.AlignRight))
				table.SetCell(row, 2, tview.NewTableCell(sparkline).
					SetTextColor(color).
					SetAlign(tview.AlignLeft))
				table.SetCell(row, 3, tview.NewTableCell(fmt.Sprintf("%.1f", maxVal)).
					SetTextColor(color).
					SetAlign(tview.AlignRight))

				row++
			}

			table.SetTitle(fmt.Sprintf("Metric Log: %s to %s", fromStr, toStr)).
				SetBorder(true)

			table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
				if event.Key() == tcell.KeyEscape {
					a.pages.SwitchToPage("main")
					return nil
				}
				return event
			})

			a.pages.AddPage("metric_log", table, true, true)
			a.pages.SwitchToPage("metric_log")
		})
	}()
}

// Helper function to generate ASCII sparkline
func generateSparkline(values []float64) string {
	if len(values) == 0 {
		return ""
	}

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

	rangeVal := maxVal - minVal
	if rangeVal == 0 {
		rangeVal = 1
	}

	sparks := []rune("▁▂▃▄▅▆▇█")
	var result strings.Builder
	for _, v := range values {
		pos := int(((v - minVal) / rangeVal) * float64(len(sparks)-1))
		if pos < 0 {
			pos = 0
		}
		if pos >= len(sparks) {
			pos = len(sparks) - 1
		}
		result.WriteRune(sparks[pos])
	}
	return result.String()
}
