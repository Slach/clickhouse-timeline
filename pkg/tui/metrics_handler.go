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

	go func() {
		// Get available metric columns
		rows, err := a.clickHouse.Query(metricLogColumnsQuery)
		if err != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Error getting metric columns: %v", err))
			})
			return
		}
		defer rows.Close()

		var columns []string
		maxNameLen := 0
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
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

		if err := rows.Err(); err != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Error reading columns: %v", err))
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

		// Format time strings for queries
		fromStr := fromTime.Format("2006-01-02 15:04:05 -07:00")
		toStr := toTime.Format("2006-01-02 15:04:05 -07:00")

		// Build and execute queries
		var currentFields []string
		var profileFields []string
		for _, col := range columns {
			if strings.HasPrefix(col, "Current") {
				currentFields = append(currentFields, col)
			} else if strings.HasPrefix(col, "Profile") {
				profileFields = append(profileFields, col)
			}
		}

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

			// Execute query and process results...
			// Similar processing for ProfileEvents query...
		}

		// Create table to display results
		a.tviewApp.QueueUpdateDraw(func() {
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

			// Add data rows with sparklines
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

				sparkline := generateSparkline(values, buckets)
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
func generateSparkline(values []float64, width int) string {
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
