package tui

import (
	"fmt"
	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"strings"
	"time"
)

func (a *App) ExecuteAndProcessSparklineQuery(query string, prefix string, fields []string, filteredTable *widgets.FilteredTable, row *int) error {
	rows, err := a.clickHouse.Query(query)
	if err != nil {
		return fmt.Errorf("error executing %s query: %v", prefix, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			a.mainView.SetText(fmt.Sprintf("can't close %s rows", prefix))
		}
	}()

	// Store results for display
	results := make(map[string][]float64)
	for rows.Next() {
		switch prefix {
		case "AsynchronousMetric":
			// Handle metrics which returns name, array(tuple(time,value))
			var name string
			var timeValue [][]interface{}

			if scanErr := rows.Scan(&name, &timeValue); scanErr != nil {
				return fmt.Errorf("error scanning %s row: %v", prefix, scanErr)
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
			alias := "A_" + name
			results[alias] = values
		case "CurrentMetric":
			// Handle metrics which returns multiple array(tuple(time,value)) for each field
			valuePtrs := make([]interface{}, len(fields))
			values := make([]*[][]interface{}, len(fields))
			for i := range values {
				values[i] = new([][]interface{})
				valuePtrs[i] = values[i]
			}

			if scanErr := rows.Scan(valuePtrs...); scanErr != nil {
				return fmt.Errorf("error scanning %s row: %v", prefix, scanErr)
			}

			for i, field := range fields {
				alias := "M_" + strings.TrimPrefix(field, prefix+"_")
				for _, tuple := range *values[i] {
					if len(tuple) >= 2 {
						if val, ok := tuple[1].(float64); ok {
							results[alias] = append(results[alias], val)
						}
					}
				}
			}

		case "ProfileEvent":
			// Handle ProfileEvent which returns bucket_time + multiple sum()
			values := make([]float64, len(fields))
			valuePtrs := make([]interface{}, len(fields)+1)
			for i := range values {
				valuePtrs[i+1] = &values[i]
			}
			var bucketTime time.Time
			valuePtrs[0] = &bucketTime
			if scanErr := rows.Scan(valuePtrs...); scanErr != nil {
				return fmt.Errorf("error scanning %s row: %v", prefix, scanErr)
			}

			for i, field := range fields {
				alias := "P_" + strings.TrimPrefix(field, prefix+"_")
				results[alias] = append(results[alias], values[i])
			}
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error reading %s rows: %v", prefix, err)
	}

	// Add results to display table
	for name, values := range results {
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

		sparkline := a.GenerateSparkline(values)
		color := tcell.ColorWhite
		if maxVal > 2*minVal {
			color = tcell.ColorYellow
		}
		if maxVal > 4*minVal {
			color = tcell.ColorRed
		}

		displayName := name
		if prefix != "AsynchronousMetric" {
			displayName = strings.TrimPrefix(name, prefix+"_")
		}

		filteredTable.AddRow([]*tview.TableCell{
			tview.NewTableCell(displayName).
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

		*row++
	}
	return nil
}

func (a *App) GenerateSparkline(values []float64) string {
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

func (a *App) ShowDescription(name, description string) {
	a.tviewApp.QueueUpdateDraw(func() {
		modal := tview.NewModal().
			SetText(fmt.Sprintf("[yellow]%s[-]\n\n%s", name, description)).
			AddButtons([]string{"OK"}).
			SetDoneFunc(func(buttonIndex int, buttonLabel string) {
				a.pages.HidePage("metric_desc")
			})

		a.pages.AddPage("metric_desc", modal, true, true)
	})
}
