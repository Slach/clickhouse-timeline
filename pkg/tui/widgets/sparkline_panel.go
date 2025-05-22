package widgets

import (
	"fmt"
	"github.com/rivo/tview"
	"strings"
)

func ExecuteAndProcessQuery(query string, prefix string, filteredTable *FilteredTable, row *int) error {
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
		case "CurrentMetric", "AsynchronousMetric":
			// Handle metrics which returns array(tuple(time,value))
			var name string
			var timeValue [][]interface{}
			
			if err := rows.Scan(&name, &timeValue); err != nil {
				return fmt.Errorf("error scanning %s row: %v", prefix, err)
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
			results[name] = values

		case "ProfileEvent":
			// Handle ProfileEvent which returns direct values
			var bucketTime time.Time
			values := make([]float64, len(fields))
			valuePtrs := make([]interface{}, len(fields)+1)
			for i := range values {
				valuePtrs[i+1] = &values[i]
			}
			valuePtrs[0] = &bucketTime
			
			if err := rows.Scan(valuePtrs...); err != nil {
				return fmt.Errorf("error scanning %s row: %v", prefix, err)
			}

			for i, field := range fields {
				results[field] = append(results[field], values[i])
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

		sparkline := GenerateSparkline(values)
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

func GenerateSparkline(values []float64) string {
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

func ShowDescription(app *tview.Application, pages *tview.Pages, name, description string) {
	app.QueueUpdateDraw(func() {
		modal := tview.NewModal().
			SetText(fmt.Sprintf("[yellow]%s[-]\n\n%s", name, description)).
			AddButtons([]string{"OK"}).
			SetDoneFunc(func(buttonIndex int, buttonLabel string) {
				pages.HidePage("metric_desc")
			})

		pages.AddPage("metric_desc", modal, true, true)
	})
}
