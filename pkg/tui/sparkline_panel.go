package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/evertras/bubble-table/table"
)

// SparklineRowData represents a single sparkline row for display
type SparklineRowData struct {
	Name      string
	MinValue  float64
	MaxValue  float64
	Values    []float64
	Sparkline string
	Color     string // lipgloss color
}

// SparklineResultMsg is sent when sparkline query completes
type SparklineResultMsg struct {
	Rows []SparklineRowData
	Err  error
}

// ExecuteAndProcessSparklineQueryBubble executes a query and returns sparkline data for bubbletea
func (a *App) ExecuteAndProcessSparklineQueryBubble(query string, prefix string, fields []string) ([]SparklineRowData, error) {
	rows, err := a.state.ClickHouse.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error executing %s query: %v", prefix, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			// Log error but don't fail
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
				return nil, fmt.Errorf("error scanning %s row: %v", prefix, scanErr)
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
				return nil, fmt.Errorf("error scanning %s row: %v", prefix, scanErr)
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
				return nil, fmt.Errorf("error scanning %s row: %v", prefix, scanErr)
			}

			for i, field := range fields {
				alias := "P_" + strings.TrimPrefix(field, prefix+"_")
				results[alias] = append(results[alias], values[i])
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading %s rows: %v", prefix, err)
	}

	// Convert results to SparklineRowData
	var rowData []SparklineRowData
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

		// Determine color based on variance
		color := "15" // White
		if maxVal > 2*minVal {
			color = "11" // Yellow
		}
		if maxVal > 4*minVal {
			color = "9" // Red
		}

		displayName := name
		if prefix != "AsynchronousMetric" {
			displayName = strings.TrimPrefix(name, prefix+"_")
		}

		rowData = append(rowData, SparklineRowData{
			Name:      displayName,
			MinValue:  minVal,
			MaxValue:  maxVal,
			Values:    values,
			Sparkline: sparkline,
			Color:     color,
		})
	}

	return rowData, nil
}

// GenerateSparkline creates a sparkline string using Unicode characters
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

// ConvertSparklineDataToTableRows converts sparkline row data to bubble-table rows
func ConvertSparklineDataToTableRows(data []SparklineRowData) []table.Row {
	var rows []table.Row

	for _, item := range data {
		rowData := table.RowData{
			"name":      item.Name,
			"min":       fmt.Sprintf("%.1f", item.MinValue),
			"sparkline": item.Sparkline,
			"max":       fmt.Sprintf("%.1f", item.MaxValue),
		}

		row := table.NewRow(rowData).
			WithStyle(lipgloss.NewStyle().Foreground(lipgloss.Color(item.Color)))

		rows = append(rows, row)
	}

	return rows
}
