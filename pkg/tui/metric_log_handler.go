package tui

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/evertras/bubble-table/table"
	"github.com/rs/zerolog/log"
)

const (
	metricLogColumnsQuery = `
SELECT DISTINCT name
FROM system.columns
WHERE database='system'
  AND table LIKE 'metric_log%'
  AND type NOT LIKE 'Date%'`
)

// MetricLogDataMsg is sent when metric log data is loaded
type MetricLogDataMsg struct {
	Rows  []table.Row
	Title string
	Err   error
}

// metricLogViewer is a bubbletea model for metric log display
type metricLogViewer struct {
	table   widgets.FilteredTable
	loading bool
	err     error
	width   int
	height  int
}

func newMetricLogViewer(width, height int) metricLogViewer {
	tableModel := widgets.NewFilteredTable(
		"Metric Log",
		[]string{"Metric", "Min", "Spark line", "Max"},
		width,
		height-4,
	)

	return metricLogViewer{
		table:   tableModel,
		loading: true,
		width:   width,
		height:  height,
	}
}

func (m metricLogViewer) Init() tea.Cmd {
	return nil
}

func (m metricLogViewer) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case MetricLogDataMsg:
		m.loading = false
		if msg.Err != nil {
			m.err = msg.Err
			return m, nil
		}

		// Update table with data
		m.table = widgets.NewFilteredTable(
			msg.Title,
			[]string{"Metric", "Min", "Spark line", "Max"},
			m.width,
			m.height-4,
		)
		m.table.SetRows(msg.Rows)
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "q":
			return m, func() tea.Msg {
				return tea.KeyMsg{Type: tea.KeyEsc}
			}
		case "enter":
			// Get selected metric and show description
			selected := m.table.HighlightedRow()
			if selected.Data != nil {
				if metricName, ok := selected.Data["Metric"].(string); ok {
					// TODO: Show metric description in a modal
					_ = metricName
				}
			}
			return m, nil
		}
	}

	// Delegate to table
	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

func (m metricLogViewer) View() string {
	if m.loading {
		return "Loading metric_log data, please wait..."
	}
	if m.err != nil {
		return fmt.Sprintf("Error loading metric log: %v\n\nPress ESC to return", m.err)
	}
	return m.table.View()
}

// ShowMetricLog displays metric log data
func (a *App) ShowMetricLog(fromTime, toTime time.Time, cluster string) tea.Cmd {
	if a.state.ClickHouse == nil {
		a.SwitchToMainPage("Error: Please connect to a ClickHouse instance first using :connect command")
		return nil
	}
	if cluster == "" {
		a.SwitchToMainPage("Error: Please select a cluster first using :cluster command")
		return nil
	}

	// Create and show viewer
	viewer := newMetricLogViewer(a.width, a.height)
	a.metricLogHandler = viewer
	a.currentPage = pageMetricLog

	// Start async data fetch
	return a.fetchMetricLogDataCmd(fromTime, toTime, cluster)
}

// fetchMetricLogDataCmd fetches metric log data from ClickHouse
func (a *App) fetchMetricLogDataCmd(fromTime, toTime time.Time, cluster string) tea.Cmd {
	return func() tea.Msg {
		fromStr := fromTime.Format("2006-01-02 15:04:05 -07:00")
		toStr := toTime.Format("2006-01-02 15:04:05 -07:00")

		// Get available metric columns
		columnNameRows, columnNameErr := a.state.ClickHouse.Query(metricLogColumnsQuery)
		if columnNameErr != nil {
			return MetricLogDataMsg{Err: fmt.Errorf("error getting metric columns: %v", columnNameErr)}
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
				return MetricLogDataMsg{Err: fmt.Errorf("error scanning column name: %v", err)}
			}
			columns = append(columns, name)
			name = strings.TrimPrefix(strings.TrimPrefix(name, "CurrentMetric_"), "ProfileEvent_")
			if len(name) > maxNameLen {
				maxNameLen = len(name)
			}
		}

		if rowErr := columnNameRows.Err(); rowErr != nil {
			return MetricLogDataMsg{Err: fmt.Errorf("error reading columns: %v", rowErr)}
		}

		// Calculate buckets based on screen width
		buckets := a.width - 15 - maxNameLen
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

		// Collect sparkline data
		var allSparklineData []SparklineRowData

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

			sparklineData, err := a.ExecuteAndProcessSparklineQueryBubble(query, "CurrentMetric", currentFields)
			if err != nil {
				return MetricLogDataMsg{Err: err}
			}
			allSparklineData = append(allSparklineData, sparklineData...)
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

			sparklineData, err := a.ExecuteAndProcessSparklineQueryBubble(query, "ProfileEvent", profileFields)
			if err != nil {
				return MetricLogDataMsg{Err: err}
			}
			allSparklineData = append(allSparklineData, sparklineData...)
		}

		// Convert sparkline data to bubble-table rows
		var tableRows []table.Row
		for _, item := range allSparklineData {
			rowData := table.RowData{
				"Metric":     item.Name,
				"Min":        fmt.Sprintf("%.1f", item.MinValue),
				"Spark line": item.Sparkline,
				"Max":        fmt.Sprintf("%.1f", item.MaxValue),
			}
			tableRows = append(tableRows, table.NewRow(rowData))
		}

		title := fmt.Sprintf("Metric Log: %s to %s", fromStr, toStr)
		return MetricLogDataMsg{
			Rows:  tableRows,
			Title: title,
		}
	}
}
