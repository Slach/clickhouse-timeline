package tui

import (
	"fmt"
	"time"

	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/evertras/bubble-table/table"
	"github.com/rs/zerolog/log"
)

const (
	asyncMetricLogColumnsQuery = `SELECT DISTINCT metric FROM clusterAllReplicas('%s',merge(system, '^asynchronous_metrics'))`
)

// AsyncMetricLogDataMsg is sent when async metric log data is loaded
type AsyncMetricLogDataMsg struct {
	Rows            []table.Row
	Title           string
	MetricNameWidth int
	MinWidth        int
	MaxWidth        int
	Err             error
}

// asyncMetricLogViewer is a bubbletea model for async metric log display
type asyncMetricLogViewer struct {
	table   widgets.FilteredTable
	loading bool
	err     error
	width   int
	height  int
}

func newAsyncMetricLogViewer(width, height int) asyncMetricLogViewer {
	// Use default widths initially, will be updated when data is loaded
	tableModel := widgets.NewFilteredTable(
		"Asynchronous Metric Log",
		[]string{"Metric", "Min", "Spark line", "Max"},
		width,
		height-4,
	)

	return asyncMetricLogViewer{
		table:   tableModel,
		loading: true,
		width:   width,
		height:  height,
	}
}

func (m asyncMetricLogViewer) Init() tea.Cmd {
	return nil
}

func (m asyncMetricLogViewer) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case AsyncMetricLogDataMsg:
		m.loading = false
		if msg.Err != nil {
			m.err = msg.Err
			return m, nil
		}

		// Calculate appropriate column widths
		// Account for borders and padding (~12 chars total)
		metricWidth := msg.MetricNameWidth
		if metricWidth < 15 {
			metricWidth = 15
		}
		minWidth := msg.MinWidth
		if minWidth < 8 {
			minWidth = 8 // Ensure at least "Min" header + padding
		}
		maxWidth := msg.MaxWidth
		if maxWidth < 8 {
			maxWidth = 8 // Ensure at least "Max" header + padding
		}
		sparklineWidth := m.width - metricWidth - minWidth - maxWidth - 12
		if sparklineWidth < 10 {
			sparklineWidth = 10
		}

		// Update table with data and custom widths
		m.table = widgets.NewFilteredTableBubbleWithWidths(
			msg.Title,
			[]string{"Metric", "Min", "Spark line", "Max"},
			[]int{metricWidth, minWidth, sparklineWidth, maxWidth},
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

func (m asyncMetricLogViewer) View() string {
	if m.loading {
		return "Loading asynchronous_metric_log data, please wait..."
	}
	if m.err != nil {
		return fmt.Sprintf("Error loading async metric log: %v\n\nPress ESC to return", m.err)
	}
	return m.table.View()
}

// ShowAsynchronousMetricLog displays async metric log data
func (a *App) ShowAsynchronousMetricLog(fromTime, toTime time.Time, cluster string) tea.Cmd {
	if a.state.ClickHouse == nil {
		a.SwitchToMainPage("Error: Please connect to a ClickHouse instance first using :connect command")
		return nil
	}
	if cluster == "" {
		a.SwitchToMainPage("Error: Please select a cluster first using :cluster command")
		return nil
	}

	// Create and show viewer
	viewer := newAsyncMetricLogViewer(a.width, a.height)
	a.asyncMetricHandler = viewer
	a.currentPage = pageAsyncMetricLog

	// Start async data fetch
	return a.fetchAsyncMetricLogDataCmd(fromTime, toTime, cluster)
}

// fetchAsyncMetricLogDataCmd fetches async metric log data from ClickHouse
func (a *App) fetchAsyncMetricLogDataCmd(fromTime, toTime time.Time, cluster string) tea.Cmd {
	return func() tea.Msg {
		fromStr := fromTime.Format("2006-01-02 15:04:05 -07:00")
		toStr := toTime.Format("2006-01-02 15:04:05 -07:00")

		// Get available metric fields
		columnNameRows, columnNameErr := a.state.ClickHouse.Query(fmt.Sprintf(asyncMetricLogColumnsQuery, cluster))
		if columnNameErr != nil {
			return AsyncMetricLogDataMsg{Err: fmt.Errorf("error getting async metric fields: %v", columnNameErr)}
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
				return AsyncMetricLogDataMsg{Err: fmt.Errorf("error scanning column name: %v", err)}
			}
			asyncMetricFields = append(asyncMetricFields, name)
			if len(name) > maxNameLen {
				maxNameLen = len(name)
			}
		}

		if rowErr := columnNameRows.Err(); rowErr != nil {
			return AsyncMetricLogDataMsg{Err: fmt.Errorf("error reading fields: %v", rowErr)}
		}

		// Calculate buckets for sparkline based on estimated column widths
		// Metric: maxNameLen, Min: estimate 15, Max: estimate 15, Borders/padding: ~12
		metricWidth := maxNameLen
		if metricWidth < 15 {
			metricWidth = 15
		}
		// Estimate generous widths for numeric columns to avoid truncation
		estimatedMinWidth := 15
		estimatedMaxWidth := 15
		sparklineWidth := a.width - metricWidth - estimatedMinWidth - estimatedMaxWidth - 12
		if sparklineWidth < 10 {
			sparklineWidth = 10
		}
		buckets := sparklineWidth

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

		// Use the new bubbletea sparkline helper
		sparklineData, err := a.ExecuteAndProcessSparklineQueryBubble(query, "AsynchronousMetric", asyncMetricFields)
		if err != nil {
			return AsyncMetricLogDataMsg{Err: err}
		}

		// Convert sparkline data to bubble-table rows and calculate column widths
		var tableRows []table.Row
		maxMinWidth := 3 // Start with header "Min" length
		maxMaxWidth := 3 // Start with header "Max" length

		for _, item := range sparklineData {
			minStr := fmt.Sprintf("%.1f", item.MinValue)
			maxStr := fmt.Sprintf("%.1f", item.MaxValue)

			// Track maximum widths
			if len(minStr) > maxMinWidth {
				maxMinWidth = len(minStr)
			}
			if len(maxStr) > maxMaxWidth {
				maxMaxWidth = len(maxStr)
			}

			rowData := table.RowData{
				"Metric":     item.Name,
				"Min":        minStr,
				"Spark line": item.Sparkline,
				"Max":        maxStr,
			}
			tableRows = append(tableRows, table.NewRow(rowData))
		}

		title := fmt.Sprintf("Asynchronous Metric Log: %s to %s", fromStr, toStr)
		return AsyncMetricLogDataMsg{
			Rows:            tableRows,
			Title:           title,
			MetricNameWidth: metricWidth,
			MinWidth:        maxMinWidth + 2, // Add padding
			MaxWidth:        maxMaxWidth + 2, // Add padding
		}
	}
}
