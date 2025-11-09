package tui

import (
	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	tea "github.com/charmbracelet/bubbletea"
)

// getMetricSQL returns the SQL expression for the given metric
func getMetricSQL(metric HeatmapMetric) string {
	switch metric {
	case MetricCount:
		return "count()"
	case MetricMemoryUsage:
		return "sum(memory_usage)"
	case MetricCPUUsage:
		return "sum(ProfileEvents['OSCPUVirtualTimeMicroseconds'])"
	case MetricNetworkSent:
		return "sum(ProfileEvents['NetworkSendBytes'])"
	case MetricNetworkReceive:
		return "sum(ProfileEvents['NetworkReceiveBytes'])"
	case MetricReadRows:
		return "sum(read_rows)"
	case MetricWrittenRows:
		return "sum(written_rows)"
	case MetricReadBytes:
		return "sum(read_bytes)"
	case MetricWrittenBytes:
		return "sum(written_bytes)"
	default:
		return "count()"
	}
}

// getMetricName returns a human-readable name for the metric
func getMetricName(metric HeatmapMetric) string {
	switch metric {
	case MetricCount:
		return "Query Count"
	case MetricMemoryUsage:
		return "Memory Usage"
	case MetricCPUUsage:
		return "CPU Usage"
	case MetricNetworkSent:
		return "Network Sent"
	case MetricNetworkReceive:
		return "Network Received"
	case MetricReadRows:
		return "Read Rows"
	case MetricWrittenRows:
		return "Written Rows"
	case MetricReadBytes:
		return "Read Bytes"
	case MetricWrittenBytes:
		return "Written Bytes"
	default:
		return "Count"
	}
}

// MetricSelectedMsg is sent when a metric is selected
type MetricSelectedMsg struct {
	Metric HeatmapMetric
	Name   string
}

// metricSelector is a bubbletea model for selecting metric type
type metricSelector struct {
	list    widgets.FilteredList
	metrics []HeatmapMetric
	names   []string
}

func newMetricSelector(width, height int) metricSelector {
	metrics := []HeatmapMetric{
		MetricCount,
		MetricMemoryUsage,
		MetricCPUUsage,
		MetricNetworkSent,
		MetricNetworkReceive,
		MetricReadRows,
		MetricWrittenRows,
		MetricReadBytes,
		MetricWrittenBytes,
	}

	names := []string{
		"Query Count",
		"Memory Usage",
		"CPU Usage",
		"Network Sent",
		"Network Received",
		"Read Rows",
		"Written Rows",
		"Read Bytes",
		"Written Bytes",
	}

	listModel := widgets.NewFilteredList("Select Metric", names, width, height)

	return metricSelector{
		list:    listModel,
		metrics: metrics,
		names:   names,
	}
}

func (m metricSelector) Init() tea.Cmd {
	return nil
}

func (m metricSelector) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "enter":
			// Get selected metric
			selectedIdx := m.list.SelectedIndex()
			if selectedIdx >= 0 && selectedIdx < len(m.metrics) {
				return m, func() tea.Msg {
					return MetricSelectedMsg{
						Metric: m.metrics[selectedIdx],
						Name:   m.names[selectedIdx],
					}
				}
			}
		case "esc", "q":
			// Return to main - parent will handle this
			return m, nil
		}
	}

	// Delegate to list
	m.list, cmd = m.list.Update(msg)
	return m, cmd
}

func (m metricSelector) View() string {
	return m.list.View()
}

// showMetricSelector displays a list of available metrics
func (a *App) showMetricSelector() {
	// Create bubbletea metric selector
	selector := newMetricSelector(a.width, a.height)
	a.metricHandler = selector
	a.currentPage = pageMetric
}
