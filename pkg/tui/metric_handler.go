package tui

import (
	"fmt"
	"github.com/rivo/tview"
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

// showMetricSelector displays a list of available metrics
func (a *App) showMetricSelector() {
	metricList := tview.NewList()
	metricList.SetTitle("Select Metric")
	metricList.SetBorder(true)

	metrics := []struct {
		name   string
		metric HeatmapMetric
	}{
		{"Query Count", MetricCount},
		{"Memory Usage", MetricMemoryUsage},
		{"CPU Usage", MetricCPUUsage},
		{"Network Sent", MetricNetworkSent},
		{"Network Received", MetricNetworkReceive},
		{"Read Rows", MetricReadRows},
		{"Written Rows", MetricWrittenRows},
		{"Read Bytes", MetricReadBytes},
		{"Written Bytes", MetricWrittenBytes},
	}

	for i, m := range metrics {
		metricList.AddItem(m.name, "", rune('1'+i), nil)
	}

	metricList.SetSelectedFunc(func(i int, _ string, _ string, _ rune) {
		a.currentMetric = metrics[i].metric
		a.mainView.SetText(fmt.Sprintf("Metric set to: %s", metrics[i].name))
		a.pages.SwitchToPage("main")

		// If we already have a heatmap, regenerate it with the new metric
		if a.heatmapTable != nil {
			a.ShowHeatmap()
		}
	})

	a.metricList = metricList
	a.pages.AddPage("metrics", metricList, true, true)
	a.pages.SwitchToPage("metrics")
}
