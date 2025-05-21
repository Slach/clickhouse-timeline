package tui

// Available commands
const (
	CmdHelp          = "help"
	CmdConnect       = "connect"
	CmdQuit          = "quit"
	CmdFlamegraph    = "flamegraph"
	CmdFrom          = "from"
	CmdTo            = "to"
	CmdRange         = "range"
	CmdHeatmap       = "heatmap"
	CmdCategory      = "category"
	CmdCluster       = "cluster"
	CmdMetric        = "metric"
	CmdScale         = "scale"
	CmdProfileEvents = "profile_events"
	CmdMetricLog     = "metric_log"
)

type TraceType string

const (
	TraceMemory       TraceType = "Memory"
	TraceCPU          TraceType = "CPU"
	TraceReal         TraceType = "Real"
	TraceMemorySample TraceType = "MemorySample"
)

// Heatmap metric types
type HeatmapMetric string

const (
	MetricCount          HeatmapMetric = "count"
	MetricMemoryUsage    HeatmapMetric = "memoryUsage"
	MetricCPUUsage       HeatmapMetric = "cpuUsage"
	MetricNetworkSent    HeatmapMetric = "networkSent"
	MetricNetworkReceive HeatmapMetric = "networkReceive"
	MetricReadRows       HeatmapMetric = "readRows"
	MetricWrittenRows    HeatmapMetric = "writtenRows"
	MetricReadBytes      HeatmapMetric = "readBytes"
	MetricWrittenBytes   HeatmapMetric = "writtenBytes"
)

// Category types for heatmap
type CategoryType string

const (
	CategoryQueryHash CategoryType = "normalized_query_hash"
	CategoryTable     CategoryType = "tables"
	CategoryHost      CategoryType = "hosts"
	CategoryError     CategoryType = "errors"
)

var availableCommands = []string{
	CmdHelp,
	CmdConnect,
	CmdQuit,
	CmdFlamegraph,
	CmdFrom,
	CmdTo,
	CmdRange,
	CmdHeatmap,
	CmdCategory,
	CmdCluster,
	CmdMetric,
	CmdScale,
	CmdProfileEvents,
	CmdMetricLog,
}

// Help text
const helpText = `ClickHouse Timeline Commands:
:help           - Show this help
:connect        - Connect to a ClickHouse instance
:quit           - Exit the application
:flamegraph     - Generate a flamegraph
:from           - Set the start time
:to             - Set the end time
:range          - Set time range with predefined options
:heatmap        - Generate a heatmap visualization
:profile_events - Show profile events
:category       - Set category for heatmap (query_hash, tables, hosts)
:cluster        - Select cluster for queries
:metric         - Select metric for heatmap visualization
:scale          - Set scale type for heatmap (linear, log2, log10)
:metric_log     - Show system.metric_log metrics

Navigation:
- Use arrow keys to navigate
- Press / to filter connections list
- Press Esc to cancel current operation
- Press Enter in heatmap to show action menu
- Double click in heatmap to show action menu`
