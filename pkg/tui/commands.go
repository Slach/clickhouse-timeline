package tui

// Available commands
const (
	CmdHelp       = "help"
	CmdConnect    = "connect"
	CmdQuit       = "quit"
	CmdFlamegraph = "flamegraph"
	CmdFrom       = "from"
	CmdTo         = "to"
	CmdRange      = "range"
)

type TraceType string

const (
	TraceMemory       TraceType = "Memory"
	TraceCPU          TraceType = "CPU"
	TraceReal         TraceType = "Real"
	TraceMemorySample TraceType = "MemorySample"
)

var availableCommands = []string{
	CmdHelp,
	CmdConnect,
	CmdQuit,
	CmdFlamegraph,
	CmdFrom,
	CmdTo,
	CmdRange,
}

// Help text
const helpText = `ClickHouse Timeline Commands:
:help       - Show this help
:connect    - Connect to a ClickHouse instance
:quit       - Exit the application
:flamegraph - Generate a flamegraph
:from       - Set the start time
:to         - Set the end time
:range      - Set time range with predefined options

Navigation:
- Use arrow keys to navigate
- Press / to filter connections list
- Press Esc to cancel current operation`
