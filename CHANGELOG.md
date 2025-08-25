# v0.0.2
FEATURES
- Added memory viewer command and UI integration
- Added heatmap zoom controls (zoom in/zoom out/zoom reset) to action menu
- Implemented Explain query flow with hash filtering and percentiles
- Added explain command and explain mode in CLI and TUI
- Enhanced logging format with unquoted values and stack traces
- Implemented overlay filter input for filtered list
- Enabled text selection for explain outputs and QueryView
- Added RenderList hook to preserve prefixes and selection state
- Added / filter input for tablesList and kindList in explain
- Always show header line in filtered memory usage table during scrolling
- Display memory values using formatReadableSize in ShowMemory

BUG FIXES
- Fixed heatmap errors when query_start_time equals '1970-01-01'
- Guarded nil tviewApp and queue UI updates in explain_handler.go
- Adjusted table row index to start from 0 in memory handler
- Removed trailing newline after LIMIT in SQL formatter
- Guarded list item access to prevent panic in explain selection
- Added runtime fallback for error stack traces in logging
- Fixed ANSI parsing in QueryView
- Preserved cursor when toggling list selections in explain UI
- Fixed small SQL errors in various handlers
- Preserved multiline logs by converting escaped \\n to real newlines

Small IMPROVEMENTS
- Overhauled Explain UI flow and percentile drilldown
- Refactored to show only queries list; reveal filter on '/'
- Enhanced tab navigation for explain query form and hash field input
- Allow Enter/Tab to switch focus from filter to list in explain UI
- Use standard FilteredList input via SetupFilterInput
- Render explain output with dynamic column formatting
- Use explicit 5-column scan and render aligned table
- Update selection prefix to [+] instead of [x]
- Move Search/Cancel buttons after lists and enable tab navigation
- Auto-load tables/kinds on open and hash change
- Place Tables and Query kinds inside Explain Query - Selection box
- Use separate output TextView for explain flow
- Load memory data asynchronously and update UI
- Overhauled ShowMemory to compute by-byte metrics and ratios
- Code cleanup and refactoring throughout the codebase
- Improved documentation and changelog maintenance

# v0.0.1
FEATURES
- Initial release of ClickHouse Timeline, an interactive performance analysis tool
- Added TUI with commands for heatmap, flamegraph, profile events, metric log, asynchronous metric log, logs, and system audit
- Implemented native flamegraph viewer with navigation and search
- Supported connection management with multiple contexts from YAML config
- Integrated logging with zerolog and stack traces for errors
- Added system metric monitoring with sparkline visualizations from metric_log
- Added asynchronous metric viewing with trend analysis from asynchronous_metric_log
