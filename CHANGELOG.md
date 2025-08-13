# v0.0.1
IMPROVEMENTS
- Initial release of ClickHouse Timeline, an interactive performance analysis tool
- Added TUI with commands for heatmap, flamegraph, profile events, metric log, asynchronous metric log, logs, and system audit
- Implemented native flamegraph viewer with navigation and search
- Supported connection management with multiple contexts from YAML config
- Added self-profiling capabilities using pprof for CPU and memory
- Integrated logging with zerolog and stack traces for errors
- Added custom widgets for filtered lists and tables in the TUI
- Added system metric monitoring with sparkline visualizations from metric_log
- Added asynchronous metric viewing with trend analysis from asynchronous_metric_log
