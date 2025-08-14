package tui

import (
	"fmt"
	"strings"

	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	"github.com/rivo/tview"
)

// ShowMemory displays memory usage aggregated from various system tables across the selected cluster.
// It builds a single UNION ALL SQL query using cluster('<cluster>','system','table') and adds hostName()
// as the first column. The resulting table has columns: Host, Group, Name, Value.
// The widget supports filtering by pressing '/' (handled by widgets.FilteredTable).
func (a *App) ShowMemory() {
	if a.clickHouse == nil {
		a.SwitchToMainPage("Error: Please connect to a ClickHouse instance first using :connect command")
		return
	}
	if a.cluster == "" {
		a.SwitchToMainPage("Error: Please select a cluster first using :cluster command")
		return
	}

	cluster := a.cluster
	query := fmt.Sprintf(strings.TrimSpace(`
SELECT * FROM (
SELECT hostName() AS host, 1 AS priority, 'OS' as group, metric as name, toInt64(value) as val FROM cluster('%[1]s','system','asynchronous_metrics') WHERE metric like 'OSMemory%%'
UNION ALL
SELECT hostName() AS host, 2 AS priority, 'Process' as group, metric as name, toInt64(value) FROM cluster('%[1]s','system','asynchronous_metrics') WHERE metric LIKE 'Memory%%'
UNION ALL
SELECT hostName() AS host, 3 AS priority, 'Caches' as group, metric as name, toInt64(value) FROM cluster('%[1]s','system','asynchronous_metrics') WHERE metric LIKE '%%CacheBytes'
UNION ALL
SELECT hostName() AS host, 4 AS priority, 'MMaps' as group, metric as name, toInt64(value) FROM cluster('%[1]s','system','metrics') WHERE metric LIKE 'MMappedFileBytes'
UNION ALL
SELECT hostName() AS host, 5 AS priority, 'StorageBuffer' as group, metric as name, toInt64(value) FROM cluster('%[1]s','system','metrics') WHERE metric='StorageBufferBytes'
UNION ALL
SELECT hostName() AS host, 6 AS priority, 'MemoryTables' as group, engine as name, toInt64(sum(total_bytes)) FROM cluster('%[1]s','system','tables') WHERE engine IN ('Join','Memory','Buffer','Set') GROUP BY engine
UNION ALL
SELECT hostName() AS host, 7 AS priority, 'Dictionaries' as group, type as name, toInt64(sum(bytes_allocated)) FROM cluster('%[1]s','system','dictionaries') GROUP BY name
UNION ALL
SELECT hostName() AS host, 8 AS priority, 'PrimaryKeys' as group, 'db:'||database as name, toInt64(sum(primary_key_bytes_in_memory_allocated) FROM cluster('%[1]s','system','parts') GROUP BY name
UNION ALL
SELECT hostName() AS host, 9 AS priority, 'Merges' as group, 'db:'||database as name, toInt64(sum(memory_usage)) FROM cluster('%[1]s','system','merges') GROUP BY name
UNION ALL
SELECT hostName() AS host, 10 AS priority, 'Queries' as group, left(query,7) as name, toInt64(sum(memory_usage)) FROM cluster('%[1]s','system','processes') GROUP BY name
UNION ALL
SELECT hostName() AS host, 11 AS priority, 'AsyncInserts' as group, 'db:'||database as name, toInt64(sum(total_bytes)) FROM cluster('%[1]s','system','asynchronous_inserts') GROUP BY name
UNION ALL
SELECT hostName() AS host, 12 AS priority 'InMemoryParts' as group, 'db:'||database as name, toInt64(sum(data_uncompressed_bytes)) FROM cluster('%[1]s','system','parts') WHERE part_type = 'InMemory' GROUP BY name
UNION ALL
SELECT hostName() AS host, 13 AS priority, 'UserMemoryTracking' as group, user as name, toInt64(memory_usage) FROM cluster('%[1]s','system','user_processes')
UNION ALL
SELECT hostName() AS host, 14 AS priority, 'QueryCacheBytes' as group, '' as name, toInt64(sum(result_size)) FROM cluster('%[1]s','system','query_cache')
UNION ALL
SELECT hostName() AS host, 15 AS priority, 'FileBuffersVirtual' as group, metric as name, toInt64(value * 2*1024*1024) FROM cluster('%[1]s','system','metrics') WHERE metric like 'OpenFileFor%%'
UNION ALL
SELECT hostName() AS host, 16 AS priority, 'ThreadStacksVirtual' as group, metric as name, toInt64(value * 8*1024*1024) FROM cluster('%[1]s','system','metrics') WHERE metric = 'GlobalThread'
UNION ALL
SELECT hostName() AS host, 17 AS priority, 'MemoryTracking' as group, 'total' as name, toInt64(value) FROM cluster('%[1]s','system','metrics') WHERE metric = 'MemoryTracking'
) ORDER BY priority,"val" DESC
SETTINGS skip_unavailable_shards=1
`), cluster)

	rows, err := a.clickHouse.Query(query)
	if err != nil {
		a.SwitchToMainPage(fmt.Sprintf("Error running memory query: %v", err))
		return
	}
	defer rows.Close()

	// Create filtered table and populate
	ft := widgets.NewFilteredTable()
	headers := []string{"Host", "Group", "Name", "Value"}
	ft.SetupHeaders(headers)
	ft.Table.SetBorder(true)
	ft.Table.SetTitle("Memory usage")

	rowIndex := 1
	for rows.Next() {
		var host, groupName, name string
		var priority, val int64
		if err := rows.Scan(&host, &priority, &groupName, &name, &val); err != nil {
			// Skip malformed rows but continue
			continue
		}

		cells := []*tview.TableCell{
			tview.NewTableCell(host),
			tview.NewTableCell(groupName),
			tview.NewTableCell(name),
			tview.NewTableCell(formatReadableSize(val)),
		}
		// Add original cells so filtering works over host, group, name
		ft.SetRow(rowIndex, cells)
		rowIndex++
	}

	// Make sure table is selectable and keyboard capture allows '/'
	ft.Table.SetSelectable(true, true)
	ft.Table.SetInputCapture(ft.GetInputCapture(a.tviewApp, a.pages))
	// Remove any existing memory page and add new
	a.pages.RemovePage("memory")
	a.pages.AddPage("memory", ft.Table, true, true)
	a.tviewApp.SetFocus(ft.Table)
}

// formatReadableSize returns a human-readable representation of bytes similar to
// ClickHouse's formatReadableSize. It uses 1024-based units and returns values
// like "123 B", "1.23 KB", "4.00 MB", etc.
func formatReadableSize(val int64) string {
	if val < 0 {
		return "-" + formatReadableSize(-val)
	}

	units := []string{"B", "KB", "MB", "GB", "TB", "PB", "EB"}
	s := float64(val)
	i := 0
	for s >= 1024 && i < len(units)-1 {
		s /= 1024
		i++
	}

	// For bytes (unit B) print as integer, for larger units use 2 decimal places,
	// but drop decimal when value is an integer to keep output tidy.
	if i == 0 {
		return fmt.Sprintf("%d %s", int64(s), units[i])
	}
	if s == float64(int64(s)) {
		return fmt.Sprintf("%.0f %s", s, units[i])
	}
	return fmt.Sprintf("%.2f %s", s, units[i])
}
