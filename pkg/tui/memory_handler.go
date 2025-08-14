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

	// Build SQL using cluster(...) for each FROM clause and adding hostName() as first column.
	cluster := a.cluster
	query := fmt.Sprintf(strings.TrimSpace(`
SELECT hostName() AS host, 'OS' as group, metric as name, formatReadableSize(toInt64(value)) as val FROM cluster('%s','system','asynchronous_metrics') WHERE metric like 'OSMemory%%'
UNION ALL
SELECT hostName() AS host, 'Caches' as group, metric as name, formatReadableSize(toInt64(value)) FROM cluster('%s','system','asynchronous_metrics') WHERE metric LIKE '%%CacheBytes'
UNION ALL
SELECT hostName() AS host, 'MMaps' as group, metric as name, formatReadableSize(toInt64(value)) FROM cluster('%s','system','metrics') WHERE metric LIKE 'MMappedFileBytes'
UNION ALL
SELECT hostName() AS host, 'Process' as group, metric as name, formatReadableSize(toInt64(value)) FROM cluster('%s','system','asynchronous_metrics') WHERE metric LIKE 'Memory%%'
UNION ALL
SELECT hostName() AS host, 'MemoryTables' as group, engine as name, formatReadableSize(toInt64(sum(total_bytes))) FROM cluster('%s','system','tables') WHERE engine IN ('Join','Memory','Buffer','Set') GROUP BY engine
UNION ALL
SELECT hostName() AS host, 'StorageBuffer' as group, metric as name, formatReadableSize(toInt64(value)) FROM cluster('%s','system','metrics') WHERE metric='StorageBufferBytes'
UNION ALL
SELECT hostName() AS host, 'Queries' as group, left(query,7) as name, formatReadableSize(toInt64(sum(memory_usage))) FROM cluster('%s','system','processes') GROUP BY name
UNION ALL
SELECT hostName() AS host, 'Dictionaries' as group, type as name, formatReadableSize(toInt64(sum(bytes_allocated))) FROM cluster('%s','system','dictionaries') GROUP BY name
UNION ALL
SELECT hostName() AS host, 'PrimaryKeys' as group, 'db:'||database as name, formatReadableSize(toInt64(sum(primary_key_bytes_in_memory_allocated))) FROM cluster('%s','system','parts') GROUP BY name
UNION ALL
SELECT hostName() AS host, 'Merges' as group, 'db:'||database as name, formatReadableSize(toInt64(sum(memory_usage))) FROM cluster('%s','system','merges') GROUP BY name
UNION ALL
SELECT hostName() AS host, 'InMemoryParts' as group, 'db:'||database as name, formatReadableSize(toInt64(sum(data_uncompressed_bytes))) FROM cluster('%s','system','parts') WHERE part_type = 'InMemory' GROUP BY name
UNION ALL
SELECT hostName() AS host, 'AsyncInserts' as group, 'db:'||database as name, formatReadableSize(toInt64(sum(total_bytes))) FROM cluster('%s','system','asynchronous_inserts') GROUP BY name
UNION ALL
SELECT hostName() AS host, 'FileBuffersVirtual' as group, metric as name, formatReadableSize(toInt64(value * 2*1024*1024)) FROM cluster('%s','system','metrics') WHERE metric like 'OpenFileFor%%'
UNION ALL
SELECT hostName() AS host, 'ThreadStacksVirual' as group, metric as name, formatReadableSize(toInt64(value * 8*1024*1024)) FROM cluster('%s','system','metrics') WHERE metric = 'GlobalThread'
UNION ALL
SELECT hostName() AS host, 'UserMemoryTracking' as group, user as name, formatReadableSize(toInt64(memory_usage)) FROM cluster('%s','system','user_processes')
UNION ALL
select hostName() AS host, 'QueryCacheBytes' as group, '' as name, formatReadableSize(toInt64(sum(result_size))) FROM cluster('%s','system','query_cache')
UNION ALL
SELECT hostName() AS host, 'MemoryTracking' as group, 'total' as name, formatReadableSize(toInt64(value)) FROM cluster('%s','system','metrics') WHERE metric = 'MemoryTracking'
`), cluster, cluster, cluster, cluster, cluster, cluster, cluster, cluster, cluster, cluster, cluster, cluster, cluster, cluster, cluster, cluster, cluster)

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
		var host, groupName, name, val string
		if err := rows.Scan(&host, &groupName, &name, &val); err != nil {
			// Skip malformed rows but continue
			continue
		}

		cells := []*tview.TableCell{
			tview.NewTableCell(host),
			tview.NewTableCell(groupName),
			tview.NewTableCell(name),
			tview.NewTableCell(val),
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
