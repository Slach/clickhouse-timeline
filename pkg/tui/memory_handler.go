package tui

import (
	"fmt"
	"strings"

	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	"github.com/rivo/tview"
)

type memoryRow struct {
	group string
	name  string
	val   int64
}

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
SELECT hostName() AS host, 8 AS priority, 'PrimaryKeys' as group, 'db:'||database as name, toInt64(sum(primary_key_bytes_in_memory_allocated)) FROM cluster('%[1]s','system','parts') GROUP BY name
UNION ALL
SELECT hostName() AS host, 9 AS priority, 'Merges' as group, 'db:'||database as name, toInt64(sum(memory_usage)) FROM cluster('%[1]s','system','merges') GROUP BY name
UNION ALL
SELECT hostName() AS host, 10 AS priority, 'Queries' as group, left(query,7) as name, toInt64(sum(memory_usage)) FROM cluster('%[1]s','system','processes') GROUP BY name
UNION ALL
SELECT hostName() AS host, 11 AS priority, 'AsyncInserts' as group, 'db:'||database as name, toInt64(sum(total_bytes)) FROM cluster('%[1]s','system','asynchronous_inserts') GROUP BY name
UNION ALL
SELECT hostName() AS host, 12 AS priority, 'InMemoryParts' as group, 'db:'||database as name, toInt64(sum(data_uncompressed_bytes)) FROM cluster('%[1]s','system','parts') WHERE part_type = 'InMemory' GROUP BY name
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
) ORDER BY host, priority, "val" DESC
SETTINGS skip_unavailable_shards=1
`), cluster)

	// Inform the user that we're loading data
	a.mainView.SetText("Loading memory data, please wait...")

	// Prepare the filtered table ahead of the goroutine so UI can be updated atomically later
	ft := widgets.NewFilteredTable()
	ft.Table.SetBorder(true)
	ft.Table.SetTitle("Memory usage")
	ft.Table.SetFixed(1, 1) // Fix first row and column

	// Perform the long-running query and row scanning in a goroutine,
	// then queue a single UI update to add the page and focus the table.
	go func() {
		rows, err := a.clickHouse.Query(query)
		if err != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.SwitchToMainPage(fmt.Sprintf("Error running memory query: %v", err))
			})
			return
		}
		defer rows.Close()

		// Collect data for pivot table
		hosts := []string{}
		prevHost := ""
		groupData := make(map[string][]memoryRow) // group -> rows
		groupPriority := make(map[string]int64)    // group -> priority
		data := make(map[string]map[string]int64)  // host -> (group,name) -> value

		// First pass: collect all data
		for rows.Next() {
			var host, groupName, name string
			var priority, val int64
			if err := rows.Scan(&host, &priority, &groupName, &name, &val); err != nil {
				// Skip malformed rows but continue
				continue
			}

			// Collect hosts in order
			if host != prevHost {
				hosts = append(hosts, host)
				prevHost = host
			}

			// Store row data
			row := memoryRow{
				group: groupName,
				name:  name,
				val:   val,
			}
			
			groupData[groupName] = append(groupData[groupName], row)
			
			// Store priority for group (first occurrence will be the priority from SQL)
			if _, exists := groupPriority[groupName]; !exists {
				groupPriority[groupName] = priority
			}

			key := fmt.Sprintf("%s,%s", groupName, name)
			if data[host] == nil {
				data[host] = make(map[string]int64)
			}
			data[host][key] = val
		}

		// Sort rows within each group by value descending
		for group := range groupData {
			rows := groupData[group]
			// Simple bubble sort by value descending
			for i := 0; i < len(rows)-1; i++ {
				for j := i + 1; j < len(rows); j++ {
					if rows[i].val < rows[j].val {
						rows[i], rows[j] = rows[j], rows[i]
					}
				}
			}
		}

		// Create sorted list of groups by priority
		type groupInfo struct {
			name     string
			priority int64
		}
		
		groups := make([]groupInfo, 0, len(groupData))
		for groupName := range groupData {
			groups = append(groups, groupInfo{name: groupName, priority: groupPriority[groupName]})
		}
		
		// Sort groups by priority
		for i := 0; i < len(groups)-1; i++ {
			for j := i + 1; j < len(groups); j++ {
				if groups[i].priority > groups[j].priority {
					groups[i], groups[j] = groups[j], groups[i]
				}
			}
		}

		// Set up headers: Group, Name, then each host column
		headers := append([]string{"Group", "Name"}, hosts...)
		ft.SetupHeaders(headers)

		// Create rows grouped by group (sorted by priority) and sorted by value within each group
		rowIndex := 1
		processedKeys := make(map[string]bool) // track processed (group,name) combinations
		
		for _, groupInfo := range groups {
			groupRows := groupData[groupInfo.name]
			for _, row := range groupRows {
				key := fmt.Sprintf("%s,%s", row.group, row.name)
				if processedKeys[key] {
					continue // skip duplicates
				}
				processedKeys[key] = true

				// Create cells for this row
				cells := make([]*tview.TableCell, len(headers))
				cells[0] = tview.NewTableCell(row.group)
				cells[1] = tview.NewTableCell(row.name)

				// Add value for each host
				for i, host := range hosts {
					val := int64(0)
					if hostData, exists := data[host]; exists {
						if v, exists := hostData[key]; exists {
							val = v
						}
					}
					cells[i+2] = tview.NewTableCell(formatReadableSize(val))
				}

				ft.SetRow(rowIndex, cells)
				rowIndex++
			}
		}

		// Queue a single UI update to attach the populated table and set focus/input handlers.
		a.tviewApp.QueueUpdateDraw(func() {
			// Make sure table is selectable and keyboard capture allows '/'
			ft.Table.SetSelectable(true, true)
			ft.Table.SetInputCapture(ft.GetInputCapture(a.tviewApp, a.pages))
			// Replace any existing memory page with the new content and focus it
			a.pages.RemovePage("memory")
			a.pages.AddPage("memory", ft.Table, true, true)
			a.tviewApp.SetFocus(ft.Table)
		})
	}()
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
