package tui

import (
	"fmt"
	"strings"

	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/evertras/bubble-table/table"
)

type memoryRow struct {
	group string
	name  string
	val   int64
}

// MemoryDataMsg is sent when memory data is loaded
type MemoryDataMsg struct {
	Headers []string
	Rows    []table.Row
	Err     error
}

// memoryViewer is a bubbletea model for memory usage display
type memoryViewer struct {
	table   widgets.FilteredTable
	loading bool
	err     error
	width   int
	height  int
}

func newMemoryViewer(width, height int) memoryViewer {
	// Create empty table, will be populated when data arrives
	tableModel := widgets.NewFilteredTable(
		"Memory Usage",
		[]string{"Group", "Name"},
		width,
		height-4,
	)

	return memoryViewer{
		table:   tableModel,
		loading: true,
		width:   width,
		height:  height,
	}
}

func (m memoryViewer) Init() tea.Cmd {
	return nil
}

func (m memoryViewer) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case MemoryDataMsg:
		m.loading = false
		if msg.Err != nil {
			m.err = msg.Err
			return m, nil
		}

		// Update table with data
		m.table = widgets.NewFilteredTable(
			"Memory Usage",
			msg.Headers,
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
		}
	}

	// Delegate to table
	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

func (m memoryViewer) View() string {
	if m.loading {
		return "Loading memory data, please wait..."
	}
	if m.err != nil {
		return fmt.Sprintf("Error loading memory data: %v\n\nPress ESC to return", m.err)
	}
	return m.table.View()
}

// ShowMemory displays memory usage aggregated from various system tables across the selected cluster.
func (a *App) ShowMemory() tea.Cmd {
	if a.state.ClickHouse == nil {
		a.SwitchToMainPage("Error: Please connect to a ClickHouse instance first using :connect command")
		return nil
	}
	if a.state.Cluster == "" {
		a.SwitchToMainPage("Error: Please select a cluster first using :cluster command")
		return nil
	}

	// Create and show viewer
	viewer := newMemoryViewer(a.width, a.height)
	a.memoryHandler = viewer
	a.currentPage = pageMemory

	// Start async data fetch
	return a.fetchMemoryDataCmd()
}

// fetchMemoryDataCmd fetches memory data from ClickHouse
func (a *App) fetchMemoryDataCmd() tea.Cmd {
	return func() tea.Msg {
		cluster := a.state.Cluster
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

		rows, err := a.state.ClickHouse.Query(query)
		if err != nil {
			return MemoryDataMsg{Err: err}
		}
		defer rows.Close()

		// Collect data for pivot table
		hosts := []string{}
		prevHost := ""
		groupData := make(map[string][]memoryRow)
		groupPriority := make(map[string]int64)
		data := make(map[string]map[string]int64)

		// First pass: collect all data
		for rows.Next() {
			var host, groupName, name string
			var priority, val int64
			if err := rows.Scan(&host, &priority, &groupName, &name, &val); err != nil {
				continue
			}

			if host != prevHost {
				hosts = append(hosts, host)
				prevHost = host
			}

			row := memoryRow{
				group: groupName,
				name:  name,
				val:   val,
			}

			groupData[groupName] = append(groupData[groupName], row)

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

		// Build headers
		headers := append([]string{"Group", "Name"}, hosts...)

		// Build rows
		var tableRows []table.Row
		processedKeys := make(map[string]bool)

		for _, groupInfo := range groups {
			groupRows := groupData[groupInfo.name]
			for _, row := range groupRows {
				key := fmt.Sprintf("%s,%s", row.group, row.name)
				if processedKeys[key] {
					continue
				}
				processedKeys[key] = true

				rowData := table.RowData{
					"Group": row.group,
					"Name":  row.name,
				}

				// Add value for each host
				for _, host := range hosts {
					val := int64(0)
					if hostData, exists := data[host]; exists {
						if v, exists := hostData[key]; exists {
							val = v
						}
					}
					rowData[host] = formatReadableSize(val)
				}

				tableRows = append(tableRows, table.NewRow(rowData))
			}
		}

		return MemoryDataMsg{
			Headers: headers,
			Rows:    tableRows,
		}
	}
}

// formatReadableSize returns a human-readable representation of bytes
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

	if i == 0 {
		return fmt.Sprintf("%d %s", int64(s), units[i])
	}
	if s == float64(int64(s)) {
		return fmt.Sprintf("%.0f %s", s, units[i])
	}
	return fmt.Sprintf("%.2f %s", s, units[i])
}
