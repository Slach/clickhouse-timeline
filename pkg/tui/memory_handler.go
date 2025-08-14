package tui

import (
	"database/sql"
	"fmt"
	"math"
	"strings"

	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	"github.com/rivo/tview"
)

// This refactor improves the memory view by:
//  - querying raw byte counts (toInt64) instead of pre-formatted strings
//  - separating groups and avoiding double-counting (e.g. QueryCacheBytes ⊂ Caches)
//  - computing percentages relative to MemoryTracking (mt) and to process RSS
//  - presenting a clear table: Group, Name, Bytes, % of MemoryTracking, % of RSS
//
// Notes:
//  - We query mt and rss separately and then a UNION ALL "base" of categorized items.
//  - Any remainder (mt - sum(categorized)) is shown as OtherTracked.
//  - MMaps, ThreadStacksVirtual, FileBuffersVirtual are shown but treated as not part of MemoryTracking.
//
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

	// 1) Fetch denominators: MemoryTracking (mt) and process RSS (rss)
	var mt int64
	if err := a.clickHouse.QueryRow("SELECT toInt64(value) FROM system.metrics WHERE metric = 'MemoryTracking' LIMIT 1").Scan(&mt); err != nil {
		// If metric missing, set to 0 to avoid division by zero
		if err == sql.ErrNoRows {
			mt = 0
		} else {
			a.SwitchToMainPage(fmt.Sprintf("Error reading MemoryTracking metric: %v", err))
			return
		}
	}

	var rss int64
	if err := a.clickHouse.QueryRow("SELECT toInt64(value) FROM system.asynchronous_metrics WHERE metric = 'OSMemoryResident' LIMIT 1").Scan(&rss); err != nil {
		if err == sql.ErrNoRows {
			rss = 0
		} else {
			a.SwitchToMainPage(fmt.Sprintf("Error reading OSMemoryResident metric: %v", err))
			return
		}
	}

	// 2) Build a base query that returns group,name,bytes (raw int64) - avoid string formatting inside SQL
	baseQuery := fmt.Sprintf(strings.TrimSpace(`
-- Caches (exclude QueryCacheBytes to avoid double counting)
SELECT 'Caches' AS grp, metric AS name, toInt64(value) AS bytes FROM cluster('%[1]s','system','asynchronous_metrics') WHERE metric LIKE '%%CacheBytes' AND metric NOT IN ('QueryCacheBytes')
UNION ALL
-- QueryCache as separate group
SELECT 'QueryCache' AS grp, 'result' AS name, toInt64(sum(result_size)) AS bytes FROM cluster('%[1]s','system','query_cache')
UNION ALL
-- Dictionaries
SELECT 'Dictionaries' AS grp, type AS name, toInt64(sum(bytes_allocated)) AS bytes FROM cluster('%[1]s','system','dictionaries') GROUP BY type
UNION ALL
-- Primary keys in memory
SELECT 'PrimaryKeys' AS grp, concat('db:', database) AS name, toInt64(sum(primary_key_bytes_in_memory_allocated)) AS bytes FROM cluster('%[1]s','system','parts') GROUP BY database
UNION ALL
-- Memory tables engines
SELECT 'MemoryTables' AS grp, engine AS name, toInt64(sum(total_bytes)) AS bytes FROM cluster('%[1]s','system','tables') WHERE engine IN ('Join','Memory','Buffer','Set') GROUP BY engine
UNION ALL
-- StorageBuffer
SELECT 'StorageBuffer' AS grp, 'StorageBufferBytes' AS name, toInt64(value) AS bytes FROM cluster('%[1]s','system','metrics') WHERE metric = 'StorageBufferBytes'
UNION ALL
-- InMemoryParts
SELECT 'InMemoryParts' AS grp, concat('db:', database) AS name, toInt64(sum(data_uncompressed_bytes)) AS bytes FROM cluster('%[1]s','system','parts') WHERE part_type = 'InMemory' GROUP BY database
UNION ALL
-- Queries (memory_usage)
SELECT 'Queries' AS grp, left(query,7) AS name, toInt64(sum(memory_usage)) AS bytes FROM cluster('%[1]s','system','processes') GROUP BY name
UNION ALL
-- Merges
SELECT 'Merges' AS grp, concat('db:', database) AS name, toInt64(sum(memory_usage)) AS bytes FROM cluster('%[1]s','system','merges') GROUP BY database
UNION ALL
-- MMaps (may be non-resident, shown separately)
SELECT 'MMaps' AS grp, metric AS name, toInt64(value) AS bytes FROM cluster('%[1]s','system','metrics') WHERE metric LIKE 'MMappedFileBytes'
UNION ALL
-- Thread stacks / virtual reservations (not strictly resident)
SELECT 'ThreadStacksVirtual' AS grp, metric AS name, toInt64(value * 8*1024*1024) AS bytes FROM cluster('%[1]s','system','metrics') WHERE metric = 'GlobalThread'
UNION ALL
-- File buffers virtual (estimates)
SELECT 'FileBuffersVirtual' AS grp, metric AS name, toInt64(value * 2*1024*1024) AS bytes FROM cluster('%[1]s','system','metrics') WHERE metric LIKE 'OpenFileFor%%'
UNION ALL
-- User memory tracking (per-user view of memory_usage) - shown but treat as alternate to Queries
SELECT 'UserMemoryTracking' AS grp, user AS name, toInt64(memory_usage) AS bytes FROM cluster('%[1]s','system','user_processes')
UNION ALL
-- Async inserts (in-flight)
SELECT 'AsyncInserts' AS grp, concat('db:', database) AS name, toInt64(sum(total_bytes)) AS bytes FROM cluster('%[1]s','system','asynchronous_inserts') GROUP BY database
UNION ALL
-- Query cache total (already excluded above)
SELECT 'QueryCacheBytes' AS grp, '' AS name, toInt64(sum(result_size)) AS bytes FROM cluster('%[1]s','system','query_cache')
UNION ALL
-- MemoryTracking total (for reference; will be used only as denominator)
SELECT 'MemoryTracking' AS grp, 'total' AS name, toInt64(value) AS bytes FROM cluster('%[1]s','system','metrics') WHERE metric = 'MemoryTracking'
`), cluster)

	rows, err := a.clickHouse.Query(baseQuery)
	if err != nil {
		a.SwitchToMainPage(fmt.Sprintf("Error running memory breakdown query: %v", err))
		return
	}
	defer rows.Close()

	// Collect results and compute categorized sum
	type rowItem struct {
		grp   string
		name  string
		bytes int64
	}
	var items []rowItem
	var categorizedSum int64 = 0
	groupTotals := map[string]int64{}

	for rows.Next() {
		var grp, name string
		var bytes int64
		if err := rows.Scan(&grp, &name, &bytes); err != nil {
			// non-fatal: skip malformed row
			continue
		}
		items = append(items, rowItem{grp: grp, name: name, bytes: bytes})
		// We consider certain groups as part of MemoryTracking categorization.
		// Exclude groups that are known to be not part of MemoryTracking totals (MMaps, ThreadStacksVirtual, FileBuffersVirtual).
		switch grp {
		case "MMaps", "ThreadStacksVirtual", "FileBuffersVirtual":
			// do not add to categorizedSum
		default:
			// For QueryCacheBytes we already excluded it from Caches; still include here as categorized
			if bytes > 0 {
				categorizedSum += bytes
				groupTotals[grp] += bytes
			}
		}
	}

	// Compute OtherTracked = max(mt - categorizedSum, 0)
	otherTracked := int64(0)
	if mt > categorizedSum {
		otherTracked = mt - categorizedSum
	}

	// Prepare filtered table
	ft := widgets.NewFilteredTable()
	headers := []string{"Group", "Name", "Bytes", "% of MemoryTracking", "% of RSS"}
	ft.SetupHeaders(headers)
	ft.Table.SetBorder(true)
	ft.Table.SetTitle("Memory usage breakdown")

	rowIndex := 1

	// Helper for human-readable bytes
	formatBytes := func(b int64) string {
		if b < 1024 {
			return fmt.Sprintf("%d B", b)
		}
		const unit = 1024
		div, exp := int64(unit), 0
		for n := b / unit; n >= unit; n /= unit {
			div *= unit
			exp++
		}
		value := float64(b) / float64(int64(1)<<(10*(exp+1)))
		prefix := []string{"Ki", "Mi", "Gi", "Ti", "Pi"}[exp]
		return fmt.Sprintf("%.2f %sB", value, prefix)
	}

	// Add aggregated group totals first for quick overview (sorted by total bytes descending)
	// We'll create a slice of groups from groupTotals
	type gt struct {
		grp   string
		bytes int64
	}
	var gts []gt
	for g, b := range groupTotals {
		gts = append(gts, gt{grp: g, bytes: b})
	}
	// simple sort by bytes desc
	for i := 0; i < len(gts); i++ {
		for j := i + 1; j < len(gts); j++ {
			if gts[j].bytes > gts[i].bytes {
				gts[i], gts[j] = gts[j], gts[i]
			}
		}
	}

	for _, g := range gts {
		pctMT := 0.0
		pctRSS := 0.0
		if mt > 0 {
			pctMT = (float64(g.bytes) / float64(mt)) * 100.0
		}
		if rss > 0 {
			pctRSS = (float64(g.bytes) / float64(rss)) * 100.0
		}
		cells := []*tview.TableCell{
			tview.NewTableCell(g.grp).SetTextColor(tview.Styles.TitleColor),
			tview.NewTableCell("(total)").SetTextColor(tview.Styles.SecondaryTextColor),
			tview.NewTableCell(formatBytes(g.bytes)),
			tview.NewTableCell(fmt.Sprintf("%.2f%%", pctMT)),
			tview.NewTableCell(fmt.Sprintf("%.2f%%", pctRSS)),
		}
		ft.SetRow(rowIndex, cells)
		rowIndex++
	}

	// Add detailed items
	for _, it := range items {
		// Skip zero-sized items for brevity
		if it.bytes == 0 {
			continue
		}
		pctMT := 0.0
		pctRSS := 0.0
		if mt > 0 {
			pctMT = (float64(it.bytes) / float64(mt)) * 100.0
		}
		if rss > 0 {
			pctRSS = (float64(it.bytes) / float64(rss)) * 100.0
		}
		cells := []*tview.TableCell{
			tview.NewTableCell(it.grp),
			tview.NewTableCell(it.name),
			tview.NewTableCell(formatBytes(it.bytes)),
			tview.NewTableCell(fmt.Sprintf("%.2f%%", roundTo(pctMT, 2))),
			tview.NewTableCell(fmt.Sprintf("%.2f%%", roundTo(pctRSS, 2))),
		}
		ft.SetRow(rowIndex, cells)
		rowIndex++
	}

	// Add OtherTracked row if present
	if otherTracked > 0 {
		pctMT := 0.0
		pctRSS := 0.0
		if mt > 0 {
			pctMT = (float64(otherTracked) / float64(mt)) * 100.0
		}
		if rss > 0 {
			pctRSS = (float64(otherTracked) / float64(rss)) * 100.0
		}
		cells := []*tview.TableCell{
			tview.NewTableCell("OtherTracked").SetTextColor(tview.Styles.ContrastBackgroundColor),
			tview.NewTableCell("remainder"),
			tview.NewTableCell(formatBytes(otherTracked)),
			tview.NewTableCell(fmt.Sprintf("%.2f%%", roundTo(pctMT, 2))),
			tview.NewTableCell(fmt.Sprintf("%.2f%%", roundTo(pctRSS, 2))),
		}
		ft.SetRow(rowIndex, cells)
		rowIndex++
	}

	// Also add Process RSS and OS Used metrics (contextual)
	if rss > 0 {
		// try to get OSMemoryTotal to show pct of OS
		var osTotal int64
		if err := a.clickHouse.QueryRow("SELECT toInt64(maxIf(value, metric = 'OSMemoryTotal')) FROM system.asynchronous_metrics WHERE metric IN ('OSMemoryTotal')").Scan(&osTotal); err != nil {
			// ignore error, just skip OS percent if not available
			osTotal = 0
		}
		pctOfOS := 0.0
		if osTotal > 0 {
			pctOfOS = (float64(rss) / float64(osTotal)) * 100.0
		}

		cells := []*tview.TableCell{
			tview.NewTableCell("Process").SetTextColor(tview.Styles.ContrastBackgroundColor),
			tview.NewTableCell("RSS"),
			tview.NewTableCell(formatBytes(rss)),
			tview.NewTableCell(""), // no mt pct
			tview.NewTableCell(fmt.Sprintf("%.2f%% of OS", roundTo(pctOfOS, 2))),
		}
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

// roundTo rounds to given decimals
func roundTo(v float64, decimals int) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	p := math.Pow(10, float64(decimals))
	return math.Round(v*p) / p
}
