package tui

import (
	"database/sql"
	"fmt"
	"github.com/rs/zerolog/log"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// AuditResult represents a single audit finding
type AuditResult struct {
	ID       string
	Object   string
	Severity string
	Details  string
	Values   map[string]float64
}

// AuditPanel manages the audit interface
type AuditPanel struct {
	app         *App
	table       *tview.Table
	statusText  *tview.TextView
	progressBar *tview.TextView
	flex        *tview.Flex
	results     []AuditResult
	isRunning   bool
}

// ShowAudit displays the audit interface
func (a *App) ShowAudit() {
	if a.clickHouse == nil {
		a.mainView.SetText("Error: Please connect to a ClickHouse instance first")
		return
	}

	panel := &AuditPanel{
		app: a,
	}
	panel.setupUI()
	panel.runAudit()
}

func (ap *AuditPanel) setupUI() {
	// Create table for audit results
	ap.table = tview.NewTable().
		SetBorders(true).
		SetSelectable(true, false)

	// Set headers
	headers := []string{"ID", "Severity", "Object", "Details"}
	for col, header := range headers {
		cell := tview.NewTableCell(header).
			SetTextColor(tcell.ColorYellow).
			SetAlign(tview.AlignCenter).
			SetSelectable(false)
		ap.table.SetCell(0, col, cell)
	}

	// Status text
	ap.statusText = tview.NewTextView().
		SetDynamicColors(true).
		SetText("[yellow]Initializing audit...[white]")

	// Progress bar
	ap.progressBar = tview.NewTextView().
		SetDynamicColors(true).
		SetText("")

	// Create layout
	ap.flex = tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(ap.statusText, 1, 0, false).
		AddItem(ap.progressBar, 1, 0, false).
		AddItem(ap.table, 0, 1, true)

	ap.flex.SetBorder(true).SetTitle("ClickHouse System Audit")

	// Add to pages
	ap.app.pages.AddPage("audit", ap.flex, true, false)
	ap.app.pages.SwitchToPage("audit")
	ap.app.tviewApp.SetFocus(ap.table)

	// Setup key bindings
	ap.table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEscape:
			ap.app.pages.SwitchToPage("main")
			ap.app.tviewApp.SetFocus(ap.app.mainView)
			return nil
		case tcell.KeyEnter:
			ap.showResultDetails()
			return nil
		}
		return event
	})
}

func (ap *AuditPanel) updateProgress(message string, step, total int) {
	ap.app.tviewApp.QueueUpdateDraw(func() {
		ap.statusText.SetText(fmt.Sprintf("[yellow]%s[white]", message))
		if total > 0 {
			progress := float64(step) / float64(total) * 100
			progressBar := strings.Repeat("█", int(progress/5)) + strings.Repeat("░", 20-int(progress/5))
			ap.progressBar.SetText(fmt.Sprintf("[green]%s[white] %.1f%%", progressBar, progress))
		}
	})
}

func (ap *AuditPanel) runAudit() {
	ap.isRunning = true
	go func() {
		defer func() {
			ap.isRunning = false
		}()

		// Run audit checks
		checks := []struct {
			name string
			fn   func() []AuditResult
		}{
			{"System Counts", ap.checkSystemCounts},
			{"System Logs", ap.checkSystemLogs},
			{"Rates", ap.checkRates},
			{"Partitions", ap.checkPartitions},
			{"Marks Cache", ap.checkMarksCache},
			{"Tables", ap.checkTables},
			{"Background Pools", ap.checkBackgroundPools},
			{"Uncompressed Cache", ap.checkUncompressedCache},
			{"Replication Queue", ap.checkReplicationQueue},
			{"Memory Usage", ap.checkMemoryUsage},
			{"Disk Usage", ap.checkDiskUsage},
			{"Primary Keys", ap.checkPrimaryKeys},
			{"Materialized Views", ap.checkMaterializedViews},
			{"Performance Metrics", ap.checkPerformanceMetrics},
			{"Version Check", ap.checkVersions},
			{"Long Names", ap.checkLongNames},
		}

		totalChecks := len(checks)
		allResults := make([]AuditResult, 0)

		for i, check := range checks {
			ap.updateProgress(fmt.Sprintf("Running %s checks...", check.name), i, totalChecks)

			results := check.fn()
			allResults = append(allResults, results...)

			time.Sleep(100 * time.Millisecond) // Small delay for visual feedback
		}

		ap.updateProgress("Audit completed", totalChecks, totalChecks)
		ap.displayResults(allResults)
	}()
}

func (ap *AuditPanel) displayResults(results []AuditResult) {
	ap.app.tviewApp.QueueUpdateDraw(func() {
		ap.results = results

		// Clear existing rows (keep headers)
		for row := ap.table.GetRowCount() - 1; row > 0; row-- {
			ap.table.RemoveRow(row)
		}

		// Sort results by severity (Critical, Major, Moderate, Minor)
		severityOrder := map[string]int{
			"Critical": 0,
			"Major":    1,
			"Moderate": 2,
			"Minor":    3,
		}

		// Simple sort
		for i := 0; i < len(results); i++ {
			for j := i + 1; j < len(results); j++ {
				if severityOrder[results[i].Severity] > severityOrder[results[j].Severity] {
					results[i], results[j] = results[j], results[i]
				}
			}
		}

		// Add results to table
		for i, result := range results {
			row := i + 1

			// Color code by severity
			var color tcell.Color
			switch result.Severity {
			case "Critical":
				color = tcell.ColorRed
			case "Major":
				color = tcell.ColorOrange
			case "Moderate":
				color = tcell.ColorYellow
			case "Minor":
				color = tcell.ColorGreen
			default:
				color = tcell.ColorWhite
			}

			ap.table.SetCell(row, 0, tview.NewTableCell(result.ID).SetTextColor(color))
			ap.table.SetCell(row, 1, tview.NewTableCell(result.Severity).SetTextColor(color))
			ap.table.SetCell(row, 2, tview.NewTableCell(result.Object).SetTextColor(color))

			// Truncate details if too long
			details := result.Details
			if len(details) > 80 {
				details = details[:77] + "..."
			}
			ap.table.SetCell(row, 3, tview.NewTableCell(details).SetTextColor(color))
		}

		// Update status
		criticalCount := 0
		majorCount := 0
		moderateCount := 0
		minorCount := 0

		for _, result := range results {
			switch result.Severity {
			case "Critical":
				criticalCount++
			case "Major":
				majorCount++
			case "Moderate":
				moderateCount++
			case "Minor":
				minorCount++
			}
		}

		statusMsg := fmt.Sprintf("[red]Critical: %d[white] | [orange]Major: %d[white] | [yellow]Moderate: %d[white] | [green]Minor: %d[white] | Total: %d issues found",
			criticalCount, majorCount, moderateCount, minorCount, len(results))

		ap.statusText.SetText(statusMsg)
		ap.progressBar.SetText("[green]Press Enter for details, Esc to return[white]")
	})
}

func (ap *AuditPanel) showResultDetails() {
	row, _ := ap.table.GetSelection()
	if row <= 0 || row > len(ap.results) {
		return
	}

	result := ap.results[row-1]

	details := fmt.Sprintf(`[yellow::b]Audit Result Details[white::-]

[yellow]ID:[white] %s
[yellow]Severity:[white] %s
[yellow]Object:[white] %s

[yellow]Details:[white]
%s

[yellow]Values:[white]`, result.ID, result.Severity, result.Object, result.Details)

	if len(result.Values) > 0 {
		for key, value := range result.Values {
			details += fmt.Sprintf("\n  %s: %.2f", key, value)
		}
	} else {
		details += "\n  No additional values"
	}

	details += "\n\n[green]Press Esc to return[white]"

	detailView := tview.NewTextView().
		SetDynamicColors(true).
		SetText(details).
		SetBorder(true).
		SetTitle("Audit Result Details")

	detailView.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape {
			ap.app.pages.SwitchToPage("audit")
			ap.app.tviewApp.SetFocus(ap.table)
			return nil
		}
		return event
	})

	ap.app.pages.AddPage("audit_detail", detailView, true, false)
	ap.app.pages.SwitchToPage("audit_detail")
	ap.app.tviewApp.SetFocus(detailView)
}

// Audit check implementations
func (ap *AuditPanel) checkSystemCounts() []AuditResult {
	var results []AuditResult

	// Check replicated tables count
	row := ap.app.clickHouse.QueryRow("SELECT count() FROM system.tables WHERE engine LIKE 'Replicated%'")
	var replicatedCount int64
	if err := row.Scan(&replicatedCount); err == nil {
		severity := ""
		if replicatedCount > 2000 {
			severity = "Critical"
		} else if replicatedCount > 900 {
			severity = "Major"
		} else if replicatedCount > 200 {
			severity = "Moderate"
		}

		if severity != "" {
			results = append(results, AuditResult{
				ID:       "A0.1.01",
				Object:   "ReplicatedTables",
				Severity: severity,
				Details:  fmt.Sprintf("Too many replicated tables (count: %d) - background_schedule_pool_size should be tuned", replicatedCount),
				Values:   map[string]float64{"replicated_tables_count": float64(replicatedCount)},
			})

		}
	}

	// Check MergeTree tables count
	row = ap.app.clickHouse.QueryRow("SELECT count() FROM system.tables WHERE engine LIKE '%MergeTree%'")
	var mergeTreeCount int64
	if err := row.Scan(&mergeTreeCount); err == nil {
		severity := ""
		if mergeTreeCount > 10000 {
			severity = "Critical"
		} else if mergeTreeCount > 3000 {
			severity = "Major"
		} else if mergeTreeCount > 1000 {
			severity = "Moderate"
		}
		if severity != "" {
			results = append(results, AuditResult{
				ID:       "A0.1.02",
				Object:   "MergeTreeTables",
				Severity: severity,
				Details:  fmt.Sprintf("Too many MergeTree tables (count: %d)", mergeTreeCount),
				Values:   map[string]float64{"merge_tree_tables_count": float64(mergeTreeCount)},
			})

		}
	}

	// Check databases count
	row = ap.app.clickHouse.QueryRow("SELECT count() FROM system.databases")
	var databasesCount int64
	if err := row.Scan(&databasesCount); err == nil {
		severity := ""
		if databasesCount > 1000 {
			severity = "Critical"
		} else if databasesCount > 300 {
			severity = "Major"
		} else if databasesCount > 100 {
			severity = "Moderate"
		}

		if severity != "" {
			results = append(results, AuditResult{
				ID:       "A0.1.03",
				Object:   "Databases",
				Severity: severity,
				Details:  fmt.Sprintf("Too many databases (count: %d)", databasesCount),
				Values:   map[string]float64{"databases_count": float64(databasesCount)},
			})
		}
	}

	// Check column files in parts vs inodes
	row = ap.app.clickHouse.QueryRow(`
		SELECT 
			(SELECT count() * 4 FROM system.parts_columns) as column_files_in_parts_count,
			(SELECT min(value) FROM system.asynchronous_metrics WHERE metric='FilesystemMainPathTotalINodes') as total_inodes,
			column_files_in_parts_count / total_inodes as ratio
	`)
	var columnFilesCount, totalInodes int64
	var inodesRatio float64
	if err := row.Scan(&columnFilesCount, &totalInodes, &inodesRatio); err == nil && inodesRatio > 0.5 {
		severity := ""
		if inodesRatio > 0.8 {
			severity = "Critical"
		} else if inodesRatio > 0.7 {
			severity = "Major"
		} else if inodesRatio > 0.6 {
			severity = "Moderate"
		}

		if severity != "" {
			results = append(results, AuditResult{
				ID:       "A0.1.04",
				Object:   "PartsColumns",
				Severity: severity,
				Details:  fmt.Sprintf("Total columns files in parts too close to max inodes (column_files: %d, inodes: %d)", columnFilesCount, totalInodes),
				Values: map[string]float64{
					"column_files_in_parts_count": float64(columnFilesCount),
					"total_inodes":                float64(totalInodes),
				},
			})
		}
	}

	// Check total parts count
	row = ap.app.clickHouse.QueryRow("SELECT count() FROM system.parts")
	var partsCount int64
	if err := row.Scan(&partsCount); err == nil {
		severity := ""
		if partsCount > 120000 {
			severity = "Critical"
		} else if partsCount > 90000 {
			severity = "Major"
		} else if partsCount > 60000 {
			severity = "Moderate"
		}
		if severity != "" {
			results = append(results, AuditResult{
				ID:       "A0.1.05",
				Object:   "Parts",
				Severity: severity,
				Details:  fmt.Sprintf("Too many parts (count: %d)", partsCount),
				Values:   map[string]float64{"parts_count": float64(partsCount)},
			})
		}
	}

	return results
}

func (ap *AuditPanel) checkRates() []AuditResult {
	var results []AuditResult

	// Check parts creation rate
	row := ap.app.clickHouse.QueryRow(`
		WITH 
			(SELECT max(toUInt32(value)) FROM system.merge_tree_settings WHERE name='old_parts_lifetime') as old_parts_lifetime_raw,
			if(old_parts_lifetime_raw IS NULL OR old_parts_lifetime_raw = 0, 480, old_parts_lifetime_raw) as old_parts_lifetime
		SELECT 
			count() as parts_created_count,
			parts_created_count / old_parts_lifetime as parts_created_per_second
		FROM system.parts 
		WHERE modification_time > (SELECT max(modification_time) FROM system.parts) - old_parts_lifetime 
		AND level = 0
	`)
	var partsCreatedCount int64
	var partsCreatedPerSecond float64
	if err := row.Scan(&partsCreatedCount, &partsCreatedPerSecond); err == nil && partsCreatedPerSecond > 5 {
		severity := "Minor"
		if partsCreatedPerSecond > 50 {
			severity = "Critical"
		} else if partsCreatedPerSecond > 30 {
			severity = "Major"
		} else if partsCreatedPerSecond > 10 {
			severity = "Moderate"
		}

		results = append(results, AuditResult{
			ID:       "A0.3.01",
			Object:   "PartsCreatedPerSecond",
			Severity: severity,
			Details:  fmt.Sprintf("Too many parts created per second (%.2f)", partsCreatedPerSecond),
			Values:   map[string]float64{"parts_created_per_second": partsCreatedPerSecond},
		})
	}

	// Check parts creation rate per table
	rows, err := ap.app.clickHouse.Query(`
		WITH 
			(SELECT max(toUInt32(value)) FROM system.merge_tree_settings WHERE name='old_parts_lifetime') as old_parts_lifetime_raw,
			if(old_parts_lifetime_raw IS NULL OR old_parts_lifetime_raw = 0, 480, old_parts_lifetime_raw) as old_parts_lifetime
		SELECT 
			database,
			table,
			count() as parts_created_count,
			parts_created_count / old_parts_lifetime as parts_created_per_second
		FROM system.parts 
		WHERE modification_time > (SELECT max(modification_time) FROM system.parts) - old_parts_lifetime 
		AND level = 0
		GROUP BY database, table
		HAVING parts_created_per_second > 5
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkRates")
			}
		}()
		for rows.Next() {
			var database, table string
			var partsCount int64
			var rate float64
			if err := rows.Scan(&database, &table, &partsCount, &rate); err == nil {
				severity := "Minor"
				if rate > 50 {
					severity = "Critical"
				} else if rate > 30 {
					severity = "Major"
				} else if rate > 10 {
					severity = "Moderate"
				}

				results = append(results, AuditResult{
					ID:       "A0.3.02",
					Object:   fmt.Sprintf("%s.%s", database, table),
					Severity: severity,
					Details:  fmt.Sprintf("Too many parts created per second (%.2f)", rate),
					Values:   map[string]float64{"parts_created_per_second": rate},
				})
			}
		}
	}

	return results
}

func (ap *AuditPanel) checkMarksCache() []AuditResult {
	var results []AuditResult

	// Check marks cache hit ratio
	row := ap.app.clickHouse.QueryRow(`
		SELECT 
			(SELECT value FROM system.events WHERE event = 'MarkCacheHits') as hits,
			(SELECT value FROM system.events WHERE event = 'MarkCacheMisses') as misses,
			hits / (hits + misses) as hit_ratio
	`)
	var hits, misses, hitRatio float64
	if err := row.Scan(&hits, &misses, &hitRatio); err == nil && hitRatio < 0.8 {
		severity := "Minor"
		if hitRatio < 0.3 {
			severity = "Critical"
		} else if hitRatio < 0.5 {
			severity = "Major"
		} else if hitRatio < 0.7 {
			severity = "Moderate"
		}

		results = append(results, AuditResult{
			ID:       "A1.2.02",
			Object:   "MarkCache",
			Severity: severity,
			Details:  fmt.Sprintf("Bad hit/miss ratio for marks cache (hits: %.0f, misses: %.0f, ratio: %.3f)", hits, misses, hitRatio),
			Values:   map[string]float64{"hit_ratio": hitRatio},
		})
	}

	// Check marks cache size vs total RAM
	row = ap.app.clickHouse.QueryRow(`
		SELECT 
			(SELECT value FROM system.asynchronous_metrics WHERE metric = 'MarkCacheBytes') as actual_mark_cache_size,
			(SELECT value FROM system.asynchronous_metrics WHERE metric = 'OSMemoryTotal') as total_ram,
			actual_mark_cache_size / total_ram as marks_cache_ratio
	`)
	var markCacheSize, totalRam, marksCacheRatio float64
	if err := row.Scan(&markCacheSize, &totalRam, &marksCacheRatio); err == nil && marksCacheRatio > 0.1 {
		severity := "Minor"
		if marksCacheRatio > 0.25 {
			severity = "Critical"
		} else if marksCacheRatio > 0.2 {
			severity = "Major"
		} else if marksCacheRatio > 0.15 {
			severity = "Moderate"
		}

		results = append(results, AuditResult{
			ID:       "A1.2.04",
			Object:   "MarkCache",
			Severity: severity,
			Details:  fmt.Sprintf("Too big marks cache (%.1f%% of total RAM)", marksCacheRatio*100),
			Values:   map[string]float64{"actual_mark_cache_size": markCacheSize},
		})
	}

	// Check percentage of marks in memory
	row = ap.app.clickHouse.QueryRow(`
		SELECT 
			(SELECT value FROM system.asynchronous_metrics WHERE metric = 'MarkCacheBytes') as actual_mark_cache_size,
			(SELECT sum(marks_bytes) FROM system.parts WHERE active) as overall_marks_size,
			actual_mark_cache_size / overall_marks_size as marks_in_memory_ratio
	`)
	var overallMarksSize, marksInMemoryRatio float64
	if err := row.Scan(&markCacheSize, &overallMarksSize, &marksInMemoryRatio); err == nil && marksInMemoryRatio < 0.01 {
		severity := "Minor"
		if marksInMemoryRatio < 0.001 {
			severity = "Moderate"
		}

		results = append(results, AuditResult{
			ID:       "A1.2.03",
			Object:   "MarkCache",
			Severity: severity,
			Details:  fmt.Sprintf("Less than 1%% of marks loaded (%.3f%%)", marksInMemoryRatio*100),
			Values:   map[string]float64{"overall_marks_size": overallMarksSize},
		})
	}

	return results
}

func (ap *AuditPanel) checkBackgroundPools() []AuditResult {
	var results []AuditResult

	// Check background pool overload
	rows, err := ap.app.clickHouse.Query(`
		SELECT 
			extract(m.metric, '^Background(.*)Task') AS pool_name,
			m.value as current_value,
			s.max_value,
			m.value / s.max_value as pool_load_ratio
		FROM (
			SELECT metric, value 
			FROM system.metrics 
			WHERE metric LIKE 'Background%PoolTask'
		) m
		INNER JOIN (
			SELECT 
				transform(
					extract(name, '^background_(.*)_size'),
					['buffer_flush_schedule_pool', 'pool', 'fetches_pool', 'move_pool', 'common_pool', 'schedule_pool', 'message_broker_schedule_pool', 'distributed_schedule_pool'],
					['BufferFlushSchedulePool','MergesAndMutationsPool','FetchesPool', 'MovePool', 'CommonPool', 'SchedulePool', 'MessageBrokerSchedulePool', 'DistributedSchedulePool'],
					''
				) as pool_name,
				toFloat64(value) AS max_value
			FROM system.settings 
			WHERE name LIKE 'background%pool_size'
		) s USING (pool_name)
		WHERE pool_load_ratio > 0.8
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkBackgroundPools")
			}
		}()
		for rows.Next() {
			var poolName string
			var currentValue, maxValue, loadRatio float64
			if err := rows.Scan(&poolName, &currentValue, &maxValue, &loadRatio); err == nil {
				severity := "Minor"
				if loadRatio > 0.99 {
					severity = "Major"
				} else if loadRatio > 0.9 {
					severity = "Moderate"
				}

				results = append(results, AuditResult{
					ID:       "A1.4.01",
					Object:   poolName,
					Severity: severity,
					Details:  fmt.Sprintf("%s is overloaded (used: %.0f, size: %.0f, load ratio: %.3f)", poolName, currentValue, maxValue, loadRatio),
					Values: map[string]float64{
						"size":       currentValue,
						"load_ratio": loadRatio,
					},
				})
			}
		}
	}

	// Check MessageBrokerSchedulePool size vs Kafka/RabbitMQ tables
	row := ap.app.clickHouse.QueryRow(`
		SELECT 
			(SELECT toUInt64(value) FROM system.settings WHERE name = 'background_message_broker_schedule_pool_size') as pool_size,
			(SELECT count() FROM system.tables WHERE engine = 'Kafka' OR engine = 'RabbitMQ') as tables_with_engines
	`)
	var poolSize, tablesWithEngines int64
	if err := row.Scan(&poolSize, &tablesWithEngines); err == nil && poolSize < tablesWithEngines {
		results = append(results, AuditResult{
			ID:       "A1.4.02",
			Object:   "MessageBrokerSchedulePool",
			Severity: "Critical",
			Details:  fmt.Sprintf("MessageBrokerSchedulePool size is too small (size: %d / tables with Kafka or RabbitMQ engines: %d)", poolSize, tablesWithEngines),
			Values: map[string]float64{
				"size":    float64(poolSize),
				"engines": float64(tablesWithEngines),
			},
		})
	}

	return results
}

func (ap *AuditPanel) checkUncompressedCache() []AuditResult {
	var results []AuditResult

	// Check uncompressed cache hit ratio
	row := ap.app.clickHouse.QueryRow(`
		SELECT 
			(SELECT value FROM system.events WHERE event = 'UncompressedCacheHits') as hits,
			(SELECT value FROM system.events WHERE event = 'UncompressedCacheMisses') as misses,
			hits / (hits + misses) as hit_ratio
	`)
	var hits, misses, hitRatio float64
	if err := row.Scan(&hits, &misses, &hitRatio); err == nil && hitRatio < 0.1 {
		severity := "Minor"
		if hitRatio < 0.01 {
			severity = "Moderate"
		}

		results = append(results, AuditResult{
			ID:       "A1.5.01",
			Object:   "UncompressedCache",
			Severity: severity,
			Details:  fmt.Sprintf("Bad hit/miss ratio for uncompressed cache (hits: %.0f, misses: %.0f, ratio: %.3f)", hits, misses, hitRatio),
			Values: map[string]float64{
				"hits":   hits,
				"misses": misses,
				"ratio":  hitRatio,
			},
		})
	}

	// Check uncompressed cache size vs total RAM
	row = ap.app.clickHouse.QueryRow(`
		SELECT 
			(SELECT value FROM system.asynchronous_metrics WHERE metric = 'UncompressedCacheBytes') as actual_uncompressed_cache_size,
			(SELECT value FROM system.asynchronous_metrics WHERE metric = 'OSMemoryTotal') as total_ram,
			actual_uncompressed_cache_size / total_ram as uncompressed_cache_ratio
	`)
	var uncompressedCacheSize, totalRam, uncompressedCacheRatio float64
	if err := row.Scan(&uncompressedCacheSize, &totalRam, &uncompressedCacheRatio); err == nil && uncompressedCacheRatio > 0.1 {
		severity := "Minor"
		if uncompressedCacheRatio > 0.25 {
			severity = "Critical"
		} else if uncompressedCacheRatio > 0.2 {
			severity = "Major"
		} else if uncompressedCacheRatio > 0.15 {
			severity = "Moderate"
		}

		results = append(results, AuditResult{
			ID:       "A1.5.02",
			Object:   "UncompressedCache",
			Severity: severity,
			Details:  fmt.Sprintf("Too big uncompressed cache (%.1f%% of total RAM)", uncompressedCacheRatio*100),
			Values:   map[string]float64{"actual_uncompressed_cache_size": uncompressedCacheSize, "total_ram": totalRam},
		})
	}

	return results
}

func (ap *AuditPanel) checkReplicationQueue() []AuditResult {
	var results []AuditResult

	// Check replication queue size (moved from checkReplication)
	rows, err := ap.app.clickHouse.Query(`
		SELECT 
			database, 
			table, 
			count() as count_all,
			countIf(last_exception != '') as count_err,
			countIf(num_postponed > 0) as count_postponed,
			countIf(is_currently_executing) as count_executing
		FROM system.replication_queue
		GROUP BY database, table
		HAVING count_all > 100
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkReplicationQueue")
			}
		}()
		for rows.Next() {
			var database, table string
			var countAll, countErr, countPostponed, countExecuting int64

			if err := rows.Scan(&database, &table, &countAll, &countErr, &countPostponed, &countExecuting); err == nil {
				severity := "Minor"
				if countAll > 500 {
					severity = "Critical"
				} else if countAll > 400 {
					severity = "Major"
				} else if countAll > 200 {
					severity = "Moderate"
				}

				results = append(results, AuditResult{
					ID:       "A1.6",
					Object:   fmt.Sprintf("%s.%s", database, table),
					Severity: severity,
					Details:  fmt.Sprintf("Too many tasks in the replication_queue (count: %d)", countAll),
					Values: map[string]float64{
						"count_all":       float64(countAll),
						"count_err":       float64(countErr),
						"count_postponed": float64(countPostponed),
						"count_executing": float64(countExecuting),
					},
				})
			}
		}
	}

	// Check for old tasks in replication queue
	rows, err = ap.app.clickHouse.Query(`
		WITH 
			(SELECT maxArray([create_time, last_attempt_time, last_postpone_time]) FROM system.replication_queue) AS max_time
		SELECT 
			database,
			table,
			max_time - min(create_time) as relative_delay
		FROM system.replication_queue
		GROUP BY database, table
		HAVING relative_delay > 300
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkReplicationQueue old tasks")
			}
		}()
		for rows.Next() {
			var database, table string
			var relativeDelay float64

			if err := rows.Scan(&database, &table, &relativeDelay); err == nil {
				severity := "Minor"
				if relativeDelay > 24*3600 {
					severity = "Critical"
				} else if relativeDelay > 2*3600 {
					severity = "Major"
				} else if relativeDelay > 1800 {
					severity = "Moderate"
				}

				results = append(results, AuditResult{
					ID:       "A1.6.1",
					Object:   fmt.Sprintf("%s.%s", database, table),
					Severity: severity,
					Details:  fmt.Sprintf("Old tasks in replication_queue (max age: %.0f seconds)", relativeDelay),
					Values:   map[string]float64{"delay": relativeDelay},
				})
			}
		}
	}

	return results
}

func (ap *AuditPanel) checkMaterializedViews() []AuditResult {
	var results []AuditResult

	// Check for MVs not using TO syntax
	rows, err := ap.app.clickHouse.Query(`
		SELECT database, name 
		FROM system.tables 
		WHERE engine='MaterializedView' 
		AND splitByChar(' ', create_table_query)[5] != 'TO'
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkMaterializedViews")
			}
		}()
		for rows.Next() {
			var database, name string
			if err := rows.Scan(&database, &name); err == nil {
				results = append(results, AuditResult{
					ID:       "A2.2",
					Object:   fmt.Sprintf("%s.%s", database, name),
					Severity: "Moderate",
					Details:  "MV: TO syntax is not used",
					Values:   map[string]float64{},
				})
			}
		}
	}

	// Check for MVs using JOINs
	rows, err = ap.app.clickHouse.Query(`
		SELECT database, name 
		FROM system.tables 
		WHERE engine='MaterializedView' 
		AND create_table_query ILIKE '%JOIN%'
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkMaterializedViews JOINs")
			}
		}()
		for rows.Next() {
			var database, name string
			if err := rows.Scan(&database, &name); err == nil {
				results = append(results, AuditResult{
					ID:       "A2.3",
					Object:   fmt.Sprintf("%s.%s", database, name),
					Severity: "Moderate",
					Details:  "MV: JOIN is used",
					Values:   map[string]float64{},
				})
			}
		}
	}

	return results
}

func (ap *AuditPanel) checkVersions() []AuditResult {
	var results []AuditResult

	// Check ClickHouse version age
	row := ap.app.clickHouse.QueryRow(`
		SELECT 
			maxIf(value, name = 'VERSION_DESCRIBE') AS version_full,
			maxIf(toDate(parseDateTimeBestEffortOrNull(value)), lower(name) LIKE '%date%') AS release_date
		FROM system.build_options
		WHERE (name = 'VERSION_DESCRIBE') OR (lower(name) LIKE '%date%')
	`)
	var versionFull string
	var releaseDate sql.NullTime
	if err := row.Scan(&versionFull, &releaseDate); err == nil && releaseDate.Valid {
		versionAgeDays := int(time.Since(releaseDate.Time).Hours() / 24)

		if versionAgeDays > 182 {
			severity := "Minor"
			if versionAgeDays > 900 {
				severity = "Critical"
			} else if versionAgeDays > 700 {
				severity = "Major"
			} else if versionAgeDays > 365 {
				severity = "Moderate"
			}

			results = append(results, AuditResult{
				ID:       "A.2.1.01",
				Object:   "system",
				Severity: severity,
				Details:  fmt.Sprintf("You use old ClickHouse version (%s, %d days old), consider upgrade", versionFull, versionAgeDays),
				Values:   map[string]float64{},
			})
		}
	}

	return results
}

func (ap *AuditPanel) checkLongNames() []AuditResult {
	var results []AuditResult

	// Check for long database names
	rows, err := ap.app.clickHouse.Query(`
		SELECT name, length(name) as name_length
		FROM system.databases 
		WHERE length(name) > 32
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkLongNames databases")
			}
		}()
		for rows.Next() {
			var name string
			var nameLength int64
			if err := rows.Scan(&name, &nameLength); err == nil {
				severity := "Moderate"
				if nameLength > 196 {
					severity = "Critical"
				} else if nameLength > 128 {
					severity = "Major"
				} else if nameLength > 64 {
					severity = "Moderate"
				}

				results = append(results, AuditResult{
					ID:       "A0.0.6",
					Object:   name,
					Severity: severity,
					Details:  "Long database name",
					Values:   map[string]float64{},
				})
			}
		}
	}

	// Check for long table names
	rows, err = ap.app.clickHouse.Query(`
		SELECT database, name, length(name) as name_length
		FROM system.tables 
		WHERE length(name) > 32
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkLongNames tables")
			}
		}()
		for rows.Next() {
			var database, name string
			var nameLength int64
			if err := rows.Scan(&database, &name, &nameLength); err == nil {
				severity := "Moderate"
				if nameLength > 196 {
					severity = "Critical"
				} else if nameLength > 128 {
					severity = "Major"
				} else if nameLength > 64 {
					severity = "Moderate"
				}

				results = append(results, AuditResult{
					ID:       "A0.0.6",
					Object:   fmt.Sprintf("%s.%s", database, name),
					Severity: severity,
					Details:  "Long table name",
					Values:   map[string]float64{},
				})
			}
		}
	}

	// Check for long column names
	rows, err = ap.app.clickHouse.Query(`
		SELECT database, table, name, length(name) as name_length
		FROM system.columns 
		WHERE length(name) > 32 AND database NOT IN ('system','INFORMATION_SCHEMA','information_schema')
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkLongNames columns")
			}
		}()
		for rows.Next() {
			var database, table, name string
			var nameLength int64
			if err := rows.Scan(&database, &table, &name, &nameLength); err == nil {
				severity := "Moderate"
				if nameLength > 196 {
					severity = "Critical"
				} else if nameLength > 128 {
					severity = "Major"
				} else if nameLength > 64 {
					severity = "Moderate"
				}

				results = append(results, AuditResult{
					ID:       "A0.0.6",
					Object:   fmt.Sprintf("%s.%s.%s", database, table, name),
					Severity: severity,
					Details:  "Long column name",
					Values:   map[string]float64{},
				})
			}
		}
	}

	return results
}

func (ap *AuditPanel) checkSystemLogs() []AuditResult {
	var results []AuditResult

	// Check if query_log is enabled and has recent data
	row := ap.app.clickHouse.QueryRow(`
		SELECT max(event_time) 
		FROM system.query_log 
		WHERE event_time > now() - INTERVAL 4 HOUR
	`)
	var maxTime sql.NullTime
	if err := row.Scan(&maxTime); err == nil {
		if !maxTime.Valid || time.Since(maxTime.Time) > 4*time.Hour {
			results = append(results, AuditResult{
				ID:       "A0.2.01",
				Object:   "system.query_log",
				Severity: "Major",
				Details:  "No fresh records in system.query_log to analyze",
				Values:   map[string]float64{},
			})
		}
	}

	// Check if part_log is enabled and has recent data
	row = ap.app.clickHouse.QueryRow(`
		SELECT max(event_time) 
		FROM system.part_log 
		WHERE event_time > now() - INTERVAL 4 HOUR
	`)
	if err := row.Scan(&maxTime); err == nil {
		if !maxTime.Valid || time.Since(maxTime.Time) > 4*time.Hour {
			results = append(results, AuditResult{
				ID:       "A0.2.02",
				Object:   "system.part_log",
				Severity: "Major",
				Details:  "No fresh records in system.part_log to analyze",
				Values:   map[string]float64{},
			})
		}
	}

	// Check for system log tables without TTL
	rows, err := ap.app.clickHouse.Query(`
		SELECT database, name 
		FROM system.tables 
		WHERE database='system' AND name LIKE '%_log' 
		AND engine_full NOT LIKE '% TTL %'
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkSystemLogs")
			}
		}()
		for rows.Next() {
			var database, name string
			if err := rows.Scan(&database, &name); err == nil {
				results = append(results, AuditResult{
					ID:       "A0.2.04",
					Object:   fmt.Sprintf("%s.%s", database, name),
					Severity: "Major",
					Details:  "System log tables should have TTL enabled",
					Values:   map[string]float64{},
				})
			}
		}
	}

	// Check for query_thread_log being enabled (should be disabled in production)
	row = ap.app.clickHouse.QueryRow("SELECT count() FROM system.tables WHERE database='system' AND name='query_thread_log'")
	var threadLogExists int64
	if err := row.Scan(&threadLogExists); err == nil && threadLogExists > 0 {
		results = append(results, AuditResult{
			ID:       "A0.2.07",
			Object:   "System",
			Severity: "Major",
			Details:  "system.query_thread_log should be disabled in production systems",
			Values:   map[string]float64{},
		})
	}

	// Check for recent crashes
	row = ap.app.clickHouse.QueryRow("SELECT count() FROM system.crash_log WHERE event_time > now() - INTERVAL 5 DAY")
	var crashCount int64
	if err := row.Scan(&crashCount); err == nil && crashCount > 1 {
		results = append(results, AuditResult{
			ID:       "A0.2.08",
			Object:   "System",
			Severity: "Major",
			Details:  fmt.Sprintf("There are %d crashes for last 5 days", crashCount),
			Values:   map[string]float64{},
		})
	}

	// Check for warnings
	rows, err = ap.app.clickHouse.Query("SELECT message FROM system.warnings")
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close warnings check")
			}
		}()
		for rows.Next() {
			var message string
			if err := rows.Scan(&message); err == nil {
				results = append(results, AuditResult{
					ID:       "A0.2.09",
					Object:   "System",
					Severity: "Minor",
					Details:  fmt.Sprintf("Warning: %s", message),
					Values:   map[string]float64{},
				})
			}
		}
	}

	return results
}

func (ap *AuditPanel) checkPartitions() []AuditResult {
	var results []AuditResult

	// Check for tables with too many small partitions
	rows, err := ap.app.clickHouse.Query(`
		WITH 
			sum(bytes_on_disk) as b,
			sum(rows) as r,
			count() as partition_count
		SELECT 
			database, 
			table,
			partition_count,
			median(b) as median_partition_size_bytes,
			median(r) as median_partition_size_rows
		FROM (
			SELECT database, table, partition, sum(bytes_on_disk) as bytes_on_disk, sum(rows) as rows
			FROM system.parts 
			WHERE active AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
			GROUP BY database, table, partition
		)
		GROUP BY database, table
		HAVING partition_count > 1
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkPartitions")
			}
		}()
		for rows.Next() {
			var database, table string
			var partitionCount int64
			var medianBytes, medianRows float64

			if err := rows.Scan(&database, &table, &partitionCount, &medianBytes, &medianRows); err == nil {
				severity := "Minor"
				if partitionCount > 1500 && (medianBytes < 16000000 || medianRows < 250000) {
					severity = "Critical"
				} else if partitionCount > 500 && (medianBytes < 16000000 || medianRows < 250000) {
					severity = "Major"
				} else if partitionCount > 100 && (medianBytes < 16000000 || medianRows < 250000) {
					severity = "Moderate"
				} else if partitionCount > 1 && (medianBytes < 16000000 || medianRows < 250000) {
					severity = "Minor"
				} else if partitionCount > 1500 {
					severity = "Minor"
				} else {
					continue // Skip if no issues
				}

				results = append(results, AuditResult{
					ID:       "A1.1.01",
					Object:   fmt.Sprintf("%s.%s", database, table),
					Severity: severity,
					Details:  fmt.Sprintf("Too small partitions (count: %d, median size: %.0f bytes)", partitionCount, medianBytes),
					Values: map[string]float64{
						"partition_count":             float64(partitionCount),
						"median_partition_size_bytes": medianBytes,
						"median_partition_size_rows":  medianRows,
					},
				})
			}
		}
	}

	// Check for too fast inserts
	rows, err = ap.app.clickHouse.Query(`
		WITH 
			(SELECT max(toUInt32(value)) FROM system.merge_tree_settings WHERE name='old_parts_lifetime') as old_parts_lifetime_raw,
			if(old_parts_lifetime_raw IS NULL OR old_parts_lifetime_raw = 0, 480, old_parts_lifetime_raw) as old_parts_lifetime
		SELECT 
			database,
			table,
			count() as parts_created_count,
			sum(rows) as rows_in_parts,
			round(rows_in_parts / parts_created_count, 2) as average_rows_in_parts,
			round(1 / avg(dateDiff('second', 
				lagInFrame(modification_time) OVER (PARTITION BY database, table ORDER BY modification_time), 
				modification_time
			)), 2) as average_insert_rate
		FROM system.parts
		WHERE level = 0 
		AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
		AND modification_time > (SELECT max(modification_time) FROM system.parts) - old_parts_lifetime
		GROUP BY database, table
		HAVING average_rows_in_parts < 10000
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkPartitions fast inserts")
			}
		}()
		for rows.Next() {
			var database, table string
			var partsCount, rowsInParts int64
			var avgRowsInParts, avgInsertRate float64

			if err := rows.Scan(&database, &table, &partsCount, &rowsInParts, &avgRowsInParts, &avgInsertRate); err == nil {
				if avgInsertRate > 1 {
					severity := "Minor"
					if avgInsertRate > 10 {
						severity = "Critical"
					} else if avgInsertRate > 5 {
						severity = "Major"
					} else if avgInsertRate > 2 {
						severity = "Moderate"
					}

					results = append(results, AuditResult{
						ID:       "A1.1.05",
						Object:   fmt.Sprintf("%s.%s", database, table),
						Severity: severity,
						Details:  fmt.Sprintf("Too fast Inserts (%.2f per second)", avgInsertRate),
						Values: map[string]float64{
							"average_rows_in_parts": avgRowsInParts,
							"average_insert_rate":   avgInsertRate,
						},
					})
				}
			}
		}
	}

	// Check average row size
	rows, err = ap.app.clickHouse.Query(`
		SELECT 
			database,
			table,
			sum(data_uncompressed_bytes) as data_uncompressed_bytes_sum,
			sum(rows) as rows_sum,
			data_uncompressed_bytes_sum / rows_sum as average_row_size
		FROM system.parts
		WHERE active 
		GROUP BY database, table
		HAVING average_row_size > 3000
		ORDER BY average_row_size DESC
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkPartitions row size")
			}
		}()
		for rows.Next() {
			var database, table string
			var dataUncompressed, rowsSum int64
			var avgRowSize float64

			if err := rows.Scan(&database, &table, &dataUncompressed, &rowsSum, &avgRowSize); err == nil {
				severity := "Minor"
				if avgRowSize > 12000 {
					severity = "Critical"
				} else if avgRowSize > 8000 {
					severity = "Major"
				} else if avgRowSize > 5000 {
					severity = "Moderate"
				}

				results = append(results, AuditResult{
					ID:       "A1.1.06",
					Object:   fmt.Sprintf("%s.%s", database, table),
					Severity: severity,
					Details:  fmt.Sprintf("Too big average row size (%.0f bytes)", avgRowSize),
					Values:   map[string]float64{"average_row_size": avgRowSize},
				})
			}
		}
	}

	// Check detached parts
	rows, err = ap.app.clickHouse.Query(`
		SELECT database, table, count() as parts_count
		FROM system.detached_parts
		GROUP BY database, table
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkPartitions detached")
			}
		}()
		for rows.Next() {
			var database, table string
			var partsCount int64

			if err := rows.Scan(&database, &table, &partsCount); err == nil {
				severity := "Minor"
				if partsCount > 500 {
					severity = "Critical"
				} else if partsCount > 200 {
					severity = "Major"
				} else if partsCount > 50 {
					severity = "Moderate"
				}

				results = append(results, AuditResult{
					ID:       "A1.1.07",
					Object:   fmt.Sprintf("%s.%s", database, table),
					Severity: severity,
					Details:  fmt.Sprintf("Detached parts (count: %d)", partsCount),
					Values:   map[string]float64{"parts_count": float64(partsCount)},
				})
			}
		}
	}

	return results
}

func (ap *AuditPanel) checkPrimaryKeys() []AuditResult {
	var results []AuditResult

	// Check primary key size per mark
	rows, err := ap.app.clickHouse.Query(`
		SELECT 
			database, 
			table,
			sum(primary_key_bytes_in_memory) / sum(marks) as pk_per_mark,
			sum(primary_key_bytes_in_memory) as total_pk_memory
		FROM system.parts
		WHERE active
		GROUP BY database, table
		HAVING sum(marks) > 0 AND pk_per_mark > 64
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkSystemLogs")
			}
		}()
		for rows.Next() {
			var database, table string
			var pkPerMark, totalPkMemory float64

			if err := rows.Scan(&database, &table, &pkPerMark, &totalPkMemory); err == nil {
				severity := "Minor"
				if pkPerMark > 256 {
					severity = "Critical"
				} else if pkPerMark > 128 {
					severity = "Moderate"
				}

				results = append(results, AuditResult{
					ID:       "A1.2.01",
					Object:   fmt.Sprintf("%s.%s", database, table),
					Severity: severity,
					Details:  fmt.Sprintf("Too heavy PK (avg PK size per mark %.0f bytes)", pkPerMark),
					Values: map[string]float64{
						"pk_per_mark":                 pkPerMark,
						"primary_key_bytes_in_memory": totalPkMemory,
					},
				})
			}
		}
	}

	return results
}

func (ap *AuditPanel) checkTables() []AuditResult {
	var results []AuditResult

	// Check for tables with too many columns
	rows, err := ap.app.clickHouse.Query(`
		SELECT 
			database, 
			table, 
			count() as columns
		FROM system.columns 
		WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
		GROUP BY database, table
		HAVING columns > 600
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkTables")
			}
		}()
		for rows.Next() {
			var database, table string
			var columns int64

			if err := rows.Scan(&database, &table, &columns); err == nil {
				severity := "Minor"
				if columns > 1500 {
					severity = "Critical"
				} else if columns > 1000 {
					severity = "Major"
				} else if columns > 800 {
					severity = "Moderate"
				}

				results = append(results, AuditResult{
					ID:       "A1.3.01",
					Object:   fmt.Sprintf("%s.%s", database, table),
					Severity: severity,
					Details:  fmt.Sprintf("Too many columns (count: %d)", columns),
					Values:   map[string]float64{"columns": float64(columns)},
				})
			}
		}
	}

	// Check for tables with TTL but without ttl_only_drop_parts=1
	rows, err = ap.app.clickHouse.Query(`
		SELECT database, name
		FROM system.tables
		WHERE create_table_query LIKE '% TTL %'
		AND name NOT IN ('grants')
		AND NOT (create_table_query LIKE '%ttl_only_drop_parts = 1%' OR create_table_query LIKE '%ttl_only_drop_parts=1%')
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkTables TTL")
			}
		}()
		for rows.Next() {
			var database, name string
			if err := rows.Scan(&database, &name); err == nil {
				results = append(results, AuditResult{
					ID:       "A1.3.02",
					Object:   fmt.Sprintf("%s.%s", database, name),
					Severity: "Minor",
					Details:  "Table has TTL but ttl_only_drop_parts=1 is not used",
					Values:   map[string]float64{},
				})
			}
		}
	}

	return results
}

func (ap *AuditPanel) checkMemoryUsage() []AuditResult {
	var results []AuditResult

	// Check memory usage ratio
	row := ap.app.clickHouse.QueryRow(`
		SELECT 
			(SELECT value FROM system.asynchronous_metrics WHERE metric = 'MemoryResident') as memory_resident,
			(SELECT value FROM system.asynchronous_metrics WHERE metric = 'OSMemoryTotal') as memory_total
	`)
	var memoryResident, memoryTotal float64
	if err := row.Scan(&memoryResident, &memoryTotal); err == nil && memoryTotal > 0 {
		ratio := memoryResident / memoryTotal
		if ratio > 0.8 {
			severity := "Major"
			if ratio > 0.9 {
				severity = "Critical"
			}

			results = append(results, AuditResult{
				ID:       "A3.0.15",
				Object:   "Memory",
				Severity: severity,
				Details:  fmt.Sprintf("Memory usage is high (%.1f%% of total)", ratio*100),
				Values: map[string]float64{
					"memory_resident": memoryResident,
					"memory_total":    memoryTotal,
					"ratio":           ratio,
				},
			})
		}
	}

	// Check memory used by dictionaries and memory tables
	row = ap.app.clickHouse.QueryRow(`
		SELECT 
			(SELECT sum(bytes_allocated) FROM system.dictionaries) as dictionaries,
			(SELECT sum(total_bytes) FROM system.tables WHERE engine IN ('Memory','Set','Join')) as mem_tables,
			(SELECT value FROM system.asynchronous_metrics WHERE metric='OSMemoryTotal') as total_memory,
			(dictionaries + mem_tables) / total_memory as ratio
	`)
	var dictionaries, memTables, totalMemory, dictMemRatio float64
	if err := row.Scan(&dictionaries, &memTables, &totalMemory, &dictMemRatio); err == nil && dictMemRatio > 0.1 {
		severity := "Minor"
		if dictMemRatio > 0.3 {
			severity = "Critical"
		} else if dictMemRatio > 0.25 {
			severity = "Major"
		} else if dictMemRatio > 0.2 {
			severity = "Moderate"
		}

		results = append(results, AuditResult{
			ID:       "A1.7.01",
			Object:   "system OS RAM",
			Severity: severity,
			Details:  fmt.Sprintf("Too much memory used by dictionaries and memory tables (ratio: %.3f)", dictMemRatio),
			Values: map[string]float64{
				"ratio":        dictMemRatio,
				"dictionaries": dictionaries,
				"mem_tables":   memTables,
			},
		})
	}

	// Check memory used by primary keys
	row = ap.app.clickHouse.QueryRow(`
		SELECT 
			(SELECT sum(primary_key_bytes_in_memory) FROM system.parts) as primary_key_bytes_in_memory,
			(SELECT value FROM system.asynchronous_metrics WHERE metric='OSMemoryTotal') as total_memory,
			primary_key_bytes_in_memory / total_memory as ratio
	`)
	var primaryKeyMemory, pkMemRatio float64
	if err := row.Scan(&primaryKeyMemory, &totalMemory, &pkMemRatio); err == nil && pkMemRatio > 0.1 {
		severity := "Minor"
		if pkMemRatio > 0.3 {
			severity = "Critical"
		} else if pkMemRatio > 0.25 {
			severity = "Major"
		} else if pkMemRatio > 0.2 {
			severity = "Moderate"
		}

		results = append(results, AuditResult{
			ID:       "A1.7.02",
			Object:   "system OS RAM",
			Severity: severity,
			Details:  fmt.Sprintf("Too much memory used by primary keys (ratio: %.3f)", pkMemRatio),
			Values: map[string]float64{
				"ratio":                       pkMemRatio,
				"primary_key_bytes_in_memory": primaryKeyMemory,
			},
		})
	}

	return results
}

func (ap *AuditPanel) checkDiskUsage() []AuditResult {
	var results []AuditResult

	// Check disk space
	rows, err := ap.app.clickHouse.Query(`
		SELECT 
			name,
			free_space,
			total_space,
			free_space / total_space as ratio
		FROM system.disks 
		WHERE type = 'Local' AND ratio < 0.3
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkDiskUsage")
			}
		}()
		for rows.Next() {
			var name string
			var freeSpace, totalSpace, ratio float64

			if err := rows.Scan(&name, &freeSpace, &totalSpace, &ratio); err == nil {
				usedRatio := 1.0 - ratio
				severity := "Minor"
				if usedRatio > 0.9 {
					severity = "Critical"
				} else if usedRatio > 0.85 {
					severity = "Major"
				} else if usedRatio > 0.8 {
					severity = "Moderate"
				}

				results = append(results, AuditResult{
					ID:       "A1.8.01",
					Object:   fmt.Sprintf("Disk %s", name),
					Severity: severity,
					Details:  fmt.Sprintf("Too low free space (%.1f%% used)", usedRatio*100),
					Values: map[string]float64{
						"ratio":            usedRatio,
						"unreserved_space": freeSpace,
					},
				})
			}
		}
	}

	return results
}

func (ap *AuditPanel) checkPerformanceMetrics() []AuditResult {
	var results []AuditResult

	// Check if there are readonly replicas
	row := ap.app.clickHouse.QueryRow("SELECT value FROM system.metrics WHERE metric='ReadonlyReplica'")
	var readonlyReplicas float64
	if err := row.Scan(&readonlyReplicas); err == nil && readonlyReplicas > 0 {
		results = append(results, AuditResult{
			ID:       "A3.0.3",
			Object:   "System",
			Severity: "Critical",
			Details:  "Some replicas are read-only",
			Values:   map[string]float64{"readonly_replicas": readonlyReplicas},
		})
	}

	// Check load average
	rows, err := ap.app.clickHouse.Query(`
		SELECT 
			metric, 
			value,
			(SELECT count() FROM system.asynchronous_metrics WHERE metric LIKE 'CPUFrequencyMHz%') as cpu_count
		FROM system.asynchronous_metrics 
		WHERE metric LIKE 'LoadAverage%' AND value > 0
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkPerformanceMetrics")
			}
		}()
		for rows.Next() {
			var metric string
			var value, cpuCount float64

			if err := rows.Scan(&metric, &value, &cpuCount); err == nil {
				if cpuCount > 0 {
					ratio := value / cpuCount
					if ratio > 0.9 {
						severity := "Minor"
						if ratio > 10 {
							severity = "Critical"
						} else if ratio > 2 {
							severity = "Major"
						} else if ratio > 1 {
							severity = "Moderate"
						}

						results = append(results, AuditResult{
							ID:       "A3.0.5",
							Object:   metric,
							Severity: severity,
							Details:  fmt.Sprintf("Load average is high (%s %.2f, %d cores)", metric, value, int(cpuCount)),
							Values: map[string]float64{
								"load":      value,
								"cpu_count": cpuCount,
								"ratio":     ratio,
							},
						})
					}
				}
			}
		}
	}

	// Check replica delays
	rows, err = ap.app.clickHouse.Query(`
		SELECT metric, value
		FROM system.asynchronous_metrics
		WHERE metric IN ('ReplicasMaxAbsoluteDelay', 'ReplicasMaxRelativeDelay') 
		AND value > 300
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkPerformanceMetrics delays")
			}
		}()
		for rows.Next() {
			var metric string
			var value float64

			if err := rows.Scan(&metric, &value); err == nil {
				severity := "Minor"
				if value > 24*3600 {
					severity = "Critical"
				} else if value > 3*3600 {
					severity = "Major"
				} else if value > 1800 {
					severity = "Moderate"
				}

				results = append(results, AuditResult{
					ID:       "A3.0.6",
					Object:   metric,
					Severity: severity,
					Details:  fmt.Sprintf("Replica delay is too big (%s, %.0f seconds)", metric, value),
					Values:   map[string]float64{"delay": value},
				})
			}
		}
	}

	// Check queue sizes
	queueChecks := []struct {
		metric    string
		id        string
		threshold float64
		name      string
	}{
		{"ReplicasMaxInsertsInQueue", "A3.0.7", 100, "inserts in queue"},
		{"ReplicasSumInsertsInQueue", "A3.0.8", 300, "inserts in queue"},
		{"ReplicasMaxMergesInQueue", "A3.0.9", 80, "merges in queue"},
		{"ReplicasSumMergesInQueue", "A3.0.10", 200, "merges in queue"},
		{"ReplicasMaxQueueSize", "A3.0.11", 200, "tasks in queue"},
		{"ReplicasSumQueueSize", "A3.0.12", 500, "tasks in queue"},
	}

	for _, check := range queueChecks {
		row := ap.app.clickHouse.QueryRow(fmt.Sprintf("SELECT value FROM system.asynchronous_metrics WHERE metric = '%s'", check.metric))
		var value float64
		if err := row.Scan(&value); err == nil && value > check.threshold {
			results = append(results, AuditResult{
				ID:       check.id,
				Object:   check.metric,
				Severity: "Minor",
				Details:  fmt.Sprintf("Too many %s (%s, %.0f)", check.name, check.metric, value),
				Values:   map[string]float64{strings.Replace(check.name, " ", "_", -1): value},
			})
		}
	}

	// Check max parts in partition
	row = ap.app.clickHouse.QueryRow(`
		SELECT 
			value,
			(SELECT toUInt32(value) FROM system.merge_tree_settings WHERE name='parts_to_delay_insert') as parts_to_delay_insert,
			(SELECT toUInt32(value) FROM system.merge_tree_settings WHERE name='parts_to_throw_insert') as parts_to_throw_insert
		FROM system.asynchronous_metrics 
		WHERE metric = 'MaxPartCountForPartition'
	`)
	var maxParts, partsToDelay, partsToThrow float64
	if err := row.Scan(&maxParts, &partsToDelay, &partsToThrow); err == nil && maxParts > partsToDelay*0.9 {
		severity := "Minor"
		if maxParts > partsToThrow {
			severity = "Critical"
		} else if maxParts > partsToDelay {
			severity = "Major"
		}

		results = append(results, AuditResult{
			ID:       "A3.0.14",
			Object:   "MaxPartCountForPartition",
			Severity: severity,
			Details:  fmt.Sprintf("Too many parts in partition (%.0f)", maxParts),
			Values:   map[string]float64{"max_parts_in_partition": maxParts},
		})
	}

	return results
}
