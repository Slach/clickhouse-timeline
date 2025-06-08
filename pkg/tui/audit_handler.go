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
			{"Partitions", ap.checkPartitions},
			{"Primary Keys", ap.checkPrimaryKeys},
			{"Tables", ap.checkTables},
			{"Memory Usage", ap.checkMemoryUsage},
			{"Disk Usage", ap.checkDiskUsage},
			{"Replication", ap.checkReplication},
			{"Performance Metrics", ap.checkPerformanceMetrics},
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
		if replicatedCount > 2000 {
			results = append(results, AuditResult{
				ID:       "A0.1.01",
				Object:   "ReplicatedTables",
				Severity: "Critical",
				Details:  fmt.Sprintf("Too many replicated tables (count: %d) - background_schedule_pool_size should be tuned", replicatedCount),
				Values:   map[string]float64{"replicated_tables_count": float64(replicatedCount)},
			})
		} else if replicatedCount > 900 {
			results = append(results, AuditResult{
				ID:       "A0.1.01",
				Object:   "ReplicatedTables",
				Severity: "Major",
				Details:  fmt.Sprintf("Too many replicated tables (count: %d) - background_schedule_pool_size should be tuned", replicatedCount),
				Values:   map[string]float64{"replicated_tables_count": float64(replicatedCount)},
			})
		} else if replicatedCount > 200 {
			results = append(results, AuditResult{
				ID:       "A0.1.01",
				Object:   "ReplicatedTables",
				Severity: "Moderate",
				Details:  fmt.Sprintf("Too many replicated tables (count: %d) - background_schedule_pool_size should be tuned", replicatedCount),
				Values:   map[string]float64{"replicated_tables_count": float64(replicatedCount)},
			})
		}
	}

	// Check MergeTree tables count
	row = ap.app.clickHouse.QueryRow("SELECT count() FROM system.tables WHERE engine LIKE '%MergeTree%'")
	var mergeTreeCount int64
	if err := row.Scan(&mergeTreeCount); err == nil {
		if mergeTreeCount > 10000 {
			results = append(results, AuditResult{
				ID:       "A0.1.02",
				Object:   "MergeTreeTables",
				Severity: "Critical",
				Details:  fmt.Sprintf("Too many MergeTree tables (count: %d)", mergeTreeCount),
				Values:   map[string]float64{"merge_tree_tables_count": float64(mergeTreeCount)},
			})
		} else if mergeTreeCount > 3000 {
			results = append(results, AuditResult{
				ID:       "A0.1.02",
				Object:   "MergeTreeTables",
				Severity: "Major",
				Details:  fmt.Sprintf("Too many MergeTree tables (count: %d)", mergeTreeCount),
				Values:   map[string]float64{"merge_tree_tables_count": float64(mergeTreeCount)},
			})
		} else if mergeTreeCount > 1000 {
			results = append(results, AuditResult{
				ID:       "A0.1.02",
				Object:   "MergeTreeTables",
				Severity: "Moderate",
				Details:  fmt.Sprintf("Too many MergeTree tables (count: %d)", mergeTreeCount),
				Values:   map[string]float64{"merge_tree_tables_count": float64(mergeTreeCount)},
			})
		}
	}

	// Check total parts count
	row = ap.app.clickHouse.QueryRow("SELECT count() FROM system.parts")
	var partsCount int64
	if err := row.Scan(&partsCount); err == nil {
		if partsCount > 120000 {
			results = append(results, AuditResult{
				ID:       "A0.1.05",
				Object:   "Parts",
				Severity: "Critical",
				Details:  fmt.Sprintf("Too many parts (count: %d)", partsCount),
				Values:   map[string]float64{"parts_count": float64(partsCount)},
			})
		} else if partsCount > 90000 {
			results = append(results, AuditResult{
				ID:       "A0.1.05",
				Object:   "Parts",
				Severity: "Major",
				Details:  fmt.Sprintf("Too many parts (count: %d)", partsCount),
				Values:   map[string]float64{"parts_count": float64(partsCount)},
			})
		} else if partsCount > 60000 {
			results = append(results, AuditResult{
				ID:       "A0.1.05",
				Object:   "Parts",
				Severity: "Moderate",
				Details:  fmt.Sprintf("Too many parts (count: %d)", partsCount),
				Values:   map[string]float64{"parts_count": float64(partsCount)},
			})
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
				log.Error().Err(closeErr).Msg("can't close checkSystemLogs")
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
				log.Error().Err(closeErr).Msg("can't close checkSystemLogs")
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
		if ratio > 0.9 {
			results = append(results, AuditResult{
				ID:       "A3.0.15",
				Object:   "Memory",
				Severity: "Critical",
				Details:  fmt.Sprintf("Memory usage is high (%.1f%% of total)", ratio*100),
				Values: map[string]float64{
					"memory_resident": memoryResident,
					"memory_total":    memoryTotal,
					"ratio":           ratio,
				},
			})
		} else if ratio > 0.8 {
			results = append(results, AuditResult{
				ID:       "A3.0.15",
				Object:   "Memory",
				Severity: "Major",
				Details:  fmt.Sprintf("Memory usage is high (%.1f%% of total)", ratio*100),
				Values: map[string]float64{
					"memory_resident": memoryResident,
					"memory_total":    memoryTotal,
					"ratio":           ratio,
				},
			})
		}
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
		WHERE type = 'Local'
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkSystemLogs")
			}
		}()
		for rows.Next() {
			var name string
			var freeSpace, totalSpace, ratio float64

			if err := rows.Scan(&name, &freeSpace, &totalSpace, &ratio); err == nil {
				usedRatio := 1.0 - ratio
				if usedRatio > 0.9 {
					results = append(results, AuditResult{
						ID:       "A1.8.01",
						Object:   fmt.Sprintf("Disk %s", name),
						Severity: "Critical",
						Details:  fmt.Sprintf("Too low free space (%.1f%% used)", usedRatio*100),
						Values: map[string]float64{
							"ratio":            usedRatio,
							"unreserved_space": freeSpace,
						},
					})
				} else if usedRatio > 0.85 {
					results = append(results, AuditResult{
						ID:       "A1.8.01",
						Object:   fmt.Sprintf("Disk %s", name),
						Severity: "Major",
						Details:  fmt.Sprintf("Too low free space (%.1f%% used)", usedRatio*100),
						Values: map[string]float64{
							"ratio":            usedRatio,
							"unreserved_space": freeSpace,
						},
					})
				} else if usedRatio > 0.8 {
					results = append(results, AuditResult{
						ID:       "A1.8.01",
						Object:   fmt.Sprintf("Disk %s", name),
						Severity: "Moderate",
						Details:  fmt.Sprintf("Too low free space (%.1f%% used)", usedRatio*100),
						Values: map[string]float64{
							"ratio":            usedRatio,
							"unreserved_space": freeSpace,
						},
					})
				}
			}
		}
	}

	return results
}

func (ap *AuditPanel) checkReplication() []AuditResult {
	var results []AuditResult

	// Check replication queue size
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
				log.Error().Err(closeErr).Msg("can't close checkSystemLogs")
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
				log.Error().Err(closeErr).Msg("can't close checkSystemLogs")
			}
		}()
		for rows.Next() {
			var metric string
			var value, cpuCount float64

			if err := rows.Scan(&metric, &value, &cpuCount); err == nil {
				if cpuCount > 0 {
					ratio := value / cpuCount
					if ratio > 10 {
						results = append(results, AuditResult{
							ID:       "A3.0.5",
							Object:   metric,
							Severity: "Critical",
							Details:  fmt.Sprintf("Load average is high (%s %.2f, %d cores)", metric, value, int(cpuCount)),
							Values: map[string]float64{
								"load":      value,
								"cpu_count": cpuCount,
								"ratio":     ratio,
							},
						})
					} else if ratio > 2 {
						results = append(results, AuditResult{
							ID:       "A3.0.5",
							Object:   metric,
							Severity: "Major",
							Details:  fmt.Sprintf("Load average is high (%s %.2f, %d cores)", metric, value, int(cpuCount)),
							Values: map[string]float64{
								"load":      value,
								"cpu_count": cpuCount,
								"ratio":     ratio,
							},
						})
					} else if ratio > 1 {
						results = append(results, AuditResult{
							ID:       "A3.0.5",
							Object:   metric,
							Severity: "Moderate",
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

	return results
}
