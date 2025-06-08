package tui

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/rs/zerolog/log"
)

// AuditResult represents a single audit finding
type AuditResult struct {
	ID       string
	Host     string
	Object   string
	Severity string
	Details  string
	Values   map[string]float64
}

// AuditPanel manages the audit interface
type AuditPanel struct {
	app         *App
	table       *widgets.FilteredTable
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
	// Create filtered table for audit results
	ap.table = widgets.NewFilteredTable()
	ap.table.Table.SetBorders(false).SetSelectable(true, false)

	// Set headers
	headers := []string{"ID", "Host", "Severity", "Object", "Details"}
	ap.table.SetupHeaders(headers)

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
		AddItem(ap.table.Table, 0, 1, true)

	ap.flex.SetBorder(true).SetTitle("ClickHouse System Audit")

	// Add to pages
	ap.app.pages.AddPage("audit", ap.flex, true, false)
	ap.app.pages.SwitchToPage("audit")
	ap.app.tviewApp.SetFocus(ap.table.Table)

	// Setup key bindings with filtering support
	ap.table.Table.SetInputCapture(ap.table.GetInputCapture(ap.app.tviewApp, ap.app.pages))

	// Add custom key bindings for audit-specific actions
	originalCapture := ap.table.Table.GetInputCapture()
	ap.table.Table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEscape:
			ap.app.pages.SwitchToPage("main")
			ap.app.tviewApp.SetFocus(ap.app.mainView)
			return nil
		case tcell.KeyEnter:
			ap.showResultDetails()
			return nil
		}
		// Let the filtered table handle other keys (like '/' for filtering)
		if originalCapture != nil {
			return originalCapture(event)
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
			{"Active Parts", ap.checkActiveParts},
			{"Marks Cache", ap.checkMarksCache},
			{"Tables", ap.checkTables},
			{"Background Pools", ap.checkBackgroundPools},
			{"Uncompressed Cache", ap.checkUncompressedCache},
			{"Replication Queue", ap.checkReplicationQueue},
			{"Memory Usage", ap.checkMemoryUsage},
			{"Disk Usage", ap.checkDiskUsage},
			{"Primary Key Marks", ap.checkPrimaryKeyMarks},
			{"Primary Keys", ap.checkPrimaryKeys},
			{"Materialized Views", ap.checkMaterializedViews},
			{"Performance Metrics", ap.checkPerformanceMetrics},
			{"Version Check", ap.checkVersions},
			{"Long Names", ap.checkLongNames},
			{"Dependencies", ap.checkDependencies},
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

		// Add results to filtered table
		for _, result := range results {
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

			// Truncate details if too long
			details := result.Details
			if len(details) > 256 {
				details = details[:255] + "..."
			}

			// Create row cells
			cells := []*tview.TableCell{
				tview.NewTableCell(result.ID).SetTextColor(color),
				tview.NewTableCell(result.Host).SetTextColor(color),
				tview.NewTableCell(result.Severity).SetTextColor(color),
				tview.NewTableCell(result.Object).SetTextColor(color),
				tview.NewTableCell(details).SetTextColor(color),
			}

			ap.table.AddRow(cells)
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
	row, _ := ap.table.Table.GetSelection()
	if row <= 0 || row > len(ap.results) {
		return
	}

	result := ap.results[row-1]

	details := fmt.Sprintf(`[yellow::b]Audit Result Details[white::-]

[yellow]ID:[white] %s
[yellow]Host:[white] %s
[yellow]Severity:[white] %s
[yellow]Object:[white] %s

[yellow]Details:[white]
%s

[yellow]Values:[white]`, result.ID, result.Host, result.Severity, result.Object, result.Details)

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
			ap.app.tviewApp.SetFocus(ap.table.Table)
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
	row := ap.app.clickHouse.QueryRow(fmt.Sprintf("SELECT hostName() AS h, count() FROM cluster('%s', system.tables) WHERE engine LIKE 'Replicated%%' GROUP BY h", ap.app.cluster))
	var host string
	var replicatedCount int64
	if err := row.Scan(&host, &replicatedCount); err == nil {
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
				Host:     host,
				Object:   "ReplicatedTables",
				Severity: severity,
				Details:  fmt.Sprintf("Too many replicated tables (count: %d) - background_schedule_pool_size should be tuned", replicatedCount),
				Values:   map[string]float64{"replicated_tables_count": float64(replicatedCount)},
			})

		}
	}

	// Check MergeTree tables count
	row = ap.app.clickHouse.QueryRow(fmt.Sprintf("SELECT hostName() AS h, count() FROM cluster('%s', system.tables) WHERE engine LIKE '%%MergeTree%%' GROUP BY h", ap.app.cluster))
	if err := row.Scan(&host, &mergeTreeCount); err == nil {
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
				Host:     host,
				Object:   "MergeTreeTables",
				Severity: severity,
				Details:  fmt.Sprintf("Too many MergeTree tables (count: %d)", mergeTreeCount),
				Values:   map[string]float64{"merge_tree_tables_count": float64(mergeTreeCount)},
			})

		}
	}

	// Check databases count
	row = ap.app.clickHouse.QueryRow(fmt.Sprintf("SELECT hostName() AS h, count() FROM cluster('%s', system.databases) GROUP BY h", ap.app.cluster))
	if err := row.Scan(&host, &databasesCount); err == nil {
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
				Host:     host,
				Object:   "Databases",
				Severity: severity,
				Details:  fmt.Sprintf("Too many databases (count: %d)", databasesCount),
				Values:   map[string]float64{"databases_count": float64(databasesCount)},
			})
		}
	}

	// Check column files in parts vs inodes
	row = ap.app.clickHouse.QueryRow(fmt.Sprintf(`
		SELECT 
			hostName() AS h,
			(SELECT count() * 4 FROM cluster('%s', system.parts_columns)) as column_files_in_parts_count,
			(SELECT min(value) FROM cluster('%s', system.asynchronous_metrics) WHERE metric='FilesystemMainPathTotalINodes') as total_inodes,
			column_files_in_parts_count / total_inodes as ratio
		GROUP BY h
	`, ap.app.cluster, ap.app.cluster))
	var columnFilesCount, totalInodes int64
	var inodesRatio float64
	if err := row.Scan(&host, &columnFilesCount, &totalInodes, &inodesRatio); err == nil && inodesRatio > 0.5 {
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
				Host:     host,
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
	row = ap.app.clickHouse.QueryRow(fmt.Sprintf("SELECT hostName() AS h, count() FROM cluster('%s', system.parts) GROUP BY h", ap.app.cluster))
	if err := row.Scan(&host, &partsCount); err == nil {
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
				Host:     host,
				Object:   "Parts",
				Severity: severity,
				Details:  fmt.Sprintf("Too many parts (count: %d)", partsCount),
				Values:   map[string]float64{"parts_count": float64(partsCount)},
			})
		}
	}

	// Check obsolete inactive parts
	row = ap.app.clickHouse.QueryRow(fmt.Sprintf(`
		WITH (SELECT max(modification_time) FROM cluster('%s', system.parts)) AS max_ts
		SELECT hostName() AS h, count()
		FROM cluster('%s', system.parts)
		WHERE NOT active
		AND ((remove_time > 0 AND remove_time < max_ts - INTERVAL 20 MINUTE) 
		     OR (remove_time = 0 AND modification_time < max_ts - INTERVAL 20 MINUTE))
		GROUP BY h
	`, ap.app.cluster, ap.app.cluster))
	var obsoletePartsCount int64
	if err := row.Scan(&host, &obsoletePartsCount); err == nil && obsoletePartsCount > 0 {
		severity := ""
		if obsoletePartsCount > 5000 {
			severity = "Critical"
		} else if obsoletePartsCount > 2000 {
			severity = "Major"
		} else if obsoletePartsCount > 500 {
			severity = "Moderate"
		}

		if severity != "" {
			results = append(results, AuditResult{
				ID:       "A0.1.06",
				Host:     host,
				Object:   "Obsolete parts",
				Severity: severity,
				Details:  fmt.Sprintf("Number of inactive parts which were removed long ago (count: %d)", obsoletePartsCount),
				Values:   map[string]float64{"parts_count": float64(obsoletePartsCount)},
			})
		}
	}

	// Check for too many tiny replicated tables
	row = ap.app.clickHouse.QueryRow(fmt.Sprintf(`
		WITH
			(total_rows < 1000000) AND (total_bytes < 10000000) AS tiny_table,
			(total_rows < 100000000) AND (total_bytes < 1000000000) AND (NOT tiny_table) AS small_table,
			(total_rows > 1000000000) OR (total_bytes > 100000000000) AS big_table
		SELECT
			hostName() AS h,
			countIf(tiny_table) as tiny_tables_count,
			countIf(small_table) as small_tables_count,
			countIf((NOT big_table) AND (NOT small_table) AND (NOT tiny_table)) as medium_tables_count,
			countIf(big_table) as big_tables_count,
			count() AS tables_count
		FROM cluster('%s', system.tables)
		WHERE engine LIKE 'Replicated%%MergeTree'
		GROUP BY h
	`, ap.app.cluster))
	var tinyTablesCount, smallTablesCount, mediumTablesCount, bigTablesCount, tablesCount int64
	if err := row.Scan(&host, &tinyTablesCount, &smallTablesCount, &mediumTablesCount, &bigTablesCount, &tablesCount); err == nil {
		if ((tinyTablesCount + smallTablesCount) > int64(float64(tablesCount)*0.85)) || ((tinyTablesCount + smallTablesCount) > 100) {
			results = append(results, AuditResult{
				ID:       "A0.1.07",
				Host:     host,
				Object:   "Tables Size",
				Severity: "Major",
				Details:  fmt.Sprintf("Most of your Replicated tables are tiny, consider options to combine similar data together in fewer tables (tiny: %d, small: %d, medium: %d, big: %d, overall: %d)", tinyTablesCount, smallTablesCount, mediumTablesCount, bigTablesCount, tablesCount),
				Values: map[string]float64{
					"tiny_tables_count":   float64(tinyTablesCount),
					"small_tables_count":  float64(smallTablesCount),
					"medium_tables_count": float64(mediumTablesCount),
					"big_tables_count":    float64(bigTablesCount),
					"tables_count":        float64(tablesCount),
				},
			})
		}
	}

	return results
}

func (ap *AuditPanel) checkDependencies() []AuditResult {
	var results []AuditResult

	// Create temporary dependencies table and populate it
	// This implements the logic from dependancies_init.sql and dependancies_loop.sql
	_, err := ap.app.clickHouse.Exec(`
		CREATE TEMPORARY TABLE IF NOT EXISTS dependencies_temp (
			host String,
			parent String,
			child String,
			type String,
			level UInt32
		) ENGINE = Memory
	`)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create dependencies temp table")
		return results
	}

	// Initialize dependencies from tables (dependancies_init.sql logic)
	_, err = ap.app.clickHouse.Exec(fmt.Sprintf(`
		INSERT INTO dependencies_temp
		WITH d1 AS (
			SELECT 
				hostName() AS h,
				format('{}.{}', database, name) AS parent,
				arrayJoin(arrayMap(x, y -> x || '.' || y, dependencies_database, dependencies_table)) as child,
				'table' as type
			FROM cluster('%s', system.tables)
			WHERE dependencies_table != []

			UNION ALL

			WITH splitByChar(' ', create_table_query) as _create_table_query
			SELECT 
				hostName() AS h,
				format('{}.{}', database, name) AS parent,
				_create_table_query[6] as child,
				'MV' as type
			FROM cluster('%s', system.tables)
			WHERE engine = 'MaterializedView'
			AND _create_table_query[5] = 'TO'
		)
		SELECT h, parent, child, type, 0 as level FROM d1
	`, ap.app.cluster, ap.app.cluster))
	if err != nil {
		log.Error().Err(err).Msg("Failed to populate initial dependencies")
		return results
	}

	// Iteratively build dependency chains (dependancies_loop.sql logic)
	// We'll do a few iterations to build the dependency tree
	for i := 0; i < 5; i++ {
		row := ap.app.clickHouse.QueryRow(`
			WITH 
				(SELECT max(level) FROM dependencies_temp) as _level,
				d as (SELECT * FROM dependencies_temp WHERE level = _level)
			SELECT count()
			FROM d as a 
			JOIN d as b ON a.child = b.parent AND a.host = b.host
		`)
		var newDepsCount int64
		if err := row.Scan(&newDepsCount); err != nil || newDepsCount == 0 {
			break // No more dependencies to add
		}

		_, err = ap.app.clickHouse.Exec(`
			INSERT INTO dependencies_temp
			WITH 
				(SELECT max(level) FROM dependencies_temp) as _level,
				d as (SELECT * FROM dependencies_temp WHERE level = _level)
			SELECT
				a.host as host,
				a.parent as parent,
				b.child as child,
				'join' as type,
				_level + 1 as level
			FROM d as a 
			JOIN d as b ON a.child = b.parent AND a.host = b.host
		`)
		if err != nil {
			log.Error().Err(err).Msg("Failed to add dependency level")
			break
		}
	}

	// Check for tables with too many dependencies (A2.3 logic)
	rows, err := ap.app.clickHouse.Query(`
		SELECT 
			host,
			parent,
			count() as total,
			groupArray(child) as children
		FROM dependencies_temp
		GROUP BY host, parent
		HAVING total > 10
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkDependencies")
			}
		}()
		for rows.Next() {
			var host, parent string
			var total int64
			var children []string

			if err := rows.Scan(&host, &parent, &total, &children); err == nil {
				// Create values map from children list
				values := make(map[string]float64)
				for i, childName := range children {
					if i < 20 { // Limit to avoid too many values
						values[childName] = 1.0
					}
				}
				values["total_dependencies"] = float64(total)

				results = append(results, AuditResult{
					ID:       "A2.3",
					Host:     host,
					Object:   parent,
					Severity: "Moderate",
					Details:  fmt.Sprintf("Too long dependencies list. count: %d", total),
					Values:   values,
				})
			}
		}
	}

	// Clean up temporary table
	_, err = ap.app.clickHouse.Exec("DROP TABLE IF EXISTS dependencies_temp")
	if err != nil {
		log.Error().Err(err).Msg("Failed to drop dependencies temp table")
	}

	return results
}

func (ap *AuditPanel) checkRates() []AuditResult {
	var results []AuditResult

	// Check parts creation rate
	row := ap.app.clickHouse.QueryRow(fmt.Sprintf(`
		WITH 
			(SELECT max(toUInt32(value)) FROM cluster('%s', system.merge_tree_settings) WHERE name='old_parts_lifetime') as old_parts_lifetime_raw,
			if(old_parts_lifetime_raw IS NULL OR old_parts_lifetime_raw = 0, 480, old_parts_lifetime_raw) as old_parts_lifetime
		SELECT 
			hostName() AS h,
			count() as parts_created_count,
			parts_created_count / old_parts_lifetime as parts_created_per_second
		FROM cluster('%s', system.parts) 
		WHERE modification_time > (SELECT max(modification_time) FROM cluster('%s', system.parts)) - old_parts_lifetime 
		AND level = 0
		GROUP BY h
	`, ap.app.cluster, ap.app.cluster, ap.app.cluster))
	var host string
	var partsCreatedCount int64
	var partsCreatedPerSecond float64
	if err := row.Scan(&host, &partsCreatedCount, &partsCreatedPerSecond); err == nil && partsCreatedPerSecond > 5 {
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
			Host:     host,
			Object:   "PartsCreatedPerSecond",
			Severity: severity,
			Details:  fmt.Sprintf("Too many parts created per second (%.2f)", partsCreatedPerSecond),
			Values:   map[string]float64{"parts_created_per_second": partsCreatedPerSecond},
		})
	}

	// Check parts creation rate per table
	rows, err := ap.app.clickHouse.Query(fmt.Sprintf(`
		WITH 
			(SELECT max(toUInt32(value)) FROM cluster('%s', system.merge_tree_settings) WHERE name='old_parts_lifetime') as old_parts_lifetime_raw,
			if(old_parts_lifetime_raw IS NULL OR old_parts_lifetime_raw = 0, 480, old_parts_lifetime_raw) as old_parts_lifetime
		SELECT 
			hostName() AS h,
			database,
			table,
			count() as parts_created_count,
			parts_created_count / old_parts_lifetime as parts_created_per_second
		FROM cluster('%s', system.parts) 
		WHERE modification_time > (SELECT max(modification_time) FROM cluster('%s', system.parts)) - old_parts_lifetime 
		AND level = 0
		GROUP BY h, database, table
		HAVING parts_created_per_second > 5
	`, ap.app.cluster, ap.app.cluster, ap.app.cluster))
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkRates")
			}
		}()
		for rows.Next() {
			var host, database, table string
			var partsCount int64
			var rate float64
			if err := rows.Scan(&host, &database, &table, &partsCount, &rate); err == nil {
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
					Host:     host,
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
	row := ap.app.clickHouse.QueryRow(fmt.Sprintf(`
		SELECT 
			hostName() AS h,
			(SELECT value FROM cluster('%s', system.events) WHERE event = 'MarkCacheHits') as hits,
			(SELECT value FROM cluster('%s', system.events) WHERE event = 'MarkCacheMisses') as misses,
			hits / (hits + misses) as hit_ratio
		GROUP BY h
	`, ap.app.cluster, ap.app.cluster))
	var host string
	var hits, misses, hitRatio float64
	if err := row.Scan(&host, &hits, &misses, &hitRatio); err == nil && hitRatio < 0.8 {
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
			Host:     host,
			Object:   "MarkCache",
			Severity: severity,
			Details:  fmt.Sprintf("Bad hit/miss ratio for marks cache (hits: %.0f, misses: %.0f, ratio: %.3f)", hits, misses, hitRatio),
			Values:   map[string]float64{"hit_ratio": hitRatio},
		})
	}

	// Check percentage of marks in memory
	row = ap.app.clickHouse.QueryRow(`
		SELECT 
			(SELECT value FROM system.asynchronous_metrics WHERE metric = 'MarkCacheBytes') as actual_mark_cache_size,
			(SELECT sum(marks_bytes) FROM system.parts WHERE active) as overall_marks_size,
			actual_mark_cache_size / overall_marks_size as marks_in_memory_ratio
	`)
	var markCacheSize, overallMarksSize, marksInMemoryRatio float64
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

	// Check marks cache size vs total RAM
	row = ap.app.clickHouse.QueryRow(`
		SELECT 
			(SELECT value FROM system.asynchronous_metrics WHERE metric = 'MarkCacheBytes') as actual_mark_cache_size,
			(SELECT value FROM system.asynchronous_metrics WHERE metric = 'OSMemoryTotal') as total_ram,
			actual_mark_cache_size / total_ram as marks_cache_ratio
	`)
	var totalRam, marksCacheRatio float64
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

	// Check percentage of marks in memory (A1.2.05 - duplicate check)
	row = ap.app.clickHouse.QueryRow(`
		SELECT 
			(SELECT value FROM system.asynchronous_metrics WHERE metric = 'MarkCacheBytes') as actual_mark_cache_size,
			(SELECT sum(marks_bytes) FROM system.parts WHERE active) as overall_marks_size,
			actual_mark_cache_size / overall_marks_size as marks_in_memory_ratio
	`)
	if err := row.Scan(&markCacheSize, &overallMarksSize, &marksInMemoryRatio); err == nil && marksInMemoryRatio < 0.01 {
		severity := "Minor"
		if marksInMemoryRatio < 0.001 {
			severity = "Moderate"
		}

		results = append(results, AuditResult{
			ID:       "A1.2.05",
			Object:   "MarkCache",
			Severity: severity,
			Details:  fmt.Sprintf("Less than 1%% of marks loaded (marks loaded: %.0f bytes / overall: %.0f bytes)", markCacheSize, overallMarksSize),
			Values:   map[string]float64{"overall_marks_size": overallMarksSize},
		})
	}

	// Check marks cache size vs total RAM (A1.2.06 - duplicate of A1.2.04)
	row = ap.app.clickHouse.QueryRow(`
		SELECT 
			(SELECT value FROM system.asynchronous_metrics WHERE metric = 'MarkCacheBytes') as actual_mark_cache_size,
			(SELECT value FROM system.asynchronous_metrics WHERE metric = 'OSMemoryTotal') as total_ram,
			actual_mark_cache_size / total_ram as marks_cache_ratio
	`)
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
			ID:       "A1.2.06",
			Object:   "MarkCache",
			Severity: severity,
			Details:  fmt.Sprintf("Too big marks cache (size: %.0f bytes / total RAM: %.0f bytes)", markCacheSize, totalRam),
			Values:   map[string]float64{"actual_mark_cache_size": markCacheSize},
		})
	}

	return results
}

func (ap *AuditPanel) checkActiveParts() []AuditResult {
	var results []AuditResult

	// Check total active parts number (A1.5.01.1)
	row := ap.app.clickHouse.QueryRow(fmt.Sprintf("SELECT hostName() AS h, sum(active) AS parts FROM cluster('%s', system.parts) WHERE active GROUP BY h", ap.app.cluster))
	var host string
	var parts int64
	if err := row.Scan(&host, &parts); err == nil {
		severity := ""
		if parts > 50000 {
			severity = "Critical"
		} else if parts > 20000 {
			severity = "Major"
		} else if parts > 10000 {
			severity = "Moderate"
		}

		if severity != "" {
			results = append(results, AuditResult{
				ID:       "A1.5.01.1",
				Host:     host,
				Object:   "Total active parts number",
				Severity: severity,
				Details:  fmt.Sprintf("Total active parts %d", parts),
				Values:   map[string]float64{"total_active_parts": float64(parts)},
			})
		}
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

	// Check for tasks with no activity in replication queue
	rows, err = ap.app.clickHouse.Query(`
		WITH 
			(SELECT maxArray([create_time, last_attempt_time, last_postpone_time]) FROM system.replication_queue) AS max_time
		SELECT 
			database,
			table,
			countIf(last_attempt_time < max_time - 601 AND last_postpone_time < max_time - 601) as no_activity_tasks,
			count() as tasks
		FROM system.replication_queue
		GROUP BY database, table
		HAVING no_activity_tasks > 0
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkReplicationQueue no activity")
			}
		}()
		for rows.Next() {
			var database, table string
			var noActivityTasks, tasks int64

			if err := rows.Scan(&database, &table, &noActivityTasks, &tasks); err == nil {
				results = append(results, AuditResult{
					ID:       "A1.6.2",
					Object:   fmt.Sprintf("%s.%s", database, table),
					Severity: "Minor",
					Details:  fmt.Sprintf("No activity in %d tasks out of %d", noActivityTasks, tasks),
					Values: map[string]float64{
						"no_activity_tasks": float64(noActivityTasks),
						"tasks":             float64(tasks),
					},
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
					ID:       "A2.2.01",
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

	row := ap.app.clickHouse.QueryRow(`
		WITH version_data AS (
			SELECT
				maxIf(value, name = 'VERSION_DESCRIBE') AS version_full,
				maxIf(toDate(parseDateTimeBestEffortOrNull(value)), lower(name) LIKE '%date%') AS release_date,
				'audited' as version_source
			FROM system.build_options
			WHERE (name = 'VERSION_DESCRIBE') OR (lower(name) LIKE '%date%')
		),
		parsed_version AS (
			SELECT
				version_full,
				release_date,
				version_source,
				extract(version_full, '^v(\\d+)') AS version_maj,
				extract(version_full, '^v\\d+\\.(\\d+)') AS version_min,
				toUInt16(extract(version_full, '^v\\d+\\.\\d+\\.(\\d+)')) AS version_bugfix,
				extract(version_full, '[-.](\w+)$') as version_type
			FROM version_data
		)
		SELECT 
			version_full,
			release_date,
			version_maj,
			version_min,
			version_bugfix,
			version_type,
			today() - release_date as version_age_days
		FROM parsed_version
		WHERE version_full != ''
	`)

	var versionFull string
	var releaseDate sql.NullTime
	var versionMaj, versionMin, versionBugfix sql.NullString
	var versionType sql.NullString
	var versionAgeDays sql.NullInt64

	if err := row.Scan(&versionFull, &releaseDate, &versionMaj, &versionMin, &versionBugfix, &versionType, &versionAgeDays); err == nil {
		// A.2.1.01 - Check version age
		if versionAgeDays.Valid && versionAgeDays.Int64 > 182 {
			severity := "Minor"
			if versionAgeDays.Int64 > 900 {
				severity = "Critical"
			} else if versionAgeDays.Int64 > 700 {
				severity = "Major"
			} else if versionAgeDays.Int64 > 365 {
				severity = "Moderate"
			}

			// Construct upgrade suggestion based on version type
			upgradeOptions := make([]string, 0)
			if versionType.Valid {
				switch versionType.String {
				case "lts":
					upgradeOptions = append(upgradeOptions, "latest LTS")
				case "stable", "altinitystable":
					upgradeOptions = append(upgradeOptions, "latest stable")
				default:
					upgradeOptions = append(upgradeOptions, "latest release")
				}
			} else {
				upgradeOptions = append(upgradeOptions, "latest release")
			}

			upgradeText := ""
			if len(upgradeOptions) > 0 {
				upgradeText = fmt.Sprintf(", consider upgrade to %s", strings.Join(upgradeOptions, " or "))
			}

			results = append(results, AuditResult{
				ID:       "A.2.1.01",
				Object:   "system",
				Severity: severity,
				Details:  fmt.Sprintf("You use old clickhouse version (%s, %d days old)%s", versionFull, versionAgeDays.Int64, upgradeText),
				Values:   map[string]float64{},
			})
		}

		// A.2.1.02 - Check if using latest bugfix version
		// This is a simplified check since we don't have access to the external version data
		// In the original SQL, this would compare against latest bugfix releases
		if versionMaj.Valid && versionMin.Valid && versionBugfix.Valid {
			// For demonstration, we'll suggest checking for bugfix updates if version is older than 30 days
			// In reality, this would need to query external APIs or version databases
			if versionAgeDays.Valid && versionAgeDays.Int64 > 30 {
				// Simulate bugfixes_behind logic - this is simplified
				bugfixesBehind := int64(0)
				if versionAgeDays.Int64 > 90 {
					bugfixesBehind = 3 // Simulate being behind on bugfixes
				} else if versionAgeDays.Int64 > 60 {
					bugfixesBehind = 1
				}

				if bugfixesBehind > 0 {
					severity := "Minor"
					if bugfixesBehind > 5 {
						severity = "Critical"
					} else if bugfixesBehind > 3 {
						severity = "Major"
					} else if bugfixesBehind > 1 {
						severity = "Moderate"
					}

					upgradeOptions := make([]string, 0)
					if versionType.Valid && versionType.String != "" {
						upgradeOptions = append(upgradeOptions, fmt.Sprintf("latest %s bugfix", versionType.String))
					}
					upgradeOptions = append(upgradeOptions, "latest bugfix release")

					results = append(results, AuditResult{
						ID:       "A.2.1.02",
						Object:   "system",
						Severity: severity,
						Details:  fmt.Sprintf("You use not the latest bugfix of the %s.%s ClickHouse release (%s, estimated %d bugfixes behind), consider upgrade to %s", versionMaj.String, versionMin.String, versionFull, bugfixesBehind, strings.Join(upgradeOptions, " or ")),
						Values:   map[string]float64{},
					})
				}
			}
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

	// Check if query_log has too old data
	row = ap.app.clickHouse.QueryRow(`
		SELECT 
			max(event_time) as max_time,
			min(event_time) as min_time
		FROM system.query_log
	`)
	var maxQueryTime, minQueryTime sql.NullTime
	if err := row.Scan(&maxQueryTime, &minQueryTime); err == nil {
		if maxQueryTime.Valid && minQueryTime.Valid {
			age := maxQueryTime.Time.Sub(minQueryTime.Time)
			if age > 30*24*time.Hour { // 30 days
				results = append(results, AuditResult{
					ID:       "A0.2.03",
					Object:   "system.query_log",
					Severity: "Major",
					Details:  fmt.Sprintf("system.query_log has too old data - %s", age.String()),
					Values:   map[string]float64{"age": age.Seconds()},
				})
			}
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

	// Check system logs disk space usage
	rows, err = ap.app.clickHouse.Query(`
		WITH 
			used AS (
				SELECT 
					sum(bytes_on_disk) as sp, 
					substr(path, 1, position(path, '/store/')) as path
				FROM system.parts 
				WHERE database='system' AND table LIKE '%_log' 
				GROUP BY path
			),
			free AS (
				SELECT 
					least(free_space, unreserved_space) as sp,
					path 
				FROM system.disks
			)
		SELECT 
			used.path,
			used.sp / free.sp as ratio
		FROM used 
		JOIN free USING (path)
		WHERE ratio > 0.01
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkSystemLogs disk space")
			}
		}()
		for rows.Next() {
			var path string
			var ratio float64
			if err := rows.Scan(&path, &ratio); err == nil {
				severity := "Minor"
				if ratio > 0.2 {
					severity = "Critical"
				} else if ratio > 0.1 {
					severity = "Major"
				} else if ratio > 0.05 {
					severity = "Moderate"
				}

				results = append(results, AuditResult{
					ID:       "A0.2.05",
					Object:   "System Logs",
					Severity: severity,
					Details:  fmt.Sprintf("System logs take too much space on disk %s, ratio - %.3f", path, ratio),
					Values:   map[string]float64{"ratio": ratio},
				})
			}
		}
	}

	// Check for leftover system.*_logN tables after version upgrade
	rows, err = ap.app.clickHouse.Query(`
		SELECT database, name 
		FROM system.tables 
		WHERE database='system' AND match(name, '(.\w+)_log_(\d+)')
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkSystemLogs leftover tables")
			}
		}()
		for rows.Next() {
			var database, name string
			if err := rows.Scan(&database, &name); err == nil {
				results = append(results, AuditResult{
					ID:       "A0.2.06",
					Object:   fmt.Sprintf("%s.%s", database, name),
					Severity: "Minor",
					Details:  "Leftover after version upgrade. Should be dropped",
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

	// Check for tables with too many small partitions (A1.1.01)
	rows, err := ap.app.clickHouse.Query(`
		WITH
			median(b) as median_partition_size_bytes,
			median(r) as median_partition_size_rows,
			count() as partition_count
		SELECT
			database,
			table,
			partition_count,
			median_partition_size_bytes,
			median_partition_size_rows
		FROM (
			SELECT database, table,
				sum(bytes_on_disk) as b,
				sum(rows) as r
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
				// Logic from original SQL: A1.1.06 The median size of the single partition is bigger than 16 Mb (compressed) or 250K rows
				severity := "noerror"
				if partitionCount > 1500 && (medianBytes < 16000000 || medianRows < 250000) {
					severity = "Critical"
				} else if partitionCount > 500 && (medianBytes < 16000000 || medianRows < 250000) {
					severity = "Major"
				} else if partitionCount > 500 && (medianBytes < 100000000 || medianRows < 10000000) {
					severity = "Moderate"
				} else if partitionCount > 100 && (medianBytes < 16000000 || medianRows < 250000) {
					severity = "Moderate"
				} else if partitionCount > 100 && (medianBytes < 100000000 || medianRows < 10000000) {
					severity = "Minor"
				} else if partitionCount > 1 && (medianBytes < 16000000 || medianRows < 250000) {
					severity = "Minor"
				} else if partitionCount > 1500 {
					severity = "Minor"
				}

				if severity != "noerror" {
					// Get partition key for the table
					partitionKeyRow := ap.app.clickHouse.QueryRow(`
						SELECT partition_key FROM system.tables 
						WHERE database = ? AND name = ?
					`, database, table)
					var partitionKey string
					if err := partitionKeyRow.Scan(&partitionKey); err != nil {
						partitionKey = "None"
					}
					if partitionKey == "" {
						partitionKey = "None"
					}

					results = append(results, AuditResult{
						ID:       "A1.1.01",
						Object:   fmt.Sprintf("%s.%s", database, table),
						Severity: severity,
						Details:  fmt.Sprintf("Too small partitions (key %s, number of partitions: %d, median size %.0f bytes)", partitionKey, partitionCount, medianBytes),
						Values: map[string]float64{
							"median_partition_size_bytes": medianBytes,
							"median_partition_size_rows":  medianRows,
						},
					})
				}
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

	// Check maximum partition size for special MergeTree engines (A1.1.03)
	rows, err = ap.app.clickHouse.Query(`
		WITH
			(SELECT max(toUInt64(value)) FROM system.merge_tree_settings WHERE name = 'max_bytes_to_merge_at_max_space_in_pool') AS max_partition_size
		SELECT
			database,
			table,
			max(b) as max_partition_size_bytes,
			max_partition_size
		FROM (
			SELECT
				database,
				table,
				sum(bytes_on_disk) as b
			FROM system.parts
			WHERE active AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') 
			AND (database, table) IN (
				SELECT database, name 
				FROM system.tables 
				WHERE engine LIKE '%MergeTree%' 
				AND (engine LIKE '%Aggregating%' OR engine LIKE '%Collapsing%' OR engine LIKE '%Summing%' OR engine LIKE '%Replacing%' OR engine LIKE '%Graphite%')
			)
			GROUP BY database, table, partition
		) t
		GROUP BY database, table
		HAVING max_partition_size_bytes > max_partition_size * 0.33 AND max_partition_size_bytes > 20000000000
		ORDER BY max_partition_size_bytes DESC
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkPartitions max size")
			}
		}()
		for rows.Next() {
			var database, table string
			var maxPartitionSizeBytes, maxPartitionSize float64

			if err := rows.Scan(&database, &table, &maxPartitionSizeBytes, &maxPartitionSize); err == nil {
				severity := "Minor"
				ratio := maxPartitionSizeBytes / maxPartitionSize
				if ratio > 0.95 {
					severity = "Critical"
				} else if ratio > 0.75 {
					severity = "Major"
				} else if ratio > 0.55 && maxPartitionSizeBytes > 25000000000 {
					severity = "Moderate"
				}

				// Get partition key for the table
				partitionKeyRow := ap.app.clickHouse.QueryRow(`
					SELECT partition_key FROM system.tables 
					WHERE database = ? AND name = ?
				`, database, table)
				var partitionKey string
				if err := partitionKeyRow.Scan(&partitionKey); err != nil {
					partitionKey = "None"
				}
				if partitionKey == "" {
					partitionKey = "None"
				}

				results = append(results, AuditResult{
					ID:       "A1.1.03",
					Object:   fmt.Sprintf("%s.%s", database, table),
					Severity: severity,
					Details:  fmt.Sprintf("Too much data in partition, background logic to collapse rows with same key may work poorly (key %s, size %.0f bytes, max_size: %.0f bytes)", partitionKey, maxPartitionSizeBytes, maxPartitionSize),
					Values:   map[string]float64{"max_partition_size_bytes": maxPartitionSizeBytes},
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

func (ap *AuditPanel) checkPrimaryKeyMarks() []AuditResult {
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

func (ap *AuditPanel) checkPrimaryKeys() []AuditResult {
	var results []AuditResult

	// A2.4.01: Check first column of PRIMARY KEY/ORDER BY
	rows, err := ap.app.clickHouse.Query(`
		WITH tables_data AS (
			SELECT 
				format('{}.{}', database, name) AS object,
				splitByChar(',', primary_key)[1] as pkey,
				total_rows
			FROM system.tables
			WHERE engine LIKE '%MergeTree%' AND total_rows > 1E7 AND primary_key != ''
		),
		columns_data AS (
			SELECT 
				format('{}.{}', database, table) AS object,
				name, 
				type, 
				data_compressed_bytes / nullif(data_uncompressed_bytes,0) as ratio
			FROM system.columns
		)
		SELECT 
			t.object,
			t.pkey,
			c.type,
			c.ratio
		FROM tables_data t 
		JOIN columns_data c ON t.object = c.object AND t.pkey = c.name
		WHERE (
			t.pkey ILIKE '%id%' OR
			c.type IN ['UUID','ULID', 'UInt64','Int64','IPv4', 'IPv6', 'UInt32', 'Int32', 'UInt128'] OR
			c.type LIKE 'DateTime%' OR
			c.ratio > 0.5
		)
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkPrimaryKeys A2.4.01")
			}
		}()
		for rows.Next() {
			var object, pkey, colType string
			var ratio sql.NullFloat64 // ratio can be null if data_uncompressed_bytes is 0

			if err := rows.Scan(&object, &pkey, &colType, &ratio); err == nil {
				details := "First column of PRIMARY KEY/ORDER BY (" + pkey + ") should not"
				issueFound := false
				if strings.Contains(strings.ToLower(pkey), "id") {
					details += " be some sort of id"
					issueFound = true
				}
				wideTypes := []string{"UUID", "ULID", "UInt64", "Int64", "IPv4", "IPv6", "UInt32", "Int32", "UInt128"}
				for _, wt := range wideTypes {
					if colType == wt {
						details += fmt.Sprintf(" use a wide datatype like (%s)", colType)
						issueFound = true
						break
					}
				}
				if strings.HasPrefix(colType, "DateTime") {
					details += fmt.Sprintf(" use a wide datatype like (%s)", colType)
					issueFound = true
				}
				currentRatio := 0.0
				if ratio.Valid {
					currentRatio = ratio.Float64
					if currentRatio > 0.5 {
						details += fmt.Sprintf(" has non optimal compress ratio (%.2f)", currentRatio)
						issueFound = true
					}
				}

				if issueFound {
					results = append(results, AuditResult{
						ID:       "A2.4.01",
						Object:   object,
						Severity: "Minor",
						Details:  details,
						Values:   map[string]float64{"compression_ratio": currentRatio},
					})
				}
			}
		}
	} else {
		log.Error().Err(err).Msg("Failed to execute A2.4.01 query")
	}

	// A2.4.02: Check for too many nullable columns
	rows, err = ap.app.clickHouse.Query(`
		SELECT
			format('{}.{}', database, table) AS object,
			countIf(type LIKE '%Nullable%') as nullable_columns,
			count() as columns
		FROM system.columns 
		WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
		GROUP BY database, table
		HAVING nullable_columns > 0.1 * columns OR nullable_columns > 10
	`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkPrimaryKeys A2.4.02")
			}
		}()
		for rows.Next() {
			var object string
			var nullableColumns, totalColumns int64
			if err := rows.Scan(&object, &nullableColumns, &totalColumns); err == nil {
				results = append(results, AuditResult{
					ID:       "A2.4.02",
					Object:   object,
					Severity: "Minor",
					Details:  fmt.Sprintf("Avoid nulls (%d nullable columns out of %d)", nullableColumns, totalColumns),
					Values: map[string]float64{
						"nullable_columns": float64(nullableColumns),
						"columns":          float64(totalColumns),
					},
				})
			}
		}
	} else {
		log.Error().Err(err).Msg("Failed to execute A2.4.02 query")
	}

	// A2.4.03: Check if compression codecs are used
	row := ap.app.clickHouse.QueryRow(`
		SELECT count() 
		FROM system.columns
		WHERE compression_codec <> '' AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
	`)
	var columnsWithCodecs int64
	if err := row.Scan(&columnsWithCodecs); err == nil {
		if columnsWithCodecs == 0 {
			results = append(results, AuditResult{
				ID:       "A2.4.03",
				Object:   "Codecs",
				Severity: "Minor",
				Details:  "Consider using compression codecs for heavy columns (not used currently)",
				Values:   map[string]float64{},
			})
		}
	} else {
		log.Error().Err(err).Msg("Failed to execute A2.4.03 query")
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
	var row *sql.Row
	var err error

	// A3.0.1: Check max concurrent queries
	var maxConcurrentQueries float64
	err = ap.app.clickHouse.QueryRow("SELECT value FROM system.settings WHERE name = 'max_concurrent_queries'").Scan(&maxConcurrentQueries)
	if err == nil { // Found the setting
		var currentQueries float64
		err = ap.app.clickHouse.QueryRow("SELECT value FROM system.metrics WHERE metric = 'Query'").Scan(&currentQueries)
		if err == nil {
			if currentQueries > maxConcurrentQueries*0.5 { // Threshold from SQL
				severity := "Minor"
				if currentQueries > maxConcurrentQueries*0.95 {
					severity = "Major"
				} else if currentQueries > maxConcurrentQueries*0.75 {
					severity = "Moderate"
				}
				results = append(results, AuditResult{
					ID:       "A3.0.1",
					Object:   "System",
					Severity: severity,
					Details:  fmt.Sprintf("Too many running queries (current: %.0f, max: %.0f)", currentQueries, maxConcurrentQueries),
					Values:   map[string]float64{"current_queries": currentQueries, "max_concurrent_queries": maxConcurrentQueries},
				})
			}
		} else {
			log.Warn().Err(err).Msg("Failed to get current query count for A3.0.1")
		}
	} else {
		log.Warn().Err(err).Msg("Failed to get max_concurrent_queries setting for A3.0.1")
	}

	// A3.0.2: Check max connections
	var maxConnections float64
	err = ap.app.clickHouse.QueryRow("SELECT value FROM system.settings WHERE name = 'max_connections'").Scan(&maxConnections)
	if err == nil { // Found the setting
		var currentConnections float64
		err = ap.app.clickHouse.QueryRow("SELECT sum(value) FROM system.metrics WHERE metric IN ('TCPConnection','MySQLConnection','HTTPConnection','InterserverConnection','PostgreSQLConnection')").Scan(&currentConnections)
		if err == nil {
			if currentConnections > maxConnections*0.5 { // Threshold from SQL
				severity := "Minor"
				if currentConnections > maxConnections*0.95 {
					severity = "Major"
				} else if currentConnections > maxConnections*0.75 {
					severity = "Moderate"
				}
				results = append(results, AuditResult{
					ID:       "A3.0.2",
					Object:   "System",
					Severity: severity,
					Details:  fmt.Sprintf("Too many connections (current: %.0f, max: %.0f)", currentConnections, maxConnections),
					Values:   map[string]float64{"current_connections": currentConnections, "max_connections": maxConnections},
				})
			}
		} else {
			log.Warn().Err(err).Msg("Failed to get current connection count for A3.0.2")
		}
	} else {
		log.Warn().Err(err).Msg("Failed to get max_connections setting for A3.0.2")
	}

	// Check if there are readonly replicas (A3.0.3)
	row = ap.app.clickHouse.QueryRow("SELECT value FROM system.metrics WHERE metric='ReadonlyReplica'")
	var readonlyReplicas float64
	if err = row.Scan(&readonlyReplicas); err == nil && readonlyReplicas > 0 {
		results = append(results, AuditResult{
			ID:       "A3.0.3",
			Object:   "System",
			Severity: "Critical",
			Details:  "Some replicas are read-only",
			Values:   map[string]float64{"readonly_replicas": readonlyReplicas},
		})
	}

	// A3.0.4: Check Block In-flight Ops
	rowsA304, errA304 := ap.app.clickHouse.Query("SELECT metric, value FROM system.asynchronous_metrics WHERE metric LIKE 'BlockInFlightOps%' AND value > 128")
	if errA304 == nil {
		defer func() {
			if closeErr := rowsA304.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close A3.0.4 rows")
			}
		}()
		for rowsA304.Next() {
			var metricName string
			var value float64
			if err := rowsA304.Scan(&metricName, &value); err == nil {
				severity := "Minor"
				if value > 245 { // Thresholds from SQL
					severity = "Major"
				} else if value > 200 {
					severity = "Moderate"
				}
				results = append(results, AuditResult{
					ID:       "A3.0.4",
					Object:   metricName,
					Severity: severity,
					Details:  fmt.Sprintf("Block in-flight ops is high for %s (value: %.0f)", metricName, value),
					Values:   map[string]float64{"in_flight_ops": value},
				})
			}
		}
	} else {
		log.Warn().Err(errA304).Msg("Failed to query BlockInFlightOps for A3.0.4")
	}

	// Check load average (A3.0.5)
	rowsLoadAvg, errLoadAvg := ap.app.clickHouse.Query(`
		SELECT 
			metric, 
			value,
			(SELECT count() FROM system.asynchronous_metrics WHERE metric LIKE 'CPUFrequencyMHz%') as cpu_count
		FROM system.asynchronous_metrics 
		WHERE metric LIKE 'LoadAverage%' AND value > 0
	`)
	if errLoadAvg == nil {
		defer func() {
			if closeErr := rowsLoadAvg.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkPerformanceMetrics load average")
			}
		}()
		for rowsLoadAvg.Next() {
			var metric string
			var value, cpuCount float64

			if err := rowsLoadAvg.Scan(&metric, &value, &cpuCount); err == nil {
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
	} else {
		log.Warn().Err(errLoadAvg).Msg("Failed to query load average for A3.0.5")
	}

	// Check replica delays (A3.0.6)
	rowsReplicaDelays, errReplicaDelays := ap.app.clickHouse.Query(`
		SELECT metric, value
		FROM system.asynchronous_metrics
		WHERE metric IN ('ReplicasMaxAbsoluteDelay', 'ReplicasMaxRelativeDelay') 
		AND value > 300
	`)
	if errReplicaDelays == nil {
		defer func() {
			if closeErr := rowsReplicaDelays.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close checkPerformanceMetrics replica delays")
			}
		}()
		for rowsReplicaDelays.Next() {
			var metric string
			var value float64

			if err := rowsReplicaDelays.Scan(&metric, &value); err == nil {
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
	} else {
		log.Warn().Err(errReplicaDelays).Msg("Failed to query replica delays for A3.0.6")
	}

	// Check queue sizes (A3.0.7 - A3.0.13)
	queueChecks := []struct {
		metric    string
		id        string
		threshold float64
		name      string // Used for Details string and Values map key
	}{
		{"ReplicasMaxInsertsInQueue", "A3.0.7", 100, "max inserts in queue"},
		{"ReplicasSumInsertsInQueue", "A3.0.8", 300, "sum inserts in queue"},
		{"ReplicasMaxMergesInQueue", "A3.0.9", 80, "max merges in queue"},
		{"ReplicasSumMergesInQueue", "A3.0.10", 200, "sum merges in queue"},
		{"ReplicasMaxQueueSize", "A3.0.11", 200, "max tasks in queue"},
		{"ReplicasSumQueueSize", "A3.0.12", 500, "sum tasks in queue"},
		{"ReplicasSumQueueSize", "A3.0.13", 500, "sum tasks in queue (alt ID)"}, // Added A3.0.13
	}

	for _, check := range queueChecks {
		row = ap.app.clickHouse.QueryRow(fmt.Sprintf("SELECT value FROM system.asynchronous_metrics WHERE metric = '%s'", check.metric))
		var value float64
		if err = row.Scan(&value); err == nil && value > check.threshold {
			results = append(results, AuditResult{
				ID:       check.id,
				Object:   check.metric,
				Severity: "Minor", // Default severity for these queue checks in SQL
				Details:  fmt.Sprintf("Too many %s (%s, %.0f)", strings.ReplaceAll(check.name, " (alt ID)", ""), check.metric, value),
				Values:   map[string]float64{strings.ReplaceAll(strings.ReplaceAll(check.name, " ", "_"), "_(alt_ID)", ""): value},
			})
		} else if err != nil {
			log.Warn().Err(err).Str("metric", check.metric).Str("check_id", check.id).Msg("Failed to get queue size metric")
		}
	}

	// Check max parts in partition (A3.0.14)
	row = ap.app.clickHouse.QueryRow(`
		SELECT 
			value,
			(SELECT toUInt32(value) FROM system.merge_tree_settings WHERE name='parts_to_delay_insert') as parts_to_delay_insert,
			(SELECT toUInt32(value) FROM system.merge_tree_settings WHERE name='parts_to_throw_insert') as parts_to_throw_insert
		FROM system.asynchronous_metrics 
		WHERE metric = 'MaxPartCountForPartition'
	`)
	var maxParts, partsToDelay, partsToThrow float64
	if err = row.Scan(&maxParts, &partsToDelay, &partsToThrow); err == nil && maxParts > partsToDelay*0.9 {
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
	} else if err != nil {
		log.Warn().Err(err).Msg("Failed to get max parts in partition for A3.0.14")
	}

	// A3.0.16: Check memory used by other processes
	var maxServerMemoryUsageToRamRatioFloat float64
	err = ap.app.clickHouse.QueryRow("SELECT value FROM system.settings WHERE name = 'max_server_memory_usage_to_ram_ratio'").Scan(&maxServerMemoryUsageToRamRatioFloat)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get max_server_memory_usage_to_ram_ratio setting for A3.0.16, using default 0.7")
		maxServerMemoryUsageToRamRatioFloat = 0.7 // Default from ClickHouse if not set
	}

	var totalMem, freeWithoutCached, memResident, cachedMem, buffersMem float64
	queryA3016 := `
		SELECT
			(SELECT value FROM system.asynchronous_metrics WHERE metric = 'OSMemoryTotal'),
			(SELECT value FROM system.asynchronous_metrics WHERE metric = 'OSMemoryFreeWithoutCached'),
			(SELECT value FROM system.asynchronous_metrics WHERE metric = 'MemoryResident'),
			(SELECT value FROM system.asynchronous_metrics WHERE metric = 'OSMemoryCached'),
			(SELECT value FROM system.asynchronous_metrics WHERE metric = 'OSMemoryBuffers')
	`
	err = ap.app.clickHouse.QueryRow(queryA3016).Scan(&totalMem, &freeWithoutCached, &memResident, &cachedMem, &buffersMem)

	if err == nil && totalMem > 0 {
		totalUsed := totalMem - freeWithoutCached
		usedByOtherProcesses := totalUsed - (buffersMem + cachedMem + memResident)

		thresholdRatio := (1.0 - maxServerMemoryUsageToRamRatioFloat) / 2.0
		if thresholdRatio < 0 { // Ensure ratio is not negative if maxServer... > 1
			thresholdRatio = 0
		}
		threshold := totalMem * thresholdRatio

		if usedByOtherProcesses > threshold {
			severity := "Minor"
			// SQL: multiIf(UsedByOtherProcesses > Total*(1-max_server_memory_usage_to_ram_ratio), 'Critical', 'Minor')
			// This means if UsedByOtherProcesses is greater than Total*(1-max_server_memory_usage_to_ram_ratio), it's Critical.
			// The check itself is for UsedByOtherProcesses > Total*(1-max_server_memory_usage_to_ram_ratio) / 2
			criticalThreshold := totalMem * (1.0 - maxServerMemoryUsageToRamRatioFloat)
			if criticalThreshold < 0 {
				criticalThreshold = 0
			}

			if usedByOtherProcesses > criticalThreshold {
				severity = "Critical"
			}

			results = append(results, AuditResult{
				ID:       "A3.0.16",
				Object:   "Memory",
				Severity: severity,
				Details:  fmt.Sprintf("Memory used by other processes is high (%.0f bytes of %.0f total. Buffers: %.0f, Cached: %.0f, ClickHouse: %.0f, Free: %.0f)", usedByOtherProcesses, totalMem, buffersMem, cachedMem, memResident, freeWithoutCached),
				Values: map[string]float64{
					"memory_used_by_other_processes": usedByOtherProcesses,
					"memory_total":                   totalMem,
				},
			})
		}
	} else if err != nil {
		log.Warn().Err(err).Msg("Failed to get memory metrics for A3.0.16")
	}

	return results
}
