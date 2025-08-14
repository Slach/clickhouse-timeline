package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/rs/zerolog/log"
)

// ShowExplain is the entry point for the explain flow.
// If categoryType == CategoryQueryHash (called from heatmap) we'll pre-fill the normalized hash.
// Otherwise, the selection form will be shown.
func (a *App) ShowExplain(categoryType CategoryType, categoryValue string, fromTime, toTime time.Time, cluster string) {
	// If not coming from heatmap (no prefilled normalized_query_hash), show selection form
	if categoryType != CategoryQueryHash {
		a.ShowExplainQuerySelectionForm(fromTime, toTime, cluster)
		return
	}

	// Coming from heatmap and categoryType == CategoryQueryHash
	// Open the selection form with normalized_query_hash pre-filled
	a.ShowExplainQuerySelectionFormWithPrefill(categoryValue, fromTime, toTime, cluster)
}

// ShowExplainQuerySelectionForm shows the form allowing to filter queries by normalized hash, tables and query_kind.
func (a *App) ShowExplainQuerySelectionForm(fromTime, toTime time.Time, cluster string) {
	a.ShowExplainQuerySelectionFormWithPrefill("", fromTime, toTime, cluster)
}

// ShowExplainQuerySelectionFormWithPrefill shows the same form but with an optional prefilled normalized_query_hash.
func (a *App) ShowExplainQuerySelectionFormWithPrefill(prefillHash string, fromTime, toTime time.Time, cluster string) {
	fromStr := fromTime.Format("2006-01-02 15:04:05 -07:00")
	toStr := toTime.Format("2006-01-02 15:04:05 -07:00")

	// UI state
	selectedTables := map[string]bool{}
	selectedKinds := map[string]bool{}

	form := tview.NewForm()
	form.SetBorder(true).SetTitle("Explain Query - Selection").SetTitleAlign(tview.AlignLeft)

	hashField := tview.NewInputField().SetLabel("normalized_query_hash: ").SetText(prefillHash)
	form.AddFormItem(hashField)

	// Separate output area for explain flow (do not use a.mainView)
	output := tview.NewTextView()
	output.SetDynamicColors(true)
	output.SetWrap(true)
	output.SetWordWrap(true)
	output.SetBorder(true)
	output.SetTitle("Explain Output")

	// Placeholders for lists
	tablesList := tview.NewList().ShowSecondaryText(false)
	kindList := tview.NewList().ShowSecondaryText(false)

	// Helper to toggle selection in a list and reflect prefix
	var toggleSelect func(list *tview.List, items []string, selMap map[string]bool)
	toggleSelect = func(list *tview.List, items []string, selMap map[string]bool) {
		list.Clear()
		for _, it := range items {
			prefix := " [ ] "
			if selMap[it] {
				prefix = " [x] "
			}
			list.AddItem(prefix+it, "", 0, func(i string) func() {
				return func() {
					selMap[i] = !selMap[i]
					// refresh lists
					toggleSelect(list, items, selMap)
				}
			}(it))
		}
	}

	// Load options button - queries DB to fill tables and kinds
	var loadFunc func()
	loadFunc = func() {
		a.tviewApp.QueueUpdateDraw(func() {
			output.SetText("Loading tables and query kinds...")
		})

		// Build optional hash filter
		hashVal := strings.TrimSpace(hashField.GetText())
		var hashFilter string
		if hashVal != "" {
			hashFilter = fmt.Sprintf("AND normalized_query_hash = '%s'", strings.ReplaceAll(hashVal, "'", "''"))
		}

		// Query for tables
		tablesQuery := fmt.Sprintf(
			"SELECT DISTINCT arrayJoin(tables) AS t FROM clusterAllReplicas('%s', merge(system,'^query_log')) WHERE event_date >= toDate(parseDateTimeBestEffort('%s')) AND event_date <= toDate(parseDateTimeBestEffort('%s')) AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s') %s ORDER BY t",
			cluster, fromStr, toStr, fromStr, toStr, hashFilter,
		)
		rows, err := a.clickHouse.Query(tablesQuery)
		if err != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				output.SetText(fmt.Sprintf("Error loading tables: %v\n%s", err, tablesQuery))
			})
			return
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close tables rows")
			}
		}()

		var tables []string
		for rows.Next() {
			var t string
			if err := rows.Scan(&t); err != nil {
				log.Error().Err(err).Msg("scan table")
				continue
			}
			tables = append(tables, t)
		}

		// Query for query_kind
		kindQuery := fmt.Sprintf(
			"SELECT DISTINCT query_kind FROM clusterAllReplicas('%s', merge(system,'^query_log')) WHERE event_date >= toDate(parseDateTimeBestEffort('%s')) AND event_date <= toDate(parseDateTimeBestEffort('%s')) AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s') %s ORDER BY query_kind",
			cluster, fromStr, toStr, fromStr, toStr, hashFilter,
		)
		kindRows, err := a.clickHouse.Query(kindQuery)
		if err != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				output.SetText(fmt.Sprintf("Error loading query kinds: %v\n%s", err, kindQuery))
			})
			return
		}
		defer func() {
			if closeErr := kindRows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close kind rows")
			}
		}()

		var kinds []string
		for kindRows.Next() {
			var k string
			if err := kindRows.Scan(&k); err != nil {
				log.Error().Err(err).Msg("scan kind")
				continue
			}
			kinds = append(kinds, k)
		}

		// Update UI lists
		a.tviewApp.QueueUpdateDraw(func() {
			if len(tables) == 0 {
				toggleSelect(tablesList, []string{"<no tables>"}, selectedTables)
			} else {
				toggleSelect(tablesList, tables, selectedTables)
			}
			if len(kinds) == 0 {
				toggleSelect(kindList, []string{"<no kinds>"}, selectedKinds)
			} else {
				toggleSelect(kindList, kinds, selectedKinds)
			}
			output.SetText("Options loaded. Toggle selections and press Search.")
		})
	}

	var searchFunc func()
	searchFunc = func() {
		// Build filters
		hashVal := strings.TrimSpace(hashField.GetText())
		var whereParts []string
		whereParts = append(whereParts, fmt.Sprintf("event_date >= toDate(parseDateTimeBestEffort('%s'))", fromStr))
		whereParts = append(whereParts, fmt.Sprintf("event_date <= toDate(parseDateTimeBestEffort('%s'))", toStr))
		whereParts = append(whereParts, fmt.Sprintf("event_time >= parseDateTimeBestEffort('%s')", fromStr))
		whereParts = append(whereParts, fmt.Sprintf("event_time <= parseDateTimeBestEffort('%s')", toStr))
		whereParts = append(whereParts, "type != 'QueryStart'")

		if hashVal != "" {
			whereParts = append(whereParts, fmt.Sprintf("normalized_query_hash = '%s'", strings.ReplaceAll(hashVal, "'", "''")))
		}

		// tables
		var chosenTables []string
		for t, sel := range selectedTables {
			if sel {
				chosenTables = append(chosenTables, t)
			}
		}
		if len(chosenTables) > 0 {
			// format as ['t1','t2']
			escaped := make([]string, 0, len(chosenTables))
			for _, tt := range chosenTables {
				escaped = append(escaped, fmt.Sprintf("'%s'", strings.ReplaceAll(tt, "'", "''")))
			}
			whereParts = append(whereParts, fmt.Sprintf("hasAny(tables, [%s])", strings.Join(escaped, ",")))
		}

		// kinds
		var chosenKinds []string
		for k, sel := range selectedKinds {
			if sel {
				chosenKinds = append(chosenKinds, k)
			}
		}
		if len(chosenKinds) > 0 {
			escaped := make([]string, 0, len(chosenKinds))
			for _, kk := range chosenKinds {
				escaped = append(escaped, fmt.Sprintf("'%s'", strings.ReplaceAll(kk, "'", "''")))
			}
			whereParts = append(whereParts, fmt.Sprintf("query_kind IN (%s)", strings.Join(escaped, ",")))
		}

		whereClause := strings.Join(whereParts, " AND ")

		query := fmt.Sprintf(
			"SELECT DISTINCT normalized_query_hash, normalizedQuery(query) AS q FROM clusterAllReplicas('%s', merge(system,'^query_log')) WHERE %s ORDER BY normalized_query_hash",
			cluster, whereClause,
		)

		rows, err := a.clickHouse.Query(query)
		if err != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				output.SetText(fmt.Sprintf("Error executing query: %v\n%s", err, query))
			})
			return
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close queries rows")
			}
		}()

		type qrow struct {
			hash string
			q    string
		}
		var results []qrow
		for rows.Next() {
			var h, q string
			if err := rows.Scan(&h, &q); err != nil {
				log.Error().Err(err).Msg("scan query row")
				continue
			}
			results = append(results, qrow{h, q})
		}

		// Show queries list with filter input
		a.tviewApp.QueueUpdateDraw(func() {
			if len(results) == 0 {
				output.SetText("No queries found with given filters")
				return
			}

			// Build UI: filter input on top + list
			filterInput := tview.NewInputField().SetLabel("/").SetFieldWidth(40)
			queriesList := tview.NewList().ShowSecondaryText(false)
			queriesList.SetBorder(true).SetTitle("Queries (Enter to inspect)")

			// Populate
			populate := func(filter string) {
				queriesList.Clear()
				lower := strings.ToLower(filter)
				for _, r := range results {
					if lower == "" || strings.Contains(strings.ToLower(r.q), lower) || strings.Contains(strings.ToLower(r.hash), lower) {
						display := fmt.Sprintf("%s : %s", r.hash, truncate(r.q, 120))
						// capture r for closure
						r := r
						queriesList.AddItem(display, "", 0, func() {
							// on enter - show percentile legend and allow drilling
							a.showExplainPercentiles(r.hash, r.q, fromTime, toTime, cluster, output)
						})
					}
				}
			}
			populate("")

			filterInput.SetChangedFunc(func(text string) {
				populate(text)
			})
			filterInput.SetDoneFunc(func(key tcell.Key) {
				if key == tcell.KeyEscape {
					a.pages.SwitchToPage("main")
				}
			})

			// Layout
			flex := tview.NewFlex().SetDirection(tview.FlexRow).
				AddItem(filterInput, 1, 0, true).
				AddItem(queriesList, 0, 1, true)

			a.pages.AddPage("explain_queries", flex, true, true)
			a.pages.SwitchToPage("explain_queries")
		})
	}

	var cancelFunc func()
	cancelFunc = func() {
		a.pages.SwitchToPage("main")
	}

	form.AddButton("Load options", loadFunc)
	form.AddButton("Search", searchFunc)
	form.AddButton("Cancel", cancelFunc)

	// Layout: left = form + lists, right = mainView for messages
	leftFlex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(form, 7, 0, true).
		AddItem(tview.NewTextView().SetText("Tables:"), 1, 0, false).
		AddItem(tablesList, 0, 1, false).
		AddItem(tview.NewTextView().SetText("Query kinds:"), 1, 0, false).
		AddItem(kindList, 0, 1, false)

	mainFlex := tview.NewFlex().SetDirection(tview.FlexColumn).
		AddItem(leftFlex, 0, 2, true).
		AddItem(output, 0, 1, false)

	a.pages.AddPage("explain", mainFlex, true, true)
	a.pages.SwitchToPage("explain")
	a.tviewApp.SetFocus(form)
}

// truncate utility
func truncate(s string, l int) string {
	if len(s) <= l {
		return s
	}
	return s[:l-1] + "…"
}

// showExplainPercentiles queries p50/p90/p99 and shows a simple legend modal, allowing to drill into percentile value.
func (a *App) showExplainPercentiles(hash, queryText string, fromTime, toTime time.Time, cluster string, output *tview.TextView) {
	fromStr := fromTime.Format("2006-01-02 15:04:05 -07:00")
	toStr := toTime.Format("2006-01-02 15:04:05 -07:00")

	q := fmt.Sprintf("SELECT quantile(0.5)(query_duration_ms) AS p50, quantile(0.9)(query_duration_ms) AS p90, quantile(0.99)(query_duration_ms) AS p99 FROM clusterAllReplicas('%s', merge(system,'^query_log')) WHERE event_date >= toDate(parseDateTimeBestEffort('%s')) AND event_date <= toDate(parseDateTimeBestEffort('%s')) AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s') AND normalized_query_hash = '%s' AND query_duration_ms > 0",
		cluster, fromStr, toStr, fromStr, toStr, strings.ReplaceAll(hash, "'", "''"),
	)

	rows, err := a.clickHouse.Query(q)
	if err != nil {
		a.tviewApp.QueueUpdateDraw(func() {
			output.SetText(fmt.Sprintf("Error fetching percentiles: %v\n%s", err, q))
		})
		return
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("can't close percentiles rows")
		}
	}()

	var p50, p90, p99 float64
	if rows.Next() {
		if err := rows.Scan(&p50, &p90, &p99); err != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Error scanning percentiles: %v", err))
			})
			return
		}
	}

	// Create modal with percentile values and allow selection
	modal := tview.NewModal().
		SetText(fmt.Sprintf("Percentiles for %s\n\np50: %.2f ms\np90: %.2f ms\np99: %.2f ms\n\nSelect percentile to show matching query", truncate(queryText, 200), p50, p90, p99)).
		AddButtons([]string{"p50", "p90", "p99", "Back"}).
		SetDoneFunc(func(idx int, label string) {
			switch label {
			case "p50":
				a.showExplainQueryByThreshold(hash, int64(p50), fromTime, toTime, cluster, output)
			case "p90":
				a.showExplainQueryByThreshold(hash, int64(p90), fromTime, toTime, cluster, output)
			case "p99":
				a.showExplainQueryByThreshold(hash, int64(p99), fromTime, toTime, cluster, output)
			default:
				a.pages.SwitchToPage("explain_queries")
			}
		})

	a.pages.AddPage("explain_percentiles", modal, true, true)
	a.pages.SwitchToPage("explain_percentiles")
}

// showExplainQueryByThreshold finds the top query above threshold and shows explain plans and query text.
func (a *App) showExplainQueryByThreshold(hash string, threshold int64, fromTime, toTime time.Time, cluster string, output *tview.TextView) {
	fromStr := fromTime.Format("2006-01-02 15:04:05 -07:00")
	toStr := toTime.Format("2006-01-02 15:04:05 -07:00")

	// Get top query above threshold
	q := fmt.Sprintf("SELECT query, query_duration_ms FROM clusterAllReplicas('%s', merge(system,'^query_log')) WHERE event_date >= toDate(parseDateTimeBestEffort('%s')) AND event_date <= toDate(parseDateTimeBestEffort('%s')) AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s') AND normalized_query_hash = '%s' AND query_duration_ms > %d ORDER BY query_duration_ms DESC LIMIT 1",
		cluster, fromStr, toStr, fromStr, toStr, strings.ReplaceAll(hash, "'", "''"), threshold,
	)

	rows, err := a.clickHouse.Query(q)
	if err != nil {
		a.tviewApp.QueueUpdateDraw(func() {
			output.SetText(fmt.Sprintf("Error fetching query: %v\n%s", err, q))
		})
		return
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("can't close top query rows")
		}
	}()

	var queryText string
	var duration float64
	if rows.Next() {
		if err := rows.Scan(&queryText, &duration); err != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				output.SetText(fmt.Sprintf("Error scanning top query: %v", err))
			})
			return
		}
	} else {
		a.tviewApp.QueueUpdateDraw(func() {
			output.SetText("No query found above threshold")
		})
		return
	}

	// Build explain queries
	explain1 := fmt.Sprintf("EXPLAIN PLAN indexes=1, projections=1 %s", queryText)
	explain2 := fmt.Sprintf("EXPLAIN PIPELINE %s", queryText)
	explain3 := fmt.Sprintf("EXPLAIN ESTIMATE %s", queryText)

	// Use QueryView to show the normalized query
	qv := widgets.NewQueryView()
	qv.SetSQL(queryText)
	qv.SetBorder(true).SetTitle("Query Text")

	// Three text areas for explain outputs (scrollable)
	ex1 := tview.NewTextView().SetWrap(true).SetDynamicColors(true)
	ex1.SetBorder(true).SetTitle("EXPLAIN PLAN indexes=1, projections=1")
	ex2 := tview.NewTextView().SetWrap(true).SetDynamicColors(true)
	ex2.SetBorder(true).SetTitle("EXPLAIN PIPELINE")
	ex3 := tview.NewTextView().SetWrap(true).SetDynamicColors(true)
	ex3.SetBorder(true).SetTitle("EXPLAIN ESTIMATE")

	a.pages.AddAndSwitchToPage("explain_loading", tview.NewModal().SetText("Running EXPLAINs...").AddButtons([]string{"OK"}), true)

	// Run explains (best-effort)
	go func() {
		// EXPLAIN PLAN (may be heavy); we just attempt to fetch textual output via clickhouse
		if rows1, err1 := a.clickHouse.Query(explain1); err1 == nil {
			var buf strings.Builder
			for rows1.Next() {
				var s string
				_ = rows1.Scan(&s)
				buf.WriteString(s)
				buf.WriteString("\n")
			}
			rows1.Close()
			a.tviewApp.QueueUpdateDraw(func() {
				ex1.SetText(buf.String())
			})
		} else {
			a.tviewApp.QueueUpdateDraw(func() {
				ex1.SetText(fmt.Sprintf("Error running explain: %v", err1))
			})
		}

		if rows2, err2 := a.clickHouse.Query(explain2); err2 == nil {
			var buf strings.Builder
			for rows2.Next() {
				var s string
				_ = rows2.Scan(&s)
				buf.WriteString(s)
				buf.WriteString("\n")
			}
			rows2.Close()
			a.tviewApp.QueueUpdateDraw(func() {
				ex2.SetText(buf.String())
			})
		} else {
			a.tviewApp.QueueUpdateDraw(func() {
				ex2.SetText(fmt.Sprintf("Error running explain: %v", err2))
			})
		}

		if rows3, err3 := a.clickHouse.Query(explain3); err3 == nil {
			var buf strings.Builder
			for rows3.Next() {
				var s string
				_ = rows3.Scan(&s)
				buf.WriteString(s)
				buf.WriteString("\n")
			}
			rows3.Close()
			a.tviewApp.QueueUpdateDraw(func() {
				ex3.SetText(buf.String())
			})
		} else {
			a.tviewApp.QueueUpdateDraw(func() {
				ex3.SetText(fmt.Sprintf("Error running explain: %v", err3))
			})
		}

		// Build final layout on UI goroutine
		a.tviewApp.QueueUpdateDraw(func() {
			// Three columns with query view on top or left
			rightFlex := tview.NewFlex().SetDirection(tview.FlexRow).
				AddItem(ex1, 0, 1, false).
				AddItem(ex2, 0, 1, false).
				AddItem(ex3, 0, 1, false)

			mainFlex := tview.NewFlex().SetDirection(tview.FlexColumn).
				AddItem(qv, 0, 1, false).
				AddItem(rightFlex, 0, 2, false)

			a.pages.AddPage("explain_result", mainFlex, true, true)
			a.pages.SwitchToPage("explain_result")
		})
	}()
}
