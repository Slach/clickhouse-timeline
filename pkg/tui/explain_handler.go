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
	form.SetBorder(false)

	hashField := tview.NewInputField().SetLabel("normalized_query_hash: ").SetText(prefillHash)
	form.AddFormItem(hashField)

	selectionBox := tview.NewFlex().SetDirection(tview.FlexRow)
	selectionBox.SetBorder(true)
	selectionBox.SetTitle("Explain Query - Selection")
	selectionBox.SetTitleAlign(tview.AlignLeft)

	// Separate explainOutput area for explain flow (do not use a.mainView)
	explainOutput := tview.NewTextView().
		SetDynamicColors(true).
		SetWrap(true).
		SetWordWrap(true).
		SetScrollable(true).
		SetRegions(false) // Disable regions for clean text selection
	explainOutput.SetBorder(true)
	explainOutput.SetTitle("Explain Output")

	// Placeholders for lists wrapped in FilteredList (provides filtering helpers)
	tablesTList := tview.NewList()
	tablesTList.SetMainTextColor(tcell.ColorWhite)
	tablesTList.ShowSecondaryText(false)
	tablesFL := widgets.NewFilteredList(tablesTList, "Tables", []string{}, "explain_tables_filter")
	tablesList := tablesFL.List

	kindTList := tview.NewList()
	kindTList.SetMainTextColor(tcell.ColorWhite)
	kindTList.ShowSecondaryText(false)
	kindFL := widgets.NewFilteredList(kindTList, "Query kinds", []string{}, "explain_kinds_filter")
	kindList := kindFL.List

	// Helper to toggle selection in a list and reflect prefix.
	// Preserve the currently focused item (by visible text) so refreshing the list
	// doesn't reset the cursor to the first row when the user presses space.
	var toggleSelect func(list *tview.List, items []string, selMap map[string]bool)
	toggleSelect = func(list *tview.List, items []string, selMap map[string]bool) {
		// Capture current item text (may include prefix). Only attempt to read if list has items.
		count := list.GetItemCount()
		curText := ""
		if count > 0 {
			curIdx := list.GetCurrentItem()
			if curIdx >= 0 && curIdx < count {
				curText, _ = list.GetItemText(curIdx)
			}
		}

		list.Clear()
		for _, it := range items {
			prefix := " [ ] "
			if selMap[it] {
				prefix = " [+] "
			}
			// capture item by value for closure
			item := it
			list.AddItem(prefix+item, "", 0, func() {
				// Toggle selection for this item and refresh the rendered list.
				selMap[item] = !selMap[item]
				toggleSelect(list, items, selMap)
			})
		}

		// Try to restore previously focused item if it still exists.
		if curText != "" {
			trimmed := strings.TrimSpace(curText)
			// Remove common prefix patterns like "[ ]" or "[+]" if present.
			if strings.HasPrefix(trimmed, "[") {
				if idx := strings.Index(trimmed, "]"); idx != -1 && idx+1 < len(trimmed) {
					trimmed = strings.TrimSpace(trimmed[idx+1:])
				}
			}
			// Find the same item in the new list and restore the cursor.
			for i, it := range items {
				if it == trimmed {
					// Ensure index is still valid for the current list before setting.
					if i >= 0 && i < list.GetItemCount() {
						list.SetCurrentItem(i)
					}
					break
				}
			}
		}
	}

	// Ensure the filtered-list rendering preserves the selection prefixes inserted by toggleSelect.
	// The RenderList closures capture the selected tables/kinds maps so prefix state is kept.
	tablesFL.RenderList = func(list *tview.List, items []string) {
		toggleSelect(list, items, selectedTables)
	}
	kindFL.RenderList = func(list *tview.List, items []string) {
		toggleSelect(list, items, selectedKinds)
	}

	// Load options button - queries DB to fill tables and kinds
	var loadFunc func()
	loadFunc = func() {
		a.tviewApp.QueueUpdateDraw(func() {
			explainOutput.SetText("Loading tables and query kinds...")
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
				explainOutput.SetText(fmt.Sprintf("Error loading tables: %v\n%s", err, tablesQuery))
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
				explainOutput.SetText(fmt.Sprintf("Error loading query kinds: %v\n%s", err, kindQuery))
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

		// Update UI lists and update underlying FilteredList items so filtering works
		a.tviewApp.QueueUpdateDraw(func() {
			// update filter sources
			tablesFL.Items = tables
			kindFL.Items = kinds

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
			explainOutput.SetText("Options loaded.")
		})
	}

	// Do not reload options on each keystroke. Initial load only.
	// Users can trigger searches by pressing Enter in the normalized_query_hash field.
	// Trigger initial load of tables/kinds.
	go loadFunc()

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
			"SELECT DISTINCT normalized_query_hash, normalizeQuery(query) AS q FROM clusterAllReplicas('%s', merge(system,'^query_log')) WHERE %s ORDER BY normalized_query_hash",
			cluster, whereClause,
		)

		rows, err := a.clickHouse.Query(query)
		if err != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				explainOutput.SetText(fmt.Sprintf("Error executing query: %v\n%s", err, query))
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

		// Show queries list (filter input is a transient overlay shown on '/')
		a.tviewApp.QueueUpdateDraw(func() {
			if len(results) == 0 {
				explainOutput.SetText("No queries found with given filters")
				return
			}

			// Build UI: filtered list backed by widgets.FilteredList
			queriesTList := tview.NewList().ShowSecondaryText(false)
			queriesTList.SetMainTextColor(tcell.ColorWhite)
			queriesFL := widgets.NewFilteredList(queriesTList, "Queries (Enter to inspect)", []string{}, "explain_queries_filter")
			queriesList := queriesFL.List
			queriesList.SetBorder(true).SetTitle("Queries (Enter to inspect)")

			// Prepare mapping from display string to result row for selection callbacks
			displayMap := make(map[string]qrow)
			var items []string
			for _, r := range results {
				display := fmt.Sprintf("%s : %s", r.hash, truncate(r.q, 120))
				items = append(items, display)
				displayMap[display] = r
			}
			queriesFL.Items = items

			// RenderList preserves selection behavior and attaches callbacks
			queriesFL.RenderList = func(list *tview.List, items []string) {
				list.Clear()
				for _, display := range items {
					if r, ok := displayMap[display]; ok {
						d := display
						rr := r
						list.AddItem(d, "", 0, func() {
							a.showExplainPercentiles(rr.hash, rr.q, fromTime, toTime, cluster, explainOutput)
						})
					} else {
						dd := display
						list.AddItem(dd, "", 0, nil)
					}
				}
			}

			// Initial render
			queriesFL.ResetList()

			// Show filter input overlay only when user presses '/'
			queriesList.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
				if event == nil {
					return event
				}
				// Open transient filter overlay when user types '/'
				if event.Rune() == '/' {
					queriesFL.ShowFilterInput(a.tviewApp, a.pages)
					return nil
				}
				// Escape returns to explain selection page
				if event.Key() == tcell.KeyEscape {
					a.pages.SwitchToPage("explain")
					return nil
				}
				return event
			})

			// Layout: only the list is visible; filter input appears as an overlay when requested.
			a.pages.AddPage("explain_queries", queriesList, true, true)
			a.pages.SwitchToPage("explain_queries")
			a.tviewApp.SetFocus(queriesList)
		})
	}

	var cancelFunc func()
	cancelFunc = func() {
		a.pages.SwitchToPage("main")
	}

	// Create standalone buttons (not part of the form) so we can control focus order
	searchBtn := tview.NewButton("Search").SetSelectedFunc(func() {
		// Run search in a goroutine to avoid blocking the UI
		go searchFunc()
	})
	cancelBtn := tview.NewButton("Cancel").SetSelectedFunc(func() {
		cancelFunc()
	})
	// Tab navigation for buttons: forward/backwards between kindList -> searchBtn -> cancelBtn -> hashField
	searchBtn.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event == nil {
			return event
		}
		if event.Key() == tcell.KeyTab {
			a.tviewApp.SetFocus(cancelBtn)
			return nil
		} else if event.Key() == tcell.KeyBacktab {
			a.tviewApp.SetFocus(kindList)
			return nil
		}
		return event
	})
	cancelBtn.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event == nil {
			return event
		}
		if event.Key() == tcell.KeyTab {
			a.tviewApp.SetFocus(hashField)
			return nil
		} else if event.Key() == tcell.KeyBacktab {
			a.tviewApp.SetFocus(searchBtn)
			return nil
		}
		return event
	})
	buttonsFlex := tview.NewFlex().SetDirection(tview.FlexColumn).
		AddItem(searchBtn, 0, 1, false).
		AddItem(cancelBtn, 0, 1, false)

	// Make Tab/Shift-Tab navigation work:
	// - From hashField (input) Tab -> tablesList, Enter -> trigger search
	hashField.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyTab {
			a.tviewApp.SetFocus(tablesList)
		} else if key == tcell.KeyEnter {
			go searchFunc()
		}
	})
	// Also capture raw input events on the field so Tab works even when the surrounding Form captures it,
	// and allow '/' to open the tables filter.
	hashField.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event == nil {
			return event
		}
		if event.Rune() == '/' {
			tablesFL.ShowFilterInput(a.tviewApp, a.pages)
			return nil
		}
		if event.Key() == tcell.KeyTab {
			a.tviewApp.SetFocus(tablesList)
			return nil
		} else if event.Key() == tcell.KeyBacktab {
			// Move focus backwards to cancel button
			a.tviewApp.SetFocus(cancelBtn)
			return nil
		}
		return event
	})

	// - From tablesList Tab -> kindList, Shift-Tab -> hashField
	tablesList.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Rune() == '/' {
			tablesFL.ShowFilterInput(a.tviewApp, a.pages)
			return nil
		}
		if event.Key() == tcell.KeyTab {
			a.tviewApp.SetFocus(kindList)
			return nil
		} else if event.Key() == tcell.KeyBacktab {
			a.tviewApp.SetFocus(hashField)
			return nil
		}
		return event
	})

	// - From kindList Tab -> searchBtn, Shift-Tab -> tablesList
	kindList.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Rune() == '/' {
			kindFL.ShowFilterInput(a.tviewApp, a.pages)
			return nil
		}
		if event.Key() == tcell.KeyTab {
			a.tviewApp.SetFocus(searchBtn)
			return nil
		} else if event.Key() == tcell.KeyBacktab {
			a.tviewApp.SetFocus(tablesList)
			return nil
		}
		return event
	})

	// Layout: left = form + lists + buttons, right = explainOutput area
	// assemble selection content inside bordered selectionBox
	selectionBox.AddItem(form, 7, 0, true)
	selectionBox.AddItem(tview.NewTextView().SetText("Tables:"), 1, 0, false)
	selectionBox.AddItem(tablesList, 0, 1, false)
	selectionBox.AddItem(tview.NewTextView().SetText("Query kinds:"), 1, 0, false)
	selectionBox.AddItem(kindList, 0, 1, false)
	selectionBox.AddItem(buttonsFlex, 1, 0, false)

	leftFlex := selectionBox

	mainFlex := tview.NewFlex().SetDirection(tview.FlexColumn).
		AddItem(leftFlex, 0, 2, true).
		AddItem(explainOutput, 0, 1, false)

	a.pages.AddPage("explain", mainFlex, true, true)
	a.pages.SwitchToPage("explain")
	// Focus the hash input field initially so Tab moves into the lists
	a.tviewApp.SetFocus(hashField)
}

// truncate utility
func truncate(s string, l int) string {
	if len(s) <= l {
		return s
	}
	return s[:l-1] + "…"
}

// showExplainPercentiles queries p50/p90/p99 and shows a simple legend modal, allowing to drill into percentile value.
func (a *App) showExplainPercentiles(hash, queryText string, fromTime, toTime time.Time, cluster string, explainOutput *tview.TextView) {
	fromStr := fromTime.Format("2006-01-02 15:04:05 -07:00")
	toStr := toTime.Format("2006-01-02 15:04:05 -07:00")

	q := fmt.Sprintf("SELECT quantile(0.5)(query_duration_ms) AS p50, quantile(0.9)(query_duration_ms) AS p90, quantile(0.99)(query_duration_ms) AS p99 FROM clusterAllReplicas('%s', merge(system,'^query_log')) WHERE event_date >= toDate(parseDateTimeBestEffort('%s')) AND event_date <= toDate(parseDateTimeBestEffort('%s')) AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s') AND normalized_query_hash = '%s' AND query_duration_ms > 0",
		cluster, fromStr, toStr, fromStr, toStr, strings.ReplaceAll(hash, "'", "''"),
	)

	rows, err := a.clickHouse.Query(q)
	if err != nil {
		a.tviewApp.QueueUpdateDraw(func() {
			explainOutput.SetText(fmt.Sprintf("Error fetching percentiles: %v\n%s", err, q))
			a.SwitchToMainPage("explain")
			a.tviewApp.SetFocus(explainOutput)
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
		if scanErr := rows.Scan(&p50, &p90, &p99); scanErr != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				explainOutput.SetText(fmt.Sprintf("Error scanning percentiles: %v", scanErr))
				a.pages.SwitchToPage("explain_percentiles")
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
				a.showExplainQueryByThreshold(hash, p50, fromTime, toTime, cluster, explainOutput)
			case "p90":
				a.showExplainQueryByThreshold(hash, p90, fromTime, toTime, cluster, explainOutput)
			case "p99":
				a.showExplainQueryByThreshold(hash, p99, fromTime, toTime, cluster, explainOutput)
			default:
				a.pages.SwitchToPage("explain_queries")
			}
		})

	a.pages.AddPage("explain_percentiles", modal, true, true)
	a.pages.SwitchToPage("explain_percentiles")
}

// showExplainQueryByThreshold finds the top query above threshold and shows explain plans and query text.
// Note: Text selection in terminal applications works through your terminal emulator, not the TUI itself.
// To select and copy text:
//   - Linux/Windows: Hold Shift and drag with mouse to select, then Ctrl+Shift+C to copy
//   - macOS Terminal: Simply drag with mouse to select, then Cmd+C to copy
//   - Most terminals: Right-click after selection to copy
//
// The selected text can then be pasted elsewhere using standard paste shortcuts.
func (a *App) showExplainQueryByThreshold(hash string, threshold float64, fromTime, toTime time.Time, cluster string, explainOutput *tview.TextView) {
	fromStr := fromTime.Format("2006-01-02 15:04:05 -07:00")
	toStr := toTime.Format("2006-01-02 15:04:05 -07:00")

	// Run the database query in a goroutine to avoid blocking the UI thread
	go func() {
		// Get top query above threshold
		q := fmt.Sprintf("SELECT query, query_duration_ms FROM clusterAllReplicas('%s', merge(system,'^query_log')) WHERE event_date >= toDate(parseDateTimeBestEffort('%s')) AND event_date <= toDate(parseDateTimeBestEffort('%s')) AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s') AND normalized_query_hash = '%s' AND query_duration_ms <= %f ORDER BY query_duration_ms DESC LIMIT 1",
			cluster, fromStr, toStr, fromStr, toStr, strings.ReplaceAll(hash, "'", "''"), threshold,
		)
		rows, err := a.clickHouse.Query(q)
		if err != nil {
			a.tviewApp.QueueUpdate(func() {
				explainOutput.SetText(fmt.Sprintf("Error fetching query: %v\n%s", err, q))
				a.pages.SwitchToPage("explain")
				a.tviewApp.SetFocus(explainOutput)
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
			if scanErr := rows.Scan(&queryText, &duration); scanErr != nil {
				a.tviewApp.QueueUpdate(func() {
					explainOutput.SetText(fmt.Sprintf("Error scanning top query: %v", scanErr))
					a.pages.SwitchToPage("explain")
					a.tviewApp.SetFocus(explainOutput)
				})
				return
			}
		} else {
			log.Debug().Msg("Queuing UI update for no-query-found")
			a.tviewApp.QueueUpdateDraw(func() {
				explainOutput.SetText("[red]No query found above threshold[-]")
				a.pages.SwitchToPage("explain")
				// Make sure the page is visible and focus is set
				a.pages.SendToFront("explain")
				a.tviewApp.SetFocus(explainOutput)
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
		// Text selection: Use your terminal's native selection (mouse drag) and copy (Ctrl+Shift+C or Cmd+C)
		ex1 := tview.NewTextView().
			SetWrap(true).
			SetDynamicColors(true).
			SetScrollable(true).
			SetRegions(false) // Disable regions to ensure clean text explainOutput
		ex1.SetBorder(true).SetTitle("EXPLAIN PLAN indexes=1, projections=1")

		ex2 := tview.NewTextView().
			SetWrap(true).
			SetDynamicColors(true).
			SetScrollable(true).
			SetRegions(false)
		ex2.SetBorder(true).SetTitle("EXPLAIN PIPELINE")

		ex3 := tview.NewTextView().
			SetWrap(true).
			SetDynamicColors(true).
			SetScrollable(true).
			SetRegions(false)
		ex3.SetBorder(true).SetTitle("EXPLAIN ESTIMATE")

		a.tviewApp.QueueUpdate(func() {
			modal := tview.NewModal().SetText("Running EXPLAINs...").AddButtons([]string{"OK"})
			// Ensure any previous loading page is removed, then show the loading modal.
			a.pages.RemovePage("explain_loading")
			a.pages.AddPage("explain_loading", modal, true, true)
			a.pages.SwitchToPage("explain_loading")
		})

		// Run explains (best-effort)
		go func() {
			log.Info().Msgf("running explain1: %s", explain1)
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

			log.Info().Msgf("running explain2: %s", explain2)
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

			log.Info().Msgf("running explain3: %s", explain3)
			if rows3, err3 := a.clickHouse.Query(explain3); err3 == nil {
				var buf strings.Builder

				cols, _ := rows3.Columns()

				// Prefer explicit scan when we have the expected five columns:
				// database (string), table (string), parts (UInt64), rows (UInt64), marks (UInt64)
				if len(cols) >= 5 {
					type rec struct {
						db    string
						table string
						parts uint64
						rows  uint64
						marks uint64
					}
					var rowsData []rec

					for rows3.Next() {
						var db, table string
						var parts, rcount, marks uint64
						if err := rows3.Scan(&db, &table, &parts, &rcount, &marks); err != nil {
							log.Error().Err(err).Msg("scan explain estimate row")
							continue
						}
						rowsData = append(rowsData, rec{db: db, table: table, parts: parts, rows: rcount, marks: marks})
					}

					// Compute column widths for a compact aligned table
					col0 := "database.table"
					w0 := len(col0)
					w1 := len("parts")
					w2 := len("rows")
					w3 := len("marks")

					for _, r := range rowsData {
						n := fmt.Sprintf("%s.%s", r.db, r.table)
						if len(n) > w0 {
							w0 = len(n)
						}
						if l := len(fmt.Sprintf("%d", r.parts)); l > w1 {
							w1 = l
						}
						if l := len(fmt.Sprintf("%d", r.rows)); l > w2 {
							w2 = l
						}
						if l := len(fmt.Sprintf("%d", r.marks)); l > w3 {
							w3 = l
						}
					}

					// Header and separator
					fmt.Fprintf(&buf, "%-*s  %*s  %*s  %*s\n", w0, col0, w1, "parts", w2, "rows", w3, "marks")
					fmt.Fprintf(&buf, "%s\n", strings.Repeat("-", w0+2+w1+2+w2+2+w3))

					// Rows
					for _, r := range rowsData {
						fmt.Fprintf(&buf, "%-*s  %*d  %*d  %*d\n",
							w0, fmt.Sprintf("%s.%s", r.db, r.table),
							w1, r.parts,
							w2, r.rows,
							w3, r.marks,
						)
					}
				} else {
					// Fallback: render generically for unknown schemas
					for rows3.Next() {
						dest := make([]interface{}, len(cols))
						for i := range dest {
							var v interface{}
							dest[i] = &v
						}
						if err := rows3.Scan(dest...); err != nil {
							continue
						}
						for i := range cols {
							if i > 0 {
								buf.WriteString("\t")
							}
							buf.WriteString(fmt.Sprintf("%s: %v", cols[i], *(dest[i].(*interface{}))))
						}
						buf.WriteString("\n")
					}
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

				// Remove loading modal if present, then show results.
				a.pages.RemovePage("explain_loading")
				a.pages.AddPage("explain_result", mainFlex, true, true)
				a.pages.SwitchToPage("explain_result")
			})
		}()
	}()
}
