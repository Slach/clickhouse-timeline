package tui

import (
	"database/sql"
	"fmt"
	"github.com/Slach/clickhouse-timeline/pkg/flamegraph"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"os"
	"os/exec"
	"strings"
	"time"
)

// Different query templates for flamegraph depending on the category
const flamegraphQueryByHash = `
SELECT
	count() AS samples, 
	concat(
		multiIf( 
			position( toString(trace_type), 'Memory') > 0 AND sum(size) >= 0, 'allocate;',
			position( toString(trace_type), 'Memory') > 0 AND sum(size) < 0, 'free;',
			concat( toString(trace_type), ';')
		),
		arrayStringConcat(arrayReverse(arrayMap(x -> concat( demangle(addressToSymbol(x)), '#', addressToLine(x) ), trace)), ';')
	) AS stack
FROM clusterAllReplicas('%s', merge(system, '^trace_log'))
WHERE query_id IN (
    SELECT query_id 
    FROM clusterAllReplicas('%s', merge(system, '^query_log'))
    WHERE normalized_query_hash = '%s'
    AND event_date >= toDate('%s') AND event_date <= toDate('%s')
    AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s')
)
AND trace_type = '%s'
GROUP BY trace, trace_type
SETTINGS allow_introspection_functions=1
`

const flamegraphQueryByTable = `
SELECT
	count() AS samples, 
	concat(
		multiIf( 
			position( toString(trace_type), 'Memory') > 0 AND sum(size) >= 0, 'allocate;',
			position( toString(trace_type), 'Memory') > 0 AND sum(size) < 0, 'free;',
			concat( toString(trace_type), ';')
		),
		arrayStringConcat(arrayReverse(arrayMap(x -> concat( demangle(addressToSymbol(x)), '#', addressToLine(x) ), trace)), ';')
	) AS stack
FROM clusterAllReplicas('%s', merge(system, '^trace_log'))
WHERE query_id IN (
    SELECT query_id 
    FROM clusterAllReplicas('%s', merge(system, '^query_log'))
    WHERE hasAll(tables, ['%s'])
    AND event_date >= toDate('%s') AND event_date <= toDate('%s')
    AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s')
)
AND trace_type = '%s'
GROUP BY trace, trace_type
SETTINGS allow_introspection_functions=1
`

const flamegraphQueryByHost = `
SELECT
	count() AS samples, 
	concat(
		multiIf( 
			position( toString(trace_type), 'Memory') > 0 AND sum(size) >= 0, 'allocate;',
			position( toString(trace_type), 'Memory') > 0 AND sum(size) < 0, 'free;',
			concat( toString(trace_type), ';')
		),
		arrayStringConcat(arrayReverse(arrayMap(x -> concat( demangle(addressToSymbol(x)), '#', addressToLine(x) ), trace)), ';')
	) AS stack
FROM clusterAllReplicas('%s', merge(system, '^trace_log'))
WHERE query_id IN (
    SELECT query_id 
    FROM clusterAllReplicas('%s', merge(system, '^query_log'))
    WHERE hostName() = '%s'
    AND event_date >= toDate('%s') AND event_date <= toDate('%s')
    AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s')
)
AND trace_type = '%s'
GROUP BY trace, trace_type
SETTINGS allow_introspection_functions=1
`

const flamegraphQueryByError = `
SELECT
	count() AS samples, 
	concat(
		multiIf( 
			position( toString(trace_type), 'Memory') > 0 AND sum(size) >= 0, 'allocate;',
			position( toString(trace_type), 'Memory') > 0 AND sum(size) < 0, 'free;',
			concat( toString(trace_type), ';')
		),
		arrayStringConcat(arrayReverse(arrayMap(x -> concat( demangle(addressToSymbol(x)), '#', addressToLine(x) ), trace)), ';')
	) AS stack
FROM clusterAllReplicas('%s', merge(system, '^trace_log'))
WHERE query_id IN (
    SELECT query_id 
    FROM clusterAllReplicas('%s', merge(system, '^query_log'))
    WHERE normalized_query_hash = '%s'
    AND event_date >= toDate('%s') AND event_date <= toDate('%s')
    AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s')
)
AND trace_type = '%s'
GROUP BY trace, trace_type
SETTINGS allow_introspection_functions=1
`

const flamegraphQueryByTimeRange = `
SELECT
	count() AS samples, 
	concat(
		multiIf( 
			position( toString(trace_type), 'Memory') > 0 AND sum(size) >= 0, 'allocate;',
			position( toString(trace_type), 'Memory') > 0 AND sum(size) < 0, 'free;',
			concat( toString(trace_type), ';')
		),
		arrayStringConcat(arrayReverse(arrayMap(x -> concat( demangle(addressToSymbol(x)), '#', addressToLine(x) ), trace)), ';')
	) AS stack
FROM clusterAllReplicas('%s', merge(system, '^trace_log'))
WHERE query_id IN (
    SELECT query_id 
    FROM clusterAllReplicas('%s', merge(system, '^query_log'))
    WHERE event_date >= toDate('%s') AND event_date <= toDate('%s')
    AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s')
)
AND trace_type = '%s'
GROUP BY trace, trace_type
SETTINGS allow_introspection_functions=1
`

// FlamegraphParams Structure for storing flamegraph parameters
type FlamegraphParams struct {
	CategoryType  CategoryType
	CategoryValue string
	TraceType     TraceType
	FromTime      time.Time
	ToTime        time.Time
	SourcePage    string // Tracks where the flamegraph was called from
}

func (a *App) showNativeFlamegraph(rows *sql.Rows, sourcePage string) {
	flameView := flamegraph.NewFlamegraphView()
	err := flameView.BuildFromRows(rows)
	if err != nil {
		a.mainView.SetText(fmt.Sprintf("Error building flamegraph: %v", err))
		return
	}

	// Handle case when no rows were returned
	if flameView.GetTotalCount() == 0 {
		flameView.SetDirection(flamegraph.DirectionTopDown)
		flameView.SetFrameHandler(func(stack []string, count int) {}) // No-op handler
		a.mainView.SetText("No data found for the selected parameters")
	}

	flameView.SetDirection(flamegraph.DirectionTopDown)
	flameView.SetFrameHandler(func(stack []string, count int) {
		// Calculate percentage of total
		total := flameView.GetTotalCount()
		percentage := 0.0
		if total > 0 {
			percentage = float64(count) / float64(total)
		}

		// Create content for the stack trace view
		stackTraceText := fmt.Sprintf("Selected stacktrace count: %d (%.2f%% of total)\n\nFull Stack Trace:\n%s\n\n[yellow]Use arrow keys to scroll, ESC or Close button to return[-]",
			count, percentage*100.0, flamegraph.FormatStackWithNumbers(stack))

		// Create a proper TextView for the stack trace
		stackTraceView := tview.NewTextView()
		stackTraceView.SetTextAlign(tview.AlignLeft)
		stackTraceView.SetScrollable(true)
		stackTraceView.SetDynamicColors(true)
		stackTraceView.SetBorder(true)
		stackTraceView.SetTitle("Stack Trace")
		stackTraceView.SetText(stackTraceText) // Set text after other properties
		stackTraceView.ScrollToBeginning()

		// Make sure the text view can receive focus for keyboard navigation
		stackTraceView.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
			switch event.Key() {
			case tcell.KeyEscape:
				a.pages.SwitchToPage("flamegraph")
				a.tviewApp.SetFocus(flameView)
				return nil
			default:
				return event
			}
		})

		// Create a button to close the stackTraceFlex
		closeButton := tview.NewButton("Close").
			SetSelectedFunc(func() {
				a.pages.SwitchToPage("flamegraph")
				a.tviewApp.SetFocus(flameView)
			})

		// Create a simple stackTraceFlex with the text view and a button
		stackTraceFlex := tview.NewFlex().
			SetDirection(tview.FlexRow).
			AddItem(stackTraceView, 0, 1, true).
			AddItem(closeButton, 1, 0, true)

		a.pages.AddPage("stacktrace", stackTraceFlex, true, true)
		a.pages.SwitchToPage("stacktrace")
		a.tviewApp.SetFocus(stackTraceView)

	})

	// Set the source page for ESC key handling
	// Set source page with stacktrace suffix if we're coming from stacktrace
	if strings.HasSuffix(sourcePage, "stacktrace") {
		flameView.SetSourcePage("stacktrace")
	} else if sourcePage != "" {
		flameView.SetSourcePage(sourcePage)
	} else {
		flameView.SetSourcePage("main")
	}

	// Set the page switcher function
	flameView.SetPageSwitcher(func(targetPage string) {
		a.pages.SwitchToPage(targetPage)
	})

	// Create help text at bottom
	flameTitle := tview.NewTextView().
		SetText("Flamegraph Viewer (Use arrow keys to navigate, Enter to select, ESC to go back)").
		SetTextAlign(tview.AlignCenter)

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(flameTitle, 1, 0, false).
		AddItem(flameView, 0, 1, true)

	a.pages.AddPage("flamegraph", flex, true, true)
	a.pages.SwitchToPage("flamegraph")
	a.tviewApp.SetFocus(flameView)
}

func (a *App) ShowFlamegraphForm(params ...FlamegraphParams) {
	if a.clickHouse == nil {
		a.mainView.SetText("Error: Please connect to a ClickHouse instance first")
		return
	}

	// Create a form with proper type
	form := tview.NewForm()
	form.SetTitle("Flamegraph Parameters").
		SetBorder(true)

	// Add mouse support
	form.SetMouseCapture(func(action tview.MouseAction, event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse) {
		// On any click, focus the form
		if action == tview.MouseLeftClick {
			a.tviewApp.SetFocus(form)
		}
		return action, event
	})

	// Set mouse handler for the form
	form.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		// Handle mouse events
		if event.Key() == tcell.KeyRune && event.Rune() == 'M' {
			// Mouse event
			return event
		}
		return event
	})

	var categoryType = CategoryQueryHash
	var categoryValue, traceType string
	var fromTime, toTime time.Time

	// If parameters are passed, use them
	if len(params) > 0 {
		categoryType = params[0].CategoryType
		categoryValue = params[0].CategoryValue
		if params[0].TraceType != "" {
			traceType = string(params[0].TraceType)
		}
		fromTime = params[0].FromTime
		toTime = params[0].ToTime
	}

	// If time is not set, use current time and range
	if fromTime.IsZero() {
		fromTime = a.fromTime
	}
	if toTime.IsZero() {
		toTime = a.toTime
	}

	// Add category selection
	categoryOptions := []string{
		"Query Hash",
		"Table",
		"Host",
		"Time Range Only",
	}

	categoryIndex := 0
	switch categoryType {
	case CategoryTable:
		categoryIndex = 1
	case CategoryHost:
		categoryIndex = 2
	case "":
		categoryIndex = 3
	}

	form.AddDropDown("Category Type:", categoryOptions, categoryIndex, func(option string, index int) {
		switch index {
		case 0:
			categoryType = CategoryQueryHash
		case 1:
			categoryType = CategoryTable
		case 2:
			categoryType = CategoryHost
		case 3:
			categoryType = ""
		}
	})

	// Field for category value
	form.AddInputField("Category Value:", categoryValue, 40, nil, func(text string) {
		categoryValue = text
	})

	// Trace type selection
	traceOptions := []string{
		string(TraceMemory),
		string(TraceCPU),
		string(TraceReal),
		string(TraceMemorySample),
	}

	traceIndex := 0
	for i, opt := range traceOptions {
		if opt == traceType {
			traceIndex = i
			break
		}
	}

	form.AddDropDown("Trace Type:", traceOptions, traceIndex, func(option string, _ int) {
		traceType = option
	})

	// Display time range
	timeRangeText := fmt.Sprintf("from %s to %s",
		fromTime.Format("2006-01-02 15:04:05 -07:00"),
		toTime.Format("2006-01-02 15:04:05 -07:00"))
	form.AddTextView("Time Range:", timeRangeText, 50, 2, true, false)

	// Define generate function
	generateFunc := func() {
		if (categoryType != "" && categoryValue == "") || traceType == "" {
			a.mainView.SetText("Error: Category Value and Trace Type are required")
			return
		}
		// Clear the main view, switch to main page, and then generate flamegraph
		a.mainView.Clear()
		a.pages.SwitchToPage("main")
		a.generateFlamegraph(categoryType, categoryValue, TraceType(traceType), fromTime, toTime, a.cluster, "flamegraph_form")
	}

	cancelFunc := func() {
		a.pages.SwitchToPage("main")
	}
	form.AddButton("Generate", generateFunc)
	form.AddButton("Cancel", cancelFunc)

	// Add Ctrl+Enter hotkey for Generate button
	form.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEnter && event.Modifiers() == tcell.ModCtrl {
			// Trigger Generate button action directly
			generateFunc()
			return nil
		} else if event.Key() == tcell.KeyEscape {
			cancelFunc()
			return nil
		}
		return event
	})

	a.pages.AddPage("flamegraph_form", form, true, true)
	a.pages.SwitchToPage("flamegraph_form")
	a.tviewApp.SetFocus(form)
}

// getFlamegraphQuery returns the appropriate query string based on category type
func (a *App) getFlamegraphQuery(categoryType CategoryType, categoryValue string, traceType TraceType,
	fromDateStr, toDateStr, fromStr, toStr, cluster string) string {
	switch categoryType {
	case CategoryQueryHash:
		return fmt.Sprintf(flamegraphQueryByHash, cluster, cluster, categoryValue, fromDateStr, toDateStr, fromStr, toStr, traceType)
	case CategoryTable:
		return fmt.Sprintf(flamegraphQueryByTable, cluster, cluster, categoryValue, fromDateStr, toDateStr, fromStr, toStr, traceType)
	case CategoryHost:
		return fmt.Sprintf(flamegraphQueryByHost, cluster, cluster, categoryValue, fromDateStr, toDateStr, fromStr, toStr, traceType)
	case CategoryError:
		parts := strings.Split(categoryValue, ":")
		if len(parts) != 2 {
			return ""
		}
		return fmt.Sprintf(flamegraphQueryByError, cluster, cluster, parts[1], fromDateStr, toDateStr, fromStr, toStr, traceType)
	default:
		// If category is not specified, use only time range
		return fmt.Sprintf(flamegraphQueryByTimeRange, cluster, cluster, fromDateStr, toDateStr, fromStr, toStr, traceType)
	}
}

func (a *App) generateFlamegraph(categoryType CategoryType, categoryValue string, traceType TraceType, fromTime, toTime time.Time, cluster string, sourcePage string) {
	a.mainView.SetText("Preparing flamegraph data, please wait...")

	// We carry out a request and preparation of data in go-routine so as not to block UI
	go func() {
		// Format dates for the query
		fromStr := fromTime.Format("2006-01-02 15:04:05 -07:00")
		toStr := toTime.Format("2006-01-02 15:04:05 -07:00")
		fromDateStr := fromTime.Format("2006-01-02")
		toDateStr := toTime.Format("2006-01-02")

		var query string

		query = a.getFlamegraphQuery(categoryType, categoryValue, traceType, fromDateStr, toDateStr, fromStr, toStr, cluster)

		rows, queryErr := a.clickHouse.Query(query)
		if queryErr != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Error ClickHouse querying\n%s\n: %v", query, queryErr))
			})
			return
		}

		if a.flamegraphNative {
			// For native flamegraph, pass rows directly to the viewer
			// We'll clone the rows to avoid closing the original
			a.tviewApp.QueueUpdateDraw(func() {
				a.showNativeFlamegraph(rows, sourcePage)
			})
			return
		} else {
			// For flamelens, we still need to write to a temp file
			defer func() {
				if closeErr := rows.Close(); closeErr != nil {
					a.tviewApp.QueueUpdateDraw(func() {
						a.mainView.SetText(fmt.Sprintf("Can't rows.Close: %v", closeErr))
					})
				}
			}()

			tmpFile, createErr := os.CreateTemp("", "flamegraph-*.txt")
			if createErr != nil {
				a.tviewApp.QueueUpdateDraw(func() {
					a.mainView.SetText(fmt.Sprintf("Error creating temporary file: %v", createErr))
				})
				return
			}
			tmpFileName := tmpFile.Name()
			defer func() {
				if removeErr := os.Remove(tmpFileName); removeErr != nil {
					a.tviewApp.QueueUpdateDraw(func() {
						a.mainView.SetText(fmt.Sprintf("Error removing %s: %v", tmpFileName, removeErr))
					})
				}
			}()
			defer func() {
				if closeErr := tmpFile.Close(); closeErr != nil {
					a.tviewApp.QueueUpdateDraw(func() {
						a.mainView.SetText(fmt.Sprintf("Error closing %s: %v", tmpFileName, closeErr))
					})
				}
			}()

			// Write query results to temporary file
			for rows.Next() {
				var stack string
				var count int
				if createErr = rows.Scan(&count, &stack); createErr != nil {
					a.tviewApp.QueueUpdateDraw(func() {
						a.mainView.SetText(fmt.Sprintf("Error scanning row: %v", createErr))
					})
					return
				}
				if _, createErr = fmt.Fprintf(tmpFile, "%s %d\n", stack, count); createErr != nil {
					a.tviewApp.QueueUpdateDraw(func() {
						a.mainView.SetText(fmt.Sprintf("Error writing to temp file: %v", createErr))
					})
					return
				}
			}

			// Check for errors after the loop
			if rowsErr := rows.Err(); rowsErr != nil {
				a.tviewApp.QueueUpdateDraw(func() {
					a.mainView.SetText(fmt.Sprintf("Error reading query results: %v", rowsErr))
				})
				return
			}

			// Flush and close the file before reading from it
			if syncErr := tmpFile.Sync(); syncErr != nil {
				a.tviewApp.QueueUpdateDraw(func() {
					a.mainView.SetText(fmt.Sprintf("Error syncing temp file: %v", syncErr))
				})
				return
			}
			// Find flamelens in PATH
			flamelensPath, pathErr := exec.LookPath("flamelens")
			if pathErr != nil {
				a.tviewApp.QueueUpdateDraw(func() {
					a.mainView.SetText(fmt.Sprintf("Can't find flamelens: %v", pathErr))
				})
				return
			}

			// Update UI before running flamelens
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText("Generating flamegraph, please wait...")
			})
			var runErr error
			// Suspend tview and run flamelens
			a.tviewApp.Suspend(func() {
				// Create command to run flamelens
				cmd := exec.Command(flamelensPath, tmpFile.Name())
				cmd.Stdin = os.Stdin
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr

				fmt.Println("\nRunning flamelens, press any key to return to the application when finished...")

				// Run the command and wait for it to complete
				if runErr = cmd.Run(); runErr != nil {
					// Show error in tview UI
					a.tviewApp.QueueUpdateDraw(func() {
						a.mainView.SetText(fmt.Sprintf("Error running flamelens: %v\nPress any key to continue...", runErr))
					})
					// Wait for key press in tview
					a.tviewApp.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
						// Any key press will clear the error and restore default handler
						a.mainView.SetText("Flamegraph generation completed! Press ':' to continue\n")
						a.tviewApp.SetInputCapture(a.defaultInputHandler)
						return a.defaultInputHandler(event)
					})
				}
			})

			if runErr == nil {
				// Update UI after completion
				a.tviewApp.QueueUpdateDraw(func() {
					a.mainView.SetText("Flamegraph generation completed! Press ':' to continue\n")
				})
			}
		}
	}()
}
