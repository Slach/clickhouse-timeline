package tui

import (
	"fmt"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"os"
	"os/exec"
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
}

func (a *App) showFlamegraphForm(params ...FlamegraphParams) {
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
		a.generateFlamegraph(categoryType, categoryValue, TraceType(traceType), fromTime, toTime, a.cluster)
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
	default:
		// If category is not specified, use only time range
		return fmt.Sprintf(flamegraphQueryByTimeRange, cluster, cluster, fromDateStr, toDateStr, fromStr, toStr, traceType)
	}
}

func (a *App) generateFlamegraph(categoryType CategoryType, categoryValue string, traceType TraceType, fromTime, toTime time.Time, cluster string) {
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

		rows, createErr := a.clickHouse.Query(query)
		if createErr != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Error ClickHouse querying\n%s\n: %v", query, createErr))
			})
			return
		}
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
					a.mainView.SetText(fmt.Sprintf("Error removing temp file: %v", removeErr))
				})
			}
		}()
		defer func() {
			if err := tmpFile.Close(); err != nil {
				a.tviewApp.QueueUpdateDraw(func() {
					a.mainView.SetText(fmt.Sprintf("Error closing temp file: %v", err))
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
		if createErr = rows.Err(); createErr != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Error reading query results: %v", createErr))
			})
			return
		}

		// Flush and close the file before reading from it
		if createErr = tmpFile.Sync(); createErr != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Error syncing temp file: %v", createErr))
			})
			return
		}

		// Find flamelens in PATH
		flamelensPath, createErr := exec.LookPath("flamelens")
		if createErr != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Can't find flamelens: %v", createErr))
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
	}()
}
