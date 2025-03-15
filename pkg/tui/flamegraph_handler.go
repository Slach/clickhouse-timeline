package tui

import (
	"fmt"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"os"
	"os/exec"
)

const flamegraphQuery = `
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
FROM system.trace_log
WHERE query_id IN (
    SELECT query_id 
    FROM system.query_log 
    WHERE normalized_query_hash = %s
    AND event_date=today() 
)
AND trace_type = '%s'
GROUP BY trace, trace_type
SETTINGS allow_introspection_functions=1
`

func (a *App) showFlamegraphForm() {
	if a.clickHouse == nil {
		a.mainView.SetText("Error: Please connect to a ClickHouse instance first")
		return
	}

	form := tview.NewForm()
	form.SetTitle("Flamegraph Parameters")

	var queryHash, traceType string

	form.AddInputField("Query Hash:", "", 40, nil, func(text string) {
		queryHash = text
	})

	form.AddDropDown("Trace Type:", []string{
		string(TraceMemory),
		string(TraceCPU),
		string(TraceReal),
		string(TraceMemorySample),
	}, 0, func(option string, _ int) {
		traceType = option
	})

	form.AddButton("Generate", func() {
		if queryHash == "" || traceType == "" {
			a.mainView.SetText("Error: Both Query Hash and Trace Type are required")
			return
		}
		// Clear the main view, switch to main page, and then generate flamegraph
		a.mainView.Clear()
		a.pages.SwitchToPage("main")
		a.generateFlamegraph(queryHash, TraceType(traceType))
	})

	cancelFunc := func() {
		a.pages.SwitchToPage("main")
	}

	form.AddButton("Cancel", cancelFunc)

	// Set form's input capture to handle Escape key
	form.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape {
			cancelFunc()
			return nil
		}
		return event
	})

	a.pages.AddPage("flamegraph_form", form, true, true)
	a.pages.SwitchToPage("flamegraph_form")
	a.tviewApp.SetFocus(form)
}

func (a *App) generateFlamegraph(queryHash string, traceType TraceType) {
	a.mainView.SetText("Preparing flamegraph data, please wait...")

	// We carry out a request and preparation of data in go-routine so as not to block UI
	go func() {
		query := fmt.Sprintf(flamegraphQuery, queryHash, traceType)

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

		// Suspend tview and run flamelens
		a.tviewApp.Suspend(func() {
			// Create command to run flamelens
			cmd := exec.Command(flamelensPath, tmpFile.Name())
			cmd.Stdin = os.Stdin
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr

			fmt.Println("\nRunning flamelens, press any key to return to the application when finished...")

			// Run the command and wait for it to complete
			if runErr := cmd.Run(); runErr != nil {
				fmt.Printf("\nError running flamelens: %v\n", runErr)
				fmt.Println("Press any key to continue...")
				// Wait for key press before returning
				b := make([]byte, 1)
				_, _ = os.Stdin.Read(b)
			}
		})

		// Update UI after completion
		a.tviewApp.QueueUpdateDraw(func() {
			a.mainView.SetText("Flamegraph generation completed! Press ':' to continue\n")
		})
	}()
}
