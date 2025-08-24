package tui

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/Slach/clickhouse-timeline/pkg/timezone"
	"github.com/rs/zerolog/log"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// SQL template for heatmap queries
const heatmapQueryTemplate = `
WITH
/* alias broken in 25.3
   toStartOfInterval(toTimeZone(event_time, '%s'), INTERVAL %s) AS query_finish,
   toStartOfInterval(toTimeZone(query_start_time, '%s'), INTERVAL %s) AS query_start,
*/
   intDiv(toUInt32(toStartOfInterval(toTimeZone(event_time, '%s'), INTERVAL %s) - toStartOfInterval(toTimeZone(if(toUInt32(query_start_time)>0, query_start_time, event_time), '%s'), INTERVAL %s) + 1),%d) AS intervals,
   arrayMap(i -> (toStartOfInterval(toTimeZone(if(toUInt32(query_start_time)>0, query_start_time, event_time), '%s'), INTERVAL %s) + i), range(0, toUInt32(toStartOfInterval(toTimeZone(event_time, '%s'), INTERVAL %s) - toStartOfInterval(toTimeZone(if(toUInt32(query_start_time)>0, query_start_time, event_time), '%s'), INTERVAL %s) + 1),%d)) as timestamps
SELECT
    arrayJoin(timestamps) as t,
    %s AS categoryType,
    intDiv(%s,if(intervals=0,1,intervals)) as metricValue
FROM clusterAllReplicas('%s', merge(system,'^query_log'))
WHERE
    event_date >= toDate(parseDateTimeBestEffort('%s')) AND event_date <= toDate(parseDateTimeBestEffort('%s')) AND
    event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s')
    AND type!='QueryStart'
    %s
GROUP BY ALL
SETTINGS skip_unavailable_shards=1
`

// ShowHeatmap displays the heatmap visualization
func (a *App) ShowHeatmap() {
	if a.clickHouse == nil {
		a.mainView.SetText("Error: Please connect to a ClickHouse instance first")
		return
	}

	if a.cluster == "" {
		a.mainView.SetText("Error: Please select a cluster first using :cluster command")
		return
	}

	a.mainView.SetText("Generating heatmap, please wait...")

	// Calculate appropriate interval based on time range
	duration := a.toTime.Sub(a.fromTime)

	var interval string
	var intervalSeconds int

	if duration <= 2*time.Hour {
		interval = "1 MINUTE"
		intervalSeconds = 60
	} else if duration <= 24*time.Hour {
		interval = "10 MINUTE"
		intervalSeconds = 600
	} else if duration <= 7*24*time.Hour {
		interval = "1 HOUR"
		intervalSeconds = 3600
	} else if duration <= 30*24*time.Hour {
		interval = "1 DAY"
		intervalSeconds = 86400
	} else {
		interval = "1 WEEK"
		intervalSeconds = 604800
	}

	// Format the query
	fromStr := a.fromTime.Format("2006-01-02 15:04:05 -07:00")
	toStr := a.toTime.Format("2006-01-02 15:04:05 -07:00")

	metricSQL := getMetricSQL(a.heatmapMetric)
	categorySQL := getCategorySQL(a.categoryType)

	// Get timezone name from offset
	tzName, offset := a.fromTime.Zone()
	if tzName[0] == '-' || tzName[0] == '+' {
		var tzErr error
		tzName, tzErr = timezone.ConvertOffsetToIANAName(offset)
		if tzErr != nil {
			log.Error().Err(tzErr).Int("offset", offset).Msg("Failed to get timezone from offset")
			tzName = "UTC" // Fallback to UTC
		}
	}
	tzLocation, _ := time.LoadLocation(tzName)

	// Add error filter if showing errors
	errorFilter := ""
	if a.categoryType == CategoryError {
		errorFilter = "AND exception_code != 0"
	}

	query := fmt.Sprintf(heatmapQueryTemplate,
		tzName, interval, tzName, interval,
		tzName, interval, tzName, interval,
		intervalSeconds,
		tzName, interval, tzName, interval, tzName, interval,
		intervalSeconds,
		categorySQL, metricSQL, a.cluster,
		fromStr, toStr, fromStr, toStr,
		errorFilter,
	)

	// Execute the query
	go func() {
		rows, err := a.clickHouse.Query(query)
		if err != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Error executing query: %v", err))
			})
			return
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close query")
			}
		}()

		// Collect data
		type dataPoint struct {
			timestamp time.Time
			category  string
			value     float64
		}
		var data []dataPoint

		for rows.Next() {
			var t time.Time
			var category string
			var value float64

			if err := rows.Scan(&t, &category, &value); err != nil {
				a.tviewApp.QueueUpdateDraw(func() {
					a.mainView.SetText(fmt.Sprintf("Error scanning row: %v", err))
				})
				return
			}

			data = append(data, dataPoint{t, category, value})
		}

		if rowsErr := rows.Err(); rowsErr != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Error reading rows: %v", rowsErr))
			})
			return
		}

		// Process data for heatmap
		if len(data) == 0 {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText("No data found for the selected time range and categoryType")
			})
			return
		}

		// Extract unique timestamps and categories
		timeMap := make(map[time.Time]bool)
		categoryMap := make(map[string]bool)
		valueMap := make(map[string]map[time.Time]float64)

		var minValue, maxValue = math.MaxFloat64, -math.MaxFloat64

		for _, d := range data {
			timeMap[d.timestamp] = true
			categoryMap[d.category] = true

			if valueMap[d.category] == nil {
				valueMap[d.category] = make(map[time.Time]float64)
			}
			valueMap[d.category][d.timestamp] = d.value

			if d.value < minValue {
				minValue = d.value
			}
			if d.value > maxValue {
				maxValue = d.value
			}
		}

		// If all values are the same, adjust to avoid division by zero
		if minValue == maxValue {
			maxValue = minValue + 1
		}

		// Convert to sorted slices
		var timestamps []time.Time
		for t := range timeMap {
			timestamps = append(timestamps, t)
		}

		var categories []string
		for c := range categoryMap {
			categories = append(categories, c)
		}

		// Sort timestamps in ascending order
		sort.Slice(timestamps, func(i, j int) bool {
			return timestamps[i].Before(timestamps[j])
		})

		// Sort categories alphabetically for better readability
		sort.Strings(categories)

		// Create the heatmap table
		a.tviewApp.QueueUpdateDraw(func() {
			table := tview.NewTable().
				SetBorders(false).
				SetSelectable(true, true).
				SetFixed(1, 1). // Fix first row and first column
				SetSeparator(0) // Remove column separator/padding

			// Set header row with column numbers instead of timestamps
			table.SetCell(0, 0, tview.NewTableCell(getCategoryName(a.categoryType)).
				SetTextColor(tcell.ColorYellow).
				SetAlign(tview.AlignCenter).
				SetSelectable(false))

			// Use single character column headers
			for i := range timestamps {
				table.SetCell(0, i+1, tview.NewTableCell("•").
					SetTextColor(tcell.ColorYellow).
					SetAlign(tview.AlignCenter).
					SetSelectable(true))
			}

			// Fill in the data cells
			for i, category := range categories {
				table.SetCell(i+1, 0, tview.NewTableCell(category).
					SetTextColor(tcell.ColorWhite).
					SetAlign(tview.AlignLeft).
					SetSelectable(true))

				for j, timestamp := range timestamps {
					value, exists := valueMap[category][timestamp]
					if !exists {
						table.SetCell(i+1, j+1, tview.NewTableCell(" ").
							SetSelectable(true))
						continue
					}

					// Apply scaling to the value
					normalizedValue := a.applyScaling(value, minValue, maxValue)
					var color tcell.Color

					if normalizedValue < 0.5 {
						// Green to Yellow
						green := 255
						red := uint8(255 * normalizedValue * 2)
						color = tcell.NewRGBColor(int32(red), int32(green), 0)
					} else {
						// Yellow to Red
						red := 255
						green := uint8(255 * (1 - (normalizedValue-0.5)*2))
						color = tcell.NewRGBColor(int32(red), int32(green), 0)
					}

					// Use single character with background color
					table.SetCell(i+1, j+1, tview.NewTableCell("█").
						SetBackgroundColor(color).
						SetTextColor(color).
						SetAlign(tview.AlignCenter).
						SetSelectable(true))
				}
			}

			// Set initial title
			baseTitle := fmt.Sprintf("Heatmap: %s by %s (%s to %s)",
				getMetricName(a.heatmapMetric),
				getCategoryName(a.categoryType),
				a.fromTime.Format("2006-01-02 15:04:05 -07:00"),
				a.toTime.Format("2006-01-02 15:04:05 -07:00"))

			table.SetTitle(baseTitle).SetBorder(true)

			// Create legend
			legend := a.generateLegend(minValue, maxValue)

			// Create scroll bars with dynamic sizing
			horizontalScroll := tview.NewTextView().
				SetDynamicColors(true).
				SetRegions(true).
				SetScrollable(false).
				SetTextColor(tcell.ColorWhite)
			horizontalScroll.SetBackgroundColor(tcell.ColorDarkSlateGray)

			verticalScroll := tview.NewTextView().
				SetDynamicColors(true).
				SetRegions(true).
				SetScrollable(false).
				SetTextColor(tcell.ColorWhite)
			horizontalScroll.SetBackgroundColor(tcell.ColorDarkSlateGray)

			// Create scrollable wrapper with vertical scroll
			scrollWrapper := tview.NewFlex().
				SetDirection(tview.FlexColumn).
				AddItem(table, 0, 1, true).
				AddItem(verticalScroll, 1, 0, false) // Fixed width

			// Create main flex with horizontal scroll
			mainFlex := tview.NewFlex().
				SetDirection(tview.FlexRow).
				AddItem(scrollWrapper, 0, 1, true).
				AddItem(horizontalScroll, 1, 0, false) // Fixed height

			// Store previous selection for color restoration
			var prevRow, prevCol = -1, -1

			// Update scroll bars and table title when table selection changes
			table.SetSelectionChangedFunc(func(row, column int) {
				rowsCount := table.GetRowCount()
				colsCount := table.GetColumnCount()

				// Restore previous cell colors if there was a previous selection
				if prevRow > 0 && prevCol > 0 && prevRow <= len(categories) && prevCol <= len(timestamps) {
					category := categories[prevRow-1]
					timestamp := timestamps[prevCol-1]
					value, exists := valueMap[category][timestamp]
					if exists {
						// Restore original colors
						normalizedValue := a.applyScaling(value, minValue, maxValue)
						var color tcell.Color
						if normalizedValue < 0.5 {
							green := 255
							red := uint8(255 * normalizedValue * 2)
							color = tcell.NewRGBColor(int32(red), int32(green), 0)
						} else {
							red := 255
							green := uint8(255 * (1 - (normalizedValue-0.5)*2))
							color = tcell.NewRGBColor(int32(red), int32(green), 0)
						}
						table.SetCell(prevRow, prevCol, tview.NewTableCell("█").
							SetBackgroundColor(color).
							SetTextColor(color).
							SetAlign(tview.AlignCenter).
							SetSelectable(true))
					}
				}

				// Update current cell colors if it's a data cell with value
				if row > 0 && column > 0 && row <= len(categories) && column <= len(timestamps) {
					category := categories[row-1]
					timestamp := timestamps[column-1]
					value, exists := valueMap[category][timestamp]
					if exists {
						// Invert colors for selected cell
						normalizedValue := a.applyScaling(value, minValue, maxValue)
						var originalColor tcell.Color
						if normalizedValue < 0.5 {
							green := 255
							red := uint8(255 * normalizedValue * 2)
							originalColor = tcell.NewRGBColor(int32(red), int32(green), 0)
						} else {
							red := 255
							green := uint8(255 * (1 - (normalizedValue-0.5)*2))
							originalColor = tcell.NewRGBColor(int32(red), int32(green), 0)
						}

						// Create inverted color (swap background and text)
						table.SetCell(row, column, tview.NewTableCell("█").
							SetBackgroundColor(tcell.ColorWhite).
							SetTextColor(originalColor).
							SetAlign(tview.AlignCenter).
							SetSelectable(true))
					}
				}

				// Store current selection for next iteration
				prevRow, prevCol = row, column

				// Update table title when column is selected
				var titleText string
				if column > 0 && column <= len(timestamps) {
					timestamp := timestamps[column-1]
					var timeText string
					if interval == "1 MINUTE" || interval == "10 MINUTE" {
						timeText = timestamp.In(tzLocation).Format("15:04:05")
					} else if interval == "1 HOUR" {
						timeText = timestamp.In(tzLocation).Format("15:00:00")
					} else {
						timeText = timestamp.In(tzLocation).Format("2006-01-02 15:04:05")
					}
					titleText = fmt.Sprintf("%s | [yellow]Current Time: %s[white]", baseTitle, timeText)
				} else {
					titleText = baseTitle
				}
				table.SetTitle(titleText)

				// Get available dimensions
				_, _, width, height := mainFlex.GetRect()
				scrollWidth := width - 10  // Account for legend width
				scrollHeight := height - 1 // Account for horizontal scroll height

				// Update horizontal scroll
				if colsCount > 0 && scrollWidth > 0 {
					pos := int(float64(column) / float64(colsCount-1) * float64(scrollWidth))
					scrollText := "[red]◄[white]" + strings.Repeat("─", pos) + "[red]●[white]" + strings.Repeat("─", scrollWidth-pos) + "[red]►"
					horizontalScroll.SetText(scrollText)
				}

				// Update vertical scroll
				if rowsCount > 0 && scrollHeight > 2 {
					// Reserve space for ▲ and ▼ characters
					availableHeight := scrollHeight - 2
					pos := int(float64(row) / float64(rowsCount-1) * float64(availableHeight))
					scrollText := "[red]▲[white]\n"
					for i := 0; i < availableHeight; i++ {
						if i == pos {
							scrollText += "[red::b]●[-:-:-]\n"
						} else {
							scrollText += "[white]│[-]\n"
						}
					}
					scrollText += "[red]▼[-]"
					verticalScroll.SetText(scrollText)
				} else if scrollHeight > 0 {
					// Minimal scroll bar for very small heights
					verticalScroll.SetText("[red]▲\n▼[-]")
				}
			})

			// Create a flex container for the heatmap and legend
			flex := tview.NewFlex().
				SetDirection(tview.FlexColumn).
				AddItem(mainFlex, 0, 1, true).
				AddItem(legend, 10, 0, false)

			selectedHandler := func(row, col int) {
				// Handle cell selection in the data area
				if row > 0 && col > 0 {
					category := categories[row-1]
					timestamp := timestamps[col-1]
					value, exists := valueMap[category][timestamp]

					if exists {
						info := fmt.Sprintf("Category: %s\nTime: %s\n%s: %.2f\n\nPress Enter to generate flamegraph for this selection",
							category,
							timestamp.Format("2006-01-02 15:04:05"),
							getMetricName(a.heatmapMetric),
							value)

						a.mainView.SetText(info)

						// Save selected data for use in flamegraph
						a.categoryValue = category
						a.flamegraphTimeStamp = timestamp
					}
				} else if row > 0 && col == 0 {
					// Handle categoryType selection (row header)
					category := categories[row-1]
					info := fmt.Sprintf("Selected Category: %s\n\nPress Enter to generate flamegraph for this categoryType with global time range",
						category)
					a.mainView.SetText(info)
					a.categoryValue = category
				} else if row == 0 && col > 0 {
					// Handle timestamp selection (column header)
					timestamp := timestamps[col-1]
					info := fmt.Sprintf("Selected Time: %s\n\nPress Enter to generate flamegraph for all categories at this time",
						timestamp.Format("2006-01-02 15:04:05"))
					a.mainView.SetText(info)
					a.flamegraphTimeStamp = timestamp
				}
			}
			// Add selection handler
			table.SetSelectedFunc(selectedHandler)

			// Add key handler for the table
			table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
				// Handle Ctrl+Arrow key navigation
				if event.Modifiers()&tcell.ModCtrl != 0 {
					switch event.Key() {
					case tcell.KeyUp:
						// Move to first row (0), same column
						_, col := table.GetSelection()
						table.Select(0, col)
						return nil
					case tcell.KeyDown:
						// Move to last row, same column
						_, col := table.GetSelection()
						rowCount := table.GetRowCount()
						if rowCount > 0 {
							table.Select(rowCount-1, col)
						}
						return nil
					case tcell.KeyLeft:
						// Move to first column (0), same row
						row, _ := table.GetSelection()
						table.Select(row, 0)
						return nil
					case tcell.KeyRight:
						// Move to last column, same row
						row, _ := table.GetSelection()
						colCount := table.GetColumnCount()
						if colCount > 0 {
							table.Select(row, colCount-1)
						}
						return nil
					}

					// Handle zoom functions with Ctrl+Plus/Minus/0 (more reliable than Ctrl+Alt)
					if event.Modifiers()&tcell.ModCtrl != 0 {
						switch event.Rune() {
						case '+', '=': // Zoom in (Ctrl++)
							log.Info().Msg("ZOOM-IN")
							row, col := table.GetSelection()
							// Only zoom in on data cells
							if row > 0 && col > 0 && row <= len(categories) && col <= len(timestamps) {
								timestamp := timestamps[col-1]
								fromTime := timestamp
								toTime := timestamp.Add(time.Duration(intervalSeconds) * time.Second)

								// Zoom in by reducing the time range to the selected cell's interval
								zoomFactor := 0.5
								currentRange := toTime.Sub(fromTime)
								newRange := time.Duration(float64(currentRange) * zoomFactor)
								center := fromTime.Add(currentRange / 2)
								a.fromTime = center.Add(-newRange / 2)
								a.toTime = center.Add(newRange / 2)

								// Regenerate heatmap with new time range
								a.ShowHeatmap()
							}
							return nil
						case '-': // Zoom out (Ctrl+-)
							log.Info().Msg("ZOOM-OUT")
							// Zoom out by expanding the time range
							currentRange := a.toTime.Sub(a.fromTime)
							zoomFactor := 2.0
							newRange := time.Duration(float64(currentRange) * zoomFactor)
							center := a.fromTime.Add(currentRange / 2)
							a.fromTime = center.Add(-newRange / 2)
							a.toTime = center.Add(newRange / 2)

							// But don't exceed the initial range
							if a.fromTime.Before(a.initialFromTime) {
								a.fromTime = a.initialFromTime
							}
							if a.toTime.After(a.initialToTime) {
								a.toTime = a.initialToTime
							}

							// Regenerate heatmap with new time range
							a.ShowHeatmap()
							return nil
						case '0': // Reset to initial range (Ctrl+0)
							log.Info().Msg("ZOOM-RESET")
							a.fromTime = a.initialFromTime
							a.toTime = a.initialToTime
							// Regenerate heatmap with initial time range
							a.ShowHeatmap()
							return nil
						}
					}
				}

				// When Enter is pressed, show action menu
				if event.Key() == tcell.KeyEnter {
					row, col := table.GetSelection()

					// Determine categoryType type and trace type
					var categoryType = a.categoryType
					var categoryValue string
					var fromTime, toTime time.Time

					// Set trace type based on metric
					var traceType TraceType
					if a.heatmapMetric == MetricMemoryUsage {
						traceType = TraceMemory
					} else {
						traceType = TraceReal
					}

					if row > 0 && col > 0 {
						// Cell in data area - specific categoryType and time
						categoryValue = categories[row-1]
						timestamp := timestamps[col-1]
						fromTime = timestamp
						toTime = timestamp.Add(time.Duration(intervalSeconds) * time.Second)
					} else if row > 0 && col == 0 {
						// Category row header - use global time range
						categoryValue = categories[row-1]
						fromTime = a.fromTime
						toTime = a.toTime
					} else if row == 0 && col > 0 {
						// Timestamp column header - use all categories
						timestamp := timestamps[col-1]
						var timeWindow time.Duration
						if interval == "1 MINUTE" {
							timeWindow = 5 * time.Minute
						} else if interval == "10 MINUTE" {
							timeWindow = 30 * time.Minute
						} else if interval == "1 HOUR" {
							timeWindow = 2 * time.Hour
						} else {
							timeWindow = 24 * time.Hour
						}
						fromTime = timestamp.Add(-timeWindow / 2)
						toTime = timestamp.Add(timeWindow / 2)
						categoryType = ""
						categoryValue = ""
					}

					// Build action menu dynamically: include Explain option only for normalized_query_hash category
					menuText := "Select action:\n[f] Flamegraph\n[p] Profile Events"
					buttons := []string{"Flamegraph (f)", "Profile Events (p)"}
					if categoryType == CategoryQueryHash {
						menuText += "\n[q] Explain query"
						buttons = append(buttons, "Explain (q)")
					}
					buttons = append(buttons, "Cancel")

					actionMenu := tview.NewModal().
						SetText(menuText).
						AddButtons(buttons).
						SetDoneFunc(func(buttonIndex int, buttonLabel string) {
							switch buttonLabel {
							case "Flamegraph (f)":
								a.pages.SwitchToPage("main")
								a.generateFlamegraph(categoryType, categoryValue, traceType, fromTime, toTime, a.cluster, "heatmap")
							case "Profile Events (p)":
								a.pages.SwitchToPage("main")
								a.ShowProfileEvents(categoryType, categoryValue, fromTime, toTime, a.cluster)
							case "Explain (q)":
								// Open explain flow. Keep behaviour consistent with other actions.
								a.pages.SwitchToPage("main")
								// ShowExplain will add its own page(s) and switch as needed.
								a.ShowExplain(categoryType, categoryValue, fromTime, toTime, a.cluster)
							case "Cancel":
								a.pages.SwitchToPage("heatmap")
							}
						})
					actionMenu.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
						switch event.Rune() {
						case 'f', 'F':
							a.pages.SwitchToPage("main")
							a.generateFlamegraph(categoryType, categoryValue, traceType, fromTime, toTime, a.cluster, "heatmap")
							return nil
						case 'p', 'P':
							a.pages.SwitchToPage("main")
							a.ShowProfileEvents(categoryType, categoryValue, fromTime, toTime, a.cluster)
							return nil
						case 'q', 'Q':
							// Only respond if option is relevant (category is normalized_query_hash)
							if categoryType == CategoryQueryHash {
								a.pages.SwitchToPage("main")
								a.ShowExplain(categoryType, categoryValue, fromTime, toTime, a.cluster)
								return nil
							}
						case 'c', 'C':
							a.pages.SwitchToPage("heatmap")
							return nil
						}
						if event.Key() == tcell.KeyEscape {
							a.pages.SwitchToPage("heatmap")
							return nil
						}
						return event
					})

					a.pages.AddPage("action_menu", actionMenu, true, true)
					a.pages.SwitchToPage("action_menu")
					return nil
				}
				return event
			})

			// Add mouse handler for double click
			table.SetMouseCapture(func(action tview.MouseAction, event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse) {
				if action == tview.MouseLeftDoubleClick {
					// Get current selection and trigger the selected function
					row, col := table.GetSelection()
					selectedHandler(row, col)
					return action, event
				}
				return action, event
			})

			// Store the table and display it
			a.heatmapTable = table
			a.pages.AddPage("heatmap", flex, true, true)
			a.pages.SwitchToPage("heatmap")
		})
	}()
}
