package tui

import (
	"fmt"
	"github.com/Slach/clickhouse-timeline/pkg/timezone"
	"github.com/rs/zerolog/log"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// SQL template for heatmap queries
const heatmapQueryTemplate = `
WITH
/* broken in 25.3
   toStartOfInterval(toTimeZone(event_time, '%s'), INTERVAL %s) AS query_finish,
   toStartOfInterval(toTimeZone(query_start_time, '%s'), INTERVAL %s) AS query_start,
*/
   intDiv(toUInt32(toStartOfInterval(toTimeZone(event_time, '%s'), INTERVAL %s) - toStartOfInterval(toTimeZone(query_start_time, '%s'), INTERVAL %s) + 1),%d) AS intervals,
   arrayMap(i -> (toStartOfInterval(toTimeZone(query_start_time, '%s'), INTERVAL %s) + i), range(0, toUInt32(toStartOfInterval(toTimeZone(event_time, '%s'), INTERVAL %s) - toStartOfInterval(toTimeZone(query_start_time, '%s'), INTERVAL %s) + 1),%d)) as timestamps
SELECT
    arrayJoin(timestamps) as t,
    %s AS category,
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

	metricSQL := getMetricSQL(a.currentMetric)
	categorySQL := getCategorySQL(a.category)

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
	if a.category == CategoryError {
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
				a.mainView.SetText("No data found for the selected time range and category")
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
				SetFixed(1, 1) // Fix first row and first column

			// Set header row with timestamps
			table.SetCell(0, 0, tview.NewTableCell(getCategoryName(a.category)).
				SetTextColor(tcell.ColorYellow).
				SetAlign(tview.AlignCenter).
				SetSelectable(false))

			// Format timestamps based on interval
			for i, t := range timestamps {
				var cellText string
				if interval == "1 MINUTE" || interval == "10 MINUTE" {
					cellText = t.In(tzLocation).Format("15:04")
				} else if interval == "1 HOUR" {
					cellText = t.In(tzLocation).Format("15:00")
				} else {
					cellText = t.In(tzLocation).Format("01-02")
				}

				table.SetCell(0, i+1, tview.NewTableCell(cellText).
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
						table.SetCell(i+1, j+1, tview.NewTableCell("").
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

					// Use empty cell with background color only
					table.SetCell(i+1, j+1, tview.NewTableCell("").
						SetBackgroundColor(color).
						SetAlign(tview.AlignCenter).
						SetSelectable(true))
				}
			}

			// Set title
			title := fmt.Sprintf("Heatmap: %s by %s (%s to %s)",
				getMetricName(a.currentMetric),
				getCategoryName(a.category),
				a.fromTime.Format("2006-01-02 15:04:05 -07:00"),
				a.toTime.Format("2006-01-02 15:04:05 -07:00"))

			table.SetTitle(title).SetBorder(true)

			// Create legend
			legend := a.generateLegend(minValue, maxValue)

			// Create scrollable table container
			tableContainer := tview.NewFlex().
				SetDirection(tview.FlexRow).
				AddItem(table, 0, 1, true)

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
				AddItem(tableContainer, 0, 1, true).
				AddItem(verticalScroll, 1, 0, false) // Fixed width

			// Create main flex with horizontal scroll
			mainFlex := tview.NewFlex().
				SetDirection(tview.FlexRow).
				AddItem(scrollWrapper, 0, 1, true).
				AddItem(horizontalScroll, 1, 0, false) // Fixed height

			// Update scroll bars when table selection changes
			table.SetSelectionChangedFunc(func(row, column int) {
				rowsCount := table.GetRowCount()
				colsCount := table.GetColumnCount()

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
				if rowsCount > 0 && scrollHeight > 0 {
					pos := int(float64(row) / float64(rowsCount-1) * float64(scrollHeight))
					scrollText := "[red]▲[white]\n"
					for i := 0; i < scrollHeight; i++ {
						if i == pos {
							scrollText += "[red::b]●[-:-:-]\n"
						} else {
							scrollText += "[white]│[-]\n"
						}
					}
					scrollText += "[red]▼[-]"
					verticalScroll.SetText(scrollText)
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
							getMetricName(a.currentMetric),
							value)

						a.mainView.SetText(info)

						// Save selected data for use in flamegraph
						a.selectedCategory = category
						a.selectedTimestamp = timestamp
					}
				} else if row > 0 && col == 0 {
					// Handle category selection (row header)
					category := categories[row-1]
					info := fmt.Sprintf("Selected Category: %s\n\nPress Enter to generate flamegraph for this category with global time range",
						category)
					a.mainView.SetText(info)
					a.selectedCategory = category
				} else if row == 0 && col > 0 {
					// Handle timestamp selection (column header)
					timestamp := timestamps[col-1]
					info := fmt.Sprintf("Selected Time: %s\n\nPress Enter to generate flamegraph for all categories at this time",
						timestamp.Format("2006-01-02 15:04:05"))
					a.mainView.SetText(info)
					a.selectedTimestamp = timestamp
				}
			}
			// Add selection handler
			table.SetSelectedFunc(selectedHandler)

			// Add key handler for the table
			table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
				// When Enter is pressed, show action menu
				if event.Key() == tcell.KeyEnter {
					row, col := table.GetSelection()

					// Determine category type and trace type
					var categoryType = a.category
					var categoryValue string
					var fromTime, toTime time.Time

					// Set trace type based on metric
					var traceType TraceType
					if a.currentMetric == MetricMemoryUsage {
						traceType = TraceMemory
					} else {
						traceType = TraceReal
					}

					if row > 0 && col > 0 {
						// Cell in data area - specific category and time
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

					// Create action menu
					menu := tview.NewModal().
						SetText("Select action:\n[f] Flamegraph\n[p] Profile Events").
						AddButtons([]string{"Flamegraph (f)", "Profile Events (p)", "Cancel"}).
						SetDoneFunc(func(buttonIndex int, buttonLabel string) {
							switch buttonLabel {
							case "Flamegraph (f)":
								a.pages.SwitchToPage("main")
								a.generateFlamegraph(categoryType, categoryValue, traceType, fromTime, toTime, a.cluster, "heatmap")
							case "Profile Events (p)":
								a.pages.SwitchToPage("main")
								a.ShowProfileEvents(categoryType, categoryValue, fromTime, toTime, a.cluster)
							case "Cancel":
								a.pages.SwitchToPage("heatmap")
							}
						})

					a.pages.AddPage("action_menu", menu, true, true)
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
