package tui

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/Slach/clickhouse-timeline/pkg/timezone"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/rs/zerolog/log"
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

// HeatmapDataMsg is sent when heatmap data is loaded
type HeatmapDataMsg struct {
	Timestamps   []time.Time
	Categories   []string
	ValueMap     map[string]map[time.Time]float64
	MinValue     float64
	MaxValue     float64
	Interval     string
	IntervalSecs int
	TzLocation   *time.Location
	Err          error
}

// heatmapViewer is a bubbletea model for heatmap display
type heatmapViewer struct {
	viewport viewport.Model
	loading  bool
	err      error
	width    int
	height   int

	// Heatmap data
	timestamps   []time.Time
	categories   []string
	valueMap     map[string]map[time.Time]float64
	minValue     float64
	maxValue     float64
	interval     string
	intervalSecs int
	tzLocation   *time.Location

	// Selection state
	selectedRow int // 0 = header, 1+ = data rows
	selectedCol int // 0 = category column, 1+ = time columns

	// Action menu
	showActionMenu bool
	actionMenuIdx  int
	actionMenuOpts []string

	// Scroll position
	scrollX int
	scrollY int

	// Context for actions
	categoryType  CategoryType
	categoryValue string
	fromTime      time.Time
	toTime        time.Time
	cluster       string
	scaleType     ScaleType
	heatmapMetric HeatmapMetric
}

func newHeatmapViewer(categoryType CategoryType, fromTime, toTime time.Time, cluster string, scaleType ScaleType, metric HeatmapMetric, width, height int) heatmapViewer {
	vp := viewport.New(width-4, height-8)
	vp.SetContent("Loading heatmap data...")

	return heatmapViewer{
		viewport:      vp,
		loading:       true,
		width:         width,
		height:        height,
		categoryType:  categoryType,
		fromTime:      fromTime,
		toTime:        toTime,
		cluster:       cluster,
		scaleType:     scaleType,
		heatmapMetric: metric,
		selectedRow:   1, // Start at first data row, not header
		selectedCol:   1, // Start at first data column
	}
}

func (m heatmapViewer) Init() tea.Cmd {
	return nil
}

func (m heatmapViewer) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case HeatmapDataMsg:
		m.loading = false
		if msg.Err != nil {
			m.err = msg.Err
			return m, nil
		}

		m.timestamps = msg.Timestamps
		m.categories = msg.Categories
		m.valueMap = msg.ValueMap
		m.minValue = msg.MinValue
		m.maxValue = msg.MaxValue
		m.interval = msg.Interval
		m.intervalSecs = msg.IntervalSecs
		m.tzLocation = msg.TzLocation

		// Render heatmap content
		m.viewport.SetContent(m.renderHeatmap())
		return m, nil

	case tea.KeyMsg:
		if m.showActionMenu {
			return m.handleActionMenuKey(msg)
		}

		switch msg.String() {
		case "esc", "q":
			return m, func() tea.Msg {
				return tea.KeyMsg{Type: tea.KeyEsc}
			}
		case "up":
			if m.selectedRow > 0 {
				m.selectedRow--
				m.viewport.SetContent(m.renderHeatmap())
				// Auto-scroll viewport if cursor moves above visible area
				if m.selectedRow > 0 && m.selectedRow-1 < m.viewport.YOffset {
					m.viewport.LineUp(1)
				}
			}
		case "down":
			if m.selectedRow < len(m.categories) {
				m.selectedRow++
				m.viewport.SetContent(m.renderHeatmap())
				// Auto-scroll viewport if cursor moves below visible area
				if m.selectedRow > 0 {
					visibleBottom := m.viewport.YOffset + m.viewport.Height
					if m.selectedRow-1 >= visibleBottom {
						m.viewport.LineDown(1)
					}
				}
			}
		case "left":
			if m.selectedCol > 0 {
				m.selectedCol--
				m.viewport.SetContent(m.renderHeatmap())
			}
		case "right":
			if m.selectedCol < len(m.timestamps) {
				m.selectedCol++
				m.viewport.SetContent(m.renderHeatmap())
			}
		case "home":
			// Home key - jump to first row and scroll to top
			m.selectedRow = 1
			m.viewport.SetContent(m.renderHeatmap())
			m.viewport.GotoTop()
		case "end":
			// End key - jump to last row and scroll to bottom
			m.selectedRow = len(m.categories)
			m.viewport.SetContent(m.renderHeatmap())
			m.viewport.GotoBottom()
		case "ctrl+up":
			m.selectedRow = 1 // Jump to first data row, not header
			m.viewport.SetContent(m.renderHeatmap())
			// Scroll to top
			m.viewport.GotoTop()
		case "ctrl+down":
			m.selectedRow = len(m.categories)
			m.viewport.SetContent(m.renderHeatmap())
			// Scroll to bottom
			m.viewport.GotoBottom()
		case "ctrl+left":
			m.selectedCol = 1 // Jump to first data column
			m.viewport.SetContent(m.renderHeatmap())
		case "ctrl+right":
			m.selectedCol = len(m.timestamps)
			m.viewport.SetContent(m.renderHeatmap())
		case "enter":
			// Show action menu
			m.showActionMenu = true
			m.actionMenuIdx = 0
			m.buildActionMenu()
			return m, nil
		case "pgdown", "f", " ":
			// Page down - scroll viewport and move cursor
			oldYOffset := m.viewport.YOffset
			m.viewport.ViewDown()
			newYOffset := m.viewport.YOffset

			// Move cursor by the amount scrolled (approximately)
			scrolledLines := newYOffset - oldYOffset
			if scrolledLines > 0 {
				m.selectedRow += scrolledLines
				if m.selectedRow > len(m.categories) {
					m.selectedRow = len(m.categories)
				}
				m.viewport.SetContent(m.renderHeatmap())
			}
			return m, nil
		case "pgup", "b":
			// Page up - scroll viewport and move cursor
			oldYOffset := m.viewport.YOffset
			m.viewport.ViewUp()
			newYOffset := m.viewport.YOffset

			// Move cursor by the amount scrolled (approximately)
			scrolledLines := oldYOffset - newYOffset
			if scrolledLines > 0 {
				m.selectedRow -= scrolledLines
				if m.selectedRow < 1 {
					m.selectedRow = 1
				}
				m.viewport.SetContent(m.renderHeatmap())
			}
			return m, nil
		}
	}

	// Let viewport handle other scrolling (mouse wheel, etc)
	oldYOffset := m.viewport.YOffset
	m.viewport, cmd = m.viewport.Update(msg)
	newYOffset := m.viewport.YOffset

	// If viewport scrolled (e.g., by mouse), adjust cursor to stay visible
	if oldYOffset != newYOffset {
		scrollDiff := newYOffset - oldYOffset
		if scrollDiff > 0 {
			// Scrolled down
			if m.selectedRow-1 < newYOffset {
				// Cursor is above visible area, move it to top of visible area
				m.selectedRow = newYOffset + 1
			}
		} else if scrollDiff < 0 {
			// Scrolled up
			if m.selectedRow-1 >= newYOffset+m.viewport.Height {
				// Cursor is below visible area, move it to bottom of visible area
				m.selectedRow = newYOffset + m.viewport.Height
			}
		}
		m.viewport.SetContent(m.renderHeatmap())
	}

	return m, cmd
}

func (m heatmapViewer) View() string {
	if m.loading {
		return "Loading heatmap data, please wait..."
	}
	if m.err != nil {
		return fmt.Sprintf("Error loading heatmap: %v\n\nPress ESC to return", m.err)
	}

	if m.showActionMenu {
		return m.renderActionMenu()
	}

	// Render title
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	title := fmt.Sprintf("Heatmap: %s by %s (%s to %s)",
		getMetricName(m.heatmapMetric),
		getCategoryName(m.categoryType),
		m.fromTime.Format("2006-01-02 15:04:05 -07:00"),
		m.toTime.Format("2006-01-02 15:04:05 -07:00"))

	// Add selection info to title
	selectionInfo := ""
	if m.selectedRow == 0 && m.selectedCol == 0 {
		// Top-left corner - all data
		selectionInfo = " | Selected: All categories, all times"
	} else if m.selectedRow == 0 && m.selectedCol > 0 && m.selectedCol <= len(m.timestamps) {
		// Header row with specific column - all categories for this time
		timestamp := m.timestamps[m.selectedCol-1]
		var timeText string
		if m.interval == "1 MINUTE" || m.interval == "10 MINUTE" {
			timeText = timestamp.In(m.tzLocation).Format("15:04:05")
		} else if m.interval == "1 HOUR" {
			timeText = timestamp.In(m.tzLocation).Format("15:00:00")
		} else {
			timeText = timestamp.In(m.tzLocation).Format("2006-01-02 15:04:05")
		}
		selectionInfo = fmt.Sprintf(" | Selected Column: %s (all categories)", timeText)
	} else if m.selectedCol == 0 && m.selectedRow > 0 && m.selectedRow <= len(m.categories) {
		// Category label - this category for all times
		category := m.categories[m.selectedRow-1]
		selectionInfo = fmt.Sprintf(" | Selected Row: %s (all times)", category)
	} else if m.selectedRow > 0 && m.selectedRow <= len(m.categories) && m.selectedCol > 0 && m.selectedCol <= len(m.timestamps) {
		// Both row and column selected (specific cell)
		category := m.categories[m.selectedRow-1]
		timestamp := m.timestamps[m.selectedCol-1]
		var timeText string
		if m.interval == "1 MINUTE" || m.interval == "10 MINUTE" {
			timeText = timestamp.In(m.tzLocation).Format("15:04:05")
		} else if m.interval == "1 HOUR" {
			timeText = timestamp.In(m.tzLocation).Format("15:00:00")
		} else {
			timeText = timestamp.In(m.tzLocation).Format("2006-01-02 15:04:05")
		}

		// Show value if available
		if value, exists := m.valueMap[category][timestamp]; exists {
			selectionInfo = fmt.Sprintf(" | Selected: %s @ %s = %.2f", category, timeText, value)
		} else {
			selectionInfo = fmt.Sprintf(" | Selected: %s @ %s", category, timeText)
		}
	}
	title += selectionInfo

	// Render help
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	help := "Arrows: Navigate | PgUp/PgDn: Page scroll | Home/End: Jump to top/bottom | Enter: Actions | Esc: Exit"

	// Render legend
	legend := m.renderLegend()

	// Render fixed header (outside viewport)
	header := m.renderHeatmapHeader()

	// Combine everything with fixed header
	content := lipgloss.JoinVertical(lipgloss.Left,
		titleStyle.Render(title),
		"",
		header,
		m.viewport.View(),
		"",
		legend,
		"",
		helpStyle.Render(help),
	)

	return content
}

// renderHeatmapHeader renders the fixed header row (outside viewport)
func (m heatmapViewer) renderHeatmapHeader() string {
	if len(m.categories) == 0 || len(m.timestamps) == 0 {
		return ""
	}

	var sb strings.Builder

	// Determine max category name length for alignment
	maxCatLen := len(getCategoryName(m.categoryType))
	for _, cat := range m.categories {
		if len(cat) > maxCatLen {
			maxCatLen = len(cat)
		}
	}

	// Header row - category column + timestamp columns
	headerStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("11")).Bold(true)
	categoryHeader := getCategoryName(m.categoryType)

	// Show selection on category header if col 0 is selected
	if m.selectedCol == 0 {
		selectedStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("15")).
			Background(lipgloss.Color("237")).
			Bold(true)
		categoryHeader = "▼ " + categoryHeader
		sb.WriteString(selectedStyle.Render(fmt.Sprintf("%-*s", maxCatLen+2, categoryHeader)))
	} else {
		categoryHeader = "  " + categoryHeader
		sb.WriteString(headerStyle.Render(fmt.Sprintf("%-*s", maxCatLen+2, categoryHeader)))
	}
	sb.WriteString(" ")

	// Timestamp column headers (use dots)
	for i := range m.timestamps {
		colNum := i + 1
		// Show selection on header timestamp
		if m.selectedCol == colNum && m.selectedRow == 0 {
			// Selected column indicator when on header row - bright and larger
			selectedStyle := lipgloss.NewStyle().
				Foreground(lipgloss.Color("15")).
				Background(lipgloss.Color("237")).
				Bold(true)
			sb.WriteString(selectedStyle.Render("▼"))
		} else if m.selectedCol == colNum {
			// Column is selected but we're not on header row - just show indicator
			selectedStyle := lipgloss.NewStyle().
				Foreground(lipgloss.Color("11")).
				Bold(true)
			sb.WriteString(selectedStyle.Render("•"))
		} else {
			sb.WriteString(headerStyle.Render("•"))
		}
	}

	return sb.String()
}

// renderHeatmap generates the heatmap data rows (scrollable content in viewport)
func (m heatmapViewer) renderHeatmap() string {
	if len(m.categories) == 0 || len(m.timestamps) == 0 {
		return "No data available"
	}

	var sb strings.Builder

	// Determine max category name length for alignment
	maxCatLen := len(getCategoryName(m.categoryType))
	for _, cat := range m.categories {
		if len(cat) > maxCatLen {
			maxCatLen = len(cat)
		}
	}

	// Data rows (header is now rendered separately)
	for i, category := range m.categories {
		rowNum := i + 1
		isRowSelected := m.selectedRow == rowNum

		// Category name column - highlight if this row or col 0 is selected
		var catStyle lipgloss.Style
		catText := category
		if isRowSelected && m.selectedCol == 0 {
			// Both row and col 0 selected - this category label is the focus
			catStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("15")).
				Background(lipgloss.Color("237")).
				Bold(true)
			catText = "◆ " + catText
		} else if isRowSelected {
			// Row selected - show row indicator
			catStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("15")).
				Background(lipgloss.Color("237")).
				Bold(true)
			catText = "▶ " + catText
		} else if m.selectedCol == 0 {
			// Col 0 selected but not this row - just show dimmed
			catStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("11"))
			catText = "  " + catText
		} else {
			catStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("15"))
			catText = "  " + catText
		}
		sb.WriteString(catStyle.Render(fmt.Sprintf("%-*s", maxCatLen+2, catText)))
		sb.WriteString(" ")

		// Value cells
		for j, timestamp := range m.timestamps {
			colNum := j + 1
			value, exists := m.valueMap[category][timestamp]

			// Check selection state for cursor
			isColSelected := m.selectedCol == colNum
			isCellSelected := isRowSelected && isColSelected

			var cellStyle lipgloss.Style

			if !exists {
				// Empty cell - show cursor only at intersection
				if isCellSelected {
					// Selected empty cell (intersection)
					cellStyle = lipgloss.NewStyle().
						Background(lipgloss.Color("15")).
						Foreground(lipgloss.Color("8"))
					sb.WriteString(cellStyle.Render("·"))
				} else {
					// Normal empty cell
					sb.WriteString(" ")
				}
				continue
			}

			// Calculate color based on value
			color := m.getColorForValue(value)

			if isCellSelected {
				// The selected cell (intersection) - cursor position
				cellStyle = lipgloss.NewStyle().
					Background(lipgloss.Color("15")).
					Foreground(lipgloss.Color("0")).
					Bold(true)
				sb.WriteString(cellStyle.Render("●"))
			} else {
				// Normal cell
				cellStyle = lipgloss.NewStyle().
					Background(color).
					Foreground(color)
				sb.WriteString(cellStyle.Render("█"))
			}
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

// getColorForValue returns a lipgloss color based on the value's position in the range
func (m heatmapViewer) getColorForValue(value float64) lipgloss.Color {
	normalizedValue := m.applyScaling(value)

	// Convert 0-1 range to color gradient: green -> yellow -> red
	var colorCode int
	if normalizedValue < 0.5 {
		// Green (2) to Yellow (3)
		colorCode = 2 // Green for low values
	} else if normalizedValue < 0.75 {
		// Yellow (3)
		colorCode = 3
	} else {
		// Red (1) for high values
		colorCode = 1
	}

	return lipgloss.Color(fmt.Sprintf("%d", colorCode))
}

// applyScaling normalizes the value between 0 and 1 based on min/max and scale type
func (m heatmapViewer) applyScaling(value float64) float64 {
	if m.maxValue == m.minValue {
		return 0.5
	}

	switch m.scaleType {
	case ScaleLinear:
		return (value - m.minValue) / (m.maxValue - m.minValue)
	case ScaleLog2:
		if value <= 0 {
			return 0
		}
		logMin := math.Log2(m.minValue + 1)
		logMax := math.Log2(m.maxValue + 1)
		logVal := math.Log2(value + 1)
		return (logVal - logMin) / (logMax - logMin)
	case ScaleLog10:
		if value <= 0 {
			return 0
		}
		logMin := math.Log10(m.minValue + 1)
		logMax := math.Log10(m.maxValue + 1)
		logVal := math.Log10(value + 1)
		return (logVal - logMin) / (logMax - logMin)
	default:
		return (value - m.minValue) / (m.maxValue - m.minValue)
	}
}

// renderLegend generates the color legend
func (m heatmapViewer) renderLegend() string {
	steps := 10
	var sb strings.Builder

	sb.WriteString("Legend: ")

	for i := 0; i < steps; i++ {
		normalized := float64(i) / float64(steps-1)

		// Simple color mapping
		var color lipgloss.Color
		if normalized < 0.5 {
			color = lipgloss.Color("2") // Green
		} else if normalized < 0.75 {
			color = lipgloss.Color("3") // Yellow
		} else {
			color = lipgloss.Color("1") // Red
		}

		cellStyle := lipgloss.NewStyle().
			Background(color).
			Foreground(color)
		sb.WriteString(cellStyle.Render("█"))
	}

	sb.WriteString(fmt.Sprintf("  [%.2f - %.2f]", m.minValue, m.maxValue))

	return sb.String()
}

// buildActionMenu constructs the action menu options based on context
func (m *heatmapViewer) buildActionMenu() {
	m.actionMenuOpts = []string{"Flamegraph", "Profile Events"}

	// Add Explain option only for query hash category
	if m.categoryType == CategoryQueryHash {
		m.actionMenuOpts = append(m.actionMenuOpts, "Explain query")
	}

	// Add zoom options
	m.actionMenuOpts = append(m.actionMenuOpts, "Zoom in", "Zoom out", "Reset zoom", "Cancel")
}

// handleActionMenuKey handles keyboard input for the action menu
func (m heatmapViewer) handleActionMenuKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q", "c":
		m.showActionMenu = false
		return m, nil
	case "up", "k":
		if m.actionMenuIdx > 0 {
			m.actionMenuIdx--
		}
	case "down", "j":
		if m.actionMenuIdx < len(m.actionMenuOpts)-1 {
			m.actionMenuIdx++
		}
	case "enter":
		return m.executeAction()
	case "f":
		// Quick key for flamegraph
		m.actionMenuIdx = 0
		return m.executeAction()
	case "p":
		// Quick key for profile events
		m.actionMenuIdx = 1
		return m.executeAction()
	case "e":
		// Quick key for explain (if available)
		if m.categoryType == CategoryQueryHash {
			m.actionMenuIdx = 2
			return m.executeAction()
		}
	case "z":
		// Quick key for zoom in
		for i, opt := range m.actionMenuOpts {
			if opt == "Zoom in" {
				m.actionMenuIdx = i
				return m.executeAction()
			}
		}
	case "Z":
		// Quick key for zoom out
		for i, opt := range m.actionMenuOpts {
			if opt == "Zoom out" {
				m.actionMenuIdx = i
				return m.executeAction()
			}
		}
	case "r":
		// Quick key for reset zoom
		for i, opt := range m.actionMenuOpts {
			if opt == "Reset zoom" {
				m.actionMenuIdx = i
				return m.executeAction()
			}
		}
	}

	return m, nil
}

// executeAction executes the selected action from the menu
func (m heatmapViewer) executeAction() (tea.Model, tea.Cmd) {
	if m.actionMenuIdx >= len(m.actionMenuOpts) {
		m.showActionMenu = false
		return m, nil
	}

	action := m.actionMenuOpts[m.actionMenuIdx]
	m.showActionMenu = false

	// Determine context for the action based on selection
	var categoryValue string
	var fromTime, toTime time.Time

	if m.selectedRow > 0 && m.selectedCol > 0 {
		// Specific cell selected - use specific category and time interval
		categoryValue = m.categories[m.selectedRow-1]
		timestamp := m.timestamps[m.selectedCol-1]
		fromTime = timestamp
		toTime = timestamp.Add(time.Duration(m.intervalSecs) * time.Second)
	} else if m.selectedRow > 0 && m.selectedCol == 0 {
		// Category label selected (first column) - use this category for whole time range
		categoryValue = m.categories[m.selectedRow-1]
		fromTime = m.fromTime
		toTime = m.toTime
	} else if m.selectedRow == 0 && m.selectedCol > 0 {
		// Column header selected (first row) - use all categories for this time period
		timestamp := m.timestamps[m.selectedCol-1]
		fromTime = timestamp
		toTime = timestamp.Add(time.Duration(m.intervalSecs) * time.Second)
		categoryValue = "" // Empty means all categories
	} else if m.selectedRow == 0 && m.selectedCol == 0 {
		// Top-left corner - all categories, all times
		fromTime = m.fromTime
		toTime = m.toTime
		categoryValue = "" // Empty means all categories
	}

	// Execute the action by returning appropriate message
	switch action {
	case "Flamegraph":
		return m, func() tea.Msg {
			return HeatmapActionMsg{
				Action:        "flamegraph",
				CategoryValue: categoryValue,
				FromTime:      fromTime,
				ToTime:        toTime,
			}
		}
	case "Profile Events":
		return m, func() tea.Msg {
			return HeatmapActionMsg{
				Action:        "profile_events",
				CategoryValue: categoryValue,
				FromTime:      fromTime,
				ToTime:        toTime,
			}
		}
	case "Explain query":
		return m, func() tea.Msg {
			return HeatmapActionMsg{
				Action:        "explain",
				CategoryValue: categoryValue,
				FromTime:      fromTime,
				ToTime:        toTime,
			}
		}
	case "Zoom in":
		if m.selectedRow > 0 && m.selectedCol > 0 {
			timestamp := m.timestamps[m.selectedCol-1]
			fromTime := timestamp
			toTime := timestamp.Add(time.Duration(m.intervalSecs) * time.Second)

			zoomFactor := 0.5
			currentRange := toTime.Sub(fromTime)
			newRange := time.Duration(float64(currentRange) * zoomFactor)
			center := fromTime.Add(currentRange / 2)

			return m, func() tea.Msg {
				return HeatmapActionMsg{
					Action:   "zoom_in",
					FromTime: center.Add(-newRange / 2),
					ToTime:   center.Add(newRange / 2),
				}
			}
		}
	case "Zoom out":
		return m, func() tea.Msg {
			return HeatmapActionMsg{
				Action: "zoom_out",
			}
		}
	case "Reset zoom":
		return m, func() tea.Msg {
			return HeatmapActionMsg{
				Action: "reset_zoom",
			}
		}
	case "Cancel":
		return m, nil
	}

	return m, nil
}

// renderActionMenu renders the action menu
func (m heatmapViewer) renderActionMenu() string {
	var sb strings.Builder

	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	sb.WriteString(titleStyle.Render("Select Action:"))
	sb.WriteString("\n\n")

	for i, opt := range m.actionMenuOpts {
		if i == m.actionMenuIdx {
			sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("10")).Bold(true).Render(fmt.Sprintf("> %s", opt)))
		} else {
			sb.WriteString(fmt.Sprintf("  %s", opt))
		}
		sb.WriteString("\n")
	}

	sb.WriteString("\n")
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	sb.WriteString(helpStyle.Render("↑↓: Navigate | Enter: Select | Esc: Cancel"))

	// Wrap in border
	borderStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("6")).
		Padding(1, 2)

	return borderStyle.Render(sb.String())
}

// HeatmapActionMsg is sent when a heatmap action is triggered
type HeatmapActionMsg struct {
	Action        string
	CategoryValue string
	FromTime      time.Time
	ToTime        time.Time
}

// ShowHeatmap displays the heatmap visualization
func (a *App) ShowHeatmap() tea.Cmd {
	log.Info().
		Str("cluster", a.cluster).
		Str("category", string(a.categoryType)).
		Str("metric", string(a.heatmapMetric)).
		Time("from", a.state.FromTime).
		Time("to", a.state.ToTime).
		Msg("ShowHeatmap called")

	if a.state.ClickHouse == nil {
		a.SwitchToMainPage("Error: Please connect to a ClickHouse instance first using :connect command")
		return nil
	}
	if a.cluster == "" {
		a.SwitchToMainPage("Error: Please select a cluster first using :cluster command")
		return nil
	}

	// Create and show viewer
	viewer := newHeatmapViewer(
		a.categoryType,
		a.state.FromTime,
		a.state.ToTime,
		a.cluster,
		a.scaleType,
		a.heatmapMetric,
		a.width,
		a.height,
	)
	a.heatmapHandler = viewer
	a.currentPage = pageHeatmap

	// Start async data fetch
	return a.fetchHeatmapDataCmd()
}

// fetchHeatmapDataCmd fetches heatmap data from ClickHouse
func (a *App) fetchHeatmapDataCmd() tea.Cmd {
	return func() tea.Msg {
		// Calculate appropriate interval based on time range
		duration := a.state.ToTime.Sub(a.state.FromTime)

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
		fromStr := a.state.FromTime.Format("2006-01-02 15:04:05 -07:00")
		toStr := a.state.ToTime.Format("2006-01-02 15:04:05 -07:00")

		metricSQL := getMetricSQL(a.heatmapMetric)
		categorySQL := getCategorySQL(a.categoryType)

		// Get timezone name from offset
		tzName, offset := a.state.FromTime.Zone()
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
		rows, err := a.state.ClickHouse.Query(query)
		if err != nil {
			return HeatmapDataMsg{Err: fmt.Errorf("error executing query: %v", err)}
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close heatmap query")
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
				return HeatmapDataMsg{Err: fmt.Errorf("error scanning row: %v", err)}
			}

			data = append(data, dataPoint{t, category, value})
		}

		if rowsErr := rows.Err(); rowsErr != nil {
			return HeatmapDataMsg{Err: fmt.Errorf("error reading rows: %v", rowsErr)}
		}

		// Process data for heatmap
		if len(data) == 0 {
			return HeatmapDataMsg{Err: fmt.Errorf("no data found for the selected time range and category")}
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

		// Sort categories alphabetically
		sort.Strings(categories)

		return HeatmapDataMsg{
			Timestamps:   timestamps,
			Categories:   categories,
			ValueMap:     valueMap,
			MinValue:     minValue,
			MaxValue:     maxValue,
			Interval:     interval,
			IntervalSecs: intervalSeconds,
			TzLocation:   tzLocation,
		}
	}
}
