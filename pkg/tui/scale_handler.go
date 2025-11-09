package tui

import (
	"fmt"
	"math"

	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	"github.com/Slach/clickhouse-timeline/pkg/utils"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// ScaleType represents the type of scaling to apply to heatmap values
type ScaleType string

const (
	ScaleLinear ScaleType = "linear"
	ScaleLog2   ScaleType = "log2"
	ScaleLog10  ScaleType = "log10"
)

// ScaleSelectedMsg is sent when a scale is selected
type ScaleSelectedMsg struct {
	Scale ScaleType
}

// scaleSelector is a bubbletea model for selecting scale type
type scaleSelector struct {
	list   widgets.FilteredList
	scales []ScaleType
}

func newScaleSelector(width, height int) scaleSelector {
	scales := []ScaleType{ScaleLinear, ScaleLog2, ScaleLog10}
	scaleNames := []string{
		"Linear",
		"Logarithmic (base 2)",
		"Logarithmic (base 10)",
	}

	listModel := widgets.NewFilteredList("Select Scale Type", scaleNames, width, height)

	return scaleSelector{
		list:   listModel,
		scales: scales,
	}
}

func (m scaleSelector) Init() tea.Cmd {
	return nil
}

func (m scaleSelector) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "enter":
			// Get selected scale
			selectedIdx := m.list.SelectedIndex()
			if selectedIdx >= 0 && selectedIdx < len(m.scales) {
				return m, func() tea.Msg {
					return ScaleSelectedMsg{Scale: m.scales[selectedIdx]}
				}
			}
		case "esc", "q":
			// Return to main - parent will handle this
			return m, nil
		}
	}

	// Delegate to list
	m.list, cmd = m.list.Update(msg)
	return m, cmd
}

func (m scaleSelector) View() string {
	return m.list.View()
}

// showScaleSelector displays a list of available scaling options
func (a *App) showScaleSelector() {
	// Create bubbletea scale selector
	selector := newScaleSelector(a.width, a.height)
	a.scaleHandler = selector
	a.currentPage = pageScale
}

// applyScaling applies the selected scaling to a value
func (a *App) applyScaling(value, minValue, maxValue float64) float64 {
	// Normalize to 0-1 range first
	normalizedValue := (value - minValue) / (maxValue - minValue)

	switch a.scaleType {
	case ScaleLog2:
		if normalizedValue > 0 {
			// Apply log2 scaling (add small value to avoid log(0))
			return math.Log2(normalizedValue+0.0001) / math.Log2(1.0001)
		}
		return 0
	case ScaleLog10:
		if normalizedValue > 0 {
			// Apply log10 scaling (add small value to avoid log(0))
			return math.Log10(normalizedValue+0.0001) / math.Log10(1.0001)
		}
		return 0
	default: // Linear
		return normalizedValue
	}
}

// generateLegend creates a legend for bubbletea using lipgloss
func (a *App) generateLegend(minValue, maxValue float64) string {
	var legend string

	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("white")).
		Padding(0, 1)

	legend += titleStyle.Render(fmt.Sprintf("Scale: %s", a.scaleType)) + "\n\n"

	// Create 5 color steps for the legend
	steps := 5
	for i := 0; i < steps; i++ {
		// Calculate value for this step
		stepValue := minValue + (maxValue-minValue)*float64(i)/float64(steps-1)

		// Format the value
		var displayValue string
		if a.heatmapMetric == MetricCount {
			displayValue = fmt.Sprintf("%.0f", stepValue)
		} else {
			displayValue = utils.FormatReadable(stepValue, 1)
		}

		// Calculate normalized value for color
		normalizedValue := a.applyScaling(stepValue, minValue, maxValue)

		// Get color for this step (convert to lipgloss color)
		var colorStyle lipgloss.Style
		if normalizedValue < 0.5 {
			// Green to Yellow
			red := int(255 * normalizedValue * 2)
			colorStyle = lipgloss.NewStyle().Foreground(lipgloss.Color(fmt.Sprintf("#%02xFF00", red)))
		} else {
			// Yellow to Red
			green := int(255 * (1 - (normalizedValue-0.5)*2))
			colorStyle = lipgloss.NewStyle().Foreground(lipgloss.Color(fmt.Sprintf("#FF%02X00", green)))
		}

		// Add color indicator and value
		legend += colorStyle.Render("█ ") + displayValue + "\n"
	}

	borderStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("62")).
		Padding(1)

	return borderStyle.Render(legend)
}
