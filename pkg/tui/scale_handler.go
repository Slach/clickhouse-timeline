package tui

import (
	"fmt"
	"math"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// ScaleType represents the type of scaling to apply to heatmap values
type ScaleType string

const (
	ScaleLinear ScaleType = "linear"
	ScaleLog2   ScaleType = "log2"
	ScaleLog10  ScaleType = "log10"
)

// showScaleSelector displays a list of available scaling options
func (a *App) showScaleSelector() {
	scaleList := tview.NewList()
	scaleList.SetTitle("Select Scale Type")
	scaleList.SetBorder(true)

	scales := []struct {
		name  string
		scale ScaleType
	}{
		{"Linear", ScaleLinear},
		{"Logarithmic (base 2)", ScaleLog2},
		{"Logarithmic (base 10)", ScaleLog10},
	}

	for i, s := range scales {
		scaleList.AddItem(s.name, "", rune('1'+i), nil)
	}

	scaleList.SetSelectedFunc(func(i int, _ string, _ string, _ rune) {
		a.scaleType = scales[i].scale
		a.SwitchToMainPage(fmt.Sprintf("Scale changed to: %s", a.scaleType))

		// If we already have a heatmap, regenerate it with the new scale
		if a.heatmapTable != nil {
			a.ShowHeatmap()
		}
	})

	a.scaleList = scaleList
	a.pages.AddPage("scales", scaleList, true, true)
	a.pages.SwitchToPage("scales")
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

// generateLegend creates a legend showing the color scale with values
func (a *App) generateLegend(minValue, maxValue float64) *tview.Table {
	legend := tview.NewTable().
		SetBorders(false)

	// Create 5 color steps for the legend
	steps := 5
	for i := 0; i < steps; i++ {
		// Calculate value for this step
		stepValue := minValue + (maxValue-minValue)*float64(i)/float64(steps-1)

		// Format the value
		var displayValue string
		if a.heatmapMetric == MetricCount {
			displayValue = fmt.Sprintf("%.0f", stepValue)
		} else if stepValue >= 1000000000 {
			displayValue = fmt.Sprintf("%.1fG", stepValue/1000000000)
		} else if stepValue >= 1000000 {
			displayValue = fmt.Sprintf("%.1fM", stepValue/1000000)
		} else if stepValue >= 1000 {
			displayValue = fmt.Sprintf("%.1fK", stepValue/1000)
		} else {
			displayValue = fmt.Sprintf("%.1f", stepValue)
		}

		// Calculate normalized value for color
		normalizedValue := a.applyScaling(stepValue, minValue, maxValue)

		// Get color for this step
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

		// Add color box and value
		legend.SetCell(i, 0, tview.NewTableCell(" ").
			SetBackgroundColor(color))
		legend.SetCell(i, 1, tview.NewTableCell(displayValue).
			SetAlign(tview.AlignLeft))
	}

	// Add scale type as title
	legend.SetTitle(fmt.Sprintf(" %s ", string(a.scaleType))).
		SetBorder(true).
		SetTitleAlign(tview.AlignCenter)

	return legend
}
