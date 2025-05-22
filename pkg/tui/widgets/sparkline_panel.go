package widgets

import (
	"fmt"
	"math"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

func ExecuteAndProcessQuery(query string, fields []string, prefix string, filteredTable *FilteredTable, row *int) error {
	// Implementation would be moved from metric_log_handler.go
	// This would be similar to executeAndProcessMetricLogQuery but generalized
	return nil
}

func GenerateSparkline(values []float64) string {
	if len(values) == 0 {
		return ""
	}

	minVal := values[0]
	maxVal := values[0]
	for _, v := range values {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}

	rangeVal := maxVal - minVal
	if rangeVal == 0 {
		rangeVal = 1
	}

	sparks := []rune("▁▂▃▄▅▆▇█")
	var result strings.Builder
	for _, v := range values {
		pos := int(((v - minVal) / rangeVal) * float64(len(sparks)-1))
		if pos < 0 {
			pos = 0
		}
		if pos >= len(sparks) {
			pos = len(sparks) - 1
		}
		result.WriteRune(sparks[pos])
	}
	return result.String()
}

func ShowDescription(app *tview.Application, pages *tview.Pages, name, description string) {
	app.QueueUpdateDraw(func() {
		modal := tview.NewModal().
			SetText(fmt.Sprintf("[yellow]%s[-]\n\n%s", name, description)).
			AddButtons([]string{"OK"}).
			SetDoneFunc(func(buttonIndex int, buttonLabel string) {
				pages.HidePage("metric_desc")
			})

		pages.AddPage("metric_desc", modal, true, true)
	})
}
