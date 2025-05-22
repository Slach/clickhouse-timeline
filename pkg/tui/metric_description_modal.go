package tui

import (
	"fmt"
	"github.com/rivo/tview"
	"github.com/rs/zerolog/log"
	"strings"
)

func (a *App) showMetricDescription(metricName string) {
	var query string
	var source string

	if strings.HasPrefix(metricName, "M_") {
		// Metric from system.metrics
		cleanName := strings.TrimPrefix(metricName, "M_")
		query = fmt.Sprintf("SELECT description FROM system.metrics WHERE name = '%s'", cleanName)
		source = "metric"
	} else if strings.HasPrefix(metricName, "P_") {
		// Event from system.events
		cleanName := strings.TrimPrefix(metricName, "P_")
		query = fmt.Sprintf("SELECT description FROM system.events WHERE name = '%s'", cleanName)
		source = "event"
	} else if strings.HasPrefix(metricName, "A_") {
		// Metric from system.asynchronous_metrics
		cleanName := strings.TrimPrefix(metricName, "A_")
		query = fmt.Sprintf("SELECT description FROM system.asynchronous_metrics WHERE name = '%s'", cleanName)
		source = "asynchronous_metrics"
	} else {
		return
	}

	rows, err := a.clickHouse.Query(query)
	if err != nil {
		a.tviewApp.QueueUpdateDraw(func() {
			a.SwitchToMainPage(fmt.Sprintf("Error getting %s description: %v", source, err))
		})
		return
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msgf("can't close description rows for %s metric", metricName)
		}
	}()

	var description string
	if rows.Next() {
		if err := rows.Scan(&description); err != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Error scanning %s description: %v", source, err))
			})
			return
		}
	}

	a.tviewApp.QueueUpdateDraw(func() {
		modal := tview.NewModal().
			SetText(fmt.Sprintf("[yellow]%s[-]\n\n%s", metricName, description)).
			AddButtons([]string{"OK"}).
			SetDoneFunc(func(buttonIndex int, buttonLabel string) {
				a.pages.HidePage("metric_desc")
			})

		a.pages.AddPage("metric_desc", modal, true, true)
	})
}
