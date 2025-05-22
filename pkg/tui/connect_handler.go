package tui

import (
	"fmt"

	"github.com/Slach/clickhouse-timeline/pkg/client"
	"github.com/Slach/clickhouse-timeline/pkg/config"
	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	"github.com/gdamore/tcell/v2"
	"github.com/rs/zerolog/log"
)

func (a *App) handleConnectCommand() {
	// Prepare items for filtering
	var items []string
	for _, ctx := range a.cfg.Contexts {
		items = append(items, a.getContextString(ctx))
	}

	// Create filtered list widget
	fl := widgets.NewFilteredList(
		a.connectList,
		"Connections",
		items,
		"contexts",
	)

	// Set up list with all items
	fl.ResetList()
	a.connectList.SetSelectedFunc(func(i int, _ string, _ string, _ rune) {
		a.handleContextSelection(i)
	})

	// Add key handler for filtering
	a.connectList.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Rune() == '/' {
			fl.ShowFilterInput(a.tviewApp, a.pages)
			return nil
		}
		return event
	})

	a.pages.SwitchToPage("contexts")
	a.tviewApp.SetFocus(a.connectList)
}

func (a *App) getContextString(ctx config.Context) string {
	return fmt.Sprintf("%s (%s:%d)", ctx.Name, ctx.Host, ctx.Port)
}

func (a *App) handleContextSelection(i int) {
	// Check if list is empty
	if a.connectList.GetItemCount() == 0 {
		a.SwitchToMainPage("Error: No contexts available")
		return
	}

	// Get the selected item text
	selectedText, _ := a.connectList.GetItemText(i)

	// Find the matching context
	var selectedCtx *config.Context
	found := false

	for _, ctx := range a.cfg.Contexts {
		itemText := a.getContextString(ctx)
		if itemText == selectedText {
			selectedCtx = &ctx
			found = true
			break
		}
	}

	if !found {
		a.SwitchToMainPage("Error: Could not find selected context")
		return
	}

	clickHouse := client.NewClient(*selectedCtx, a.version)

	version, err := clickHouse.GetVersion()
	if err != nil {
		log.Error().Err(err).Str("host", selectedCtx.Host).Int("port", selectedCtx.Port).Msg("failed to connect to ClickHouse")
		a.SwitchToMainPage(fmt.Sprintf("Error connecting to ClickHouse %s: %v", err, a.getContextString(*selectedCtx)))
	} else {
		a.clickHouse = clickHouse
		a.SwitchToMainPage(fmt.Sprintf("Connected to %s:%d : version %s, press ':' to continue", selectedCtx.Host, selectedCtx.Port, version))
	}
}
