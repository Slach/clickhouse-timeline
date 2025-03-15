package tui

import (
	"fmt"

	"github.com/Slach/clickhouse-timeline/pkg/client"
	"github.com/Slach/clickhouse-timeline/pkg/config"
	"github.com/rs/zerolog/log"
)

func (a *App) handleConnectCommand() {
	a.pages.SwitchToPage("contexts")
	a.tviewApp.SetFocus(a.connectList)
}

func (a *App) getContextString(ctx config.Context) string {
	return fmt.Sprintf("%s (%s:%d)", ctx.Name, ctx.Host, ctx.Port)
}

func (a *App) handleContextSelection(i int) {
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
		a.mainView.SetText("Error: Could not find selected context")
		a.pages.SwitchToPage("main")
		a.tviewApp.SetFocus(a.mainView)
		return
	}

	ctx := *selectedCtx
	clickHouse := client.NewClient(ctx, a.version)

	version, err := clickHouse.GetVersion()
	if err != nil {
		log.Error().Err(err).Str("host", ctx.Host).Int("port", ctx.Port).Msg("failed to connect to ClickHouse")
		a.mainView.SetText(fmt.Sprintf("Error connecting to ClickHouse: %v", err))
	} else {
		a.clickHouse = clickHouse
		a.mainView.SetText(fmt.Sprintf("Connected to %s:%d : version %s, press ':' to continue", ctx.Host, ctx.Port, version))
	}

	a.pages.SwitchToPage("main")
	a.tviewApp.SetFocus(a.mainView)
}
