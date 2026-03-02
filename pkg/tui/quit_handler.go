package tui

import tea "charm.land/bubbletea/v2"

func (a *App) handleQuitCommand() tea.Cmd {
	return tea.Quit
}
