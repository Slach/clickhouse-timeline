package tui

import tea "github.com/charmbracelet/bubbletea"

func (a *App) handleQuitCommand() tea.Cmd {
	return tea.Quit
}
