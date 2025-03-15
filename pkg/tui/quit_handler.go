package tui

func (a *App) handleQuitCommand() {
	a.tviewApp.Stop()
}
