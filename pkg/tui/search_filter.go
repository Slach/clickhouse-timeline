package tui

import (
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// setupFilterInput creates and configures the filter input field
func (a *App) setupFilterInput() *tview.InputField {
	filterInput := tview.NewInputField().
		SetLabel("/").
		SetFieldWidth(30).
		SetChangedFunc(func(filterText string) {
			a.filterConnectList(filterText)
		}).
		SetDoneFunc(func(key tcell.Key) {
			if key == tcell.KeyEscape {
				// Reset filter and return focus to the list
				a.resetConnectList()
				// Remove the filter input and recreate the contexts page with just the list
				a.pages.RemovePage("contexts")
				a.pages.AddPage("contexts", a.connectList, true, true)
				a.tviewApp.SetFocus(a.connectList)
			} else if key == tcell.KeyEnter {
				// Keep the filter, hide the input field and focus on the list
				filterText := a.filterInput.GetText()
				if filterText != "" {
					a.connectList.SetTitle(fmt.Sprintf("Connections [::b::cyan]/%s[-:-:-]", filterText))
				} else {
					a.connectList.SetTitle("Connections")
				}
				a.filterConnectList(filterText)

				// Remove the filter input and recreate the contexts page with just the list
				a.pages.RemovePage("contexts")
				a.pages.AddPage("contexts", a.connectList, true, true)
				a.tviewApp.SetFocus(a.connectList)
			}
		})

	filterInput.SetMouseCapture(func(action tview.MouseAction, event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse) {
		return action, event // Pass through mouse events
	})

	return filterInput
}

// showFilterInput displays the filter input field above the connection list
func (a *App) showFilterInput() {
	a.filterInput = a.setupFilterInput()

	// Add the filter input to the contexts page
	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(a.filterInput, 1, 0, true).
		AddItem(a.connectList, 0, 1, false)

	a.pages.RemovePage("contexts")
	a.pages.AddPage("contexts", flex, true, true)
	a.tviewApp.SetFocus(a.filterInput)
}

// filterConnectList filters the connection list based on the provided text
func (a *App) filterConnectList(filter string) {
	a.connectList.Clear()

	if filter == "" {
		a.resetConnectList()
		return
	}

	// Set title with filter value highlighted in cyan
	a.connectList.SetTitle(fmt.Sprintf("Connections [::b::cyan]/%s[-:-:-]", filter))

	filter = strings.ToLower(filter)
	for _, ctx := range a.cfg.Contexts {
		itemText := a.getContextString(ctx)
		if strings.Contains(strings.ToLower(itemText), filter) {
			a.connectList.AddItem(itemText, "", 0, nil).ShowSecondaryText(false)
		}
	}
}

// resetConnectList resets the connection list to show all connections
func (a *App) resetConnectList() {
	a.connectList.Clear()
	a.connectList.SetTitle("Connections")
	for _, ctx := range a.cfg.Contexts {
		a.connectList.AddItem(a.getContextString(ctx), "", 0, nil).ShowSecondaryText(false)
	}
}
