package widgets

import (
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type FilteredList struct {
	List       *tview.List
	Title      string
	Items      []string
	FilterPage string
}

func NewFilteredList(list *tview.List, title string, items []string, filterPage string) *FilteredList {
	return &FilteredList{
		List:       list,
		Title:      title,
		Items:      items,
		FilterPage: filterPage,
	}
}

func (fl *FilteredList) SetupFilterInput(app *tview.Application, pages *tview.Pages) *tview.InputField {
	filterInput := tview.NewInputField().
		SetLabel("/").
		SetFieldWidth(30)

	// DoneFunc will close the transient overlay (if present) and restore focus to the original list.
	filterInput.SetDoneFunc(func(key tcell.Key) {
		overlayName := fl.FilterPage + "_overlay"
		if key == tcell.KeyEscape {
			// Just close overlay and restore focus, do not mutate original page structure.
			pages.RemovePage(overlayName)
			app.SetFocus(fl.List)
		} else if key == tcell.KeyEnter {
			// Apply the filter to the real list and close overlay.
			filterText := filterInput.GetText()
			if filterText != "" {
				fl.List.SetTitle(fmt.Sprintf("%s [::b::cyan]/%s[-:-:-]", fl.Title, filterText))
			} else {
				fl.List.SetTitle(fl.Title)
			}
			fl.FilterList(filterText)
			pages.RemovePage(overlayName)
			app.SetFocus(fl.List)
		}
	})

	filterInput.SetMouseCapture(func(action tview.MouseAction, event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse) {
		return action, event
	})

	return filterInput
}

func (fl *FilteredList) ShowFilterInput(app *tview.Application, pages *tview.Pages) {
	// Create a transient overlay page so the underlying page remains intact.
	overlayName := fl.FilterPage + "_overlay"

	// Prepare a dedicated input for the overlay (done func already handles closing overlay).
	filterInput := fl.SetupFilterInput(app, pages)

	// Create a temporary list to render filtered results in the overlay.
	tempList := tview.NewList().ShowSecondaryText(false)
	tempList.SetMainTextColor(tcell.ColorWhite)

	// Populate function for the temporary list based on current fl.Items.
	populate := func(filter string) {
		tempList.Clear()
		lower := strings.ToLower(filter)
		for _, item := range fl.Items {
			if lower == "" || strings.Contains(strings.ToLower(item), lower) {
				// Add visible items to the temporary list.
				// Capture item text by value in the selected func closure below.
				tempList.AddItem(item, "", 0, nil)
			}
		}
	}

	// Wire changed event on the overlay input to update the temporary list.
	filterInput.SetChangedFunc(func(filterText string) {
		populate(filterText)
	})

	// When a user selects an item in the overlay list, close the overlay and focus the real list.
	tempList.SetSelectedFunc(func(index int, mainText string, _ string, _ rune) {
		selected := mainText
		// Find and select the same item in the real list if possible.
		for i, it := range fl.Items {
			if it == selected {
				fl.List.SetCurrentItem(i)
				break
			}
		}
		// Close overlay and focus the real list.
		pages.RemovePage(overlayName)
		app.SetFocus(fl.List)
	})

	// Initial population
	populate("")

	// Layout for overlay: input on top, temporary list below.
	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(filterInput, 1, 0, true).
		AddItem(tempList, 0, 1, false)

	// Add overlay page (do not remove the original page). Show overlay above current pages.
	pages.RemovePage(overlayName) // ensure no duplicate
	pages.AddPage(overlayName, flex, true, true)
	app.SetFocus(filterInput)
}

func (fl *FilteredList) FilterList(filter string) {
	fl.List.Clear()

	if filter == "" {
		fl.ResetList()
		return
	}

	fl.List.SetTitle(fmt.Sprintf("%s [::b::cyan]/%s[-:-:-]", fl.Title, filter))
	filter = strings.ToLower(filter)
	for _, item := range fl.Items {
		if strings.Contains(strings.ToLower(item), filter) {
			fl.List.AddItem(item, "", 0, nil).ShowSecondaryText(false)
		}
	}
}

func (fl *FilteredList) ResetList() {
	fl.List.Clear()
	fl.List.SetTitle(fl.Title)
	for _, item := range fl.Items {
		fl.List.AddItem(item, "", 0, nil).ShowSecondaryText(false)
	}
}
