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
		SetFieldWidth(30).
		SetChangedFunc(func(filterText string) {
			fl.FilterList(filterText)
		})
	filterInput.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEscape {
			fl.ResetList()
			pages.RemovePage(fl.FilterPage)
			pages.AddPage(fl.FilterPage, fl.List, true, true)
			app.SetFocus(fl.List)
		} else if key == tcell.KeyEnter {
			filterText := filterInput.GetText()
			if filterText != "" {
				fl.List.SetTitle(fmt.Sprintf("%s [::b::cyan]/%s[-:-:-]", fl.Title, filterText))
			} else {
				fl.List.SetTitle(fl.Title)
			}
			fl.FilterList(filterText)
			pages.RemovePage(fl.FilterPage)
			pages.AddPage(fl.FilterPage, fl.List, true, true)
			app.SetFocus(fl.List)
		}
	})

	filterInput.SetMouseCapture(func(action tview.MouseAction, event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse) {
		return action, event
	})

	return filterInput
}

func (fl *FilteredList) ShowFilterInput(app *tview.Application, pages *tview.Pages) {
	filterInput := fl.SetupFilterInput(app, pages)

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(filterInput, 1, 0, true).
		AddItem(fl.List, 0, 1, false)

	pages.RemovePage(fl.FilterPage)
	pages.AddPage(fl.FilterPage, flex, true, true)
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
