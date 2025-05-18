package tui

import (
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type FilterableList struct {
	List      *tview.List
	Title     string
	Items     []string
	FilterKey string
}

func (a *App) NewFilterableList(list *tview.List, title string, items []string, filterKey string) *FilterableList {
	return &FilterableList{
		List:      list,
		Title:     title,
		Items:     items,
		FilterKey: filterKey,
	}
}

func (a *App) setupFilterInput(fl *FilterableList) *tview.InputField {
	filterInput := tview.NewInputField().
		SetLabel("/").
		SetFieldWidth(30).
		SetChangedFunc(func(filterText string) {
			a.filterList(fl, filterText)
		}).
		SetDoneFunc(func(key tcell.Key) {
			if key == tcell.KeyEscape {
				a.resetList(fl)
				a.pages.RemovePage(fl.FilterKey)
				a.pages.AddPage(fl.FilterKey, fl.List, true, true)
				a.tviewApp.SetFocus(fl.List)
			} else if key == tcell.KeyEnter {
				filterText := filterInput.GetText()
				if filterText != "" {
					fl.List.SetTitle(fmt.Sprintf("%s [::b::cyan]/%s[-:-:-]", fl.Title, filterText))
				} else {
					fl.List.SetTitle(fl.Title)
				}
				a.filterList(fl, filterText)
				a.pages.RemovePage(fl.FilterKey)
				a.pages.AddPage(fl.FilterKey, fl.List, true, true)
				a.tviewApp.SetFocus(fl.List)
			}
		})

	filterInput.SetMouseCapture(func(action tview.MouseAction, event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse) {
		return action, event
	})

	return filterInput
}

func (a *App) showFilterInput(fl *FilterableList) {
	filterInput := a.setupFilterInput(fl)

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(filterInput, 1, 0, true).
		AddItem(fl.List, 0, 1, false)

	a.pages.RemovePage(fl.FilterKey)
	a.pages.AddPage(fl.FilterKey, flex, true, true)
	a.tviewApp.SetFocus(filterInput)
}

func (a *App) filterList(fl *FilterableList, filter string) {
	fl.List.Clear()

	if filter == "" {
		a.resetList(fl)
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

func (a *App) resetList(fl *FilterableList) {
	fl.List.Clear()
	fl.List.SetTitle(fl.Title)
	for _, item := range fl.Items {
		fl.List.AddItem(item, "", 0, nil).ShowSecondaryText(false)
	}
}
