package widgets

import (
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"strings"
)

type FilteredTable struct {
	Table      *tview.Table
	Title      string
	Headers    []string
	OriginalRows [][]*tview.TableCell
}

func NewFilteredTable() *FilteredTable {
	return &FilteredTable{
		Table: tview.NewTable().
			SetBorders(false).
			SetSelectable(true, true),
	}
}

func (ft *FilteredTable) SetupHeaders(headers []string) {
	ft.Headers = headers
	for col, header := range headers {
		ft.Table.SetCell(0, col,
			tview.NewTableCell(header).
				SetTextColor(tcell.ColorYellow).
				SetAlign(tview.AlignCenter),
		)
	}
}

func (ft *FilteredTable) AddRow(cells []*tview.TableCell) {
	row := ft.Table.GetRowCount()
	ft.SetRow(row, cells)
}

func (ft *FilteredTable) SetRow(row int, cells []*tview.TableCell) {
	for col, cell := range cells {
		if col < len(ft.Headers) {
			ft.Table.SetCell(row, col, cell)
		}
	}
	// Ensure we have enough capacity in OriginalRows
	for len(ft.OriginalRows) <= row {
		ft.OriginalRows = append(ft.OriginalRows, nil)
	}
	ft.OriginalRows[row] = cells
}

func (ft *FilteredTable) FilterTable(filter string) {
	// Clear existing rows (keep headers)
	for r := ft.Table.GetRowCount() - 1; r > 0; r-- {
		ft.Table.RemoveRow(r)
	}

	filter = strings.ToLower(filter)
	for _, row := range ft.OriginalRows {
		// Check if any cell in row matches filter (case-insensitive)
		match := false
		for _, cell := range row {
			if strings.Contains(strings.ToLower(cell.Text), filter) {
				match = true
				break
			}
		}

		if match || filter == "" {
			r := ft.Table.GetRowCount()
			for c, cell := range row {
				if c < len(ft.Headers) {
					// Clone the original cell to preserve all attributes
					newCell := tview.NewTableCell(cell.Text).
						SetStyle(cell.Style).
						SetSelectedStyle(cell.SelectedStyle).
						SetAlign(cell.Align)
					ft.Table.SetCell(r, c, newCell)
				}
			}
		}
	}
}

func (ft *FilteredTable) GetInputCapture(app *tview.Application, pages *tview.Pages) func(event *tcell.EventKey) *tcell.EventKey {
	return func(event *tcell.EventKey) *tcell.EventKey {
		if event.Rune() == '/' {
			// Show filter input for table content
			filterInput := tview.NewInputField().
				SetLabel("/").
				SetFieldWidth(30).
				SetChangedFunc(func(text string) {
					ft.FilterTable(text)
				})

			filterInput.SetDoneFunc(func(key tcell.Key) {
				if key == tcell.KeyEscape || key == tcell.KeyEnter {
					pages.RemovePage("table_filter")
					app.SetFocus(ft.Table)
				}
			})

			filterModal := tview.NewFlex().
				SetDirection(tview.FlexRow).
				AddItem(filterInput, 1, 0, true).
				AddItem(ft.Table, 0, 1, false)

			pages.AddPage("table_filter", filterModal, true, true)
			app.SetFocus(filterInput)
			return nil
		}
		return event
	}
}
