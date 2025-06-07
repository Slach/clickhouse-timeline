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
	// Store current selection to restore later
	currentRow, currentCol := ft.Table.GetSelection()
	
	// Clear all rows at once by setting row count to 1 (keep headers)
	ft.Table.Clear()
	
	// Re-add headers
	for col, header := range ft.Headers {
		ft.Table.SetCell(0, col,
			tview.NewTableCell(header).
				SetTextColor(tcell.ColorYellow).
				SetAlign(tview.AlignCenter),
		)
	}

	if filter == "" {
		// No filter - restore all original rows directly
		for rowIdx, row := range ft.OriginalRows {
			if row != nil {
				for col, cell := range row {
					if col < len(ft.Headers) {
						ft.Table.SetCell(rowIdx, col, cell)
					}
				}
			}
		}
	} else {
		// Apply filter
		filter = strings.ToLower(filter)
		displayRow := 1 // Start after header
		
		for _, row := range ft.OriginalRows {
			if row == nil {
				continue
			}
			
			// Check if any cell in row matches filter (case-insensitive)
			match := false
			for _, cell := range row {
				if cell != nil && strings.Contains(strings.ToLower(cell.Text), filter) {
					match = true
					break
				}
			}

			if match {
				for col, cell := range row {
					if col < len(ft.Headers) && cell != nil {
						// Reuse the original cell directly - no cloning needed
						ft.Table.SetCell(displayRow, col, cell)
					}
				}
				displayRow++
			}
		}
	}
	
	// Restore selection if possible
	if currentRow > 0 && currentRow < ft.Table.GetRowCount() {
		ft.Table.Select(currentRow, currentCol)
	} else if ft.Table.GetRowCount() > 1 {
		ft.Table.Select(1, currentCol) // Select first data row
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
