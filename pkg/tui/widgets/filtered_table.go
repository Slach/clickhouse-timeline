package widgets

import (
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"strings"
	"unicode/utf8"
)

type FilteredTable struct {
	Table        *tview.Table
	Title        string
	Headers      []string
	OriginalRows [][]*tview.TableCell
	maxCellWidth int // Maximum width for cell content to prevent excessive Unicode processing
}

func NewFilteredTable() *FilteredTable {
	return &FilteredTable{
		Table: tview.NewTable().
			SetBorders(false).
			SetSelectable(true, true),
		maxCellWidth: 100, // Default max width to limit Unicode processing
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
	// Optimize cells before storing/displaying
	optimizedCells := make([]*tview.TableCell, len(cells))
	for i, cell := range cells {
		if cell != nil {
			optimizedCells[i] = ft.optimizeCell(cell)
		}
	}
	
	for col, cell := range optimizedCells {
		if col < len(ft.Headers) && cell != nil {
			ft.Table.SetCell(row, col, cell)
		}
	}
	
	// Ensure we have enough capacity in OriginalRows
	for len(ft.OriginalRows) <= row {
		ft.OriginalRows = append(ft.OriginalRows, nil)
	}
	ft.OriginalRows[row] = optimizedCells
}

func (ft *FilteredTable) FilterTable(filter string) {
	// Store current selection to restore later
	currentRow, currentCol := ft.Table.GetSelection()
	
	// Clear all rows at once by setting row count to 1 (keep headers)
	ft.Table.Clear()
	
	// Re-add headers efficiently
	ft.addHeadersOptimized()

	if filter == "" {
		// No filter - restore all original rows directly with batch operation
		ft.restoreAllRows()
	} else {
		// Apply filter with optimized matching
		ft.applyFilterOptimized(filter)
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

// optimizeCell truncates text and removes complex markup to reduce Unicode processing overhead
func (ft *FilteredTable) optimizeCell(cell *tview.TableCell) *tview.TableCell {
	if cell == nil {
		return cell
	}
	
	text := cell.Text
	if text == "" {
		return cell
	}
	
	// Truncate text to prevent excessive Unicode processing
	if utf8.RuneCountInString(text) > ft.maxCellWidth {
		runes := []rune(text)
		if len(runes) > ft.maxCellWidth {
			text = string(runes[:ft.maxCellWidth-3]) + "..."
		}
	}
	
	// Create new cell with optimized text, preserving other properties
	newCell := tview.NewTableCell(text).
		SetTextColor(cell.Color).
		SetBackgroundColor(cell.BackgroundColor).
		SetAlign(cell.Align).
		SetSelectable(cell.Selectable).
		SetReference(cell.Reference).
		SetExpansion(cell.Expansion)
	
	return newCell
}

// addHeadersOptimized adds headers with minimal processing
func (ft *FilteredTable) addHeadersOptimized() {
	for col, header := range ft.Headers {
		// Use simple header without complex markup
		headerCell := tview.NewTableCell(header).
			SetTextColor(tcell.ColorYellow).
			SetAlign(tview.AlignCenter)
		ft.Table.SetCell(0, col, headerCell)
	}
}

// restoreAllRows efficiently restores all rows without filtering
func (ft *FilteredTable) restoreAllRows() {
	for rowIdx, row := range ft.OriginalRows {
		if row != nil {
			for col, cell := range row {
				if col < len(ft.Headers) && cell != nil {
					ft.Table.SetCell(rowIdx, col, cell)
				}
			}
		}
	}
}

// applyFilterOptimized applies filtering with optimized text matching
func (ft *FilteredTable) applyFilterOptimized(filter string) {
	filter = strings.ToLower(filter)
	displayRow := 1 // Start after header
	
	for _, row := range ft.OriginalRows {
		if row == nil {
			continue
		}
		
		// Optimized matching - check only visible columns and use simple string contains
		match := false
		for col, cell := range row {
			if col >= len(ft.Headers) {
				break // Don't check columns beyond headers
			}
			if cell != nil {
				// Use simple case-insensitive matching without complex Unicode processing
				cellText := strings.ToLower(cell.Text)
				if strings.Contains(cellText, filter) {
					match = true
					break
				}
			}
		}

		if match {
			for col, cell := range row {
				if col < len(ft.Headers) && cell != nil {
					ft.Table.SetCell(displayRow, col, cell)
				}
			}
			displayRow++
		}
	}
}

// SetMaxCellWidth allows configuring the maximum cell width to control Unicode processing overhead
func (ft *FilteredTable) SetMaxCellWidth(width int) {
	ft.maxCellWidth = width
}
