package widgets

import (
	"fmt"
	"strings"

	"github.com/alecthomas/chroma/quick"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type QueryView struct {
	*tview.TextView
	selectionStart int
	selectionEnd   int
	selecting      bool
}

func NewQueryView() *QueryView {
	qv := &QueryView{
		TextView: tview.NewTextView().
			SetDynamicColors(true).
			SetWrap(true).
			SetWordWrap(true),
	}
	qv.SetBorder(true)
	qv.SetTitle("Normalized Query")
	qv.SetRegions(true) // Enable text regions for selection

	// Handle mouse events for text selection
	qv.SetMouseCapture(func(action tview.MouseAction, event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse) {
		switch action {
		case tview.MouseLeftDown:
			x, y := event.Position()
			_, _, _, _, offset := qv.GetInnerRect()
			x -= offset
			y -= offset
			qv.selectionStart = qv.GetIndexFromPoint(x, y)
			qv.selectionEnd = qv.selectionStart
			qv.selecting = true
			qv.Highlight()
			return action, event
			
		case tview.MouseMove:
			if qv.selecting {
				x, y := event.Position()
				_, _, _, _, offset := qv.GetInnerRect()
				x -= offset
				y -= offset
				qv.selectionEnd = qv.GetIndexFromPoint(x, y)
				qv.Highlight()
			}
			return action, event
			
		case tview.MouseLeftUp:
			qv.selecting = false
			return action, event
			
		default:
			return action, event
		}
	})

	// Handle keyboard events for copying
	qv.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyCtrlC || event.Key() == tcell.KeyCtrlInsert {
			if qv.selectionStart != qv.selectionEnd {
				start, end := qv.selectionStart, qv.selectionEnd
				if start > end {
					start, end = end, start
				}
				text := qv.GetText(true)
				if end > len(text) {
					end = len(text)
				}
				selection := text[start:end]
				// Copy to clipboard
				qv.copyToClipboard(selection)
			}
			return nil
		}
		return event
	})

	return qv
}

func (qv *QueryView) copyToClipboard(text string) {
	// This is a placeholder - you'll need to implement actual clipboard handling
	// For Windows, you might use:
	// err := clipboard.WriteAll(text)
	// For now, we'll just print it for debugging
	fmt.Printf("Copied to clipboard: %s\n", text)
}

func (qv *QueryView) Highlight() {
	if qv.selectionStart == qv.selectionEnd {
		qv.HighlightRegions(nil)
		return
	}

	start, end := qv.selectionStart, qv.selectionEnd
	if start > end {
		start, end = end, start
	}
	
	qv.HighlightRegions([]string{
		fmt.Sprintf("%d,%d", start, end),
	})
}

func (qv *QueryView) SetSQL(sql string) {
	qv.Clear()

	// Use chroma to highlight SQL with ANSI color codes
	var highlighted strings.Builder
	err := quick.Highlight(&highlighted, sql, "sql", "terminal256", "monokai")
	if err != nil {
		// Fallback to plain text if highlighting fails
		fmt.Fprint(qv, sql)
		return
	}

	// Convert chroma ANSI colors to tcell colors
	formatted := ansiToTcell(highlighted.String())
	fmt.Fprint(qv, formatted)
}

// ansiToTcell converts ANSI color codes to tview color tags
func ansiToTcell(text string) string {
	// Map of ANSI color codes to tview color names
	colorMap := map[string]string{
		"\x1b[38;5;249m": "[gray]",   // Comments
		"\x1b[38;5;204m": "[red]",    // Strings
		"\x1b[38;5;81m":  "[blue]",   // Keywords
		"\x1b[38;5;118m": "[green]",  // Functions
		"\x1b[38;5;208m": "[orange]", // Numbers
		"\x1b[38;5;231m": "[white]",  // Operators
		"\x1b[0m":        "[-]",      // Reset
	}

	// Replace ANSI codes with tview tags
	for ansi, tag := range colorMap {
		text = strings.ReplaceAll(text, ansi, tag)
	}
	return text
}
