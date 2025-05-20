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

	// Enable full mouse support for text selection
	qv.SetMouseCapture(func(action tview.MouseAction, event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse) {
		switch action {
		case tview.MouseLeftDown, tview.MouseLeftDoubleClick, tview.MouseMove:
			// Let TextView handle selection and dragging
			return action, event
		default:
			return action, event
		}
	})
	
	// Enable mouse movement tracking for drag selection
	qv.SetChangedFunc(func() {
		// Force redraw when text changes to keep selection visible
		qv.ScrollToHighlight()
	})

	return qv
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
