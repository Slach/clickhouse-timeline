package widgets

import (
	"fmt"
	"strings"

	"github.com/alecthomas/chroma/quick"
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
	return qv
}

func (qv *QueryView) formatSQL(sql string) string {
	// Create a replacer with all our patterns
	replacer := strings.NewReplacer(
		"insert into ", "INSERT INTO\n",
		"select ", "SELECT\n",
		"from ", "\nFROM\n",
		"where ", "\nWHERE\n",
		"group by ", "\nGROUP BY\n",
		"order by ", "\nORDER BY\n",
		"limit ", "\nLIMIT\n",
		"having ", "\nHAVING\n",
		"join ", "JOIN\n",
		"union ", "\nUNION ",
	)

	// Apply the replacements to the lowercase version
	formatted := replacer.Replace(sql)

	// Return the formatted SQL with original case preserved for keywords
	return formatted
}
func (qv *QueryView) SetSQL(sql string) {
	qv.Clear()

	formattedSQL := qv.formatSQL(sql)

	// Use chroma to highlight SQL with ANSI color codes
	var highlighted strings.Builder
	err := quick.Highlight(&highlighted, formattedSQL, "sql", "terminal256", "monokai")
	if err != nil {
		// Fallback to plain text if highlighting fails
		fmt.Fprint(qv, formattedSQL)
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
		// Basic colors
		"\x1b[38;5;15m":  "[white]",    // Bright white
		"\x1b[38;5;231m": "[white]",    // White
		"\x1b[38;5;249m": "[gray]",     // Gray (comments)
		"\x1b[38;5;244m": "[darkgray]", // Dark gray

		// Reds/Pinks
		"\x1b[38;5;197m": "[pink]",    // Bright pink
		"\x1b[38;5;204m": "[red]",     // Soft red (strings)
		"\x1b[38;5;160m": "[darkred]", // Dark red
		"\x1b[38;5;196m": "[red]",     // Bright red

		// Blues
		"\x1b[38;5;81m": "[blue]",     // Light blue (keywords)
		"\x1b[38;5;39m": "[blue]",     // Bright blue
		"\x1b[38;5;27m": "[darkblue]", // Dark blue

		// Greens
		"\x1b[38;5;118m": "[green]",     // Bright green (functions)
		"\x1b[38;5;46m":  "[green]",     // Neon green
		"\x1b[38;5;34m":  "[darkgreen]", // Dark green

		// Yellows/Oranges
		"\x1b[38;5;208m": "[orange]", // Orange (numbers)
		"\x1b[38;5;226m": "[yellow]", // Bright yellow
		"\x1b[38;5;220m": "[yellow]", // Gold

		// Purples
		"\x1b[38;5;129m": "[purple]", // Purple
		"\x1b[38;5;93m":  "[purple]", // Dark purple

		// Special
		"\x1b[38;5;45m":  "[cyan]",    // Cyan
		"\x1b[38;5;51m":  "[cyan]",    // Bright cyan
		"\x1b[38;5;201m": "[magenta]", // Magenta

		// Reset
		"\x1b[0m": "[-]", // Reset
	}

	// Replace ANSI codes with tview tags
	for ansi, tag := range colorMap {
		text = strings.ReplaceAll(text, ansi, tag)
	}
	return text
}
