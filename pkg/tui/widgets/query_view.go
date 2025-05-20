package widgets

import (
	"fmt"
	"regexp"
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
	// Define our patterns with case-insensitive regex
	patterns := []struct {
		regex *regexp.Regexp
		repl  string
	}{
		{regexp.MustCompile(`(?i)\binsert into\b`), "\nINSERT INTO\n"},
		{regexp.MustCompile(`(?i)\bselect\b`), "\nSELECT\n"},
		{regexp.MustCompile(`(?i)\bfrom\b`), "\nFROM\n"},
		{regexp.MustCompile(`(?i)\bwhere\b`), "\nWHERE\n"},
		{regexp.MustCompile(`(?i)\bgroup by\b`), "\nGROUP BY\n"},
		{regexp.MustCompile(`(?i)\border by\b`), "\nORDER BY\n"},
		{regexp.MustCompile(`(?i)\blimit\b`), "\nLIMIT\n"},
		{regexp.MustCompile(`(?i)\bhaving\b`), "\nHAVING\n"},
		{regexp.MustCompile(`(?i)\bjoin\b`), "JOIN\n"},
		{regexp.MustCompile(`(?i)\bunion\b`), "\nUNION "},
	}

	// Apply each replacement pattern
	formatted := sql
	for _, p := range patterns {
		formatted = p.regex.ReplaceAllString(formatted, p.repl)
	}

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
