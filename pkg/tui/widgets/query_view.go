package widgets

import (
	"fmt"
	"regexp"
	"strconv"
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
			SetWordWrap(true).
			SetSelectable(true),
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
		{regexp.MustCompile(`(?i)\bhaving\b`), "\nHAVING\n"},
		{regexp.MustCompile(`(?i)\blimit\b`), "\nLIMIT"},
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
	// Replace SGR sequences like "\x1b[38;5;186m" or bare "[38;5;186m"
	re := regexp.MustCompile(`\x1b\[((?:\d+;)*\d+)m|\[((?:\d+;)*\d+)m`)
	out := re.ReplaceAllStringFunc(text, func(m string) string {
		// Extract the numeric part, whether the match included the ESC or not.
		var codesStr string
		if strings.HasPrefix(m, "\x1b[") {
			codesStr = strings.TrimSuffix(strings.TrimPrefix(m, "\x1b["), "m")
		} else {
			codesStr = strings.TrimSuffix(strings.TrimPrefix(m, "["), "m")
		}

		parts := strings.Split(codesStr, ";")

		// If reset or default foreground, return reset tag
		for _, p := range parts {
			if p == "0" || p == "39" {
				return "[-]"
			}
		}

		// Handle truecolor sequences: 38;2;<r>;<g>;<b>
		for i := 0; i+4 < len(parts); i++ {
			if parts[i] == "38" && parts[i+1] == "2" {
				r, _ := strconv.Atoi(parts[i+2])
				g, _ := strconv.Atoi(parts[i+3])
				b, _ := strconv.Atoi(parts[i+4])
				return fmt.Sprintf("[#%02x%02x%02x]", r, g, b)
			}
		}

		// Handle 256-color sequences: 38;5;<n>
		for i := 0; i+2 < len(parts); i++ {
			if parts[i] == "38" && parts[i+1] == "5" {
				n, _ := strconv.Atoi(parts[i+2])
				return map256ToTag(n)
			}
		}

		// Handle basic SGR color codes (30-37, 90-97)
		if len(parts) == 1 {
			switch parts[0] {
			case "30":
				return "[black]"
			case "31":
				return "[red]"
			case "32":
				return "[green]"
			case "33":
				return "[yellow]"
			case "34":
				return "[blue]"
			case "35":
				return "[magenta]"
			case "36":
				return "[cyan]"
			case "37":
				return "[white]"
			case "90":
				return "[gray]"
			}
		}

		// Unknown sequence -> strip it
		return ""
	})

	return out
}

// map256ToTag maps a 256-color palette index to a reasonable tview tag.
func map256ToTag(n int) string {
	switch {
	case n >= 196:
		return "[red]"
	case n >= 160:
		return "[darkred]"
	case n >= 129 && n < 160:
		return "[purple]"
	case n >= 93 && n < 129:
		return "[purple]"
	case n >= 81 && n < 93:
		return "[blue]"
	case n >= 46 && n < 81:
		return "[green]"
	case n >= 34 && n < 46:
		return "[darkgreen]"
	case n >= 226 && n < 231:
		return "[yellow]"
	case n >= 220 && n < 226:
		return "[yellow]"
	case n >= 208 && n < 220:
		return "[orange]"
	case n >= 118 && n < 160:
		return "[green]"
	case n >= 249 && n <= 255:
		return "[gray]"
	default:
		return "[white]"
	}
}
