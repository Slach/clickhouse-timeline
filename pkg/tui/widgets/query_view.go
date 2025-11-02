package widgets

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/alecthomas/chroma/quick"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// QueryView is a bubbletea model for displaying SQL queries with syntax highlighting
type QueryView struct {
	viewport viewport.Model
	title    string
	content  string
	width    int
	height   int
}

// NewQueryView creates a new query view
func NewQueryView(title string, width, height int) QueryView {
	vp := viewport.New(width, height-3) // Reserve space for title and border
	vp.Style = lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("62"))

	return QueryView{
		viewport: vp,
		title:    title,
		width:    width,
		height:   height,
	}
}

// SetSize updates the dimensions
func (m *QueryView) SetSize(width, height int) {
	m.width = width
	m.height = height
	m.viewport.Width = width
	m.viewport.Height = height - 3
}

// SetSQL sets the SQL query to display with syntax highlighting
func (m *QueryView) SetSQL(sql string) {
	formatted := m.formatSQL(sql)

	// Use chroma to highlight SQL with ANSI color codes
	var highlighted strings.Builder
	err := quick.Highlight(&highlighted, formatted, "sql", "terminal256", "monokai")
	if err != nil {
		// Fallback to plain text if highlighting fails
		m.content = formatted
		m.viewport.SetContent(formatted)
		return
	}

	// Convert ANSI codes to plain text (lipgloss will handle coloring differently)
	// For now, we'll strip ANSI codes and apply basic SQL highlighting manually
	content := m.ansiToLipgloss(highlighted.String())
	m.content = content
	m.viewport.SetContent(content)
}

// Init implements tea.Model
func (m QueryView) Init() tea.Cmd {
	return nil
}

// Update implements tea.Model
func (m QueryView) Update(msg tea.Msg) (QueryView, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return m, nil

	case tea.KeyMsg:
		// Delegate scrolling to viewport
		m.viewport, cmd = m.viewport.Update(msg)
		return m, cmd
	}

	return m, nil
}

// View implements tea.Model
func (m QueryView) View() string {
	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("white")).
		Padding(0, 1)

	title := titleStyle.Render(m.title)

	borderStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("62")).
		Padding(1)

	return lipgloss.JoinVertical(lipgloss.Left, title, borderStyle.Render(m.viewport.View()))
}

// formatSQL formats SQL with newlines for readability
func (m *QueryView) formatSQL(sql string) string {
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

	formatted := sql
	for _, p := range patterns {
		formatted = p.regex.ReplaceAllString(formatted, p.repl)
	}

	return formatted
}

// ansiToLipgloss converts ANSI color codes to lipgloss styled text
func (m *QueryView) ansiToLipgloss(text string) string {
	// For now, we'll strip ANSI codes and return plain text
	// Lipgloss requires a different approach to styling (applying styles to strings)
	// Full syntax highlighting would require parsing and applying styles per token

	// Strip ANSI codes
	re := regexp.MustCompile(`\x1b\[[\d;]*m`)
	stripped := re.ReplaceAllString(text, "")

	// Apply basic SQL keyword highlighting manually
	return m.applyBasicHighlighting(stripped)
}

// applyBasicHighlighting applies basic SQL syntax highlighting using lipgloss
func (m *QueryView) applyBasicHighlighting(text string) string {
	// Define styles
	keywordStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("105")).Bold(true)
	stringStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("186"))
	commentStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Italic(true)

	// SQL keywords to highlight
	keywords := []string{
		"SELECT", "FROM", "WHERE", "INSERT", "INTO", "UPDATE", "DELETE",
		"JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON", "GROUP BY",
		"ORDER BY", "LIMIT", "OFFSET", "HAVING", "UNION", "AS", "AND", "OR",
		"NOT", "IN", "EXISTS", "BETWEEN", "LIKE", "IS", "NULL", "COUNT",
		"SUM", "AVG", "MIN", "MAX", "DISTINCT", "CREATE", "TABLE", "ALTER",
		"DROP", "INDEX", "VIEW", "DATABASE", "SCHEMA",
	}

	result := text

	// Highlight keywords (case-insensitive)
	for _, keyword := range keywords {
		// Use word boundaries to match whole words
		pattern := regexp.MustCompile(`(?i)\b` + keyword + `\b`)
		result = pattern.ReplaceAllStringFunc(result, func(match string) string {
			return keywordStyle.Render(match)
		})
	}

	// Highlight strings in quotes
	stringPattern := regexp.MustCompile(`'[^']*'|"[^"]*"`)
	result = stringPattern.ReplaceAllStringFunc(result, func(match string) string {
		return stringStyle.Render(match)
	})

	// Highlight comments
	commentPattern := regexp.MustCompile(`--[^\n]*`)
	result = commentPattern.ReplaceAllStringFunc(result, func(match string) string {
		return commentStyle.Render(match)
	})

	return result
}

// map256ToColor maps a 256-color palette index to a lipgloss color
func map256ToColor(n int) lipgloss.Color {
	switch {
	case n >= 196:
		return lipgloss.Color("196") // red
	case n >= 160:
		return lipgloss.Color("160") // darkred
	case n >= 129 && n < 160:
		return lipgloss.Color("135") // purple
	case n >= 93 && n < 129:
		return lipgloss.Color("99") // purple
	case n >= 81 && n < 93:
		return lipgloss.Color("75") // blue
	case n >= 46 && n < 81:
		return lipgloss.Color("70") // green
	case n >= 34 && n < 46:
		return lipgloss.Color("28") // darkgreen
	case n >= 226 && n < 231:
		return lipgloss.Color("226") // yellow
	case n >= 220 && n < 226:
		return lipgloss.Color("220") // yellow
	case n >= 208 && n < 220:
		return lipgloss.Color("214") // orange
	case n >= 118 && n < 160:
		return lipgloss.Color("118") // green
	case n >= 249 && n <= 255:
		return lipgloss.Color("250") // gray
	default:
		return lipgloss.Color("255") // white
	}
}

// Helper function for ANSI code parsing (used if needed)
func parseANSIColor(codesStr string) string {
	parts := strings.Split(codesStr, ";")

	// If reset or default foreground
	for _, p := range parts {
		if p == "0" || p == "39" {
			return ""
		}
	}

	// Handle truecolor sequences: 38;2;<r>;<g>;<b>
	for i := 0; i+4 < len(parts); i++ {
		if parts[i] == "38" && parts[i+1] == "2" {
			r, _ := strconv.Atoi(parts[i+2])
			g, _ := strconv.Atoi(parts[i+3])
			b, _ := strconv.Atoi(parts[i+4])
			return fmt.Sprintf("#%02x%02x%02x", r, g, b)
		}
	}

	// Handle 256-color sequences: 38;5;<n>
	for i := 0; i+2 < len(parts); i++ {
		if parts[i] == "38" && parts[i+1] == "5" {
			n, _ := strconv.Atoi(parts[i+2])
			return string(map256ToColor(n))
		}
	}

	// Handle basic SGR color codes
	if len(parts) == 1 {
		switch parts[0] {
		case "30":
			return "0" // black
		case "31":
			return "196" // red
		case "32":
			return "46" // green
		case "33":
			return "226" // yellow
		case "34":
			return "21" // blue
		case "35":
			return "201" // magenta
		case "36":
			return "51" // cyan
		case "37":
			return "255" // white
		case "90":
			return "240" // gray
		}
	}

	return ""
}
