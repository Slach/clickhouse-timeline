package widgets

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/alecthomas/chroma/v2/quick"
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
func NewQueryView(title string, width, height int) *QueryView {
	vp := viewport.New(viewport.WithWidth(width), viewport.WithHeight(height-3)) // Reserve space for title and border
	vp.Style = lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("62"))

	return &QueryView{
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
	m.viewport.SetWidth(width)
	m.viewport.SetHeight(height - 3)
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
func (m *QueryView) Init() tea.Cmd {
	return nil
}

// Update implements tea.Model
func (m *QueryView) Update(msg tea.Msg) (*QueryView, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return m, nil

	case tea.KeyPressMsg:
		// Delegate scrolling to viewport
		m.viewport, cmd = m.viewport.Update(msg)
		return m, cmd
	}

	return m, nil
}

// View implements tea.Model
func (m *QueryView) View() string {
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

