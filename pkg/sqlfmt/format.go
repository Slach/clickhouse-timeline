package sqlfmt

import (
	"regexp"
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/ajitpratap0/GoSQLX/pkg/formatter"
	"github.com/ajitpratap0/GoSQLX/pkg/gosqlx"
	"github.com/ajitpratap0/GoSQLX/pkg/sql/ast"
	"github.com/ajitpratap0/GoSQLX/pkg/sql/keywords"
	"github.com/alecthomas/chroma/v2/quick"
	"github.com/rs/zerolog/log"
)

// FormatSQL formats SQL using GoSQLX with ClickHouse dialect, falls back to regexp.
func FormatSQL(sql string) string {
	parsed, err := gosqlx.ParseWithDialect(sql, keywords.DialectClickHouse)
	if err != nil {
		log.Debug().Err(err).Msg("gosqlx.ParseWithDialect failed, falling back to regexp formatter")
		return FormatSQLRegexp(sql)
	}
	opts := ast.ReadableStyle()
	opts.IndentWidth = 2
	opts.KeywordCase = ast.KeywordUpper
	return formatter.FormatAST(parsed, opts)
}

// FormatSQLRegexp is a fallback formatter using regexp when GoSQLX fails.
func FormatSQLRegexp(sql string) string {
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

// HighlightSQL applies chroma syntax highlighting and converts to lipgloss styled text.
func HighlightSQL(sql string) string {
	var highlighted strings.Builder
	err := quick.Highlight(&highlighted, sql, "sql", "terminal256", "monokai")
	if err != nil {
		return ApplyBasicHighlighting(sql)
	}
	return AnsiToLipgloss(highlighted.String())
}

// FormatAndHighlightSQL formats SQL and then applies syntax highlighting.
func FormatAndHighlightSQL(sql string) string {
	return HighlightSQL(FormatSQL(sql))
}

// AnsiToLipgloss strips ANSI codes and applies basic SQL highlighting via lipgloss.
func AnsiToLipgloss(text string) string {
	re := regexp.MustCompile(`\x1b\[[\d;]*m`)
	stripped := re.ReplaceAllString(text, "")
	return ApplyBasicHighlighting(stripped)
}

// ApplyBasicHighlighting applies basic SQL syntax highlighting using lipgloss.
func ApplyBasicHighlighting(text string) string {
	keywordStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("105")).Bold(true)
	stringStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("186"))
	commentStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Italic(true)

	sqlKeywords := []string{
		"SELECT", "FROM", "WHERE", "INSERT", "INTO", "UPDATE", "DELETE",
		"JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON", "GROUP BY",
		"ORDER BY", "LIMIT", "OFFSET", "HAVING", "UNION", "AS", "AND", "OR",
		"NOT", "IN", "EXISTS", "BETWEEN", "LIKE", "IS", "NULL", "COUNT",
		"SUM", "AVG", "MIN", "MAX", "DISTINCT", "CREATE", "TABLE", "ALTER",
		"DROP", "INDEX", "VIEW", "DATABASE", "SCHEMA", "SHOW", "DESCRIBE", "WITH",
	}

	result := text
	for _, keyword := range sqlKeywords {
		pattern := regexp.MustCompile(`(?i)\b` + keyword + `\b`)
		result = pattern.ReplaceAllStringFunc(result, func(match string) string {
			return keywordStyle.Render(match)
		})
	}

	stringPattern := regexp.MustCompile(`'[^']*'|"[^"]*"`)
	result = stringPattern.ReplaceAllStringFunc(result, func(match string) string {
		return stringStyle.Render(match)
	})

	commentPattern := regexp.MustCompile(`--[^\n]*`)
	result = commentPattern.ReplaceAllStringFunc(result, func(match string) string {
		return commentStyle.Render(match)
	})

	return result
}
