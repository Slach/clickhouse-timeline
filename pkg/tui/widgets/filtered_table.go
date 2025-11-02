package widgets

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/evertras/bubble-table/table"
)

// FilteredTable is a bubbletea model for a filterable table
type FilteredTable struct {
	table        table.Model
	filterInput  textinput.Model
	filtering    bool
	title        string
	allRows      []table.Row
	headers      []string
	width        int
	height       int
	onSelect     func(selectedRow table.Row)
	maxCellWidth int
	pageSize     int
}

// NewFilteredTable creates a new filtered table
func NewFilteredTable(title string, headers []string, width, height int) FilteredTable {
	// Create columns with equal widths
	// Account for table borders: 2 for left/right borders + (n-1) for column separators
	// Also subtract some padding for safety
	borderOverhead := 2 + (len(headers) - 1) + 4
	availableWidth := width - borderOverhead
	if availableWidth < len(headers)*10 {
		availableWidth = len(headers) * 10
	}

	columns := make([]table.Column, len(headers))
	columnWidth := availableWidth / len(headers)
	if columnWidth < 10 {
		columnWidth = 10
	}

	for i, header := range headers {
		columns[i] = table.NewColumn(header, header, columnWidth).
			WithStyle(lipgloss.NewStyle().Align(lipgloss.Left))
	}

	return createFilteredTableBubble(title, headers, columns, width, height)
}

// NewFilteredTableBubbleWithWidths creates a new filtered table with custom column widths
func NewFilteredTableBubbleWithWidths(title string, headers []string, widths []int, width, height int) FilteredTable {
	// Create columns with custom widths
	columns := make([]table.Column, len(headers))
	for i, header := range headers {
		columnWidth := widths[i]
		if columnWidth < 5 {
			columnWidth = 5
		}
		columns[i] = table.NewColumn(header, header, columnWidth).
			WithStyle(lipgloss.NewStyle().Align(lipgloss.Left))
	}

	return createFilteredTableBubble(title, headers, columns, width, height)
}

// createFilteredTableBubble is a helper that creates the table with given columns
func createFilteredTableBubble(title string, headers []string, columns []table.Column, width, height int) FilteredTable {

	// Calculate visible rows for the table viewport
	// Account for: title (1), filter input when active (2), help text (1), borders (2)
	visibleRows := height - 6
	if visibleRows < 5 {
		visibleRows = 5
	}

	// Create table with proper viewport size for scrolling
	t := table.New(columns).
		WithPageSize(visibleRows).
		WithMaxTotalWidth(width).
		Focused(true).
		WithTargetWidth(width).
		WithHorizontalFreezeColumnCount(1).
		SelectableRows(true).
		Border(table.Border{
			Top:    "─",
			Left:   "│",
			Right:  "│",
			Bottom: "─",

			TopRight:    "╮",
			TopLeft:     "╭",
			BottomRight: "╯",
			BottomLeft:  "╰",

			TopJunction:    "┬",
			LeftJunction:   "├",
			RightJunction:  "┤",
			BottomJunction: "┴",
			InnerJunction:  "┼",

			InnerDivider: "│",
		})

	// Create filter input
	ti := textinput.New()
	ti.Placeholder = "Type to filter..."
	ti.Prompt = "/"
	ti.CharLimit = 100

	return FilteredTable{
		table:        t,
		filterInput:  ti,
		filtering:    false,
		title:        title,
		allRows:      []table.Row{},
		headers:      headers,
		width:        width,
		height:       height,
		maxCellWidth: 5000, // Allow long SQL queries
	}
}

// SetOnSelect sets the callback when a row is selected
func (m *FilteredTable) SetOnSelect(callback func(selectedRow table.Row)) {
	m.onSelect = callback
}

// SetSize updates the dimensions
func (m *FilteredTable) SetSize(width, height int) {
	m.width = width
	m.height = height

	// Calculate visible rows for the table viewport
	visibleRows := height - 6
	if visibleRows < 5 {
		visibleRows = 5
	}

	m.table = m.table.WithTargetWidth(width).WithPageSize(visibleRows)
}

// AddRow adds a row to the table
func (m *FilteredTable) AddRow(rowData map[string]string) {
	// Optimize cell content
	optimized := make(table.RowData)
	for k, v := range rowData {
		optimized[k] = m.optimizeText(v)
	}

	row := table.NewRow(optimized)
	m.allRows = append(m.allRows, row)
	m.table = m.table.WithRows(m.allRows)
}

// SetRows sets all rows at once (more efficient than AddRow for bulk operations)
func (m *FilteredTable) SetRows(rows []table.Row) {
	m.allRows = rows
	m.table = m.table.WithRows(rows)
}

// ClearRows clears all rows
func (m *FilteredTable) ClearRows() {
	m.allRows = []table.Row{}
	m.table = m.table.WithRows([]table.Row{})
}

// Init implements tea.Model
func (m FilteredTable) Init() tea.Cmd {
	return nil
}

// Update implements tea.Model
func (m FilteredTable) Update(msg tea.Msg) (FilteredTable, tea.Cmd) {
	var cmd tea.Cmd
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return m, nil

	case tea.KeyMsg:
		if m.filtering {
			// Check for special keys first before passing to textinput
			switch msg.Type {
			case tea.KeyEsc:
				// Cancel filtering
				m.filtering = false
				m.filterInput.SetValue("")
				m.filterInput.Blur()
				m.applyFilter("")
				// Refocus table
				m.table = m.table.Focused(true)
				return m, nil

			case tea.KeyEnter:
				// Apply filter and exit filter mode
				m.filtering = false
				m.filterInput.Blur()
				// Refocus table so user can navigate
				m.table = m.table.Focused(true)
				return m, nil

			default:
				// Update filter input
				m.filterInput, cmd = m.filterInput.Update(msg)
				cmds = append(cmds, cmd)
				// Apply filter as user types
				m.applyFilter(m.filterInput.Value())
				return m, tea.Batch(cmds...)
			}
		} else {
			// Normal table navigation
			switch msg.String() {
			case "/":
				// Enter filter mode (unfocus table first)
				m.filtering = true
				m.table = m.table.Focused(false)
				m.filterInput.Focus()
				return m, nil

			case "enter":
				// Select current row
				if m.onSelect != nil {
					selectedRow := m.table.HighlightedRow()
					if selectedRow.Data != nil {
						m.onSelect(selectedRow)
					}
				}
				return m, nil

			case "esc", "q":
				// Propagate up (will be handled by parent)
				return m, nil

			default:
				// Delegate to table
				m.table, cmd = m.table.Update(msg)
				cmds = append(cmds, cmd)
			}
		}
	}

	return m, tea.Batch(cmds...)
}

// View implements tea.Model
func (m FilteredTable) View() string {
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("white"))
	title := titleStyle.Render(m.title)

	if m.filterInput.Value() != "" {
		filterStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("cyan"))
		title += " " + filterStyle.Render("/"+m.filterInput.Value())
	}

	if m.filtering {
		// Show filter input
		filterView := lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("62")).
			Padding(0, 1).
			Width(m.width - 4).
			Render(m.filterInput.View())

		return lipgloss.JoinVertical(lipgloss.Left, title, filterView, m.table.View())
	}

	// Add help text footer with pagination info
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))

	// Get current page info from table
	visibleRows := len(m.allRows)
	if m.filterInput.Value() != "" {
		// Count filtered rows
		filterLower := strings.ToLower(m.filterInput.Value())
		visibleRows = 0
		for _, row := range m.allRows {
			match := false
			for _, header := range m.headers {
				if cellValue, ok := row.Data[header].(string); ok {
					if strings.Contains(strings.ToLower(cellValue), filterLower) {
						match = true
						break
					}
				}
			}
			if match {
				visibleRows++
			}
		}
	}

	helpText := helpStyle.Render("↑↓: Scroll | PgUp/PgDn: Fast scroll | /: Filter | Enter: Select | ESC: Back")

	// Add row count info
	infoStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	rowInfo := infoStyle.Render(fmt.Sprintf("  [Total rows: %d]", visibleRows))

	footer := helpText + rowInfo

	return lipgloss.JoinVertical(lipgloss.Left, title, m.table.View(), footer)
}

// applyFilter filters the table based on the filter text
func (m *FilteredTable) applyFilter(filter string) {
	if filter == "" {
		// Reset to all rows
		m.table = m.table.WithRows(m.allRows)
		return
	}

	// Filter rows
	filterLower := strings.ToLower(filter)
	var filtered []table.Row
	for _, row := range m.allRows {
		// Check if any cell contains the filter text
		match := false
		for _, header := range m.headers {
			if cellValue, ok := row.Data[header].(string); ok {
				if strings.Contains(strings.ToLower(cellValue), filterLower) {
					match = true
					break
				}
			}
		}

		if match {
			filtered = append(filtered, row)
		}
	}

	m.table = m.table.WithRows(filtered)
}

// optimizeText truncates text to prevent excessive processing
func (m *FilteredTable) optimizeText(text string) string {
	if len(text) > m.maxCellWidth {
		return text[:m.maxCellWidth-3] + "..."
	}
	return text
}

// HighlightedRow returns the currently highlighted row
func (m FilteredTable) HighlightedRow() table.Row {
	return m.table.HighlightedRow()
}

// SelectedRows returns all selected rows (if multi-select is enabled)
func (m FilteredTable) SelectedRows() []table.Row {
	return m.table.SelectedRows()
}

// GetRowCount returns the number of visible rows (after filtering)
func (m FilteredTable) GetRowCount() int {
	// This is approximate - bubble-table doesn't expose this directly
	return len(m.allRows)
}
