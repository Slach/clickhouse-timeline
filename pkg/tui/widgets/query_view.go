package widgets

import (
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	sqlfmt "github.com/Slach/clickhouse-timeline/pkg/sqlfmt"
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
	content := sqlfmt.FormatAndHighlightSQL(sql)
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
