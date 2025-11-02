package tui

import (
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// ShowDescriptionMsg is sent when a description modal should be shown
type ShowDescriptionMsg struct {
	Name        string
	Description string
}

// CloseDescriptionMsg is sent when the description modal should be closed
type CloseDescriptionMsg struct{}

// metricDescriptionModal is a simple modal for displaying metric descriptions
type metricDescriptionModal struct {
	name        string
	description string
	width       int
	height      int
	visible     bool
}

func newMetricDescriptionModal() *metricDescriptionModal {
	return &metricDescriptionModal{
		visible: false,
	}
}

func (m *metricDescriptionModal) Show(name, description string) {
	m.name = name
	m.description = description
	m.visible = true
}

func (m *metricDescriptionModal) Hide() {
	m.visible = false
}

func (m *metricDescriptionModal) IsVisible() bool {
	return m.visible
}

func (m *metricDescriptionModal) Update(msg tea.Msg) (*metricDescriptionModal, tea.Cmd) {
	if !m.visible {
		return m, nil
	}

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "enter", "esc", "q":
			m.Hide()
			return m, nil
		}
	case ShowDescriptionMsg:
		m.Show(msg.Name, msg.Description)
		return m, nil
	case CloseDescriptionMsg:
		m.Hide()
		return m, nil
	}

	return m, nil
}

func (m *metricDescriptionModal) View() string {
	if !m.visible {
		return ""
	}

	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("11")). // Yellow
		Align(lipgloss.Center).
		Width(60)

	descStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("15")). // White
		Width(60).
		Align(lipgloss.Left)

	helpStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("8")). // Gray
		Align(lipgloss.Center).
		Width(60)

	borderStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("6")). // Cyan
		Padding(1, 2).
		Width(64)

	// Word wrap description to fit within modal width
	wrappedDesc := wordWrap(m.description, 56)

	content := lipgloss.JoinVertical(lipgloss.Left,
		titleStyle.Render(m.name),
		"",
		descStyle.Render(wrappedDesc),
		"",
		helpStyle.Render("Press Enter or ESC to close"),
	)

	return lipgloss.Place(
		m.width,
		m.height,
		lipgloss.Center,
		lipgloss.Center,
		borderStyle.Render(content),
	)
}

func (m *metricDescriptionModal) SetSize(width, height int) {
	m.width = width
	m.height = height
}

// wordWrap wraps text to a specified width
func wordWrap(text string, width int) string {
	if len(text) <= width {
		return text
	}

	var result strings.Builder
	words := strings.Fields(text)
	lineLen := 0

	for i, word := range words {
		wordLen := len(word)

		if lineLen+wordLen+1 > width {
			result.WriteString("\n")
			lineLen = 0
		} else if i > 0 {
			result.WriteString(" ")
			lineLen++
		}

		result.WriteString(word)
		lineLen += wordLen
	}

	return result.String()
}

// ShowDescriptionBubble is a helper to show a description modal in bubbletea
func (a *App) ShowDescriptionBubble(name, description string) tea.Cmd {
	return func() tea.Msg {
		return ShowDescriptionMsg{
			Name:        name,
			Description: description,
		}
	}
}
