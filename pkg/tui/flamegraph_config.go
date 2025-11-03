package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// FlamegraphParams structure for storing flamegraph parameters
type FlamegraphParams struct {
	CategoryType  CategoryType
	CategoryValue string
	TraceType     TraceType
	FromTime      time.Time
	ToTime        time.Time
	SourcePage    string // Tracks where the flamegraph was called from
}

// FlamegraphConfigMsg is sent when flamegraph configuration is completed
type FlamegraphConfigMsg struct {
	CategoryType  CategoryType
	CategoryValue string
	TraceType     TraceType
	FromTime      time.Time
	ToTime        time.Time
}

// flamegraphConfigForm is the configuration form for flamegraph
type flamegraphConfigForm struct {
	categoryType   CategoryType
	categoryValue  textinput.Model
	traceType      TraceType
	fromTime       time.Time
	toTime         time.Time
	currentField   int // 0=category type, 1=category value, 2=trace type, 3=buttons
	categoryIdx    int
	traceIdx       int
	selectedButton int // 0=Show flamegraph, 1=Cancel
	width          int
	height         int
}

var categoryOptions = []string{
	"Query Hash",
	"Table",
	"Host",
	"Time Range Only",
}

var traceOptions = []string{
	string(TraceMemory),
	string(TraceCPU),
	string(TraceReal),
	string(TraceMemorySample),
}

func newFlamegraphConfigForm(categoryType CategoryType, categoryValue string, traceType TraceType, fromTime, toTime time.Time, width, height int) flamegraphConfigForm {
	valueInput := textinput.New()
	valueInput.Placeholder = "category value"
	valueInput.Width = 40
	if categoryValue != "" {
		valueInput.SetValue(categoryValue)
	}
	valueInput.Focus()

	// Determine category index
	categoryIdx := 0
	switch categoryType {
	case CategoryQueryHash:
		categoryIdx = 0
	case CategoryTable:
		categoryIdx = 1
	case CategoryHost:
		categoryIdx = 2
	case "":
		categoryIdx = 3
	}

	// Determine trace index
	traceIdx := 0
	for i, opt := range traceOptions {
		if opt == string(traceType) {
			traceIdx = i
			break
		}
	}

	return flamegraphConfigForm{
		categoryType:   categoryType,
		categoryValue:  valueInput,
		traceType:      traceType,
		fromTime:       fromTime,
		toTime:         toTime,
		currentField:   1, // Start on value input
		categoryIdx:    categoryIdx,
		traceIdx:       traceIdx,
		selectedButton: 0, // Default to "Show flamegraph" button
		width:          width,
		height:         height,
	}
}

func (m flamegraphConfigForm) Init() tea.Cmd {
	return textinput.Blink
}

func (m flamegraphConfigForm) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "q":
			return m, func() tea.Msg {
				return tea.KeyMsg{Type: tea.KeyEsc}
			}
		case "tab", "down":
			m.currentField = (m.currentField + 1) % 4
			m.updateFocus()
		case "shift+tab", "up":
			m.currentField = (m.currentField + 3) % 4
			m.updateFocus()
		case "left":
			if m.currentField == 0 {
				// Category type
				m.categoryIdx = (m.categoryIdx + len(categoryOptions) - 1) % len(categoryOptions)
				m.updateCategoryType()
			} else if m.currentField == 2 {
				// Trace type
				m.traceIdx = (m.traceIdx + len(traceOptions) - 1) % len(traceOptions)
				m.updateTraceType()
			} else if m.currentField == 3 {
				// Buttons
				m.selectedButton = (m.selectedButton + 1) % 2
			}
		case "right":
			if m.currentField == 0 {
				// Category type
				m.categoryIdx = (m.categoryIdx + 1) % len(categoryOptions)
				m.updateCategoryType()
			} else if m.currentField == 2 {
				// Trace type
				m.traceIdx = (m.traceIdx + 1) % len(traceOptions)
				m.updateTraceType()
			} else if m.currentField == 3 {
				// Buttons
				m.selectedButton = (m.selectedButton + 1) % 2
			}
		case "enter", "ctrl+enter":
			// Handle button selection
			if m.currentField == 3 {
				if m.selectedButton == 1 {
					// Cancel button
					return m, func() tea.Msg {
						return tea.KeyMsg{Type: tea.KeyEsc}
					}
				}
				// Show flamegraph button - fall through to validation
			}

			// Validate and submit
			if m.categoryType != "" && m.categoryValue.Value() == "" {
				// Category type requires a value
				return m, nil
			}

			return m, func() tea.Msg {
				return FlamegraphConfigMsg{
					CategoryType:  m.categoryType,
					CategoryValue: m.categoryValue.Value(),
					TraceType:     m.traceType,
					FromTime:      m.fromTime,
					ToTime:        m.toTime,
				}
			}
		}
	}

	// Update the value input if it's focused
	if m.currentField == 1 {
		m.categoryValue, cmd = m.categoryValue.Update(msg)
	}

	return m, cmd
}

func (m *flamegraphConfigForm) updateCategoryType() {
	switch m.categoryIdx {
	case 0:
		m.categoryType = CategoryQueryHash
	case 1:
		m.categoryType = CategoryTable
	case 2:
		m.categoryType = CategoryHost
	case 3:
		m.categoryType = ""
	}
}

func (m *flamegraphConfigForm) updateTraceType() {
	m.traceType = TraceType(traceOptions[m.traceIdx])
}

func (m *flamegraphConfigForm) updateFocus() {
	if m.currentField == 1 {
		m.categoryValue.Focus()
	} else {
		m.categoryValue.Blur()
	}
}

func (m flamegraphConfigForm) View() string {
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	labelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("15"))
	selectedStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("10")).Bold(true)
	normalStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))

	var sb strings.Builder
	sb.WriteString(titleStyle.Render("Flamegraph Configuration"))
	sb.WriteString("\n\n")

	// Category Type
	if m.currentField == 0 {
		sb.WriteString(labelStyle.Render("> Category Type:"))
	} else {
		sb.WriteString(labelStyle.Render("  Category Type:"))
	}
	sb.WriteString("\n  ")
	for i, opt := range categoryOptions {
		if i == m.categoryIdx {
			sb.WriteString(selectedStyle.Render(fmt.Sprintf("[%s]", opt)))
		} else {
			sb.WriteString(normalStyle.Render(fmt.Sprintf(" %s ", opt)))
		}
		sb.WriteString(" ")
	}
	sb.WriteString("\n\n")

	// Category Value
	if m.currentField == 1 {
		sb.WriteString(labelStyle.Render("> Category Value:"))
	} else {
		sb.WriteString(labelStyle.Render("  Category Value:"))
	}
	sb.WriteString("\n  ")
	sb.WriteString(m.categoryValue.View())
	if m.categoryType == "" {
		sb.WriteString(normalStyle.Render(" (not needed for time range only)"))
	}
	sb.WriteString("\n\n")

	// Trace Type
	if m.currentField == 2 {
		sb.WriteString(labelStyle.Render("> Trace Type:"))
	} else {
		sb.WriteString(labelStyle.Render("  Trace Type:"))
	}
	sb.WriteString("\n  ")
	for i, opt := range traceOptions {
		if i == m.traceIdx {
			sb.WriteString(selectedStyle.Render(fmt.Sprintf("[%s]", opt)))
		} else {
			sb.WriteString(normalStyle.Render(fmt.Sprintf(" %s ", opt)))
		}
		sb.WriteString(" ")
	}
	sb.WriteString("\n\n")

	// Time Range
	sb.WriteString(labelStyle.Render("Time Range:"))
	sb.WriteString("\n  ")
	timeRange := fmt.Sprintf("from %s to %s",
		m.fromTime.Format("2006-01-02 15:04:05"),
		m.toTime.Format("2006-01-02 15:04:05"))
	sb.WriteString(timeRange)
	sb.WriteString("\n\n")

	// Buttons
	buttonStyle := lipgloss.NewStyle().
		Padding(0, 2).
		Foreground(lipgloss.Color("15")).
		Background(lipgloss.Color("8"))
	selectedButtonStyle := lipgloss.NewStyle().
		Padding(0, 2).
		Foreground(lipgloss.Color("0")).
		Background(lipgloss.Color("10")).
		Bold(true)

	if m.currentField == 3 {
		sb.WriteString(labelStyle.Render("> "))
	} else {
		sb.WriteString("  ")
	}

	// Show flamegraph button
	if m.currentField == 3 && m.selectedButton == 0 {
		sb.WriteString(selectedButtonStyle.Render("Show Flamegraph"))
	} else {
		sb.WriteString(buttonStyle.Render("Show Flamegraph"))
	}
	sb.WriteString("  ")

	// Cancel button
	if m.currentField == 3 && m.selectedButton == 1 {
		sb.WriteString(selectedButtonStyle.Render("Cancel"))
	} else {
		sb.WriteString(buttonStyle.Render("Cancel"))
	}
	sb.WriteString("\n\n")

	// Help
	sb.WriteString(helpStyle.Render("Tab/Shift+Tab: Navigate | ←→: Select options | Enter: Activate | Esc: Cancel"))

	// Wrap in border
	borderStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("6")).
		Padding(1, 2)

	return borderStyle.Render(sb.String())
}

// ShowFlamegraphForm displays the flamegraph configuration form
func (a *App) ShowFlamegraphForm(params ...FlamegraphParams) {
	var categoryType = CategoryQueryHash
	var categoryValue string
	var traceType = TraceReal
	var fromTime, toTime = a.state.FromTime, a.state.ToTime

	// If parameters are passed, use them
	if len(params) > 0 {
		categoryType = params[0].CategoryType
		categoryValue = params[0].CategoryValue
		if params[0].TraceType != "" {
			traceType = params[0].TraceType
		}
		if !params[0].FromTime.IsZero() {
			fromTime = params[0].FromTime
		}
		if !params[0].ToTime.IsZero() {
			toTime = params[0].ToTime
		}
	}

	form := newFlamegraphConfigForm(categoryType, categoryValue, traceType, fromTime, toTime, a.width, a.height)
	a.flamegraphHandler = form
	a.currentPage = pageFlamegraph
}
