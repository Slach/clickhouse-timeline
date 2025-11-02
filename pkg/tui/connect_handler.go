package tui

import (
	"fmt"

	"github.com/Slach/clickhouse-timeline/pkg/client"
	"github.com/Slach/clickhouse-timeline/pkg/config"
	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/rs/zerolog/log"
)

// ContextSelectedMsg is sent when a context is selected
type ContextSelectedMsg struct {
	Context config.Context
}

// ConnectionResultMsg is sent when connection attempt completes
type ConnectionResultMsg struct {
	Context config.Context
	Client  *client.Client
	Version string
	Err     error
}

// connectSelector is a bubbletea model for selecting and connecting to a context
type connectSelector struct {
	list       widgets.FilteredList
	contexts   []config.Context
	connecting bool
	err        error
}

func newConnectSelector(contexts []config.Context, width, height int) connectSelector {
	// Build display names
	names := make([]string, len(contexts))
	for i, ctx := range contexts {
		names[i] = fmt.Sprintf("%s (%s:%d)", ctx.Name, ctx.Host, ctx.Port)
	}

	listModel := widgets.NewFilteredList("Select Connection", names, width, height)

	return connectSelector{
		list:       listModel,
		contexts:   contexts,
		connecting: false,
	}
}

func (m connectSelector) Init() tea.Cmd {
	return nil
}

func (m connectSelector) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case ConnectionResultMsg:
		m.connecting = false
		// Connection result will be handled by parent App
		return m, nil

	case tea.KeyMsg:
		if m.connecting {
			// During connection, only allow escape
			if msg.String() == "esc" {
				return m, nil
			}
			return m, nil
		}

		switch msg.String() {
		case "enter":
			// Get selected context
			selectedIdx := m.list.SelectedIndex()
			if selectedIdx >= 0 && selectedIdx < len(m.contexts) {
				m.connecting = true
				return m, func() tea.Msg {
					return ContextSelectedMsg{Context: m.contexts[selectedIdx]}
				}
			}
		case "esc", "q":
			// Return to main - parent will handle this
			return m, nil
		}
	}

	// Delegate to list
	m.list, cmd = m.list.Update(msg)
	return m, cmd
}

func (m connectSelector) View() string {
	if m.connecting {
		return m.list.View() + "\n\nConnecting to ClickHouse..."
	}
	if m.err != nil {
		return m.list.View() + fmt.Sprintf("\n\nError: %v\nPress ESC to go back", m.err)
	}
	return m.list.View()
}

// connectToContextCmd attempts to connect to a ClickHouse context
func (a *App) connectToContextCmd(ctx config.Context) tea.Cmd {
	return func() tea.Msg {
		clickHouse := client.NewClient(ctx, a.version)

		version, err := clickHouse.GetVersion()
		if err != nil {
			log.Error().Err(err).Str("host", ctx.Host).Int("port", ctx.Port).Msg("failed to connect to ClickHouse")
			return ConnectionResultMsg{
				Context: ctx,
				Err:     err,
			}
		}

		return ConnectionResultMsg{
			Context: ctx,
			Client:  clickHouse,
			Version: version,
		}
	}
}

func (a *App) handleConnectCommand() {
	// Create connect selector
	selector := newConnectSelector(a.cfg.Contexts, a.width, a.height)
	a.connectHandler = selector
	a.currentPage = pageConnect
}

func (a *App) getContextString(ctx config.Context) string {
	return fmt.Sprintf("%s (%s:%d)", ctx.Name, ctx.Host, ctx.Port)
}
