package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"charm.land/bubbles/v2/textinput"
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/glamour/v2"
	"charm.land/lipgloss/v2"
	"github.com/Slach/clickhouse-timeline/pkg/config"
	"github.com/Slach/clickhouse-timeline/pkg/expert"
	"github.com/Slach/clickhouse-timeline/pkg/sqlfmt"
	"github.com/rs/zerolog/log"
)

// ExpertTokenMsg delivers a streaming token chunk.
type ExpertTokenMsg struct {
	Token string
}

// ExpertDoneMsg signals that the LLM response is complete.
type ExpertDoneMsg struct {
	Err error
}

// expertResponseMsg carries the full LLM response for display.
type expertResponseMsg struct {
	Content string
	Model   string
	Events  []expert.ChatEvent
}

// expertTickMsg is sent every second to update the thinking timer.
type expertTickMsg struct{}

// ExpertSkillsLoadedMsg signals that skills have been loaded.
type ExpertSkillsLoadedMsg struct {
	Skills []expert.Skill
	Err    error
}

// expertViewer is the TUI model for the expert chat view.
type expertViewer struct {
	viewport viewport.Model
	input    textinput.Model
	messages []expert.ChatMessage
	agent    *expert.ExpertAgent

	// Skills
	skills     []expert.Skill
	skillNames []string

	// Skill autocomplete
	skillSuggestions      []string
	selectedSkillPosition int
	showSkillSuggestions  bool
	skillScrollOffset     int

	// Markdown renderer
	mdRenderer *glamour.TermRenderer

	// State
	loading      bool
	loadingStart time.Time
	streamBuffer strings.Builder
	agentErr     error
	skillsLoaded bool

	// Config
	cfg           config.ExpertConfig
	chClient      interface{} // set later via App
	initialPrompt string      // --prompt flag, sent after skills load
	promptSent    bool

	width  int
	height int
}

var (
	expertTitleStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("226")).
				Background(lipgloss.Color("235")).
				Padding(0, 1)

	expertUserStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("86")).
			Bold(true)

	expertAssistantStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("252"))

	expertSkillBadgeStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("214")).
				Bold(true)

	expertToolStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("33")).
			Italic(true)

	expertToolResultStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("242"))

	expertModelStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("245")).
				Italic(true)

	expertErrorStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("196"))

	expertLoadingStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("241")).
				Italic(true)

	expertRetryStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("214")).
				Italic(true)

	expertSuggestionStyle = lipgloss.NewStyle().
				Background(lipgloss.Color("236")).
				Foreground(lipgloss.Color("252"))

	expertSelectedSuggStyle = lipgloss.NewStyle().
				Background(lipgloss.Color("62")).
				Foreground(lipgloss.Color("255")).
				Bold(true)

	expertScrollTrackStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("238"))

	expertScrollThumbStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("62"))
)

// maxVisibleSuggestions returns how many skill suggestions fit on screen.
// Layout: title(1) + \n(1) + viewport + \n(1) + suggestions + input(3)
// We take space from the viewport, so suggestions = min(height/3, 10, total).
func (m *expertViewer) maxVisibleSuggestions() int {
	maxFromHeight := m.height / 3
	if maxFromHeight < 3 {
		maxFromHeight = 3
	}
	if maxFromHeight > 10 {
		maxFromHeight = 10
	}
	if len(m.skillSuggestions) < maxFromHeight {
		return len(m.skillSuggestions)
	}
	return maxFromHeight
}

// syncViewportHeight adjusts viewport height to make room for skill suggestions.
// Must be called from Update() whenever suggestion visibility or scroll changes.
func (m *expertViewer) syncViewportHeight() {
	suggestionsHeight := 0
	if m.showSkillSuggestions && len(m.skillSuggestions) > 0 {
		mv := m.maxVisibleSuggestions()
		end := m.skillScrollOffset + mv
		if end > len(m.skillSuggestions) {
			end = len(m.skillSuggestions)
		}
		suggestionsHeight = end - m.skillScrollOffset
		remaining := len(m.skillSuggestions) - end
		if remaining > 0 {
			suggestionsHeight++ // +1 for "... N more" line
		}
	}

	baseVpHeight := m.height - 6
	vpHeight := baseVpHeight - suggestionsHeight
	if vpHeight < 5 {
		vpHeight = 5
	}
	if m.viewport.Height() != vpHeight {
		m.viewport.SetHeight(vpHeight)
		m.viewport.SetContent(m.renderChat())
	}
}

func newExpertViewer(width, height int, cfg config.ExpertConfig) expertViewer {
	// Chat viewport
	vp := viewport.New(viewport.WithWidth(width-2), viewport.WithHeight(height-6))

	// Input field
	ti := textinput.New()
	ti.Placeholder = "Type message or /skill..."
	ti.Prompt = "> "
	ti.CharLimit = 2000
	_ = ti.Focus()

	// Markdown renderer for LLM responses
	mdWidth := width - 4
	if mdWidth < 40 {
		mdWidth = 40
	}
	renderer, err := glamour.NewTermRenderer(
		glamour.WithStandardStyle("dark"),
		glamour.WithWordWrap(mdWidth),
	)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to create markdown renderer, falling back to plain text")
	}

	return expertViewer{
		viewport:   vp,
		input:      ti,
		mdRenderer: renderer,
		cfg:        cfg,
		width:      width,
		height:     height,
	}
}

func (m *expertViewer) Init() tea.Cmd {
	return nil
}

func (m *expertViewer) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case ExpertSkillsLoadedMsg:
		if msg.Err != nil {
			m.agentErr = msg.Err
			m.viewport.SetContent(m.renderChat())
			return m, nil
		}
		m.skills = msg.Skills
		m.skillNames = expert.SkillNames(msg.Skills)
		m.skillsLoaded = true
		m.viewport.SetContent(m.renderChat())
		// Auto-send initial prompt from --prompt flag
		if m.initialPrompt != "" && !m.promptSent {
			m.promptSent = true
			cmd := m.sendMessage(m.initialPrompt)
			return m, cmd
		}
		return m, nil

	case expertTickMsg:
		if m.loading {
			m.viewport.SetContent(m.renderChat())
			m.viewport.GotoBottom()
			return m, expertTimerTick()
		}
		return m, nil

	case ExpertTokenMsg:
		m.streamBuffer.WriteString(msg.Token)
		m.viewport.SetContent(m.renderChat())
		m.viewport.GotoBottom()
		return m, nil

	case ExpertDoneMsg:
		m.loading = false
		if msg.Err != nil {
			m.messages = append(m.messages, expert.ChatMessage{
				Role:    "assistant",
				Content: fmt.Sprintf("Error: %v", msg.Err),
			})
		}
		m.streamBuffer.Reset()
		m.viewport.SetContent(m.renderChat())
		m.viewport.GotoBottom()
		return m, nil

	case expertResponseMsg:
		m.loading = false
		// Add tool events as visible messages
		for _, ev := range msg.Events {
			switch ev.Type {
			case "tool_start":
				m.messages = append(m.messages, expert.ChatMessage{
					Role:      "tool",
					ToolName:  ev.Tool,
					ToolQuery: ev.Query,
				})
			case "tool_done":
				// Find the last tool message and update it
				for i := len(m.messages) - 1; i >= 0; i-- {
					if m.messages[i].Role == "tool" && m.messages[i].ToolName == ev.Tool && m.messages[i].ToolResult == "" && m.messages[i].ToolError == "" {
						if ev.Error != "" {
							m.messages[i].ToolError = ev.Error
						} else {
							m.messages[i].ToolResult = truncateForDisplay(ev.Result, 500)
						}
						break
					}
				}
			case "retry":
				m.messages = append(m.messages, expert.ChatMessage{
					Role:    "system",
					Content: fmt.Sprintf("Rate limited (429). Retry %d/%d...", ev.Attempt, ev.MaxRetries),
				})
			}
		}
		// Add the assistant response
		m.messages = append(m.messages, expert.ChatMessage{
			Role:    "assistant",
			Content: msg.Content,
			Model:   msg.Model,
		})
		m.streamBuffer.Reset()
		m.viewport.SetContent(m.renderChat())
		m.viewport.GotoBottom()
		return m, nil

	case tea.KeyPressMsg:
		if m.loading {
			return m, nil // Ignore input while loading
		}

		// Handle skill suggestions
		if m.showSkillSuggestions {
			switch msg.String() {
			case "esc":
				m.showSkillSuggestions = false
				m.syncViewportHeight()
				return m, nil
			case "tab", "enter":
				if len(m.skillSuggestions) > 0 {
					selected := m.skillSuggestions[m.selectedSkillPosition]
					m.input.SetValue("/" + selected + " ")
					m.input.SetCursor(len(m.input.Value()))
					m.showSkillSuggestions = false
					m.syncViewportHeight()
				}
				return m, nil
			case "down", "ctrl+n":
				if m.selectedSkillPosition < len(m.skillSuggestions)-1 {
					m.selectedSkillPosition++
					mv := m.maxVisibleSuggestions()
					if m.selectedSkillPosition >= m.skillScrollOffset+mv {
						m.skillScrollOffset = m.selectedSkillPosition - mv + 1
					}
				}
				return m, nil
			case "up", "ctrl+p":
				if m.selectedSkillPosition > 0 {
					m.selectedSkillPosition--
					if m.selectedSkillPosition < m.skillScrollOffset {
						m.skillScrollOffset--
					}
				}
				return m, nil
			}
		}

		switch msg.String() {
		case "enter":
			value := strings.TrimSpace(m.input.Value())
			if value == "" {
				return m, nil
			}
			cmd := m.sendMessage(value)
			return m, cmd

		default:
			// Update input
			var cmd tea.Cmd
			m.input, cmd = m.input.Update(msg)
			cmds = append(cmds, cmd)

			// Check for skill autocomplete trigger
			m.updateSkillSuggestions()
			m.syncViewportHeight()
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.viewport.SetWidth(msg.Width - 2)
		m.viewport.SetHeight(msg.Height - 6)
		// Recreate markdown renderer with new width
		mdWidth := msg.Width - 2
		if mdWidth < 40 {
			mdWidth = 40
		}
		if renderer, err := glamour.NewTermRenderer(
			glamour.WithStandardStyle("dark"),
			glamour.WithWordWrap(mdWidth),
		); err == nil {
			m.mdRenderer = renderer
		}
		m.viewport.SetContent(m.renderChat())
	}

	// Update viewport for scrolling
	var cmd tea.Cmd
	m.viewport, cmd = m.viewport.Update(msg)
	cmds = append(cmds, cmd)

	return m, tea.Batch(cmds...)
}

func (m *expertViewer) sendMessage(value string) tea.Cmd {
	m.input.SetValue("")
	m.showSkillSuggestions = false
	m.loading = true
	m.loadingStart = time.Now()

	// Parse skill invocation
	var skill *expert.Skill
	userMsg := value

	if strings.HasPrefix(value, "/") {
		parts := strings.SplitN(value[1:], " ", 2)
		if len(parts) > 0 {
			skill = expert.FindSkillByName(m.skills, parts[0])
			if skill != nil && len(parts) > 1 {
				userMsg = parts[1]
			} else if skill != nil {
				userMsg = "Help me with " + skill.DisplayName
			}
		}
	}

	// Add user message to display
	skillName := ""
	if skill != nil {
		skillName = skill.Name
	}
	m.messages = append(m.messages, expert.ChatMessage{
		Role:    "user",
		Content: value,
		Skill:   skillName,
	})
	m.viewport.SetContent(m.renderChat())
	m.viewport.GotoBottom()

	// Return command that streams the response + starts the timer tick
	agent := m.agent
	selectedSkill := skill

	agentErr := m.agentErr
	timeout := m.cfg.LlmTimeout
	llmCmd := func() tea.Msg {
		if agent == nil {
			if agentErr != nil {
				return ExpertDoneMsg{Err: agentErr}
			}
			return ExpertDoneMsg{Err: fmt.Errorf("agent not initialized — check API key configuration in expert section of clickhouse-timeline.yml")}
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		result, err := agent.Chat(ctx, userMsg, selectedSkill)
		if err != nil {
			log.Error().Stack().Err(err).
				Str("provider", agent.Provider()).
				Str("model", agent.Model()).
				Dur("timeout", timeout).
				Msg("Expert LLM call failed")
			return ExpertDoneMsg{Err: fmt.Errorf("%w (timeout=%s)", err, timeout)}
		}

		return expertResponseMsg{
			Content: result.Content,
			Model:   result.Model,
			Events:  result.Events,
		}
	}
	return tea.Batch(llmCmd, expertTimerTick())
}

func (m *expertViewer) updateSkillSuggestions() {
	value := m.input.Value()

	// Only show suggestions when input starts with /
	if !strings.HasPrefix(value, "/") || strings.Contains(value[1:], " ") {
		m.showSkillSuggestions = false
		return
	}

	prefix := value[1:] // Remove /
	m.showSkillSuggestions = true
	m.skillSuggestions = nil

	// Prefix match
	for _, name := range m.skillNames {
		if strings.HasPrefix(name, prefix) {
			m.skillSuggestions = append(m.skillSuggestions, name)
		}
	}

	// Contains fallback
	if len(m.skillSuggestions) == 0 {
		for _, name := range m.skillNames {
			if strings.Contains(name, prefix) {
				m.skillSuggestions = append(m.skillSuggestions, name)
			}
		}
	}

	// Show all if just "/"
	if prefix == "" {
		m.skillSuggestions = append([]string{}, m.skillNames...)
	}

	if len(m.skillSuggestions) == 0 {
		m.showSkillSuggestions = false
	}
	if m.selectedSkillPosition >= len(m.skillSuggestions) {
		m.selectedSkillPosition = 0
		m.skillScrollOffset = 0
	}
}

func (m *expertViewer) renderChat() string {
	var b strings.Builder

	// Status line
	if m.agentErr != nil {
		errText := fmt.Sprintf("Error: %v", m.agentErr)
		b.WriteString(wrapText(expertErrorStyle.Render(errText), m.width-2))
		b.WriteString("\n\n")
	}

	if !m.skillsLoaded && m.agentErr == nil {
		b.WriteString(expertLoadingStyle.Render("Loading skills..."))
		b.WriteString("\n\n")
	} else if m.skillsLoaded {
		b.WriteString(expertLoadingStyle.Render(fmt.Sprintf("%d skills loaded. Type / for autocomplete.", len(m.skills))))
		b.WriteString("\n\n")
	}

	// Messages
	for _, msg := range m.messages {
		switch msg.Role {
		case "user":
			prefix := expertUserStyle.Render("You: ")
			if msg.Skill != "" {
				prefix = expertUserStyle.Render("You ") + expertSkillBadgeStyle.Render("[/"+msg.Skill+"]") + expertUserStyle.Render(": ")
			}
			b.WriteString(prefix + msg.Content + "\n\n")

		case "tool":
			b.WriteString(expertToolStyle.Render(fmt.Sprintf("  >> %s", msg.ToolName)))
			if msg.ToolQuery != "" {
				// Format and highlight SQL query
				formatted := sqlfmt.FormatAndHighlightSQL(msg.ToolQuery)
				queryLines := strings.Split(formatted, "\n")
				for _, line := range queryLines {
					b.WriteString("\n     " + line)
				}
			}
			b.WriteString("\n")
			if msg.ToolError != "" {
				b.WriteString(expertErrorStyle.Render("     Error: "+msg.ToolError) + "\n")
			} else if msg.ToolResult != "" {
				resultLines := strings.Split(msg.ToolResult, "\n")
				for _, line := range resultLines {
					b.WriteString(expertToolResultStyle.Render("     "+line) + "\n")
				}
			}
			b.WriteString("\n")

		case "assistant":
			modelInfo := ""
			if msg.Model != "" {
				modelInfo = expertModelStyle.Render(" [" + msg.Model + "]")
			}
			b.WriteString(expertAssistantStyle.Render("Expert:") + modelInfo + "\n")
			b.WriteString(m.renderMarkdown(msg.Content))
			b.WriteString("\n\n")

		case "system":
			b.WriteString(expertRetryStyle.Render("  ⟳ "+msg.Content) + "\n")
		}
	}

	// Streaming buffer
	if m.loading {
		if m.streamBuffer.Len() > 0 {
			b.WriteString(expertAssistantStyle.Render("Expert: " + m.streamBuffer.String()))
			b.WriteString(expertLoadingStyle.Render("..."))
		} else {
			elapsed := int(time.Since(m.loadingStart).Seconds())
			b.WriteString(expertLoadingStyle.Render(fmt.Sprintf("[%s|%s] Thinking... %ds", m.cfg.Provider, m.cfg.Model, elapsed)))
		}
		b.WriteString("\n")
	}

	return b.String()
}

func (m *expertViewer) renderScrollbar() string {
	vpHeight := m.viewport.Height()
	if vpHeight <= 0 {
		return ""
	}

	scrollPercent := m.viewport.ScrollPercent()
	atTop := m.viewport.AtTop()
	atBottom := m.viewport.AtBottom()

	// If content fits in viewport, no scrollbar needed
	if atTop && atBottom {
		return ""
	}

	// Calculate thumb size and position
	// Minimum thumb size is 1 character
	thumbSize := max(1, vpHeight/5)
	trackSize := vpHeight - thumbSize

	var thumbPos int
	if atTop {
		thumbPos = 0
	} else if atBottom {
		thumbPos = trackSize
	} else {
		thumbPos = int(float64(trackSize) * scrollPercent)
	}

	var sb strings.Builder
	for i := range vpHeight {
		if i >= thumbPos && i < thumbPos+thumbSize {
			sb.WriteString(expertScrollThumbStyle.Render("┃"))
		} else {
			sb.WriteString(expertScrollTrackStyle.Render("│"))
		}
		if i < vpHeight-1 {
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

func (m *expertViewer) View() tea.View {
	// Title
	title := expertTitleStyle.Render(" Expert Chat ")
	if len(m.skills) > 0 {
		title += expertLoadingStyle.Render(fmt.Sprintf(" [%d skills]", len(m.skills)))
	}

	// Viewport with scrollbar
	vpView := m.viewport.View()
	scrollbar := m.renderScrollbar()

	var chatArea string
	if scrollbar != "" {
		chatArea = lipgloss.JoinHorizontal(lipgloss.Top, vpView, " ", scrollbar)
	} else {
		chatArea = vpView
	}

	// Skill suggestions dropdown
	suggestionsView := ""
	if m.showSkillSuggestions && len(m.skillSuggestions) > 0 {
		var lines []string
		maxVisible := m.maxVisibleSuggestions()
		start := m.skillScrollOffset
		end := start + maxVisible
		if end > len(m.skillSuggestions) {
			end = len(m.skillSuggestions)
		}

		for i := start; i < end; i++ {
			name := m.skillSuggestions[i]
			// Find description
			desc := ""
			if s := expert.FindSkillByName(m.skills, name); s != nil {
				desc = s.Description
				if len(desc) > 60 {
					desc = desc[:57] + "..."
				}
				if desc != "" {
					desc = " - " + desc
				}
			}

			line := fmt.Sprintf(" /%s%s ", name, desc)
			if i == m.selectedSkillPosition {
				lines = append(lines, expertSelectedSuggStyle.Render(line))
			} else {
				lines = append(lines, expertSuggestionStyle.Render(line))
			}
		}

		remaining := len(m.skillSuggestions) - end
		if remaining > 0 {
			lines = append(lines, expertLoadingStyle.Render(fmt.Sprintf("  ... %d more (arrows to scroll)", remaining)))
		}

		suggestionsView = strings.Join(lines, "\n") + "\n"
	}

	// Input
	inputView := lipgloss.NewStyle().
		BorderStyle(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("62")).
		Padding(0, 1).
		Render(m.input.View())

	// Compose layout
	content := title + "\n" + chatArea + "\n" + suggestionsView + inputView

	v := tea.NewView(content)
	v.AltScreen = true
	return v
}

func (m *expertViewer) renderMarkdown(text string) string {
	if m.mdRenderer == nil {
		return text
	}
	rendered, err := m.mdRenderer.Render(text)
	if err != nil {
		return text
	}
	return strings.TrimSpace(rendered)
}

func expertTimerTick() tea.Cmd {
	return tea.Tick(time.Second, func(time.Time) tea.Msg {
		return expertTickMsg{}
	})
}

func truncateForDisplay(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "\n... (truncated)"
}

// ShowExpert initializes and shows the expert chat view.
func (a *App) ShowExpert() tea.Cmd {
	viewer := newExpertViewer(a.width, a.height, a.cfg.Expert)
	a.currentPage = pageExpert

	// Set initial prompt from --prompt CLI flag
	if a.state.CLI != nil && a.state.CLI.ExpertPrompt != "" {
		viewer.initialPrompt = a.state.CLI.ExpertPrompt
	}

	// Try to create agent immediately if we have config
	cfg := a.cfg.Expert
	cfg.ExpertDefaults()
	chClient := a.clickHouse

	agent, err := expert.NewExpertAgent(cfg, nil, chClient)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to create expert agent")
		viewer.agentErr = fmt.Errorf("failed to create LLM agent (provider=%q, model=%q): %w\n\nSupported providers: openai, anthropic, openrouter, groq, ollama, azure_openai, google, mistral, cohere, deepseek, bedrock, lambda, lmstudio, vllm, aliyun\n\nCheck expert section in ~/.clickhouse-timeline/clickhouse-timeline.yml and ensure API key env var is exported (use 'export VAR=value', not just 'VAR=value')", cfg.Provider, cfg.Model, err)
	} else {
		viewer.agent = agent
	}
	a.expertHandler = &viewer

	// Start async skills loading
	return loadAndUpdateSkillsCmd(cfg.GetSkillsRepo())
}

// loadAndUpdateSkillsCmd clones/updates skills repo and loads skills.
func loadAndUpdateSkillsCmd(repoURL string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		if err := expert.CloneOrUpdateSkills(ctx, repoURL); err != nil {
			log.Warn().Err(err).Msg("Failed to clone/update skills")
			// Try to load existing skills anyway
		}

		skills, err := expert.LoadSkills()
		if err != nil {
			return ExpertSkillsLoadedMsg{Err: fmt.Errorf("load skills: %w", err)}
		}

		return ExpertSkillsLoadedMsg{Skills: skills}
	}
}
