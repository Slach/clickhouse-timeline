package tui

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"charm.land/bubbles/v2/key"
	"charm.land/bubbles/v2/textarea"
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

// ExpertExitMsg signals that the user wants to exit the expert view.
type ExpertExitMsg struct{}

// expertResponseMsg carries the full LLM response for display.
type expertResponseMsg struct {
	Content   string
	Model     string
	Reasoning string
	Events    []expert.ChatEvent
}

// expertEventMsg delivers a single progressive event from the LLM chat.
type expertEventMsg struct {
	Event expert.ChatEvent
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
	input    textarea.Model
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
	cancelFunc   context.CancelFunc // cancel the in-flight LLM request
	eventCh      <-chan expert.ChatEvent // progressive event channel

	// Block navigation (reasoning, tool, assistant blocks)
	blockFocusIdx     int          // which navigable block is focused (-1 = none)
	blockExpanded     map[int]bool // message idx -> expanded state
	focusedLineOffset int          // line offset of focused block in rendered content

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

	expertReasonStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("240")).
				Italic(true)

	expertReasonFocusedStyle = lipgloss.NewStyle().
					Foreground(lipgloss.Color("214")).
					Italic(true)

	expertReasonCollapsedStyle = lipgloss.NewStyle().
					Foreground(lipgloss.Color("245")).
					Bold(true)

	expertFocusedBlockStyle = lipgloss.NewStyle().
				Background(lipgloss.Color("236"))
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

	// title(1) + \n(1) + \n(1) + input border(2) + input lines(3) = 8
	baseVpHeight := m.height - 8
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
	vp := viewport.New(viewport.WithWidth(width-2), viewport.WithHeight(height-8))

	// Input field (multiline textarea)
	ti := textarea.New()
	ti.Placeholder = "Type message or /skill... (Shift+Enter newline, /exit to quit)"
	ti.Prompt = "> "
	ti.CharLimit = 4000
	ti.ShowLineNumbers = false
	ti.SetHeight(3)
	ti.MaxHeight = 8
	ti.SetWidth(width - 4)
	// Enter sends the message; Shift+Enter inserts a newline
	ti.KeyMap.InsertNewline = key.NewBinding(key.WithKeys("shift+enter"))
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
		viewport:          vp,
		input:             ti,
		mdRenderer:        renderer,
		cfg:               cfg,
		width:             width,
		height:            height,
		blockFocusIdx: -1,
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

	case expertEventMsg:
		// Progressive event from the LLM chat — update screen immediately
		m.applyEvent(msg.Event)
		m.viewport.SetContent(m.renderChat())
		m.viewport.GotoBottom()
		// Schedule reading the next event from the channel
		return m, waitForExpertEvent(m.eventCh)

	case ExpertDoneMsg:
		m.loading = false
		m.eventCh = nil
		if msg.Err != nil && !errors.Is(msg.Err, context.Canceled) {
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
		m.eventCh = nil
		// Add the assistant response (may also have reasoning from final response)
		if msg.Reasoning != "" {
			m.messages = append(m.messages, expert.ChatMessage{
				Role:      "reasoning",
				Reasoning: msg.Reasoning,
				ReasonLen: len(msg.Reasoning),
			})
		}
		m.messages = append(m.messages, expert.ChatMessage{
			Role:    "assistant",
			Content: msg.Content,
			Model:   msg.Model,
		})
		m.streamBuffer.Reset()
		m.viewport.SetContent(m.renderChat())
		m.viewport.GotoBottom()
		return m, nil

	case tea.MouseWheelMsg:
		// Handle mouse wheel scrolling
		if msg.Button == tea.MouseWheelUp {
			m.viewport.ScrollUp(3)
		} else if msg.Button == tea.MouseWheelDown {
			m.viewport.ScrollDown(3)
		}
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
			case "tab", "shift+tab", "enter":
				if len(m.skillSuggestions) > 0 {
					selected := m.skillSuggestions[m.selectedSkillPosition]
					m.input.SetValue("/" + selected + " ")
					m.input.CursorEnd()
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
			// If input has text, always send message (and reset block focus)
			if value != "" {
				m.blockFocusIdx = -1
			} else if m.blockFocusIdx >= 0 {
				// Input is empty and a block is focused — toggle its expansion
				if m.blockExpanded == nil {
					m.blockExpanded = make(map[int]bool)
				}
				cur := m.isBlockExpanded(m.blockFocusIdx)
				m.blockExpanded[m.blockFocusIdx] = !cur
				m.viewport.SetContent(m.renderChat())
				m.scrollToFocusedBlock()
				return m, nil
			}
			if value == "" {
				return m, nil
			}
			// Handle /exit command to return to main window
			if strings.EqualFold(value, "/exit") {
				m.input.Reset()
				return m, func() tea.Msg { return ExpertExitMsg{} }
			}
			// Handle /clear command to reset the dialog
			if strings.EqualFold(value, "/clear") {
				m.input.Reset()
				m.messages = nil
				m.streamBuffer.Reset()
				m.blockFocusIdx = -1
				m.blockExpanded = nil
				if m.agent != nil {
					m.agent.ClearHistory()
				}
				m.viewport.SetContent(m.renderChat())
				return m, nil
			}
			cmd := m.sendMessage(value)
			return m, cmd

		case "tab":
			// Navigate to next block
			if m.countNavigableBlocks() > 0 {
				m.navigateBlocks(1)
				m.viewport.SetContent(m.renderChat())
				m.scrollToFocusedBlock()
				return m, nil
			}

		case "shift+tab":
			// Navigate to previous block
			if m.countNavigableBlocks() > 0 {
				m.navigateBlocks(-1)
				m.viewport.SetContent(m.renderChat())
				m.scrollToFocusedBlock()
				return m, nil
			}

		case "esc":
			// Deselect block focus
			if m.blockFocusIdx >= 0 {
				m.blockFocusIdx = -1
				m.viewport.SetContent(m.renderChat())
				return m, nil
			}

		case "up":
			// Scroll viewport when input is empty; otherwise let textarea handle cursor
			if strings.TrimSpace(m.input.Value()) == "" {
				m.viewport.ScrollUp(1)
				return m, nil
			}
			var cmd tea.Cmd
			m.input, cmd = m.input.Update(msg)
			cmds = append(cmds, cmd)

		case "down":
			if strings.TrimSpace(m.input.Value()) == "" {
				m.viewport.ScrollDown(1)
				return m, nil
			}
			var cmd tea.Cmd
			m.input, cmd = m.input.Update(msg)
			cmds = append(cmds, cmd)

		case "pgup", "pgdown", "ctrl+u", "ctrl+d":
			// Let viewport handle scrolling (falls through to viewport.Update below)

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
		m.viewport.SetHeight(msg.Height - 8)
		m.input.SetWidth(msg.Width - 4)
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
	m.input.Reset()
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
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	m.cancelFunc = cancel

	// Create channel for progressive event updates
	eventCh := make(chan expert.ChatEvent, 10)
	m.eventCh = eventCh

	llmCmd := func() tea.Msg {
		defer close(eventCh)

		if agent == nil {
			cancel()
			if agentErr != nil {
				return ExpertDoneMsg{Err: agentErr}
			}
			return ExpertDoneMsg{Err: fmt.Errorf("agent not initialized — check API key configuration in expert section of clickhouse-timeline.yml")}
		}

		defer cancel()

		result, err := agent.ChatWithProgress(ctx, userMsg, selectedSkill, eventCh)
		if err != nil {
			log.Error().Stack().Err(err).
				Str("provider", agent.Provider()).
				Str("model", agent.Model()).
				Dur("timeout", timeout).
				Msg("Expert LLM call failed")
			return ExpertDoneMsg{Err: fmt.Errorf("%w (timeout=%s)", err, timeout)}
		}

		return expertResponseMsg{
			Content:   result.Content,
			Model:     result.Model,
			Reasoning: result.Reasoning,
		}
	}
	return tea.Batch(llmCmd, waitForExpertEvent(eventCh), expertTimerTick())
}

// builtinCommands are always available in autocomplete alongside skills.
var builtinCommands = []string{"clear", "exit"}

func (m *expertViewer) updateSkillSuggestions() {
	value := m.input.Value()

	// Only show suggestions when input starts with /
	if !strings.HasPrefix(value, "/") || strings.Contains(value[1:], " ") {
		m.showSkillSuggestions = false
		return
	}

	prefix := strings.ToLower(value[1:]) // Remove / and lowercase for case-insensitive match
	m.showSkillSuggestions = true
	m.skillSuggestions = nil

	allNames := append(builtinCommands, m.skillNames...)

	// Prefix match (case-insensitive)
	for _, name := range allNames {
		if strings.HasPrefix(strings.ToLower(name), prefix) {
			m.skillSuggestions = append(m.skillSuggestions, name)
		}
	}

	// Contains fallback (case-insensitive)
	if len(m.skillSuggestions) == 0 {
		for _, name := range allNames {
			if strings.Contains(strings.ToLower(name), prefix) {
				m.skillSuggestions = append(m.skillSuggestions, name)
			}
		}
	}

	// Show all if just "/"
	if prefix == "" {
		m.skillSuggestions = append([]string{}, allNames...)
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
	m.focusedLineOffset = -1

	// Status line
	if m.agentErr != nil {
		errText := fmt.Sprintf("Error: %v", m.agentErr)
		b.WriteString(wrapText(expertErrorStyle.Render(errText), m.width-2))
		b.WriteString("\n\n")
	}


	// Messages
	for i, msg := range m.messages {
		if i == m.blockFocusIdx {
			m.focusedLineOffset = strings.Count(b.String(), "\n")
		}
		isFocused := i == m.blockFocusIdx
		expanded := m.isBlockExpanded(i)

		switch msg.Role {
		case "user":
			prefix := expertUserStyle.Render("You: ")
			if msg.Skill != "" {
				prefix = expertUserStyle.Render("You ") + expertSkillBadgeStyle.Render("[/"+msg.Skill+"]") + expertUserStyle.Render(": ")
			}
			b.WriteString(prefix + msg.Content + "\n\n")

		case "tool":
			icon := "[+]"
			if expanded {
				icon = "[-]"
			}
			style := expertToolStyle
			if isFocused {
				style = expertReasonFocusedStyle
			}
			var block strings.Builder
			block.WriteString(style.Render(fmt.Sprintf("  %s >> %s", icon, msg.ToolName)))
			if msg.ToolQuery != "" {
				formatted := sqlfmt.FormatAndHighlightSQL(msg.ToolQuery)
				for _, line := range strings.Split(formatted, "\n") {
					block.WriteString("\n     " + line)
				}
			}
			block.WriteString("\n")
			if expanded {
				if msg.ToolError != "" {
					block.WriteString(expertErrorStyle.Render("     Error: "+msg.ToolError) + "\n")
				} else if msg.ToolResult != "" {
					block.WriteString(renderTSVTable(msg.ToolResult, m.width-7))
				}
			}
			if isFocused {
				for _, line := range strings.Split(block.String(), "\n") {
					b.WriteString(expertFocusedBlockStyle.Width(m.width - 4).Render(line) + "\n")
				}
			} else {
				b.WriteString(block.String())
			}
			b.WriteString("\n")

		case "assistant":
			icon := "[+]"
			if expanded {
				icon = "[-]"
			}
			modelInfo := ""
			if msg.Model != "" {
				modelInfo = expertModelStyle.Render(" [" + msg.Model + "]")
			}
			style := expertAssistantStyle
			if isFocused {
				style = expertReasonFocusedStyle
			}
			var block strings.Builder
			block.WriteString(style.Render(fmt.Sprintf("  %s Expert:", icon)) + modelInfo + "\n")
			if expanded {
				block.WriteString(m.renderMarkdown(msg.Content))
			} else {
				preview := strings.ReplaceAll(msg.Content, "\n", " ")
				if len(preview) > 80 {
					preview = preview[:77] + "..."
				}
				block.WriteString(expertToolResultStyle.Render("     " + preview))
			}
			if isFocused {
				for _, line := range strings.Split(block.String(), "\n") {
					b.WriteString(expertFocusedBlockStyle.Width(m.width - 4).Render(line) + "\n")
				}
			} else {
				b.WriteString(block.String())
			}
			b.WriteString("\n\n")

		case "reasoning":
			tokens := estimateReasoningTokens(msg.Reasoning)
			var block strings.Builder
			if expanded {
				icon := "[-]"
				style := expertReasonStyle
				if isFocused {
					style = expertReasonFocusedStyle
				}
				block.WriteString(style.Render(fmt.Sprintf("  %s Reasoning (%d tokens)", icon, tokens)))
				block.WriteString("\n")
				for _, line := range strings.Split(msg.Reasoning, "\n") {
					block.WriteString(style.Render("  | " + line) + "\n")
				}
			} else {
				icon := "[+]"
				style := expertReasonCollapsedStyle
				if isFocused {
					style = expertReasonFocusedStyle
				}
				preview := msg.Reasoning
				if len(preview) > 80 {
					preview = preview[:77] + "..."
				}
				block.WriteString(style.Render(fmt.Sprintf("  %s Reasoning (%d tokens) -- %s", icon, tokens, preview)))
			}
			if isFocused {
				for _, line := range strings.Split(block.String(), "\n") {
					b.WriteString(expertFocusedBlockStyle.Width(m.width - 4).Render(line) + "\n")
				}
			} else {
				b.WriteString(block.String())
				b.WriteString("\n")
			}
			b.WriteString("\n")

		case "system":
			b.WriteString(expertRetryStyle.Render("  > "+msg.Content) + "\n")
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
	contentFits := atTop && atBottom

	// Calculate thumb size and position
	// Minimum thumb size is 1 character, max 1/3 of viewport
	thumbSize := max(1, min(vpHeight/3, vpHeight/5))
	trackSize := vpHeight - thumbSize

	var thumbPos int
	if contentFits {
		// When content fits, thumb at top but still visible
		thumbPos = 0
	} else if atTop {
		thumbPos = 0
	} else if atBottom {
		thumbPos = trackSize
	} else {
		thumbPos = int(float64(trackSize) * scrollPercent)
		if thumbPos > trackSize {
			thumbPos = trackSize
		}
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
	if !m.skillsLoaded && m.agentErr == nil {
		title += expertLoadingStyle.Render(" Loading skills...")
	} else if m.skillsLoaded {
		statusLine := fmt.Sprintf(" %d skills loaded. Type / for autocomplete. | Tab/Shift+Tab: navigate blocks | Enter: expand/collapse | /clear: reset", len(m.skills))
		title += expertLoadingStyle.Render(statusLine)
	}
	if m.agentErr != nil {
		title += " " + expertErrorStyle.Render(fmt.Sprintf("Error: %v", m.agentErr))
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

// applyEvent processes a single chat event and updates the messages list.
func (m *expertViewer) applyEvent(ev expert.ChatEvent) {
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
					m.messages[i].ToolResult = truncateForDisplay(ev.Result, 3000)
				}
				break
			}
		}
	case "retry":
		m.messages = append(m.messages, expert.ChatMessage{
			Role:    "system",
			Content: fmt.Sprintf("Rate limited (429). Retry %d/%d...", ev.Attempt, ev.MaxRetries),
		})
	case "reasoning":
		m.messages = append(m.messages, expert.ChatMessage{
			Role:      "reasoning",
			Reasoning: ev.Reasoning,
			ReasonLen: len(ev.Reasoning),
		})
	}
}

// waitForExpertEvent returns a command that reads the next event from the channel.
func waitForExpertEvent(ch <-chan expert.ChatEvent) tea.Cmd {
	if ch == nil {
		return nil
	}
	return func() tea.Msg {
		ev, ok := <-ch
		if !ok {
			return nil // channel closed
		}
		return expertEventMsg{Event: ev}
	}
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
// If an expert session already exists, it is reused to preserve the dialog.
func (a *App) ShowExpert() tea.Cmd {
	a.currentPage = pageExpert

	// Reuse existing expert session if available
	if a.expertHandler != nil {
		if ev, ok := a.expertHandler.(*expertViewer); ok {
			ev.width = a.width
			ev.height = a.height
			ev.viewport.SetWidth(a.width - 2)
			ev.viewport.SetHeight(a.height - 6)
			ev.viewport.SetContent(ev.renderChat())
			_ = ev.input.Focus()
		}
		return nil
	}

	viewer := newExpertViewer(a.width, a.height, a.cfg.Expert)

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

// scrollToFocusedBlock scrolls the viewport so the focused block is visible.
func (m *expertViewer) scrollToFocusedBlock() {
	if m.focusedLineOffset < 0 {
		return
	}
	vpHeight := m.viewport.Height()
	offset := m.focusedLineOffset - vpHeight/3
	if offset < 0 {
		offset = 0
	}
	m.viewport.SetYOffset(offset)
}

// isBlockExpanded returns whether a block at the given index is expanded.
// Defaults: tool and assistant blocks start expanded, reasoning starts collapsed.
func (m *expertViewer) isBlockExpanded(idx int) bool {
	if m.blockExpanded == nil {
		m.blockExpanded = make(map[int]bool)
	}
	if val, ok := m.blockExpanded[idx]; ok {
		return val
	}
	// Default: reasoning and read_skill_sql collapsed, everything else expanded
	if idx >= 0 && idx < len(m.messages) {
		msg := m.messages[idx]
		if msg.Role == "reasoning" {
			return false
		}
		if msg.Role == "tool" && msg.ToolName == "read_skill_sql" {
			return false
		}
	}
	return true
}

// countNavigableBlocks returns the number of navigable blocks (reasoning, tool, assistant).
func (m *expertViewer) countNavigableBlocks() int {
	count := 0
	for _, msg := range m.messages {
		switch msg.Role {
		case "reasoning", "tool", "assistant":
			count++
		}
	}
	return count
}

// navigateBlocks moves the focus to the next/prev navigable block.
func (m *expertViewer) navigateBlocks(delta int) {
	blocks := m.collectNavigableIndices()
	if len(blocks) == 0 {
		m.blockFocusIdx = -1
		return
	}

	curPos := -1
	for i, idx := range blocks {
		if idx == m.blockFocusIdx {
			curPos = i
			break
		}
	}

	curPos += delta
	if curPos < 0 {
		curPos = len(blocks) - 1
	}
	if curPos >= len(blocks) {
		curPos = 0
	}

	m.blockFocusIdx = blocks[curPos]
}

// collectNavigableIndices returns indices of all navigable blocks.
func (m *expertViewer) collectNavigableIndices() []int {
	var result []int
	for i, msg := range m.messages {
		switch msg.Role {
		case "reasoning", "tool", "assistant":
			result = append(result, i)
		}
	}
	return result
}

// renderTSVTable renders tab-separated data as a formatted table with box-drawing borders.
func renderTSVTable(tsv string, maxWidth int) string {
	lines := strings.Split(strings.TrimRight(tsv, "\n"), "\n")
	if len(lines) == 0 {
		return tsv
	}

	// Parse rows
	var rows [][]string
	for _, line := range lines {
		if strings.HasPrefix(line, "...") {
			// Truncation marker — pass through
			rows = append(rows, []string{line})
			continue
		}
		rows = append(rows, strings.Split(line, "\t"))
	}

	if len(rows) == 0 {
		return tsv
	}

	// If first row has only 1 column and no tabs anywhere, it's not really TSV
	numCols := len(rows[0])
	if numCols <= 1 {
		// Fallback: plain text rendering
		var b strings.Builder
		for _, line := range lines {
			b.WriteString(expertToolResultStyle.Render("     " + line) + "\n")
		}
		return b.String()
	}

	// Calculate column widths
	widths := make([]int, numCols)
	for _, row := range rows {
		for j := 0; j < numCols && j < len(row); j++ {
			if len(row[j]) > widths[j] {
				widths[j] = len(row[j])
			}
		}
	}

	// Cap column widths to fit available space
	maxColWidth := 40
	if maxWidth > 0 {
		// Available = maxWidth - indent(5) - borders(numCols+1) - padding(numCols*2)
		available := maxWidth - 5 - (numCols + 1) - (numCols * 2)
		if available > 0 {
			perCol := available / numCols
			if perCol < maxColWidth {
				maxColWidth = perCol
			}
		}
	}
	if maxColWidth < 8 {
		maxColWidth = 8
	}
	for j := range widths {
		if widths[j] > maxColWidth {
			widths[j] = maxColWidth
		}
	}

	pad := func(s string, w int) string {
		if len(s) > w {
			return s[:w-1] + "~"
		}
		return s + strings.Repeat(" ", w-len(s))
	}

	borderRow := func(left, mid, right string) string {
		var b strings.Builder
		b.WriteString("     " + left)
		for j, w := range widths {
			b.WriteString(strings.Repeat("\u2500", w+2))
			if j < numCols-1 {
				b.WriteString(mid)
			}
		}
		b.WriteString(right + "\n")
		return b.String()
	}

	dataRow := func(row []string) string {
		var b strings.Builder
		b.WriteString("     \u2502")
		for j := 0; j < numCols; j++ {
			cell := ""
			if j < len(row) {
				cell = row[j]
			}
			b.WriteString(" " + pad(cell, widths[j]) + " \u2502")
		}
		b.WriteString("\n")
		return b.String()
	}

	var out strings.Builder

	// Top border
	out.WriteString(borderRow("\u250c", "\u252c", "\u2510"))

	// Header row (first row)
	out.WriteString(dataRow(rows[0]))

	// Header separator
	out.WriteString(borderRow("\u251c", "\u253c", "\u2524"))

	// Data rows
	for _, row := range rows[1:] {
		if len(row) == 1 && strings.HasPrefix(row[0], "...") {
			out.WriteString(expertToolResultStyle.Render("     "+row[0]) + "\n")
			continue
		}
		out.WriteString(dataRow(row))
	}

	// Bottom border
	out.WriteString(borderRow("\u2514", "\u2534", "\u2518"))

	return out.String()
}

// estimateReasoningTokens estimates token count from text length (~4 chars/token for English).
func estimateReasoningTokens(text string) int {
	if len(text) == 0 {
		return 0
	}
	// Rough estimate: ~4 chars per token for mixed English/other languages
	tokens := len(text) / 4
	if tokens < 10 {
		return 10
	}
	return tokens
}

