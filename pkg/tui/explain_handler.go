package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	"github.com/Slach/clickhouse-timeline/pkg/utils"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/evertras/bubble-table/table"
	"github.com/rs/zerolog/log"
)

// Explain stages
type explainStage string

const (
	stageFilter      explainStage = "filter"
	stageQueryList   explainStage = "query_list"
	stagePercentiles explainStage = "percentiles"
	stageResults     explainStage = "results"
)

// ExplainDataMsg messages
type ExplainOptionsLoadedMsg struct {
	Tables []string
	Kinds  []string
	Err    error
}

type ExplainQueriesLoadedMsg struct {
	Queries []QueryRow
	Err     error
}

type ExplainPercentilesMsg struct {
	Hash  string
	Query string
	P50   float64
	P90   float64
	P99   float64
	Err   error
}

type ExplainResultsMsg struct {
	QueryText   string
	Duration    float64
	ExplainPlan string
	ExplainPipe string
	ExplainEst  string
	Err         error
}

type QueryRow struct {
	Hash  string
	Query string
}

// explainViewer is a bubbletea model for EXPLAIN display
type explainViewer struct {
	stage       explainStage
	hashInput   textinput.Model
	tablesList  widgets.FilteredTable
	kindsList   widgets.FilteredTable
	queriesList widgets.FilteredTable

	selectedTables map[string]bool
	selectedKinds  map[string]bool

	allTables  []string
	allKinds   []string
	allQueries []QueryRow

	// Cursor positions for lists
	tablesCursor int
	kindsCursor  int

	// Filtering
	tablesFilter string
	kindsFilter  string

	// Percentiles
	currentHash        string
	currentQuery       string
	p50, p90, p99      float64
	selectedPercentile int // 0=p50, 1=p90, 2=p99, -1=back

	// Results
	queryText     string
	duration      float64
	explainPlan   viewport.Model
	explainPipe   viewport.Model
	explainEst    viewport.Model
	focusedResult int // 0=plan, 1=pipe, 2=est

	loading      bool
	err          error
	fromTime     time.Time
	toTime       time.Time
	cluster      string
	categoryType CategoryType
	prefillHash  string
	width        int
	height       int
	focusedItem  int // Which UI element is focused: 0=hash, 1=tables, 2=kinds, 3=show button, 4=cancel button

	// Command function references (set by App)
	searchQueriesFn      func(explainViewer) tea.Cmd
	loadPercentilesFn    func(string, string, time.Time, time.Time, string) tea.Cmd
	loadExplainResultsFn func(string, float64, time.Time, time.Time, string) tea.Cmd
}

func newExplainViewer(categoryType CategoryType, prefillHash string, fromTime, toTime time.Time, cluster string, width, height int) explainViewer {
	// Hash input
	hashInput := textinput.New()
	hashInput.Placeholder = "normalized_query_hash (optional)"
	hashInput.CharLimit = 100
	hashInput.Width = width - 20 // Make it responsive to terminal width
	if prefillHash != "" {
		hashInput.SetValue(prefillHash)
	}
	hashInput.Focus()

	// Tables list
	tablesList := widgets.NewFilteredTable("Tables", []string{"Table"}, width/3, height-10)

	// Kinds list
	kindsList := widgets.NewFilteredTable("Query Kinds", []string{"Kind"}, width/3, height-10)

	// Queries list with custom column widths
	// Hash column: 20 chars (typical ClickHouse hash is 16 hex chars)
	// Query column: remaining space
	hashWidth := 20
	queryWidth := width - hashWidth - 10 // Account for borders and padding
	if queryWidth < 40 {
		queryWidth = 40
	}
	queriesList := widgets.NewFilteredTableBubbleWithWidths(
		"Queries",
		[]string{"Hash", "Query"},
		[]int{hashWidth, queryWidth},
		width,
		height-8,
	)

	// Result viewports
	vpHeight := (height - 10) / 3
	explainPlan := viewport.New(width-4, vpHeight)
	explainPipe := viewport.New(width-4, vpHeight)
	explainEst := viewport.New(width-4, vpHeight)

	viewer := explainViewer{
		stage:              stageFilter,
		hashInput:          hashInput,
		tablesList:         tablesList,
		kindsList:          kindsList,
		queriesList:        queriesList,
		selectedTables:     make(map[string]bool),
		selectedKinds:      make(map[string]bool),
		allQueries:         []QueryRow{},
		explainPlan:        explainPlan,
		explainPipe:        explainPipe,
		explainEst:         explainEst,
		loading:            true,
		fromTime:           fromTime,
		toTime:             toTime,
		cluster:            cluster,
		categoryType:       categoryType,
		prefillHash:        prefillHash,
		width:              width,
		height:             height,
		focusedItem:        0,
		selectedPercentile: -1,
		focusedResult:      0,
	}

	return viewer
}

func (m explainViewer) Init() tea.Cmd {
	return textinput.Blink
}

func (m explainViewer) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case ExplainOptionsLoadedMsg:
		m.loading = false
		if msg.Err != nil {
			m.err = msg.Err
			return m, nil
		}

		m.allTables = msg.Tables
		m.allKinds = msg.Kinds

		// Populate tables list
		var tableRows []table.Row
		for _, t := range msg.Tables {
			tableRows = append(tableRows, table.NewRow(table.RowData{
				"Table": t,
			}))
		}
		m.tablesList.SetRows(tableRows)

		// Populate kinds list
		var kindRows []table.Row
		for _, k := range msg.Kinds {
			kindRows = append(kindRows, table.NewRow(table.RowData{
				"Kind": k,
			}))
		}
		m.kindsList.SetRows(kindRows)

		return m, nil

	case ExplainQueriesLoadedMsg:
		m.loading = false
		if msg.Err != nil {
			m.err = msg.Err
			return m, nil
		}

		m.allQueries = msg.Queries
		m.stage = stageQueryList

		// Populate queries list
		var queryRows []table.Row
		for _, q := range msg.Queries {
			queryRows = append(queryRows, table.NewRow(table.RowData{
				"Hash":  q.Hash,
				"Query": q.Query, // Show full query without truncation
			}))
		}
		m.queriesList.SetRows(queryRows)

		return m, nil

	case ExplainPercentilesMsg:
		m.loading = false
		if msg.Err != nil {
			m.err = msg.Err
			return m, nil
		}

		m.currentHash = msg.Hash
		m.currentQuery = msg.Query
		m.p50 = msg.P50
		m.p90 = msg.P90
		m.p99 = msg.P99
		m.stage = stagePercentiles
		m.selectedPercentile = 0

		return m, nil

	case ExplainResultsMsg:
		m.loading = false
		if msg.Err != nil {
			m.err = msg.Err
			return m, nil
		}

		m.queryText = msg.QueryText
		m.duration = msg.Duration
		m.explainPlan.SetContent(msg.ExplainPlan)
		m.explainPipe.SetContent(msg.ExplainPipe)
		m.explainEst.SetContent(msg.ExplainEst)
		m.stage = stageResults
		m.focusedResult = 0

		return m, nil

	case tea.KeyMsg:
		switch m.stage {
		case stageFilter:
			return m.handleFilterKeys(msg)
		case stageQueryList:
			return m.handleQueryListKeys(msg)
		case stagePercentiles:
			return m.handlePercentilesKeys(msg)
		case stageResults:
			return m.handleResultsKeys(msg)
		}
	}

	// Update focused component based on stage
	switch m.stage {
	case stageFilter:
		if m.focusedItem == 0 {
			m.hashInput, cmd = m.hashInput.Update(msg)
			cmds = append(cmds, cmd)
		} else if m.focusedItem == 1 {
			m.tablesList, cmd = m.tablesList.Update(msg)
			cmds = append(cmds, cmd)
		} else if m.focusedItem == 2 {
			m.kindsList, cmd = m.kindsList.Update(msg)
			cmds = append(cmds, cmd)
		}
	case stageQueryList:
		m.queriesList, cmd = m.queriesList.Update(msg)
		cmds = append(cmds, cmd)
	case stageResults:
		switch m.focusedResult {
		case 0:
			m.explainPlan, cmd = m.explainPlan.Update(msg)
		case 1:
			m.explainPipe, cmd = m.explainPipe.Update(msg)
		case 2:
			m.explainEst, cmd = m.explainEst.Update(msg)
		}
		cmds = append(cmds, cmd)
	}

	return m, tea.Batch(cmds...)
}

func (m explainViewer) handleFilterKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q":
		// Clear filter if active, otherwise exit
		if m.tablesFilter != "" && m.focusedItem == 1 {
			m.tablesFilter = ""
			m.tablesCursor = 0
			return m, nil
		}
		if m.kindsFilter != "" && m.focusedItem == 2 {
			m.kindsFilter = ""
			m.kindsCursor = 0
			return m, nil
		}
		return m, func() tea.Msg {
			return tea.KeyMsg{Type: tea.KeyEsc}
		}
	case "tab":
		m.focusedItem = (m.focusedItem + 1) % 5 // 0=hash, 1=tables, 2=kinds, 3=show button, 4=cancel button
		if m.focusedItem == 0 {
			m.hashInput.Focus()
		} else {
			m.hashInput.Blur()
		}
		return m, textinput.Blink
	case "shift+tab":
		m.focusedItem--
		if m.focusedItem < 0 {
			m.focusedItem = 4
		}
		if m.focusedItem == 0 {
			m.hashInput.Focus()
		} else {
			m.hashInput.Blur()
		}
		return m, textinput.Blink
	case "up":
		filteredTables := m.getFilteredTables()
		filteredKinds := m.getFilteredKinds()
		if m.focusedItem == 1 && len(filteredTables) > 0 {
			// Move cursor up in tables list
			m.tablesCursor--
			if m.tablesCursor < 0 {
				m.tablesCursor = len(filteredTables) - 1
			}
		} else if m.focusedItem == 2 && len(filteredKinds) > 0 {
			// Move cursor up in kinds list
			m.kindsCursor--
			if m.kindsCursor < 0 {
				m.kindsCursor = len(filteredKinds) - 1
			}
		}
		return m, nil
	case "down":
		filteredTables := m.getFilteredTables()
		filteredKinds := m.getFilteredKinds()
		if m.focusedItem == 1 && len(filteredTables) > 0 {
			// Move cursor down in tables list
			m.tablesCursor++
			if m.tablesCursor >= len(filteredTables) {
				m.tablesCursor = 0
			}
		} else if m.focusedItem == 2 && len(filteredKinds) > 0 {
			// Move cursor down in kinds list
			m.kindsCursor++
			if m.kindsCursor >= len(filteredKinds) {
				m.kindsCursor = 0
			}
		}
		return m, nil
	case "enter", " ":
		filteredTables := m.getFilteredTables()
		filteredKinds := m.getFilteredKinds()
		if m.focusedItem == 1 && len(filteredTables) > 0 {
			// Toggle table selection at cursor
			tableName := filteredTables[m.tablesCursor]
			m.selectedTables[tableName] = !m.selectedTables[tableName]
		} else if m.focusedItem == 2 && len(filteredKinds) > 0 {
			// Toggle kind selection at cursor
			kindName := filteredKinds[m.kindsCursor]
			m.selectedKinds[kindName] = !m.selectedKinds[kindName]
		} else if m.focusedItem == 3 {
			// Show Explain button - search for queries
			m.loading = true
			return m, m.searchQueriesCmd()
		} else if m.focusedItem == 4 {
			// Cancel button - exit
			return m, func() tea.Msg {
				return tea.KeyMsg{Type: tea.KeyEsc}
			}
		}
		return m, nil
	case "backspace":
		// Handle backspace in filter mode
		if m.focusedItem == 1 && len(m.tablesFilter) > 0 {
			m.tablesFilter = m.tablesFilter[:len(m.tablesFilter)-1]
			m.tablesCursor = 0
			return m, nil
		}
		if m.focusedItem == 2 && len(m.kindsFilter) > 0 {
			m.kindsFilter = m.kindsFilter[:len(m.kindsFilter)-1]
			m.kindsCursor = 0
			return m, nil
		}
	default:
		// Check if it's a printable character for filtering
		if len(msg.String()) == 1 {
			r := []rune(msg.String())[0]
			if r >= 32 && r <= 126 { // Printable ASCII
				if m.focusedItem == 1 {
					m.tablesFilter += msg.String()
					m.tablesCursor = 0
					return m, nil
				} else if m.focusedItem == 2 {
					m.kindsFilter += msg.String()
					m.kindsCursor = 0
					return m, nil
				}
			}
		}
	}
	return m, nil
}

func (m explainViewer) handleQueryListKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q":
		// Go back to filter stage
		m.stage = stageFilter
		return m, nil
	case "enter":
		// Load percentiles for selected query
		selectedRow := m.queriesList.HighlightedRow()
		if selectedRow.Data != nil {
			hash := selectedRow.Data["Hash"].(string)
			// Find the full query from allQueries
			for _, q := range m.allQueries {
				if q.Hash == hash {
					m.loading = true
					return m, m.loadPercentilesCmd(q.Hash, q.Query)
				}
			}
		}
		return m, nil
	}

	// Pass all other keys (including arrow keys) to the table for navigation
	var cmd tea.Cmd
	m.queriesList, cmd = m.queriesList.Update(msg)
	return m, cmd
}

func (m explainViewer) handlePercentilesKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q":
		// Go back to query list
		m.stage = stageQueryList
		return m, nil
	case "up":
		m.selectedPercentile--
		if m.selectedPercentile < -1 {
			m.selectedPercentile = 2
		}
	case "down":
		m.selectedPercentile++
		if m.selectedPercentile > 2 {
			m.selectedPercentile = -1
		}
	case "enter":
		if m.selectedPercentile == -1 {
			// Back to query list
			m.stage = stageQueryList
			return m, nil
		}
		// Load EXPLAIN for selected percentile
		var threshold float64
		switch m.selectedPercentile {
		case 0:
			threshold = m.p50
		case 1:
			threshold = m.p90
		case 2:
			threshold = m.p99
		}
		m.loading = true
		return m, m.loadExplainResultsCmd(m.currentHash, threshold)
	}
	return m, nil
}

func (m explainViewer) handleResultsKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q":
		// Go back to percentiles
		m.stage = stagePercentiles
		return m, nil
	case "tab":
		m.focusedResult = (m.focusedResult + 1) % 3
	case "shift+tab":
		m.focusedResult--
		if m.focusedResult < 0 {
			m.focusedResult = 2
		}
	}
	return m, nil
}

func (m explainViewer) View() string {
	if m.loading {
		return lipgloss.NewStyle().
			Width(m.width).
			Height(m.height).
			Align(lipgloss.Center, lipgloss.Center).
			Render("Loading...")
	}

	if m.err != nil {
		return lipgloss.NewStyle().
			Width(m.width).
			Height(m.height).
			Foreground(lipgloss.Color("1")).
			Render(fmt.Sprintf("Error: %v\n\nPress ESC to go back", m.err))
	}

	switch m.stage {
	case stageFilter:
		return m.viewFilter()
	case stageQueryList:
		return m.viewQueryList()
	case stagePercentiles:
		return m.viewPercentiles()
	case stageResults:
		return m.viewResults()
	}

	return ""
}

func (m explainViewer) viewFilter() string {
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))

	var sb strings.Builder
	sb.WriteString(titleStyle.Render("EXPLAIN Query - Filter"))
	sb.WriteString("\n")

	// Hash input with proper box styling
	inputLabel := "Hash: "
	focusIndicator := ""
	borderColor := lipgloss.Color("8")

	if m.focusedItem == 0 {
		focusIndicator = lipgloss.NewStyle().Foreground(lipgloss.Color("10")).Bold(true).Render("▶ ")
		borderColor = lipgloss.Color("10")
	} else {
		focusIndicator = "  "
	}

	inputBox := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(borderColor).
		Padding(0, 1).
		Width(m.width - 10).
		Render(m.hashInput.View())

	sb.WriteString(focusIndicator + inputLabel)
	sb.WriteString("\n")
	sb.WriteString(inputBox)
	sb.WriteString("\n")

	// Calculate list height to fit viewport
	// Total height - title(1) - hash label(1) - hash box(3) - buttons(1) - help(1) - margins(3) = height - 10
	listHeight := m.height - 10
	if listHeight < 5 {
		listHeight = 5
	}

	// Tables and Kinds side by side with same height
	tablesView := m.renderTablesList(listHeight)
	kindsView := m.renderKindsList(listHeight)

	listsStyle := lipgloss.NewStyle().Width(m.width)
	sb.WriteString(listsStyle.Render(
		lipgloss.JoinHorizontal(lipgloss.Top, tablesView, kindsView),
	))

	sb.WriteString("\n")

	// Buttons
	showBtn := "[ Show Explain ]"
	cancelBtn := "[ Cancel ]"

	if m.focusedItem == 3 {
		showBtn = lipgloss.NewStyle().
			Foreground(lipgloss.Color("0")).
			Background(lipgloss.Color("10")).
			Bold(true).
			Render("[ Show Explain ]")
	} else {
		showBtn = lipgloss.NewStyle().
			Foreground(lipgloss.Color("15")).
			Render(showBtn)
	}

	if m.focusedItem == 4 {
		cancelBtn = lipgloss.NewStyle().
			Foreground(lipgloss.Color("0")).
			Background(lipgloss.Color("9")).
			Bold(true).
			Render("[ Cancel ]")
	} else {
		cancelBtn = lipgloss.NewStyle().
			Foreground(lipgloss.Color("15")).
			Render(cancelBtn)
	}

	buttonsView := lipgloss.JoinHorizontal(lipgloss.Center, showBtn, "  ", cancelBtn)
	sb.WriteString(lipgloss.NewStyle().Width(m.width).Align(lipgloss.Center).Render(buttonsView))
	sb.WriteString("\n")

	// Help
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	sb.WriteString(helpStyle.Render("Tab/Shift+Tab: Switch  |  ↑↓: Navigate  |  Space/Enter: Select  |  Esc: Exit"))

	return sb.String()
}

func (m explainViewer) getFilteredTables() []string {
	if m.tablesFilter == "" {
		return m.allTables
	}

	var filtered []string
	filterLower := strings.ToLower(m.tablesFilter)
	for _, t := range m.allTables {
		if strings.Contains(strings.ToLower(t), filterLower) {
			filtered = append(filtered, t)
		}
	}
	return filtered
}

func (m explainViewer) getFilteredKinds() []string {
	if m.kindsFilter == "" {
		return m.allKinds
	}

	var filtered []string
	filterLower := strings.ToLower(m.kindsFilter)
	for _, k := range m.allKinds {
		if strings.Contains(strings.ToLower(k), filterLower) {
			filtered = append(filtered, k)
		}
	}
	return filtered
}

func (m explainViewer) renderTablesList(listHeight int) string {
	filteredTables := m.getFilteredTables()

	var items []string
	for i, t := range filteredTables {
		checkbox := "[ ]"
		if m.selectedTables[t] {
			checkbox = "[✓]"
		}

		line := fmt.Sprintf("%s %s", checkbox, t)

		// Highlight cursor position when focused
		if m.focusedItem == 1 && i == m.tablesCursor {
			line = lipgloss.NewStyle().
				Foreground(lipgloss.Color("0")).
				Background(lipgloss.Color("14")).
				Bold(true).
				Render("▶ " + line)
		} else {
			line = "  " + line
		}

		items = append(items, line)
	}

	// Title with filter indicator
	title := "Tables"
	if m.tablesFilter != "" {
		title = fmt.Sprintf("Tables (filter: %s)", m.tablesFilter)
	}

	titleStyle := lipgloss.NewStyle().Bold(true)
	borderColor := lipgloss.Color("8")
	if m.focusedItem == 1 {
		titleStyle = titleStyle.Foreground(lipgloss.Color("10"))
		borderColor = lipgloss.Color("10")
	}

	content := strings.Join(items, "\n")
	if content == "" {
		if m.tablesFilter != "" {
			content = "  No matches"
		} else {
			content = "  No tables found"
		}
	}

	// Ensure content has exactly the right number of lines to fill the box
	// Border takes 2 lines, padding takes 2 lines = 4 lines overhead
	contentLines := strings.Split(content, "\n")
	targetLines := listHeight - 4
	if targetLines < 1 {
		targetLines = 1
	}

	// Pad or truncate to exact target
	if len(contentLines) < targetLines {
		for len(contentLines) < targetLines {
			contentLines = append(contentLines, "")
		}
	} else if len(contentLines) > targetLines {
		contentLines = contentLines[:targetLines]
	}
	content = strings.Join(contentLines, "\n")

	boxStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(borderColor).
		Padding(1, 1).
		Width(m.width/2 - 2).
		Height(listHeight)

	log.Debug().Int("listHeight", listHeight).Int("contentLines", len(contentLines)).Int("targetLines", targetLines).Msg("renderTablesList")

	return titleStyle.Render(title) + "\n" + boxStyle.Render(content)
}

func (m explainViewer) renderKindsList(listHeight int) string {
	filteredKinds := m.getFilteredKinds()

	var items []string
	for i, k := range filteredKinds {
		checkbox := "[ ]"
		if m.selectedKinds[k] {
			checkbox = "[✓]"
		}

		line := fmt.Sprintf("%s %s", checkbox, k)

		// Highlight cursor position when focused
		if m.focusedItem == 2 && i == m.kindsCursor {
			line = lipgloss.NewStyle().
				Foreground(lipgloss.Color("0")).
				Background(lipgloss.Color("14")).
				Bold(true).
				Render("▶ " + line)
		} else {
			line = "  " + line
		}

		items = append(items, line)
	}

	// Title with filter indicator
	title := "Query Kinds"
	if m.kindsFilter != "" {
		title = fmt.Sprintf("Query Kinds (filter: %s)", m.kindsFilter)
	}

	titleStyle := lipgloss.NewStyle().Bold(true)
	borderColor := lipgloss.Color("8")
	if m.focusedItem == 2 {
		titleStyle = titleStyle.Foreground(lipgloss.Color("10"))
		borderColor = lipgloss.Color("10")
	}

	content := strings.Join(items, "\n")
	if content == "" {
		if m.kindsFilter != "" {
			content = "  No matches"
		} else {
			content = "  No query kinds found"
		}
	}

	// Ensure content has exactly the right number of lines to fill the box
	// Border takes 2 lines, padding takes 2 lines = 4 lines overhead
	contentLines := strings.Split(content, "\n")
	targetLines := listHeight - 4
	if targetLines < 1 {
		targetLines = 1
	}

	// Pad or truncate to exact target
	if len(contentLines) < targetLines {
		for len(contentLines) < targetLines {
			contentLines = append(contentLines, "")
		}
	} else if len(contentLines) > targetLines {
		contentLines = contentLines[:targetLines]
	}
	content = strings.Join(contentLines, "\n")

	boxStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(borderColor).
		Padding(1, 1).
		Width(m.width/2 - 2).
		Height(listHeight)

	log.Debug().Int("listHeight", listHeight).Int("contentLines", len(contentLines)).Int("targetLines", targetLines).Msg("renderKindsList")

	return titleStyle.Render(title) + "\n" + boxStyle.Render(content)
}

func (m explainViewer) viewQueryList() string {
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))

	var sb strings.Builder
	sb.WriteString(titleStyle.Render("Queries (Enter to inspect)"))
	sb.WriteString("\n\n")
	sb.WriteString(m.queriesList.View())
	sb.WriteString("\n")
	sb.WriteString(helpStyle.Render("Enter: Select query  |  Esc: Back to filter"))

	return sb.String()
}

func (m explainViewer) viewPercentiles() string {
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))

	var sb strings.Builder
	sb.WriteString(titleStyle.Render("Percentiles for Query"))
	sb.WriteString("\n\n")
	sb.WriteString(fmt.Sprintf("Query: %s\n\n", truncate(m.currentQuery, 100)))

	// Percentile options
	options := []string{
		fmt.Sprintf("p50: %.2f ms", m.p50),
		fmt.Sprintf("p90: %.2f ms", m.p90),
		fmt.Sprintf("p99: %.2f ms", m.p99),
		"Back",
	}

	for i, opt := range options {
		if i == m.selectedPercentile+1 {
			sb.WriteString("> ")
		} else {
			sb.WriteString("  ")
		}
		sb.WriteString(opt)
		sb.WriteString("\n")
	}

	sb.WriteString("\n")
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	sb.WriteString(helpStyle.Render("↑↓: Navigate  |  Enter: Select  |  Esc: Back"))

	boxStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("6")).
		Padding(2, 4).
		Width(60)

	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, boxStyle.Render(sb.String()))
}

func (m explainViewer) viewResults() string {
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))

	var sb strings.Builder
	sb.WriteString(titleStyle.Render("EXPLAIN Results"))
	sb.WriteString("\n\n")
	sb.WriteString(fmt.Sprintf("Query: %s\n", truncate(m.queryText, 120)))
	sb.WriteString(fmt.Sprintf("Duration: %.2f ms\n\n", m.duration))

	// Three explain outputs stacked vertically
	planTitle := "EXPLAIN PLAN indexes=1, projections=1"
	if m.focusedResult == 0 {
		planTitle = "> " + planTitle
	}
	sb.WriteString(lipgloss.NewStyle().Bold(true).Render(planTitle))
	sb.WriteString("\n")
	sb.WriteString(m.renderViewport(m.explainPlan))
	sb.WriteString("\n")

	pipeTitle := "EXPLAIN PIPELINE"
	if m.focusedResult == 1 {
		pipeTitle = "> " + pipeTitle
	}
	sb.WriteString(lipgloss.NewStyle().Bold(true).Render(pipeTitle))
	sb.WriteString("\n")
	sb.WriteString(m.renderViewport(m.explainPipe))
	sb.WriteString("\n")

	estTitle := "EXPLAIN ESTIMATE"
	if m.focusedResult == 2 {
		estTitle = "> " + estTitle
	}
	sb.WriteString(lipgloss.NewStyle().Bold(true).Render(estTitle))
	sb.WriteString("\n")
	sb.WriteString(m.renderViewport(m.explainEst))
	sb.WriteString("\n")

	sb.WriteString(helpStyle.Render("Tab: Switch view  |  ↑↓: Scroll  |  Esc: Back"))

	return sb.String()
}

func (m explainViewer) renderViewport(vp viewport.Model) string {
	boxStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("8")).
		Padding(0, 1)
	return boxStyle.Render(vp.View())
}

// Command functions

func (m explainViewer) searchQueriesCmd() tea.Cmd {
	if m.searchQueriesFn != nil {
		return m.searchQueriesFn(m)
	}
	return func() tea.Msg {
		return ExplainQueriesLoadedMsg{Err: fmt.Errorf("searchQueriesFn not set")}
	}
}

func (m explainViewer) loadPercentilesCmd(hash, query string) tea.Cmd {
	if m.loadPercentilesFn != nil {
		return m.loadPercentilesFn(hash, query, m.fromTime, m.toTime, m.cluster)
	}
	return func() tea.Msg {
		return ExplainPercentilesMsg{Err: fmt.Errorf("loadPercentilesFn not set")}
	}
}

func (m explainViewer) loadExplainResultsCmd(hash string, threshold float64) tea.Cmd {
	if m.loadExplainResultsFn != nil {
		return m.loadExplainResultsFn(hash, threshold, m.fromTime, m.toTime, m.cluster)
	}
	return func() tea.Msg {
		return ExplainResultsMsg{Err: fmt.Errorf("loadExplainResultsFn not set")}
	}
}

// truncate utility
func truncate(s string, l int) string {
	if len(s) <= l {
		return s
	}
	return s[:l-3] + "..."
}

// ShowExplain is the entry point for the explain flow
func (a *App) ShowExplain(categoryType CategoryType, categoryValue string, fromTime, toTime time.Time, cluster string) tea.Cmd {
	if a.state.ClickHouse == nil {
		a.SwitchToMainPage("Error: Please connect to a ClickHouse instance first using :connect command")
		return nil
	}
	if cluster == "" {
		a.SwitchToMainPage("Error: Please select a cluster first using :cluster command")
		return nil
	}

	// Determine prefill hash
	prefillHash := ""
	if categoryType == CategoryQueryHash {
		prefillHash = categoryValue
	}

	// Create and show viewer
	viewer := newExplainViewer(categoryType, prefillHash, fromTime, toTime, cluster, a.width, a.height)

	// Wire up command functions
	viewer.searchQueriesFn = a.searchExplainQueriesCmd
	viewer.loadPercentilesFn = a.loadExplainPercentilesCmd
	viewer.loadExplainResultsFn = a.loadExplainResultsCmd

	a.explainHandler = viewer
	a.currentPage = pageExplain

	// Start by loading options (tables and kinds)
	return a.loadExplainOptionsCmd(prefillHash, fromTime, toTime, cluster)
}

// loadExplainOptionsCmd loads tables and query kinds
func (a *App) loadExplainOptionsCmd(hashFilter string, fromTime, toTime time.Time, cluster string) tea.Cmd {
	return func() tea.Msg {
		fromStr := fromTime.Format("2006-01-02 15:04:05 -07:00")
		toStr := toTime.Format("2006-01-02 15:04:05 -07:00")

		var hashWhere string
		if hashFilter != "" {
			hashWhere = fmt.Sprintf("AND normalized_query_hash = '%s'", strings.ReplaceAll(hashFilter, "'", "''"))
		}

		// Query for tables
		tablesQuery := fmt.Sprintf(
			"SELECT DISTINCT arrayJoin(tables) AS t FROM clusterAllReplicas('%s', merge(system,'^query_log')) WHERE event_date >= toDate(parseDateTimeBestEffort('%s')) AND event_date <= toDate(parseDateTimeBestEffort('%s')) AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s') %s ORDER BY t",
			cluster, fromStr, toStr, fromStr, toStr, hashWhere,
		)
		rows, err := a.state.ClickHouse.Query(tablesQuery)
		if err != nil {
			return ExplainOptionsLoadedMsg{Err: err}
		}
		defer rows.Close()

		var tables []string
		for rows.Next() {
			var t string
			if err := rows.Scan(&t); err != nil {
				log.Error().Err(err).Msg("scan table")
				continue
			}
			tables = append(tables, t)
		}

		// Query for query_kind
		kindQuery := fmt.Sprintf(
			"SELECT DISTINCT query_kind FROM clusterAllReplicas('%s', merge(system,'^query_log')) WHERE event_date >= toDate(parseDateTimeBestEffort('%s')) AND event_date <= toDate(parseDateTimeBestEffort('%s')) AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s') %s ORDER BY query_kind",
			cluster, fromStr, toStr, fromStr, toStr, hashWhere,
		)
		kindRows, err := a.state.ClickHouse.Query(kindQuery)
		if err != nil {
			return ExplainOptionsLoadedMsg{Err: err}
		}
		defer kindRows.Close()

		var kinds []string
		for kindRows.Next() {
			var k string
			if err := kindRows.Scan(&k); err != nil {
				log.Error().Err(err).Msg("scan kind")
				continue
			}
			kinds = append(kinds, k)
		}

		return ExplainOptionsLoadedMsg{Tables: tables, Kinds: kinds}
	}
}

// searchExplainQueriesCmd searches for queries based on filters
func (a *App) searchExplainQueriesCmd(viewer explainViewer) tea.Cmd {
	return func() tea.Msg {
		fromStr := viewer.fromTime.Format("2006-01-02 15:04:05 -07:00")
		toStr := viewer.toTime.Format("2006-01-02 15:04:05 -07:00")

		// Build filters
		hashVal := strings.TrimSpace(viewer.hashInput.Value())
		var whereParts []string
		whereParts = append(whereParts, fmt.Sprintf("event_date >= toDate(parseDateTimeBestEffort('%s'))", fromStr))
		whereParts = append(whereParts, fmt.Sprintf("event_date <= toDate(parseDateTimeBestEffort('%s'))", toStr))
		whereParts = append(whereParts, fmt.Sprintf("event_time >= parseDateTimeBestEffort('%s')", fromStr))
		whereParts = append(whereParts, fmt.Sprintf("event_time <= parseDateTimeBestEffort('%s')", toStr))
		whereParts = append(whereParts, "type != 'QueryStart'")

		if hashVal != "" {
			whereParts = append(whereParts, fmt.Sprintf("normalized_query_hash = '%s'", strings.ReplaceAll(hashVal, "'", "''")))
		}

		// Filter by selected tables
		var chosenTables []string
		for t, sel := range viewer.selectedTables {
			if sel {
				chosenTables = append(chosenTables, t)
			}
		}
		if len(chosenTables) > 0 {
			escaped := make([]string, 0, len(chosenTables))
			for _, tt := range chosenTables {
				escaped = append(escaped, fmt.Sprintf("'%s'", strings.ReplaceAll(tt, "'", "''")))
			}
			whereParts = append(whereParts, fmt.Sprintf("hasAny(tables, [%s])", strings.Join(escaped, ",")))
		}

		// Filter by selected query kinds
		var chosenKinds []string
		for k, sel := range viewer.selectedKinds {
			if sel {
				chosenKinds = append(chosenKinds, k)
			}
		}
		if len(chosenKinds) > 0 {
			escaped := make([]string, 0, len(chosenKinds))
			for _, kk := range chosenKinds {
				escaped = append(escaped, fmt.Sprintf("'%s'", strings.ReplaceAll(kk, "'", "''")))
			}
			whereParts = append(whereParts, fmt.Sprintf("query_kind IN (%s)", strings.Join(escaped, ",")))
		}

		whereClause := strings.Join(whereParts, " AND ")

		query := fmt.Sprintf(
			"SELECT DISTINCT normalized_query_hash, normalizeQuery(query) AS q FROM clusterAllReplicas('%s', merge(system,'^query_log')) WHERE %s ORDER BY normalized_query_hash",
			viewer.cluster, whereClause,
		)

		rows, err := a.state.ClickHouse.Query(query)
		if err != nil {
			return ExplainQueriesLoadedMsg{Err: err}
		}
		defer rows.Close()

		var queries []QueryRow
		for rows.Next() {
			var h, q string
			if err := rows.Scan(&h, &q); err != nil {
				log.Error().Err(err).Msg("scan query row")
				continue
			}
			queries = append(queries, QueryRow{Hash: h, Query: q})
		}

		return ExplainQueriesLoadedMsg{Queries: queries}
	}
}

// loadExplainPercentilesCmd loads percentiles for a query
func (a *App) loadExplainPercentilesCmd(hash, query string, fromTime, toTime time.Time, cluster string) tea.Cmd {
	return func() tea.Msg {
		fromStr := fromTime.Format("2006-01-02 15:04:05 -07:00")
		toStr := toTime.Format("2006-01-02 15:04:05 -07:00")

		q := fmt.Sprintf("SELECT quantile(0.5)(query_duration_ms) AS p50, quantile(0.9)(query_duration_ms) AS p90, quantile(0.99)(query_duration_ms) AS p99 FROM clusterAllReplicas('%s', merge(system,'^query_log')) WHERE event_date >= toDate(parseDateTimeBestEffort('%s')) AND event_date <= toDate(parseDateTimeBestEffort('%s')) AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s') AND normalized_query_hash = '%s' AND query_duration_ms > 0",
			cluster, fromStr, toStr, fromStr, toStr, strings.ReplaceAll(hash, "'", "''"),
		)

		rows, err := a.state.ClickHouse.Query(q)
		if err != nil {
			return ExplainPercentilesMsg{Err: err}
		}
		defer rows.Close()

		var p50, p90, p99 float64
		if rows.Next() {
			if err := rows.Scan(&p50, &p90, &p99); err != nil {
				return ExplainPercentilesMsg{Err: err}
			}
		}

		return ExplainPercentilesMsg{Hash: hash, Query: query, P50: p50, P90: p90, P99: p99}
	}
}

// loadExplainResultsCmd loads EXPLAIN results for a query
func (a *App) loadExplainResultsCmd(hash string, threshold float64, fromTime, toTime time.Time, cluster string) tea.Cmd {
	return func() tea.Msg {
		fromStr := fromTime.Format("2006-01-02 15:04:05 -07:00")
		toStr := toTime.Format("2006-01-02 15:04:05 -07:00")

		// Get top query above threshold
		q := fmt.Sprintf("SELECT query, query_duration_ms FROM clusterAllReplicas('%s', merge(system,'^query_log')) WHERE event_date >= toDate(parseDateTimeBestEffort('%s')) AND event_date <= toDate(parseDateTimeBestEffort('%s')) AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s') AND normalized_query_hash = '%s' AND query_duration_ms <= %f ORDER BY query_duration_ms DESC LIMIT 1",
			cluster, fromStr, toStr, fromStr, toStr, strings.ReplaceAll(hash, "'", "''"), threshold,
		)

		rows, err := a.state.ClickHouse.Query(q)
		if err != nil {
			return ExplainResultsMsg{Err: err}
		}
		defer rows.Close()

		var queryText string
		var duration float64
		if !rows.Next() {
			return ExplainResultsMsg{Err: fmt.Errorf("no query found for hash %s at threshold %.2f", hash, threshold)}
		}

		if err := rows.Scan(&queryText, &duration); err != nil {
			return ExplainResultsMsg{Err: err}
		}

		// Build explain queries
		explain1 := fmt.Sprintf("EXPLAIN PLAN indexes=1, projections=1 %s", queryText)
		explain2 := fmt.Sprintf("EXPLAIN PIPELINE %s", queryText)
		explain3 := fmt.Sprintf("EXPLAIN ESTIMATE %s", queryText)

		// Run EXPLAIN PLAN
		var explainPlan strings.Builder
		if rows1, err1 := a.state.ClickHouse.Query(explain1); err1 == nil {
			for rows1.Next() {
				var s string
				_ = rows1.Scan(&s)
				explainPlan.WriteString(s)
				explainPlan.WriteString("\n")
			}
			rows1.Close()
		} else {
			explainPlan.WriteString(fmt.Sprintf("Error: %v", err1))
		}

		// Run EXPLAIN PIPELINE
		var explainPipe strings.Builder
		if rows2, err2 := a.state.ClickHouse.Query(explain2); err2 == nil {
			for rows2.Next() {
				var s string
				_ = rows2.Scan(&s)
				explainPipe.WriteString(s)
				explainPipe.WriteString("\n")
			}
			rows2.Close()
		} else {
			explainPipe.WriteString(fmt.Sprintf("Error: %v", err2))
		}

		// Run EXPLAIN ESTIMATE
		var explainEst strings.Builder
		if rows3, err3 := a.state.ClickHouse.Query(explain3); err3 == nil {
			cols, _ := rows3.Columns()

			if len(cols) >= 5 {
				type rec struct {
					db    string
					table string
					parts uint64
					rows  uint64
					marks uint64
				}
				var rowsData []rec

				for rows3.Next() {
					var db, table string
					var parts, rcount, marks uint64
					if err := rows3.Scan(&db, &table, &parts, &rcount, &marks); err != nil {
						log.Error().Err(err).Msg("scan explain estimate row")
						continue
					}
					rowsData = append(rowsData, rec{db: db, table: table, parts: parts, rows: rcount, marks: marks})
				}

				// Compute column widths
				col0 := "database.table"
				w0 := len(col0)
				w1 := len("parts")
				w2 := len("rows")
				w3 := len("marks")

				for _, r := range rowsData {
					n := fmt.Sprintf("%s.%s", r.db, r.table)
					if len(n) > w0 {
						w0 = len(n)
					}
					if l := len(fmt.Sprintf("%d", r.parts)); l > w1 {
						w1 = l
					}
					if l := len(utils.FormatReadable(float64(r.rows), 0)); l > w2 {
						w2 = l
					}
					if l := len(utils.FormatReadable(float64(r.marks), 0)); l > w3 {
						w3 = l
					}
				}

				// Header and separator
				fmt.Fprintf(&explainEst, "%-*s  %*s  %*s  %*s\n", w0, col0, w1, "parts", w2, "rows", w3, "marks")
				fmt.Fprintf(&explainEst, "%s\n", strings.Repeat("-", w0+2+w1+2+w2+2+w3))

				// Rows
				for _, r := range rowsData {
					fmt.Fprintf(&explainEst, "%-*s  %*s  %*s  %*s\n",
						w0, fmt.Sprintf("%s.%s", r.db, r.table),
						w1, fmt.Sprintf("%d", r.parts),
						w2, utils.FormatReadable(float64(r.rows), 0),
						w3, utils.FormatReadable(float64(r.marks), 0),
					)
				}
			} else {
				// Fallback for unknown schemas
				for rows3.Next() {
					dest := make([]interface{}, len(cols))
					for i := range dest {
						var v interface{}
						dest[i] = &v
					}
					if err := rows3.Scan(dest...); err != nil {
						continue
					}
					for i := range cols {
						if i > 0 {
							explainEst.WriteString("\t")
						}
						explainEst.WriteString(fmt.Sprintf("%s: %v", cols[i], *(dest[i].(*interface{}))))
					}
					explainEst.WriteString("\n")
				}
			}
			rows3.Close()
		} else {
			explainEst.WriteString(fmt.Sprintf("Error: %v", err3))
		}

		return ExplainResultsMsg{
			QueryText:   queryText,
			Duration:    duration,
			ExplainPlan: explainPlan.String(),
			ExplainPipe: explainPipe.String(),
			ExplainEst:  explainEst.String(),
		}
	}
}

// ShowExplainQuerySelectionForm shows the form for explain (compatibility wrapper)
func (a *App) ShowExplainQuerySelectionForm(fromTime, toTime time.Time, cluster string) {
	a.ShowExplain(CategoryQueryHash, "", fromTime, toTime, cluster)
}

// ShowExplainQuerySelectionFormWithPrefill shows the form with prefilled hash (compatibility wrapper)
func (a *App) ShowExplainQuerySelectionFormWithPrefill(prefillHash string, fromTime, toTime time.Time, cluster string) {
	a.ShowExplain(CategoryQueryHash, prefillHash, fromTime, toTime, cluster)
}
