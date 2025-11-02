package tui

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/evertras/bubble-table/table"
	"github.com/rs/zerolog/log"
)

// LogFilter represents a filter condition
type LogFilter struct {
	Field    string
	Operator string
	Value    string
}

// LogEntry represents a single log entry
type LogEntry struct {
	Time      time.Time
	Message   string
	Level     string
	AllFields map[string]interface{}
}

// LogConfig holds configuration for log viewing
type LogConfig struct {
	Database     string
	Table        string
	MessageField string
	TimeField    string
	TimeMsField  string
	DateField    string
	LevelField   string
	WindowSize   int
}

// LogsDataMsg is sent when log data is loaded
type LogsDataMsg struct {
	Entries        []LogEntry
	FirstEntryTime time.Time
	LastEntryTime  time.Time
	TotalRows      int
	LevelCounts    map[string]int
	Err            error
}

// LogsConfigMsg is sent when configuration is completed
type LogsConfigMsg struct {
	Config LogConfig
}

// logsConfigForm is the configuration form for log viewing
type logsConfigForm struct {
	databases     []string
	tables        []string
	allFields     []string
	timeFields    []string
	timeMsFields  []string
	dateFields    []string
	messageFields []string

	dbInput        textinput.Model
	tableInput     textinput.Model
	msgFieldInput  textinput.Model
	timeFieldInput textinput.Model
	windowInput    textinput.Model

	currentField int // 0=db, 1=table, 2=msg, 3=time, 4=window
	width        int
	height       int
	loading      bool
	err          error
}

func newLogsConfigForm(width, height int) logsConfigForm {
	dbInput := textinput.New()
	dbInput.Placeholder = "database name"
	dbInput.Width = 40
	dbInput.Focus()

	tableInput := textinput.New()
	tableInput.Placeholder = "table name"
	tableInput.Width = 40

	msgFieldInput := textinput.New()
	msgFieldInput.Placeholder = "message field name"
	msgFieldInput.Width = 40

	timeFieldInput := textinput.New()
	timeFieldInput.Placeholder = "time field name"
	timeFieldInput.Width = 40

	windowInput := textinput.New()
	windowInput.Placeholder = "1000"
	windowInput.SetValue("1000")
	windowInput.Width = 40

	return logsConfigForm{
		dbInput:        dbInput,
		tableInput:     tableInput,
		msgFieldInput:  msgFieldInput,
		timeFieldInput: timeFieldInput,
		windowInput:    windowInput,
		currentField:   0,
		width:          width,
		height:         height,
	}
}

func (m logsConfigForm) Init() tea.Cmd {
	return textinput.Blink
}

func (m logsConfigForm) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "q":
			return m, func() tea.Msg {
				return tea.KeyMsg{Type: tea.KeyEsc}
			}
		case "tab", "down":
			m.currentField = (m.currentField + 1) % 5
			m.updateFocus()
		case "shift+tab", "up":
			m.currentField = (m.currentField + 4) % 5
			m.updateFocus()
		case "enter":
			// Validate and submit
			if m.dbInput.Value() == "" || m.tableInput.Value() == "" ||
				m.msgFieldInput.Value() == "" || m.timeFieldInput.Value() == "" {
				return m, nil
			}

			windowSize, _ := strconv.Atoi(m.windowInput.Value())
			if windowSize == 0 {
				windowSize = 1000
			}

			config := LogConfig{
				Database:     m.dbInput.Value(),
				Table:        m.tableInput.Value(),
				MessageField: m.msgFieldInput.Value(),
				TimeField:    m.timeFieldInput.Value(),
				WindowSize:   windowSize,
			}

			return m, func() tea.Msg {
				return LogsConfigMsg{Config: config}
			}
		}
	}

	// Update the active input
	switch m.currentField {
	case 0:
		m.dbInput, cmd = m.dbInput.Update(msg)
	case 1:
		m.tableInput, cmd = m.tableInput.Update(msg)
	case 2:
		m.msgFieldInput, cmd = m.msgFieldInput.Update(msg)
	case 3:
		m.timeFieldInput, cmd = m.timeFieldInput.Update(msg)
	case 4:
		m.windowInput, cmd = m.windowInput.Update(msg)
	}

	return m, cmd
}

func (m *logsConfigForm) updateFocus() {
	m.dbInput.Blur()
	m.tableInput.Blur()
	m.msgFieldInput.Blur()
	m.timeFieldInput.Blur()
	m.windowInput.Blur()

	switch m.currentField {
	case 0:
		m.dbInput.Focus()
	case 1:
		m.tableInput.Focus()
	case 2:
		m.msgFieldInput.Focus()
	case 3:
		m.timeFieldInput.Focus()
	case 4:
		m.windowInput.Focus()
	}
}

func (m logsConfigForm) View() string {
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	labelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("15"))
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))

	var sb strings.Builder
	sb.WriteString(titleStyle.Render("Log Explorer Configuration"))
	sb.WriteString("\n\n")

	// Database
	sb.WriteString(labelStyle.Render("Database:"))
	sb.WriteString("\n")
	sb.WriteString(m.dbInput.View())
	sb.WriteString("\n\n")

	// Table
	sb.WriteString(labelStyle.Render("Table:"))
	sb.WriteString("\n")
	sb.WriteString(m.tableInput.View())
	sb.WriteString("\n\n")

	// Message Field
	sb.WriteString(labelStyle.Render("Message Field:"))
	sb.WriteString("\n")
	sb.WriteString(m.msgFieldInput.View())
	sb.WriteString("\n\n")

	// Time Field
	sb.WriteString(labelStyle.Render("Time Field:"))
	sb.WriteString("\n")
	sb.WriteString(m.timeFieldInput.View())
	sb.WriteString("\n\n")

	// Window Size
	sb.WriteString(labelStyle.Render("Window Size:"))
	sb.WriteString("\n")
	sb.WriteString(m.windowInput.View())
	sb.WriteString("\n\n")

	// Help
	sb.WriteString(helpStyle.Render("Tab/Shift+Tab: Navigate | Enter: Continue | Esc: Cancel"))

	// Wrap in border
	borderStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("6")).
		Padding(1, 2)

	return borderStyle.Render(sb.String())
}

// logsViewer is the main log viewer
type logsViewer struct {
	config         LogConfig
	table          widgets.FilteredTable
	filters        []LogFilter
	entries        []LogEntry
	firstEntryTime time.Time
	lastEntryTime  time.Time
	totalRows      int
	levelCounts    map[string]int
	loading        bool
	err            error
	width          int
	height         int
	showDetails    bool
	selectedEntry  LogEntry
}

func newLogsViewer(config LogConfig, width, height int) logsViewer {
	tableModel := widgets.NewFilteredTable(
		"Log Entries",
		[]string{"Time", "Level", "Message"},
		width,
		height-10,
	)

	return logsViewer{
		config:  config,
		table:   tableModel,
		loading: true,
		width:   width,
		height:  height,
	}
}

func (m logsViewer) Init() tea.Cmd {
	return nil
}

func (m logsViewer) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case LogsDataMsg:
		m.loading = false
		if msg.Err != nil {
			m.err = msg.Err
			return m, nil
		}

		m.entries = msg.Entries
		m.firstEntryTime = msg.FirstEntryTime
		m.lastEntryTime = msg.LastEntryTime
		m.totalRows = msg.TotalRows
		m.levelCounts = msg.LevelCounts

		// Convert to table rows
		var rows []table.Row
		for _, entry := range msg.Entries {
			timeStr := entry.Time.Format("2006-01-02 15:04:05.000")
			rowData := table.RowData{
				"Time":    timeStr,
				"Level":   entry.Level,
				"Message": entry.Message,
			}
			rows = append(rows, table.NewRow(rowData))
		}
		m.table.SetRows(rows)
		return m, nil

	case tea.KeyMsg:
		if m.showDetails {
			switch msg.String() {
			case "esc", "q":
				m.showDetails = false
				return m, nil
			}
			return m, nil
		}

		switch msg.String() {
		case "esc", "q":
			return m, func() tea.Msg {
				return tea.KeyMsg{Type: tea.KeyEsc}
			}
		case "enter":
			// Show details for selected row
			selected := m.table.HighlightedRow()
			if selected.Data != nil && len(m.entries) > 0 {
				// Find corresponding entry
				timeStr := selected.Data["Time"].(string)
				for _, entry := range m.entries {
					if entry.Time.Format("2006-01-02 15:04:05.000") == timeStr {
						m.selectedEntry = entry
						m.showDetails = true
						break
					}
				}
			}
			return m, nil
		case "ctrl+n":
			// Load newer logs
			return m, m.loadMoreLogsCmd(true)
		case "ctrl+p":
			// Load older logs
			return m, m.loadMoreLogsCmd(false)
		}
	}

	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

func (m logsViewer) View() string {
	if m.loading {
		return "Loading logs, please wait..."
	}
	if m.err != nil {
		return fmt.Sprintf("Error loading logs: %v\n\nPress ESC to return", m.err)
	}

	if m.showDetails {
		return m.renderDetails()
	}

	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))

	// Title with stats
	title := fmt.Sprintf("Log Entries: %s to %s (Total: %d)",
		m.firstEntryTime.Format("15:04:05"),
		m.lastEntryTime.Format("15:04:05"),
		m.totalRows)

	// Level counts
	var levelStats []string
	for level, count := range m.levelCounts {
		levelStats = append(levelStats, fmt.Sprintf("%s:%d", level, count))
	}
	stats := strings.Join(levelStats, " | ")

	help := "Enter: Details | Ctrl+N: Newer | Ctrl+P: Older | /: Filter | Esc: Exit"

	content := lipgloss.JoinVertical(lipgloss.Left,
		titleStyle.Render(title),
		stats,
		"",
		m.table.View(),
		"",
		helpStyle.Render(help),
	)

	return content
}

func (m logsViewer) renderDetails() string {
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	labelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("11"))
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))

	var sb strings.Builder
	sb.WriteString(titleStyle.Render("Log Entry Details"))
	sb.WriteString("\n\n")

	sb.WriteString(labelStyle.Render("Time: "))
	sb.WriteString(m.selectedEntry.Time.Format("2006-01-02 15:04:05.000 MST"))
	sb.WriteString("\n\n")

	if m.selectedEntry.Level != "" {
		sb.WriteString(labelStyle.Render("Level: "))
		sb.WriteString(m.selectedEntry.Level)
		sb.WriteString("\n\n")
	}

	sb.WriteString(labelStyle.Render("Message:"))
	sb.WriteString("\n")
	sb.WriteString(m.selectedEntry.Message)
	sb.WriteString("\n\n")

	// Show all fields if available
	if len(m.selectedEntry.AllFields) > 0 {
		sb.WriteString(labelStyle.Render("Additional Fields:"))
		sb.WriteString("\n")
		for k, v := range m.selectedEntry.AllFields {
			sb.WriteString(fmt.Sprintf("%s: %v\n", k, v))
		}
	}

	sb.WriteString("\n")
	sb.WriteString(helpStyle.Render("Press ESC to return"))

	borderStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("6")).
		Padding(1, 2)

	return borderStyle.Render(sb.String())
}

func (m logsViewer) loadMoreLogsCmd(newer bool) tea.Cmd {
	return func() tea.Msg {
		// TODO: Implement pagination
		return nil
	}
}

// ShowLogs displays the log explorer
func (a *App) ShowLogs() tea.Cmd {
	if a.state.ClickHouse == nil {
		a.SwitchToMainPage("Error: Please connect to a ClickHouse instance first using :connect command")
		return nil
	}

	// Show configuration form
	form := newLogsConfigForm(a.width, a.height)
	a.logsHandler = form
	a.currentPage = pageLogs

	return nil
}

// ShowLogsViewer shows the log viewer with the given configuration
func (a *App) ShowLogsViewer(config LogConfig) tea.Cmd {
	viewer := newLogsViewer(config, a.width, a.height)
	a.logsHandler = viewer
	a.currentPage = pageLogs

	// Start async data fetch
	return a.fetchLogsDataCmd(config)
}

// fetchLogsDataCmd fetches log data from ClickHouse
func (a *App) fetchLogsDataCmd(config LogConfig) tea.Cmd {
	return func() tea.Msg {
		// Build query
		selectFields := []string{config.TimeField, config.MessageField}
		if config.LevelField != "" {
			selectFields = append(selectFields, config.LevelField)
		}

		query := fmt.Sprintf(
			"SELECT %s FROM `%s`.`%s` ORDER BY %s DESC LIMIT %d",
			strings.Join(selectFields, ", "),
			config.Database,
			config.Table,
			config.TimeField,
			config.WindowSize,
		)

		rows, err := a.state.ClickHouse.Query(query)
		if err != nil {
			return LogsDataMsg{Err: fmt.Errorf("error executing query: %v", err)}
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close logs query")
			}
		}()

		var entries []LogEntry
		levelCounts := make(map[string]int)
		var firstTime, lastTime time.Time

		for rows.Next() {
			var entry LogEntry
			var values []interface{}

			// Prepare scan destinations
			values = append(values, &entry.Time)
			values = append(values, &entry.Message)
			if config.LevelField != "" {
				values = append(values, &entry.Level)
			}

			if err := rows.Scan(values...); err != nil {
				return LogsDataMsg{Err: fmt.Errorf("error scanning row: %v", err)}
			}

			entries = append(entries, entry)

			if firstTime.IsZero() || entry.Time.Before(firstTime) {
				firstTime = entry.Time
			}
			if lastTime.IsZero() || entry.Time.After(lastTime) {
				lastTime = entry.Time
			}

			if entry.Level != "" {
				levelCounts[entry.Level]++
			}
		}

		if err := rows.Err(); err != nil {
			return LogsDataMsg{Err: fmt.Errorf("error reading rows: %v", err)}
		}

		// Reverse entries to show oldest first
		for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
			entries[i], entries[j] = entries[j], entries[i]
		}

		return LogsDataMsg{
			Entries:        entries,
			FirstEntryTime: firstTime,
			LastEntryTime:  lastTime,
			TotalRows:      len(entries),
			LevelCounts:    levelCounts,
		}
	}
}

// handleLogsCommand handles the :logs command
func (a *App) handleLogsCommand() {
	a.ShowLogs()
}
