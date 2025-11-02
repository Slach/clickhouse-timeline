package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/evertras/bubble-table/table"
	"github.com/rs/zerolog/log"
)

const profileEventsQueryTemplate = `
SELECT
    key AS EventName,
    count(),
    quantile(0.5)(value) AS p50,
    quantile(0.9)(value) AS p90,
    quantile(0.99)(value) AS p99,
    formatReadableQuantity(p50) AS p50_s,
    formatReadableQuantity(p90) AS p90_s,
    formatReadableQuantity(p99) AS p99_s,
    any(normalizeQueryKeepNames(query)) AS normalized_query
FROM clusterAllReplicas('%s', merge(system,'^query_log'))
LEFT ARRAY JOIN mapKeys(ProfileEvents) AS key, mapValues(ProfileEvents) AS value
WHERE
    event_date >= toDate(parseDateTimeBestEffort('%s')) AND event_date <= toDate(parseDateTimeBestEffort('%s')) AND
    event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s')
    AND type != 'QueryStart'
    %s
GROUP BY key
ORDER BY key
`

// ProfileEventsDataMsg is sent when profile events data is loaded
type ProfileEventsDataMsg struct {
	Rows  []table.Row
	Title string
	Err   error
}

// profileEventsViewer is a bubbletea model for profile events display
type profileEventsViewer struct {
	table     widgets.FilteredTable
	queryView widgets.QueryView
	loading   bool
	err       error
	width     int
	height    int
}

func newProfileEventsViewer(width, height int) profileEventsViewer {
	tableWidth := (width * 2) / 3
	queryWidth := width - tableWidth

	tableModel := widgets.NewFilteredTable(
		"Profile Events",
		[]string{"Event", "Count", "p50", "p90", "p99"},
		tableWidth,
		height-4,
	)

	queryModel := widgets.NewQueryView("Query", queryWidth, height-4)

	return profileEventsViewer{
		table:     tableModel,
		queryView: queryModel,
		loading:   true,
		width:     width,
		height:    height,
	}
}

func (m profileEventsViewer) Init() tea.Cmd {
	return nil
}

func (m profileEventsViewer) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case ProfileEventsDataMsg:
		m.loading = false
		if msg.Err != nil {
			m.err = msg.Err
			return m, nil
		}

		tableWidth := (m.width * 2) / 3
		queryWidth := m.width - tableWidth

		// Update table with data
		m.table = widgets.NewFilteredTable(
			msg.Title,
			[]string{"Event", "Count", "p50", "p90", "p99"},
			tableWidth,
			m.height-4,
		)
		m.table.SetRows(msg.Rows)
		m.queryView = widgets.NewQueryView("Query", queryWidth, m.height-4)
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "q":
			return m, func() tea.Msg {
				return tea.KeyMsg{Type: tea.KeyEsc}
			}
		case "enter":
			// TODO: Show event description in a modal
			return m, nil
		}
	}

	// Update table and query view based on selection
	m.table, cmd = m.table.Update(msg)
	cmds = append(cmds, cmd)

	// Update query view if selection changed
	selected := m.table.HighlightedRow()
	if selected.Data != nil {
		if query, ok := selected.Data["query"].(string); ok && query != "" {
			m.queryView.SetSQL(query)
		}
	}

	m.queryView, cmd = m.queryView.Update(msg)
	cmds = append(cmds, cmd)

	return m, tea.Batch(cmds...)
}

func (m profileEventsViewer) View() string {
	if m.loading {
		return "Loading profile events, please wait..."
	}
	if m.err != nil {
		return fmt.Sprintf("Error loading profile events: %v\n\nPress ESC to return", m.err)
	}

	// Split-pane layout: table on left, query view on right
	return lipgloss.JoinHorizontal(
		lipgloss.Top,
		m.table.View(),
		m.queryView.View(),
	)
}

// ShowProfileEvents displays profile events data
func (a *App) ShowProfileEvents(categoryType CategoryType, categoryValue string, fromTime, toTime time.Time, cluster string) tea.Cmd {
	if a.state.ClickHouse == nil {
		a.SwitchToMainPage("Error: Please connect to a ClickHouse instance first using :connect command")
		return nil
	}
	if cluster == "" {
		a.SwitchToMainPage("Error: Please select a cluster first using :cluster command")
		return nil
	}

	// Create and show viewer
	viewer := newProfileEventsViewer(a.width, a.height)
	a.profileHandler = viewer
	a.currentPage = pageProfileEvents

	// Start async data fetch
	return a.fetchProfileEventsDataCmd(categoryType, categoryValue, fromTime, toTime, cluster)
}

// fetchProfileEventsDataCmd fetches profile events data from ClickHouse
func (a *App) fetchProfileEventsDataCmd(categoryType CategoryType, categoryValue string, fromTime, toTime time.Time, cluster string) tea.Cmd {
	return func() tea.Msg {
		fromStr := fromTime.Format("2006-01-02 15:04:05 -07:00")
		toStr := toTime.Format("2006-01-02 15:04:05 -07:00")

		// Build category filter if categoryValue is provided
		var categoryFilter string
		if categoryValue != "" {
			switch categoryType {
			case CategoryQueryHash:
				categoryFilter = fmt.Sprintf("AND normalized_query_hash = '%s'", categoryValue)
			case CategoryTable:
				categoryFilter = fmt.Sprintf("AND has(tables, ['%s'])", categoryValue)
			case CategoryHost:
				categoryFilter = fmt.Sprintf("AND hostName() = '%s'", categoryValue)
			case CategoryError:
				parts := strings.Split(categoryValue, ":")
				categoryFilter = fmt.Sprintf("AND errorCodeToName(exception_code)='%s' AND normalized_query_hash = %s", parts[0], parts[1])
			}
		}

		query := fmt.Sprintf(
			profileEventsQueryTemplate,
			cluster,
			fromStr, toStr, fromStr, toStr,
			categoryFilter,
		)

		rows, err := a.state.ClickHouse.Query(query)
		if err != nil {
			return ProfileEventsDataMsg{Err: fmt.Errorf("error executing query: %v\n%s", err, query)}
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close ProfileEvents rows")
			}
		}()

		// Process rows
		var tableRows []table.Row
		for rows.Next() {
			var (
				event           string
				count           int
				p50             float64
				p90             float64
				p99             float64
				p50s            string
				p90s            string
				p99s            string
				normalizedQuery string
			)

			if err := rows.Scan(&event, &count, &p50, &p90, &p99, &p50s, &p90s, &p99s, &normalizedQuery); err != nil {
				return ProfileEventsDataMsg{Err: fmt.Errorf("error scanning row: %v", err)}
			}

			// Determine color based on percentile differences (stored as metadata for rendering)
			// For now, we'll just use the formatted strings
			rowData := table.RowData{
				"Event": event,
				"Count": fmt.Sprintf("%d", count),
				"p50":   p50s,
				"p90":   p90s,
				"p99":   p99s,
				"query": normalizedQuery, // Hidden column for query view
			}
			tableRows = append(tableRows, table.NewRow(rowData))
		}

		if err := rows.Err(); err != nil {
			return ProfileEventsDataMsg{Err: fmt.Errorf("error reading rows: %v", err)}
		}

		title := fmt.Sprintf("Profile Events: %s (%s to %s)", categoryValue, fromStr, toStr)
		return ProfileEventsDataMsg{
			Rows:  tableRows,
			Title: title,
		}
	}
}
