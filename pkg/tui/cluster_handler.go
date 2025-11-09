package tui

import (
	"fmt"

	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	tea "github.com/charmbracelet/bubbletea"
)

// ClusterSelectedMsg is sent when a cluster is selected
type ClusterSelectedMsg struct {
	Cluster string
}

// ClustersFetchedMsg is sent when clusters are fetched from the database
type ClustersFetchedMsg struct {
	Clusters []string
	Err      error
}

// clusterSelector is a bubbletea model for selecting a cluster
type clusterSelector struct {
	list     widgets.FilteredList
	clusters []string
	loading  bool
	err      error
	width    int
	height   int
}

func newClusterSelector(width, height int) clusterSelector {
	// Start with empty list, will be populated when clusters are fetched
	listModel := widgets.NewFilteredList("Select Cluster", []string{"Loading clusters..."}, width, height)

	return clusterSelector{
		list:     listModel,
		clusters: []string{},
		loading:  true,
		width:    width,
		height:   height,
	}
}

func (m clusterSelector) Init() tea.Cmd {
	return nil
}

func (m clusterSelector) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case ClustersFetchedMsg:
		m.loading = false
		if msg.Err != nil {
			m.err = msg.Err
			m.list = widgets.NewFilteredList(
				"Select Cluster",
				[]string{fmt.Sprintf("Error: %v", msg.Err)},
				m.width,
				m.height,
			)
			return m, nil
		}

		m.clusters = msg.Clusters
		if len(m.clusters) == 0 {
			m.list = widgets.NewFilteredList(
				"Select Cluster",
				[]string{"No clusters found"},
				m.width,
				m.height,
			)
		} else {
			m.list = widgets.NewFilteredList(
				"Select Cluster",
				m.clusters,
				m.width,
				m.height,
			)
		}
		return m, nil

	case tea.KeyMsg:
		if m.loading || m.err != nil {
			// During loading or error, only allow escape
			if msg.String() == "esc" || msg.String() == "q" {
				return m, nil
			}
			return m, nil
		}

		switch msg.String() {
		case "enter":
			// Get selected cluster
			selectedIdx := m.list.SelectedIndex()
			if selectedIdx >= 0 && selectedIdx < len(m.clusters) {
				return m, func() tea.Msg {
					return ClusterSelectedMsg{Cluster: m.clusters[selectedIdx]}
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

func (m clusterSelector) View() string {
	if m.loading {
		return m.list.View() + "\n\nFetching clusters from database..."
	}
	if m.err != nil {
		return m.list.View() + "\n\nPress ESC to go back"
	}
	return m.list.View()
}

// fetchClustersCmd fetches clusters from the database
func (a *App) fetchClustersCmd() tea.Cmd {
	return func() tea.Msg {
		if a.state.ClickHouse == nil {
			return ClustersFetchedMsg{Err: fmt.Errorf("not connected to ClickHouse")}
		}

		rows, err := a.state.ClickHouse.Query("SELECT DISTINCT cluster FROM system.clusters ORDER BY cluster")
		if err != nil {
			return ClustersFetchedMsg{Err: err}
		}
		defer rows.Close()

		var clusters []string
		for rows.Next() {
			var cluster string
			if err := rows.Scan(&cluster); err != nil {
				return ClustersFetchedMsg{Err: err}
			}
			clusters = append(clusters, cluster)
		}

		if err := rows.Err(); err != nil {
			return ClustersFetchedMsg{Err: err}
		}

		return ClustersFetchedMsg{Clusters: clusters}
	}
}

// showClusterSelector fetches and displays available clusters
func (a *App) showClusterSelector() {
	if a.state.ClickHouse == nil {
		a.SwitchToMainPage("Error: Please connect to a ClickHouse instance first")
		return
	}

	// Create cluster selector and fetch clusters
	selector := newClusterSelector(a.width, a.height)
	a.clusterHandler = selector
	a.currentPage = pageCluster

	// Trigger async cluster fetch
	// This will be handled in the main Update loop
}
