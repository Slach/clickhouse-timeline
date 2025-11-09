package tui

import (
	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	tea "github.com/charmbracelet/bubbletea"
)

// getCategorySQL returns the SQL expression for the given categoryType
func getCategorySQL(category CategoryType) string {
	switch category {
	case CategoryQueryHash:
		return "normalized_query_hash"
	case CategoryTable:
		return "tables"
	case CategoryHost:
		return "hostName()"
	case CategoryError:
		return "concat(errorCodeToName(exception_code),':',normalized_query_hash)"
	default:
		return "normalized_query_hash"
	}
}

// getCategoryName returns a human-readable name for the categoryType
func getCategoryName(category CategoryType) string {
	switch category {
	case CategoryQueryHash:
		return "Query Hash"
	case CategoryTable:
		return "Tables"
	case CategoryHost:
		return "Hosts"
	case CategoryError:
		return "Errors"
	default:
		return "Unknown categoryType"
	}
}

// CategorySelectedMsg is sent when a category is selected
type CategorySelectedMsg struct {
	Category CategoryType
	Name     string
}

// categorySelector is a bubbletea model for selecting category type
type categorySelector struct {
	list       widgets.FilteredList
	categories []CategoryType
	names      []string
}

func newCategorySelector(width, height int) categorySelector {
	categories := []CategoryType{CategoryQueryHash, CategoryTable, CategoryHost, CategoryError}
	names := []string{"Query Hash", "Tables", "Hosts", "Errors"}

	listModel := widgets.NewFilteredList("Select Category", names, width, height)

	return categorySelector{
		list:       listModel,
		categories: categories,
		names:      names,
	}
}

func (m categorySelector) Init() tea.Cmd {
	return nil
}

func (m categorySelector) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "enter":
			// Get selected category
			selectedIdx := m.list.SelectedIndex()
			if selectedIdx >= 0 && selectedIdx < len(m.categories) {
				return m, func() tea.Msg {
					return CategorySelectedMsg{
						Category: m.categories[selectedIdx],
						Name:     m.names[selectedIdx],
					}
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

func (m categorySelector) View() string {
	return m.list.View()
}

// showCategorySelector displays a list of available categories
func (a *App) showCategorySelector() {
	// Create bubbletea category selector
	selector := newCategorySelector(a.width, a.height)
	a.categoryHandler = selector
	a.currentPage = pageCategory
}
