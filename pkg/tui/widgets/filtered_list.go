package widgets

import (
	"strings"

	"github.com/charmbracelet/bubbles/list"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// FilteredList is a bubbletea model for a filterable list
type FilteredList struct {
	list        list.Model
	filterInput textinput.Model
	filtering   bool
	title       string
	allItems    []list.Item
	width       int
	height      int
	onSelect    func(selectedIndex int, selectedItem list.Item)
}

// FilteredListItem implements list.Item interface
type FilteredListItem struct {
	title       string
	description string
}

func (i FilteredListItem) Title() string       { return i.title }
func (i FilteredListItem) Description() string { return i.description }
func (i FilteredListItem) FilterValue() string { return i.title }

// NewFilteredList creates a new filtered list
func NewFilteredList(title string, items []string, width, height int) FilteredList {
	// Convert strings to list items
	listItems := make([]list.Item, len(items))
	for i, item := range items {
		listItems[i] = FilteredListItem{title: item, description: ""}
	}

	// Create list
	delegate := list.NewDefaultDelegate()
	l := list.New(listItems, delegate, width, height-3) // Reserve space for filter input
	l.Title = title
	l.SetShowStatusBar(false)
	l.SetFilteringEnabled(false) // We handle filtering ourselves

	// Create filter input
	ti := textinput.New()
	ti.Placeholder = "Type to filter..."
	ti.Prompt = "/"
	ti.CharLimit = 100

	return FilteredList{
		list:        l,
		filterInput: ti,
		filtering:   false,
		title:       title,
		allItems:    listItems,
		width:       width,
		height:      height,
	}
}

// SetOnSelect sets the callback when an item is selected
func (m *FilteredList) SetOnSelect(callback func(selectedIndex int, selectedItem list.Item)) {
	m.onSelect = callback
}

// SetSize updates the dimensions
func (m *FilteredList) SetSize(width, height int) {
	m.width = width
	m.height = height
	m.list.SetSize(width, height-3)
}

// Init implements tea.Model
func (m FilteredList) Init() tea.Cmd {
	return nil
}

// Update implements tea.Model
func (m FilteredList) Update(msg tea.Msg) (FilteredList, tea.Cmd) {
	var cmd tea.Cmd
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return m, nil

	case tea.KeyMsg:
		if m.filtering {
			switch msg.String() {
			case "esc":
				// Cancel filtering
				m.filtering = false
				m.filterInput.SetValue("")
				m.filterInput.Blur()
				m.applyFilter("")
				return m, nil

			case "enter":
				// Apply filter and exit filter mode
				m.filtering = false
				m.filterInput.Blur()
				return m, nil

			default:
				// Update filter input
				m.filterInput, cmd = m.filterInput.Update(msg)
				cmds = append(cmds, cmd)
				// Apply filter as user types
				m.applyFilter(m.filterInput.Value())
				return m, tea.Batch(cmds...)
			}
		} else {
			// Normal list navigation
			switch msg.String() {
			case "/":
				// Enter filter mode
				m.filtering = true
				m.filterInput.Focus()
				return m, nil

			case "enter":
				// Select current item
				if m.onSelect != nil {
					selectedItem := m.list.SelectedItem()
					if selectedItem != nil {
						// Find the index in the original allItems
						for i, item := range m.allItems {
							if item.(FilteredListItem).title == selectedItem.(FilteredListItem).title {
								m.onSelect(i, selectedItem)
								break
							}
						}
					}
				}
				return m, nil

			case "esc", "q":
				// Propagate up (will be handled by parent)
				return m, nil

			default:
				// Delegate to list
				m.list, cmd = m.list.Update(msg)
				cmds = append(cmds, cmd)
			}
		}
	}

	return m, tea.Batch(cmds...)
}

// View implements tea.Model
func (m FilteredList) View() string {
	if m.filtering {
		// Show filter input at top
		filterView := lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("62")).
			Padding(0, 1).
			Width(m.width - 4).
			Render(m.filterInput.View())

		return lipgloss.JoinVertical(lipgloss.Left, filterView, m.list.View())
	}

	// Update title to show active filter
	if m.filterInput.Value() != "" {
		titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("cyan"))
		m.list.Title = m.title + " " + titleStyle.Render("/"+m.filterInput.Value())
	} else {
		m.list.Title = m.title
	}

	return m.list.View()
}

// applyFilter filters the list based on the filter text
func (m *FilteredList) applyFilter(filter string) {
	if filter == "" {
		// Reset to all items
		m.list.SetItems(m.allItems)
		return
	}

	// Filter items
	filterLower := strings.ToLower(filter)
	var filtered []list.Item
	for _, item := range m.allItems {
		if strings.Contains(strings.ToLower(item.FilterValue()), filterLower) {
			filtered = append(filtered, item)
		}
	}

	m.list.SetItems(filtered)
}

// SelectedIndex returns the index of the selected item in the original list
func (m FilteredList) SelectedIndex() int {
	selectedItem := m.list.SelectedItem()
	if selectedItem == nil {
		return -1
	}

	// Find index in original allItems
	for i, item := range m.allItems {
		if item.(FilteredListItem).title == selectedItem.(FilteredListItem).title {
			return i
		}
	}
	return -1
}

// SelectedItem returns the currently selected item
func (m FilteredList) SelectedItem() list.Item {
	return m.list.SelectedItem()
}
