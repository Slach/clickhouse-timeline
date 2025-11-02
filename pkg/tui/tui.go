package tui

import (
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/Slach/clickhouse-timeline/pkg/client"
	"github.com/Slach/clickhouse-timeline/pkg/config"
	"github.com/Slach/clickhouse-timeline/pkg/models"
	"github.com/Slach/clickhouse-timeline/pkg/types"
	"github.com/araddon/dateparse"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/rs/zerolog/log"
)

var logo = `████ ████ ████ ████
████ ████ ████ ████
████ ████ ████ ████
████ ████ ████ ████  ██
████ ████ ████ ████  ██
████ ████ ████ ████  ██
████ ████ ████ ████  ██
████ ████ ████ ████  ██
████ ████ ████ ████  ██
████ ████ ████ ████
████ ████ ████ ████
████ ████ ████ ████
████ ████ ████ ████
████ ████ ████ ████
████ ████ ████ ████
████ ████ ████ ████
████ ████ ████ ████
████ ████ ████ ████
████ ████ ████ ████
████ ████ ████ ████
████ ████ ████ ████
████ ████ ████ ████
████ ████ ████ ████`

// Page types
type pageType string

const (
	pageMain           pageType = "main"
	pageConnect        pageType = "connect"
	pageCluster        pageType = "cluster"
	pageCategory       pageType = "category"
	pageMetric         pageType = "metric"
	pageScale          pageType = "scale"
	pageHeatmap        pageType = "heatmap"
	pageFlamegraph     pageType = "flamegraph"
	pageExplain        pageType = "explain"
	pageProfileEvents  pageType = "profile_events"
	pageMetricLog      pageType = "metric_log"
	pageAsyncMetricLog pageType = "async_metric_log"
	pageLogs           pageType = "logs"
	pageMemory         pageType = "memory"
	pageAudit          pageType = "audit"
	pageDatePicker     pageType = "datepicker"
	pageRangePicker    pageType = "rangepicker"
)

// App is the main bubbletea model
type App struct {
	// Core state
	state *models.AppState

	// UI state
	currentPage            pageType
	mainMessage            string
	commandMode            bool
	bubbleCommandInput     textinput.Model // Bubbletea version (for new UI)
	commandSuggestions     []string        // Filtered command suggestions
	selectedSuggestion     int             // Selected suggestion index
	suggestionScrollOffset int             // Scroll offset for suggestions list
	initialCommand         string          // CLI subcommand to execute on startup
	width                  int
	height                 int

	// Sub-models for different views (will be populated as we migrate handlers)
	connectHandler     tea.Model
	clusterHandler     tea.Model
	heatmapHandler     tea.Model
	flamegraphHandler  tea.Model
	explainHandler     tea.Model
	profileHandler     tea.Model
	metricLogHandler   tea.Model
	asyncMetricHandler tea.Model
	logsHandler        tea.Model
	memoryHandler      tea.Model
	auditHandler       tea.Model
	datePickerHandler  tea.Model
	rangePickerHandler tea.Model
	categoryHandler    tea.Model
	metricHandler      tea.Model
	scaleHandler       tea.Model

	// CLI parameter handling
	initialContext *config.Context // Context to connect to from CLI params

	// Legacy fields for compatibility during migration
	// These will be removed as we migrate handlers
	cluster             string
	categoryType        CategoryType
	heatmapMetric       HeatmapMetric
	scaleType           ScaleType
	categoryValue       string
	flamegraphTimeStamp time.Time

	// Compatibility accessors for old handlers (to be removed during migration)
	clickHouse      *client.Client
	selectedContext *config.Context
	fromTime        time.Time
	toTime          time.Time
	initialFromTime time.Time
	initialToTime   time.Time
	CLI             *types.CLI

	version string
	cfg     *config.Config
}

// NewApp creates a new App instance
func NewApp(cfg *config.Config, version string) *App {
	state := models.NewAppState(cfg, version)

	// Bubbletea command input
	ti := textinput.New()
	ti.Placeholder = "Enter command..."
	ti.Prompt = ":"
	ti.CharLimit = 100

	app := &App{
		state:              state,
		currentPage:        pageMain,
		bubbleCommandInput: ti,
		version:            version,
		cfg:                cfg,
		mainMessage: lipgloss.NewStyle().
			Foreground(lipgloss.Color("226")).
			Render(logo) +
			"\n\nWelcome to ClickHouse Timeline\nPress ':' to enter command mode\n\nTip: To copy text from any view, use your terminal's selection (mouse drag) and copy (Ctrl+Shift+C or Cmd+C)",
		// Default values
		categoryType:  CategoryQueryHash,
		heatmapMetric: MetricCount,
		scaleType:     ScaleLinear,
		//  Sync legacy fields with state
		clickHouse:      state.ClickHouse,
		selectedContext: state.SelectedContext,
		fromTime:        state.FromTime,
		toTime:          state.ToTime,
		initialFromTime: state.InitialFromTime,
		initialToTime:   state.InitialToTime,
		CLI:             state.CLI,
	}

	return app
}

// Init initializes the bubbletea application
func (a *App) Init() tea.Cmd {
	// If we have an initial context from CLI params, connect first
	if a.initialContext != nil {
		return a.connectToContextCmd(*a.initialContext)
	}
	// Execute CLI subcommand if provided (when no connection needed)
	if a.initialCommand != "" {
		return a.executeCommand(a.initialCommand)
	}
	return nil
}

// Update handles all messages and state updates
func (a *App) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		a.width = msg.Width
		a.height = msg.Height
		return a, nil

	case ScaleSelectedMsg:
		// Handle scale selection
		a.scaleType = msg.Scale
		a.state.ScaleType = string(msg.Scale)
		a.SwitchToMainPage(fmt.Sprintf("Scale changed to: %s", msg.Scale))
		// If we already have a heatmap, regenerate it with the new scale
		if a.heatmapHandler != nil {
			return a, a.ShowHeatmap()
		}
		return a, nil

	case CategorySelectedMsg:
		// Handle category selection
		a.categoryType = msg.Category
		a.state.CategoryType = string(msg.Category)
		a.SwitchToMainPage(fmt.Sprintf("Heatmap category set to: %s", msg.Name))
		return a, nil

	case MetricSelectedMsg:
		// Handle metric selection
		a.heatmapMetric = msg.Metric
		a.state.HeatmapMetric = string(msg.Metric)
		a.SwitchToMainPage(fmt.Sprintf("Metric changed to: %s", msg.Name))
		// If we already have a heatmap, regenerate it with the new metric
		if a.heatmapHandler != nil {
			return a, a.ShowHeatmap()
		}
		return a, nil

	case ClusterSelectedMsg:
		// Handle cluster selection
		a.cluster = msg.Cluster
		a.state.Cluster = msg.Cluster
		a.SwitchToMainPage(fmt.Sprintf("Cluster set to: %s", msg.Cluster))
		return a, nil

	case ClustersFetchedMsg:
		// Forward to cluster handler if it's active
		if a.currentPage == pageCluster && a.clusterHandler != nil {
			a.clusterHandler, cmd = a.clusterHandler.Update(msg)
			cmds = append(cmds, cmd)
		}
		return a, tea.Batch(cmds...)

	case ContextSelectedMsg:
		// Trigger connection attempt
		return a, a.connectToContextCmd(msg.Context)

	case ConnectionResultMsg:
		// Forward to connect handler first
		if a.currentPage == pageConnect && a.connectHandler != nil {
			a.connectHandler, cmd = a.connectHandler.Update(msg)
			cmds = append(cmds, cmd)
		}

		// Handle connection result
		if msg.Err != nil {
			a.SwitchToMainPage(fmt.Sprintf("Error connecting to ClickHouse: %v\nPress ':' to try again", msg.Err))
		} else {
			// Update state
			a.state.ClickHouse = msg.Client
			a.state.SelectedContext = &msg.Context
			a.clickHouse = msg.Client
			a.selectedContext = &msg.Context
			a.SwitchToMainPage(fmt.Sprintf("Connected to %s:%d : version %s\nPress ':' to continue",
				msg.Context.Host, msg.Context.Port, msg.Version))

			// If we have an initial command from CLI params, execute it now
			if a.initialCommand != "" {
				cmd := a.executeCommand(a.initialCommand)
				a.initialCommand = "" // Clear it so it doesn't re-execute
				cmds = append(cmds, cmd)
			}
		}
		return a, tea.Batch(cmds...)

	case DateSelectedMsg:
		// Handle date selection from date picker
		if msg.Canceled {
			a.SwitchToMainPage("")
			return a, nil
		}

		if msg.IsFrom {
			a.state.FromTime = msg.Time
			a.fromTime = msg.Time
			a.SwitchToMainPage(fmt.Sprintf("From time set to: %s", msg.Time.Format(time.RFC3339)))
		} else {
			a.state.ToTime = msg.Time
			a.toTime = msg.Time
			a.SwitchToMainPage(fmt.Sprintf("To time set to: %s", msg.Time.Format(time.RFC3339)))
		}
		return a, nil

	case RangeSelectedMsg:
		// Handle range selection from range picker
		if msg.Canceled {
			a.SwitchToMainPage("")
			return a, nil
		}

		a.state.FromTime = msg.FromTime
		a.state.ToTime = msg.ToTime
		a.fromTime = msg.FromTime
		a.toTime = msg.ToTime
		a.SwitchToMainPage(fmt.Sprintf("Time range set:\nFrom: %s\nTo: %s",
			msg.FromTime.Format("2006-01-02 15:04:05 -07:00"),
			msg.ToTime.Format("2006-01-02 15:04:05 -07:00")))
		return a, nil

	case MemoryDataMsg:
		// Forward to memory handler if active
		if a.currentPage == pageMemory && a.memoryHandler != nil {
			a.memoryHandler, cmd = a.memoryHandler.Update(msg)
			cmds = append(cmds, cmd)
		}
		return a, tea.Batch(cmds...)

	case AsyncMetricLogDataMsg:
		// Forward to async metric log handler if active
		if a.currentPage == pageAsyncMetricLog && a.asyncMetricHandler != nil {
			a.asyncMetricHandler, cmd = a.asyncMetricHandler.Update(msg)
			cmds = append(cmds, cmd)
		}
		return a, tea.Batch(cmds...)

	case MetricLogDataMsg:
		// Forward to metric log handler if active
		if a.currentPage == pageMetricLog && a.metricLogHandler != nil {
			a.metricLogHandler, cmd = a.metricLogHandler.Update(msg)
			cmds = append(cmds, cmd)
		}
		return a, tea.Batch(cmds...)

	case ProfileEventsDataMsg:
		// Forward to profile events handler if active
		if a.currentPage == pageProfileEvents && a.profileHandler != nil {
			a.profileHandler, cmd = a.profileHandler.Update(msg)
			cmds = append(cmds, cmd)
		}
		return a, tea.Batch(cmds...)

	case ExplainOptionsLoadedMsg, ExplainQueriesLoadedMsg, ExplainPercentilesMsg, ExplainResultsMsg:
		// Forward to explain handler if active
		if a.currentPage == pageExplain && a.explainHandler != nil {
			a.explainHandler, cmd = a.explainHandler.Update(msg)
			cmds = append(cmds, cmd)
		}
		return a, tea.Batch(cmds...)

	case HeatmapDataMsg:
		// Forward to heatmap handler if active
		if a.currentPage == pageHeatmap && a.heatmapHandler != nil {
			a.heatmapHandler, cmd = a.heatmapHandler.Update(msg)
			cmds = append(cmds, cmd)
		}
		return a, tea.Batch(cmds...)

	case LogsConfigMsg:
		// Handle logs configuration completion
		return a, a.ShowLogsViewer(msg.Config)

	case LogsDataMsg:
		// Forward to logs handler if active
		if a.currentPage == pageLogs && a.logsHandler != nil {
			a.logsHandler, cmd = a.logsHandler.Update(msg)
			cmds = append(cmds, cmd)
		}
		return a, tea.Batch(cmds...)

	case AuditProgressMsg:
		// Forward to audit handler if active
		if a.currentPage == pageAudit && a.auditHandler != nil {
			a.auditHandler, cmd = a.auditHandler.Update(msg)
			cmds = append(cmds, cmd)
		}
		return a, tea.Batch(cmds...)

	case AuditResultsMsg:
		// Forward to audit handler if active
		if a.currentPage == pageAudit && a.auditHandler != nil {
			a.auditHandler, cmd = a.auditHandler.Update(msg)
			cmds = append(cmds, cmd)
		}
		return a, tea.Batch(cmds...)

	case FlamegraphConfigMsg:
		// Handle flamegraph configuration completion
		// Use new bubbletea viewer
		return a, a.ShowFlamegraphViewer(msg.CategoryType, msg.CategoryValue, msg.TraceType, msg.FromTime, msg.ToTime, a.cluster)

	case FlamegraphDataMsg:
		// Forward to flamegraph handler if active
		if a.currentPage == pageFlamegraph && a.flamegraphHandler != nil {
			a.flamegraphHandler, cmd = a.flamegraphHandler.Update(msg)
			cmds = append(cmds, cmd)
		}
		return a, tea.Batch(cmds...)

	case HeatmapActionMsg:
		// Handle heatmap action requests
		switch msg.Action {
		case "flamegraph":
			// Determine trace type based on metric
			var traceType TraceType
			if a.heatmapMetric == MetricMemoryUsage {
				traceType = TraceMemory
			} else {
				traceType = TraceReal
			}
			// Use new bubbletea flamegraph viewer
			return a, a.ShowFlamegraphViewer(a.categoryType, msg.CategoryValue, traceType, msg.FromTime, msg.ToTime, a.cluster)
		case "profile_events":
			return a, a.ShowProfileEvents(a.categoryType, msg.CategoryValue, msg.FromTime, msg.ToTime, a.cluster)
		case "explain":
			return a, a.ShowExplain(a.categoryType, msg.CategoryValue, msg.FromTime, msg.ToTime, a.cluster)
		case "zoom_in":
			a.state.FromTime = msg.FromTime
			a.state.ToTime = msg.ToTime
			a.fromTime = msg.FromTime
			a.toTime = msg.ToTime
			return a, a.ShowHeatmap()
		case "zoom_out":
			currentRange := a.state.ToTime.Sub(a.state.FromTime)
			zoomFactor := 2.0
			newRange := time.Duration(float64(currentRange) * zoomFactor)
			center := a.state.FromTime.Add(currentRange / 2)
			newFrom := center.Add(-newRange / 2)
			newTo := center.Add(newRange / 2)

			// Don't exceed initial range
			if newFrom.Before(a.initialFromTime) {
				newFrom = a.initialFromTime
			}
			if newTo.After(a.initialToTime) {
				newTo = a.initialToTime
			}

			a.state.FromTime = newFrom
			a.state.ToTime = newTo
			a.fromTime = newFrom
			a.toTime = newTo
			return a, a.ShowHeatmap()
		case "reset_zoom":
			a.state.FromTime = a.initialFromTime
			a.state.ToTime = a.initialToTime
			a.fromTime = a.initialFromTime
			a.toTime = a.initialToTime
			return a, a.ShowHeatmap()
		}
		return a, nil

	case tea.KeyMsg:
		// Handle command mode
		if a.commandMode {
			switch msg.String() {
			case "esc":
				a.commandMode = false
				a.bubbleCommandInput.SetValue("")
				a.commandSuggestions = nil
				a.selectedSuggestion = 0
				a.suggestionScrollOffset = 0
				return a, nil
			case "enter":
				// Get the command to execute
				var cmd string
				// If a suggestion is selected, use it directly
				if a.selectedSuggestion >= 0 && a.selectedSuggestion < len(a.commandSuggestions) {
					cmd = a.commandSuggestions[a.selectedSuggestion]
				} else {
					// Otherwise use the input value
					cmd = strings.TrimSpace(a.bubbleCommandInput.Value())
				}

				// Clean up and execute
				a.commandMode = false
				a.bubbleCommandInput.SetValue("")
				a.commandSuggestions = nil
				a.selectedSuggestion = 0
				a.suggestionScrollOffset = 0
				return a, a.executeCommand(cmd)
			case "tab":
				// Tab - select first suggestion or cycle through
				if len(a.commandSuggestions) > 0 {
					a.bubbleCommandInput.SetValue(a.commandSuggestions[a.selectedSuggestion])
					a.commandSuggestions = nil
					a.selectedSuggestion = 0
					a.suggestionScrollOffset = 0
				}
				return a, nil
			case "down", "ctrl+n":
				// Navigate down in suggestions
				if len(a.commandSuggestions) > 0 {
					a.selectedSuggestion++
					if a.selectedSuggestion >= len(a.commandSuggestions) {
						a.selectedSuggestion = 0
						a.suggestionScrollOffset = 0
					} else {
						// Auto-scroll to keep selection visible
						maxVisible := 8
						if a.selectedSuggestion >= a.suggestionScrollOffset+maxVisible {
							a.suggestionScrollOffset = a.selectedSuggestion - maxVisible + 1
						}
					}
				}
				return a, nil
			case "up", "ctrl+p":
				// Navigate up in suggestions
				if len(a.commandSuggestions) > 0 {
					a.selectedSuggestion--
					if a.selectedSuggestion < 0 {
						a.selectedSuggestion = len(a.commandSuggestions) - 1
						// Scroll to show the last item
						maxVisible := 8
						a.suggestionScrollOffset = len(a.commandSuggestions) - maxVisible
						if a.suggestionScrollOffset < 0 {
							a.suggestionScrollOffset = 0
						}
					} else {
						// Auto-scroll to keep selection visible
						if a.selectedSuggestion < a.suggestionScrollOffset {
							a.suggestionScrollOffset = a.selectedSuggestion
						}
					}
				}
				return a, nil
			default:
				// Update input and filter suggestions
				a.bubbleCommandInput, cmd = a.bubbleCommandInput.Update(msg)
				a.updateCommandSuggestions()
				return a, cmd
			}
		}

		// Global key handlers
		switch msg.String() {
		case ":":
			// Enter command mode
			a.commandMode = true
			a.bubbleCommandInput.Focus()
			a.updateCommandSuggestions()
			return a, nil

		case "ctrl+c", "q":
			if a.currentPage == pageMain {
				return a, tea.Quit
			}
			// On other pages, 'q' goes back to main
			a.currentPage = pageMain
			return a, nil

		case "esc":
			// Go back to main page
			a.SwitchToMainPage("")
			return a, nil
		}

		// Delegate to current page handler
		switch a.currentPage {
		case pageConnect:
			if a.connectHandler != nil {
				a.connectHandler, cmd = a.connectHandler.Update(msg)
				cmds = append(cmds, cmd)
			}
		case pageCluster:
			if a.clusterHandler != nil {
				a.clusterHandler, cmd = a.clusterHandler.Update(msg)
				cmds = append(cmds, cmd)
			}
		case pageScale:
			if a.scaleHandler != nil {
				a.scaleHandler, cmd = a.scaleHandler.Update(msg)
				cmds = append(cmds, cmd)
			}
		case pageCategory:
			if a.categoryHandler != nil {
				a.categoryHandler, cmd = a.categoryHandler.Update(msg)
				cmds = append(cmds, cmd)
			}
		case pageMetric:
			if a.metricHandler != nil {
				a.metricHandler, cmd = a.metricHandler.Update(msg)
				cmds = append(cmds, cmd)
			}
		case pageDatePicker:
			if a.datePickerHandler != nil {
				a.datePickerHandler, cmd = a.datePickerHandler.Update(msg)
				cmds = append(cmds, cmd)
			}
		case pageRangePicker:
			if a.rangePickerHandler != nil {
				a.rangePickerHandler, cmd = a.rangePickerHandler.Update(msg)
				cmds = append(cmds, cmd)
			}
		case pageMemory:
			if a.memoryHandler != nil {
				a.memoryHandler, cmd = a.memoryHandler.Update(msg)
				cmds = append(cmds, cmd)
			}
		case pageAsyncMetricLog:
			if a.asyncMetricHandler != nil {
				a.asyncMetricHandler, cmd = a.asyncMetricHandler.Update(msg)
				cmds = append(cmds, cmd)
			}
		case pageMetricLog:
			if a.metricLogHandler != nil {
				a.metricLogHandler, cmd = a.metricLogHandler.Update(msg)
				cmds = append(cmds, cmd)
			}
		case pageProfileEvents:
			if a.profileHandler != nil {
				a.profileHandler, cmd = a.profileHandler.Update(msg)
				cmds = append(cmds, cmd)
			}
		case pageExplain:
			if a.explainHandler != nil {
				a.explainHandler, cmd = a.explainHandler.Update(msg)
				cmds = append(cmds, cmd)
			}
		case pageHeatmap:
			if a.heatmapHandler != nil {
				a.heatmapHandler, cmd = a.heatmapHandler.Update(msg)
				cmds = append(cmds, cmd)
			}
		case pageLogs:
			if a.logsHandler != nil {
				a.logsHandler, cmd = a.logsHandler.Update(msg)
				cmds = append(cmds, cmd)
			}
		case pageAudit:
			if a.auditHandler != nil {
				a.auditHandler, cmd = a.auditHandler.Update(msg)
				cmds = append(cmds, cmd)
			}
		case pageFlamegraph:
			if a.flamegraphHandler != nil {
				a.flamegraphHandler, cmd = a.flamegraphHandler.Update(msg)
				cmds = append(cmds, cmd)
			}
			// Add more page handlers as we migrate them
		}
	}

	return a, tea.Batch(cmds...)
}

// View renders the current view
func (a *App) View() string {
	if a.width == 0 {
		return "Loading..."
	}

	var content string

	// Render current page
	switch a.currentPage {
	case pageMain:
		content = a.renderMainPage()
	case pageConnect:
		if a.connectHandler != nil {
			content = a.connectHandler.View()
		} else {
			content = "Connect page not yet implemented"
		}
	case pageCluster:
		if a.clusterHandler != nil {
			content = a.clusterHandler.View()
		} else {
			content = "Cluster selector not yet implemented"
		}
	case pageScale:
		if a.scaleHandler != nil {
			content = a.scaleHandler.View()
		} else {
			content = "Scale selector not yet implemented"
		}
	case pageCategory:
		if a.categoryHandler != nil {
			content = a.categoryHandler.View()
		} else {
			content = "Category selector not yet implemented"
		}
	case pageMetric:
		if a.metricHandler != nil {
			content = a.metricHandler.View()
		} else {
			content = "Metric selector not yet implemented"
		}
	case pageDatePicker:
		if a.datePickerHandler != nil {
			content = a.datePickerHandler.View()
		} else {
			content = "Date picker not yet implemented"
		}
	case pageRangePicker:
		if a.rangePickerHandler != nil {
			content = a.rangePickerHandler.View()
		} else {
			content = "Range picker not yet implemented"
		}
	case pageMemory:
		if a.memoryHandler != nil {
			content = a.memoryHandler.View()
		} else {
			content = "Memory viewer not yet implemented"
		}
	case pageAsyncMetricLog:
		if a.asyncMetricHandler != nil {
			content = a.asyncMetricHandler.View()
		} else {
			content = "Async metric log viewer not yet implemented"
		}
	case pageMetricLog:
		if a.metricLogHandler != nil {
			content = a.metricLogHandler.View()
		} else {
			content = "Metric log viewer not yet implemented"
		}
	case pageProfileEvents:
		if a.profileHandler != nil {
			content = a.profileHandler.View()
		} else {
			content = "Profile events viewer not yet implemented"
		}
	case pageExplain:
		if a.explainHandler != nil {
			content = a.explainHandler.View()
		} else {
			content = "Explain viewer not yet implemented"
		}
	case pageHeatmap:
		if a.heatmapHandler != nil {
			content = a.heatmapHandler.View()
		} else {
			content = "Heatmap not yet implemented"
		}
	case pageLogs:
		if a.logsHandler != nil {
			content = a.logsHandler.View()
		} else {
			content = "Logs viewer not yet implemented"
		}
	case pageAudit:
		if a.auditHandler != nil {
			content = a.auditHandler.View()
		} else {
			content = "Audit viewer not yet implemented"
		}
	case pageFlamegraph:
		if a.flamegraphHandler != nil {
			content = a.flamegraphHandler.View()
		} else {
			content = "Flamegraph configuration not yet implemented"
		}
	// Add more page renderers as we migrate them
	default:
		content = fmt.Sprintf("Page '%s' not yet implemented\nPress ESC to return to main", a.currentPage)
	}

	// Add command input if in command mode
	if a.commandMode {
		commandView := lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("62")).
			Padding(0, 1).
			Render(a.bubbleCommandInput.View())

		// Render suggestions if available
		var suggestionsView string
		if len(a.commandSuggestions) > 0 {
			var suggestionLines []string
			maxSuggestions := 8 // Show max 8 suggestions

			// Calculate the visible window based on scroll offset
			endIdx := a.suggestionScrollOffset + maxSuggestions
			if endIdx > len(a.commandSuggestions) {
				endIdx = len(a.commandSuggestions)
			}
			visibleSuggestions := a.commandSuggestions[a.suggestionScrollOffset:endIdx]

			for i, suggestion := range visibleSuggestions {
				actualIdx := a.suggestionScrollOffset + i
				if actualIdx == a.selectedSuggestion {
					// Highlight selected suggestion
					suggestionLines = append(suggestionLines,
						lipgloss.NewStyle().
							Foreground(lipgloss.Color("0")).
							Background(lipgloss.Color("6")).
							Bold(true).
							Render("▶ "+suggestion))
				} else {
					suggestionLines = append(suggestionLines,
						lipgloss.NewStyle().
							Foreground(lipgloss.Color("8")).
							Render("  "+suggestion))
				}
			}

			suggestionsView = lipgloss.NewStyle().
				BorderStyle(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("8")).
				Padding(0, 1).
				Render(strings.Join(suggestionLines, "\n"))
		}

		// Add help text
		helpText := lipgloss.NewStyle().
			Foreground(lipgloss.Color("8")).
			Render("Tab/Enter: Select | ↑↓: Navigate | Esc: Cancel")

		if suggestionsView != "" {
			content = lipgloss.JoinVertical(lipgloss.Left, content, "", commandView, suggestionsView, helpText)
		} else {
			content = lipgloss.JoinVertical(lipgloss.Left, content, "", commandView, helpText)
		}
	}

	return content
}

// updateCommandSuggestions filters available commands based on current input
func (a *App) updateCommandSuggestions() {
	input := strings.TrimSpace(a.bubbleCommandInput.Value())

	// If input is empty, show all commands
	if input == "" {
		a.commandSuggestions = append([]string{}, availableCommands...)
		a.selectedSuggestion = 0
		return
	}

	// Filter commands that start with the input
	var suggestions []string
	for _, cmd := range availableCommands {
		if strings.HasPrefix(cmd, input) {
			suggestions = append(suggestions, cmd)
		}
	}

	// If no exact prefix matches, try contains
	if len(suggestions) == 0 {
		for _, cmd := range availableCommands {
			if strings.Contains(cmd, input) {
				suggestions = append(suggestions, cmd)
			}
		}
	}

	a.commandSuggestions = suggestions
	// Reset selection if list changed
	if a.selectedSuggestion >= len(suggestions) {
		a.selectedSuggestion = 0
	}
}

// renderMainPage renders the main welcome page
func (a *App) renderMainPage() string {
	var content strings.Builder
	content.WriteString(a.mainMessage)

	if a.state.SelectedContext != nil {
		content.WriteString(fmt.Sprintf("\nConnected to %s", a.getContextString(*a.state.SelectedContext)))
	}
	if a.heatmapMetric != "" {
		content.WriteString(fmt.Sprintf("\nSet heatmap metric to %s", a.heatmapMetric))
	}
	if a.categoryType != "" {
		content.WriteString(fmt.Sprintf("\nSet flamegraph category to %s", a.categoryValue))
	}

	return content.String()
}

// SwitchToMainPage switches to the main page with an optional message
func (a *App) SwitchToMainPage(mainMsg string) {
	a.currentPage = pageMain
	if mainMsg != "" {
		a.mainMessage = mainMsg
	}
}

// executeCommand executes a command and returns a tea.Cmd
func (a *App) executeCommand(commandName string) tea.Cmd {
	log.Info().Str("command", commandName).Msg("Executing command")

	// Check prerequisites for commands that need them
	if slices.Contains([]string{CmdHeatmap, CmdFlamegraph, CmdProfileEvents, CmdMetricLog, CmdAsyncMetricLog, CmdExplain, CmdLogs, CmdMemory}, commandName) {
		if a.state.ClickHouse == nil {
			a.SwitchToMainPage("Error: Please connect to a ClickHouse instance first using :connect command")
			return nil
		}
		if a.cluster == "" {
			a.SwitchToMainPage("Error: Please select a cluster first using :cluster command")
			return nil
		}
	}

	switch commandName {
	case CmdHelp:
		a.mainMessage = helpText
		a.currentPage = pageMain

	case CmdConnect:
		a.handleConnectCommand()

	case CmdQuit:
		return tea.Quit

	case CmdFrom:
		a.showFromDatePicker()

	case CmdTo:
		a.showToDatePicker()

	case CmdRange:
		a.showRangePicker()

	case CmdCategory:
		a.showCategorySelector()

	case CmdCluster:
		a.showClusterSelector()
		// Trigger async cluster fetch
		return a.fetchClustersCmd()

	case CmdMetric:
		a.showMetricSelector()

	case CmdScale:
		a.showScaleSelector()

	case CmdHeatmap:
		return a.ShowHeatmap()

	case CmdFlamegraph:
		a.ShowFlamegraphForm()

	case CmdExplain:
		return a.ShowExplain(a.categoryType, a.categoryValue, a.state.FromTime, a.state.ToTime, a.cluster)

	case CmdProfileEvents:
		return a.ShowProfileEvents(a.categoryType, a.categoryValue, a.state.FromTime, a.state.ToTime, a.cluster)

	case CmdMetricLog:
		return a.ShowMetricLog(a.state.FromTime, a.state.ToTime, a.cluster)

	case CmdAsyncMetricLog:
		return a.ShowAsynchronousMetricLog(a.state.FromTime, a.state.ToTime, a.cluster)

	case CmdMemory:
		return a.ShowMemory()

	case CmdLogs:
		a.handleLogsCommand()

	case CmdAudit:
		return a.ShowAudit()

	default:
		a.SwitchToMainPage(fmt.Sprintf("Unknown command: %s\nType :help for available commands", commandName))
	}

	return nil
}

// Run starts the bubbletea program
func (a *App) Run() error {
	defer func() {
		if a.state.ClickHouse != nil {
			if err := a.state.ClickHouse.Close(); err != nil {
				log.Error().Err(err).Stack().Send()
			}
		}
	}()

	p := tea.NewProgram(a, tea.WithAltScreen(), tea.WithMouseCellMotion())
	_, err := p.Run()
	return err
}

// ApplyCLIParameters applies CLI parameters to the app state
func (a *App) ApplyCLIParameters(c *types.CLI, commandName string) {
	a.state.CLI = c
	mainMsg := ""
	a.state.FlamegraphNative = c.FlamegraphNative

	// Check if flamelens binary exists, if not then use native flamegraph
	if _, err := exec.LookPath("flamelens"); err != nil {
		a.state.FlamegraphNative = true
		log.Info().Msg("flamelens binary not found in PATH, using native flamegraph viewer")
	}

	if c.ConnectTo != "" {
		if found := a.SetConnectByName(c.ConnectTo); !found {
			mainMsg += fmt.Sprintf("Error: Context '%s' not found\nAvailable contexts:\n%s", c.ConnectTo, a.GetContextList())
		} else {
			mainMsg += fmt.Sprintf("Set connect context to: '%s'\n", c.ConnectTo)
		}
	}

	if c.FromTime != "" {
		if t, err := dateparse.ParseAny(c.FromTime); err == nil {
			a.SetFromTime(t)
			mainMsg += fmt.Sprintf("Set time range from: '%s'\n", a.state.FromTime.Format("2006-01-02 15:04:05 -07:00"))
		} else {
			mainMsg += fmt.Sprintf("can't parse --from='%s': %v\n", c.FromTime, err)
		}
	}

	if c.ToTime != "" {
		if t, err := dateparse.ParseAny(c.ToTime); err == nil {
			a.SetToTime(t)
			mainMsg += fmt.Sprintf("Set time range to: '%s'\n", a.state.ToTime.Format("2006-01-02 15:04:05 -07:00"))
		} else {
			mainMsg += fmt.Sprintf("can't parse --to='%s': %v\n", c.ToTime, err)
		}
	}

	if c.RangeOption != "" {
		a.ApplyPredefinedRange(c.RangeOption)
		mainMsg += fmt.Sprintf("Set time range '%s' from: '%s' to: '%s'\n", c.RangeOption, a.state.FromTime.Format("2006-01-02 15:04:05 -07:00"), a.state.ToTime.Format("2006-01-02 15:04:05 -07:00"))
	}

	// Update initial time range after applying CLI parameters
	a.state.InitialFromTime = a.state.FromTime
	a.state.InitialToTime = a.state.ToTime

	if c.Cluster != "" {
		a.SetCluster(c.Cluster)
		mainMsg += fmt.Sprintf("Set cluster '%s'\n", c.Cluster)
	}

	if c.Metric != "" {
		a.SetMetric(c.Metric)
		mainMsg += fmt.Sprintf("Set metric '%s'\n", c.Metric)
	}

	if c.Category != "" {
		a.SetCategory(c.Category)
		mainMsg += fmt.Sprintf("Set categoryType '%s'\n", c.Category)
	}

	// Store command for execution in Init()
	if commandName != "" && commandName != "clickhouse-timeline" {
		a.initialCommand = commandName
		mainMsg += fmt.Sprintf("Executing command: %s\n", commandName)
	}

	if mainMsg != "" {
		mainMsg += "Press ':' to continue"
		a.mainMessage = mainMsg
	}
}

// Helper methods

func (a *App) SetFromTime(t time.Time) {
	a.state.FromTime = t
	if a.state.InitialFromTime.IsZero() || a.state.InitialToTime.IsZero() {
		a.state.InitialFromTime = t
	}
}

func (a *App) SetToTime(t time.Time) {
	a.state.ToTime = t
	if a.state.InitialFromTime.IsZero() || a.state.InitialToTime.IsZero() {
		a.state.InitialToTime = t
	}
}

func (a *App) SetCluster(cluster string) {
	a.cluster = cluster
	a.state.Cluster = cluster
}

func (a *App) SetMetric(metric string) {
	a.heatmapMetric = HeatmapMetric(metric)
	a.state.HeatmapMetric = metric
}

func (a *App) SetCategory(category string) {
	a.categoryType = CategoryType(category)
	a.state.CategoryType = category
}

func (a *App) GetContextList() string {
	var contextList strings.Builder
	for _, ctx := range a.cfg.Contexts {
		contextList.WriteString(fmt.Sprintf("  - %s\n", ctx.Name))
	}
	return contextList.String()
}

func (a *App) SetConnectByName(contextName string) bool {
	if len(a.cfg.Contexts) == 0 {
		return false
	}

	found := false
	for _, ctx := range a.cfg.Contexts {
		if ctx.Name == contextName {
			// Store the context for connection in Init()
			ctxCopy := ctx
			a.initialContext = &ctxCopy
			found = true
			break
		}
	}
	return found
}

func (a *App) ApplyPredefinedRange(rangeOption string) {
	a.state.ToTime = time.Now()
	a.SetToTime(a.state.ToTime)

	switch rangeOption {
	case "1h":
		a.state.FromTime = a.state.ToTime.Add(-1 * time.Hour)
	case "24h":
		a.state.FromTime = a.state.ToTime.Add(-24 * time.Hour)
	case "7d":
		a.state.FromTime = a.state.ToTime.Add(-7 * 24 * time.Hour)
	case "30d":
		a.state.FromTime = a.state.ToTime.Add(-30 * 24 * time.Hour)
	default:
		a.state.FromTime = a.state.ToTime.Add(-24 * time.Hour)
	}
	a.SetFromTime(a.state.FromTime)
}

// Handler methods are implemented in their respective handler files
