package tui

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/Slach/clickhouse-timeline/pkg/client"
	"github.com/Slach/clickhouse-timeline/pkg/config"
	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	"github.com/Slach/clickhouse-timeline/pkg/types"
	"github.com/araddon/dateparse"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/rs/zerolog/log"
)

var logo = `
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]  [yellow::b]██
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]  [yellow::b]██  
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]  [yellow::b]██  
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]  [yellow::b]██  
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]  [yellow::b]██  
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]  [yellow::b]██
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]
[yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]
[red::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]  
[red::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]  
[red::b]████[white] [yellow::b]████[white] [yellow::b]████[white] [yellow::b]████[white]  
                                                      
`

type App struct {
	cfg *config.Config
	// connection
	clickHouse      *client.Client
	cluster         string
	clusterList     *tview.List
	selectedContext *config.Context

	tviewApp     *tview.Application
	pages        *tview.Pages
	connectList  *tview.List
	mainView     *tview.TextView
	commandInput *tview.InputField
	mainFlex     *tview.Flex
	version      string
	CLI          *types.CLI

	// Date range fields
	fromTime  time.Time
	toTime    time.Time
	rangeForm *tview.Form

	// Heatmap fields
	categoryType  CategoryType
	heatmapMetric HeatmapMetric
	scaleType     ScaleType
	heatmapTable  *tview.Table
	categoryList  *tview.List
	metricList    *tview.List
	scaleList     *tview.List

	// Selection fields for flamegraph integration
	categoryValue       string
	flamegraphTimeStamp time.Time

	//use Native Flamegraph widget
	flamegraphNative bool

	// Log panel state
	logPanel *LogPanel
}

func NewApp(cfg *config.Config, version string) *App {
	now := time.Now()
	app := &App{
		cfg:           cfg,
		tviewApp:      tview.NewApplication(),
		version:       version,
		fromTime:      now.Add(-24 * time.Hour), // Default: 24 hours ago
		toTime:        now,                      // Default: now
		categoryType:  CategoryQueryHash,        // Default categoryType
		heatmapMetric: MetricCount,              // Default metric
		scaleType:     ScaleLinear,              // Default scale
		CLI:           &types.CLI{},             // Initialize empty CLI
	}

	app.setupUI()
	return app
}

func (a *App) SwitchToMainPage(mainMsg string) {
	if a.selectedContext != nil {
		mainMsg += fmt.Sprintf("\nConnected to %s", a.getContextString(*a.selectedContext))
	}
	if a.heatmapMetric != "" {
		mainMsg += fmt.Sprintf("\nSet heatmap metric to %s", a.heatmapMetric)
	}
	if a.categoryType != "" {
		mainMsg += fmt.Sprintf("\nSet flamegraph category to %s", a.categoryValue)
	}
	mainMsg += "\nPress ':' to continue"
	a.mainView.SetText(mainMsg)
	a.pages.SwitchToPage("main")
	a.tviewApp.SetFocus(a.mainView)
}

func (a *App) ApplyCLIParameters(c *types.CLI, commandName string) {
	mainMsg := ""
	a.flamegraphNative = c.FlamegraphNative
	// Check if flamelens binary exists, if not then use native flamegraph
	if _, err := exec.LookPath("flamelens"); err != nil {
		a.flamegraphNative = true
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
			mainMsg += fmt.Sprintf("Set time range from: '%s'\n", a.fromTime.Format("2006-01-02 15:04:05 -07:00"))
		} else {
			mainMsg += fmt.Sprintf("can't parse --from='%s': %v\n", c.FromTime, err)
		}
	}

	if c.ToTime != "" {
		if t, err := dateparse.ParseAny(c.ToTime); err == nil {
			a.SetToTime(t)
			mainMsg += fmt.Sprintf("Set time range to: '%s'\n", a.toTime.Format("2006-01-02 15:04:05 -07:00"))
		} else {
			mainMsg += fmt.Sprintf("can't parse --to='%s': %v\n", c.ToTime, err)
		}
	}

	if c.RangeOption != "" {
		a.ApplyPredefinedRange(c.RangeOption)
		mainMsg += fmt.Sprintf("Set time range '%s' from: '%s' to: '%s'\n", c.RangeOption, a.fromTime.Format("2006-01-02 15:04:05 -07:00"), a.toTime.Format("2006-01-02 15:04:05 -07:00"))
	}

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

	// Handle command execution if specified
	if commandName != "" {
		mainMsg += a.executeCommand(commandName)
	}

	if mainMsg != "" {
		mainMsg += "Press ':' to continue"
		a.mainView.SetText(mainMsg)
	}
}

func (a *App) SetFromTime(t time.Time) {
	a.fromTime = t
}

func (a *App) SetToTime(t time.Time) {
	a.toTime = t
}

func (a *App) SetCluster(cluster string) {
	a.cluster = cluster
}

func (a *App) SetMetric(metric string) {
	a.heatmapMetric = HeatmapMetric(metric)
}

func (a *App) SetCategory(category string) {
	a.categoryType = CategoryType(category)
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
	for i, ctx := range a.cfg.Contexts {
		if ctx.Name == contextName {
			a.handleContextSelection(i)
			found = true
			break
		}
	}
	return found
}

// executeCommand return message if something wrong, return empty string if all OK
func (a *App) executeCommand(commandName string) string {
	// Check prerequisites for commands that need them
	if commandName == CmdHeatmap || commandName == CmdFlamegraph ||
		commandName == CmdProfileEvents || commandName == CmdMetricLog ||
		commandName == CmdAsyncMetricLog || commandName == CmdLogs {
		if a.clickHouse == nil {
			return "Error: Please connect to a ClickHouse instance first using :connect command\n"
		}
		if a.cluster == "" {
			return "Error: Please select a cluster first using :cluster command\n"
		}
	}

	switch commandName {
	case CmdHeatmap:
		a.ShowHeatmap()
	case CmdFlamegraph:
		a.ShowFlamegraphForm()
	case CmdProfileEvents:
		a.ShowProfileEvents(
			a.categoryType,
			a.categoryValue,
			a.fromTime,
			a.toTime,
			a.cluster,
		)
	case CmdMetricLog:
		a.ShowMetricLog(a.fromTime, a.toTime, a.cluster)
	case CmdAsyncMetricLog:
		a.ShowAsynchronousMetricLog(a.fromTime, a.toTime, a.cluster)
	case CmdLogs:
		// Only apply CLI params when explicitly executing logs command
		// Initialize log panel with CLI params if available
		a.logPanel = &LogPanel{
			app:          a,
			windowSize:   1000,
			database:     "",
			table:        "",
			messageField: "",
			timeField:    "",
			timeMsField:  "",
			dateField:    "",
			levelField:   "",
		}

		if a.CLI != nil {
			if a.CLI.LogsParams.Database != "" {
				a.logPanel.database = a.CLI.LogsParams.Database
			}
			if a.CLI.LogsParams.Table != "" {
				a.logPanel.table = a.CLI.LogsParams.Table
			}
			if a.CLI.LogsParams.Message != "" {
				a.logPanel.messageField = a.CLI.LogsParams.Message
			}
			if a.CLI.LogsParams.Time != "" {
				a.logPanel.timeField = a.CLI.LogsParams.Time
			}
			if a.CLI.LogsParams.TimeMs != "" {
				a.logPanel.timeMsField = a.CLI.LogsParams.TimeMs
			}
			if a.CLI.LogsParams.Date != "" {
				a.logPanel.dateField = a.CLI.LogsParams.Date
			}
			if a.CLI.LogsParams.Level != "" {
				a.logPanel.levelField = a.CLI.LogsParams.Level
			}
			if a.CLI.LogsParams.Window > 0 {
				a.logPanel.windowSize = a.CLI.LogsParams.Window
			}
		}
		a.logPanel.Show()
	case CmdAudit:
		a.ShowAudit()
	}
	return ""
}

func (a *App) ApplyPredefinedRange(rangeOption string) {
	a.toTime = time.Now()
	switch rangeOption {
	case "1h":
		a.fromTime = a.toTime.Add(-1 * time.Hour)
	case "24h":
		a.fromTime = a.toTime.Add(-24 * time.Hour)
	case "7d":
		a.fromTime = a.toTime.Add(-7 * 24 * time.Hour)
	case "30d":
		a.fromTime = a.toTime.Add(-30 * 24 * time.Hour)
	default:
		a.fromTime = a.toTime.Add(-24 * time.Hour)
	}
}

func (a *App) setupUI() {
	a.pages = tview.NewPages()

	// ClickHouse ASCII logo

	a.mainView = tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignLeft).
		SetText(logo + "\nWelcome to ClickHouse Timeline\nPress ':' to enter command mode")

	a.connectList = tview.NewList()
	a.connectList.SetMainTextColor(tcell.ColorWhite)
	a.connectList.SetShortcutColor(tcell.ColorYellow)
	a.connectList.SetSelectedTextColor(tcell.ColorBlack)
	a.connectList.SetSelectedBackgroundColor(tcell.ColorGreen)
	a.connectList.SetWrapAround(true)
	a.connectList.SetBorder(true)
	a.connectList.SetTitle("Connections")
	a.connectList.ShowSecondaryText(false)
	a.connectList.SetHighlightFullLine(true)

	// Initialize connections list
	// Prepare items for filtering
	var items []string
	for _, ctx := range a.cfg.Contexts {
		items = append(items, a.getContextString(ctx))
	}

	fl := widgets.NewFilteredList(
		a.connectList,
		"Connections",
		items,
		"contexts",
	)
	fl.ResetList()

	a.commandInput = tview.NewInputField().
		SetLabel(":").
		SetFieldWidth(30)

	a.mainFlex = tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(a.commandInput, 0, 0, false). // height 0 to hide
		AddItem(a.mainView, 0, 1, true)

	a.pages.AddPage("main", a.mainFlex, true, true)
	a.pages.AddPage("contexts", a.connectList, true, false)

	a.tviewApp.SetRoot(a.pages, true)
	a.tviewApp.EnableMouse(true)

	a.setupKeybindings()
}

func (a *App) defaultInputHandler(event *tcell.EventKey) *tcell.EventKey {
	if event.Rune() == ':' {
		currentFocus := a.tviewApp.GetFocus()
		// Don't trigger command mode when editing time fields
		if frontPage, _ := a.pages.GetFrontPage(); frontPage == "datepicker" {
			// If we're editing a time field, don't trigger command mode
			if _, ok := currentFocus.(*tview.InputField); ok {
				return event
			}
		}

		// Otherwise proceed with command mode
		if a.pages.HasPage("main") {
			a.pages.SwitchToPage("main")
			a.commandInput.SetText("")
			a.mainFlex.ResizeItem(a.commandInput, 1, 0) // Show with height 1
			a.tviewApp.SetFocus(a.commandInput)
			return nil
		}
	}

	return event
}

func (a *App) setupKeybindings() {
	a.tviewApp.SetInputCapture(a.defaultInputHandler)

	a.commandInput.
		SetAutocompleteFunc(func(currentText string) []string {
			var matches []string
			for _, cmd := range availableCommands {
				if strings.Contains(cmd, currentText) {
					matches = append(matches, cmd)
				}
			}
			return matches
		}).
		SetDoneFunc(func(key tcell.Key) {
			if key == tcell.KeyEnter {
				a.mainFlex.ResizeItem(a.commandInput, 0, 0) // Hide with height 0
				cmd := strings.TrimSpace(a.commandInput.GetText())

				switch cmd {
				case CmdHelp:
					a.mainView.SetText(helpText)
				case CmdConnect:
					a.handleConnectCommand()
				case CmdQuit:
					a.handleQuitCommand()
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
				case CmdMetric:
					a.showMetricSelector()
				case CmdScale:
					a.showScaleSelector()
				case CmdAudit:
					a.executeCommand(CmdAudit)
				default:
					mainMsg := a.executeCommand(cmd)
					if mainMsg != "" {
						a.SwitchToMainPage(mainMsg)
					}
				}
			}
		})

	a.connectList.SetSelectedFunc(func(i int, _ string, _ string, _ rune) {
		a.handleContextSelection(i)
	})
}

func (a *App) Run() error {
	defer func() {
		if a.clickHouse != nil {
			if err := a.clickHouse.Close(); err != nil {
				log.Error().Err(err).Stack().Send()
			}
		}
	}()

	return a.tviewApp.Run()
}
