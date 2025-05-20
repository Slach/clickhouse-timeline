package tui

import (
	"fmt"
	"github.com/Slach/clickhouse-timeline/pkg/types"
	"github.com/Slach/clickhouse-timeline/pkg/tui/widgets"
	"github.com/araddon/dateparse"
	"strings"
	"time"

	"github.com/Slach/clickhouse-timeline/pkg/client"
	"github.com/Slach/clickhouse-timeline/pkg/config"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/rs/zerolog/log"
)

type App struct {
	cfg          *config.Config
	tviewApp     *tview.Application
	pages        *tview.Pages
	connectList  *tview.List
	mainView     *tview.TextView
	commandInput *tview.InputField
	clickHouse   *client.Client
	mainFlex     *tview.Flex
	version      string

	// Date range fields
	fromTime  time.Time
	toTime    time.Time
	rangeForm *tview.Form

	// Heatmap fields
	category      CategoryType
	cluster       string
	currentMetric HeatmapMetric
	scaleType     ScaleType
	heatmapTable  *tview.Table
	clusterList   *tview.List
	categoryList  *tview.List
	metricList    *tview.List
	scaleList     *tview.List

	// Selection fields for flamegraph integration
	selectedCategory  string
	selectedTimestamp time.Time

	//use Native Flamegraph widget
	flamegraphNative bool
}

func (a *App) ApplyCLIParameters(c *types.CLI, commandName string) {
	mainMsg := ""
	a.flamegraphNative = c.FlamegraphNative
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
		mainMsg += fmt.Sprintf("Set category '%s'\n", c.Metric)
	}
	if mainMsg != "" {
		mainMsg += "Press ':' to continue"
		a.mainView.SetText(mainMsg)
	}

	// Switch to appropriate mode based on command
	switch commandName {
	case "heatmap":
		a.ShowHeatmap()
	case "flamegraph":
		a.ShowFlamegraphForm()
	case "profile_events":
		if a.clickHouse == nil {
			a.mainView.SetText("Error: Please connect to a ClickHouse instance first")
			break
		}
		if a.cluster == "" {
			a.mainView.SetText("Error: Please select a cluster first using :cluster command")
			break
		}
		a.ShowProfileEvents(
			a.category,
			a.selectedCategory,
			a.fromTime,
			a.toTime,
			a.cluster,
		)
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
	a.currentMetric = HeatmapMetric(metric)
}

func (a *App) SetCategory(category string) {
	a.category = CategoryType(category)
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

func (a *App) ApplyPredefinedRange(rangeOption string) {
	switch rangeOption {
	case "1h":
		a.fromTime = time.Now().Add(-1 * time.Hour)
	case "24h":
		a.fromTime = time.Now().Add(-24 * time.Hour)
	case "7d":
		a.fromTime = time.Now().Add(-7 * 24 * time.Hour)
	case "30d":
		a.fromTime = time.Now().Add(-30 * 24 * time.Hour)
	default:
		a.fromTime = time.Now().Add(-24 * time.Hour)
	}
	a.toTime = time.Now()
}

func NewApp(cfg *config.Config, version string) *App {
	app := &App{
		cfg:           cfg,
		tviewApp:      tview.NewApplication(),
		version:       version,
		fromTime:      time.Now().Add(-24 * time.Hour), // Default: 24 hours ago
		toTime:        time.Now(),                      // Default: now
		category:      CategoryQueryHash,               // Default category
		currentMetric: MetricCount,                     // Default metric
		scaleType:     ScaleLinear,                     // Default scale
	}

	app.setupUI()
	return app
}

func (a *App) setupUI() {
	a.pages = tview.NewPages()
	a.mainView = tview.NewTextView().
		SetTextAlign(tview.AlignLeft).
		SetText("Welcome to ClickHouse Timeline\nPress ':' to enter command mode")

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
	// Check if we're currently in a time field by examining the focused primitive
	currentFocus := a.tviewApp.GetFocus()

	// Don't trigger command mode when editing time fields
	if event.Rune() == ':' {
		// Check if we're on the datepicker page and focused on a time input
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

	// Filter mode with '/' when on the connections list
	frontPageName, _ := a.pages.GetFrontPage()
	if event.Rune() == '/' && a.pages.HasPage("contexts") && frontPageName == "contexts" {
		// Create filterable list for connections
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
		fl.ShowFilterInput(a.tviewApp, a.pages)
		return nil
	}

	return event
}

func (a *App) setupKeybindings() {
	a.tviewApp.SetInputCapture(a.defaultInputHandler)

	a.commandInput.
		SetAutocompleteFunc(func(currentText string) []string {
			var matches []string
			for _, cmd := range availableCommands {
				if strings.HasPrefix(cmd, currentText) {
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
				case CmdFlamegraph:
					a.ShowFlamegraphForm()
				case CmdFrom:
					a.showFromDatePicker()
				case CmdTo:
					a.showToDatePicker()
				case CmdRange:
					a.showRangePicker()
				case CmdHeatmap:
					a.ShowHeatmap()
				case CmdCategory:
					a.showCategorySelector()
				case CmdCluster:
					a.showClusterSelector()
				case CmdMetric:
					a.showMetricSelector()
				case CmdScale:
					a.showScaleSelector()
				case CmdProfileEvents:
					a.ShowProfileEvents(
						a.category,
						a.selectedCategory,
						a.fromTime,
						a.toTime,
						a.cluster,
					)
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
