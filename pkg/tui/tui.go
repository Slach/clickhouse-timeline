package tui

import (
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
	filterInput  *tview.InputField
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

	a.resetConnectList()

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
		a.showFilterInput()
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
					a.showFlamegraphForm()
				case CmdFrom:
					a.showFromDatePicker()
				case CmdTo:
					a.showToDatePicker()
				case CmdRange:
					a.showRangePicker()
				case CmdHeatmap:
					a.showHeatmap()
				case CmdCategory:
					a.showCategorySelector()
				case CmdCluster:
					a.showClusterSelector()
				case CmdMetric:
					a.showMetricSelector()
				case CmdScale:
					a.showScaleSelector()
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
