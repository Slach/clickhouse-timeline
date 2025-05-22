package main

import (
	"fmt"
	"github.com/Slach/clickhouse-timeline/pkg/flamegraph"
	"github.com/rivo/tview"
)

func main() {
	app := tview.NewApplication()
	app.EnableMouse(true)
	// Sample flamegraph data.
	flameData := `root;func1;func2 30
root;func1;func4 70
root;func3;func5;func 10;func 100 50
root;func3;func6 20`

	flameView := flamegraph.NewFlamegraphView()
	flameView.SetData(flameData)
	flameView.SetBorder(true).SetTitle("Flamegraph (Use arrow keys to navigate)")

	if err := app.SetRoot(flameView, true).Run(); err != nil {
		panic(fmt.Sprintf("Error running application: %v", err))
	}
}
