package tui

import (
	"fmt"
	"github.com/rivo/tview"
)

// showClusterSelector fetches and displays available clusters
func (a *App) showClusterSelector() {
	if a.clickHouse == nil {
		a.mainView.SetText("Error: Please connect to a ClickHouse instance first")
		return
	}

	a.mainView.SetText("Fetching available clusters...")

	go func() {
		// Query system.clusters table
		rows, err := a.clickHouse.Query("SELECT DISTINCT cluster FROM system.clusters ORDER BY cluster")
		if err != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Error fetching clusters: %v", err))
			})
			return
		}
		defer rows.Close()

		var clusters []string
		for rows.Next() {
			var cluster string
			if err := rows.Scan(&cluster); err != nil {
				a.tviewApp.QueueUpdateDraw(func() {
					a.mainView.SetText(fmt.Sprintf("Error scanning row: %v", err))
				})
				return
			}
			clusters = append(clusters, cluster)
		}

		if err := rows.Err(); err != nil {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText(fmt.Sprintf("Error reading rows: %v", err))
			})
			return
		}

		if len(clusters) == 0 {
			a.tviewApp.QueueUpdateDraw(func() {
				a.mainView.SetText("No clusters found")
			})
			return
		}

		a.tviewApp.QueueUpdateDraw(func() {
			clusterList := tview.NewList()
			clusterList.SetTitle("Select Cluster")
			clusterList.SetBorder(true)

			for i, cluster := range clusters {
				clusterList.AddItem(cluster, "", rune('1'+i), nil)
			}

			clusterList.SetSelectedFunc(func(i int, _ string, _ string, _ rune) {
				a.cluster = clusters[i]
				a.SwitchToMainPage(fmt.Sprintf("Cluster set to: %s", a.cluster))
			})

			a.clusterList = clusterList
			a.pages.AddPage("clusters", clusterList, true, true)
			a.pages.SwitchToPage("clusters")
		})
	}()
}
