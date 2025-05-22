package tui

import (
	"fmt"
	"github.com/rivo/tview"
)

// getCategorySQL returns the SQL expression for the given category
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

// getCategoryName returns a human-readable name for the category
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
		return "Unknown category"
	}
}

// showCategorySelector displays a list of available categories
func (a *App) showCategorySelector() {
	categoryList := tview.NewList()
	categoryList.SetTitle("Select Category")
	categoryList.SetBorder(true)

	categories := []struct {
		name     string
		category CategoryType
	}{
		{"Query Hash", CategoryQueryHash},
		{"Tables", CategoryTable},
		{"Hosts", CategoryHost},
		{"Errors", CategoryError},
	}

	for i, cat := range categories {
		categoryList.AddItem(cat.name, "", rune('1'+i), nil)
	}

	categoryList.SetSelectedFunc(func(i int, _ string, _ string, _ rune) {
		a.category = categories[i].category
		a.SwitchToMainPage(fmt.Sprintf("Category set to: %s", categories[i].name))
	})

	a.categoryList = categoryList
	a.pages.AddPage("categories", categoryList, true, true)
	a.pages.SwitchToPage("categories")
}
