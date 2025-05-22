package main

import (
	"github.com/rivo/tview"
)

func main() {
	app := tview.NewApplication()

	form := tview.NewForm()
	form.SetBorder(true).SetTitle("Form with Focus Control").SetRect(0, 0, 40, 10)

	// Slice to keep references to form items for indexing
	var items []tview.Primitive

	// Add some fields
	input1 := tview.NewInputField().SetLabel("Name:")
	items = append(items, input1)

	input2 := tview.NewInputField().SetLabel("Email:")
	items = append(items, input2)

	checkbox := tview.NewCheckbox().SetLabel("Subscribe:")
	items = append(items, checkbox)

	// Add to form
	form.AddFormItem(input1)
	form.AddFormItem(input2)
	form.AddFormItem(checkbox)

	// Add a button to switch focus
	form.AddButton("Focus Name", func() {
		app.SetFocus(items[0]) // Focus first item
	})

	form.AddButton("Focus Email", func() {
		app.SetFocus(items[1]) // Focus second item
	})

	form.AddButton("Focus Checkbox", func() {
		app.SetFocus(items[2]) // Focus third item
	})

	// Quit button
	form.AddButton("Quit", func() {
		app.Stop()
	})

	if err := app.SetRoot(form, true).Run(); err != nil {
		panic(err)
	}
}
