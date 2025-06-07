package widgets

import (
	"fmt"
	"github.com/rivo/tview"
	"testing"
	"time"
)

// BenchmarkFilteredTable_FilterTable benchmarks the FilterTable method
func BenchmarkFilteredTable_FilterTable(b *testing.B) {
	// Create test data with varying sizes
	testSizes := []int{100, 500, 1000, 5000, 10000}
	
	for _, size := range testSizes {
		b.Run(fmt.Sprintf("rows_%d", size), func(b *testing.B) {
			ft := NewFilteredTable()
			ft.SetupHeaders([]string{"Time", "Message", "Level"})
			
			// Populate with test data
			for i := 0; i < size; i++ {
				cells := []*tview.TableCell{
					tview.NewTableCell(fmt.Sprintf("2024-01-01 10:%02d:%02d", i/60, i%60)),
					tview.NewTableCell(fmt.Sprintf("Test log message %d with some content", i)),
					tview.NewTableCell([]string{"INFO", "ERROR", "WARN", "DEBUG"}[i%4]),
				}
				ft.AddRow(cells)
			}
			
			// Test filtering with a common search term
			filter := "error"
			
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ft.FilterTable(filter)
			}
		})
	}
}

// BenchmarkFilteredTable_Navigation simulates navigation after filtering
func BenchmarkFilteredTable_Navigation(b *testing.B) {
	testSizes := []int{1000, 5000, 10000}
	
	for _, size := range testSizes {
		b.Run(fmt.Sprintf("navigation_rows_%d", size), func(b *testing.B) {
			ft := NewFilteredTable()
			ft.SetupHeaders([]string{"Time", "Message", "Level"})
			
			// Populate with test data
			for i := 0; i < size; i++ {
				cells := []*tview.TableCell{
					tview.NewTableCell(fmt.Sprintf("2024-01-01 10:%02d:%02d", i/60, i%60)),
					tview.NewTableCell(fmt.Sprintf("Test log message %d with some content", i)),
					tview.NewTableCell([]string{"INFO", "ERROR", "WARN", "DEBUG"}[i%4]),
				}
				ft.AddRow(cells)
			}
			
			// Apply filter first
			ft.FilterTable("error")
			
			b.ResetTimer()
			// Simulate navigation operations
			for i := 0; i < b.N; i++ {
				// Simulate getting current selection and moving
				row, col := ft.Table.GetSelection()
				newRow := (row + 1) % ft.Table.GetRowCount()
				if newRow == 0 {
					newRow = 1 // Skip header
				}
				ft.Table.Select(newRow, col)
			}
		})
	}
}

// BenchmarkFilteredTable_SetRow benchmarks adding rows to the table
func BenchmarkFilteredTable_SetRow(b *testing.B) {
	ft := NewFilteredTable()
	ft.SetupHeaders([]string{"Time", "Message", "Level"})
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cells := []*tview.TableCell{
			tview.NewTableCell(fmt.Sprintf("2024-01-01 10:%02d:%02d", i/60, i%60)),
			tview.NewTableCell(fmt.Sprintf("Test log message %d with some content", i)),
			tview.NewTableCell([]string{"INFO", "ERROR", "WARN", "DEBUG"}[i%4]),
		}
		ft.SetRow(i+1, cells)
	}
}

// BenchmarkFilteredTable_RealWorldScenario simulates real-world usage
func BenchmarkFilteredTable_RealWorldScenario(b *testing.B) {
	b.Run("realistic_usage", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ft := NewFilteredTable()
			ft.SetupHeaders([]string{"Time", "Message", "Level"})
			
			// Add 1000 rows (typical log window)
			for j := 0; j < 1000; j++ {
				cells := []*tview.TableCell{
					tview.NewTableCell(fmt.Sprintf("2024-01-01 10:%02d:%02d.%03d", j/60, j%60, j%1000)),
					tview.NewTableCell(fmt.Sprintf("Application log entry %d: Processing request with ID %d, status: %s", j, j*123, []string{"success", "error", "pending", "timeout"}[j%4])),
					tview.NewTableCell([]string{"INFO", "ERROR", "WARN", "DEBUG", "TRACE"}[j%5]),
				}
				ft.AddRow(cells)
			}
			
			// Apply filter (this is where the performance issue likely occurs)
			ft.FilterTable("error")
			
			// Simulate some navigation
			for k := 0; k < 10; k++ {
				row, col := ft.Table.GetSelection()
				newRow := (row + 1) % ft.Table.GetRowCount()
				if newRow == 0 {
					newRow = 1
				}
				ft.Table.Select(newRow, col)
			}
		}
	})
}

// TestFilteredTable_Performance measures actual time for operations
func TestFilteredTable_Performance(t *testing.T) {
	sizes := []int{1000, 5000, 10000}
	
	for _, size := range sizes {
		t.Run(fmt.Sprintf("performance_test_%d_rows", size), func(t *testing.T) {
			ft := NewFilteredTable()
			ft.SetupHeaders([]string{"Time", "Message", "Level"})
			
			// Populate with test data
			start := time.Now()
			for i := 0; i < size; i++ {
				cells := []*tview.TableCell{
					tview.NewTableCell(fmt.Sprintf("2024-01-01 10:%02d:%02d.%03d", i/60, i%60, i%1000)),
					tview.NewTableCell(fmt.Sprintf("Application log entry %d: Processing request with ID %d, status: %s", i, i*123, []string{"success", "error", "pending", "timeout"}[i%4])),
					tview.NewTableCell([]string{"INFO", "ERROR", "WARN", "DEBUG", "TRACE"}[i%5]),
				}
				ft.AddRow(cells)
			}
			populateTime := time.Since(start)
			
			// Test filtering time
			start = time.Now()
			ft.FilterTable("error")
			filterTime := time.Since(start)
			
			// Test navigation time (simulate multiple up/down key presses)
			start = time.Now()
			for i := 0; i < 100; i++ {
				row, col := ft.Table.GetSelection()
				newRow := (row + 1) % ft.Table.GetRowCount()
				if newRow == 0 {
					newRow = 1
				}
				ft.Table.Select(newRow, col)
			}
			navigationTime := time.Since(start)
			
			t.Logf("Size: %d rows", size)
			t.Logf("Populate time: %v", populateTime)
			t.Logf("Filter time: %v", filterTime)
			t.Logf("Navigation time (100 moves): %v", navigationTime)
			t.Logf("Average navigation per move: %v", navigationTime/100)
			
			// Flag performance issues
			if filterTime > 100*time.Millisecond {
				t.Logf("WARNING: Filter operation took %v (>100ms) for %d rows", filterTime, size)
			}
			if navigationTime/100 > 10*time.Millisecond {
				t.Logf("WARNING: Navigation per move took %v (>10ms) for %d rows", navigationTime/100, size)
			}
		})
	}
}

// BenchmarkFilteredTable_RealWorldSlowNavigation reproduces the exact issue from manual testing
// This simulates: load 5000 rows, press "/", enter "NULL", then navigate with up/down keys
func BenchmarkFilteredTable_RealWorldSlowNavigation(b *testing.B) {
	b.Run("simulate_manual_test_scenario", func(b *testing.B) {
		ft := NewFilteredTable()
		ft.SetupHeaders([]string{"Time", "Message", "Level"})
		
		// Populate with 5000 rows like in manual test (--window 5000)
		for i := 0; i < 5000; i++ {
			cells := []*tview.TableCell{
				tview.NewTableCell(fmt.Sprintf("2024-01-01 10:%02d:%02d.%03d", i/60, i%60, i%1000)),
				tview.NewTableCell(fmt.Sprintf("Log message %d with NULL values and other content", i)),
				tview.NewTableCell([]string{"INFO", "ERROR", "WARN", "DEBUG", "TRACE"}[i%5]),
			}
			ft.AddRow(cells)
		}
		
		// Apply "NULL" filter (like pressing "/" and entering "NULL")
		ft.FilterTable("NULL")
		
		b.ResetTimer()
		
		// Measure navigation performance after filtering
		for i := 0; i < b.N; i++ {
			// Simulate single up/down key press (this is where the slowness occurs)
			row, col := ft.Table.GetSelection()
			newRow := (row + 1) % ft.Table.GetRowCount()
			if newRow == 0 {
				newRow = 1 // Skip header
			}
			ft.Table.Select(newRow, col)
		}
	})
}

// TestFilteredTable_SlowNavigationAfterFilter tests the specific scenario causing slowness
func TestFilteredTable_SlowNavigationAfterFilter(t *testing.T) {
	ft := NewFilteredTable()
	ft.SetupHeaders([]string{"Time", "Message", "Level"})
	
	// Populate with 5000 rows like in manual test
	for i := 0; i < 5000; i++ {
		cells := []*tview.TableCell{
			tview.NewTableCell(fmt.Sprintf("2024-01-01 10:%02d:%02d.%03d", i/60, i%60, i%1000)),
			tview.NewTableCell(fmt.Sprintf("Log message %d with NULL values and other content", i)),
			tview.NewTableCell([]string{"INFO", "ERROR", "WARN", "DEBUG", "TRACE"}[i%5]),
		}
		ft.AddRow(cells)
	}
	
	t.Logf("Initial table has %d rows", ft.Table.GetRowCount())
	
	// Apply "NULL" filter
	start := time.Now()
	ft.FilterTable("NULL")
	filterTime := time.Since(start)
	t.Logf("Filter 'NULL' took: %v", filterTime)
	t.Logf("After filter table has %d rows", ft.Table.GetRowCount())
	
	// Test individual navigation steps (this should reveal the slowness)
	navigationTimes := make([]time.Duration, 10)
	for i := 0; i < 10; i++ {
		start = time.Now()
		row, col := ft.Table.GetSelection()
		newRow := (row + 1) % ft.Table.GetRowCount()
		if newRow == 0 {
			newRow = 1
		}
		ft.Table.Select(newRow, col)
		navigationTimes[i] = time.Since(start)
		t.Logf("Navigation step %d took: %v", i+1, navigationTimes[i])
		
		// Flag if any single navigation step is slow
		if navigationTimes[i] > 50*time.Millisecond {
			t.Logf("WARNING: Single navigation step took %v (>50ms)", navigationTimes[i])
		}
	}
	
	// Calculate average
	var total time.Duration
	for _, dur := range navigationTimes {
		total += dur
	}
	avgNavTime := total / time.Duration(len(navigationTimes))
	t.Logf("Average navigation time per step: %v", avgNavTime)
}
