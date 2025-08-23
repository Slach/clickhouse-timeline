package widgets

import "fmt"

// FormatReadable formats a numeric value into a human-readable string
// isCount indicates if the value represents a count (integer) or a measurement (may have decimals)
func FormatReadable(value float64, isCount bool) string {
	if isCount {
		return formatCount(value)
	}
	return formatMeasurement(value)
}

func formatCount(value float64) string {
	if value >= 1000000000 {
		return fmt.Sprintf("%.1fG", value/1000000000)
	} else if value >= 1000000 {
		return fmt.Sprintf("%.1fM", value/1000000)
	} else if value >= 1000 {
		return fmt.Sprintf("%.1fK", value/1000)
	}
	return fmt.Sprintf("%.0f", value)
}

func formatMeasurement(value float64) string {
	if value >= 1000000000 {
		return fmt.Sprintf("%.1fG", value/1000000000)
	} else if value >= 1000000 {
		return fmt.Sprintf("%.1fM", value/1000000)
	} else if value >= 1000 {
		return fmt.Sprintf("%.1fK", value/1000)
	}
	return fmt.Sprintf("%.1f", value)
}
