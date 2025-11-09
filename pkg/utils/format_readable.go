package utils

import "fmt"

func FormatReadable(value float64, digits uint64) string {
	digitsStr := fmt.Sprintf("%d", digits)
	if value >= 1000000000 {
		return fmt.Sprintf("%."+digitsStr+"fG", value/1000000000)
	} else if value >= 1000000 {
		return fmt.Sprintf("%."+digitsStr+"fM", value/1000000)
	} else if value >= 1000 {
		return fmt.Sprintf("%."+digitsStr+"fK", value/1000)
	}
	return fmt.Sprintf("%.1f", value)
}
