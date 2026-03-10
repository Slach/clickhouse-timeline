package utils

import (
	"testing"
)

func TestFormatReadable(t *testing.T) {
	testCases := []struct {
		name     string
		value    float64
		digits   uint64
		expected string
	}{
		// Gigabyte range (>= 1,000,000,000)
		{"one_billion", 1000000000, 1, "1.0G"},
		{"two_point_five_billion", 2500000000, 1, "2.5G"},
		{"nine_point nine_billion", 9900000000, 1, "9.9G"},
		{"ten_billion", 10000000000, 1, "10.0G"},

		// Megabyte range (>= 1,000,000)
		{"one_million", 1000000, 1, "1.0M"},
		{"five_hundred_thousand", 500000, 1, "500.0K"},
		{"nine_hundred_ninety_nine_thousand", 999000, 1, "999.0K"},
		{"two_point_five_million", 2500000, 1, "2.5M"},

		// Kilobyte range (>= 1,000)
		{"one_thousand", 1000, 1, "1.0K"},
		{"five_hundred", 500, 1, "500.0"},
		{"nine_hundred_ninety_nine", 999, 1, "999.0"},
		{"two_point_five_thousand", 2500, 1, "2.5K"},

		// Base range (< 1,000)
		{"zero", 0, 1, "0.0"},
		{"one", 1, 1, "1.0"},
		{"ninety_nine", 99, 1, "99.0"},
		{"hundred", 100, 1, "100.0"},
		{"nine_hundred_ninety_nine_small", 999, 1, "999.0"},

		// Different digit precision
		{"two_digits_billion", 1234567890, 2, "1.23G"},
		{"two_digits_million", 1234567, 2, "1.23M"},
		{"two_digits_thousand", 1234, 2, "1.23K"},
		{"zero_digits_billion", 1500000000, 0, "2G"},

		// Edge cases
		{"negative_value", -1500, 1, "-1.5K"},
		{"very_small_positive", 0.001, 1, "0.0"},
		{"very_small_negative", -0.001, 1, "-0.0"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := FormatReadable(tc.value, tc.digits)
			if result != tc.expected {
				t.Errorf("FormatReadable(%v, %d) = %q, want %q",
					tc.value, tc.digits, result, tc.expected)
			}
		})
	}
}

// Test boundary conditions precisely
func TestFormatReadableBoundaries(t *testing.T) {
	boundaries := []struct {
		value    float64
		expected string
		desc     string
	}{
		{999999999, "999.9M", "just under 1 billion"},
		{1000000000, "1.0G", "exactly 1 billion"},
		{1000000001, "1.0G", "just over 1 billion"},

		{999999, "999.9K", "just under 1 million"},
		{1000000, "1.0M", "exactly 1 million"},
		{1000001, "1.0M", "just over 1 million"},

		{999, "999.0", "just under 1 thousand"},
		{1000, "1.0K", "exactly 1 thousand"},
		{1001, "1.0K", "just over 1 thousand"},
	}

	for _, tc := range boundaries {
		t.Run(tc.desc, func(t *testing.T) {
			result := FormatReadable(tc.value, 1)
			if result != tc.expected {
				t.Errorf("FormatReadable(%v, 1) = %q, want %q",
					tc.value, result, tc.expected)
			}
		})
	}
}
