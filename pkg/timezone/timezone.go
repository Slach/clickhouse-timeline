package timezone

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp/syntax"
	"runtime"
	"slices"
	"sort"
	"strings"
	"time"
	_ "time/tzdata"
	"unicode/utf8"
)

var zoneDirs = map[string]string{
	"android":   "/system/usr/share/zoneinfo/",
	"darwin":    "/usr/share/zoneinfo/",
	"dragonfly": "/usr/share/zoneinfo/",
	"freebsd":   "/usr/share/zoneinfo/",
	"linux":     "/usr/share/zoneinfo/",
	"netbsd":    "/usr/share/zoneinfo/",
	"openbsd":   "/usr/share/zoneinfo/",
	"solaris":   "/usr/share/lib/zoneinfo/",
}

// GetCurrentTimeZone returns the current timezone information
func GetCurrentTimeZone() (ZoneInfo, error) {
	tzName, err := getCurrentTimezone()
	if err != nil {
		return ZoneInfo{}, err
	}

	tzName = windowsToIANA(tzName)

	// Load the timezone location
	location, err := time.LoadLocation(tzName)
	if err != nil {
		return ZoneInfo{}, fmt.Errorf("failed to load timezone '%s': %w", tzName, err)
	}

	// Get current time in the timezone
	now := time.Now().In(location)
	_, offset := now.Zone()

	// Convert offset from seconds to minutes
	offsetMinutes := offset / 60

	// Create display text
	hours := offsetMinutes / 60
	minutes := offsetMinutes % 60
	sign := "+"
	if hours < 0 {
		sign = "-"
		hours = -hours
	}

	// Try to find the timezone in our list first
	for _, tz := range TimeZones {
		if tz.Name == tzName {
			return tz, nil
		}
	}

	// If not found by name, try to find by Windows name
	if runtime.GOOS == "windows" {
		for _, tz := range TimeZones {
			if tz.WindowsName == tzName {
				return tz, nil
			}
		}
	}

	// If not found, create a new ZoneInfo
	var windowsName string
	if runtime.GOOS == "windows" {
		windowsName = tzName
	}

	return ZoneInfo{
		DisplayText: fmt.Sprintf("(UTC %s%02d:%02d) %s", sign, hours, minutes, tzName),
		Name:        tzName,
		WindowsName: windowsName,
		Offset:      offsetMinutes,
	}, nil
}

func windowsToIANA(tzName string) string {
	// For Windows, convert the timezone ID to IANA format if needed
	if runtime.GOOS == "windows" {
		// Check if this is a Windows timezone name that needs conversion
		ianaName, found := WindowsToIana[tzName]
		if found {
			// Use the IANA name instead
			tzName = ianaName
		}
	}
	return tzName
}

// getCurrentTimezone returns the current timezone name based on the operating system
func getCurrentTimezone() (string, error) {
	switch runtime.GOOS {
	case "windows":
		return getWindowsTimezone()
	case "darwin":
		return getMacOSTimezone()
	case "linux":
		return getLinuxTimezone()
	default:
		// Fallback to Go's time package for other operating systems
		name, _ := time.Now().Zone()
		return name, nil
	}
}

// getMacOSTimezone gets the current timezone on macOS
func getMacOSTimezone() (string, error) {
	cmd := exec.Command("systemsetup", "-gettimezone")
	output, err := cmd.Output()
	if err != nil {
		// Fallback to Go's time package if the command fails
		name, _ := time.Now().Zone()
		return name, nil
	}
	timezone := strings.TrimSpace(strings.Replace(string(output), "Time Zone: ", "", 1))
	return timezone, nil
}

// getWindowsTimezone gets the current timezone on Windows using PowerShell
func getWindowsTimezone() (string, error) {
	// Try PowerShell first for more reliable IANA-compatible IDs
	cmd := exec.Command("powershell", "-Command", "(Get-TimeZone).Id")
	output, err := cmd.Output()
	if err == nil {
		timezone := strings.TrimSpace(string(output))
		if timezone != "" {
			return timezone, nil
		}
	}

	// Fallback to tzutil if PowerShell fails
	cmd = exec.Command("tzutil", "/g")
	output, err = cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to get current timezone: %w", err)
	}
	return strings.TrimSpace(string(output)), nil
}

// getLinuxTimezone gets the current timezone on Linux
func getLinuxTimezone() (string, error) {
	tzPath, err := exec.Command("readlink", "-f", "/etc/localtime").Output()
	if err != nil {
		// Fallback to Go's time package if the command fails
		name, _ := time.Now().Zone()
		return name, nil
	}

	tzFull := strings.TrimSpace(string(tzPath))
	const zonePrefix = "/usr/share/zoneinfo/"
	idx := strings.Index(tzFull, zonePrefix)
	if idx == -1 {
		// Fallback to Go's time package if we can't parse the path
		name, _ := time.Now().Zone()
		return name, nil
	}

	return tzFull[idx+len(zonePrefix):], nil
}

var zoneNames []string

// fillZoneNamesFromTZFiles ... read timezone file and append into timeZones slice
func fillZoneNamesFromTZFiles(zonePath string) {
	files, _ := os.ReadDir(zonePath)
	for _, f := range files {
		if f.Name() != strings.ToUpper(f.Name()[:1])+f.Name()[1:] {
			continue
		}
		if f.IsDir() {
			fillZoneNamesFromTZFiles(filepath.Join(zonePath, f.Name()))
		} else {
			tz := filepath.Join(zonePath, f.Name())[1:]
			if !slices.Contains(zoneNames, tz) {
				// convert string to rune
				tzRune, _ := utf8.DecodeRuneInString(tz[:1])

				if syntax.IsWordChar(tzRune) { // filter out entry that does not start with A-Za-z such as +VERSION
					zoneNames = append(zoneNames, tz)
				}
			}
		}
	}

}

type ZoneInfo struct {
	DisplayText string
	Name        string
	WindowsName string
	Offset      int
}

var TimeZones []ZoneInfo

// ConvertOffsetToIANAName returns a timezone name matching the given offset string (e.g. "+04")
func ConvertOffsetToIANAName(offset int) (string, error) {
	_, nowOffset := time.Now().Zone()
	if nowOffset == offset {
		tz, err := GetCurrentTimeZone()
		if err != nil {
			return "", err
		}
		return tz.Name, nil
	}
	// Find matching timezone
	for _, tz := range TimeZones {
		if tz.Offset == offset {
			tzName := windowsToIANA(tz.Name)
			return tzName, nil
		}
	}

	return "", fmt.Errorf("no matching timezone found for offset %d", offset)
}

func init() {
	if runtime.GOOS == "nacl" || runtime.GOOS == "" {
		log.Error().Str("OS", runtime.GOOS).Msg("Unsupported platform for parsing timeZones")
		return
	}

	if runtime.GOOS == "windows" {
		cmd := exec.Command("tzutil", "/l")
		out, err := cmd.Output()
		if err != nil {
			log.Error().Bytes("out", out).Msg("tzutil /l return error")
			return
		}

		// Parse the output line by line
		lines := strings.Split(string(out), "\n")

		// Process lines in pairs (display name followed by Windows name)
		for i := 0; i < len(lines); i++ {
			line := strings.TrimSpace(lines[i])

			// Skip empty lines
			if line == "" {
				continue
			}

			// Check if this is a display line with UTC offset
			if strings.HasPrefix(line, "(UTC") && strings.Contains(line, ") ") {
				// Extract the timezone name and offset
				parts := strings.SplitN(line, ") ", 2)
				if len(parts) == 2 {
					offsetPart := strings.TrimPrefix(parts[0], "(UTC")
					if offsetPart == "" {
						offsetPart = "+00:00"
					}
					timezoneName := parts[1]

					// Convert offset format from "+11:00" to "+11h00m" for ParseDuration
					offsetForParsing := strings.Replace(offsetPart, ":", "h", 1) + "m"

					// Parse the offset
					offset, parseErr := time.ParseDuration(offsetForParsing)
					if parseErr != nil {
						log.Error().Err(parseErr).Str("offset", offsetPart).Msg("Failed to parse offset")
						continue
					}

					// Convert offset to minutes
					offsetMinutes := int(offset.Minutes())

					// Check if next line exists and is the Windows timezone name
					windowsName := ""
					if i+1 < len(lines) {
						nextLine := strings.TrimSpace(lines[i+1])
						if nextLine != "" && !strings.HasPrefix(nextLine, "(UTC") {
							windowsName = nextLine
							i++ // Skip the next line as we've processed it
						}
					}

					// Append to TimeZones
					TimeZones = append(TimeZones, ZoneInfo{
						DisplayText: line,
						Name:        timezoneName,
						WindowsName: windowsName,
						Offset:      offsetMinutes,
					})
				}
			}
		}
		// Sort timezones by offset
		sort.Slice(TimeZones, func(i, j int) bool {
			return TimeZones[i].Offset < TimeZones[j].Offset
		})
		return
	}

	now := time.Now()
	fillZoneNamesFromTZFiles(zoneDirs[runtime.GOOS])

	for _, tz := range zoneNames {
		location, err := time.LoadLocation(tz)
		if err != nil {
			log.Error().Err(err)
			continue
		}

		// Check timeZone
		t := now.In(location)
		offset := t.Format("-07:00")
		hours := offset[0:3]
		minutes := offset[3:]

		utc := "UTC " + fmt.Sprintf("%s:%s", hours, minutes)

		// Append to TimeZones
		TimeZones = append(TimeZones, ZoneInfo{
			DisplayText: tz + " " + utc,
			Name:        tz,
			WindowsName: "", // Non-Windows systems don't have Windows timezone names
			Offset:      (int(t.UTC().Sub(t.In(location)).Hours()) * 60) + int(t.UTC().Sub(t.In(location)).Minutes()),
		})
	}
	sort.Slice(TimeZones, func(i, j int) bool {
		return TimeZones[i].Offset < TimeZones[j].Offset
	})

}
