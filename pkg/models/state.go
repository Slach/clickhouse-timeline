package models

import (
	"strconv"
	"strings"
	"time"

	"github.com/Slach/clickhouse-timeline/pkg/client"
	"github.com/Slach/clickhouse-timeline/pkg/config"
	"github.com/Slach/clickhouse-timeline/pkg/types"
)

// AppState holds the core application state
// This is separated from UI state to enable better testing and business logic separation
type AppState struct {
	// Configuration
	Config    *config.Config
	Version   string
	CHVersion string // ClickHouse server version (e.g. "26.4.1.1234")
	CLI       *types.CLI

	// Connection state
	ClickHouse      *client.Client
	Cluster         string
	SelectedContext *config.Context

	// Time range state
	FromTime        time.Time
	ToTime          time.Time
	InitialFromTime time.Time
	InitialToTime   time.Time

	// Heatmap configuration
	CategoryType  string
	HeatmapMetric string
	ScaleType     string

	// Flamegraph configuration
	CategoryValue       string
	FlamegraphTimeStamp time.Time
	FlamegraphNative    bool

	// Log panel state (will be expanded when migrating logs_panel_handler.go)
	LogPanelState interface{}
}

// NewAppState creates a new application state with default values
func NewAppState(cfg *config.Config, version string) *AppState {
	now := time.Now()
	return &AppState{
		Config:          cfg,
		Version:         version,
		CLI:             &types.CLI{},
		FromTime:        now.Add(-24 * time.Hour), // Default: 24 hours ago
		ToTime:          now,                      // Default: now
		InitialFromTime: now.Add(-24 * time.Hour), // Store initial range
		InitialToTime:   now,                      // Store initial range
	}
}

// ConnectionInfo returns formatted connection information
func (s *AppState) ConnectionInfo() string {
	if s.SelectedContext == nil {
		return ""
	}
	// This will be populated with business logic from connect handler
	return ""
}

// IsConnected returns true if there is an active connection
func (s *AppState) IsConnected() bool {
	return s.ClickHouse != nil && s.SelectedContext != nil
}

// CHVersionAtLeast checks if the connected ClickHouse server version is >= major.minor
// Handles version strings like "26.4.1.1234" or "25.12.3.1-lts"
func (s *AppState) CHVersionAtLeast(major, minor int) bool {
	if s.CHVersion == "" {
		return false
	}
	parts := strings.SplitN(s.CHVersion, ".", 3)
	if len(parts) < 2 {
		return false
	}
	vMajor, err := strconv.Atoi(parts[0])
	if err != nil {
		return false
	}
	vMinor, err := strconv.Atoi(strings.Split(parts[1], "-")[0])
	if err != nil {
		return false
	}
	return vMajor > major || (vMajor == major && vMinor >= minor)
}

// TimeRangeFormatted returns formatted time range string
func (s *AppState) TimeRangeFormatted() string {
	return "From: " + s.FromTime.Format("2006-01-02 15:04:05 -07:00") +
		" To: " + s.ToTime.Format("2006-01-02 15:04:05 -07:00")
}
