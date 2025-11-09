package models

import (
	"time"

	"github.com/Slach/clickhouse-timeline/pkg/client"
	"github.com/Slach/clickhouse-timeline/pkg/config"
	"github.com/Slach/clickhouse-timeline/pkg/types"
)

// AppState holds the core application state
// This is separated from UI state to enable better testing and business logic separation
type AppState struct {
	// Configuration
	Config  *config.Config
	Version string
	CLI     *types.CLI

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

// TimeRangeFormatted returns formatted time range string
func (s *AppState) TimeRangeFormatted() string {
	return "From: " + s.FromTime.Format("2006-01-02 15:04:05 -07:00") +
		" To: " + s.ToTime.Format("2006-01-02 15:04:05 -07:00")
}
