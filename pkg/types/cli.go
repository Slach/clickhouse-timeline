package types

import (
	"time"

	"github.com/araddon/dateparse"
)

type CLI struct {
	FromTime         string
	ToTime           string
	RangeOption      string
	ConnectTo        string
	Cluster          string
	Metric           string
	Category         string
	ConfigPath       string
	LogPath          string
	LogLevel         string
	Pprof            bool
	PprofPath        string
	FlamegraphNative bool
	DisableMouse     bool
	LogsParams       LogsParams
}

type LogsParams struct {
	Database  string
	Table     string
	Message   string
	Time      string
	TimeMs    string
	Date      string
	Level     string
	Window    int
}

func (c *CLI) ParseToTime() (time.Time, error) {
	return dateparse.ParseAny(c.ToTime)
}
