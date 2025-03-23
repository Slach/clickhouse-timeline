package types

import (
	"time"

	"github.com/araddon/dateparse"
)

type CLI struct {
	FromTime    string
	ToTime      string
	RangeOption string
	ConnectTo   string
	Cluster     string
	Metric      string
	Category    string
	ConfigPath  string
	LogPath     string
}

func (c *CLI) ParseToTime() (time.Time, error) {
	return dateparse.ParseAny(c.ToTime)
}
