package main

import (
	"os"
	"path/filepath"

	"github.com/Slach/clickhouse-timeline/pkg/config"
	"github.com/Slach/clickhouse-timeline/pkg/logging"
	"github.com/Slach/clickhouse-timeline/pkg/tui"
	"github.com/rs/zerolog/log"
)

var version = "dev"

func main() {
	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatal().Stack().Err(err).Msg("failed to get user home directory")
	}

	// Initialize logging
	if err = logging.InitLogFile(
		filepath.Join(home, ".clickhouse-timeline", "clickhouse-timeline.log"),
		version,
	); err != nil {
		log.Fatal().Stack().Err(err).Msg("failed to initialize logger")
	}

	configPath := filepath.Join(home, "clickhouse-timeline.yml")
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatal().Stack().Err(err)
	}

	app := tui.NewApp(cfg, version)
	if err := app.Run(); err != nil {
		log.Fatal().Stack().Err(err)
	}
}
