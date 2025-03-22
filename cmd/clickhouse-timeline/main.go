package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/Slach/clickhouse-timeline/pkg/config"
	"github.com/Slach/clickhouse-timeline/pkg/logging"
	"github.com/Slach/clickhouse-timeline/pkg/tui"
	"github.com/rs/zerolog/log"
)

var version = "dev"

func main() {
	logging.InitConsoleStdErrLog()
	home, homeErr := os.UserHomeDir()
	if homeErr != nil {
		fmt.Println(homeErr.Error())
		log.Fatal().Stack().Err(homeErr).Msg("failed to get user home directory")
	}
	home = filepath.Join(home, ".clickhouse-timeline")

	// Initialize logging
	if initLogErr := logging.InitLogFile(
		filepath.Join(home, "clickhouse-timeline.log"),
		version,
	); initLogErr != nil {
		fmt.Println(initLogErr.Error())
		log.Fatal().Stack().Err(initLogErr).Msg("failed to initialize logger")
	}

	configPath := filepath.Join(home, "clickhouse-timeline.yml")
	cfg, configErr := config.Load(configPath)
	if configErr != nil {
		fmt.Println(configErr.Error())
		log.Fatal().Stack().Err(configErr).Send()
	}

	app := tui.NewApp(cfg, version)
	if runErr := app.Run(); runErr != nil {
		fmt.Println(runErr.Error())
		log.Fatal().Stack().Err(runErr).Send()
	}
}
