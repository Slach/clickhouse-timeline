package main

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"os"

	"github.com/Slach/clickhouse-timeline/pkg/cli"
	"github.com/Slach/clickhouse-timeline/pkg/logging"
	"github.com/Slach/clickhouse-timeline/pkg/pprof"
	"github.com/Slach/clickhouse-timeline/pkg/types"
)

var version = "dev"

func main() {
	logging.InitConsoleStdErrLog()
	cliInstance := &types.CLI{}
	rootCmd := cli.NewRootCommand(cliInstance, version)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
