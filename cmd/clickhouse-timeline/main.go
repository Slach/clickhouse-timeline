package main

import (
	"fmt"
	"github.com/Slach/clickhouse-timeline/pkg/cli"
	"github.com/Slach/clickhouse-timeline/pkg/logging"
	"github.com/Slach/clickhouse-timeline/pkg/types"
	"os"
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
