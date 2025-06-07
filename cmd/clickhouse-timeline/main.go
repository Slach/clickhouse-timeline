package main

import (
	"fmt"
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

	// Setup profiling if enabled
	if cliInstance.Pprof {
		if err := pprof.Setup(cliInstance.PprofPath); err != nil {
			fmt.Printf("Failed to setup profiling: %v\n", err)
			os.Exit(1)
		}
		defer pprof.Stop(cliInstance.PprofPath)
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
