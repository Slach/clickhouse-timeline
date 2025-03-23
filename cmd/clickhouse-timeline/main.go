package main

import (
	"fmt"
	"github.com/Slach/clickhouse-timeline/pkg/cli"
	"github.com/Slach/clickhouse-timeline/pkg/logging"
	"github.com/Slach/clickhouse-timeline/pkg/types"
	"github.com/spf13/cobra"
	"os"
)

var version = "dev"

func main() {
	logging.InitConsoleStdErrLog()
	cliInstance := &types.CLI{}
	rootCmd := cli.NewRootCommand(cliInstance)
	rootCmd.Run = func(cmd *cobra.Command, args []string) {
		cli.RunRootCommand(cliInstance, version, cmd, args)
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
