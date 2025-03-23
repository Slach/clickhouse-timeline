package cli

import (
	"context"
	"fmt"
	"github.com/Slach/clickhouse-timeline/pkg/config"
	"github.com/Slach/clickhouse-timeline/pkg/logging"
	"github.com/rs/zerolog/log"
	"os"
	"path/filepath"

	"github.com/Slach/clickhouse-timeline/pkg/tui"
	"github.com/Slach/clickhouse-timeline/pkg/types"
	"github.com/spf13/cobra"
)

func NewRootCommand(cli *types.CLI) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "clickhouse-timeline",
		Short: "ClickHouse Timeline - interactive performance analysis tool",
	}

	heatmapCmd := &cobra.Command{
		Use:   "heatmap",
		Short: "Start in heatmap mode",
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunSubCommand(cli, cmd, args)
		},
	}

	flamegraphCmd := &cobra.Command{
		Use:   "flamegraph",
		Short: "Start in flamegraph mode",
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunSubCommand(cli, cmd, args)
		},
	}

	// Add global flags
	rootCmd.PersistentFlags().StringVar(&cli.ConfigPath, "config", "", "Path to config file (default: ~/.clickhouse-timeline/clickhouse-timeline.yml)")
	rootCmd.PersistentFlags().StringVar(&cli.LogPath, "log", "", "Path to log file (default: ~/.clickhouse-timeline/clickhouse-timeline.log)")
	rootCmd.PersistentFlags().StringVar(&cli.FromTime, "from", "", "Start time (in any parsable format, see https://github.com/araddon/dateparse)")
	rootCmd.PersistentFlags().StringVar(&cli.ToTime, "to", "", "End time (in any parsable format, see https://github.com/araddon/dateparse)")
	rootCmd.PersistentFlags().StringVar(&cli.RangeOption, "range", "", "Predefined time range (e.g. 1h, 24h, 7d)")
	rootCmd.PersistentFlags().StringVar(&cli.ConnectTo, "connect", "", "Connection name to use from config")
	rootCmd.PersistentFlags().StringVar(&cli.Cluster, "cluster", "", "Cluster name to analyze")
	rootCmd.PersistentFlags().StringVar(&cli.Metric, "metric", "", "Metric to visualize (count, memoryUsage, cpuUsage, etc)")
	rootCmd.PersistentFlags().StringVar(&cli.Category, "category", "", "Category to group by (query_hash, tables, hosts)")

	// Add subcommands
	rootCmd.AddCommand(heatmapCmd)
	rootCmd.AddCommand(flamegraphCmd)

	return rootCmd
}

func RunRootCommand(cliInstance *types.CLI, version string, cmd *cobra.Command, args []string) {
	// Get CLI instance from command context
	if cliInstance == nil {
		if cliValue := cmd.Context().Value("cli"); cliValue != nil {
			if instance, ok := cliValue.(*types.CLI); ok {
				cliInstance = instance
			}
		}
	}

	// Set default paths
	home, homeErr := os.UserHomeDir()
	if homeErr != nil {
		fmt.Println(homeErr.Error())
		log.Fatal().Stack().Err(homeErr).Msg("failed to get user home directory")
	}
	home = filepath.Join(home, ".clickhouse-timeline")

	// Initialize logging
	if initLogErr := logging.InitLogFile(cliInstance, version); initLogErr != nil {
		fmt.Println(initLogErr.Error())
		log.Fatal().Stack().Err(initLogErr).Msg("failed to initialize logger")
	}

	cfg, configErr := config.Load(cliInstance, home)
	if configErr != nil {
		fmt.Println(configErr.Error())
		log.Fatal().Stack().Err(configErr).Send()
	}

	app := tui.NewApp(cfg, version)

	// Get CLI instance from command context
	if cliInstance != nil {
		app.ApplyCLIParameters(cliInstance, cmd.Name())
	}

	if runErr := app.Run(); runErr != nil {
		fmt.Println(runErr.Error())
		log.Fatal().Stack().Err(runErr).Send()
	}
}

func RunSubCommand(c *types.CLI, cmd *cobra.Command, args []string) error {
	// Create a new context with the CLI instance if the current context is nil
	ctx := context.Background()
	if cmd.Context() != nil {
		ctx = cmd.Context()
	}

	// Store the CLI instance in the context
	ctx = context.WithValue(ctx, "cli", c)

	// Set the context on both the current command and root command
	cmd.SetContext(ctx)
	cmd.Root().SetContext(ctx)

	// Execute the root command with this context
	return cmd.Root().ExecuteContext(ctx)
}
