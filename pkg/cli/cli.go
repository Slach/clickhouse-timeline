package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Slach/clickhouse-timeline/pkg/config"
	"github.com/Slach/clickhouse-timeline/pkg/logging"
	"github.com/rs/zerolog/log"

	"github.com/Slach/clickhouse-timeline/pkg/tui"
	"github.com/Slach/clickhouse-timeline/pkg/types"
	"github.com/spf13/cobra"
)

func NewRootCommand(cli *types.CLI, version string) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "clickhouse-timeline",
		Short: "ClickHouse Timeline - interactive performance analysis tool",
	}
	rootCmd.RunE = func(cmd *cobra.Command, args []string) error {
		return RunRootCommand(cli, version, cmd, args)
	}

	// Add global flags
	rootCmd.PersistentFlags().StringVar(&cli.ConfigPath, "config", "", "Path to config file (default: ~/.clickhouse-timeline/clickhouse-timeline.yml)")
	rootCmd.PersistentFlags().StringVar(&cli.LogPath, "log", "", "Path to log file (default: ~/.clickhouse-timeline/clickhouse-timeline.log)")
	rootCmd.PersistentFlags().StringVar(&cli.FromTime, "from", "", "Start time (in any parsable format, see https://github.com/araddon/dateparse)")
	rootCmd.PersistentFlags().StringVar(&cli.ToTime, "to", "", "End time (in any parsable format, see https://github.com/araddon/dateparse)")
	rootCmd.PersistentFlags().StringVar(&cli.RangeOption, "range", "", "Predefined time range (e.g. 1h, 24h, 7d)")
	rootCmd.PersistentFlags().StringVar(&cli.ConnectTo, "connect", "", "ClickHouse connection context name from config")
	rootCmd.PersistentFlags().StringVar(&cli.Cluster, "cluster", "", "Cluster name to analyze")
	rootCmd.PersistentFlags().StringVar(&cli.Metric, "metric", "", "Metric to visualize (count, memoryUsage, cpuUsage, etc)")
	rootCmd.PersistentFlags().StringVar(&cli.Category, "category", "", "Category to group by (query_hash, tables, hosts, errors)")
	rootCmd.PersistentFlags().BoolVar(&cli.FlamegraphNative, "flamegraph-native", false, "Use native flamegraph viewer instead of flamelens")

	heatmapCmd := &cobra.Command{
		Use:   "heatmap",
		Short: "Start in heatmap mode",
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunSubCommand(cli, version, cmd, args)
		},
	}
	rootCmd.AddCommand(heatmapCmd)

	flamegraphCmd := &cobra.Command{
		Use:   "flamegraph",
		Short: "Start in flamegraph mode",
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunSubCommand(cli, version, cmd, args)
		},
	}
	rootCmd.AddCommand(flamegraphCmd)

	profileEventsCmd := &cobra.Command{
		Use:   "profile_events",
		Short: "Start in profile events mode",
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunSubCommand(cli, version, cmd, args)
		},
	}
	rootCmd.AddCommand(profileEventsCmd)

	metricLogCmd := &cobra.Command{
		Use:   "metric_log",
		Short: "Start in metric_log mode",
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunSubCommand(cli, version, cmd, args)
		},
	}
	rootCmd.AddCommand(metricLogCmd)

	asyncMetricLogCmd := &cobra.Command{
		Use:   "asynchronous_metric_log",
		Short: "Start in asynchronous_metric_log mode",
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunSubCommand(cli, version, cmd, args)
		},
	}
	rootCmd.AddCommand(asyncMetricLogCmd)

	logsCmd := &cobra.Command{
		Use:   "logs",
		Short: "Start in logs mode (text_log, error_log, query_log, query_thread_log)",
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunSubCommand(cli, version, cmd, args)
		},
	}
	logsCmd.Flags().StringVar(&cli.Database, "database", "", "Database which will use for look log")
	logsCmd.Flags().StringVar(&cli.Table, "table", "", "Database which will use for look log")
	logsCmd.Flags().StringVar(&cli.Message, "message", "", "Column name for message")
	logsCmd.Flags().StringVar(&cli.Time, "time", "", "Column name for time")
	logsCmd.Flags().StringVar(&cli.TimeMs, "time-ms", "", "Column name for time with milliseconds")
	logsCmd.Flags().StringVar(&cli.Date, "date", "", "Column name for date")
	logsCmd.Flags().StringVar(&cli.Level, "level", "", "Column name for message level")
	logsCmd.Flags().IntVar(&cli.Window, "window", 1000, "Sliding window size in milliseconds")
	rootCmd.AddCommand(logsCmd)

	return rootCmd
}

func RunRootCommand(cliInstance *types.CLI, version string, cmd *cobra.Command, args []string) error {
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
		app.cli = cliInstance // Store the CLI instance
		app.ApplyCLIParameters(cliInstance, cmd.Name())
	}

	if runErr := app.Run(); runErr != nil {
		log.Error().Stack().Err(runErr).Send()
		return runErr
	}
	return nil
}

func RunSubCommand(c *types.CLI, version string, cmd *cobra.Command, args []string) error {
	// Create a new context with the CLI instance if the current context is nil
	ctx := context.Background()
	if cmd.Context() != nil {
		ctx = cmd.Context()
	}

	// Store the CLI instance in the context
	ctx = context.WithValue(ctx, "cli", c)

	// Set the context on the current command
	cmd.SetContext(ctx)

	// Run the root command directly instead of executing again
	return RunRootCommand(c, version, cmd, args)
}
