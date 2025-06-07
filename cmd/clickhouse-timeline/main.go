package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"

	"github.com/Slach/clickhouse-timeline/pkg/cli"
	"github.com/Slach/clickhouse-timeline/pkg/logging"
	"github.com/Slach/clickhouse-timeline/pkg/types"
	"github.com/rs/zerolog/log"
)

var version = "dev"

func main() {
	logging.InitConsoleStdErrLog()
	cliInstance := &types.CLI{}
	rootCmd := cli.NewRootCommand(cliInstance, version)

	// Setup profiling if pprof path is provided
	if cliInstance.PprofPath != "" {
		if err := setupProfiling(cliInstance.PprofPath); err != nil {
			fmt.Printf("Failed to setup profiling: %v\n", err)
			os.Exit(1)
		}
		defer stopProfiling(cliInstance.PprofPath)
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func setupProfiling(pprofPath string) error {
	// Expand tilde to home directory if needed
	if pprofPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("failed to get user home directory: %w", err)
		}
		pprofPath = filepath.Join(home, ".clickhouse-timeline")
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(pprofPath, 0755); err != nil {
		return fmt.Errorf("failed to create pprof directory: %w", err)
	}

	// Start CPU profiling
	cpuFile := filepath.Join(pprofPath, "cpu.pprof")
	f, err := os.Create(cpuFile)
	if err != nil {
		return fmt.Errorf("could not create CPU profile file: %w", err)
	}

	if err := pprof.StartCPUProfile(f); err != nil {
		f.Close()
		return fmt.Errorf("could not start CPU profile: %w", err)
	}

	log.Info().Str("path", cpuFile).Msg("CPU profiling started")
	return nil
}

func stopProfiling(pprofPath string) {
	// Stop CPU profiling
	pprof.StopCPUProfile()

	// Write memory profile
	memFile := filepath.Join(pprofPath, "memory.pprof")
	f, err := os.Create(memFile)
	if err != nil {
		log.Error().Err(err).Str("path", memFile).Msg("Could not create memory profile file")
		return
	}
	defer f.Close()

	runtime.GC() // Get up-to-date statistics
	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Error().Err(err).Str("path", memFile).Msg("Could not write memory profile")
		return
	}

	log.Info().Str("path", memFile).Msg("Memory profile written")
}
