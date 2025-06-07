package pprof

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/rs/zerolog/log"
)

var (
	memProfileFile *os.File
	memProfileTicker *time.Ticker
	memProfileDone chan bool
)

// Setup starts CPU and memory profiling
func Setup(pprofPath string) error {
	// Set default path if not provided
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

	// Start continuous memory profiling
	if err := startMemoryProfiling(pprofPath); err != nil {
		log.Error().Err(err).Msg("Failed to start memory profiling")
		// Don't fail the entire setup if memory profiling fails
	}

	return nil
}

// Stop stops CPU and memory profiling
func Stop(pprofPath string) {
	// Set default path if not provided
	if pprofPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			log.Error().Err(err).Msg("Failed to get user home directory for stopping profiling")
			return
		}
		pprofPath = filepath.Join(home, ".clickhouse-timeline")
	}

	// Stop CPU profiling
	pprof.StopCPUProfile()

	// Stop continuous memory profiling
	stopMemoryProfiling()

	// Write final memory profile
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

// startMemoryProfiling starts continuous memory profiling
func startMemoryProfiling(pprofPath string) error {
	// Create memory profile file for continuous profiling
	memFile := filepath.Join(pprofPath, "memory_continuous.pprof")
	f, err := os.Create(memFile)
	if err != nil {
		return fmt.Errorf("could not create continuous memory profile file: %w", err)
	}

	memProfileFile = f
	memProfileDone = make(chan bool)
	memProfileTicker = time.NewTicker(30 * time.Second) // Sample every 30 seconds

	// Start goroutine for continuous memory profiling
	go func() {
		defer memProfileFile.Close()
		
		for {
			select {
			case <-memProfileTicker.C:
				// Write memory profile sample
				runtime.GC() // Force GC to get accurate stats
				if err := pprof.WriteHeapProfile(memProfileFile); err != nil {
					log.Error().Err(err).Msg("Failed to write memory profile sample")
				}
				// Add separator for multiple samples in same file
				memProfileFile.WriteString("\n--- Memory Sample ---\n")
				
			case <-memProfileDone:
				return
			}
		}
	}()

	log.Info().Str("path", memFile).Msg("Continuous memory profiling started (30s intervals)")
	return nil
}

// stopMemoryProfiling stops continuous memory profiling
func stopMemoryProfiling() {
	if memProfileTicker != nil {
		memProfileTicker.Stop()
	}
	if memProfileDone != nil {
		close(memProfileDone)
	}
	if memProfileFile != nil {
		memProfileFile.Close()
	}
	log.Info().Msg("Continuous memory profiling stopped")
}
