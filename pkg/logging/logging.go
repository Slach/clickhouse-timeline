package logging

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/Slach/clickhouse-timeline/pkg/types"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	_ "github.com/rs/zerolog/pkgerrors"
)

func InitConsoleStdErrLog() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	// ErrorStackMarshaler tries to extract a short, useful stack trace for logging.
	// First try to use pkg/errors StackTrace if present (for wrapped errors).
	// If not present, fall back to capturing runtime.Callers at the point of logging
	// so .Stack() produces something even for errors that don't carry a stack trace.
	zerolog.ErrorStackMarshaler = func(err error) interface{} {
		// Preferred: pkg/errors stacktrace extraction (if the error was created with pkg/errors).
		if stackErr, ok := err.(interface{ StackTrace() errors.StackTrace }); ok {
			st := stackErr.StackTrace()
			if len(st) > 0 {
				// Format the first frame
				frame := fmt.Sprintf("%+v", st[0])
				parts := strings.Split(frame, "\n\t")
				if len(parts) >= 2 {
					// Extract file:line and function name
					fileLine := strings.TrimPrefix(parts[0], "github.com/Slach/clickhouse-timeline/")
					funcName := strings.TrimPrefix(parts[1], "github.com/Slach/clickhouse-timeline/")
					return fmt.Sprintf("%s > %s", fileLine, funcName)
				}
			}
		}

		// Fallback: capture a short runtime stack from the point where the marshaler is invoked.
		// Skip a few frames to avoid showing internal zerolog/runtime frames.
		const maxFrames = 10
		pcs := make([]uintptr, maxFrames)
		// Skip 3 frames: runtime.Callers, this function, and zerolog internals that call into marshaler.
		n := runtime.Callers(3, pcs)
		if n == 0 {
			return nil
		}
		var b strings.Builder
		for i := 0; i < n; i++ {
			pc := pcs[i] - 1
			fn := runtime.FuncForPC(pc)
			if fn == nil {
				continue
			}
			file, line := fn.FileLine(pc)
			file = strings.TrimPrefix(file, "github.com/Slach/clickhouse-timeline/")
			// Use a compact representation: file:line > function;
			fmt.Fprintf(&b, "%s:%d > %s; ", file, line, fn.Name())
		}
		s := b.String()
		if s == "" {
			return nil
		}
		return s
	}
	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		return strings.TrimPrefix(file, "github.com/Slach/clickhouse-timeline/") + ":" + strconv.Itoa(line)
	}

	// First setup basic console logging in case we fail
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).
		With().
		Timestamp().
		Caller(). // Normal caller info for regular logging
		Logger()
}

// fatalStackHook adds stack traces to Fatal level logs
type fatalStackHook struct{}

func (h fatalStackHook) Run(e *zerolog.Event, level zerolog.Level, _ string) {
	if level == zerolog.FatalLevel {
		e.Stack() // Add stack trace only for Fatal level
	}
}

func InitLogFile(cliInstance *types.CLI, version string) error {
	logPath := ""
	if cliInstance != nil && cliInstance.LogPath != "" {
		logPath = cliInstance.LogPath
	}
	// If no path provided, use default in home directory
	if logPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return errors.Wrap(err, "failed to get user home directory")
		}
		logPath = filepath.Join(home, ".clickhouse-timeline", "clickhouse-timeline.log")
	}

	// Ensure log directory exists
	logDir := filepath.Dir(logPath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return errors.Wrap(err, "failed to create log directory")
	}

	// Open log file
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrap(err, "failed to open log file")
	}

	// Now set up the proper file logging
	consoleWriter := zerolog.ConsoleWriter{
		Out:        logFile,
		NoColor:    true,
		TimeFormat: "2006-01-02 15:04:05.000",
	}

	// Create base logger with normal caller info
	baseLogger := zerolog.New(zerolog.SyncWriter(consoleWriter)).
		With().
		Timestamp().
		Caller().
		Str("version", version).
		Logger()

	baseLogger.Hook(fatalStackHook{})

	// Wrap the logger to add stack traces for Fatal level
	log.Logger = baseLogger

	return nil
}
