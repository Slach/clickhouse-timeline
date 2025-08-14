package logging

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/Slach/clickhouse-timeline/pkg/types"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	_ "github.com/rs/zerolog/pkgerrors"
)

const mainPackage = "github.com/Slach/clickhouse-timeline/"

/* Formatting is now handled by zerolog.ConsoleWriter in InitLogFile so the custom
   prettyWriter implementation has been removed. */

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
					fileLine := strings.TrimPrefix(parts[0], mainPackage)
					funcName := strings.TrimPrefix(parts[1], mainPackage)
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
			file = strings.TrimPrefix(file, mainPackage)
			// Use a compact representation: file:line > function;
			_, _ = fmt.Fprintf(&b, "%s:%d > %s\n", file, line, strings.TrimPrefix(fn.Name(), mainPackage))
		}
		s := b.String()
		if s == "" {
			return nil
		}
		return s
	}
	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		return strings.TrimPrefix(file, mainPackage) + ":" + strconv.Itoa(line)
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

	// Now set up the proper file logging using zerolog.ConsoleWriter.
	// ConsoleWriter will print a pretty, non-quoted representation while preserving time.
	out := zerolog.ConsoleWriter{
		Out:        logFile,
		NoColor:    true,
		TimeFormat: time.RFC3339Nano,
		// Put parts in the order we prefer and avoid duplicating error fields in the trailing section.
		PartsOrder:    []string{zerolog.TimestampFieldName, zerolog.LevelFieldName, zerolog.CallerFieldName, zerolog.MessageFieldName, zerolog.ErrorFieldName, zerolog.ErrorStackFieldName},
		FieldsExclude: []string{zerolog.ErrorFieldName, zerolog.ErrorStackFieldName},
	}

	// Remove unnecessary quoting and control formatting of parts.
	out.FormatMessage = func(i interface{}) string { return fmt.Sprint(i) }
	out.FormatFieldName = func(i interface{}) string { return fmt.Sprintf("%s=", i) }
	out.FormatFieldValue = func(i interface{}) string { return fmt.Sprint(i) }

	// Make stack trace multiline & readable when present.
	out.FormatPartValueByName = func(v interface{}, name string) string {
		if name == zerolog.ErrorStackFieldName {
			if frames, ok := v.([]interface{}); ok {
				var b strings.Builder
				for _, fr := range frames {
					if m, ok := fr.(map[string]interface{}); ok {
						// keys: "func", "source", "line"
						fmt.Fprintf(&b, "\n    at %s (%v:%v)", m["func"], m["source"], m["line"])
					}
				}
				return b.String()
			}
		}
		return fmt.Sprint(v)
	}

	// Build logger that writes pretty output to the file (with timestamp and caller).
	baseLogger := zerolog.New(out).
		With().
		Timestamp().
		Caller().
		Str("version", version).
		Logger()

	baseLogger.Hook(fatalStackHook{})

	// Use this logger for package-global logging.
	log.Logger = baseLogger

	return nil
}
