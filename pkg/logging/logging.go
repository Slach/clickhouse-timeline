package logging

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	_ "github.com/rs/zerolog/pkgerrors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func InitConsoleStdErrLog() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	zerolog.ErrorStackMarshaler = func(err error) interface{} {
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
		return nil
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

func InitLogFile(logPath, version string) error {

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

	// Now setup the proper file logging
	consoleWriter := zerolog.ConsoleWriter{
		Out:        logFile,
		NoColor:    true,
		TimeFormat: "2006-01-02 15:04:05.000",
	}

	// Create base logger with normal caller info
	baseLogger := zerolog.New(zerolog.SyncWriter(consoleWriter)).
		With().
		Timestamp().
		Caller(). // Normal caller info
		Str("version", version).
		Logger()

	baseLogger.Hook(fatalStackHook{})

	// Wrap the logger to add stack traces for Fatal level
	log.Logger = baseLogger

	return nil
}
