package logging

import (
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"path/filepath"
)

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

	consoleWriter := zerolog.ConsoleWriter{
		Out:        logFile,
		NoColor:    true,
		TimeFormat: "2006-01-02 15:04:05.000",
	}

	log.Logger = zerolog.New(zerolog.SyncWriter(consoleWriter)).
		With().
		Timestamp().
		Caller().
		Str("version", version).
		Logger()

	return nil
}
