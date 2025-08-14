package logging

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/Slach/clickhouse-timeline/pkg/types"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	_ "github.com/rs/zerolog/pkgerrors"
)

const mainPackage = "github.com/Slach/clickhouse-timeline/"

// prettyWriter implements io.Writer and pretty-prints zerolog JSON events to a text writer.
type prettyWriter struct {
	Out io.Writer
}

func (w *prettyWriter) Write(p []byte) (int, error) {
	// Trim surrounding whitespace/newline
	b := bytes.TrimSpace(p)

	// Try to unmarshal JSON into raw messages for stable handling of fields.
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		// Fallback: write raw bytes if we can't parse JSON.
		return w.Out.Write(p)
	}

	// Extract common fields.
	var ts, level, message, caller string
	_ = json.Unmarshal(m["time"], &ts)
	_ = json.Unmarshal(m["level"], &level)
	_ = json.Unmarshal(m["message"], &message)
	_ = json.Unmarshal(m["caller"], &caller)

	// Prepare deterministic key order for remaining fields.
	keys := make([]string, 0, len(m))
	for k := range m {
		if k == "time" || k == "level" || k == "message" || k == "caller" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var out strings.Builder
	// Header: timestamp LEVEL [caller > ]message
	if ts != "" {
		out.WriteString(ts)
		out.WriteString(" ")
	}
	if level != "" {
		out.WriteString(strings.ToUpper(level))
		out.WriteString(" ")
	}
	if caller != "" {
		out.WriteString(caller)
		out.WriteString(" > ")
	}
	out.WriteString(message)

	// Append other fields. Strings that contain newlines are printed as
	// a multiline block (key= newline + raw value).
	for _, k := range keys {
		raw := m[k]
		// Try as string first.
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			// json.Unmarshal already unescaped \n sequences into real newlines.
			// Trim a trailing newline to avoid double blank lines.
			s = strings.TrimSuffix(s, "\n")
			if strings.Contains(s, "\n") {
				out.WriteString(" ")
				out.WriteString(k)
				out.WriteString("=")
				out.WriteString("\n")
				out.WriteString(s)
				out.WriteString("\n")
				continue
			}
			out.WriteString(" ")
			out.WriteString(k)
			out.WriteString("=")
			out.WriteString(s)
			continue
		}

		// Non-string fields: unmarshal into interface{} and format.
		var iv interface{}
		if err := json.Unmarshal(raw, &iv); err == nil {
			out.WriteString(" ")
			out.WriteString(k)
			out.WriteString("=")
			out.WriteString(fmt.Sprint(iv))
			continue
		}

		// As a last resort, write the raw bytes.
		out.WriteString(" ")
		out.WriteString(k)
		out.WriteString("=")
		out.Write(raw)
	}

	out.WriteString("\n")
	sout := out.String()
	return w.Out.Write([]byte(sout))
}

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

	// Now set up the proper file logging.
	// Use our prettyWriter (defined at package level) that converts zerolog JSON
	// events into readable text blocks and preserves multiline fields.
	// Instantiate it with the opened log file.
	pw := &prettyWriter{Out: logFile}

	// Create base logger using our prettyWriter wrapped with zerolog.SyncWriter.
	baseLogger := zerolog.New(zerolog.SyncWriter(&prettyWriter{Out: logFile})).
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
