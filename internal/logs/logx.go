package logx

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Config via env or code
type Config struct {
	Service        string // "bot" or "worker"
	Level          string // debug|info|warn|error
	Format         string // json|console
	FilePath       string // e.g. /var/log/uniqueizer/worker.log ("" = disabled)
	FileMaxSizeMB  int    // rotate at ~MB (default 50)
	FileMaxBackups int    // keep N old logs (default 3)
	FileMaxAgeDays int    // keep #days (default 7)
	FileCompress   bool   // gzip old logs (default true)
	SampleEveryN   int    // >0 enables BasicSampler (e.g., 10 = keep 1/10 logs)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" { return v }
	return def
}
func getenvInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil { return n }
	}
	return def
}
func getenvBool(k string, def bool) bool {
	if v := os.Getenv(k); v != "" {
		v = strings.ToLower(v)
		return v == "1" || v == "true" || v == "yes"
	}
	return def
}

// Build config from environment with sane defaults.
func FromEnv(service string) Config {
	return Config{
		Service:        service,
		Level:          strings.ToLower(getenv("LOG_LEVEL", "info")),
		Format:         strings.ToLower(getenv("LOG_FORMAT", "json")), // json|console
		FilePath:       getenv("LOG_FILE", ""),                        // empty = no file
		FileMaxSizeMB:  getenvInt("LOG_FILE_MAX_SIZE", 50),
		FileMaxBackups: getenvInt("LOG_FILE_MAX_BACKUPS", 3),
		FileMaxAgeDays: getenvInt("LOG_FILE_MAX_AGE", 7),
		FileCompress:   getenvBool("LOG_FILE_COMPRESS", true),
		SampleEveryN:   getenvInt("LOG_SAMPLE_EVERY", 0),
	}
}

// Setup configures zerolog global `log` and returns the logger instance.
func Setup(c Config) zerolog.Logger {
	zerolog.TimeFieldFormat = time.RFC3339
	lvl, err := zerolog.ParseLevel(c.Level)
	if err != nil { lvl = zerolog.InfoLevel }

	// Writers: stdout (+ optional console formatting) and optional rotating file
	var writers []io.Writer
	if c.Format == "console" {
		writers = append(writers, zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
		})
	} else {
		writers = append(writers, os.Stdout)
	}
	if c.FilePath != "" {
		writers = append(writers, &lumberjack.Logger{
			Filename:   c.FilePath,
			MaxSize:    c.FileMaxSizeMB,
			MaxBackups: c.FileMaxBackups,
			MaxAge:     c.FileMaxAgeDays,
			Compress:   c.FileCompress,
		})
	}
	multi := io.MultiWriter(writers...)

	logger := zerolog.New(multi).Level(lvl).With().
		Timestamp().
		Str("svc", c.Service).
		Logger()

	// Optional global sampling
	if c.SampleEveryN > 0 {
		logger = logger.Sample(&zerolog.BasicSampler{N: c.SampleEveryN})
	}

	log.Logger = logger
	return logger
}

// FromCtx attaches standard fields (if present) to the global logger.
func FromCtx(ctx context.Context) zerolog.Logger {
	l := log.Logger
	if ctx == nil { return l }
	if v := ctx.Value(CtxKeySessionID); v != nil {
		l = l.With().Str("sid", fmt.Sprint(v)).Logger()
	}
	if v := ctx.Value(CtxKeyUserID); v != nil {
		switch t := v.(type) {
		case int64:   l = l.With().Int64("uid", t).Logger()
		case int:     l = l.With().Int("uid", t).Logger()
		case string:  l = l.With().Str("uid", t).Logger()
		default:      l = l.With().Str("uid", fmt.Sprint(t)).Logger()
		}
	}
	return l
}
