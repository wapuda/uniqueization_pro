package logx

import (
	"bufio"
	"io"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// LineWriter turns stream output into per-line zerolog events at a given level.
type LineWriter struct {
	logger zerolog.Logger
	level  zerolog.Level
}

func NewLineWriter(fields map[string]string, level zerolog.Level) *LineWriter {
	l := log.Logger
	w := l.With()
	for k, v := range fields {
		w = w.Str(k, v)
	}
	return &LineWriter{logger: w.Logger(), level: level}
}

func (lw *LineWriter) Pipe(r io.Reader) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		switch lw.level {
		case zerolog.DebugLevel:
			lw.logger.Debug().Msg(sc.Text())
		case zerolog.ErrorLevel:
			lw.logger.Error().Msg(sc.Text())
		default:
			lw.logger.Info().Msg(sc.Text())
		}
	}
}
