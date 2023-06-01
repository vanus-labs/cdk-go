// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"context"
	"io"
	"os"
	"strings"

	"github.com/rs/zerolog"
)

var lg zerolog.Logger
var lvl zerolog.Level

func init() {
	out := zerolog.NewConsoleWriter()
	out.TimeFormat = zerolog.TimeFieldFormat
	lg = zerolog.New(out).With().Timestamp().Caller().Logger()
	level := os.Getenv("VANUS_LOG_LEVEL")
	SetLogLevel(level)
}

func SetLogLevel(level string) {
	switch strings.ToLower(level) {
	case "debug":
		lvl = zerolog.DebugLevel
	case "info":
		lvl = zerolog.InfoLevel
	case "warn":
		lvl = zerolog.WarnLevel
	case "error":
		lvl = zerolog.ErrorLevel
	case "fatal":
		lvl = zerolog.FatalLevel
	default:
		lvl = zerolog.InfoLevel
	}
	lg.Level(lvl)
}

func SetOutput(w io.Writer) {
	lg = lg.Output(w)
}

func GetLogger() zerolog.Logger {
	return lg.With().Logger()
}

func Debug() *zerolog.Event {
	return lg.Debug()
}

func Info() *zerolog.Event {
	return lg.Info()
}

func Warn() *zerolog.Event {
	return lg.Warn()
}

func Error() *zerolog.Event {
	return lg.Error()
}

func NewConnectorLog(connectorID string) zerolog.Logger {
	return lg.With().Str(KeyConnectorID, connectorID).Logger().Level(lvl)
}

type loggerKey struct{}

func WithLogger(ctx context.Context, logger zerolog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

func FromContext(ctx context.Context) zerolog.Logger {
	if logger, ok := ctx.Value(loggerKey{}).(zerolog.Logger); ok {
		return logger
	}
	return lg
}
