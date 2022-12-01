/*
Copyright 2022-Present The Vance Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package log

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

type Logger interface {
	Debug(msg string, fields map[string]interface{})
	Info(msg string, fields map[string]interface{})
	Warning(msg string, fields map[string]interface{})
	Error(msg string, fields map[string]interface{})
	Fatal(msg string, fields map[string]interface{})
	SetLevel(level string)
	SetLogWriter(writer io.Writer)
	SetName(name string)
}

func init() {
	logger := logrus.New()
	logger.Formatter = &logrus.TextFormatter{TimestampFormat: time.RFC3339Nano, FullTimestamp: true}
	r := &defaultLogger{
		logger: logger,
	}
	level := os.Getenv("LOG_LEVEL")
	if level == "" {
		level = "INFO"
	}
	r.SetLevel(level)
	vLog = r
	vLog.Info("logger level is set", map[string]interface{}{
		"log_level": level,
	})
}

var vLog Logger

func NewLogger() Logger {
	l := &defaultLogger{
		logger: logrus.New(),
	}
	level := os.Getenv("LOG_LEVEL")
	l.SetLevel(level)
	return l
}

type defaultLogger struct {
	logger *logrus.Logger
	name   string
}

func (l *defaultLogger) SetName(name string) {
	l.name = name
}

func (l *defaultLogger) Debug(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Debug(msg)
}

func (l *defaultLogger) Info(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Info(msg)
}

func (l *defaultLogger) Warning(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Warning(msg)
}

func (l *defaultLogger) Error(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Error(msg)
}

func (l *defaultLogger) Fatal(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Fatal(msg)
}

func (l *defaultLogger) SetLevel(level string) {
	switch strings.ToLower(level) {
	case "debug":
		l.logger.SetLevel(logrus.DebugLevel)
	case "warn":
		l.logger.SetLevel(logrus.WarnLevel)
	case "error":
		l.logger.SetLevel(logrus.ErrorLevel)
	case "fatal":
		l.logger.SetLevel(logrus.FatalLevel)
	default:
		l.logger.SetLevel(logrus.InfoLevel)
	}
}

func (l *defaultLogger) SetLogWriter(writer io.Writer) {
	l.logger.Out = writer
	return
}

// SetLogger use specified logger user customized, in general, we suggest user to replace the default logger with specified
func SetLogger(logger Logger) {
	vLog = logger
}
func SetLogLevel(level string) {
	if level == "" {
		return
	}
	vLog.SetLevel(level)
}

func SetLogWriter(writer io.Writer) {
	if writer == nil {
		return
	}
	vLog.SetLogWriter(writer)
}

func Debug(msg string, fields map[string]interface{}) {
	vLog.Debug(msg, fields)
}

func Info(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	vLog.Info(msg, fields)
}

func Warning(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	vLog.Warning(msg, fields)
}

func Error(msg string, fields map[string]interface{}) {
	vLog.Error(msg, fields)
}
