package log

import (
	"context"
	"github.com/go-logr/logr"
	"sync"
)

var (
	loggerWasSetLock sync.Mutex
	loggerWasSet     bool
	Log              logr.Logger
)

// SetLogger sets a concrete logging implementation
func SetLogger(l logr.Logger) {
	loggerWasSetLock.Lock()
	defer loggerWasSetLock.Unlock()

	loggerWasSet = true
	Log = logr.New(l.GetSink())
}

// FromContext returns a logger with predefined values from a context.Context.
func FromContext(ctx context.Context, keysAndValues ...interface{}) logr.Logger {
	log := Log
	if ctx != nil {
		if logger, err := logr.FromContext(ctx); err == nil {
			log = logger
		}
	}
	return log.WithValues(keysAndValues...)
}
