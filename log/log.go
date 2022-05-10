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
