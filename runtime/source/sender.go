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

package source

import (
	"context"
	"errors"
	"sync"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/rs/zerolog"

	"github.com/vanus-labs/cdk-go/config"
	"github.com/vanus-labs/cdk-go/connector"
	"github.com/vanus-labs/cdk-go/log"
	"github.com/vanus-labs/cdk-go/runtime/sender"
	"github.com/vanus-labs/cdk-go/util"
)

type sourceSender struct {
	cfg      config.SourceConfigAccessor
	source   connector.Source
	ceClient ce.Client
	wg       sync.WaitGroup
	sd       sender.CloudEventSender
	mutex    sync.RWMutex
	current  []*connector.Tuple
	ctx      context.Context
	cancel   context.CancelFunc
	logger   zerolog.Logger
}

func newSourceSender(cfg config.SourceConfigAccessor, source connector.Source) *sourceSender {
	return &sourceSender{
		cfg:    cfg,
		source: source,
	}
}

func (w *sourceSender) GetSource() connector.Source {
	return w.source
}

func (w *sourceSender) Start(ctx context.Context) {
	w.logger = log.FromContext(ctx)
	w.ctx, w.cancel = context.WithCancel(ctx)
	if w.cfg.GetVanusConfig() != nil {
		w.sd = sender.NewVanusSender(w.cfg.GetVanusConfig().Eventbus, w.cfg.GetVanusConfig().Eventbus)
	} else {
		w.sd = sender.NewHTTPSender(w.cfg.GetTarget())
	}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.execute(w.ctx)
	}()

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.send(w.ctx)
	}()
}

func (w *sourceSender) execute(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case tuple, ok := <-w.source.Chan():
			if !ok {
				return
			}
			w.mutex.RLock()
			w.current = append(w.current, tuple)
			w.mutex.RUnlock()
			w.doSend(false)
		}
	}
}

func (w *sourceSender) send(ctx context.Context) {
	t := time.NewTicker(200 * time.Microsecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.doSend(true)
		}
	}
}

func (w *sourceSender) needAttempt(attempt int) bool {
	if w.cfg.GetAttempts() <= 0 {
		return true
	}
	return attempt < w.cfg.GetAttempts()
}

func (w *sourceSender) Stop() error {
	w.cancel()
	w.wg.Wait()
	return w.source.Destroy()
}

func (w *sourceSender) doSend(force bool) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	eventNum := len(w.current)
	if eventNum <= 0 || (eventNum < w.cfg.GetBatchSize() && !force) {
		return
	}

	events := make([]*ce.Event, len(w.current))
	for idx := range w.current {
		events[idx] = w.current[idx].Event
	}

	ctx := ce.WithEncodingStructured(context.Background())
	var attempt int
	var err error
	for {
		err = func() ce.Result {
			ceCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			return w.sd.SendEvent(ceCtx, events...)
		}()
		attempt++
		// TODO(wenfeng) remove ce.IsACK?
		if err == nil || ce.IsACK(err) {
			w.logger.Debug().Interface("attempt", attempt).Int("event_num", len(events)).Msg("send event success")
			err = nil
			break
		}
		if errors.Is(err, context.Canceled) || !w.needAttempt(attempt) {
			w.logger.Error().Interface("attempt", attempt).Int("event_num", len(events)).Err(err).Msg("send event failed")
			break
		}
		w.logger.Warn().Interface("attempt", attempt).Int("event_num", len(events)).Err(err).Msg("send event failed, will retry")
		time.Sleep(util.Backoff(attempt, time.Second*5))
	}

	if err == nil {
		for idx := range w.current {
			if w.current[idx].Success != nil {
				w.current[idx].Success()
			}
		}
	} else {
		for idx := range w.current {
			if w.current[idx].Failed != nil {
				w.current[idx].Failed(err)
			}
		}
	}
	w.current = nil
}
