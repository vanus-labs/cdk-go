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

package runtime

import (
	"context"
	"os"
	"sync"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/cdk-go/config"
	"github.com/linkall-labs/cdk-go/connector"
	"github.com/linkall-labs/cdk-go/log"
	"github.com/linkall-labs/cdk-go/runtime/sender"
	"github.com/linkall-labs/cdk-go/util"
	"github.com/pkg/errors"
)

type SourceConfigConstructor func() config.SourceConfigAccessor

type SourceConstructor func() connector.Source

func RunSource(cfgCtor SourceConfigConstructor, sourceCtor SourceConstructor) {
	cfg := cfgCtor()
	source := sourceCtor()
	err := runConnector(cfg, source)
	if err != nil {
		log.Error("run source error", map[string]interface{}{
			log.KeyError: err,
			"name":       source.Name(),
		})
		os.Exit(-1)
	}
}

type SourceWorker struct {
	cfg      config.SourceConfigAccessor
	source   connector.Source
	ceClient ce.Client
	wg       sync.WaitGroup
	sd       sender.CloudEventSender
	mutex    sync.RWMutex
	current  []*connector.Tuple
}

func newSourceWorker(cfg config.SourceConfigAccessor, source connector.Source) Worker {
	return &SourceWorker{
		cfg:    cfg,
		source: source,
	}
}

func (w *SourceWorker) Start(ctx context.Context) error {
	if w.cfg.GetVanusConfig() != nil {
		w.sd = sender.NewVanusSender(w.cfg.GetVanusConfig().Eventbus, w.cfg.GetVanusConfig().Eventbus)
	} else {
		w.sd = sender.NewHTTPSender(w.cfg.GetTarget())
	}
	if w.sd == nil {
		return errors.New("failed to init cloudevents sender")
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.execute(ctx)
	}()

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.send(ctx)
	}()

	log.Info("the connector started", map[string]interface{}{
		log.ConnectorName: w.source.Name(),
	})
	return nil
}

func (w *SourceWorker) execute(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case tuple := <-w.source.Chan():
			w.mutex.RLock()
			w.current = append(w.current, tuple)
			w.mutex.RUnlock()
			w.doSend(false)
		}
	}
}

func (w *SourceWorker) send(ctx context.Context) {
	t := time.NewTimer(200 * time.Microsecond)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.doSend(true)
		}
	}
}

func (w *SourceWorker) needAttempt(attempt int) bool {
	if w.cfg.GetAttempts() <= 0 {
		return true
	}
	return attempt < w.cfg.GetAttempts()
}

func (w *SourceWorker) Stop() error {
	w.wg.Wait()
	return w.source.Destroy()
}

func (w *SourceWorker) doSend(force bool) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if len(w.current) < w.cfg.GetBatchSize() && !force {
		return
	}

	events := make([]*ce.Event, len(w.current))
	for idx := range w.current {
		events[idx] = w.current[idx].Event
	}

	ctx := context.Background()
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
			log.Debug("send event success", map[string]interface{}{
				"event":   events,
				"attempt": attempt,
			})
			err = nil
			break
		}
		if errors.Is(err, context.Canceled) || !w.needAttempt(attempt) {
			log.Error("send event fail", map[string]interface{}{
				log.KeyError: err,
				"attempt":    attempt,
				"event":      events,
			})
			break
		}
		log.Warning("send event failed, will retry", map[string]interface{}{
			log.KeyError: err,
			"attempt":    attempt,
			"event":      events,
		})
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
