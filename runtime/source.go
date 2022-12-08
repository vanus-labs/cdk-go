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
}

func newSourceWorker(cfg config.SourceConfigAccessor, source connector.Source) Worker {
	return &SourceWorker{
		cfg:    cfg,
		source: source,
	}
}

func (w *SourceWorker) Start(ctx context.Context) error {
	target := w.cfg.GetTarget()
	ceClient, err := ce.NewClientHTTP(ce.WithTarget(target))
	if err != nil {
		return errors.Wrap(err, "failed to init ce client")
	}
	w.ceClient = ceClient
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.execute(ctx)
	}()
	log.Info("the connector started", map[string]interface{}{
		log.ConnectorName: w.source.Name(),
		"target":          target,
	})
	return nil
}

func (w *SourceWorker) execute(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case tuple := <-w.source.Chan():
			err := w.sendEvent(ctx, tuple.Event)
			if err == nil {
				if tuple.Success != nil {
					tuple.Success()
				}
			} else {
				if tuple.Failed != nil {
					tuple.Failed()
				}
			}
		}
	}
}

func (w *SourceWorker) sendEvent(ctx context.Context, event *ce.Event) error {
	var attempt int
	for {
		result := func() ce.Result {
			ceCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			return w.ceClient.Send(ceCtx, *event)
		}()
		attempt++
		if ce.IsACK(result) {
			log.Debug("send event success", map[string]interface{}{
				"event":   event,
				"attempt": attempt,
			})
			return nil
		}
		if errors.Is(result, context.Canceled) || !w.needAttempt(attempt) {
			log.Error("send event fail", map[string]interface{}{
				log.KeyError: result,
				"attempt":    attempt,
				"event":      event,
			})
			return result
		}
		log.Warning("send event failed, will retry", map[string]interface{}{
			log.KeyError: result,
			"attempt":    attempt,
			"event":      event,
		})
		time.Sleep(util.Backoff(attempt, time.Second*5))
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
