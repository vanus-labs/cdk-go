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
	"github.com/linkall-labs/cdk-go/connector"
	"github.com/linkall-labs/cdk-go/log"
	"github.com/pkg/errors"
)

type SourceConfigConstructor func() connector.SourceConfigAccessor

func RunSource(cfg connector.SourceConfigAccessor, source connector.Source) {
	err := runConnector(cfg, source)
	if err != nil {
		log.Error("run source error", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}
}

type SourceWorker struct {
	cfg      connector.SourceConfigAccessor
	source   connector.Source
	ceClient ce.Client
	wg       sync.WaitGroup
}

func newSourceWorker(cfg connector.SourceConfigAccessor, Source connector.Source) Worker {
	return &SourceWorker{
		cfg:    cfg,
		source: Source,
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
		default:
		}
		event := w.source.PollEvent()
		w.sendEvent(ctx, event)
	}
}

func (w *SourceWorker) sendEvent(ctx context.Context, event ce.Event) {
	var attempt int
	for {
		err := func() error {
			ceCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			return w.ceClient.Send(ceCtx, event)
		}()
		if ce.IsACK(err) {
			w.source.Commit(event)
			return
		}
		if errors.Is(err, context.Canceled) {
			return
		}
		attempt++
		if w.cfg.GetAttempts() <= 0 || attempt < w.cfg.GetAttempts() {
			log.Info("send event fail,will retry", map[string]interface{}{
				log.KeyError: err,
				"attempt":    attempt,
				"event":      event,
			})
			time.Sleep(time.Second)
			continue
		}
		log.Warning("send event fail", map[string]interface{}{
			log.KeyError: err,
			"attempt":    attempt,
			"event":      event,
		})
		return
	}
}

func (w *SourceWorker) Stop() error {
	w.wg.Wait()
	return w.source.Destroy()
}
