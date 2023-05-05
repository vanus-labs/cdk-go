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
	"sync"

	"github.com/go-playground/validator/v10"
	"github.com/pkg/errors"

	"github.com/vanus-labs/cdk-go/config"
	"github.com/vanus-labs/cdk-go/connector"
	"github.com/vanus-labs/cdk-go/log"
	rc "github.com/vanus-labs/cdk-go/runtime/worker"
	"github.com/vanus-labs/cdk-go/util"
)

type worker struct {
	cfgCtor         func() config.ConfigAccessor
	connectorCtor   func() connector.Connector
	connectorWorker map[string]rc.Connector
	cLock           sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
	running         bool
}

var _ Worker = &worker{}

func newSourceWorker(cfgCtor func() config.SourceConfigAccessor,
	sourceCtor func() connector.Source) *worker {
	return newWorker(func() config.ConfigAccessor {
		return cfgCtor()
	}, func() connector.Connector {
		return sourceCtor()
	})
}

func newSinkWorker(cfgCtor func() config.SinkConfigAccessor,
	sinkCtor func() connector.Sink) *worker {
	return newWorker(func() config.ConfigAccessor {
		return cfgCtor()
	}, func() connector.Connector {
		return sinkCtor()
	})
}

func newWorker(cfgCtor func() config.ConfigAccessor,
	connectorCtor func() connector.Connector) *worker {
	return &worker{
		cfgCtor:       cfgCtor,
		connectorCtor: connectorCtor,
	}
}

func (s *worker) Start(ctx context.Context) error {
	s.cLock.Lock()
	defer s.cLock.Unlock()
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.running = true
	return nil
}

func (s *worker) Stop() error {
	s.cLock.Lock()
	defer s.cLock.Unlock()
	s.cancel()
	s.running = false
	var wg sync.WaitGroup
	for id := range s.connectorWorker {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			err := s.connectorWorker[id].Stop()
			log.Info("connector stop", map[string]interface{}{
				"id":         id,
				log.KeyError: err,
			})
		}(id)
	}
	wg.Wait()
	return nil
}

func (s *worker) initConfig(config []byte, cfg config.ConfigAccessor) error {
	err := util.ParseConfig(config, cfg)
	if err != nil {
		return errors.Wrap(err, "parse config error")
	}
	err = validator.New().StructCtx(s.ctx, cfg)
	if err != nil {
		return errors.Wrap(err, "config tag validator has error")
	}
	err = cfg.Validate()
	if err != nil {
		return errors.Wrap(err, "config validate error")
	}
	return nil
}

func (s *worker) newConnectorWorker(cfg config.ConfigAccessor) (rc.Connector, error) {
	c := s.connectorCtor()
	err := c.Initialize(s.ctx, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "source connector initialize failed")
	}
	var wc rc.Connector
	switch cfg.ConnectorKind() {
	case config.SourceConnector:
		wc = rc.NewSourceWorker(cfg.(config.SourceConfigAccessor), c.(connector.Source))
	case config.SinkConnector:
		wc = rc.NewSinkWorker(cfg.(config.SinkConfigAccessor), c.(connector.Sink))
	}
	return wc, nil
}

func (s *worker) getConnector(connectorID string) connector.Connector {
	s.cLock.RLock()
	defer s.cLock.RUnlock()
	rc, ok := s.connectorWorker[connectorID]
	if !ok {
		return nil
	}
	return rc.GetConnector()
}

func (s *worker) RegisterConnector(connectorID string, cfgBytes []byte) error {
	s.cLock.Lock()
	defer s.cLock.Unlock()
	if !s.running {
		return nil
	}
	cfg := s.cfgCtor()
	err := s.initConfig(cfgBytes, cfg)
	if err != nil {
		return err
	}
	wc, err := s.newConnectorWorker(cfg)
	if err != nil {
		return err
	}
	err = wc.Start(s.ctx)
	if err != nil {
		return err
	}
	s.connectorWorker[connectorID] = wc
	return nil
}

func (s *worker) RemoveConnector(connectorID string) {
	s.cLock.Lock()
	defer s.cLock.Unlock()
	if !s.running {
		return
	}
	wc, ok := s.connectorWorker[connectorID]
	if !ok {
		return
	}
	err := wc.Stop()
	if err != nil {
		log.Warning("connector stop failed", map[string]interface{}{
			log.KeyError:   err,
			"connector_id": connectorID,
		})
	}
	delete(s.connectorWorker, connectorID)
}
