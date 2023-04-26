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

	"github.com/vanus-labs/cdk-go/log"
	"github.com/vanus-labs/cdk-go/runtime/worker"
)

type commonSource struct {
	cLock      sync.RWMutex
	workers    map[string]*worker.SourceWorker
	cfgCtor    SourceConfigConstructor
	sourceCtor SourceConstructor
	ctx        context.Context
	cancel     context.CancelFunc
	running    bool
}

func NewCommonSource(cfgCtor SourceConfigConstructor, sourceCtor SourceConstructor) *commonSource {
	return &commonSource{
		cfgCtor:    cfgCtor,
		sourceCtor: sourceCtor,
		workers:    map[string]*worker.SourceWorker{},
	}
}

func (s *commonSource) getSource(connectorID string) *worker.SourceWorker {
	s.cLock.RLock()
	defer s.cLock.RUnlock()
	return s.workers[connectorID]
}

func (s *commonSource) Start(ctx context.Context) error {
	s.cLock.Lock()
	defer s.cLock.Unlock()
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.running = true
	return nil
}

func (s *commonSource) Stop() error {
	s.cLock.Lock()
	defer s.cLock.Unlock()
	s.cancel()
	s.running = false
	for id := range s.workers {
		go func() {
			s.workers[id].Stop()
			log.Info("source stop", map[string]interface{}{
				"id": id,
			})
		}()
	}
	return nil
}

func (s *commonSource) RegisterSource(connectorID string, config []byte) error {
	s.cLock.Lock()
	defer s.cLock.Unlock()
	if !s.running {
		return nil
	}
	cfg := s.cfgCtor()
	err := worker.NewConfig(s.ctx, config, cfg)
	if err != nil {
		return err
	}
	c := s.sourceCtor()
	w, err := worker.NewSourceWorker(s.ctx, cfg, c)
	if err != nil {
		return err
	}
	err = w.Start(s.ctx)
	if err != nil {
		return err
	}
	s.workers[connectorID] = w
	return nil
}

func (s *commonSource) RemoveSource(connectorID string) {
	s.cLock.Lock()
	defer s.cLock.Unlock()
	if !s.running {
		return
	}
	w, ok := s.workers[connectorID]
	if !ok {
		return
	}
	w.Stop()
	delete(s.workers, connectorID)
}
