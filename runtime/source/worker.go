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
	"sync"

	"github.com/vanus-labs/cdk-go/connector"
	"github.com/vanus-labs/cdk-go/log"
	"github.com/vanus-labs/cdk-go/runtime/common"
	"github.com/vanus-labs/cdk-go/store"
)

type sourceWorker struct {
	cfgCtor      common.SourceConfigConstructor
	sourceCtor   common.SourceConstructor
	config       common.WorkerConfig
	senders      map[string]*sourceSender
	cLock        sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	shuttingDown bool
}

var _ common.Worker = &sourceWorker{}

func NewSourceWorker(cfgCtor common.SourceConfigConstructor,
	sourceCtor common.SourceConstructor, config common.WorkerConfig) *sourceWorker {
	return &sourceWorker{
		cfgCtor:    cfgCtor,
		sourceCtor: sourceCtor,
		senders:    map[string]*sourceSender{},
		config:     config,
	}
}

func (w *sourceWorker) Config() common.WorkerConfig {
	return w.config
}

func (w *sourceWorker) Start(ctx context.Context) error {
	w.ctx, w.cancel = context.WithCancel(ctx)
	return nil
}

func (w *sourceWorker) Stop() error {
	w.cLock.Lock()
	w.shuttingDown = true
	w.cLock.Unlock()
	w.cancel()
	var wg sync.WaitGroup
	for id := range w.senders {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			err := w.senders[id].Stop()
			log.Info().Str(log.KeyConnectorID, id).Err(err).Msg("connector stop")
		}(id)
	}
	wg.Wait()
	return nil
}

func (w *sourceWorker) getSource(connectorID string) connector.Source {
	w.cLock.RLock()
	defer w.cLock.RUnlock()
	rc, ok := w.senders[connectorID]
	if !ok {
		return nil
	}
	return rc.GetSource()
}

func (w *sourceWorker) RegisterConnector(connectorID string, config []byte) error {
	w.cLock.Lock()
	defer w.cLock.Unlock()
	if w.shuttingDown {
		return nil
	}
	log.Info().Str(log.KeyConnectorID, connectorID).Msg("add a connector")
	// check the connector is existed,if true stop it
	w.removeConnector(connectorID)
	cfg := w.cfgCtor()
	source := w.sourceCtor()
	ctor := common.Connector{Config: cfg, Connector: source}
	ctx := log.WithLogger(context.Background(), log.NewConnectorLog(connectorID))
	ctx = store.WithKVStore(ctx, store.NewConnectorStore(connectorID))
	err := ctor.InitConnector(ctx, config)
	if err != nil {
		return err
	}
	sender := newSourceSender(cfg, source)
	sender.Start(ctx)
	log.Info().Str(log.KeyConnectorID, connectorID).Msg("connector start")
	w.senders[connectorID] = sender
	return nil
}

func (w *sourceWorker) RemoveConnector(connectorID string) {
	w.cLock.Lock()
	defer w.cLock.Unlock()
	if w.shuttingDown {
		return
	}
	log.Info().Str(log.KeyConnectorID, connectorID).Msg("remove a connector")
	w.removeConnector(connectorID)
}

func (w *sourceWorker) removeConnector(connectorID string) {
	sender, ok := w.senders[connectorID]
	if !ok {
		return
	}
	err := sender.Stop()
	if err != nil {
		log.Warn().Str(log.KeyConnectorID, connectorID).Err(err).Msg("connector stop failed")
	} else {
		log.Info().Str(log.KeyConnectorID, connectorID).Msg("connector stop success")
	}
	delete(w.senders, connectorID)
}
