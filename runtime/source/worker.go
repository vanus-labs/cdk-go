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
)

type sourceWorker struct {
	cfgCtor    common.SourceConfigConstructor
	sourceCtor common.SourceConstructor

	senders      map[string]*sourceSender
	cLock        sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	shuttingDown bool
}

var _ common.Worker = &sourceWorker{}

func NewSourceWorker(cfgCtor common.SourceConfigConstructor,
	sourceCtor common.SourceConstructor) *sourceWorker {
	return &sourceWorker{
		cfgCtor:    cfgCtor,
		sourceCtor: sourceCtor,
		senders:    map[string]*sourceSender{},
	}
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
			log.Info("connector stop", map[string]interface{}{
				"id":         id,
				log.KeyError: err,
			})
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
	if w.shuttingDown {
		return nil
	}
	w.cLock.Unlock()
	cfg := w.cfgCtor()
	source := w.sourceCtor()
	ctor := common.Connector{Config: cfg, Connector: source}
	err := ctor.InitConnector(w.ctx, config)
	if err != nil {
		return err
	}
	sender := newSourceSender(cfg, source)
	w.addSource(connectorID, sender)
	return nil
}

func (w *sourceWorker) addSource(connectorID string, sender *sourceSender) {
	w.cLock.Lock()
	defer w.cLock.Unlock()
	if _sender, exist := w.senders[connectorID]; exist {
		log.Info("connector exist,will stop it", map[string]interface{}{
			"connector_id": connectorID,
		})
		_sender.Stop()
	}
	log.Info("add a connector", map[string]interface{}{
		"connector_id": connectorID,
	})
	sender.Start(w.ctx)
	w.senders[connectorID] = sender
}

func (w *sourceWorker) RemoveConnector(connectorID string) {
	w.cLock.Lock()
	defer w.cLock.Unlock()
	if w.shuttingDown {
		return
	}
	wc, ok := w.senders[connectorID]
	if !ok {
		return
	}
	log.Info("remove a connector", map[string]interface{}{
		"connector_id": connectorID,
	})
	err := wc.Stop()
	if err != nil {
		log.Warning("connector stop failed", map[string]interface{}{
			log.KeyError:   err,
			"connector_id": connectorID,
		})
	}
	delete(w.senders, connectorID)
}
