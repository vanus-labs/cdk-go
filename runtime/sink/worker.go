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

package sink

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/vanus-labs/cdk-go/connector"
	"github.com/vanus-labs/cdk-go/log"
	"github.com/vanus-labs/cdk-go/runtime/common"
	"github.com/vanus-labs/cdk-go/runtime/util"
)

type sinkWorker struct {
	cfgCtor      common.SinkConfigConstructor
	sinkCtor     common.SinkConstructor
	config       common.HTTPConfig
	sinks        map[string]connector.Sink
	cLock        sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	shuttingDown bool
	basePathLen  int
}

func NewSinkWorker(cfgCtor common.SinkConfigConstructor, sinkCtor common.SinkConstructor, config common.HTTPConfig) common.Worker {
	return &sinkWorker{
		cfgCtor:     cfgCtor,
		sinkCtor:    sinkCtor,
		sinks:       map[string]connector.Sink{},
		config:      config,
		basePathLen: len(config.BasePath),
	}
}

func (w *sinkWorker) Config() common.WorkerConfig {
	return w.config.WorkerConfig
}

func (w *sinkWorker) getPort() int {
	if w.config.Port <= 0 {
		return 8080
	}
	return w.config.Port
}

func (w *sinkWorker) Start(ctx context.Context) error {
	w.ctx, w.cancel = context.WithCancel(ctx)
	go func() {
		r := util.NewHTTPReceiver(w.getPort())
		if err := r.StartListen(w.ctx, w); err != nil {
			panic(fmt.Sprintf("cloud not listen on %d, error: %s", w.getPort(), err.Error()))
		}
	}()
	return nil
}

func (w *sinkWorker) Stop() error {
	w.cLock.Lock()
	w.shuttingDown = true
	w.cLock.Unlock()
	w.cancel()
	var wg sync.WaitGroup
	for id := range w.sinks {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			err := w.sinks[id].Destroy()
			log.Info().Str(log.KeyConnectorID, id).Err(err).Msg("sink destroy")
		}(id)
	}
	wg.Wait()
	return nil
}

func (w *sinkWorker) RegisterConnector(connectorID string, config []byte) error {
	w.cLock.Lock()
	defer w.cLock.Unlock()
	if w.shuttingDown {
		return nil
	}
	log.Info().Str(log.KeyConnectorID, connectorID).Msg("add a connector")
	// check the connector is existed,if true stop it
	w.removeConnector(connectorID)
	cfg := w.cfgCtor()
	sink := w.sinkCtor()
	c := common.Connector{Config: cfg, Connector: sink}
	err := c.InitConnector(log.WithLogger(context.Background(), log.NewConnectorLog(connectorID)), config)
	if err != nil {
		return err
	}
	log.Info().Str(log.KeyConnectorID, connectorID).Msg("connector start")
	w.sinks[connectorID] = sink
	return nil
}

func (w *sinkWorker) RemoveConnector(connectorID string) {
	w.cLock.Lock()
	defer w.cLock.Unlock()
	if w.shuttingDown {
		return
	}
	log.Info().Str(log.KeyConnectorID, connectorID).Msg("remove a connector")
	w.removeConnector(connectorID)
}

func (w *sinkWorker) removeConnector(connectorID string) {
	sink, exist := w.sinks[connectorID]
	if !exist {
		return
	}
	err := sink.Destroy()
	if err != nil {
		log.Warn().Str(log.KeyConnectorID, connectorID).Err(err).Msg("connector destroy failed")
	} else {
		log.Info().Str(log.KeyConnectorID, connectorID).Msg("connector destroy success")
	}
	delete(w.sinks, connectorID)
}

func (w *sinkWorker) getSink(connectorID string) connector.Sink {
	w.cLock.RLock()
	defer w.cLock.RUnlock()
	return w.sinks[connectorID]
}

func (w *sinkWorker) ServeHTTP(writer http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	connectorID := req.URL.Path
	if w.basePathLen > 0 {
		if len(connectorID) <= w.basePathLen {
			writer.WriteHeader(http.StatusBadRequest)
			writer.Write([]byte("connectorID invalid"))
			return
		}
		connectorID = connectorID[w.basePathLen:]
	} else {
		connectorID = strings.TrimPrefix(connectorID, "/")
	}
	sink := w.getSink(connectorID)
	if sink == nil {
		writer.WriteHeader(http.StatusBadRequest)
		writer.Write([]byte("connectorID invalid"))
		return
	}
	w.handHttpRequest(req.Context(), connectorModel{connectorID: connectorID, sink: sink}, writer, req)
}
