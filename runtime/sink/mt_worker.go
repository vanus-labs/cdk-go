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

type mtSinkWorker struct {
	cfgCtor      common.SinkConfigConstructor
	sinkCtor     common.SinkConstructor
	sinks        map[string]connector.Sink
	cLock        sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	shuttingDown bool
}

func NewMtSinkWorker(cfgCtor common.SinkConfigConstructor, sinkCtor common.SinkConstructor) common.Worker {
	return &mtSinkWorker{
		cfgCtor:  cfgCtor,
		sinkCtor: sinkCtor,
		sinks:    map[string]connector.Sink{},
	}
}

func (w *mtSinkWorker) getPort() int {
	return 8080
}

func (w *mtSinkWorker) Start(ctx context.Context) error {
	w.ctx, w.cancel = context.WithCancel(ctx)
	go func() {
		r := util.NewHTTPReceiver(w.getPort())
		if err := r.StartListen(w.ctx, w); err != nil {
			panic(fmt.Sprintf("cloud not listen on %d, error: %s", w.getPort(), err.Error()))
		}
	}()
	return nil
}

func (w *mtSinkWorker) Stop() error {
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
			log.Info("sink destroy", map[string]interface{}{
				"connector_id": id,
				log.KeyError:   err,
			})
		}(id)
	}
	wg.Wait()
	return nil
}

func (w *mtSinkWorker) RegisterConnector(connectorID string, config []byte) error {
	w.cLock.Lock()
	if w.shuttingDown {
		return nil
	}
	w.cLock.Unlock()
	cfg := w.cfgCtor()
	sink := w.sinkCtor()
	c := common.Connector{Config: cfg, Connector: sink}
	err := c.InitConnector(w.ctx, config)
	if err != nil {
		return err
	}
	w.addSink(connectorID, sink)
	return nil
}

func (w *mtSinkWorker) RemoveConnector(connectorID string) {
	w.cLock.Lock()
	defer w.cLock.Unlock()
	if w.shuttingDown {
		return
	}
	sink, exist := w.sinks[connectorID]
	if !exist {
		return
	}
	log.Info("remove a connector stop it", map[string]interface{}{
		"connector_id": connectorID,
	})
	sink.Destroy()
	delete(w.sinks, connectorID)
}

func (w *mtSinkWorker) addSink(connectorID string, sink connector.Sink) {
	w.cLock.Lock()
	defer w.cLock.Unlock()
	if _sink, exist := w.sinks[connectorID]; exist {
		log.Info("connector exist,will stop it", map[string]interface{}{
			"connector_id": connectorID,
		})
		_sink.Destroy()
	}
	log.Info("add a connector", map[string]interface{}{
		"connector_id": connectorID,
	})
	w.sinks[connectorID] = sink
}

func (w *mtSinkWorker) getSink(connectorID string) connector.Sink {
	w.cLock.RLock()
	defer w.cLock.RUnlock()
	return w.sinks[connectorID]
}

func (w *mtSinkWorker) ServeHTTP(writer http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if req.RequestURI == "/" {
		writer.WriteHeader(http.StatusNotFound)
		writer.Write([]byte("connectorID not found"))
		return
	}
	connectorID := strings.TrimPrefix(strings.TrimSuffix(req.RequestURI, "/"), "/")
	sink := w.getSink(connectorID)
	if sink == nil {
		writer.WriteHeader(http.StatusBadRequest)
		writer.Write([]byte("connectorID invalid"))
		return
	}
	handHttpRequest(w.ctx, connectorModel{connectorID: connectorID, sink: sink}, writer, req)
}
