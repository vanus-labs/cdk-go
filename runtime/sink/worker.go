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

	"github.com/vanus-labs/cdk-go/config"
	"github.com/vanus-labs/cdk-go/connector"
	"github.com/vanus-labs/cdk-go/runtime/common"
	"github.com/vanus-labs/cdk-go/runtime/util"
)

type sinkWorker struct {
	cfg    config.SinkConfigAccessor
	sink   connector.Sink
	ctx    context.Context
	cancel context.CancelFunc
}

func (w *sinkWorker) RegisterConnector(_ string, config []byte) error {
	c := common.Connector{Config: w.cfg, Connector: w.sink}
	err := c.InitConnector(w.ctx, config)
	if err != nil {
		return err
	}
	return nil
}

func (w *sinkWorker) RemoveConnector(_ string) {
	panic("not support")
}

func NewSinkWorker(cfgCtor common.SinkConfigConstructor, sinkCtor common.SinkConstructor) common.Worker {
	return &sinkWorker{
		cfg:  cfgCtor(),
		sink: sinkCtor(),
	}
}

func (w *sinkWorker) Start(ctx context.Context) error {
	w.ctx, w.cancel = context.WithCancel(ctx)
	port := w.cfg.GetPort()
	go func() {
		r := util.NewHTTPReceiver(port)
		if err := r.StartListen(w.ctx, w); err != nil {
			panic(fmt.Sprintf("cloud not listen on %d, error: %s", port, err.Error()))
		}
	}()
	return nil
}

func (w *sinkWorker) ServeHTTP(writer http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	handHttpRequest(w.ctx, connectorModel{sink: w.sink}, writer, req)
}

func (w *sinkWorker) Stop() error {
	w.cancel()
	return w.sink.Destroy()
}
