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
	"fmt"
	"net/http"
	"strings"

	"github.com/vanus-labs/cdk-go/connector"
	"github.com/vanus-labs/cdk-go/runtime/common"
	"github.com/vanus-labs/cdk-go/runtime/util"
)

type httpSourceWorker struct {
	sourceWorker
	config      common.HTTPConfig
	basePathLen int
}

func NewHTTPSourceWorker(cfgCtor common.SourceConfigConstructor,
	httpSourceCtor common.HTTPSourceConstructor, config common.HTTPConfig) *httpSourceWorker {
	w := NewSourceWorker(cfgCtor, func() connector.Source {
		return httpSourceCtor()
	}, config.WorkerConfig)
	source := &httpSourceWorker{
		sourceWorker: *w,
		config:       config,
		basePathLen:  len(config.BasePath),
	}
	return source
}

func (w *httpSourceWorker) getPort() int {
	if w.config.Port <= 0 {
		return 8080
	}
	return w.config.Port
}

func (w *httpSourceWorker) Start(ctx context.Context) error {
	err := w.sourceWorker.Start(ctx)
	if err != nil {
		return err
	}
	go func() {
		r := util.NewHTTPReceiver(w.getPort())
		if err := r.StartListen(w.ctx, w); err != nil {
			panic(fmt.Sprintf("cloud not listen on %d, error: %s", w.getPort(), err.Error()))
		}
	}()
	return nil
}

func (w *httpSourceWorker) getHandler(connectorID string) http.Handler {
	source := w.getSource(connectorID)
	if source == nil {
		return nil
	}
	return source.(http.Handler)
}

func (w *httpSourceWorker) ServeHTTP(writer http.ResponseWriter, req *http.Request) {
	connectorID := strings.TrimSuffix(req.URL.Path, "/")
	if w.basePathLen > 0 {
		if len(connectorID) <= w.basePathLen {
			writer.WriteHeader(http.StatusBadRequest)
			writer.Write([]byte("connectorID invalid"))
			return
		}
		connectorID = connectorID[w.basePathLen:]
	}
	handler := w.getHandler(connectorID)
	if handler == nil {
		writer.WriteHeader(http.StatusNotFound)
		writer.Write([]byte("connectorID invalid"))
		return
	}
	handler.ServeHTTP(writer, req)
}
