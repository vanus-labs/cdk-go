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

type mtHTTPSourceWorker struct {
	mtSourceWorker
}

func NewMtHTTPSourceWorker(cfgCtor common.SourceConfigConstructor,
	httpSourceCtor common.HTTPSourceConstructor) *mtHTTPSourceWorker {
	w := NewMtSourceWorker(cfgCtor, func() connector.Source {
		return httpSourceCtor()
	})
	source := &mtHTTPSourceWorker{
		mtSourceWorker: *w,
	}
	return source
}

func (w *mtHTTPSourceWorker) getPort() int {
	return 8080
}

func (w *mtHTTPSourceWorker) Start(ctx context.Context) error {
	err := w.mtSourceWorker.Start(ctx)
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

func (w *mtHTTPSourceWorker) getHandler(connectorID string) http.Handler {
	source := w.getSource(connectorID)
	if source == nil {
		return nil
	}
	return source.(http.Handler)
}

func (w *mtHTTPSourceWorker) ServeHTTP(writer http.ResponseWriter, req *http.Request) {
	if req.RequestURI == "/" {
		writer.WriteHeader(http.StatusNotFound)
		writer.Write([]byte("connectorID not found"))
		return
	}
	connectorID := strings.TrimPrefix(strings.TrimSuffix(req.RequestURI, "/"), "/")
	handler := w.getHandler(connectorID)
	if handler == nil {
		writer.WriteHeader(http.StatusNotFound)
		writer.Write([]byte("connectorID invalid"))
		return
	}
	handler.ServeHTTP(writer, req)
}
