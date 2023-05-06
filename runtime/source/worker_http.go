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

	"github.com/vanus-labs/cdk-go/connector"
	"github.com/vanus-labs/cdk-go/runtime/common"
	"github.com/vanus-labs/cdk-go/runtime/util"
)

type httpSourceWorker struct {
	sourceWorker
}

func NewHTTPSourceWorker(cfgCtor common.SourceConfigConstructor,
	httpSourceCtor common.HTTPSourceConstructor) *httpSourceWorker {
	w := NewSourceWorker(cfgCtor, func() connector.Source {
		return httpSourceCtor()
	})
	source := &httpSourceWorker{
		sourceWorker: *w,
	}
	return source
}

func (w *httpSourceWorker) getPort() int {
	return 8080
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

func (w *httpSourceWorker) getHandler() http.Handler {
	source := w.getSource()
	return source.(http.Handler)
}

func (w *httpSourceWorker) ServeHTTP(writer http.ResponseWriter, req *http.Request) {
	handler := w.getHandler()
	handler.ServeHTTP(writer, req)
}
