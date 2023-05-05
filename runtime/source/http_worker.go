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
	"sync"
	"time"

	"github.com/vanus-labs/cdk-go/config"
	"github.com/vanus-labs/cdk-go/connector"
	"github.com/vanus-labs/cdk-go/log"
)

type httpSourceWorker struct {
	*sourceWorker
	server    *http.Server
	routersMu sync.RWMutex

	services map[string]http.Handler
}

func NewHttpSourceWorker(cfgCtor func() config.SourceConfigAccessor,
	httpSourceCtor func() connector.HTTPSource) *httpSourceWorker {
	w := NewSourceWorker(cfgCtor, func() connector.Source {
		return httpSourceCtor()
	})
	source := &httpSourceWorker{
		sourceWorker: w,
		services:     map[string]http.Handler{},
	}
	return source
}

func (s *httpSourceWorker) getPort() int {
	return 8080
}

func (s *httpSourceWorker) Start(ctx context.Context) error {
	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.getPort()),
		Handler: s,
	}
	go func() {
		log.Info("http server is ready to start", map[string]interface{}{
			"port": s.getPort(),
		})
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic(fmt.Sprintf("cloud not listen on %d, error: %s", s.getPort(), err.Error()))
		}
		log.Info("http server stopped", nil)
	}()
	return s.sourceWorker.Start(ctx)
}

func (s *httpSourceWorker) Stop() error {
	s.routersMu.Lock()
	defer s.routersMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.server.SetKeepAlivesEnabled(false)
	_ = s.sourceWorker.Stop()
	return s.server.Shutdown(ctx)
}

func (s *httpSourceWorker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.routersMu.RLock()
	handler, ok := s.services[r.URL.Path]
	s.routersMu.RUnlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	handler.ServeHTTP(w, r)
}

func (s *httpSourceWorker) RegisterConnector(connectorID string, config []byte) error {
	err := s.sourceWorker.RegisterConnector(connectorID, config)
	if err != nil {
		return err
	}
	source := s.sourceWorker.getConnector(connectorID)
	if source == nil {
		return nil
	}
	s.routersMu.Lock()
	defer s.routersMu.Unlock()
	s.services["/"+connectorID] = source.(http.Handler)
	return nil
}

func (s *httpSourceWorker) RemoveConnector(connectorID string) {
	s.sourceWorker.RemoveConnector(connectorID)
	s.routersMu.Lock()
	defer s.routersMu.Unlock()
	delete(s.services, "/"+connectorID)
}
