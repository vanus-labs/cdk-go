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

package util

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/vanus-labs/cdk-go/log"
)

type HTTPReceiver struct {
	port   int
	server *http.Server
}

func NewHTTPReceiver(port int) *HTTPReceiver {
	return &HTTPReceiver{
		port: port,
	}
}

func (r *HTTPReceiver) StartListen(ctx context.Context, handler http.Handler) error {
	r.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", r.port),
		Handler: handler,
	}
	errChan := make(chan error, 1)
	go func() {
		log.Info("http server is ready to start", map[string]interface{}{
			"port": r.port,
		})
		errChan <- r.server.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
		// disable keep-alives to avoid clients hanging onto connections.
		r.server.SetKeepAlivesEnabled(false)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := r.server.Shutdown(ctx)
		return err
	case err := <-errChan:
		return err
	}
}
