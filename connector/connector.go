/*
Copyright 2022-Present The Vance Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package connector

import (
	"context"
	"fmt"
	"sync"

	"github.com/linkall-labs/cdk-go/log"
)

type Connector interface {
	Init(cfgPath, secretPath string) error
	Name() string
	SetLogger(logger log.Logger)
	Destroy() error
}

//type Source interface {
//	Connector
//	Start() error
//	Stop() error
//	Receive(ctx context.Context) error
//}
//
//func RunSource(source Source) {
//}

func wait(ctx context.Context, c Connector, f func(), cfg *Config) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		f()
		wg.Done()
	}()
	log.Info(ctx, "the connector started", map[string]interface{}{
		log.ConnectorName: c.Name(),
		"listening":       fmt.Sprintf(":%d", cfg.Port),
	})
	select {
	case <-ctx.Done():
		log.Info(ctx, "received system signal, preparing to exit tge connector", map[string]interface{}{
			"name": c.Name(),
		})
	}
}
