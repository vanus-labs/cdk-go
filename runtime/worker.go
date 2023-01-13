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

package runtime

import (
	"context"

	"github.com/go-playground/validator/v10"
	"github.com/linkall-labs/cdk-go/config"
	"github.com/linkall-labs/cdk-go/connector"
	"github.com/linkall-labs/cdk-go/log"
	"github.com/linkall-labs/cdk-go/store"
	"github.com/linkall-labs/cdk-go/util"
	"github.com/pkg/errors"
)

func runConnector(cfg config.ConfigAccessor, c connector.Connector) error {
	err := config.ParseConfig(cfg)
	if err != nil {
		return errors.Wrap(err, "init config error")
	}
	log.SetLogLevel(cfg.GetLogConfig().GetLogLevel())
	ctx := util.SignalContext()
	err = validator.New().StructCtx(ctx, cfg)
	if err != nil {
		return errors.Wrap(err, "config tag validator has error")
	}
	err = cfg.Validate()
	if err != nil {
		return errors.Wrap(err, "config validate error")
	}
	err = store.InitKvStore(cfg.GetStoreConfig())
	if err != nil {
		return errors.Wrap(err, "init kv store error")
	}
	err = c.Initialize(ctx, cfg)
	if err != nil {
		return errors.Wrap(err, "connector initialize failed")
	}
	worker := getWorker(cfg, c)
	err = worker.Start(ctx)
	if err != nil {
		return errors.Wrap(err, "worker start failed")
	}
	select {
	case <-ctx.Done():
		log.Info("received system signal, beginning shutdown", map[string]interface{}{
			"name": c.Name(),
		})
		if err = worker.Stop(); err != nil {
			log.Error("worker stop fail", map[string]interface{}{
				log.KeyError: err,
				"name":       c.Name(),
			})
		} else {
			log.Info("connector shutdown graceful", map[string]interface{}{
				"name": c.Name(),
			})
		}
	}
	return nil
}

type Worker interface {
	Start(ctx context.Context) error
	Stop() error
}

func getWorker(cfg config.ConfigAccessor, c connector.Connector) Worker {
	switch cfg.ConnectorType() {
	case config.SourceConnector:
		return newSourceWorker(cfg.(config.SourceConfigAccessor), c.(connector.Source))
	case config.SinkConnector:
		return newSinkWorker(cfg.(config.SinkConfigAccessor), c.(connector.Sink))
	}
	panic("unknown connector type:" + cfg.ConnectorType())
}
