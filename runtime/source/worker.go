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

	"github.com/vanus-labs/cdk-go/config"
	"github.com/vanus-labs/cdk-go/connector"
	"github.com/vanus-labs/cdk-go/runtime/common"
)

type sourceWorker struct {
	cfg    config.SourceConfigAccessor
	source connector.Source
	sender *sourceSender

	ctx    context.Context
	cancel context.CancelFunc
}

var _ common.Worker = &sourceWorker{}

func NewSourceWorker(cfgCtor common.SourceConfigConstructor,
	sourceCtor common.SourceConstructor) *sourceWorker {
	return &sourceWorker{
		cfg:    cfgCtor(),
		source: sourceCtor(),
	}
}

func (w *sourceWorker) Start(ctx context.Context) error {
	w.ctx, w.cancel = context.WithCancel(ctx)
	return nil
}

func (w *sourceWorker) Stop() error {
	return w.sender.Stop()
}

func (w *sourceWorker) getSource() connector.Source {
	return w.sender.GetSource()
}

func (w *sourceWorker) RegisterConnector(_ string, config []byte) error {
	c := common.Connector{Config: w.cfg, Connector: w.source}
	err := c.InitConnector(w.ctx, config)
	if err != nil {
		return err
	}
	w.sender = newSourceSender(w.cfg, w.source)
	w.sender.Start(w.ctx)
	return nil
}

func (w *sourceWorker) RemoveConnector(connectorID string) {
	panic("not support")
}
