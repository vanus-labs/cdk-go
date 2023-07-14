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

package common

import (
	"context"

	"github.com/vanus-labs/cdk-go/config"
	"github.com/vanus-labs/cdk-go/connector"
)

type SourceConfigConstructor func() config.SourceConfigAccessor
type SourceConstructor func() connector.Source
type HTTPSourceConstructor func() connector.HTTPSource

type SinkConfigConstructor func() config.SinkConfigAccessor
type SinkConstructor func() connector.Sink

type ConnectorConfigConstructor func() config.ConfigAccessor
type ConnectorConstructor func() connector.Connector

type Worker interface {
	Config() WorkerConfig
	Start(ctx context.Context) error
	Stop() error
	RegisterConnector(connectorID string, config []byte) error
	RemoveConnector(connectorID string)
}
