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
	"os"
	"strings"

	"github.com/vanus-labs/cdk-go/config"
	"github.com/vanus-labs/cdk-go/connector"
)

type SourceConfigConstructor func() config.SourceConfigAccessor
type SourceConstructor func() connector.Source

type SinkConfigConstructor func() config.SinkConfigAccessor
type SinkConstructor func() connector.Sink

type Connector interface {
	Start(ctx context.Context) error
	Stop() error
}

type Source interface {
	Connector
	RegisterSource(connectorID string, config []byte) error
	RemoveSource(connectorID string)
}

type Sink interface {
	Connector
	RegisterSink(connectorID string, config []byte) error
	RemoveSink(connectorID string)
}

func isShare() bool {
	runtime := os.Getenv("CONNECTOR-RUNTIME")
	return strings.ToLower(runtime) == "k8s"
}

func RunSource(name string, cfgCtor SourceConfigConstructor, sourceCtor SourceConstructor) {
	share := isShare()
	source := NewCommonSource(cfgCtor, sourceCtor)
	if !share {
		runStandaloneSource(name, source)
		return
	}
	runShareSource(name, source)
}

func RunHttpSource(name string, cfgCtor SourceConfigConstructor, sourceCtor SourceConstructor) {
	share := isShare()
	source := NewHttpSource(NewCommonSource(cfgCtor, sourceCtor))
	if !share {
		runStandaloneSource(name, source)
		return
	}
	runShareSource(name, source)
}
