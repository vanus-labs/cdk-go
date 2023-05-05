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

type SinkConfigConstructor func() config.SinkConfigAccessor

type Worker interface {
	Start(ctx context.Context) error
	Stop() error
	RegisterConnector(connectorID string, config []byte) error
	RemoveConnector(connectorID string)
}

func isShare() bool {
	runtime := os.Getenv("CONNECTOR-RUNTIME")
	return strings.ToLower(runtime) == "k8s"
}

func RunSink(component string, cfgCtor SinkConfigConstructor, sinkCtor func() connector.Sink) {
	sink := newSinkWorker(cfgCtor, sinkCtor)
	runConnector(config.SinkConnector, component, sink)
}

func RunSource(component string, cfgCtor SourceConfigConstructor, sourceCtor func() connector.Source) {
	source := newSourceWorker(cfgCtor, sourceCtor)
	runSource(component, source)
}

func RunHttpSource(component string, cfgCtor SourceConfigConstructor, sourceCtor func() HttpSource) {
	httpSource := newHttpSourceWorker(cfgCtor, sourceCtor)
	runSource(component, httpSource)
}

func runSource(component string, w Worker) {
	runConnector(config.SourceConnector, component, w)
}

func runConnector(kind config.Kind, component string, w Worker) {
	share := isShare()
	if !share {
		runStandaloneConnector(string(kind)+"-"+component, w)
		return
	}
	runShareConnector(kind, component, w)
}
