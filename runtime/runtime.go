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
	"os"
	"strings"

	"github.com/vanus-labs/cdk-go/config"
	"github.com/vanus-labs/cdk-go/runtime/common"
	"github.com/vanus-labs/cdk-go/runtime/sink"
	"github.com/vanus-labs/cdk-go/runtime/source"
)

func isShare() bool {
	runtime := os.Getenv("CONNECTOR-RUNTIME")
	return strings.ToLower(runtime) == "share"
}

func RunSink(component string, cfgCtor common.SinkConfigConstructor, sinkCtor common.SinkConstructor) {
	share := isShare()
	var worker common.Worker
	if share {
		worker = sink.NewMtSinkWorker(cfgCtor, sinkCtor)
	} else {
		worker = sink.NewSinkWorker(cfgCtor, sinkCtor)
	}
	runConnector(config.SinkConnector, component, worker)
}

func RunSource(component string, cfgCtor common.SourceConfigConstructor, sourceCtor common.SourceConstructor) {
	share := isShare()
	var worker common.Worker
	if share {
		worker = source.NewMtSourceWorker(cfgCtor, sourceCtor)
	} else {
		worker = source.NewSourceWorker(cfgCtor, sourceCtor)
	}
	runSource(component, worker)
}

func RunHttpSource(component string, cfgCtor common.SourceConfigConstructor, sourceCtor common.HTTPSourceConstructor) {
	share := isShare()
	var worker common.Worker
	if share {
		worker = source.NewMtHTTPSourceWorker(cfgCtor, sourceCtor)
	} else {
		worker = source.NewHTTPSourceWorker(cfgCtor, sourceCtor)
	}
	runSource(component, worker)
}

func runSource(component string, w common.Worker) {
	runConnector(config.SourceConnector, component, w)
}

func runConnector(kind config.Kind, component string, w common.Worker) {
	share := isShare()
	if !share {
		runStandaloneConnector(string(kind)+"-"+component, w)
		return
	}
	runShareConnector(kind, component, w)
}
