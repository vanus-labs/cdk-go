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

func isMulti() bool {
	runtime := os.Getenv("CONNECTOR-RUNTIME")
	return strings.ToLower(runtime) == "multi"
}

func RunSink(component string, cfgCtor common.SinkConfigConstructor, sinkCtor common.SinkConstructor) {
	worker := sink.NewSinkWorker(cfgCtor, sinkCtor)
	runConnector(config.SinkConnector, component, worker)
}

func RunSource(component string, cfgCtor common.SourceConfigConstructor, sourceCtor common.SourceConstructor) {
	worker := source.NewSourceWorker(cfgCtor, sourceCtor)
	runConnector(config.SourceConnector, component, worker)
}

func RunHTTPSource(component string, cfgCtor common.SourceConfigConstructor, sourceCtor common.HTTPSourceConstructor) {
	worker := source.NewHTTPSourceWorker(cfgCtor, sourceCtor)
	runConnector(config.SourceConnector, component, worker)
}

func runConnector(kind config.Kind, component string, w common.Worker) {
	multi := isMulti()
	if !multi {
		runStandaloneConnector(string(kind)+"-"+component, w)
		return
	}
	runMultiConnector(kind, component, w)
}
