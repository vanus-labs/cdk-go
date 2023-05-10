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
	"github.com/vanus-labs/cdk-go/config"
	"github.com/vanus-labs/cdk-go/log"
	"github.com/vanus-labs/cdk-go/runtime/common"
	"github.com/vanus-labs/cdk-go/runtime/sink"
	"github.com/vanus-labs/cdk-go/runtime/source"
	"github.com/vanus-labs/cdk-go/util"
)

func RunSink(cfgCtor common.SinkConfigConstructor, sinkCtor common.SinkConstructor) {
	c := common.HTTPConfig{}
	err := util.ParseConfigFile(&c)
	if err != nil {
		panic("parse config failed:" + err.Error())
	}
	worker := sink.NewSinkWorker(cfgCtor, sinkCtor, c)
	runConnector(config.SinkConnector, worker)
}

func RunSource(cfgCtor common.SourceConfigConstructor, sourceCtor common.SourceConstructor) {
	c := common.WorkerConfig{}
	err := util.ParseConfigFile(&c)
	if err != nil {
		panic("parse config failed:" + err.Error())
	}
	worker := source.NewSourceWorker(cfgCtor, sourceCtor, c)
	runConnector(config.SourceConnector, worker)
}

func RunHTTPSource(cfgCtor common.SourceConfigConstructor, sourceCtor common.HTTPSourceConstructor) {
	c := common.HTTPConfig{}
	err := util.ParseConfigFile(&c)
	if err != nil {
		panic("parse config failed:" + err.Error())
	}
	worker := source.NewHTTPSourceWorker(cfgCtor, sourceCtor, c)
	runConnector(config.SourceConnector, worker)
}

func runConnector(kind config.Kind, w common.Worker) {
	log.SetLogLevel(w.Config().LogConfig.GetLogLevel())
	multi := w.Config().Multi
	if !multi {
		runStandaloneConnector(kind, w.Config().ConnectorType, w)
		return
	}
	runMultiConnector(kind, w.Config().ConnectorType, w)
}
