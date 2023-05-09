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

	"github.com/vanus-labs/cdk-go/config"
	"github.com/vanus-labs/cdk-go/log"
	"github.com/vanus-labs/cdk-go/runtime/common"
	"github.com/vanus-labs/cdk-go/util"
)

func runStandaloneConnector(kind config.Kind, connectorType string, worker common.Worker) {
	ctx := util.SignalContext()
	err := worker.Start(ctx)
	if err != nil {
		log.Error("worker start error", map[string]interface{}{
			"kind":       kind,
			"type":       connectorType,
			log.KeyError: err,
		})
		os.Exit(-1)
	}
	cfg, err := util.ReadConfigFile()
	if err != nil {
		log.Error("read config file error", map[string]interface{}{
			"kind":       kind,
			"type":       connectorType,
			log.KeyError: err,
		})
		os.Exit(-1)
	}
	err = worker.RegisterConnector("", cfg)
	if err != nil {
		log.Error("read config file error", map[string]interface{}{
			"kind":       kind,
			"type":       connectorType,
			log.KeyError: err,
		})
		os.Exit(-1)
	}
	<-ctx.Done()
	log.Info("received system signal, beginning shutdown", map[string]interface{}{
		"kind": kind,
		"type": connectorType,
	})
	_ = worker.Stop()
	log.Info("connector shutdown graceful", map[string]interface{}{
		"kind": kind,
		"type": connectorType,
	})
}
