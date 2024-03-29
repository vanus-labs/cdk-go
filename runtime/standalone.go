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
		log.Error().Interface("kind", kind).Str("type", connectorType).Err(err).Msg("worker start error")
		os.Exit(-1)
	}
	cfg, err := util.ReadConfigFile()
	if err != nil {
		log.Error().Interface("kind", kind).Str("type", connectorType).Err(err).Msg("new runtime error")
		os.Exit(-1)
	}
	err = worker.RegisterConnector("", cfg)
	if err != nil {
		log.Error().Interface("kind", kind).Str("type", connectorType).Err(err).Msg("read config file error")
		os.Exit(-1)
	}
	<-ctx.Done()
	log.Info().Interface("kind", kind).Str("type", connectorType).Err(err).Msg("received system signal, beginning shutdown")
	_ = worker.Stop()
	log.Info().Interface("kind", kind).Str("type", connectorType).Err(err).Msg("connector shutdown graceful")
}
