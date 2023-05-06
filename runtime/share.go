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
	"github.com/vanus-labs/vanus-connect-runtime/pkg/controller"
)

func runShareConnector(connectorKind config.Kind, connectorType string, worker common.Worker) {
	ctx := util.SignalContext()
	err := worker.Start(ctx)
	if err != nil {
		log.Error("worker start error", map[string]interface{}{
			"kind":       connectorKind,
			"type":       connectorType,
			log.KeyError: err,
		})
		os.Exit(-1)
	}
	newConnector := func(connectorID, config string) error {
		return worker.RegisterConnector(connectorID, []byte(config))
	}
	ctrl, err := controller.NewController(controller.FilterConnector{
		Kind: string(connectorKind),
		Type: connectorType,
	}, controller.ConnectorHandlerFuncs{
		AddFunc:    newConnector,
		UpdateFunc: newConnector,
		DeleteFunc: func(connectorID string) error {
			worker.RemoveConnector(connectorID)
			return nil
		},
	})
	if err != nil {
		panic("new connector controller failed")
	}
	go ctrl.Run(ctx)
	<-ctx.Done()
	log.Info("received system signal, beginning shutdown", map[string]interface{}{
		"kind": connectorKind,
		"type": connectorType,
	})
	worker.Stop()
	log.Info("connector shutdown graceful", map[string]interface{}{
		"kind": connectorKind,
		"type": connectorType,
	})
}
