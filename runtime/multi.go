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
	"fmt"
	"os"
	"strings"

	"github.com/vanus-labs/cdk-go/config"
	"github.com/vanus-labs/cdk-go/log"
	"github.com/vanus-labs/cdk-go/runtime/common"
	"github.com/vanus-labs/cdk-go/util"
	cr "github.com/vanus-labs/vanus-connect-runtime/pkg/runtime"
)

func getConnectorID(connectorID string) string {
	arr := strings.Split(connectorID, "-")
	return arr[len(arr)-1]
}

func runMultiConnector(kind config.Kind, connectorType string, worker common.Worker) {
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
	newConnector := func(connectorID, config string) error {
		return worker.RegisterConnector(getConnectorID(connectorID), []byte(config))
	}
	connectorHandler := cr.ConnectorEventHandlerFuncs{
		AddFunc:    newConnector,
		UpdateFunc: newConnector,
		DeleteFunc: func(connectorID string) error {
			worker.RemoveConnector(getConnectorID(connectorID))
			return nil
		},
	}
	r, err := cr.New(cr.WithFilter(fmt.Sprintf("kind=%s,type=%s", kind, connectorType)),
		cr.WithEventHandler(connectorHandler))
	if err != nil {
		log.Error("new runtime error", map[string]interface{}{
			"kind":       kind,
			"type":       connectorType,
			log.KeyError: err,
		})
		os.Exit(-1)
	}
	go r.Run(ctx)
	<-ctx.Done()
	log.Info("received system signal, beginning shutdown", map[string]interface{}{
		"kind": kind,
		"type": connectorType,
	})
	worker.Stop()
	log.Info("connector shutdown graceful", map[string]interface{}{
		"kind": kind,
		"type": connectorType,
	})
}
