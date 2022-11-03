/*
Copyright 2022-Present The Vance Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package connector

import (
	"context"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/client"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"net"
	"os"

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/protocol"
	"github.com/linkall-labs/cdk-go/log"
	cdkutil "github.com/linkall-labs/cdk-go/utils"
)

type Sink interface {
	Connector
	Receive(ctx context.Context, event v2.Event) protocol.Result
}

func RunSink(sink Sink) {
	logger := log.NewLogger()
	logger.SetName(sink.Name())
	sink.SetLogger(logger)
	cfg, err := initConnectorConfig()
	if err != nil {
		panic("init global config file failed: " + err.Error())
	}

	if err := sink.Init(os.Getenv(configFileEnv), os.Getenv(secretFileEnv)); err != nil {
		panic("init config file failed: " + err.Error())
	}
	var ctx = cdkutil.SetupSignalContext()
	ctx = context.WithValue(ctx, log.ConnectorName, sink.Name())

	run := func() error {
		ls, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
		if err != nil {
			return fmt.Errorf("failed to listen port: %d", cfg.Port)
		}

		c, err := client.NewHTTP(cehttp.WithListener(ls), cehttp.WithRequestDataAtContextMiddleware())
		if err != nil {
			return fmt.Errorf("failed to init cloudevnets client: %s", err)
		}

		go func() {
			err = c.StartReceiver(ctx, sink.Receive)
			if err != nil {
				panic(fmt.Sprintf("failed to start cloudevnets receiver: %s", err))
			}
		}()
		log.Info("the connector started", map[string]interface{}{
			log.ConnectorName: sink.Name(),
			"listening":       fmt.Sprintf(":%d", cfg.Port),
		})
		return nil
	}
	wait(ctx, sink, run)
	if err := sink.Destroy(); err != nil {
		log.Warning("there was error when destroy sink", map[string]interface{}{
			log.KeyError: err,
		})
	} else {
		log.Info("the sink server has been shutdown gracefully", map[string]interface{}{
			log.ConnectorName: sink.Name(),
		})
	}
}
