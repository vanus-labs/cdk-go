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

package runtime

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	"github.com/cloudevents/sdk-go/v2/protocol"
	"github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/linkall-labs/cdk-go/log"
	cdkutil "github.com/linkall-labs/cdk-go/utils"
)

const (
	configFileEnv = "CONNECTOR_CONFIG"
	secretFileEnv = "CONNECTOR_SECRET"
)

type Connector interface {
	Name() string
	Port() int
	SetLogger(logger log.Logger)
	Init(cfgPath, secretPath string) error
}

type Source interface {
	Connector
	Start() error
	Stop() error
	Receive(ctx context.Context) error
}

type Sink interface {
	Connector
	Destroy() error
	Handle(ctx context.Context, event v2.Event) protocol.Result
}

func RunSource(source Source) {
}

func RunSink(sink Sink) {
	logger := log.NewLogger()
	logger.SetName(sink.Name())
	sink.SetLogger(logger)
	if err := sink.Init(os.Getenv(configFileEnv), os.Getenv(secretFileEnv)); err != nil {
		panic("init config file failed: " + err.Error())
	}
	var ctx = cdkutil.SetupSignalContext()
	ctx = context.WithValue(ctx, log.ConnectorName, sink.Name())
	sa := &sinkApplication{sink: sink}
	run := func() {
		err := sa.startReceive(ctx)
		if err != nil {
			panic("start sink server failed: " + err.Error())
		}
	}
	wait(ctx, sink, run)
	if err := sink.Destroy(); err != nil {
		log.Warning(ctx, "there was error when destroy sink", map[string]interface{}{
			log.KeyError: err,
		})
	} else {
		log.Info(ctx, "the sink server has been shutdown gracefully", map[string]interface{}{
			log.ConnectorName: sink.Name(),
		})
	}
}

func wait(ctx context.Context, c Connector, f func()) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		f()
		wg.Done()
	}()
	log.Info(ctx, "the sink connector started", map[string]interface{}{
		log.ConnectorName: c.Name(),
		"listening":       fmt.Sprintf("0.0.0.0:%d", c.Port()),
	})
	select {
	case <-ctx.Done():
		log.Info(ctx, "received system signal, preparing exit", nil)
	}
}

type sinkApplication struct {
	sink Sink
}

func (sa *sinkApplication) startReceive(ctx context.Context) error {
	ls, err := net.Listen("tcp", fmt.Sprintf(":%d", sa.sink.Port()))
	if err != nil {
		return err
	}

	c, err := client.NewHTTP(http.WithListener(ls), http.WithRequestDataAtContextMiddleware())
	if err != nil {
		return err
	}
	return c.StartReceiver(ctx, sa.sink.Handle)
}
