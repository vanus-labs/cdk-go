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

package main

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/protocol"
	"github.com/linkall-labs/cdk-go/connector"
	"github.com/linkall-labs/cdk-go/log"
	cdkutil "github.com/linkall-labs/cdk-go/utils"
)

type Sink struct {
	client cloudevents.Client
	logger log.Logger
	ctx    context.Context
}

func (s *Sink) Init(cfgPath, secretPath string) error {
	cfg := &connector.Config{}
	if err := cdkutil.ParseConfig(cfgPath, cfg); err != nil {
		return err
	}
	/*p, err := cloudevents.NewHTTP()
	if err != nil {
		s.logger.Error(err, "new http protocol failed")
	}

	h, err := cloudevents.NewHTTPReceiveHandler(s.ctx, p, s.receive)
	if err != nil {
		s.logger.Error(err, "new handler failed")
	}

	s.logger.Info("will listen on :8080")
	if err := http.ListenAndServe(":8080", h); err != nil {
		s.logger.Error(err, "new server failed")
	}*/
	//ctx = cloudevents.con
	s.logger.Info(context.Background(), "start listening on port", map[string]interface{}{
		"port": cfg.Target,
	})
	err := s.client.StartReceiver(s.ctx, s.Receive)
	if err == nil {
		s.logger.Error(context.Background(), "StartReceiver err", map[string]interface{}{
			"error": err,
		})
	}
	return nil
}

func (s *Sink) Name() string {
	return "mySink"
}

func (s *Sink) SetLogger(logger log.Logger) {
	s.logger = logger
}

func (s *Sink) Destroy() error {
	return nil
}

func (s *Sink) Receive(ctx context.Context, event v2.Event) protocol.Result {
	s.logger.Info(ctx, "event-print", map[string]interface{}{
		"event": event.String(),
	})
	return nil
}

//func (s *Sink) receive(event cloudevents.Event) {
//	s.logger.Info("event-print", "event", event.String())
//}

// CreateSink implements a function to construct a Sink
func CreateSink(ctx context.Context, ceClient cloudevents.Client) connector.Sink {
	//logger := log.FromContext(ctx)
	return &Sink{
		client: ceClient,
		//logger: ,
		ctx: ctx,
	}
}
