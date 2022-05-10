package main

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/go-logr/logr"
	"linkall.com/cdk-go/v1/config"
	"linkall.com/cdk-go/v1/connector"
	"linkall.com/cdk-go/v1/log"
)

type Sink struct {
	client cloudevents.Client
	logger logr.Logger
	ctx    context.Context
}

func (s *Sink) Start() error {
	s.logger.Info("Start method")
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
	s.logger.Info("start listening on port", "port", config.Accessor.VancePort())
	err := s.client.StartReceiver(s.ctx, s.receive)
	if err == nil {
		s.logger.Error(err, "StartReceiver err")
	}
	return nil
}
func (s *Sink) receive(event cloudevents.Event) {
	s.logger.Info("event-print", "event", event.String())
}

func CreateSink(ctx context.Context, ceClient cloudevents.Client) connector.Sink {
	logger := log.FromContext(ctx)
	return &Sink{
		client: ceClient,
		logger: logger,
		ctx:    ctx,
	}
}
