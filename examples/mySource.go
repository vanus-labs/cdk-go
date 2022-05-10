package main

import (
	"context"
	"encoding/json"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/go-logr/logr"
	"io/ioutil"
	"linkall.com/cdk-go/v1/connector"
	"linkall.com/cdk-go/v1/log"
	"net/http"
	"time"
)

type Source struct {
	client cloudevents.Client
	logger logr.Logger
	ctx    context.Context
}

func (s *Source) myHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	event := s.Adapt(r)
	s.logger.Info("event", "event", event)
	answer := "receive data success"

	if result := s.client.Send(s.ctx, event); !cloudevents.IsACK(result) {
		s.logger.Info("failed to send cloudEvent", event, result)
		answer = "send CloudEvent failed"
	}
	w.Write([]byte(answer))
}

func (s *Source) Start() error {
	//env := config.Accessor.Get("vance_sink")
	http.HandleFunc("/", s.myHandler)
	http.ListenAndServe(":8080", nil)

	return nil
}

func (s *Source) Adapt(args ...interface{}) cloudevents.Event {
	req := args[0].(*http.Request)
	headerBytes, err := json.Marshal(req.Header)
	if err != nil {
		s.logger.Error(err, "Marshal headers failed")
	}
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		s.logger.Error(err, "Marshal body failed")
	}
	event := cloudevents.NewEvent()
	event.SetSource("vance-http-source")
	event.SetType("http")
	event.SetTime(time.Now())
	event.SetData(cloudevents.ApplicationJSON, map[string]string{
		"headers": string(headerBytes),
		"body":    string(body),
	})
	return event
}

func CreateSource(ctx context.Context, ceClient cloudevents.Client) connector.Source {
	logger := log.FromContext(ctx)
	return &Source{
		client: ceClient,
		logger: logger,
		ctx:    ctx,
	}
}
