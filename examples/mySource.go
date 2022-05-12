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
	"encoding/json"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/go-logr/logr"
	"github.com/linkall-labs/cdk-go/connector"
	"github.com/linkall-labs/cdk-go/log"
	"io/ioutil"
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
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		s.logger.Error(err, "server listens on 8080 failed.")
	}
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

//CreateSource implements a function to construct a Source
func CreateSource(ctx context.Context, ceClient cloudevents.Client) connector.Source {
	logger := log.FromContext(ctx)
	return &Source{
		client: ceClient,
		logger: logger,
		ctx:    ctx,
	}
}
