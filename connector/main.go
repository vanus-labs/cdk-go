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
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/go-logr/logr"
	"github.com/linkall-labs/cdk-go/config"
	"github.com/linkall-labs/cdk-go/log"
	"strconv"
)

const (
	sourceConstructorStr string = "func(context.Context, client.Client) connector.Source"
	sinkConstructorStr   string = "func(context.Context, client.Client) connector.Sink"
)

// Source is the interface a source connector expected to implement
type Source interface {
	Sink
	//Adapt transforms data into CloudEvents
	Adapt(args ...interface{}) cloudevents.Event
}

// SourceConstructor is the function to construct a Source
type SourceConstructor func(ctx context.Context, client cloudevents.Client) Source

// Sink is the interface a sink connector expected to implement
type Sink interface {
	Start() error
}

// SinkConstructor is the function to construct a Sink
type SinkConstructor func(ctx context.Context, client cloudevents.Client) Sink

//RunSource method is used to run a source connector
func RunSource(connectorName string, sC SourceConstructor) {
	ctx, ceClient := prepareRun(connectorName)
	ctx = cloudevents.ContextWithTarget(ctx, config.Accessor.VanceSink())

	source := sC(ctx, ceClient)
	source.Start()
}

//RunSink method is used to run a sink connector
func RunSink(connectorName string, sC SinkConstructor) {
	//fmt.Println("run sink")
	ctx, ceClient := prepareRun(connectorName)
	sink := sC(ctx, ceClient)
	sink.Start()
	//s.Start(context.Background())
}

/*func Run(connectorName string, sc interface{}) {
	switch reflect.TypeOf(sc).String() {
	case sourceConstructorStr:
		RunSource(connectorName, sc.(SourceConstructor))
	case sinkConstructorStr:
		RunSink(connectorName, sc.(SinkConstructor))
	default:
		logger := log.Log.WithName("Vance")
		err := errors.New("invalid parameter")
		logger.Error(err, "second parameter is invalid\nIt must be either a:\n"+
			"<func(context.Context, client.Client) connector.Source> or \n"+
			"<func(context.Context, client.Client) connector.Sink>")
	}
}*/
func prepareRun(name string) (context.Context, cloudevents.Client) {
	ctx := context.Background()
	logger := log.Log.WithName(name)
	ctx = logr.NewContext(ctx, logger)
	port, _ := strconv.Atoi(config.Accessor.VancePort())
	op := cloudevents.WithPort(port)
	ceClient, err := cloudevents.NewClientHTTP(op)
	if err != nil {
		logger.Error(err, "create CEClient failed")
	}
	return ctx, ceClient
}
