package connector

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/go-logr/logr"
	"linkall.com/cdk-go/v1/config"
	"linkall.com/cdk-go/v1/log"
	"strconv"
)

const (
	sourceConstructorStr string = "func(context.Context, client.Client) connector.Source"
	sinkConstructorStr   string = "func(context.Context, client.Client) connector.Sink"
)

// Source is the interface a source connector expected to implement
//
type Source interface {
	Sink
	Adapt(args ...interface{}) cloudevents.Event
}
type SourceConstructor func(ctx context.Context, client cloudevents.Client) Source

type Sink interface {
	Start() error
}
type SinkConstructor func(ctx context.Context, client cloudevents.Client) Sink

func RunSource(connectorName string, sC SourceConstructor) {
	ctx, ceClient := prepareRun(connectorName)
	ctx = cloudevents.ContextWithTarget(ctx, config.Accessor.VanceSink())

	source := sC(ctx, ceClient)
	source.Start()
}
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
