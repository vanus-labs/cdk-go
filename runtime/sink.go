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
	"context"
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"sync"

	ce "github.com/cloudevents/sdk-go/v2"
	recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/vanus-labs/cdk-go/config"
	"github.com/vanus-labs/cdk-go/connector"
	"github.com/vanus-labs/cdk-go/log"
	cloudevents "github.com/vanus-labs/cdk-go/proto"
	"github.com/vanus-labs/cdk-go/runtime/sender"
)

type (
	SinkConfigConstructor func() config.SinkConfigAccessor
	SinkConstructor       func() connector.Sink
)

func RunSink(cfgCtor SinkConfigConstructor, sinkCtor SinkConstructor) {
	cfg := cfgCtor()
	sink := sinkCtor()
	err := runConnector(cfg, sink)
	if err != nil {
		log.Error("run sink error", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}
}

type sinkWorker struct {
	cfg  config.SinkConfigAccessor
	sink connector.Sink
	wg   sync.WaitGroup
}

// Make sure the sinkWorker implements the cloudevents.CloudEventsServer.
var _ cloudevents.CloudEventsServer = (*sinkWorker)(nil)

func (w *sinkWorker) Send(ctx context.Context, event *cloudevents.BatchEvent) (*emptypb.Empty, error) {
	pbEvents := event.Events.Events
	events := make([]*ce.Event, len(pbEvents))
	for idx := range pbEvents {
		e, err := sender.FromProto(pbEvents[idx])
		if err != nil {
			return nil, err
		}
		events[idx] = e
	}

	if result := w.sink.Arrived(ctx, events...); result != connector.Success {
		return nil, result.Error()
	}
	return &emptypb.Empty{}, nil
}

func newSinkWorker(cfg config.SinkConfigAccessor, sink connector.Sink) Worker {
	return &sinkWorker{
		cfg:  cfg,
		sink: sink,
	}
}

func (w *sinkWorker) Start(ctx context.Context) error {
	port := w.cfg.GetPort()
	if port > 0 {
		ls, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			return errors.Wrapf(err, "failed to listen port: %d", port)
		}
		ceClient, err := ce.NewClientHTTP(ce.WithListener(ls))
		if err != nil {
			return errors.Wrap(err, "failed to init cloudevents client")
		}
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			err = ceClient.StartReceiver(ctx, w.receive)
			if err != nil {
				panic(fmt.Sprintf("failed to start cloudevnets receiver: %s", err))
			}
		}()
	}

	port = w.cfg.GetGRPCPort()
	if port > 0 {
		listen, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			return err
		}
		recoveryOpt := recovery.WithRecoveryHandlerContext(
			func(ctx context.Context, p interface{}) error {
				log.Error("goroutine panicked", map[string]interface{}{
					log.KeyError: fmt.Sprintf("%v", p),
					"stack":      string(debug.Stack()),
				})
				return status.Errorf(codes.Internal, "%v", p)
			},
		)

		grpcServer := grpc.NewServer(
			grpc.ChainStreamInterceptor(
				recovery.StreamServerInterceptor(recoveryOpt),
			),
			grpc.ChainUnaryInterceptor(
				recovery.UnaryServerInterceptor(recoveryOpt),
			),
		)

		cloudevents.RegisterCloudEventsServer(grpcServer, w)
		log.Info("the grpc server ready to work", nil)

		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			err = grpcServer.Serve(listen)
			if err != nil {
				log.Error("grpc server occurred an error", map[string]interface{}{
					log.KeyError: err,
				})
			}
		}()
	}

	log.Info("the connector started", map[string]interface{}{
		log.ConnectorName: w.sink.Name(),
		"HTTP":            w.cfg.GetPort(),
		"gRPC":            w.cfg.GetGRPCPort(),
	})
	return nil
}

func (w *sinkWorker) receive(ctx context.Context, event ce.Event) ce.Result {
	result := w.sink.Arrived(ctx, &event)
	return result.ConvertToCeResult()
}

func (w *sinkWorker) Stop() error {
	w.wg.Wait()
	return w.sink.Destroy()
}
