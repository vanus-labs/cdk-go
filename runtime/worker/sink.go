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

package worker

import (
	"context"
	"fmt"
	"net"
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

type SinkWorker struct {
	cfg    config.SinkConfigAccessor
	sink   connector.Sink
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// Make sure the SinkWorker implements the cloudevents.CloudEventsServer.
var _ cloudevents.CloudEventsServer = (*SinkWorker)(nil)

func (w *SinkWorker) Send(ctx context.Context, event *cloudevents.BatchEvent) (*emptypb.Empty, error) {
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
		err := result.Error()
		log.Info("event process failed", map[string]interface{}{
			log.KeyError: err,
		})
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func newSinkWorker(cfg config.SinkConfigAccessor, sink connector.Sink) *SinkWorker {
	return &SinkWorker{
		cfg:  cfg,
		sink: sink,
	}
}

func (w *SinkWorker) Start(ctx context.Context) error {
	w.ctx, w.cancel = context.WithCancel(ctx)
	port := w.cfg.GetPort()
	if port > 0 {
		ceClient, err := ce.NewClientHTTP(ce.WithPort(port))
		if err != nil {
			return errors.Wrap(err, "failed to init cloudevents client")
		}
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			err = ceClient.StartReceiver(w.ctx, w.receive)
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

func (w *SinkWorker) receive(ctx context.Context, event ce.Event) ce.Result {
	result := w.sink.Arrived(ctx, &event)
	err := result.ConvertToCeResult()
	if err != nil {
		log.Info("event process failed", map[string]interface{}{
			log.KeyError: err,
		})
	}
	return err
}

func (w *SinkWorker) Stop() error {
	w.cancel()
	w.wg.Wait()
	return w.sink.Destroy()
}
