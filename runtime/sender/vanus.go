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

package sender

import (
	"context"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/cdk-go/log"
	cloudevents "github.com/linkall-labs/cdk-go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type vanusSender struct {
	client   cloudevents.CloudEventsClient
	conn     *grpc.ClientConn
	eventbus string
}

func NewVanusSender(endpoint string, eventbus string) CloudEventSender {
	opts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, endpoint, opts...)
	if err != nil {
		log.Error("failed to connector vanus", map[string]interface{}{
			log.KeyError: err,
		})
		return nil
	}
	return &vanusSender{
		conn:     conn,
		client:   cloudevents.NewCloudEventsClient(conn),
		eventbus: eventbus,
	}
}

func (v *vanusSender) SendEvent(ctx context.Context, events ...*ce.Event) error {
	if len(events) == 0 {
		return nil
	}

	es := make([]*cloudevents.CloudEvent, len(events))
	for idx := range events {
		es[idx], _ = ToProto(events[idx])
	}

	_, err := v.client.Send(ctx, &cloudevents.BatchEvent{
		EventbusName: v.eventbus,
		Events:       &cloudevents.CloudEventBatch{Events: es},
	})
	return err
}
