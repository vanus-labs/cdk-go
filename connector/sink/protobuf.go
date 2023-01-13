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

package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/linkall-labs/cdk-go/config"
	"github.com/linkall-labs/cdk-go/connector"
)

type ProtobufSink interface {
	connector.Connector
	Handle(ctx context.Context, msg proto.Message) error
	NewEvent() proto.Message
	Validate(msg proto.Message) error
}

func WrapProtobufSink(sink ProtobufSink) connector.Sink {
	return &protobufSinkApplication{
		sink: sink,
	}
}

type protobufSinkApplication struct {
	sink ProtobufSink
}

func (sa *protobufSinkApplication) Arrived(ctx context.Context, events ...*v2.Event) connector.Result {
	for _, event := range events {
		e := sa.sink.NewEvent()
		if err := jsonpb.Unmarshal(bytes.NewReader(event.Data()), e); err != nil {
			return connector.NewResult(http.StatusBadRequest,
				fmt.Sprintf("parsing data error: %s", err.Error()))
		}

		m := map[string]interface{}{
			"metadata": map[string]interface{}{
				"id":        event.ID(),
				"source":    event.Source(),
				"type":      event.Type(),
				"time":      event.Time(),
				"extension": event.Extensions(),
			}}
		data, err := json.Marshal(m)
		if err != nil {
			return connector.NewResult(http.StatusBadRequest,
				fmt.Sprintf("parsing metadata error: %s", err.Error()))
		}

		if err = jsonpb.UnmarshalNext(json.NewDecoder(bytes.NewReader(data)), e); err != nil {
			return connector.NewResult(http.StatusBadRequest,
				fmt.Sprintf("unmarshall metadata error: %s", err.Error()))
		}

		if err := sa.sink.Validate(e); err != nil {
			return connector.NewResult(http.StatusBadRequest,
				fmt.Sprintf("validate event error: %s", err.Error()))
		}

		if err := sa.sink.Handle(ctx, e); err != nil {
			return connector.NewResult(http.StatusInternalServerError,
				fmt.Sprintf("handle event error: %s", err.Error()))
		}
	}
	return connector.Success
}

func (sa *protobufSinkApplication) Destroy() error {
	return sa.sink.Destroy()
}

func (sa *protobufSinkApplication) Name() string {
	return sa.sink.Name()
}

func (sa *protobufSinkApplication) Initialize(ctx context.Context, cfg config.ConfigAccessor) error {
	return sa.sink.Initialize(ctx, cfg)
}
