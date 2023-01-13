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

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/cdk-go/log"
	"github.com/pkg/errors"
)

var (
	ErrNotSupportBatch = errors.New("http sender: batch sending isn't supported")
)

type httpSender struct {
	ceClient ce.Client
}

func NewHTTPSender(target string) CloudEventSender {
	ceClient, err := ce.NewClientHTTP(ce.WithTarget(target))
	if err != nil {
		log.Error("failed to init HTTP client", map[string]interface{}{
			log.KeyError: err,
		})
	}
	return &httpSender{
		ceClient: ceClient,
	}
}

func (h *httpSender) SendEvent(ctx context.Context, events ...*ce.Event) error {
	if len(events) == 0 {
		return nil
	}
	if len(events) > 1 {
		return ErrNotSupportBatch
	}
	return h.ceClient.Send(ctx, *events[0])
}
