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
	"context"
	"net/http"

	ce "github.com/cloudevents/sdk-go/v2"

	"github.com/vanus-labs/cdk-go/connector"
	"github.com/vanus-labs/cdk-go/log"
)

type connectorModel struct {
	connectorID string
	sink        connector.Sink
}

func handHttpRequest(ctx context.Context, model connectorModel, writer http.ResponseWriter, req *http.Request) {
	connectorID := model.connectorID
	sink := model.sink
	event, err := ce.NewEventFromHTTPRequest(req)
	if err != nil {
		log.Info("failed to extract event from request", map[string]interface{}{
			log.KeyError:   err,
			"connector_id": connectorID,
		})
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	validationErr := event.Validate()
	if validationErr != nil {
		log.Info("failed to validate extracted event", map[string]interface{}{
			log.KeyError:   err,
			"connector_id": connectorID,
		})
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	result := sink.Arrived(ctx, event)
	if result != connector.Success {
		log.Info("event process failed", map[string]interface{}{
			log.KeyError:   result.Error(),
			"connector_id": connectorID,
		})
		writer.WriteHeader(int(result.GetCode()))
		writer.Write([]byte(result.GetMsg()))
		return
	}
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("accepted"))
}
