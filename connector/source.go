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
	ce "github.com/cloudevents/sdk-go/v2"
)

// Source is the interface a source connector expected to implement.
type Source interface {
	Connector
	// Chan transform relay data to CloudEvents
	Chan() <-chan *Tuple
}

type Tuple struct {
	Event   *ce.Event
	Success func()
	Failed  func(err error)
}

func NewTuple(event *ce.Event, success func(), failed func(error)) *Tuple {
	return &Tuple{
		Event:   event,
		Success: success,
		Failed:  failed,
	}
}
