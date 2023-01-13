// Copyright 2023 Linkall Inc.
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

package internal

import (
	"context"
	"encoding/json"
	"fmt"

	ce "github.com/cloudevents/sdk-go/v2"
	cdkgo "github.com/linkall-labs/cdk-go"
)

var _ cdkgo.Sink = &exampleSink{}

type exampleSink struct {
	number int
}

func NewExampleSink() cdkgo.Sink {
	return &exampleSink{}
}

func (s *exampleSink) Initialize(ctx context.Context, cfg cdkgo.ConfigAccessor) error {
	config := cfg.(*exampleConfig)
	s.number = config.Count
	return nil
}

func (s *exampleSink) Name() string {
	return "ExampleSink"
}

func (s *exampleSink) Destroy() error {
	return nil
}

func (s *exampleSink) Arrived(ctx context.Context, events ...*ce.Event) cdkgo.Result {
	for _, event := range events {
		s.number++
		b, _ := json.Marshal(event)
		fmt.Println(s.number, string(b))
	}
	return cdkgo.SuccessResult
}
