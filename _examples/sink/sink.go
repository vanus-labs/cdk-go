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

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	ce "github.com/cloudevents/sdk-go/v2"
	cdkgo "github.com/linkall-labs/cdk-go"
	"github.com/linkall-labs/cdk-go/connector"
)

type Config struct {
	connector.SinkConfig `json:",inline" yaml:",inline"`
	Count                int `json:"count" yaml:"count"`
}

type ExampleSink struct {
	number int
}

func (s *ExampleSink) Initialize(ctx context.Context, cfg connector.ConfigAccessor) error {
	config := cfg.(*Config)
	s.number = config.Count
	return nil
}

func (s *ExampleSink) Name() string {
	return "ExampleSink"
}

func (s *ExampleSink) Destroy() error {
	return nil
}
func (s *ExampleSink) EmitEvent(ctx context.Context, event ce.Event) ce.Result {
	b, _ := json.Marshal(event)
	fmt.Println(string(b))
	return nil
}

func main() {
	configPath := flag.String("config", "./_examples/sink/config.yaml", "the config file")
	os.Setenv(connector.EnvConfigFile, *configPath)
	cdkgo.RunSink(&Config{}, &ExampleSink{})
}
