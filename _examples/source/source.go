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
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	cdkgo "github.com/linkall-labs/cdk-go"
	"github.com/linkall-labs/cdk-go/connector"
)

type Config struct {
	connector.SourceConfig `json:",inline" yaml:",inline"`
	Source                 string `json:"source" yaml:"source"`
}

type ExampleSource struct {
	number int
	cfg    *Config
}

func (s *ExampleSource) Initialize(ctx context.Context, cfg connector.ConnectorConfigAccessor) error {
	config := cfg.(*Config)
	s.cfg = config
	return nil
}

func (s *ExampleSource) Name() string {
	return "ExampleSource"
}

func (s *ExampleSource) Destroy() error {
	fmt.Println(fmt.Sprintf("send event number:%d", s.number))
	return nil
}

func (s *ExampleSource) PollEvent() ce.Event {
	time.Sleep(time.Second)
	s.number++
	event := ce.NewEvent()
	event.SetID(fmt.Sprintf("id-%d", s.number))
	event.SetSource(s.cfg.Source)
	event.SetType("testType")
	event.SetData(ce.ApplicationJSON, map[string]interface{}{
		"number": s.number,
		"string": fmt.Sprintf("str-%d", s.number),
	})
	return event
}

func (s *ExampleSource) Commit(event ce.Event) {
	b, _ := json.Marshal(event)
	fmt.Println("send event: " + string(b))
}

func main() {
	configPath := flag.String("config", "./_examples/source/config.yaml", "the cfg file")
	os.Setenv(connector.EnvConfigFile, *configPath)
	cdkgo.RunSource(&Config{}, &ExampleSource{})
}
