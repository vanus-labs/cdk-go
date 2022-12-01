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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	cdkgo "github.com/linkall-labs/cdk-go"
	"github.com/linkall-labs/cdk-go/config"
)

type Config struct {
	cdkgo.SourceConfig `json:",inline" yaml:",inline"`
	Source             string `json:"source" yaml:"source" validate:"required"`
	Secret             Secret
}

func (c *Config) GetSecret() cdkgo.SecretAccessor {
	return &c.Secret
}

type Secret struct {
	Host     string `json:"host" yaml:"host" validate:"required"`
	Username string `json:"username" yaml:"username" validate:"required"`
	Password string `json:"password" yaml:"password" validate:"required"`
}

type ExampleSource struct {
	number int
	source string
	events chan *cdkgo.Tuple
}

func (s *ExampleSource) Initialize(ctx context.Context, cfg cdkgo.ConfigAccessor) error {
	config := cfg.(*Config)
	s.source = config.Source
	fmt.Println(config.Secret)
	s.events = make(chan *cdkgo.Tuple, 100)
	go func() {
		for {
			event := s.makeEvent()
			b, _ := json.Marshal(event)
			success := func() {
				fmt.Println("send event success: " + string(b))
			}
			failed := func() {
				fmt.Println("send event failed: " + string(b))
			}
			s.events <- cdkgo.NewTuple(event, success, failed)
		}
	}()
	return nil
}

func (s *ExampleSource) Name() string {
	return "ExampleSource"
}

func (s *ExampleSource) Destroy() error {
	fmt.Println(fmt.Sprintf("send event number:%d", s.number))
	return nil
}

func (s *ExampleSource) Chan() <-chan *cdkgo.Tuple {
	return s.events
}

func (s *ExampleSource) makeEvent() *ce.Event {
	rand.Seed(time.Now().UnixMilli())
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(2000)+100))
	s.number++
	event := ce.NewEvent()
	event.SetID(fmt.Sprintf("id-%d", s.number))
	event.SetSource(s.source)
	event.SetType("testType")
	event.SetExtension("t", time.Now())
	event.SetData(ce.ApplicationJSON, map[string]interface{}{
		"number": s.number,
		"string": fmt.Sprintf("str-%d", s.number),
	})
	return &event
}

func main() {
	os.Setenv(config.EnvConfigFile, "./_examples/source/config.yaml")
	os.Setenv(config.EnvSecretFile, "./_examples/source/secret.yaml")
	cdkgo.RunSource(&Config{}, &ExampleSource{})
}
