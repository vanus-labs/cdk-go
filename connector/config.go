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

package connector

import (
	"net/url"
	"os"
	"strconv"

	"github.com/pkg/errors"

	"github.com/linkall-labs/cdk-go/util"
)

type Type string

const (
	SinkConnector   Type = "sink"
	SourceConnector Type = "source"
)

type ConfigAccessor interface {
	ConnectorType() Type
	Validate() error
}

type SourceConfigAccessor interface {
	ConfigAccessor
	GetTarget() string
	// GetAttempts send event max attempts, 0 will retry util success, default is 3.
	GetAttempts() int
}

var _ SourceConfigAccessor = &SourceConfig{}

type SourceConfig struct {
	Target            string `json:"v_target" yaml:"v_target"`
	SendEventAttempts *int   `json:"send_event_attempts" yaml:"send_event_attempts"`
}

func (c *SourceConfig) ConnectorType() Type {
	return SourceConnector
}

func (c *SourceConfig) Validate() error {
	target := c.GetTarget()
	if target == "" {
		return errors.New("config target is empty")
	}
	_, err := url.Parse(target)
	if err != nil {
		return errors.Wrap(err, "target is invalid")
	}

	return nil
}
func (c *SourceConfig) GetAttempts() int {
	if c.SendEventAttempts == nil {
		return defaultAttempts
	}
	return *c.SendEventAttempts
}

func (c *SourceConfig) GetTarget() string {
	if c.Target != "" {
		return c.Target
	}
	return os.Getenv(EnvTarget)
}

var _ SinkConfigAccessor = &SinkConfig{}

type SinkConfigAccessor interface {
	ConfigAccessor
	// GetPort receive event server use port, default 8080
	GetPort() int
}

type SinkConfig struct {
	Port int `json:"v_port" yaml:"v_port"`
}

func (c *SinkConfig) Validate() error {
	return nil
}

func (c *SinkConfig) ConnectorType() Type {
	return SinkConnector
}

func (c *SinkConfig) GetPort() int {
	if c.Port > 0 {
		return c.Port
	}
	portStr := os.Getenv(EnvPort)
	if portStr != "" {
		p, err := strconv.ParseInt(EnvPort, 10, 16)
		if err == nil {
			return int(p)
		}
	}
	return defaultPort
}

const (
	EnvTarget     = "CONNECTOR_TARGET"
	EnvPort       = "CONNECTOR_PORT"
	EnvConfigFile = "CONNECTOR_CONFIG"
	secretFileEnv = "CONNECTOR_SECRET"

	defaultPort     = 8080
	defaultAttempts = 3
)

func ParseConfig(cfg ConfigAccessor) error {
	file := os.Getenv(EnvConfigFile)
	if file == "" {
		file = "config.yaml"
	}
	err := util.ParseConfig(file, cfg)
	if err != nil {
		return err
	}
	return nil
}
