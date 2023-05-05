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

package config

import (
	"fmt"
	"os"
	"strconv"
)

var _ SinkConfigAccessor = &SinkConfig{}

type SinkConfigAccessor interface {
	ConfigAccessor
	// GetPort receive event server use port, default 8080
	GetPort() int
	GetGRPCPort() int
}

type SinkConfig struct {
	Config   `json:",inline" yaml:",inline"`
	Port     int `json:"port" yaml:"port"`
	GRPCPort int `json:"grpc_port" yaml:"grpc_port"`
}

func (c *SinkConfig) GetGRPCPort() int {
	return c.GRPCPort
}

func (c *SinkConfig) GetSecret() SecretAccessor {
	return nil
}

func (c *SinkConfig) Validate() error {
	return c.Config.Validate()
}

func (c *SinkConfig) ConnectorKind() Kind {
	return SinkConnector
}

func (c *SinkConfig) GetPort() int {
	if c.Port > 0 {
		return c.Port
	}
	portStr := os.Getenv(EnvPort)
	if portStr != "" {
		p, err := strconv.ParseInt(portStr, 10, 16)
		if err != nil {
			panic(fmt.Sprintf("parse CONNECTOR_PORT error: %s", err.Error()))
		}
		return int(p)
	}
	return defaultPort
}
