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
}

type SinkConfig struct {
	Port int `json:"port" yaml:"port"`
}

func (c *SinkConfig) GetSecret() SecretAccessor {
	return nil
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
		p, err := strconv.ParseInt(portStr, 10, 16)
		if err != nil {
			panic(fmt.Sprintf("parse CONNECTOR_PORT error: %s", err.Error()))
		}
		return int(p)
	}
	return defaultPort
}
