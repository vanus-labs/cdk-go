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
	"github.com/linkall-labs/cdk-go/log"
)

type SourceConfigAccessor interface {
	ConfigAccessor
	GetProtocolConfig() ProtocolConfigSource
	// GetAttempts send event max attempts, 0 will retry util success, default is 3.
	GetAttempts() int
	GetBatchSize() int
}

var _ SourceConfigAccessor = &SourceConfig{}

type SourceConfig struct {
	Config            `json:",inline" yaml:",inline"`
	Protocol          ProtocolConfigSource `json:"protocol" yaml:"protocol"`
	SendEventAttempts *int                 `json:"send_event_attempts" yaml:"send_event_attempts"`
	BatchSize         int                  `json:"batch_size" yaml:"batch_size"`
}

func (c *SourceConfig) GetBatchSize() int {
	return c.BatchSize
}

func (c *SourceConfig) ConnectorType() Type {
	return SourceConnector
}

func (c *SourceConfig) GetProtocolConfig() ProtocolConfigSource {
	if c.Protocol.Type == "" {
		c.Protocol.Type = HTTPProtocol
	}
	if c.Protocol.Target == "" {
		c.Protocol.Target = getTarget()
	}
	return c.Protocol
}

func (c *SourceConfig) Validate() error {
	p := c.GetProtocolConfig()
	err := p.Validate()
	if err != nil {
		return err
	}
	// print configuration
	log.Info("config", map[string]interface{}{
		"target":   c.Protocol.Target,
		"protocol": c.Protocol.Type,
	})
	if c.BatchSize > 0 && c.Protocol.Type == HTTPProtocol {
		log.Warning("config batch_size ignored, because default HTTP sender doesn't support batch mode", nil)
	}
	return c.Config.Validate()
}

func (c *SourceConfig) GetAttempts() int {
	if c.SendEventAttempts == nil {
		return defaultAttempts
	}
	return *c.SendEventAttempts
}
