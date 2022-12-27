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
	"net/url"
	"os"

	"github.com/pkg/errors"
)

type SourceConfigAccessor interface {
	ConfigAccessor
	GetTarget() string
	// GetAttempts send event max attempts, 0 will retry util success, default is 3.
	GetAttempts() int
	GetVanusConfig() *VanusConfig
	GetBatchSize() int
}

type VanusConfig struct {
	Endpoint string `json:"endpoint" yaml:"endpoint" validate:"require"`
	Eventbus string `json:"eventbus" yaml:"eventbus" validate:"require"`
}

var _ SourceConfigAccessor = &SourceConfig{}

type SourceConfig struct {
	Config            `json:",inline" yaml:",inline"`
	Target            string       `json:"target" yaml:"target"`
	SendEventAttempts *int         `json:"send_event_attempts" yaml:"send_event_attempts"`
	Vanus             *VanusConfig `json:"vanus" yaml:"vanus"`
	BatchSize         int          `json:"batch_size" yaml:"batch_size"`
}

func (c *SourceConfig) GetVanusConfig() *VanusConfig {
	return c.Vanus
}

func (c *SourceConfig) GetBatchSize() int {
	return c.BatchSize
}

func (c *SourceConfig) ConnectorType() Type {
	return SourceConnector
}

func (c *SourceConfig) Validate() error {
	target := c.GetTarget()
	if target == "" && c.Vanus == nil {
		return errors.New("config target and vanus can't be both empty")
	}
	// print configuration
	log.Info("config", map[string]interface{}{
		"target": c.Target,
	})

	if target != "" && c.Vanus != nil {
		log.Info("vanus is configured, target was ignored", map[string]interface{}{
			"endpoint": c.Vanus.Endpoint,
			"eventbus": c.Vanus.Eventbus,
		})
	}
	log.Info("config", map[string]interface{}{
		"vanus": c.Vanus,
	})

	if c.BatchSize > 0 && c.GetVanusConfig() == nil {
		log.Warning("config batch_size ignored, because default HTTP sender doesn't support batch mode", nil)
	}
	log.Info("config", map[string]interface{}{
		"batch_size": c.BatchSize,
	})

	log.Info("config", map[string]interface{}{
		"send_event_attempts": c.SendEventAttempts,
	})

	_, err := url.Parse(target)
	if err != nil {
		return errors.Wrap(err, "target is invalid")
	}

	return c.Config.Validate()
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
