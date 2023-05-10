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

type Kind string

const (
	SinkConnector   Kind = "sink"
	SourceConnector Kind = "source"
)

type SecretAccessor interface{}

type ConfigAccessor interface {
	ConnectorKind() Kind
	Validate() error
	// GetSecret SecretAccessor implement type must be pointer
	GetSecret() SecretAccessor

	GetLogConfig() LogConfig
	GetStoreConfig() StoreConfig
}

type Config struct {
	StoreConfig StoreConfig `json:"store_config" yaml:"store_config"`
	LogConfig   LogConfig   `json:"log_config" yaml:"log_config"`
}

func (c *Config) Validate() error {
	return c.StoreConfig.Validate()
}

func (c *Config) GetStoreConfig() StoreConfig {
	return c.StoreConfig
}

func (c *Config) GetLogConfig() LogConfig {
	return c.LogConfig
}

func (c *Config) GetSecret() SecretAccessor {
	return nil
}

const (
	EnvLogLevel = "LOG_LEVEL"
	EnvTarget   = "CONNECTOR_TARGET"
	EnvPort     = "CONNECTOR_PORT"

	defaultPort     = 8080
	defaultAttempts = 3
)
